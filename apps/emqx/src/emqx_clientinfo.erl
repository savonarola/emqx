%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_clientinfo).

-export([
    merge_authn_result/3,
    get_trusted/2,
    maybe_trusted/2,
    mqtt_require_trusted_attributes/1,
    set/3,
    set_trusted/3,
    trusted/1
]).

-export_type([
    key/0,
    key_path/0,
    trusted_attrs/0,
    trusted_mask/0
]).

-type key() :: atom() | binary().
-type key_path() :: key() | [key()].
-type trusted_mask() :: true | #{key() => trusted_mask()}.
-type trusted_attrs() :: #{
    authn => map(),
    clientinfo => trusted_mask(),
    untrusted => trusted_mask()
}.

-define(STATIC_TRUSTED_KEYS, [
    zone,
    protocol,
    peerhost,
    sockport,
    is_bridge,
    cert_pem,
    cn,
    dn,
    listener,
    peername,
    peerport
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec merge_authn_result(
    emqx_types:clientinfo(), emqx_access_control:authn_result(), merge | replace
) -> emqx_types:clientinfo().
merge_authn_result(ClientInfo0, AuthResult0, ClientAttrsMode) ->
    TrustedMask0 = maps:get(trusted_attrs, AuthResult0, #{}),
    {ClientInfo1, TrustedMask1} = merge_client_attrs(
        ClientInfo0, AuthResult0, ClientAttrsMode, TrustedMask0
    ),
    {ClientInfo2, TrustedMask2} = apply_clientid_override(
        ClientInfo1, AuthResult0, TrustedMask1
    ),
    {ClientInfo, TrustedMask3} = apply_zone_override(ClientInfo2, AuthResult0, TrustedMask2),
    TrustedMask = merge_masks(TrustedMask3, configured_trusted_mask(ClientInfo)),
    ExpireAt = maps:get(expire_at, AuthResult0, undefined),
    AuthnResult0 = maps:without(
        [client_attrs, clientid_override, expire_at, trusted_attrs, zone_override],
        AuthResult0
    ),
    AuthnResult = AuthnResult0#{
        is_superuser => maps:get(is_superuser, AuthnResult0, false),
        auth_expire_at => ExpireAt
    },
    TrustedAttrs0 = maps:get(trusted_attrs, ClientInfo, #{}),
    Untrusted0 = maps:get(untrusted, TrustedAttrs0, #{}),
    Untrusted = remove_trusted_mask(Untrusted0, TrustedMask),
    ClientInfoWithoutAuthn = maps:without(
        [acl, auth_expire_at, expire_at, is_superuser], ClientInfo
    ),
    ClientInfoWithoutAuthn#{
        trusted_attrs => TrustedAttrs0#{
            authn => AuthnResult,
            clientinfo => TrustedMask,
            untrusted => Untrusted
        }
    }.

-spec get_trusted(emqx_types:clientinfo(), key_path()) -> {ok, term()} | error.
get_trusted(ClientInfo, Key) ->
    Path = key_path(Key),
    case authn_find(Path, ClientInfo) of
        {ok, _} = Found -> Found;
        error -> trusted_find(Path, ClientInfo)
    end.

-spec maybe_trusted(emqx_types:clientinfo(), boolean()) -> emqx_types:clientinfo().
maybe_trusted(ClientInfo, false) ->
    ClientInfo;
maybe_trusted(ClientInfo, true) ->
    trusted(ClientInfo).

-spec mqtt_require_trusted_attributes(emqx_types:clientinfo()) -> boolean().
mqtt_require_trusted_attributes(#{zone := Zone}) ->
    Default = emqx_security_profile:policy(mqtt_require_trusted_attributes),
    emqx_config:get_zone_conf(Zone, [mqtt, require_trusted_attributes], Default).

-spec set(emqx_types:clientinfo(), key_path(), term()) -> emqx_types:clientinfo().
set(ClientInfo0, Key, Value) ->
    Path = key_path(Key),
    ClientInfo1 = emqx_utils_maps:deep_force_put(Path, ClientInfo0, Value),
    TrustedAttrs0 = maps:get(trusted_attrs, ClientInfo1, #{}),
    ClientInfoMask0 = maps:get(clientinfo, TrustedAttrs0, #{}),
    ClientInfoMask = remove_mask(Path, ClientInfoMask0),
    Untrusted0 = maps:get(untrusted, TrustedAttrs0, #{}),
    Untrusted = put_mask(Path, Untrusted0),
    Authn0 = maps:get(authn, TrustedAttrs0, #{}),
    Authn = remove_authn(Path, Authn0),
    ClientInfo1#{
        trusted_attrs => TrustedAttrs0#{
            authn => Authn,
            clientinfo => ClientInfoMask,
            untrusted => Untrusted
        }
    }.

-spec set_trusted(emqx_types:clientinfo(), key_path(), term()) -> emqx_types:clientinfo().
set_trusted(ClientInfo0, Key, Value) ->
    Path = key_path(Key),
    ClientInfo1 = emqx_utils_maps:deep_force_put(Path, ClientInfo0, Value),
    TrustedAttrs0 = maps:get(trusted_attrs, ClientInfo1, #{}),
    ClientInfoMask0 = maps:get(clientinfo, TrustedAttrs0, #{}),
    ClientInfoMask = put_mask(Path, ClientInfoMask0),
    Untrusted0 = maps:get(untrusted, TrustedAttrs0, #{}),
    Untrusted = remove_mask(Path, Untrusted0),
    Authn0 = maps:get(authn, TrustedAttrs0, #{}),
    Authn = remove_authn(Path, Authn0),
    ClientInfo1#{
        trusted_attrs => TrustedAttrs0#{
            authn => Authn,
            clientinfo => ClientInfoMask,
            untrusted => Untrusted
        }
    }.

-spec trusted(emqx_types:clientinfo()) -> map().
trusted(ClientInfo) ->
    TrustedAttrs = maps:get(trusted_attrs, ClientInfo, #{}),
    Mask = maps:get(clientinfo, TrustedAttrs, #{}),
    Untrusted = maps:get(untrusted, TrustedAttrs, #{}),
    Static = maps:with(?STATIC_TRUSTED_KEYS, ClientInfo),
    Masked = apply_mask(maps:remove(trusted_attrs, ClientInfo), Mask),
    TrustedClientInfo0 = emqx_utils_maps:deep_merge(Static, Masked),
    TrustedClientInfo = apply_exclusions(TrustedClientInfo0, Untrusted),
    case maps:get(authn, TrustedAttrs, #{}) of
        Authn when map_size(Authn) =:= 0 ->
            TrustedClientInfo#{trusted_attrs => #{clientinfo => Mask}};
        Authn ->
            TrustedClientInfo#{trusted_attrs => #{authn => Authn, clientinfo => Mask}}
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

merge_client_attrs(ClientInfo, #{client_attrs := Attrs}, merge, TrustedMask) ->
    ExistingAttrs = maps:get(client_attrs, ClientInfo, #{}),
    merge_returned_client_attrs(
        ClientInfo#{client_attrs => maps:merge(ExistingAttrs, Attrs)}, Attrs, TrustedMask
    );
merge_client_attrs(ClientInfo, #{client_attrs := Attrs}, replace, TrustedMask) ->
    merge_returned_client_attrs(ClientInfo#{client_attrs => Attrs}, Attrs, TrustedMask);
merge_client_attrs(ClientInfo, _AuthResult, _Mode, TrustedMask) ->
    {ClientInfo, TrustedMask}.

merge_returned_client_attrs(ClientInfo, Attrs, TrustedMask) ->
    AttrsMask = maps:map(fun(_Name, _Value) -> true end, Attrs),
    {ClientInfo, merge_masks(TrustedMask, #{client_attrs => AttrsMask})}.

apply_clientid_override(ClientInfo, #{clientid_override := ClientId}, TrustedMask) when
    is_binary(ClientId) andalso ClientId =/= <<>>
->
    {ClientInfo#{clientid => ClientId}, put_mask([clientid], TrustedMask)};
apply_clientid_override(ClientInfo, _AuthResult, TrustedMask) ->
    {ClientInfo, TrustedMask}.

apply_zone_override(ClientInfo, #{zone_override := Zone}, TrustedMask) when is_binary(Zone) ->
    try emqx_config_zones:assert_zone_exists(Zone) of
        ok ->
            NewZone = binary_to_existing_atom(Zone, utf8),
            {ClientInfo#{zone => NewZone}, put_mask([zone], TrustedMask)}
    catch
        throw:{unknown_zone, _} ->
            {ClientInfo, TrustedMask}
    end;
apply_zone_override(ClientInfo, _AuthResult, TrustedMask) ->
    {ClientInfo, TrustedMask}.

configured_trusted_mask(#{zone := Zone} = ClientInfo) ->
    Paths = emqx_config:get_zone_conf(Zone, [mqtt, trusted_client_attributes], []),
    lists:foldl(
        fun(Path, Mask) ->
            case resolve_configured_path(Path, ClientInfo) of
                {ok, ResolvedPath} -> put_mask(ResolvedPath, Mask);
                error -> Mask
            end
        end,
        #{},
        Paths
    );
configured_trusted_mask(_ClientInfo) ->
    #{}.

resolve_configured_path(Path, ClientInfo) when is_binary(Path) ->
    case binary:split(Path, <<".">>, [global]) of
        Segments = [First | _] when First =/= <<>> ->
            resolve_configured_path(Segments, ClientInfo, []);
        _ ->
            error
    end.

resolve_configured_path([], _Value, Acc) ->
    {ok, lists:reverse(Acc)};
resolve_configured_path([Segment | Rest], Value, Acc) when is_map(Value) ->
    case configured_key(Segment, Value) of
        {ok, Key} -> resolve_configured_path(Rest, maps:get(Key, Value), [Key | Acc]);
        error -> error
    end;
resolve_configured_path(_Segments, _Value, _Acc) ->
    error.

configured_key(Segment, Map) ->
    case maps:is_key(Segment, Map) of
        true ->
            {ok, Segment};
        false ->
            configured_atom_key(Segment, maps:keys(Map))
    end.

configured_atom_key(Segment, [Key | Rest]) when is_atom(Key) ->
    case atom_to_binary(Key) of
        Segment -> {ok, Key};
        _ -> configured_atom_key(Segment, Rest)
    end;
configured_atom_key(Segment, [_Key | Rest]) ->
    configured_atom_key(Segment, Rest);
configured_atom_key(_Segment, []) ->
    error.

merge_masks(true, _Mask) ->
    true;
merge_masks(_Mask, true) ->
    true;
merge_masks(Mask1, Mask2) ->
    maps:fold(
        fun(Key, Value2, Acc) ->
            case maps:find(Key, Acc) of
                {ok, Value1} -> Acc#{Key => merge_masks(Value1, Value2)};
                error -> Acc#{Key => Value2}
            end
        end,
        Mask1,
        Mask2
    ).

remove_trusted_mask(_Untrusted, true) ->
    #{};
remove_trusted_mask(Untrusted, Trusted) when is_map(Untrusted), is_map(Trusted) ->
    maps:fold(
        fun
            (Key, true, Acc) ->
                maps:remove(Key, Acc);
            (Key, SubMask, Acc) ->
                case maps:find(Key, Acc) of
                    {ok, Excluded} when is_map(Excluded) ->
                        case remove_trusted_mask(Excluded, SubMask) of
                            Empty when map_size(Empty) =:= 0 -> maps:remove(Key, Acc);
                            Remaining -> Acc#{Key => Remaining}
                        end;
                    _ ->
                        Acc
                end
        end,
        Untrusted,
        Trusted
    );
remove_trusted_mask(Untrusted, _Trusted) ->
    Untrusted.

key_path(Key) when is_atom(Key); is_binary(Key) ->
    [Key];
key_path([_ | _] = Path) ->
    Path.

authn_find([Key], #{trusted_attrs := #{authn := Authn}}) ->
    maps:find(Key, Authn);
authn_find(_Path, _ClientInfo) ->
    error.

trusted_find(Path, ClientInfo) ->
    case emqx_utils_maps:deep_find(Path, trusted(ClientInfo)) of
        {ok, Value} -> {ok, Value};
        {not_found, _, _} -> error
    end.

apply_mask(Data, true) ->
    Data;
apply_mask(Data, Mask) when is_map(Data), is_map(Mask) ->
    maps:fold(
        fun(Key, SubMask, Acc) ->
            case maps:find(Key, Data) of
                {ok, Value} ->
                    case apply_mask(Value, SubMask) of
                        #{} = Empty when map_size(Empty) =:= 0 -> Acc;
                        TrustedValue -> Acc#{Key => TrustedValue}
                    end;
                error ->
                    Acc
            end
        end,
        #{},
        Mask
    );
apply_mask(_Data, _Mask) ->
    #{}.

apply_exclusions(_Data, true) ->
    #{};
apply_exclusions(Data, Mask) when is_map(Data), is_map(Mask) ->
    maps:fold(
        fun
            (Key, true, Acc) ->
                maps:remove(Key, Acc);
            (Key, SubMask, Acc) ->
                case maps:find(Key, Acc) of
                    {ok, Value} when is_map(Value) ->
                        Acc#{Key => apply_exclusions(Value, SubMask)};
                    _ ->
                        Acc
                end
        end,
        Data,
        Mask
    );
apply_exclusions(Data, _Mask) ->
    Data.

put_mask(_Path, true) ->
    true;
put_mask(Path, Mask) ->
    emqx_utils_maps:deep_force_put(Path, Mask, true).

remove_mask(_Path, Mask = #{}) when map_size(Mask) =:= 0 ->
    Mask;
remove_mask(_Path, true) ->
    %% A universal mask cannot represent one excluded field. The `untrusted'
    %% mask takes precedence and records that exclusion.
    true;
remove_mask(Path, Mask) ->
    emqx_utils_maps:deep_remove(Path, Mask).

remove_authn([Key], Authn) ->
    maps:remove(Key, Authn);
remove_authn(_Path, Authn) ->
    Authn.
