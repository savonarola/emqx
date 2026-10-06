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
    clientinfo => trusted_mask()
}.

-define(AUTHN_KEYS, [is_superuser, auth_expire_at, acl]).

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
    | ?AUTHN_KEYS
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec merge_authn_result(
    emqx_types:clientinfo(), emqx_access_control:authn_result(), merge | replace
) -> emqx_types:clientinfo().
merge_authn_result(ClientInfo0, AuthResult0, ClientAttrsMode) ->
    ClientInfo1 = merge_client_attrs(ClientInfo0, AuthResult0, ClientAttrsMode),
    ClientInfo2 = apply_clientid_override(ClientInfo1, AuthResult0),
    ClientInfo = apply_zone_override(ClientInfo2, AuthResult0),
    ExpireAt = maps:get(expire_at, AuthResult0, undefined),
    AuthnResult0 = maps:without(
        [client_attrs, clientid_override, expire_at, trusted_attrs, zone_override],
        AuthResult0
    ),
    AuthnResult = AuthnResult0#{
        is_superuser => maps:get(is_superuser, AuthnResult0, false),
        auth_expire_at => ExpireAt
    },
    ClientInfoWithoutAuthn = maps:without(
        [trusted_attrs, expire_at | ?AUTHN_KEYS], ClientInfo
    ),
    case require_trusted_attributes(ClientInfo) of
        false ->
            maps:merge(ClientInfoWithoutAuthn, AuthnResult);
        true ->
            TrustedMask = authn_trusted_mask(ClientInfo, AuthResult0),
            Authn = maps:without(?AUTHN_KEYS, AuthnResult),
            TrustedAttrs = put_authn(#{clientinfo => TrustedMask}, Authn),
            ClientInfoWithAuthn = maps:merge(
                ClientInfoWithoutAuthn, maps:with(?AUTHN_KEYS, AuthnResult)
            ),
            ClientInfoWithAuthn#{trusted_attrs => TrustedAttrs}
    end.

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
    emqx_config:get_zone_conf(Zone, [mqtt, require_trusted_attributes], Default);
mqtt_require_trusted_attributes(ClientInfo) when not is_map_key(zone, ClientInfo) ->
    mqtt_require_trusted_attributes(#{zone => default}).

-spec set(emqx_types:clientinfo(), key_path(), term()) -> emqx_types:clientinfo().
set(#{trusted_attrs := #{clientinfo := true}} = ClientInfo, Key, Value) ->
    emqx_utils_maps:deep_force_put(key_path(Key), ClientInfo, Value);
set(#{trusted_attrs := #{clientinfo := Mask0} = TrustedAttrs0} = ClientInfo0, Key, Value) ->
    Path = key_path(Key),
    ClientInfo1 = emqx_utils_maps:deep_force_put(Path, ClientInfo0, Value),
    TrustedAttrs = remove_authn(Path, TrustedAttrs0),
    ClientInfo1#{
        trusted_attrs := TrustedAttrs#{clientinfo := remove_mask(Path, Mask0)}
    };
set(ClientInfo, Key, Value) ->
    emqx_utils_maps:deep_force_put(key_path(Key), ClientInfo, Value).

-spec set_trusted(emqx_types:clientinfo(), key_path(), term()) -> emqx_types:clientinfo().
set_trusted(#{trusted_attrs := #{clientinfo := true}} = ClientInfo, Key, Value) ->
    emqx_utils_maps:deep_force_put(key_path(Key), ClientInfo, Value);
set_trusted(
    #{trusted_attrs := #{clientinfo := Mask0} = TrustedAttrs0} = ClientInfo0, Key, Value
) ->
    Path = key_path(Key),
    ClientInfo1 = emqx_utils_maps:deep_force_put(Path, ClientInfo0, Value),
    TrustedAttrs = remove_authn(Path, TrustedAttrs0),
    ClientInfo1#{
        trusted_attrs := TrustedAttrs#{clientinfo := put_mask(Path, Mask0)}
    };
set_trusted(ClientInfo0, Key, Value) ->
    Path = key_path(Key),
    ClientInfo = emqx_utils_maps:deep_force_put(Path, ClientInfo0, Value),
    case require_trusted_attributes(ClientInfo) of
        false -> ClientInfo;
        true -> ClientInfo#{trusted_attrs => #{clientinfo => put_mask(Path, #{})}}
    end.

-spec trusted(emqx_types:clientinfo()) -> map().
trusted(#{trusted_attrs := #{clientinfo := Mask} = TrustedAttrs} = ClientInfo) ->
    Static = maps:with(?STATIC_TRUSTED_KEYS, ClientInfo),
    Masked = apply_mask(maps:remove(trusted_attrs, ClientInfo), Mask),
    TrustedClientInfo = emqx_utils_maps:deep_merge(Static, Masked),
    TrustedClientInfo#{trusted_attrs => TrustedAttrs};
trusted(ClientInfo) ->
    maps:with(?STATIC_TRUSTED_KEYS, ClientInfo).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

require_trusted_attributes(ClientInfo) ->
    Default = emqx_security_profile:policy(multi_tenancy_require_trusted_attributes),
    emqx_authz_context:require_trusted_attributes() orelse
        emqx:get_config([multi_tenancy, require_trusted_attributes], Default) orelse
        mqtt_require_trusted_attributes(ClientInfo).

merge_client_attrs(ClientInfo, #{client_attrs := Attrs}, merge) ->
    ExistingAttrs = maps:get(client_attrs, ClientInfo, #{}),
    ClientInfo#{client_attrs => maps:merge(ExistingAttrs, Attrs)};
merge_client_attrs(ClientInfo, #{client_attrs := Attrs}, replace) ->
    ClientInfo#{client_attrs => Attrs};
merge_client_attrs(ClientInfo, _AuthResult, _Mode) ->
    ClientInfo.

apply_clientid_override(ClientInfo, #{clientid_override := ClientId}) when
    is_binary(ClientId) andalso ClientId =/= <<>>
->
    ClientInfo#{clientid => ClientId};
apply_clientid_override(ClientInfo, _AuthResult) ->
    ClientInfo.

apply_zone_override(ClientInfo, #{zone_override := Zone}) when is_binary(Zone) ->
    try emqx_config_zones:assert_zone_exists(Zone) of
        ok ->
            NewZone = binary_to_existing_atom(Zone, utf8),
            ClientInfo#{zone => NewZone}
    catch
        throw:{unknown_zone, _} ->
            ClientInfo
    end;
apply_zone_override(ClientInfo, _AuthResult) ->
    ClientInfo.

authn_trusted_mask(_ClientInfo, #{trusted_attrs := true}) ->
    true;
authn_trusted_mask(ClientInfo, AuthResult) ->
    Mask0 = maps:get(trusted_attrs, AuthResult, #{}),
    Mask1 = returned_client_attrs_mask(AuthResult, Mask0),
    Mask = clientid_override_mask(AuthResult, Mask1),
    merge_masks(Mask, configured_trusted_mask(ClientInfo)).

returned_client_attrs_mask(#{client_attrs := Attrs}, Mask) ->
    AttrsMask = maps:map(fun(_Name, _Value) -> true end, Attrs),
    merge_masks(Mask, #{client_attrs => AttrsMask});
returned_client_attrs_mask(_AuthResult, Mask) ->
    Mask.

clientid_override_mask(#{clientid_override := ClientId}, Mask) when
    is_binary(ClientId) andalso ClientId =/= <<>>
->
    put_mask([clientid], Mask);
clientid_override_mask(_AuthResult, Mask) ->
    Mask.

put_authn(TrustedAttrs, Authn) when map_size(Authn) =:= 0 ->
    maps:remove(authn, TrustedAttrs);
put_authn(TrustedAttrs, Authn) ->
    TrustedAttrs#{authn => Authn}.

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

put_mask(_Path, true) ->
    true;
put_mask(Path, Mask) ->
    emqx_utils_maps:deep_force_put(Path, Mask, true).

remove_mask(_Path, Mask = #{}) when map_size(Mask) =:= 0 ->
    Mask;
remove_mask(Path, Mask) ->
    emqx_utils_maps:deep_remove(Path, Mask).

remove_authn([Key], #{authn := Authn} = TrustedAttrs) ->
    put_authn(TrustedAttrs, maps:remove(Key, Authn));
remove_authn(_Path, TrustedAttrs) ->
    TrustedAttrs.
