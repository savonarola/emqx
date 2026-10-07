%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_clientinfo).

-export([
    merge_authn_result/3,
    get/2,
    get/3,
    get_trusted/2,
    get_trusted/3,
    is_trusted/2,
    maybe_trusted/2,
    maybe_trusted_for_mqtt/1,
    mqtt_require_trusted_attributes/1,
    set/3,
    set_trusted/3,
    trusted/1
]).

-export_type([
    key/0,
    key_path/0,
    trusted_mask/0
]).

-type key() :: atom() | binary().
-type key_path() :: key() | [key()].
-type trusted_mask() :: true | #{key() => trusted_mask()}.

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
    peerport,
    authn
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
    ClientInfoWithoutAuthn = maps:without(
        [authn, trusted_attrs, expire_at | ?AUTHN_KEYS], ClientInfo
    ),
    ClientInfoWithKnownAuthn = merge_acl(
        ClientInfoWithoutAuthn#{
            is_superuser => maps:get(is_superuser, AuthResult0, false),
            auth_expire_at => maps:get(expire_at, AuthResult0, undefined)
        },
        AuthResult0
    ),
    Authn = maps:without(
        [
            is_superuser,
            acl,
            client_attrs,
            clientid_override,
            expire_at,
            trusted_attrs,
            zone_override
        ],
        AuthResult0
    ),
    ClientInfoWithAuthn = put_authn(ClientInfoWithKnownAuthn, Authn),
    case require_trusted_attributes(ClientInfoWithAuthn) of
        false ->
            ClientInfoWithAuthn;
        true ->
            ClientInfoWithAuthn#{
                trusted_attrs => authn_trusted_mask(ClientInfoWithAuthn, AuthResult0)
            }
    end.

-spec get(emqx_types:clientinfo(), key_path()) -> {ok, term(), boolean()} | error.
get(ClientInfo, Key) ->
    Path = key_path(Key),
    case emqx_utils_maps:deep_find(Path, ClientInfo) of
        {ok, Value} -> {ok, Value, is_trusted(ClientInfo, Path)};
        {not_found, _, _} -> error
    end.

-spec get(emqx_types:clientinfo(), key_path(), term()) -> {term(), boolean()}.
get(ClientInfo, Key, Default) ->
    case get(ClientInfo, Key) of
        {ok, Value, IsTrusted} -> {Value, IsTrusted};
        error -> {Default, false}
    end.

-spec get_trusted(emqx_types:clientinfo(), key_path()) -> {ok, term()} | error.
get_trusted(ClientInfo, Key) when
    Key =:= is_superuser; Key =:= auth_expire_at; Key =:= acl
->
    maps:find(Key, ClientInfo);
get_trusted(ClientInfo, [Key]) ->
    get_trusted(ClientInfo, Key);
get_trusted(ClientInfo, Key) ->
    case emqx_utils_maps:deep_find(key_path(Key), trusted(ClientInfo)) of
        {ok, Value} -> {ok, Value};
        {not_found, _, _} -> error
    end.

-spec get_trusted(emqx_types:clientinfo(), key_path(), term()) -> term().
get_trusted(ClientInfo, Key, Default) ->
    case get_trusted(ClientInfo, Key) of
        {ok, Value} -> Value;
        error -> Default
    end.

-spec is_trusted(emqx_types:clientinfo(), key_path()) -> boolean().
is_trusted(ClientInfo, Key) ->
    Path = key_path(Key),
    is_statically_trusted(Path) orelse is_input_trusted(ClientInfo, Path).

-spec maybe_trusted(emqx_types:clientinfo(), boolean()) -> emqx_types:clientinfo().
maybe_trusted(ClientInfo, false) ->
    ClientInfo;
maybe_trusted(ClientInfo, true) ->
    trusted(ClientInfo).

-spec maybe_trusted_for_mqtt(emqx_types:clientinfo()) -> emqx_types:clientinfo().
maybe_trusted_for_mqtt(ClientInfo) ->
    maybe_trusted(ClientInfo, mqtt_require_trusted_attributes(ClientInfo)).

-spec mqtt_require_trusted_attributes(emqx_types:clientinfo()) -> boolean().
mqtt_require_trusted_attributes(#{zone := Zone}) ->
    Default = emqx_security_profile:policy(mqtt_require_trusted_attributes),
    emqx_config:get_zone_conf(Zone, [mqtt, require_trusted_attributes], Default);
mqtt_require_trusted_attributes(ClientInfo) when not is_map_key(zone, ClientInfo) ->
    mqtt_require_trusted_attributes(#{zone => default}).

-spec set(emqx_types:clientinfo(), key_path(), term()) -> emqx_types:clientinfo().
set(ClientInfo, Key, Value) ->
    Path = key_path(Key),
    case is_statically_trusted(Path) of
        true -> error({statically_trusted_attribute, Path});
        false -> do_set(ClientInfo, Path, Value)
    end.

-spec set_trusted(emqx_types:clientinfo(), key_path(), term()) -> emqx_types:clientinfo().
set_trusted(ClientInfo, Key, Value) ->
    Path = key_path(Key),
    case is_statically_trusted(Path) of
        true -> emqx_utils_maps:deep_force_put(Path, ClientInfo, Value);
        false -> do_set_trusted(ClientInfo, Path, Value)
    end.

-spec trusted(emqx_types:clientinfo()) -> map().
trusted(#{trusted_attrs := Mask} = ClientInfo) ->
    Static = maps:with(?STATIC_TRUSTED_KEYS, ClientInfo),
    Masked = apply_mask(maps:remove(trusted_attrs, ClientInfo), Mask),
    TrustedClientInfo = emqx_utils_maps:deep_merge(Static, Masked),
    TrustedClientInfo#{trusted_attrs => Mask};
trusted(ClientInfo) ->
    maps:with(?STATIC_TRUSTED_KEYS, ClientInfo).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

is_statically_trusted([Key | _]) ->
    lists:member(Key, ?STATIC_TRUSTED_KEYS).

is_input_trusted(#{trusted_attrs := Mask}, Path) ->
    is_mask_trusted(Path, Mask);
is_input_trusted(_ClientInfo, _Path) ->
    false.

do_set(#{trusted_attrs := true} = ClientInfo, Path, Value) ->
    emqx_utils_maps:deep_force_put(Path, ClientInfo, Value);
do_set(#{trusted_attrs := Mask0} = ClientInfo0, Path, Value) ->
    ClientInfo1 = emqx_utils_maps:deep_force_put(Path, ClientInfo0, Value),
    ClientInfo1#{trusted_attrs := remove_mask(Path, Mask0)};
do_set(ClientInfo, Path, Value) ->
    emqx_utils_maps:deep_force_put(Path, ClientInfo, Value).

do_set_trusted(#{trusted_attrs := true} = ClientInfo, Path, Value) ->
    emqx_utils_maps:deep_force_put(Path, ClientInfo, Value);
do_set_trusted(#{trusted_attrs := Mask0} = ClientInfo0, Path, Value) ->
    ClientInfo1 = emqx_utils_maps:deep_force_put(Path, ClientInfo0, Value),
    ClientInfo1#{trusted_attrs := put_mask(Path, Mask0)};
do_set_trusted(ClientInfo0, Path, Value) ->
    ClientInfo = emqx_utils_maps:deep_force_put(Path, ClientInfo0, Value),
    case require_trusted_attributes(ClientInfo) of
        false -> ClientInfo;
        true -> ClientInfo#{trusted_attrs => put_mask(Path, #{})}
    end.

require_trusted_attributes(ClientInfo) ->
    Default = emqx_security_profile:policy(multi_tenancy_require_trusted_attributes),
    emqx_authz_context:require_trusted_attributes() orelse
        emqx:get_config([multi_tenancy, require_trusted_attributes], Default) orelse
        mqtt_require_trusted_attributes(ClientInfo).

merge_acl(ClientInfo, #{acl := Acl}) ->
    ClientInfo#{acl => Acl};
merge_acl(ClientInfo, _AuthResult) ->
    ClientInfo.

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

put_authn(ClientInfo, Authn) when map_size(Authn) =:= 0 ->
    ClientInfo;
put_authn(ClientInfo, Authn) ->
    ClientInfo#{authn => Authn}.

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

is_mask_trusted(_Path, true) ->
    true;
is_mask_trusted([Key | Rest], Mask) when is_map(Mask) ->
    case Mask of
        #{Key := SubMask} -> is_mask_trusted(Rest, SubMask);
        _ -> false
    end;
is_mask_trusted(_Path, _Mask) ->
    false.

put_mask(_Path, true) ->
    true;
put_mask(Path, Mask) ->
    emqx_utils_maps:deep_force_put(Path, Mask, true).

remove_mask(_Path, Mask = #{}) when map_size(Mask) =:= 0 ->
    Mask;
remove_mask(Path, Mask) ->
    emqx_utils_maps:deep_remove(Path, Mask).
