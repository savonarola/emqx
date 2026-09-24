%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_clientinfo).

-export([
    get_trusted/2,
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

-spec get_trusted(emqx_types:clientinfo(), key_path()) -> {ok, term()} | error.
get_trusted(ClientInfo, Key) ->
    Path = key_path(Key),
    case authn_find(Path, ClientInfo) of
        {ok, _} = Found -> Found;
        error -> trusted_find(Path, ClientInfo)
    end.

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
            TrustedClientInfo;
        Authn ->
            TrustedClientInfo#{trusted_attrs => #{authn => Authn}}
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

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
