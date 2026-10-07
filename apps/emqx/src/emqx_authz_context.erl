%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_context).

-export([make/1, make_persist/1, require_trusted_attributes/0]).

-export_type([
    t/0,
    legacy/0,
    restricted/0
]).

-type legacy() :: emqx_types:clientinfo().
-type restricted() :: #{
    zone => emqx_types:zone() | undefined,
    protocol => emqx_types:protocol(),
    peerhost => emqx_types:peerhost(),
    sockport => non_neg_integer(),
    clientid => emqx_types:clientid(),
    username => emqx_types:username(),
    is_superuser => boolean(),
    auth_expire_at => non_neg_integer() | undefined,
    acl => term(),
    is_bridge => boolean(),
    mountpoint := binary() | undefined,
    anonymous => boolean(),
    cert_pem => binary(),
    client_attrs => emqx_types:client_attrs(),
    cn => binary(),
    dn => binary(),
    listener => atom(),
    now_time => non_neg_integer(),
    peername => emqx_types:peername(),
    peerport => inet:port_number(),
    authn => map(),
    trusted_attrs => emqx_clientinfo:trusted_mask()
}.
-type t() :: legacy() | restricted().

-define(PERSIST_KEYS, [
    acl,
    authn,
    anonymous,
    cert_pem,
    client_attrs,
    clientid,
    cn,
    dn,
    is_bridge,
    is_superuser,
    listener,
    mountpoint,
    peerhost,
    peername,
    peerport,
    protocol,
    sockport,
    username,
    zone,
    trusted_attrs
]).

-doc """
Create an authz context map from the client info.
""".
-spec make(emqx_types:clientinfo()) -> t().
make(ClientInfo) ->
    case require_trusted_attributes() of
        false -> ClientInfo;
        true -> emqx_clientinfo:trusted(ClientInfo)
    end.

-doc """
Limit authz context to the fields relevant for persistence
""".
-spec make_persist(emqx_types:clientinfo() | t()) -> t().
make_persist(ClientInfo) ->
    maps:with(?PERSIST_KEYS, ClientInfo).

-spec require_trusted_attributes() -> boolean().
require_trusted_attributes() ->
    Default = emqx_security_profile:policy(authorization_require_trusted_attributes),
    emqx:get_config([authorization, require_trusted_attributes], Default).
