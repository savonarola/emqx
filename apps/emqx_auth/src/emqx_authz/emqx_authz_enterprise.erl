%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_authz_enterprise).

-export([
    type_names/0,
    fields/1,
    is_enterprise_module/1,
    authz_sources_types/0,
    register_sources/0,
    desc/1
]).

-if(?EMQX_RELEASE_EDITION == ee).

%% type name set
type_names() ->
    [ldap].

%% type -> type schema
fields(ldap) ->
    emqx_ldap_authz:fields(config).

%% type -> type module
is_enterprise_module(ldap) ->
    {ok, emqx_ldap_authz};
is_enterprise_module(_) ->
    false.

%% api sources set
authz_sources_types() ->
    [ldap].

register_sources() ->
    ok = emqx_authz:register_source(ldap, emqx_ldap_authz).

desc(ldap) ->
    emqx_ldap_authz:description();
desc(_) ->
    undefined.

-else.

-dialyzer({nowarn_function, [fields/1, desc/1]}).

type_names() ->
    [].

fields(Any) ->
    error({invalid_field, Any}).

is_enterprise_module(_) ->
    false.

authz_sources_types() ->
    [].

register_sources() ->
    ok.

desc(_) ->
    undefined.
-endif.
