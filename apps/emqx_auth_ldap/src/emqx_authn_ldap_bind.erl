%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_ldap_bind).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("eldap/include/eldap.hrl").

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    emqx_authn_ldap:do_create(?MODULE, Config).

update(Config, State) ->
    emqx_authn_ldap:update(Config, State).

destroy(State) ->
    emqx_authn_ldap:destroy(State).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(
    #{password := _Password} = Credential,
    #{
        query_timeout := Timeout,
        resource_id := ResourceId
    } = _State
) ->
    case
        emqx_resource:simple_sync_query(
            ResourceId,
            {query, Credential, [], Timeout}
        )
    of
        {ok, []} ->
            ignore;
        {ok, [_Entry | _]} ->
            case
                emqx_resource:simple_sync_query(
                    ResourceId,
                    {bind, Credential}
                )
            of
                ok ->
                    {ok, #{is_superuser => false}};
                {error, Reason} ->
                    ?TRACE_AUTHN_PROVIDER(error, "ldap_bind_failed", #{
                        resource => ResourceId,
                        reason => Reason
                    }),
                    {error, bad_username_or_password}
            end;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "ldap_query_failed", #{
                resource => ResourceId,
                timeout => Timeout,
                reason => Reason
            }),
            ignore
    end.
