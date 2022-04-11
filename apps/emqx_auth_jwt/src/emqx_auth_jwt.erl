%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_auth_jwt).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[JWT]").

-export([ register_metrics/0
        , check_auth/3
        , check_acl/5
        , client_disconnected/3
        , description/0
        ]).

-record(auth_metrics, {
        success = 'client.auth.success',
        failure = 'client.auth.failure',
        ignore = 'client.auth.ignore'
    }).

-define(METRICS(Type), tl(tuple_to_list(#Type{}))).
-define(METRICS(Type, K), #Type{}#Type.K).

-define(AUTH_METRICS, ?METRICS(auth_metrics)).
-define(AUTH_METRICS(K), ?METRICS(auth_metrics, K)).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTH_METRICS).

%%--------------------------------------------------------------------
%% Authentication callbacks
%%--------------------------------------------------------------------

check_auth(#{clientid := ClientId} = ClientInfo,
      AuthResult,
      #{from := From, checklists := Checklists}) ->
    case maps:find(From, ClientInfo) of
        error ->
            ok = emqx_metrics:inc(?AUTH_METRICS(ignore));
        {ok, undefined} ->
            ok = emqx_metrics:inc(?AUTH_METRICS(ignore));
        {ok, Token} ->
            case emqx_auth_jwt_svr:verify(Token) of
                {error, not_found} ->
                    ok = emqx_metrics:inc(?AUTH_METRICS(ignore));
                {error, not_token} ->
                    ok = emqx_metrics:inc(?AUTH_METRICS(ignore));
                {error, Reason} ->
                    ok = emqx_metrics:inc(?AUTH_METRICS(failure)),
                    {stop, AuthResult#{auth_result => Reason, anonymous => false}};
                {ok, Claims} ->
                    ok = emqx_auth_jwt_cache:store(ClientId, Claims),
                    {stop, maps:merge(AuthResult, verify_claims(Checklists, Claims, ClientInfo))}
            end
    end.

client_disconnected(#{clientid := ClientId}, _Reason, _NConnInfo) ->
    emqx_auth_jwt_cache:delete(ClientId).

check_acl(ClientInfo = #{clientid := ClientId},
          PubSub,
          Topic,
          _NoMatchAction,
          #{acl_claim_name := AclClaimName}) ->
    case emqx_auth_jwt_cache:fetch(ClientId) of
        #{AclClaimName := Acl} ->
            verify_acl(ClientInfo, Acl, PubSub, Topic);
        _ -> ok
    end.

description() -> "Authentication with JWT".

%%------------------------------------------------------------------------------
%% Verify Claims
%%--------------------------------------------------------------------

verify_acl(ClientInfo, #{<<"sub">> := SubTopics}, subscribe, Topic) ->
    verify_acl(ClientInfo, SubTopics, Topic);
verify_acl(ClientInfo, #{<<"pub">> := PubTopics}, publish, Topic) ->
    verify_acl(ClientInfo, PubTopics, Topic);
verify_acl(_ClientInfo, _Acl, _PubSub, _Topic) -> ok.

verify_acl(_ClientInfo, [], _Topic) -> {stop, deny};
verify_acl(ClientInfo, [AclTopic | AclTopics], Topic) ->
    case emqx_topic:match(Topic, emqx_access_rule:feed_var(ClientInfo, AclTopic)) of
        true -> {stop, allow};
        false -> verify_acl(ClientInfo, AclTopics, Topic)
    end.

verify_claims(Checklists, Claims, ClientInfo) ->
    case do_verify_claims(feedvar(Checklists, ClientInfo), Claims) of
        {error, Reason} ->
            ok = emqx_metrics:inc(?AUTH_METRICS(failure)),
            #{auth_result => Reason, anonymous => false};
        ok ->
            ok = emqx_metrics:inc(?AUTH_METRICS(success)),
            #{auth_result => success, anonymous => false, jwt_claims => Claims}
    end.

do_verify_claims([], _Claims) ->
    ok;
do_verify_claims([{Key, Expected} | L], Claims) ->
    case maps:get(Key, Claims, undefined) =:= Expected of
        true -> do_verify_claims(L, Claims);
        false -> {error, {verify_claim_failed, Key}}
    end.

feedvar(Checklists, #{username := Username, clientid := ClientId}) ->
    lists:map(fun({K, <<"%u">>}) -> {K, Username};
                 ({K, <<"%c">>}) -> {K, ClientId};
                 ({K, Expected}) -> {K, Expected}
              end, Checklists).

