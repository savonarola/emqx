%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_jwt_cache).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-logger_header("[JWT-CACHE]").

%% APIs
-export([start_link/1]).

-export([store/2]).
-export([fetch/1]).
-export([delete/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(TAB, ?MODULE).

%% 5 min
-define(DEFAULT_CLEANUP_INTERVAL, 300000).

-record(state, {tref, intv}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Options) ->
    gen_server:start_link(?MODULE, [Options], []).

store(ClientId, #{} = Claims) when is_binary(ClientId) ->
    true = ets:insert(?TAB, {ClientId, Claims}),
    ok.

fetch(ClientId) when is_binary(ClientId) ->
    Now = erlang:system_time(second),
    case ets:lookup(?TAB, ClientId) of
        [{_, #{<<"exp">> := Exp} = Claims}] 
          when is_integer(Exp) andalso Exp >= Now ->
            Claims;
        _ ->
            #{}
    end.

delete(ClientId) when is_binary(ClientId) ->
    true = ets:delete(?TAB, ClientId),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Options]) ->
    Interval = proplists:get_value(cleanup_interval, Options, ?DEFAULT_CLEANUP_INTERVAL),
    _ = ets:new(?TAB, [set, public, named_table]),
    {ok, reset_timer(
           #state{intv = Interval})}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(cleanup, State) ->
    true = cleanup(), 
    {noreply, reset_timer(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = cancel_timer(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

cleanup() ->
    Deadline = erlang:system_time(second),
    Ms = ets:fun2ms(
          fun({Key, #{<<"exp">> := Ts}}) when Ts < Deadline -> Key end),
    [{Pattern, _, _}] = Ms,
    ets:match_delete(?TAB, Pattern).

reset_timer(State = #state{intv = Intv}) ->
    State#state{tref = erlang:send_after(Intv, self(), cleanup)}.

cancel_timer(State = #state{tref = undefined}) ->
    State;
cancel_timer(State = #state{tref = TRef}) ->
    _ = erlang:cancel_timer(TRef),
    State#state{tref = undefined}.

