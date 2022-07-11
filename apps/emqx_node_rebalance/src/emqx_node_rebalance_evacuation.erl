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

-module(emqx_node_rebalance_evacuation).

-include("emqx_node_rebalance.hrl").

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start/1,
         status/0,
         stop/0
        ]).

-export([start_link/0]).

-export([init/1,
         callback_mode/0,
         handle_event/4,
         code_change/4
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-define(EVICT_INTERVAL, 1000).
-define(EVICT_INTERVAL_NO_NODES, 30000).

-type migrate_to() :: [node()] | undefined.

-type start_opts() :: #{server_reference => emqx_eviction_agent:server_reference(),
                        conn_evict_rate => pos_integer(),
                        sess_evict_rate => pos_integer(),
                        wait_takeover => pos_integer(),
                        migrate_to => migrate_to()
                       }.
-type start_error() :: already_started | eviction_agent_busy.
-type stats() :: #{
                   initial_conns := non_neg_integer(),
                   initial_sessions := non_neg_integer(),
                   current_conns := non_neg_integer(),
                   current_sessions := non_neg_integer(),
                   conn_evict_rate := pos_integer(),
                   sess_evict_rate := pos_integer(),
                   server_reference := emqx_eviction_agent:server_reference(),
                   migrate_to := migrate_to()
                  }.
-type status() :: {started, stats()} | stopped.

-spec start(start_opts()) -> ok_or_error(start_error()).
start(StartOpts) ->
    Opts = maps:merge(default_opts(), StartOpts),
    gen_statem:call(?MODULE, {start, Opts}).

-spec stop() -> ok_or_error(not_started).
stop() ->
    gen_statem:call(?MODULE, stop).

-spec status() -> status().
status() ->
    gen_statem:call(?MODULE, status).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

%% states: disabled, evicting_conns, waiting_takeover, evicting_sessions, prohibiting

init([]) ->
    case emqx_node_rebalance_evacuation_persist:read(default_opts()) of
        {ok, #{server_reference := ServerReference} = Opts} ->
            case emqx_eviction_agent:enable(?MODULE, ServerReference) of
                ok ->
                    Data = init_data(#{}, Opts),
                    ok = warn_enabled(),
                    {ok, evicting_conns, Data, [{state_timeout, 0, evict_conns}]};
                {error, eviction_agent_busy} ->
                    emqx_node_rebalance_evacuation_persist:clear(),
                    {ok, disabled, #{}}
            end;
        none ->
            {ok, disabled, #{}}
    end.

%% start
handle_event({call, From},
             {start, #{server_reference := ServerReference} = Opts},
             disabled,
             #{} = Data) ->
    case emqx_eviction_agent:enable(?MODULE, ServerReference) of
        ok ->
            NewData = init_data(Data, Opts),
            ok = emqx_node_rebalance_evacuation_persist:save(Opts),
            {next_state,
             evicting_conns,
             NewData,
             [{state_timeout, 0, evict_conns},
              {reply, From, ok}]};
        {error, eviction_agent_busy} ->
            {keep_state_and_data,
              [{reply, From, {error, eviction_agent_busy}}]}
    end;
handle_event({call, From}, {start, _Opts}, _State, #{}) ->
    {keep_state_and_data,
     [{reply, From, {error, already_started}}]};

%% stop
handle_event({call, From}, stop, disabled, #{}) ->
    {keep_state_and_data,
     [{reply, From, {error, not_started}}]};
handle_event({call, From}, stop, _State, Data) ->
    ok = emqx_node_rebalance_evacuation_persist:clear(),
    _ = emqx_eviction_agent:disable(?MODULE),
    {next_state,
     disabled,
     deinit(Data),
     [{reply, From, ok}]};

%% status
handle_event({call, From}, status, disabled, #{}) ->
    {keep_state_and_data,
     [{reply, From, disabled}]};
handle_event({call, From}, status, State, Data) ->
    Stats = maps:with(
              [initial_conns, current_conns, server_reference, conn_evict_rate],
              Data),
    {keep_state_and_data,
     [{reply, From, {enabled, Stats#{status => State}}}]};

%% conn eviction
handle_event(state_timeout,
             evict_conns,
             evicting_conns,
             #{conn_evict_rate := ConnEvictRate,
               wait_takeover := WaitTakeover} = Data) ->
    case emqx_eviction_agent:status() of
        {enabled, #{connections := Conns}} when Conns > 0 ->
            ok = emqx_eviction_agent:evict_connections(ConnEvictRate),
            ?tp(debug, node_evacuation_evict_conn, #{conn_evict_rate => ConnEvictRate}),
            NewData = Data#{current_conns => Conns},
            {keep_state,
             NewData,
             [{state_timeout, ?EVICT_INTERVAL, evict_conns}]};
        {enabled, #{connections := 0}} ->
            NewData = Data#{current_conns => 0},
            {next_state,
             waiting_takeover,
             NewData,
             [{state_timeout, timer:seconds(WaitTakeover), evict_sessions}]}
    end;

handle_event(state_timeout,
             evict_sessions,
             waiting_takeover,
             Data) ->
            {next_state,
             evicting_sessions,
             Data,
             [{state_timeout, 0, evict_sessions}]};

%% session eviction
handle_event(state_timeout,
             evict_sessions,
             evicting_sessions,
             #{sess_evict_rate := SessEvictRate,
               migrate_to := MigrateTo,
               current_sessions := CurrSessCount} = Data) ->
    case emqx_eviction_agent:status() of
        {enabled, #{sessions := SessCount}} when SessCount > 0 ->
            case migrate_to(MigrateTo) of
                [] ->
                    ?LOG(warning,
                         "No nodes are available to evacuate sessions, session_count=~p",
                         [CurrSessCount]),
                    {keep_state_and_data,
                     [{state_timeout, ?EVICT_INTERVAL_NO_NODES, evict_sessions}]};
                Nodes ->
                    ok = emqx_eviction_agent:evict_sessions(Nodes, SessEvictRate),
                    ?tp(debug, node_evacuation_evict_sess, #{sess_evict_rate => SessEvictRate}),
                    NewData = Data#{current_sessions => SessCount},
                    {keep_state,
                     NewData,
                     [{state_timeout, ?EVICT_INTERVAL, evict_sessions}]}
            end;
        {enabled, #{sessions := 0}} ->
            NewData = Data#{current_sessions => 0},
            {next_state,
             prohibiting,
             NewData}
    end;

handle_event(info, Msg, State, Data) ->
    ?LOG(warning, "Unknown Msg: ~p, State: ~p, Data: ~p", [Msg, State, Data]),
    keep_state_and_data;

handle_event(cast, Msg, State, Data) ->
    ?LOG(warning, "Unknown cast Msg: ~p, State: ~p, Data: ~p", [Msg, State, Data]),
    keep_state_and_data.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%--------------------------------------------------------------------
%% internal funs
%%--------------------------------------------------------------------

default_opts() ->
    #{
      server_reference => undefined,
      conn_evict_rate => ?DEFAULT_CONN_EVICT_RATE,
      sess_evict_rate => ?DEFAULT_SESS_EVICT_RATE,
      wait_takeover => ?DEFAULT_WAIT_TAKEOVER,
      migrate_to => undefined
     }.

init_data(Data0, Opts) ->
    Data1 = maps:merge(Data0, Opts),
    {enabled, #{connections := ConnCount, sessions := SessCount}} = emqx_eviction_agent:status(),
    Data1#{
      initial_conns => ConnCount,
      current_conns => ConnCount,
      initial_sessions => SessCount,
      current_sessions => SessCount
     }.

deinit(Data) ->
    Keys = [initial_conns, current_conns, initial_sessions, current_sessions]
           ++ maps:keys(default_opts()),
    maps:without(Keys, Data).

warn_enabled() ->
    Msg = "Node evacuation is enabled. The node will not receive connections.",
    ?LOG(warning, Msg),
    io:format(standard_error, "~s~n", [Msg]).

migrate_to(undefined) ->
    migrate_to(all_nodes());
migrate_to(Nodes) when is_list(Nodes) ->
    lists:filter(fun is_node_available/1, Nodes).

is_node_available(Node) ->
    net_adm:ping(Node) =:= pong.

all_nodes() ->
    ekka_mnesia:cluster_nodes(all) -- [node()].
