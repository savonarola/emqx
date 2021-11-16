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
-module(emqx_connector_mongo).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-type server() :: emqx_schema:ip_port().
-reflect_type([server/0]).
-typerefl_from_string({server/0, emqx_connector_schema_lib, to_ip_port}).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        , on_jsonify/1
        ]).

-export([connect/1]).

-export([roots/0, fields/1]).

-export([mongo_query/5]).
%%=====================================================================
roots() ->
    [ {config, #{type => hoconsc:union(
                          [ hoconsc:ref(?MODULE, single)
                          , hoconsc:ref(?MODULE, rs)
                          , hoconsc:ref(?MODULE, sharded)
                          ])}}
    ].

fields(single) ->
    [ {mongo_type, #{type => single,
                     default => single}}
    , {server, fun server/1}
    ] ++ mongo_fields();
fields(rs) ->
    [ {mongo_type, #{type => rs,
                     default => rs}}
    , {servers, fun servers/1}
    , {replica_set_name, fun emqx_connector_schema_lib:database/1}
    ] ++ mongo_fields();
fields(sharded) ->
    [ {mongo_type, #{type => sharded,
                     default => sharded}}
    , {servers, fun servers/1}
    ] ++ mongo_fields();
fields(topology) ->
    [ {pool_size, fun emqx_connector_schema_lib:pool_size/1}
    , {max_overflow, fun emqx_connector_schema_lib:pool_size/1}
    , {overflow_ttl, fun duration/1}
    , {overflow_check_period, fun duration/1}
    , {local_threshold_ms, fun duration/1}
    , {connect_timeout_ms, fun duration/1}
    , {socket_timeout_ms, fun duration/1}
    , {server_selection_timeout_ms, fun duration/1}
    , {wait_queue_timeout_ms, fun duration/1}
    , {heartbeat_frequency_ms, fun duration/1}
    , {min_heartbeat_frequency_ms, fun duration/1}
    ].

mongo_fields() ->
    [ {pool_size, fun emqx_connector_schema_lib:pool_size/1}
    , {username, fun emqx_connector_schema_lib:username/1}
    , {password, fun emqx_connector_schema_lib:password/1}
    , {auth_source, #{type => binary(),
                      nullable => true}}
    , {database, fun emqx_connector_schema_lib:database/1}
    , {topology, #{type => hoconsc:ref(?MODULE, topology),
                   nullable => true}}
    ] ++
    emqx_connector_schema_lib:ssl_fields().

on_jsonify(Config) ->
    Config.

%% ===================================================================
on_start(InstId, Config = #{server := Server,
                            mongo_type := single}) ->
    ?SLOG(info, #{msg => "starting mongodb single connector",
                  connector => InstId, config => Config}),
    Opts = [{type, single},
            {hosts, [emqx_connector_schema_lib:ip_port_to_string(Server)]}
            ],
    do_start(InstId, Opts, Config);

on_start(InstId, Config = #{servers := Servers,
                            mongo_type := rs,
                            replica_set_name := RsName}) ->
    ?SLOG(info, #{msg => "starting mongodb rs connector",
                  connector => InstId, config => Config}),
    Opts = [{type,  {rs, RsName}},
            {hosts, [emqx_connector_schema_lib:ip_port_to_string(S)
                     || S <- Servers]}
           ],
    do_start(InstId, Opts, Config);

on_start(InstId, Config = #{servers := Servers,
                            mongo_type := sharded}) ->
    ?SLOG(info, #{msg => "starting mongodb sharded connector",
                  connector => InstId, config => Config}),
    Opts = [{type, sharded},
            {hosts, [emqx_connector_schema_lib:ip_port_to_string(S)
                     || S <- Servers]}
            ],
    do_start(InstId, Opts, Config).

on_stop(InstId, #{poolname := PoolName}) ->
    ?SLOG(info, #{msg => "stopping mongodb connector",
                  connector => InstId}),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {Action, Collection, Selector, Docs}, AfterQuery, #{poolname := PoolName} = State) ->
    Request = {Action, Collection, Selector, Docs},
    ?SLOG(debug, #{msg => "mongodb connector received request",
        request => Request, connector => InstId,
        state => State}),
    case ecpool:pick_and_do(PoolName, {?MODULE, mongo_query, [Action, Collection, Selector, Docs]}, no_handover) of
        {error, Reason} ->
            ?SLOG(error, #{msg => "mongodb connector do query failed",
                request => Request, reason => Reason,
                connector => InstId}),
            emqx_resource:query_failed(AfterQuery),
            {error, Reason};
        {ok, Cursor} when is_pid(Cursor) ->
            emqx_resource:query_success(AfterQuery),
            mc_cursor:foldl(fun(O, Acc2) -> [O|Acc2] end, [], Cursor, 1000);
        Result ->
            emqx_resource:query_success(AfterQuery),
            Result
    end.

-dialyzer({nowarn_function, [on_health_check/2]}).
on_health_check(_InstId, #{poolname := PoolName} = State) ->
    case health_check(PoolName) of
        true -> {ok, State};
        false -> {error, health_check_failed, State}
    end.

health_check(PoolName) ->
    Status = [begin
        case ecpool_worker:client(Worker) of
            {ok, Conn} ->
                %% we don't care if this returns something or not, we just to test the connection
                Res = mongo_api:find_one(Conn, <<"foo">>, {}, #{}),
                Res == undefined orelse is_map(Res);
            _ -> false
        end
    end || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    length(Status) > 0 andalso lists:all(fun(St) -> St =:= true end, Status).

%% ===================================================================
connect(Opts) ->
    Type = proplists:get_value(mongo_type, Opts, single),
    Hosts = proplists:get_value(hosts, Opts, []),
    Options = proplists:get_value(options, Opts, []),
    WorkerOptions = proplists:get_value(worker_options, Opts, []),
    mongo_api:connect(Type, Hosts, Options, WorkerOptions).

mongo_query(Conn, find, Collection, Selector, Projector) ->
    mongo_api:find(Conn, Collection, Selector, Projector);

mongo_query(Conn, find_one, Collection, Selector, Projector) ->
    mongo_api:find_one(Conn, Collection, Selector, Projector);

%% Todo xxx
mongo_query(_Conn, _Action, _Collection, _Selector, _Projector) ->
    ok.

do_start(InstId, Opts0, Config = #{mongo_type := Type,
                                   database := Database,
                                   pool_size := PoolSize,
                                   ssl := SSL}) ->
    SslOpts = case maps:get(enable, SSL) of
                  true ->
                      [{ssl, true},
                       {ssl_opts, emqx_plugin_libs_ssl:save_files_return_opts(SSL, "connectors", InstId)}
                      ];
                  false -> [{ssl, false}]
              end,
    Topology= maps:get(topology, Config, #{}),
    Opts = Opts0 ++
           [{pool_size, PoolSize},
            {options, init_topology_options(maps:to_list(Topology), [])},
            {worker_options, init_worker_options(maps:to_list(Config), SslOpts)}],
    %% test the connection
    TestOpts = case maps:is_key(server, Config) of
                  true ->
                    Server = maps:get(server, Config),
                    host_port(Server);
                  false ->
                    Servers = maps:get(servers, Config),
                    host_port(erlang:hd(Servers))
              end ++ [{database, Database}],
    {ok, TestConn} = mc_worker_api:connect(TestOpts),

    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    _ = emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Opts ++ SslOpts),
    {ok, #{poolname => PoolName,
           type => Type,
           test_conn => TestConn,
           test_opts => TestOpts}}.

init_topology_options([{pool_size, Val}| R], Acc) ->
    init_topology_options(R, [{pool_size, Val}| Acc]);
init_topology_options([{max_overflow, Val}| R], Acc) ->
    init_topology_options(R, [{max_overflow, Val}| Acc]);
init_topology_options([{overflow_ttl, Val}| R], Acc) ->
    init_topology_options(R, [{overflow_ttl, Val}| Acc]);
init_topology_options([{overflow_check_period, Val}| R], Acc) ->
    init_topology_options(R, [{overflow_check_period, Val}| Acc]);
init_topology_options([{local_threshold_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'localThresholdMS', Val}| Acc]);
init_topology_options([{connect_timeout_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'connectTimeoutMS', Val}| Acc]);
init_topology_options([{socket_timeout_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'socketTimeoutMS', Val}| Acc]);
init_topology_options([{server_selection_timeout_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'serverSelectionTimeoutMS', Val}| Acc]);
init_topology_options([{wait_queue_timeout_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'waitQueueTimeoutMS', Val}| Acc]);
init_topology_options([{heartbeat_frequency_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'heartbeatFrequencyMS', Val}| Acc]);
init_topology_options([{min_heartbeat_frequency_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'minHeartbeatFrequencyMS', Val}| Acc]);
init_topology_options([_| R], Acc) ->
    init_topology_options(R, Acc);
init_topology_options([], Acc) ->
    Acc.

init_worker_options([{database, V} | R], Acc) ->
    init_worker_options(R, [{database, V} | Acc]);
init_worker_options([{auth_source, V} | R], Acc) ->
    init_worker_options(R, [{auth_source, V} | Acc]);
init_worker_options([{username, V} | R], Acc) ->
    init_worker_options(R, [{login, V} | Acc]);
init_worker_options([{password, V} | R], Acc) ->
    init_worker_options(R, [{password, V} | Acc]);
init_worker_options([{w_mode, V} | R], Acc) ->
    init_worker_options(R, [{w_mode, V} | Acc]);
init_worker_options([{r_mode, V} | R], Acc) ->
    init_worker_options(R, [{r_mode, V} | Acc]);
init_worker_options([_ | R], Acc) ->
    init_worker_options(R, Acc);
init_worker_options([], Acc) -> Acc.

host_port({Host, Port}) ->
    [{host, Host}, {port, Port}].

server(type) -> server();
server(validator) -> [?NOT_EMPTY("the value of the field 'server' cannot be empty")];
server(_) -> undefined.

servers(type) -> hoconsc:array(server());
servers(validator) -> [?NOT_EMPTY("the value of the field 'servers' cannot be empty")];
servers(_) -> undefined.

duration(type) -> emqx_schema:duration_ms();
duration(nullable) -> true;
duration(_) -> undefined.
