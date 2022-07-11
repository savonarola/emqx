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

-module(emqx_node_rebalance_cli).

%% APIs
-export([ load/0
        , unload/0
        , cli/1
        ]).

load() ->
    emqx_ctl:register_command(rebalance, {?MODULE, cli}, []).

unload() ->
    emqx_ctl:unregister_command(rebalance).

cli(["start" | StartArgs]) ->
    case start_args(StartArgs) of
        %% only evacuation is supported now
        {evacuation, Opts} ->
            case emqx_node_rebalance_evacuation:status() of
                disabled ->
                    ok = emqx_node_rebalance_evacuation:start(Opts),
                    emqx_ctl:print("Rebalance(evacuation) started~n"),
                    true;
                {enabled, _} ->
                    emqx_ctl:print("Rebalance is already enabled~n"),
                    false
            end;
        {error, Error} ->
            emqx_ctl:print("Rebalance start error: ~s~n", [Error]),
            false
    end;

cli(["status"]) ->
    case emqx_node_rebalance_evacuation:status() of
        disabled ->
            emqx_ctl:print("Rebalance status: disabled~n");
        {enabled, Stats} ->
            emqx_ctl:print("Rebalance status: evacuation~n"
                           "current_connected: ~B~n"
                           "initial_connected: ~B~n",
                           [maps:get(current_conns, Stats),
                            maps:get(initial_conns, Stats)])
    end;

cli(["stop"]) ->
    case emqx_node_rebalance_evacuation:status() of
        {enabled, _} ->
            ok = emqx_node_rebalance_evacuation:stop(),
            emqx_ctl:print("Rebalance(evacuation) stopped~n");
        disabled ->
            emqx_ctl:print("Rebalance is already disabled~n")
    end;

cli(_) ->
    emqx_ctl:usage(
      [{"rebalance start --evacuation \\\n"
        "    [--redirect-to \"Host1:Port1 Host2:Port2 ...\"] \\\n"
        "    [--conn-evict-rate ConnPerSec] \\\n"
        "    [--migrate-to \"node1@host1 node2@host2\"] \\\n"
        "    [--wait-takeover 60] \\\n"
        "    [--sess-evict-rate 50]",
        "Start current node evacuation with optional server redirect to the specified servers"},

       {"rebalance status",
        "Get current node rebalance status"},

       {"rebalance stop",
        "Stop node rebalance"}]).

start_args(Args) ->
    case collect_args(Args, #{}) of
        {ok, #{"--evacuation" := true} = Collected} ->
            case validate_evacuation(maps:to_list(Collected), #{}) of
                {ok, Validated} ->
                    {evacuation, Validated};
                {error, _} = Error -> Error
            end;
        {ok, #{}} ->
            {error, "only --evacuation mode is available"};
        {error, _} = Error -> Error
    end.

collect_args([], Map) -> {ok, Map};
collect_args(["--evacuation" | Args], Map) ->
    collect_args(Args, Map#{"--evacuation" => true});
collect_args(["--redirect-to", ServerReference | Args], Map) ->
    collect_args(Args, Map#{"--redirect-to" => ServerReference});
collect_args(["--conn-evict-rate", ConnEvictRate | Args], Map) ->
    collect_args(Args, Map#{"--conn-evict-rate" => ConnEvictRate});
collect_args(["--migrate-to", MigrateTo | Args], Map) ->
    collect_args(Args, Map#{"--migrate-to" => MigrateTo});
collect_args(["--wait-takeover", WaitTakeover | Args], Map) ->
    collect_args(Args, Map#{"--wait-takeover" => WaitTakeover});
collect_args(["--sess-evict-rate", SessEvictRate | Args], Map) ->
    collect_args(Args, Map#{"--sess-evict-rate" => SessEvictRate});
collect_args(Args, _Map) ->
    {error, io_lib:format("unknown arguments: ~p", [Args])}.

validate_evacuation([], Map) ->
    {ok, Map};
validate_evacuation([{"--evacuation", _} | Rest], Map) ->
    validate_evacuation(Rest, Map);
validate_evacuation([{"--redirect-to", ServerReference} | Rest], Map) ->
    validate_evacuation(Rest, Map#{server_reference => list_to_binary(ServerReference)});
validate_evacuation([{"--conn-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(conn_evict_rate, Opts, Map);
validate_evacuation([{"--sess-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(sess_evict_rate, Opts, Map);
validate_evacuation([{"--wait-takeover", _} | _] = Opts, Map) ->
    validate_pos_int(wait_takeover, Opts, Map);
validate_evacuation([{"--migrate-to", MigrateTo} | Rest], Map) ->
    Nodes = lists:map(fun list_to_atom/1, string:tokens(MigrateTo, ", ")),
    case lists:partition(fun is_node_available/1, Nodes) of
        {[], []} ->
            {error, "invalid --migrate-to, no nodes"};
        {Nodes, []} ->
            validate_evacuation(Rest, Map#{migrate_to => Nodes});
        {_Nodes, UnavailNodes} ->
            {error, io_lib:format("invalid --migrate-to, unavailable nodes: ~p", [UnavailNodes])}
    end;
validate_evacuation(Rest, _Map) ->
    {error, io_lib:format("unknown evacuation arguments: ~p", [Rest])}.

is_node_available(Node) ->
    net_adm:ping(Node) =:= pong.

validate_pos_int(Name, [{OptionName, Value} | Rest], Map) ->
    case string:to_integer(Value) of
        {Int, ""} when Int > 0 ->
           validate_evacuation(Rest, Map#{Name => Int});
        _ ->
            {error, "invalid " ++ OptionName ++ " value"}
    end.
