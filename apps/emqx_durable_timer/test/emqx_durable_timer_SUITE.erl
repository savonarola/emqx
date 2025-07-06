%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../src/internals.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% This testcase verifies lazy initialization sequence of the
%% application. It is expected that the application is started without
%% creating any durable databases until the first timer type is
%% registered.
%%
%% Additionally, this testcase doesn't create any timer data, so it's
%% used to verify that the logic handles lack of any timers.
t_lazy_initialization(Config) ->
    Env = [{heartbeat_interval, 1_000}],
    Cluster = cluster(?FUNCTION_NAME, Config, 1, Env),
    ?check_trace(
        #{timetrap => 15_000},
        try
            [Node] = emqx_cth_cluster:start(Cluster),
            %% Application is started but dormant. Databases don't exist yet:
            ?ON(
                Node,
                begin
                    {DBs, _} = lists:unzip(emqx_ds:which_dbs()),
                    ?defer_assert(?assertNot(lists:member(?DB_GLOB, DBs)))
                end
            ),
            %%
            %% Now let's register a timer handler. This should
            %% activate the application. Expect: node adds its
            %% metadata to the tables.
            %%
            ?ON(
                Node,
                ?assertMatch(ok, emqx_durable_test_timer:init())
            ),
            %% Verify that the node has opened the DB:
            ?ON(
                Node,
                begin
                    {DBs, _} = lists:unzip(emqx_ds:which_dbs()),
                    ?defer_assert(?assert(lists:member(?DB_GLOB, DBs)))
                end
            ),
            %%
            %% The node should create a new epoch:
            %%
            wait_heartbeat(Node),
            [{Epoch1, Epoch1Lifetime}] = ?ON(
                Node, maps:to_list(emqx_durable_timer:ls_epochs(Node))
            ),
            ?assertMatch({T, undefined} when is_integer(T), Epoch1Lifetime),
            %% Also verify ls_epochs/0 API:
            NodesEpochs1 = ?ON(Node, emqx_durable_timer:ls_epochs()),
            [{Node, #{Epoch1 := Epoch1Lifetime}}] = maps:to_list(NodesEpochs1),
            %% Verify that heartbeat is periodically incrementing:
            {ok, T1_1} = ?ON(Node, emqx_durable_timer:last_heartbeat(Epoch1)),
            wait_heartbeat(Node),
            {ok, T1_2} = ?ON(Node, emqx_durable_timer:last_heartbeat(Epoch1)),
            ?assert(is_integer(T1_1), T1_1),
            ?assert(is_integer(T1_2) andalso T1_2 > T1_1, T1_2),
            %%
            %% Restart the node. Expect that node will create a new
            %% epoch and close the old one.
            %%
            ?tp(test_restart_cluster, #{}),
            emqx_cth_cluster:restart(Cluster),
            ?ON(
                Node,
                ?assertMatch(ok, emqx_durable_test_timer:init())
            ),
            wait_heartbeat(Node),
            %% Restart complete.
            NodeEpochs2 = ?ON(Node, emqx_durable_timer:ls_epochs(Node)),
            %% Verify that the new epoch has been created:
            [{Epoch2, {_, undefined}}] = maps:to_list(maps:without([Epoch1], NodeEpochs2)),
            %% Verify that the old epoch has been marked as closed:
            ?assertMatch(
                #{Epoch1 := {Begin, End}} when is_integer(Begin) andalso is_integer(End),
                NodeEpochs2
            ),
            %% Verify that heartbeat for Epoch2 is ticking:
            {ok, T1_3} = ?ON(Node, emqx_durable_timer:last_heartbeat(Epoch1)),
            {ok, T2_1} = ?ON(Node, emqx_durable_timer:last_heartbeat(Epoch2)),
            wait_heartbeat(Node),
            {ok, T2_2} = ?ON(Node, emqx_durable_timer:last_heartbeat(Epoch2)),
            %% Heartbeat for epoch1 stays the same:
            ?assertEqual({ok, T1_3}, ?ON(Node, emqx_durable_timer:last_heartbeat(Epoch1))),
            %% Epoch2 is ticking:
            ?assert(is_integer(T2_2) andalso T2_2 > T2_1, T2_2)
        after
            emqx_cth_cluster:stop(Cluster)
        end,
        [
            fun no_unexpected/1,
            fun verify_timers/1,
            fun verify_activation/1
        ]
    ).

%% This testcase verifies normal operation of the timers when the
%% owner node is not restarted.
t_normal_execution(Config) ->
    Cluster = cluster(?FUNCTION_NAME, Config, 1, []),
    ?check_trace(
        #{timetrap => 30_000},
        try
            [Node] = emqx_cth_cluster:start(Cluster),
            ?assertMatch(ok, ?ON(Node, emqx_durable_test_timer:init())),
            %%
            %% Test a timer with 0 delay:
            ?wait_async_action(
                ?assertMatch(
                    ok,
                    ?ON(
                        Node,
                        emqx_durable_test_timer:apply_after(<<>>, <<>>, 0)
                    )
                ),
                #{?snk_kind := ?tp_fire, key := <<>>, val := <<>>},
                infinity
            ),
            %%
            %% Test a timer with 1s delay:
            ?wait_async_action(
                ?assertMatch(
                    ok,
                    ?ON(
                        Node,
                        emqx_durable_test_timer:apply_after(<<1>>, <<1>>, 1_000)
                    )
                ),
                #{?snk_kind := ?tp_fire, key := <<1>>, val := <<1>>},
                infinity
            ),
            %%
            %% Test overriding of timers:
            ?wait_async_action(
                ?ON(
                    Node,
                    begin
                        %% 1. Start a timer with key <<2>>
                        ok = emqx_durable_test_timer:apply_after(<<2>>, <<1>>, 500),
                        %% 2. Then override delay and value:
                        ok = emqx_durable_test_timer:apply_after(<<2>>, <<2>>, 1_000)
                    end
                ),
                #{?snk_kind := ?tp_fire, key := <<2>>, val := <<2>>},
                infinity
            ),
            %%
            %% Test overriding of dead hand:
            ?wait_async_action(
                ?ON(
                    Node,
                    begin
                        %% Derive topic name:
                        DHT = emqx_durable_timer:dead_hand_topic(
                            emqx_durable_test_timer:durable_timer_type(),
                            emqx_durable_timer:epoch(),
                            '+'
                        ),
                        %% 1. Set up a dead hand timer with key <<3>>
                        ok = emqx_durable_test_timer:dead_hand(<<3>>, <<1>>, 0),
                        ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, DHT)),
                        %% 2. Override it with a regular timer:
                        ok = emqx_durable_test_timer:apply_after(<<3>>, <<2>>, 100),
                        %% First version of the timer is gone:
                        ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, DHT))
                    end
                ),
                #{?snk_kind := ?tp_fire, key := <<3>>, val := <<2>>},
                infinity
            ),
            ok
        after
            emqx_cth_cluster:stop(Cluster)
        end,
        [fun no_unexpected/1, fun verify_timers/1]
    ).

%% This testcase verifies the functionality related to timer
%% cancellation.
t_cancellation(Config) ->
    Cluster = cluster(?FUNCTION_NAME, Config, 1, []),
    ?check_trace(
        #{timetrap => 30_000},
        try
            [Node] = emqx_cth_cluster:start(Cluster),
            ?assertMatch(ok, ?ON(Node, emqx_durable_test_timer:init())),
            %%
            %% 1. Test dead hand functionality. It's fairly trivial as
            %% long as the node doesn't restart: we just need to
            %% verify that the record is deleted.
            ?ON(
                Node,
                begin
                    DHT = emqx_durable_timer:dead_hand_topic(
                        emqx_durable_test_timer:durable_timer_type(),
                        emqx_durable_timer:epoch(),
                        '+'
                    ),
                    emqx_durable_test_timer:dead_hand(<<0>>, <<0>>, 5_000),
                    ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, DHT)),
                    %% Cancel it and verify that the record is deleted:
                    emqx_durable_test_timer:cancel(<<0>>),
                    ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, DHT))
                end
            ),
            %% 2. Test apply_after cancellation.
            Delay = 2_000,
            AAKey = <<1>>,
            ?ON(
                Node,
                begin
                    AAT = emqx_durable_timer:started_topic(
                        emqx_durable_test_timer:durable_timer_type(),
                        emqx_durable_timer:epoch(),
                        '+'
                    ),
                    emqx_durable_test_timer:apply_after(AAKey, <<1>>, Delay),
                    ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, AAT)),
                    %% Cancel it and verify that the record is deleted:
                    emqx_durable_test_timer:cancel(AAKey),
                    ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, AAT))
                end
            ),
            ct:sleep(2 * Delay)
        after
            emqx_cth_cluster:stop(Cluster)
        end,
        [
            fun no_unexpected/1,
            fun verify_timers/1,
            fun(Trace) ->
                ?assertMatch([], ?of_kind(?tp_fire, Trace))
            end
        ]
    ).

%%------------------------------------------------------------------------------
%% Trace specs
%%------------------------------------------------------------------------------

verify_activation(Trace) ->
    ?strict_causality(
        #{?snk_kind := ?tp_test_register, ?snk_meta := #{node := N}},
        #{?snk_kind := ?tp_state_change, to := ?s_isolated, ?snk_meta := #{node := N}},
        Trace
    ).

no_unexpected(Trace) ->
    ?assertMatch([], ?of_kind(?tp_unknown_event, Trace)).

%% Active Timer State:
-record(ats, {
    val :: binary(),
    min_ts :: integer()
}).

%% Verifier State:
-record(vs, {
    active = #{},
    dead_hand = #{},
    errors = []
}).

verify_timers(Trace) ->
    do_verify_timers(Trace, #vs{}).

do_verify_timers([], #vs{errors = Errors}) ->
    ?assertMatch([], Errors);
do_verify_timers(
    [TP = #{?snk_kind := Kind, ?snk_meta := #{time := TimeUs}} | Trace], S0
) ->
    TS = erlang:convert_time_unit(TimeUs, microsecond, millisecond),
    #vs{active = A0, dead_hand = DH0, errors = Err0} = S0,
    S =
        case Kind of
            ?tp_new_apply_after ->
                #{type := T, key := K, val := Val, delay := Delay} = TP,
                S0#vs{
                    active = A0#{{T, K} => #ats{val = Val, min_ts = Delay + TS}},
                    dead_hand = maps:remove(K, DH0)
                };
            ?tp_new_dead_hand ->
                #{type := T, key := K, val := ValGot, delay := Delay} = TP,
                S0#vs{
                    active = maps:remove({T, K}, A0),
                    dead_hand = DH0#{{T, K} => {ValGot, Delay}}
                };
            ?tp_delete ->
                #{type := T, key := K} = TP,
                S0#vs{
                    active = maps:remove({T, K}, A0),
                    dead_hand = maps:remove({T, K}, DH0)
                };
            ?tp_fire ->
                #{type := T, key := K, val := ValGot} = TP,
                case maps:take({T, K}, A0) of
                    {#ats{val = ValExpected, min_ts = MinTS}, A} ->
                        Err =
                            case ValExpected of
                                ValGot when MinTS < TS ->
                                    [];
                                ValGot ->
                                    [{fired_too_early, TP}];
                                _ ->
                                    [{value_mismath, #{expected => ValExpected, got => ValGot}}]
                            end,
                        S0#vs{
                            active = A,
                            errors = Err ++ Err0
                        };
                    error ->
                        S0#vs{
                            errors = [{unexpected, TP} | Err0]
                        }
                end;
            _ ->
                S0
        end,
    do_verify_timers(
        Trace,
        S
    ).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

suite() -> [{timetrap, {minutes, 1}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_standalone_test() of
        false ->
            Config;
        true ->
            {skip, standalone_not_supported}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    %% snabbkaffe:fix_ct_logging(),
    Config.

end_per_testcase(TC, Config) ->
    %% emqx_cth_suite:clean_work_dir(emqx_cth_suite:work_dir(TC, Config)),
    ok.

cluster(TC, Config, Nnodes, Env) ->
    AppSpecs = [
        {emqx_durable_timer, #{
            override_env => Env,
            after_start => fun fix_logging/0
        }}
    ],
    NodeSpecs =
        [
            {list_to_atom("emqx_durable_timer_SUITE" ++ integer_to_list(I)), #{
                role => core,
                apps => AppSpecs
            }}
         || I <- lists:seq(1, Nnodes)
        ],
    emqx_cth_cluster:mk_nodespecs(
        NodeSpecs,
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ).

wait_heartbeat(Node) ->
    ?block_until(
        #{?snk_kind := ?tp_heartbeat, ?snk_meta := #{node := Node}}, infinity, 0
    ).

fix_logging() ->
    logger:set_primary_config(level, debug),
    logger:remove_handler(default),
    logger:add_handler(
        default,
        logger_std_h,
        #{
            config => #{file => "erlang.log", max_no_files => 1}
        }
    ).
