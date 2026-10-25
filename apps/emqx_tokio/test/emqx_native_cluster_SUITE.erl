%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_native_cluster_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Nodes = emqx_cth_cluster:start(
        [{native_mqtt_one, node_spec(1)}, {native_mqtt_two, node_spec(2)}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{nodes, Nodes} | Config].

end_per_suite(Config) -> emqx_cth_cluster:stop(?config(nodes, Config)).

end_per_testcase(_Case, _Config) -> emqx_common_test_helpers:call_janitor().

-doc "Forward native and regular messages between nodes while preserving the matched filter and origin header".
t_cluster_delivery(Config) ->
    [N1, N2] = ?config(nodes, Config),
    A = emqx_native_listener_SUITE:connect(18901),
    B = emqx_native_listener_SUITE:connect(18902),
    C = emqx_native_listener_SUITE:connect(18912),
    [emqx_native_listener_SUITE:subscribe(S, [<<"cluster/#">>]) || S <- [A, B, C]],
    %% Both nodes must see both routes before either node publishes.
    [
        ?retry(
            50,
            100,
            ?assertEqual(
                2, length(rpc:call(Node, emqx_router, match_routes, [<<"cluster/topic">>]))
            )
        )
     || Node <- [N1, N2]
    ],
    ?retry(
        50, 100, ?assertEqual(2, length(rpc:call(N2, emqx_broker, subscribers, [<<"cluster/#">>])))
    ),
    emqx_native_listener_SUITE:publish(A, <<"cluster/topic">>, <<"from-native">>),
    [
        emqx_native_listener_SUITE:expect_publish(S, <<"cluster/topic">>, <<"from-native">>)
     || S <- [A, B, C]
    ],
    [emqx_native_listener_SUITE:assert_empty(S) || S <- [A, B, C]],
    emqx_native_listener_SUITE:publish(C, <<"cluster/topic">>, <<"from-regular">>),
    [
        emqx_native_listener_SUITE:expect_publish(S, <<"cluster/topic">>, <<"from-regular">>)
     || S <- [A, B, C]
    ],
    [emqx_native_listener_SUITE:assert_empty(S) || S <- [A, B, C]].

-doc "Keep forwarding enabled on native-only nodes while a cluster peer is present".
t_native_only_cluster(Config) ->
    [N1, N2] = ?config(nodes, Config),
    [
        begin
            {ok, _} = rpc:call(
                Node,
                emqx,
                update_config,
                [[listeners, tcp, default], {update, #{<<"enable">> => false}}]
            )
        end
     || Node <- [N1, N2]
    ],
    A = emqx_native_listener_SUITE:connect(18901),
    B = emqx_native_listener_SUITE:connect(18902),
    emqx_native_listener_SUITE:subscribe(B, [<<"native-cluster">>]),
    ?retry(
        50,
        100,
        ?assertEqual(1, length(rpc:call(N1, emqx_router, match_routes, [<<"native-cluster">>])))
    ),
    emqx_native_listener_SUITE:publish(A, <<"native-cluster">>, <<"peer">>),
    emqx_native_listener_SUITE:expect_publish(B, <<"native-cluster">>, <<"peer">>),
    emqx_native_listener_SUITE:assert_empty(B).

node_spec(Index) ->
    Listeners0 = emqx_native_listener_SUITE:base_listeners(),
    Listeners = Listeners0#{
        <<"native">> := #{<<"cluster">> => emqx_native_listener_SUITE:native_conf(18900 + Index)},
        <<"tcp">> := #{
            <<"default">> => #{
                <<"bind">> => iolist_to_binary(["127.0.0.1:", integer_to_list(18910 + Index)]),
                <<"enable">> => true,
                <<"enable_authn">> => false
            }
        }
    },
    #{
        role => core,
        apps => [
            {emqx_conf, #{config => #{<<"listeners">> => Listeners}}},
            {emqx_tokio, #{override_env => [{representatives, 2}, {worker_threads, 2}]}}
        ]
    }.
