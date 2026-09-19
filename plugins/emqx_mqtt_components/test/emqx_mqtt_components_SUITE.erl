%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_components_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_retainer],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = application:ensure_all_started(cowboy),
    {ok, _} = application:ensure_all_started(inets),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    ok = emqx_retainer:clean(),
    {ok, _} = application:ensure_all_started(emqx_mqtt_components),
    Config.

end_per_testcase(_Case, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    application:stop(emqx_mqtt_components).

t_http_routes_and_handler_disconnect(_Config) ->
    Router = demo(<<"crouter">>, router),
    await(Router, active),
    H1 = demo(<<"chandler1">>, {handler, <<"/alpha">>}),
    await(H1, active),
    E1 = registered(Router),
    H2 = demo(<<"chandler2">>, {handler, <<"/beta">>}),
    await(H2, active),
    E2 = registered(Router),
    ?assertNotEqual(E1, E2),
    Port = emqx_mqtt_components_demo:port(Router),
    ?assertMatch(
        {200, #{
            <<"handler">> := <<"chandler1">>,
            <<"verb">> := <<"POST">>,
            <<"path">> := <<"/alpha/items">>,
            <<"body">> := <<"hello">>
        }},
        http(Port, post, "/alpha/items", <<"hello">>)
    ),
    ?assertMatch({200, #{<<"handler">> := <<"chandler2">>}}, http(Port, get, "/beta", <<>>)),
    ?assertEqual({404, <<>>}, http(Port, get, "/missing", <<>>)),

    ok = emqx_mqtt_components_demo:disconnect(H1),
    await(Router, {retracted, E1}),
    ?assertEqual({404, <<>>}, http(Port, get, "/alpha/items", <<>>)),
    ?assertMatch({200, #{<<"handler">> := <<"chandler2">>}}, http(Port, get, "/beta", <<>>)),

    ok = emqx_mqtt_components_demo:reconnect(H1),
    await(H1, active),
    ?assertNotEqual(E1, registered(Router)),
    ?assertMatch({200, #{<<"handler">> := <<"chandler1">>}}, http(Port, get, "/alpha", <<>>)).

t_router_reconnect(_Config) ->
    H1 = demo(<<"chandler1">>, {handler, <<"/one">>}),
    H2 = demo(<<"chandler2">>, {handler, <<"/two">>}),
    receive
        {component, _, initializing} -> ct:fail(initialized_without_router)
    after 100 -> ok
    end,
    Router = demo(<<"crouter">>, router),
    await(Router, active),
    await(H1, active),
    await(H2, active),
    OldEffects = [registered(Router), registered(Router)],
    Port = emqx_mqtt_components_demo:port(Router),
    ?assertMatch({200, #{<<"handler">> := <<"chandler1">>}}, http(Port, get, "/one", <<>>)),
    ?assertMatch({200, #{<<"handler">> := <<"chandler2">>}}, http(Port, get, "/two", <<>>)),

    ok = emqx_mqtt_components_demo:disconnect(Router),
    await(H1, inactive),
    await(H2, inactive),
    ?assertEqual({503, <<>>}, http(Port, get, "/one", <<>>)),

    ok = emqx_mqtt_components_demo:reconnect(Router),
    await(Router, active),
    await(H1, active),
    await(H2, active),
    NewEffects = [registered(Router), registered(Router)],
    ?assertEqual([], [E || E <- NewEffects, lists:member(E, OldEffects)]),
    ?assertMatch({200, #{<<"handler">> := <<"chandler1">>}}, http(Port, get, "/one", <<>>)),
    ?assertMatch({200, #{<<"handler">> := <<"chandler2">>}}, http(Port, get, "/two", <<>>)).

t_longest_prefix(_Config) ->
    Router = demo(<<"crouter">>, router),
    await(Router, active),
    H1 = demo(<<"general">>, {handler, <<"/api">>}),
    H2 = demo(<<"specific">>, {handler, <<"/api/items">>}),
    await(H1, active),
    await(H2, active),
    Port = emqx_mqtt_components_demo:port(Router),
    ?assertMatch({200, #{<<"handler">> := <<"general">>}}, http(Port, get, "/api/other", <<>>)),
    ?assertMatch({200, #{<<"handler">> := <<"specific">>}}, http(Port, get, "/api/items/1", <<>>)).

t_declaration_validation(_Config) ->
    P = client(<<"provider">>, [<<"$provide/service/a">>]),
    event(P, initialize),
    Conflict = client(<<"conflict">>, [<<"$provide/service/a">>]),
    error_event(Conflict, provider_conflict),
    Self = client(<<"self">>, [<<"$provide/service/b">>, <<"$consume/service/b">>]),
    error_event(Self, self_dependency),
    Wildcard = client(<<"wildcard">>, [<<"$provide/service/+">>]),
    error_event(Wildcard, invalid_service),
    Unsupported = client(<<"unsupported">>, [<<"$provide/unknown/a">>]),
    error_event(Unsupported, unsupported_declaration),
    {ok, _, _} = emqtt:subscribe(P, <<"$provide/service/new">>, 1),
    error_event(P, already_declared),
    A = client(<<"cycle-a">>, [<<"$provide/service/x">>, <<"$consume/service/y">>]),
    B = client(<<"cycle-b">>, [<<"$provide/service/y">>, <<"$consume/service/x">>]),
    error_event(B, dependency_cycle),
    publish(A, <<"$component/ready">>, <<>>),
    error_event(A, invalid_state_or_operation).

t_activation_waits_for_subscriptions(_Config) ->
    delay_subscription(<<"$service/a">>),
    P = client(<<"p">>, [<<"$provide/service/a">>]),
    event(P, initialize),
    C = client(<<"c">>, [<<"$consume/service/a">>]),
    publish(P, <<"$component/ready">>, <<>>),
    Channel = subscription_waiting(),
    no_mqtt(P),
    no_mqtt(C),
    publish(C, <<"$component/ready">>, <<>>),
    error_event(C, invalid_state_or_operation),
    Channel ! continue_subscription,
    event(P, activated),
    assert_service_subscriptions(Channel, <<"a">>),
    event(C, initialize),
    ready(C).

t_subscription_installation_failure(_Config) ->
    Previous = emqx_config:get_zone_conf(default, [mqtt, max_subscriptions]),
    emqx_config:put_zone_conf(default, [mqtt, max_subscriptions], 5),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_config:put_zone_conf(default, [mqtt, max_subscriptions], Previous)
    end),
    P = client(<<"p">>, [<<"$provide/service/a">>]),
    event(P, initialize),
    C = client(<<"c">>, [<<"$consume/service/a">>]),
    publish(P, <<"$component/ready">>, <<>>),
    error_event(P, subscription_installation_failed),
    no_mqtt(C),
    [Channel] = emqx_cm:lookup_channels(<<"p">>),
    ?assertEqual([], [T || {T = <<"$service/", _/binary>>, _} <- emqx_broker:subscriptions(Channel)]),
    {ok, _, _} = emqtt:unsubscribe(P, <<"test/p/reply">>),
    ready(P),
    assert_service_subscriptions(Channel, <<"a">>),
    event(C, initialize),
    ready(C).

t_dependency_loss_during_subscription_installation(_Config) ->
    Root = provider(<<"root">>, <<"$provide/service/root">>),
    delay_subscription(<<"$service/a">>),
    P = client(<<"p">>, [<<"$provide/service/a">>, <<"$consume/service/root">>]),
    event(P, initialize),
    C = client(<<"c">>, [<<"$consume/service/a">>]),
    publish(P, <<"$component/ready">>, <<>>),
    Channel = subscription_waiting(),
    ok = emqtt:disconnect(Root),
    no_mqtt(C),
    Channel ! continue_subscription,
    event(P, deactivated),
    cleanup_complete(P),
    event(P, stopped),
    cleanup_result(<<"root">>),
    no_mqtt(P),
    no_mqtt(C),
    ?assertEqual([], [T || {T = <<"$service/", _/binary>>, _} <- emqx_broker:subscriptions(Channel)]),
    ok = meck:expect(emqx_session, subscribe, fun(ClientInfo, Topic, Options, Session) ->
        meck:passthrough([ClientInfo, Topic, Options, Session])
    end),
    _Root2 = provider(<<"root">>, <<"$provide/service/root">>),
    event(P, initialize),
    ready(P),
    event(C, initialize),
    ready(C).

t_abort_retry_cleanup(_Config) ->
    {Service, P} = controlled(<<"p">>, [<<"$provide/service/p">>]),
    event(P, initialize),
    ready(P),
    S = provider(<<"s">>, <<"$provide/state/source">>),
    state_write(S, <<"$state/source">>, <<"source">>),
    state_message(S, <<"$state/source">>, <<"source">>),
    C = client(<<"c">>, [
        <<"$consume/service/p">>, <<"$consume/state/source">>, <<"$provide/state/own">>
    ]),
    event(C, initialize),
    state_write(C, <<"$state/own">>, <<"partial initialization">>),
    {ok, _, _} = emqtt:subscribe(C, <<"$state/source">>, 1),
    state_message(C, <<"$state/source">>, <<"source">>),
    A1 = request(C, <<"c">>, P, <<"p">>, <<"first">>),
    complete_apply(Service, A1, C),
    A2 = request(C, <<"c">>, P, <<"p">>, <<"pending">>),
    publish(C, <<"$component/abort">>, <<"cannot initialize">>),
    event(C, deactivated),
    lists:foreach(
        fun(Topic) ->
            publish(C, Topic, <<>>),
            error_event(C, invalid_state_or_operation)
        end,
        [<<"$component/ready">>, <<"$component/retry">>, <<"$component/abort">>, <<"$service/p">>]
    ),
    no_mqtt(P),
    no_mqtt(C),
    ok = emqx_mqtt_components_test_service:complete(Service, A2, applied),
    cleanup_complete(C),
    R2 = mqtt(P),
    ?assertEqual(retract_topic(A2), maps:get(topic, R2)),
    assert_effects(Service, [A1, A2]),
    no_mqtt(C),
    ok = emqx_mqtt_components_test_service:complete(Service, R2, retracted),
    R1 = mqtt(P),
    ?assertEqual(retract_topic(A1), maps:get(topic, R1)),
    assert_retained(<<"$state/own">>, <<"partial initialization">>),
    ok = emqx_mqtt_components_test_service:complete(Service, R1, retracted),
    event(C, aborted),
    assert_effects(Service, []),
    ?assertNot(subscribed(<<"c">>, <<"$state/source">>)),
    ?assertEqual({ok, []}, emqx_retainer:read_message(<<"$state/own">>)),
    ?assertMatch(#{initialization_failure := <<"cannot initialize">>}, cleanup_result(<<"c">>)),
    no_mqtt(C),
    Props = #{'Response-Topic' => <<"test/c/reply">>, 'Correlation-Data' => <<"retry-1">>},
    {ok, _} = emqtt:publish(C, <<"$component/retry">>, Props, <<>>, [{qos, 1}]),
    ?assertMatch(
        #{
            topic := <<"test/c/reply">>,
            properties := #{'Correlation-Data' := <<"retry-1">>},
            payload := <<"{\"event\":\"retry_accepted\"}">>
        },
        mqtt(C)
    ),
    event(C, initialize),
    A3 = request(C, <<"c">>, P, <<"p">>, <<"fresh activation">>),
    ?assertNotEqual(effect_id(A2), effect_id(A3)),
    respond(P, A2, <<"stale response">>),
    publish(C, <<"$component/ready">>, <<>>),
    error_event(C, initialization_pending),
    complete_apply(Service, A3, C),
    ready(C).

t_retry_without_dependencies(_Config) ->
    C = client(<<"c">>, [<<"$consume/service/p">>]),
    publish(C, <<"$component/retry">>, <<>>),
    event(C, retry_accepted),
    no_mqtt(C),
    P = provider(<<"p">>, <<"$provide/service/p">>),
    event(C, initialize),
    publish(C, <<"$component/abort">>, <<>>),
    event(C, deactivated),
    cleanup_complete(C),
    event(C, aborted),
    ok = emqtt:disconnect(P),
    cleanup_result(<<"p">>),
    publish(C, <<"$component/retry">>, <<>>),
    event(C, retry_accepted),
    no_mqtt(C),
    publish(C, <<"$component/retry">>, <<>>),
    event(C, retry_accepted),
    _P2 = provider(<<"p">>, <<"$provide/service/p">>),
    event(C, initialize),
    ready(C),
    no_mqtt(C).

t_abort_waits_for_retry(_Config) ->
    P = provider(<<"p">>, <<"$provide/service/p">>),
    C = client(<<"c">>, [<<"$consume/service/p">>]),
    event(C, initialize),
    publish(C, <<"$component/abort">>, <<>>),
    event(C, deactivated),
    cleanup_complete(C),
    event(C, aborted),
    ok = emqtt:disconnect(P),
    cleanup_result(<<"p">>),
    _P2 = provider(<<"p">>, <<"$provide/service/p">>),
    no_mqtt(C),
    publish(C, <<"$component/retry">>, <<>>),
    event(C, retry_accepted),
    event(C, initialize),
    ready(C).

t_admin_enable_after_abort(_Config) ->
    Admin = client(<<"admin">>, []),
    C = client(<<"c">>, [<<"$provide/service/c">>]),
    event(C, initialize),
    publish(C, <<"$component/abort">>, <<"first failure">>),
    event(C, deactivated),
    cleanup_complete(C),
    event(C, aborted),
    no_mqtt(C),
    admin(Admin, enable, <<"c">>),
    event(C, initialize),
    publish(C, <<"$component/abort">>, <<"second failure">>),
    event(C, deactivated),
    event(C, cleanup_requested),
    admin(Admin, enable, <<"c">>),
    no_mqtt(C),
    publish(C, <<"$component/cleanup_complete">>, <<>>),
    event(C, aborted),
    event(C, initialize),
    ?assertMatch(#{initialization_failure := <<"second failure">>}, cleanup_result(<<"c">>)),
    ready(C).

t_disable_replaces_abort(_Config) ->
    Admin = client(<<"admin">>, []),
    C = client(<<"c">>, [<<"$provide/service/c">>]),
    event(C, initialize),
    publish(C, <<"$component/abort">>, <<>>),
    event(C, deactivated),
    cleanup_complete(C),
    event(C, aborted),
    admin(Admin, disable, <<"c">>),
    no_mqtt(C),
    admin(Admin, enable, <<"c">>),
    event(C, initialize),
    ready(C).

t_retry_respects_disable(_Config) ->
    Admin = client(<<"admin">>, []),
    C = client(<<"c">>, [<<"$provide/service/c">>]),
    event(C, initialize),
    publish(C, <<"$component/abort">>, <<"aborted before disable">>),
    event(C, deactivated),
    admin(Admin, disable, <<"c">>),
    cleanup_complete(C),
    event(C, aborted),
    ?assertMatch(
        #{initialization_failure := <<"aborted before disable">>}, cleanup_result(<<"c">>)
    ),
    publish(C, <<"$component/retry">>, <<>>),
    event(C, retry_accepted),
    no_mqtt(C),
    admin(Admin, enable, <<"c">>),
    event(C, initialize),
    ready(C).

t_retry_and_abort_invalid_states(_Config) ->
    Stranger = client(<<"stranger">>, []),
    lists:foreach(
        fun(Topic) ->
            publish(Stranger, Topic, <<>>),
            error_event(Stranger, not_declared)
        end,
        [<<"$component/retry">>, <<"$component/abort">>]
    ),
    C = client(<<"c">>, [<<"$consume/service/p">>]),
    publish(C, <<"$component/abort">>, <<>>),
    error_event(C, invalid_state_or_operation),
    {ok, _} = emqtt:publish(C, <<"$component/retry">>, <<>>, [{qos, 1}, {retain, true}]),
    error_event(C, invalid_state_or_operation),
    P = provider(<<"p">>, <<"$provide/service/p">>),
    event(C, initialize),
    publish(C, <<"$component/retry">>, <<>>),
    error_event(C, invalid_state_or_operation),
    {ok, _} = emqtt:publish(C, <<"$component/abort">>, <<>>, [{qos, 1}, {retain, true}]),
    error_event(C, invalid_state_or_operation),
    Apply = request(C, <<"c">>, P, <<"p">>, <<"registration">>),
    respond(P, Apply, <<"{\"error\":\"route already exists\"}">>),
    ?assertMatch(#{payload := <<"{\"error\":\"route already exists\"}">>}, mqtt(C)),
    ready(C),
    lists:foreach(
        fun(Topic) ->
            publish(C, Topic, <<>>),
            error_event(C, invalid_state_or_operation)
        end,
        [<<"$component/retry">>, <<"$component/abort">>]
    ),
    ?assertEqual({ok, []}, emqx_retainer:read_message(<<"$component/retry">>)),
    ?assertEqual({ok, []}, emqx_retainer:read_message(<<"$component/abort">>)).

t_service_initialization_and_reverse_cleanup(_Config) ->
    P = provider(<<"provider">>, <<"$provide/service/a">>),
    C = client(<<"consumer">>, [<<"$consume/service/a">>]),
    event(C, initialize),
    Request = #{'Response-Topic' => <<"test/consumer/reply">>, 'Correlation-Data' => <<"first">>},
    {ok, _} = emqtt:publish(C, <<"$service/a">>, Request, <<"opaque payload">>, [{qos, 1}]),
    Apply1 = mqtt(P),
    ?assertMatch(
        #{
            topic := <<"$service/a/apply/", _/binary>>,
            payload := <<"opaque payload">>,
            properties := #{'Correlation-Data' := <<"first">>}
        },
        Apply1
    ),
    publish(C, <<"$component/ready">>, <<>>),
    error_event(C, initialization_pending),
    respond(P, Apply1, <<"accepted">>),
    ?assertMatch(
        #{
            topic := <<"test/consumer/reply">>,
            payload := <<"accepted">>,
            properties := #{'Correlation-Data' := <<"first">>}
        },
        mqtt(C)
    ),
    publish(C, <<"$component/ready">>, <<>>),
    event(C, activated),

    {ok, _} = emqtt:publish(
        C,
        <<"$service/a">>,
        Request#{'Correlation-Data' := <<"second">>},
        <<"second">>,
        [{qos, 1}]
    ),
    Apply2 = mqtt(P),
    respond(P, Apply2, <<"accepted">>),
    mqtt(C),
    {ok, _} = emqtt:publish(C, <<"$service/a">>, Request, <<"pending">>, [{qos, 1}]),
    Apply3 = mqtt(P),
    ok = emqtt:disconnect(C),
    no_mqtt(P),
    respond(P, Apply3, <<"accepted">>),
    retract(P, Apply3),
    retract(P, Apply2),
    retract(P, Apply1),
    ?assertMatch(#{local_cleanup := unknown}, cleanup_result(<<"consumer">>)).

t_interrupted_initialization(_Config) ->
    P = provider(<<"provider">>, <<"$provide/service/a">>),
    C = client(<<"consumer">>, [<<"$consume/service/a">>]),
    event(C, initialize),
    {ok, _} = emqtt:publish(
        C,
        <<"$service/a">>,
        #{'Response-Topic' => <<"test/consumer/reply">>},
        <<"register">>,
        [{qos, 1}]
    ),
    Apply = mqtt(P),
    ok = emqtt:disconnect(P),
    event(C, deactivated),
    event(C, cleanup_requested),
    publish(C, <<"$component/ready">>, <<>>),
    error_event(C, invalid_state_or_operation),
    publish(C, <<"$component/cleanup_complete">>, <<>>),
    event(C, stopped),
    P2 = provider(<<"provider">>, <<"$provide/service/a">>),
    event(C, initialize),
    respond(P2, Apply, <<"stale response">>),
    {ok, _} = emqtt:publish(
        C,
        <<"$service/a">>,
        #{'Response-Topic' => <<"test/consumer/reply">>},
        <<"register again">>,
        [{qos, 1}]
    ),
    Apply2 = mqtt(P2),
    ?assertNotEqual(maps:get(topic, Apply), maps:get(topic, Apply2)),
    publish(C, <<"$component/ready">>, <<>>),
    error_event(C, initialization_pending),
    respond(P2, Apply2, <<"accepted">>),
    ?assertMatch(#{payload := <<"accepted">>}, mqtt(C)),
    publish(C, <<"$component/ready">>, <<>>),
    event(C, activated),
    ok = emqtt:disconnect(P2),
    event(C, deactivated).

t_dependency_chain(_Config) ->
    P = provider(<<"root">>, <<"$provide/service/a">>),
    Middle = client(<<"middle">>, [<<"$consume/service/a">>, <<"$provide/service/b">>]),
    event(Middle, initialize),
    Leaf = client(<<"leaf">>, [<<"$consume/service/b">>]),
    publish(Leaf, <<"$component/ready">>, <<>>),
    error_event(Leaf, invalid_state_or_operation),
    publish(Middle, <<"$component/ready">>, <<>>),
    event(Middle, activated),
    event(Leaf, initialize),
    publish(Leaf, <<"$component/ready">>, <<>>),
    event(Leaf, activated),

    ok = emqtt:disconnect(P),
    event(Leaf, deactivated),
    event(Middle, deactivated),
    Conflict = client(<<"replacement">>, [<<"$provide/service/a">>]),
    error_event(Conflict, provider_conflict),
    publish(Middle, <<"$component/ready">>, <<>>),
    error_event(Middle, invalid_state_or_operation),
    cleanup_complete(Leaf),
    event(Leaf, stopped),
    cleanup_complete(Middle),
    event(Middle, stopped),
    cleanup_result(<<"root">>),
    _P2 = provider(<<"root">>, <<"$provide/service/a">>),
    event(Middle, initialize),
    publish(Middle, <<"$component/ready">>, <<>>),
    event(Middle, activated),
    event(Leaf, initialize),
    publish(Leaf, <<"$component/ready">>, <<>>),
    event(Leaf, activated).

t_reject_unbound_and_internal_requests(_Config) ->
    P = provider(<<"provider">>, <<"$provide/service/a">>),
    C = client(<<"consumer">>, [<<"$consume/service/missing">>]),
    publish(C, <<"$service/a">>, <<"not bound">>),
    error_event(C, invalid_state_or_operation),
    publish(P, <<"$service/a/apply/forged">>, <<"forged">>),
    error_event(P, undeclared_operation),
    publish(P, <<"$service/a">>, <<"self call">>),
    error_event(P, undeclared_operation),
    Unmanaged = client(<<"unmanaged">>, []),
    publish(Unmanaged, <<"$service/a">>, <<"not declared">>),
    error_event(Unmanaged, not_declared).

t_unmanaged_mqtt(_Config) ->
    C = client(<<"ordinary">>, [<<"ordinary/topic">>]),
    publish(C, <<"ordinary/topic">>, <<"hello">>),
    ?assertMatch(#{topic := <<"ordinary/topic">>, payload := <<"hello">>}, mqtt(C)).

t_transitive_initialization_disconnect(_Config) ->
    transitive_initialization(fun emqtt:disconnect/1).

t_transitive_initialization_connection_loss(_Config) ->
    transitive_initialization(fun(C) ->
        unlink(C),
        exit(C, kill),
        ok
    end).

t_diamond_initialization_disconnect(_Config) ->
    C = provider(<<"c">>, <<"$provide/service/c">>),
    B = client(<<"b">>, [<<"$provide/service/b">>, <<"$consume/service/c">>]),
    D = client(<<"d">>, [<<"$provide/service/d">>, <<"$consume/service/c">>]),
    lists:foreach(
        fun(P) ->
            event(P, initialize),
            ready(P)
        end,
        [B, D]
    ),
    A = client(<<"a">>, [<<"$consume/service/b">>, <<"$consume/service/d">>]),
    event(A, initialize),
    AB = request(A, <<"a">>, B, <<"b">>, <<"a-b">>),
    AD = request(A, <<"a">>, D, <<"d">>, <<"a-d">>),
    ok = emqtt:disconnect(C),
    event(A, deactivated),
    event(B, deactivated),
    event(D, deactivated),
    no_mqtt(B),
    no_mqtt(D),
    respond(B, AB, <<"late apply">>),
    respond(D, AD, <<"late apply">>),
    cleanup_complete(A),
    retract(D, AD),
    retract(B, AB),
    event(A, stopped),
    event(B, cleanup_requested),
    event(D, cleanup_requested),
    Conflict = client(<<"replacement">>, [<<"$provide/service/c">>]),
    error_event(Conflict, provider_conflict),
    lists:foreach(
        fun(P) ->
            publish(P, <<"$component/ready">>, <<>>),
            error_event(P, invalid_state_or_operation)
        end,
        [B, D]
    ),
    lists:foreach(
        fun(P) ->
            publish(P, <<"$component/cleanup_complete">>, <<>>),
            event(P, stopped)
        end,
        [B, D]
    ),
    ok = emqtt:disconnect(A),
    cleanup_result(<<"c">>),
    _C2 = provider(<<"c">>, <<"$provide/service/c">>),
    lists:foreach(
        fun(P) ->
            event(P, initialize),
            ready(P)
        end,
        [B, D]
    ),
    A2 = client(<<"a">>, [<<"$consume/service/b">>, <<"$consume/service/d">>]),
    event(A2, initialize),
    publish(A2, <<"$service/b">>, <<"missing response topic">>),
    error_event(A2, response_topic_required).

t_delayed_transitive_retractions(_Config) ->
    {EService, E} = controlled(<<"e">>, [<<"$provide/service/e">>]),
    event(E, initialize),
    ready(E),
    C = provider(<<"c">>, <<"$provide/service/c">>),
    {BService, B} = controlled(<<"b">>, [
        <<"$provide/service/b">>, <<"$consume/service/c">>, <<"$consume/service/e">>
    ]),
    event(B, initialize),
    BE = request(B, <<"b">>, E, <<"e">>, <<"b-e">>),
    complete_apply(EService, BE, B),
    ready(B),
    A = client(<<"a">>, [<<"$consume/service/b">>, <<"$consume/service/e">>]),
    D = client(<<"d">>, [<<"$consume/service/c">>, <<"$consume/service/e">>]),
    event(A, initialize),
    event(D, initialize),
    AB = request(A, <<"a">>, B, <<"b">>, <<"a-b">>),
    complete_apply(BService, AB, A),
    AE = request(A, <<"a">>, E, <<"e">>, <<"a-e">>),
    _DC = request(D, <<"d">>, C, <<"c">>, <<"d-c">>),
    DE = request(D, <<"d">>, E, <<"e">>, <<"d-e">>),
    complete_apply(EService, DE, D),
    ok = emqtt:disconnect(C),
    lists:foreach(fun(P) -> event(P, deactivated) end, [A, B, D]),
    cleanup_complete(D),
    RD = mqtt(E),
    ?assertEqual(retract_topic(DE), maps:get(topic, RD)),
    assert_effects(EService, [BE, DE]),
    assert_effects(BService, [AB]),
    no_mqtt(B),
    %% A's pending apply can land after withdrawal. It still needs an inverse.
    ok = emqx_mqtt_components_test_service:complete(EService, AE, applied),
    cleanup_complete(A),
    RA = mqtt(E),
    ?assertEqual(retract_topic(AE), maps:get(topic, RA)),
    assert_effects(EService, [BE, DE, AE]),
    publish(B, <<"$component/cleanup_complete">>, <<>>),
    error_event(B, invalid_state_or_operation),
    Conflict = client(<<"replacement">>, [<<"$provide/service/c">>]),
    error_event(Conflict, provider_conflict),
    ok = emqx_mqtt_components_test_service:complete(EService, RD, retracted),
    event(D, stopped),
    cleanup_result(<<"d">>),
    assert_effects(EService, [BE, AE]),
    no_mqtt(B),
    no_mqtt(E),
    ok = emqx_mqtt_components_test_service:complete(EService, RA, retracted),
    RB = mqtt(B),
    ?assertEqual(retract_topic(AB), maps:get(topic, RB)),
    assert_effects(EService, [BE]),
    assert_effects(BService, [AB]),
    no_mqtt(E),
    ok = emqx_mqtt_components_test_service:complete(BService, RB, retracted),
    event(A, stopped),
    cleanup_complete(B),
    RBE = mqtt(E),
    ?assertEqual(retract_topic(BE), maps:get(topic, RBE)),
    assert_effects(BService, []),
    assert_effects(EService, [BE]),
    {ok, _, _} = emqtt:subscribe(Conflict, <<"$provide/service/c">>, 1),
    error_event(Conflict, provider_conflict),
    ok = emqx_mqtt_components_test_service:complete(EService, RBE, retracted),
    event(B, stopped),
    cleanup_result(<<"c">>),
    assert_effects(EService, []),
    _C2 = provider(<<"c">>, <<"$provide/service/c">>),
    event(B, initialize),
    event(D, initialize),
    ready(B),
    event(A, initialize),
    NewAE = request(A, <<"a">>, E, <<"e">>, <<"new-a-e">>),
    respond(E, RA, <<"{\"status\":\"retracted\"}">>),
    respond(E, AE, <<"late duplicate apply response">>),
    publish(A, <<"$component/ready">>, <<>>),
    error_event(A, initialization_pending),
    complete_apply(EService, NewAE, A),
    ready(A),
    assert_effects(EService, [NewAE]).

t_lamps_switch_directory_cleanup(_Config) ->
    Admin = client(<<"admin">>, []),
    {DirectoryService, Directory} = controlled(<<"directory">>, [
        <<"$provide/service/directory/register-switch">>
    ]),
    event(Directory, initialize),
    ready(Directory),
    {SwitchService, Switch} = controlled(<<"switch">>, [
        <<"$consume/service/directory/register-switch">>,
        <<"$provide/service/switch/1/register-lamp">>
    ]),
    event(Switch, initialize),
    SwitchRegistration = request(
        Switch,
        <<"switch">>,
        Directory,
        <<"directory/register-switch">>,
        emqx_utils_json:encode(#{switch_id => <<"switch-1">>})
    ),
    complete_apply(DirectoryService, SwitchRegistration, Switch),
    ready(Switch),
    Lamps = [
        begin
            Lamp = client(Id, [<<"$consume/service/switch/1/register-lamp">>]),
            event(Lamp, initialize),
            Registration = request(
                Lamp,
                Id,
                Switch,
                <<"switch/1/register-lamp">>,
                emqx_utils_json:encode(#{
                    lamp_id => Id, command_topic => <<"devices/", Id/binary, "/command">>
                })
            ),
            complete_apply(SwitchService, Registration, Lamp),
            ready(Lamp),
            {Lamp, Registration}
        end
     || Id <- [<<"lamp-1">>, <<"lamp-2">>]
    ],
    [{Lamp1, Registration1}, {Lamp2, Registration2}] = Lamps,
    admin(Admin, disable, <<"directory">>),
    lists:foreach(fun(C) -> event(C, deactivated) end, [Directory, Switch, Lamp1, Lamp2]),
    cleanup_complete(Lamp1),
    Retract1 = mqtt(Switch),
    ?assertEqual(retract_topic(Registration1), maps:get(topic, Retract1)),
    cleanup_complete(Lamp2),
    Retract2 = mqtt(Switch),
    ?assertEqual(retract_topic(Registration2), maps:get(topic, Retract2)),
    assert_effects(SwitchService, [Registration1, Registration2]),
    assert_effects(DirectoryService, [SwitchRegistration]),
    no_mqtt(Switch),
    no_mqtt(Directory),

    %% One stopped lamp must not permit switch teardown while the other retracts.
    ok = emqx_mqtt_components_test_service:complete(SwitchService, Retract1, retracted),
    event(Lamp1, stopped),
    assert_effects(SwitchService, [Registration2]),
    no_mqtt(Lamp2),
    no_mqtt(Switch),
    no_mqtt(Directory),
    ok = emqx_mqtt_components_test_service:complete(SwitchService, Retract2, retracted),
    event(Lamp2, stopped),
    event(Switch, cleanup_requested),
    assert_effects(SwitchService, []),
    assert_effects(DirectoryService, [SwitchRegistration]),
    no_mqtt(Directory),

    %% The directory must wait for the switch's local cleanup and registration retraction.
    publish(Switch, <<"$component/cleanup_complete">>, <<>>),
    SwitchRetract = mqtt(Directory),
    ?assertEqual(retract_topic(SwitchRegistration), maps:get(topic, SwitchRetract)),
    assert_effects(DirectoryService, [SwitchRegistration]),
    no_mqtt(Switch),
    no_mqtt(Directory),
    ok = emqx_mqtt_components_test_service:complete(DirectoryService, SwitchRetract, retracted),
    event(Switch, stopped),
    event(Directory, cleanup_requested),
    assert_effects(DirectoryService, []),
    no_mqtt(Directory),
    publish(Directory, <<"$component/cleanup_complete">>, <<>>),
    event(Directory, stopped),
    ?assertEqual(
        [<<"directory">>, <<"switch">>, <<"lamp-2">>, <<"lamp-1">>],
        [Id || #{clientid := Id} <- emqx_mqtt_components:cleanup_results()]
    ).

t_failed_and_unknown_retractions(_Config) ->
    lists:foreach(fun failed_retraction/1, [failed, unknown]).

t_local_cleanup_waits_for_dependents(_Config) ->
    Admin = client(<<"admin">>, []),
    {EService, E} = controlled(<<"e">>, [<<"$provide/service/e">>]),
    event(E, initialize),
    ready(E),
    {BService, B} = controlled(<<"b">>, [
        <<"$provide/service/b">>, <<"$provide/state/b">>, <<"$consume/service/e">>
    ]),
    event(B, initialize),
    BE = request(B, <<"b">>, E, <<"e">>, <<"b-e">>),
    complete_apply(EService, BE, B),
    ready(B),
    state_write(B, <<"$state/b">>, <<"local cleanup needs this">>),
    state_message(B, <<"$state/b">>, <<"local cleanup needs this">>),
    A = client(<<"a">>, [<<"$consume/service/b">>]),
    event(A, initialize),
    AB = request(A, <<"a">>, B, <<"b">>, <<"pending a-b">>),
    admin(Admin, disable, <<"b">>),
    event(B, deactivated),
    event(A, deactivated),
    publish(B, <<"$component/cleanup_complete">>, <<>>),
    error_event(B, invalid_state_or_operation),
    no_mqtt(A),
    no_mqtt(B),
    no_mqtt(E),
    ok = emqx_mqtt_components_test_service:complete(BService, AB, applied),
    cleanup_complete(A),
    RAB = mqtt(B),
    ?assertEqual(retract_topic(AB), maps:get(topic, RAB)),
    assert_effects(BService, [AB]),
    no_mqtt(B),
    ok = emqx_mqtt_components_test_service:complete(BService, RAB, retracted),
    event(A, stopped),
    event(B, cleanup_requested),
    assert_effects(BService, []),
    assert_effects(EService, [BE]),
    assert_retained(<<"$state/b">>, <<"local cleanup needs this">>),
    {ok, _} = emqtt:publish(B, <<"$component/cleanup_complete">>, <<>>, [{qos, 1}, {retain, true}]),
    error_event(B, invalid_state_or_operation),
    admin(Admin, enable, <<"b">>),
    no_mqtt(B),
    no_mqtt(E),
    publish(B, <<"$component/cleanup_complete">>, <<>>),
    RBE = mqtt(E),
    ?assertEqual(retract_topic(BE), maps:get(topic, RBE)),
    no_mqtt(B),
    ?assertEqual({ok, []}, emqx_retainer:read_message(<<"$state/b">>)),
    ok = emqx_mqtt_components_test_service:complete(EService, RBE, retracted),
    event(B, stopped),
    event(B, initialize),
    no_mqtt(A),
    ready(B),
    event(A, initialize).

t_provider_disconnect_during_retraction(_Config) ->
    P = provider(<<"p">>, <<"$provide/service/p">>),
    C = client(<<"c">>, [<<"$consume/service/p">>]),
    event(C, initialize),
    Apply = request(C, <<"c">>, P, <<"p">>, <<"effect">>),
    complete_request(P, Apply, C),
    ready(C),
    ok = emqtt:disconnect(C),
    Retract = mqtt(P),
    ?assertEqual(retract_topic(Apply), maps:get(topic, Retract)),
    Fake = client(<<"fake">>, []),
    respond(Fake, Retract, <<"{\"status\":\"retracted\"}">>),
    respond(P, Retract, <<"not json">>),
    respond(P, Retract, <<"{\"status\":\"applied\"}">>),
    ?assertEqual([], emqx_mqtt_components:cleanup_results()),
    ok = emqtt:disconnect(P),
    #{effects := [#{cleanup := unknown}], local_cleanup := unknown} = cleanup_result(<<"c">>),
    cleanup_result(<<"p">>),
    P2 = provider(<<"p">>, <<"$provide/service/p">>),
    C2 = client(<<"c">>, [<<"$consume/service/p">>]),
    event(C2, initialize),
    New = request(C2, <<"c">>, P2, <<"p">>, <<"new effect">>),
    respond(P2, Retract, <<"{\"status\":\"retracted\"}">>),
    respond(P2, Apply, <<"stale">>),
    publish(C2, <<"$component/ready">>, <<>>),
    error_event(C2, initialization_pending),
    complete_request(P2, New, C2),
    ready(C2).

t_state_subscription_cleanup(_Config) ->
    Admin = client(<<"admin">>, []),
    P = provider(<<"state-p">>, <<"$provide/state/value">>),
    Topic = <<"$state/value">>,
    state_write(P, Topic, <<"one">>),
    state_message(P, Topic, <<"one">>),
    C = client(<<"state-c">>, [<<"$consume/state/value">>]),
    event(C, initialize),
    ?assertNot(subscribed(<<"state-c">>, Topic)),
    no_mqtt(C),
    {ok, _, _} = emqtt:subscribe(C, Topic, 1),
    state_message(C, Topic, <<"one">>),
    state_write(P, Topic, <<"two">>),
    state_message(P, Topic, <<"two">>),
    state_message(C, Topic, <<"two">>),
    {ok, _, _} = emqtt:unsubscribe(C, Topic),
    {ok, _, _} = emqtt:subscribe(C, Topic, 1),
    state_message(C, Topic, <<"two">>),
    ready(C),
    admin(Admin, disable, <<"state-c">>),
    event(C, deactivated),
    cleanup_complete(C),
    event(C, stopped),
    #{effects := Subs} = cleanup_result(<<"state-c">>),
    ?assertEqual([state_subscription, state_subscription], [T || #{type := T} <- Subs]),
    ?assert(lists:all(fun(#{cleanup := R}) -> R =:= retracted end, Subs)),
    ?assertNot(subscribed(<<"state-c">>, Topic)),
    state_write(P, Topic, <<"three">>),
    state_message(P, Topic, <<"three">>),
    no_mqtt(C),
    admin(Admin, enable, <<"state-c">>),
    event(C, initialize),
    ?assertNot(subscribed(<<"state-c">>, Topic)),
    {ok, _, _} = emqtt:subscribe(C, Topic, 1),
    state_message(C, Topic, <<"three">>),
    ready(C),
    ok = emqtt:disconnect(P),
    event(C, deactivated),
    cleanup_complete(C),
    event(C, stopped),
    #{effects := [#{type := state_write, cleanup := retracted}]} = cleanup_result(<<"state-p">>),
    ?assertEqual({ok, []}, emqx_retainer:read_message(Topic)),
    P2 = provider(<<"state-p">>, <<"$provide/state/value">>),
    event(C, initialize),
    state_write(P2, Topic, <<"replacement">>),
    state_message(P2, Topic, <<"replacement">>),
    no_mqtt(C),
    assert_retained(Topic, <<"replacement">>),
    ok = emqtt:disconnect(C),
    ok = emqtt:disconnect(P2),
    ?retry(20, 100, ?assertEqual({ok, []}, emqx_retainer:read_message(Topic))).

t_state_initialization_and_empty_write(_Config) ->
    Topic = <<"$state/initial">>,
    P = client(<<"initial-p">>, [<<"$provide/state/initial">>]),
    event(P, initialize),
    state_write(P, Topic, <<"initial">>),
    assert_retained(Topic, <<"initial">>),
    C = client(<<"initial-c">>, [<<"$consume/state/initial">>]),
    no_mqtt(C),
    publish(P, <<"$component/ready">>, <<>>),
    Messages = [mqtt(P), mqtt(P)],
    ?assertEqual([<<"initial">>], [B || #{topic := T, payload := B} <- Messages, T =:= Topic]),
    ?assertEqual([#{<<"event">> => <<"activated">>}], [
        emqx_utils_json:decode(B)
     || #{topic := T, payload := B} <- Messages, T =/= Topic
    ]),
    event(C, initialize),
    {ok, _, _} = emqtt:subscribe(C, Topic, 1),
    state_message(C, Topic, <<"initial">>),
    state_write(P, Topic, <<>>),
    state_message(P, Topic, <<>>),
    state_message(C, Topic, <<>>),
    ?assertEqual({ok, []}, emqx_retainer:read_message(Topic)),
    ok = emqtt:disconnect(C),
    ok = emqtt:disconnect(P),
    cleanup_result(<<"initial-p">>).

t_explicit_unsubscribe_waits_for_removal(_Config) ->
    Admin = client(<<"admin">>, []),
    Topic = <<"$state/value">>,
    P = provider(<<"p">>, <<"$provide/state/value">>),
    state_write(P, Topic, <<"kept until unsubscribe">>),
    state_message(P, Topic, <<"kept until unsubscribe">>),
    C = client(<<"c">>, [<<"$consume/state/value">>]),
    event(C, initialize),
    {ok, _, _} = emqtt:subscribe(C, Topic, 1),
    state_message(C, Topic, <<"kept until unsubscribe">>),
    ready(C),
    delay_unsubscribe(<<"c">>, Topic),
    Owner = self(),
    spawn_link(fun() -> Owner ! {unsuback, emqtt:unsubscribe(C, Topic)} end),
    Channel = unsubscribe_waiting(),
    ?assert(subscribed(<<"c">>, Topic)),
    #{effects := Effects} = sys:get_state(emqx_mqtt_components),
    ?assertMatch([#{cleanup := pending}], [
        E
     || E = #{type := state_subscription} <- maps:values(Effects)
    ]),
    admin(Admin, disable, <<"p">>),
    event(P, deactivated),
    no_mqtt(P),
    no_mqtt(C),
    assert_retained(Topic, <<"kept until unsubscribe">>),
    ?assertEqual([], emqx_mqtt_components:cleanup_results()),
    Channel ! continue_unsubscribe,
    receive
        {unsuback, {ok, _, _}} -> ok
    after 5000 -> ct:fail(missing_unsuback)
    end,
    event(C, deactivated),
    cleanup_complete(C),
    event(C, stopped),
    ?assertNot(subscribed(<<"c">>, Topic)),
    cleanup_complete(P),
    event(P, stopped),
    ?assertEqual({ok, []}, emqx_retainer:read_message(Topic)).

t_subscription_confinement(_Config) ->
    P = provider(<<"owner">>, <<"$provide/state/owned">>),
    Topic = <<"$state/owned">>,
    state_write(P, Topic, <<"original">>),
    state_message(P, Topic, <<"original">>),
    C = client(<<"reader">>, [<<"$consume/state/owned">>]),
    event(C, initialize),
    Stranger = client(<<"stranger">>, []),
    lists:foreach(
        fun(Filter) ->
            {ok, _, _} = emqtt:subscribe(Stranger, Filter, 1),
            error_event(Stranger, subscription_not_allowed),
            ?assertNot(subscribed(<<"stranger">>, Filter))
        end,
        [
            Topic,
            <<"$state/#">>,
            <<"$service/#">>,
            <<"$component/owner/events">>,
            <<"$component/#">>,
            <<"$share/readers/$state/owned">>
        ]
    ),
    {ok, _, _} = emqtt:subscribe(C, <<"$state/#">>, 1),
    error_event(C, subscription_not_allowed),
    state_write(Stranger, Topic, <<"forged">>),
    error_event(Stranger, not_declared),
    state_write(C, Topic, <<"forged">>),
    error_event(C, undeclared_operation),
    assert_retained(Topic, <<"original">>),
    publish(P, Topic, <<"not retained">>),
    error_event(P, invalid_state_or_operation),
    assert_retained(Topic, <<"original">>),
    {ok, _, _} = emqtt:subscribe(C, Topic, 1),
    state_message(C, Topic, <<"original">>),
    {ok, _, _} = emqtt:subscribe(Stranger, <<"ordinary/#">>, 1),
    publish(Stranger, <<"ordinary/ok">>, <<"ok">>),
    ?assertMatch(#{payload := <<"ok">>}, mqtt(Stranger)),
    ok = emqtt:disconnect(C),
    ok = emqtt:disconnect(P),
    cleanup_result(<<"owner">>).

t_state_transitive_disable(_Config) ->
    Admin = client(<<"admin">>, []),
    C = provider(<<"state-c">>, <<"$provide/state/c">>),
    state_write(C, <<"$state/c">>, <<"c">>),
    state_message(C, <<"$state/c">>, <<"c">>),
    B = client(<<"state-b">>, [<<"$consume/state/c">>, <<"$provide/state/b">>]),
    event(B, initialize),
    {ok, _, _} = emqtt:subscribe(B, <<"$state/c">>, 1),
    state_message(B, <<"$state/c">>, <<"c">>),
    ready(B),
    state_write(B, <<"$state/b">>, <<"b">>),
    state_message(B, <<"$state/b">>, <<"b">>),
    A = client(<<"state-a">>, [<<"$consume/state/b">>]),
    D = client(<<"state-d">>, [<<"$consume/state/c">>]),
    event(A, initialize),
    event(D, initialize),
    {ok, _, _} = emqtt:subscribe(A, <<"$state/b">>, 1),
    state_message(A, <<"$state/b">>, <<"b">>),
    {ok, _, _} = emqtt:subscribe(D, <<"$state/c">>, 1),
    state_message(D, <<"$state/c">>, <<"c">>),
    admin(Admin, disable, <<"state-c">>),
    lists:foreach(fun(P) -> event(P, deactivated) end, [A, B, C, D]),
    lists:foreach(
        fun(P) ->
            publish(P, <<"$component/cleanup_complete">>, <<>>),
            error_event(P, invalid_state_or_operation)
        end,
        [C, B]
    ),
    cleanup_complete(D),
    event(D, stopped),
    cleanup_result(<<"state-d">>),
    assert_retained(<<"$state/b">>, <<"b">>),
    assert_retained(<<"$state/c">>, <<"c">>),
    %% Enabling during cleanup must not restart any old activation.
    admin(Admin, enable, <<"state-c">>),
    no_mqtt(C),
    cleanup_complete(A),
    event(A, stopped),
    cleanup_complete(B),
    event(B, stopped),
    cleanup_complete(C),
    event(C, stopped),
    cleanup_result(<<"state-c">>),
    event(C, initialize),
    ?assertEqual({ok, []}, emqx_retainer:read_message(<<"$state/c">>)),
    ?assertEqual({ok, []}, emqx_retainer:read_message(<<"$state/b">>)),
    state_write(C, <<"$state/c">>, <<"new-c">>),
    ?assertNot(subscribed(<<"state-b">>, <<"$state/c">>)),
    ?assertNot(subscribed(<<"state-a">>, <<"$state/b">>)),
    ?assertNot(subscribed(<<"state-d">>, <<"$state/c">>)),
    ok = emqtt:disconnect(C),
    ?retry(20, 100, ?assertEqual({ok, []}, emqx_retainer:read_message(<<"$state/c">>))).

t_explicit_effect_release(_Config) ->
    {Service, P} = controlled(<<"release-p">>, [<<"$provide/service/release">>]),
    event(P, initialize),
    ready(P),
    C = client(<<"release-c">>, [<<"$consume/service/release">>]),
    event(C, initialize),
    A1 = request(C, <<"release-c">>, P, <<"release">>, <<"one">>),
    ok = emqx_mqtt_components_test_service:complete(Service, A1, applied),
    #{properties := Props} = mqtt(C),
    Id = proplists:get_value(<<"component-effect-id">>, maps:get('User-Property', Props)),
    ?assertEqual(effect_id(A1), Id),
    A2 = request(C, <<"release-c">>, P, <<"release">>, <<"two">>),
    complete_apply(Service, A2, C),
    ready(C),
    Other = client(<<"release-other">>, [<<"$consume/service/release">>]),
    event(Other, initialize),
    publish(Other, <<"$component/release/", Id/binary>>, <<>>),
    error_event(Other, effect_not_owned),
    publish(C, <<"$component/release/", Id/binary>>, <<>>),
    R = mqtt(P),
    ?assertEqual(retract_topic(A1), maps:get(topic, R)),
    no_mqtt(C),
    assert_effects(Service, [A1, A2]),
    ok = emqx_mqtt_components_test_service:complete(Service, R, retracted),
    released(C, Id, retracted),
    assert_effects(Service, [A2]),
    publish(C, <<"$component/release/", Id/binary>>, <<>>),
    released(C, Id, retracted),
    no_mqtt(P),
    A3 = request(C, <<"release-c">>, P, <<"release">>, <<"three">>),
    complete_apply(Service, A3, C),
    ok = emqtt:disconnect(C),
    R3 = mqtt(P),
    ?assertEqual(retract_topic(A3), maps:get(topic, R3)),
    ok = emqx_mqtt_components_test_service:complete(Service, R3, retracted),
    R2 = mqtt(P),
    ?assertEqual(retract_topic(A2), maps:get(topic, R2)),
    ok = emqx_mqtt_components_test_service:complete(Service, R2, retracted),
    cleanup_result(<<"release-c">>),
    assert_effects(Service, []),
    no_mqtt(P).

t_release_during_pending_apply(_Config) ->
    Admin = client(<<"admin">>, []),
    P = provider(<<"release-p">>, <<"$provide/service/release">>),
    C = client(<<"release-c">>, [<<"$consume/service/release">>]),
    event(C, initialize),
    A = request(C, <<"release-c">>, P, <<"release">>, <<"pending">>),
    Id = effect_id(A),
    publish(C, <<"$component/release/", Id/binary>>, <<>>),
    no_mqtt(P),
    admin(Admin, disable, <<"release-c">>),
    event(C, deactivated),
    respond(P, A, <<"accepted late">>),
    cleanup_complete(C),
    R = mqtt(P),
    ?assertEqual(retract_topic(A), maps:get(topic, R)),
    respond(P, R, <<"{\"status\":\"retracted\"}">>),
    released(C, Id, retracted),
    event(C, stopped),
    cleanup_result(<<"release-c">>),
    no_mqtt(C),
    admin(Admin, enable, <<"release-c">>),
    event(C, initialize),
    publish(C, <<"$component/release/", Id/binary>>, <<>>),
    error_event(C, effect_not_owned).

t_admin_web_router(_Config) ->
    Admin = client(<<"admin">>, []),
    Router = demo(<<"crouter">>, router),
    await(Router, active),
    H = demo(<<"chandler">>, {handler, <<"/api">>}),
    await(H, active),
    Old = registered(Router),
    Port = emqx_mqtt_components_demo:port(Router),
    ?assertMatch({200, _}, http(Port, get, "/api", <<>>)),
    admin(Admin, disable, <<"crouter">>),
    await(Router, inactive),
    await(H, inactive),
    await(Router, {retracted, Old}),
    await(H, stopped),
    await(Router, stopped),
    cleanup_result(<<"crouter">>),
    ?assertEqual({503, <<>>}, http(Port, get, "/api", <<>>)),
    admin(Admin, enable, <<"crouter">>),
    await(Router, active),
    await(H, active),
    ?assertNotEqual(Old, registered(Router)),
    ?assertMatch({200, _}, http(Port, get, "/api", <<>>)),
    publish(Admin, <<"$component-admin/disable">>, <<"not-json">>),
    error_event(Admin, invalid_admin_request),
    publish(Admin, <<"$component-admin/disable">>, <<"{\"clientid\":\"missing\"}">>),
    error_event(Admin, component_not_found).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

state_write(C, Topic, Payload) ->
    {ok, _} = emqtt:publish(C, Topic, Payload, [{qos, 1}, {retain, true}]),
    ok.

state_message(C, Topic, Payload) ->
    ?assertMatch(#{topic := Topic, payload := Payload}, mqtt(C)).

assert_retained(Topic, Payload) ->
    ?assertMatch({ok, [#message{payload = Payload}]}, emqx_retainer:read_message(Topic)).

subscribed(ClientId, Topic) ->
    [Channel] = emqx_cm:lookup_channels(ClientId),
    lists:keymember(Topic, 1, emqx_broker:subscriptions(Channel)).

admin(C, Action, ClientId) ->
    publish(
        C,
        <<"$component-admin/", (atom_to_binary(Action))/binary>>,
        emqx_utils_json:encode(#{clientid => ClientId})
    ),
    Event =
        case Action of
            enable -> <<"enabled">>;
            disable -> <<"disabled">>
        end,
    #{payload := Payload} = mqtt(C),
    ?assertEqual(
        #{<<"event">> => Event, <<"clientid">> => ClientId}, emqx_utils_json:decode(Payload)
    ).

released(C, Id, Result) ->
    #{payload := Payload} = mqtt(C),
    ?assertEqual(
        #{
            <<"event">> => <<"released">>,
            <<"effect_id">> => Id,
            <<"status">> => atom_to_binary(Result)
        },
        emqx_utils_json:decode(Payload)
    ).

delay_subscription(Topic) ->
    Owner = self(),
    ok = meck:new(emqx_session, [passthrough, no_history, no_link]),
    emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_session) end),
    ok = meck:expect(emqx_session, subscribe, fun(ClientInfo, Filter, Options, Session) ->
        case {Filter, ClientInfo} of
            {Topic, #{clientid := <<"p">>}} ->
                Owner ! {subscription_waiting, self()},
                receive
                    continue_subscription -> ok
                after 5000 -> error(subscription_not_released)
                end;
            _ ->
                ok
        end,
        meck:passthrough([ClientInfo, Filter, Options, Session])
    end).

delay_unsubscribe(ClientId, Topic) ->
    Owner = self(),
    ok = meck:new(emqx_session, [passthrough, no_history, no_link]),
    emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_session) end),
    ok = meck:expect(emqx_session, unsubscribe, fun(ClientInfo, Filter, Options, Session) ->
        case {Filter, ClientInfo} of
            {Topic, #{clientid := ClientId}} ->
                Owner ! {unsubscribe_waiting, self()},
                receive
                    continue_unsubscribe -> ok
                after 5000 -> error(unsubscribe_not_released)
                end;
            _ ->
                ok
        end,
        meck:passthrough([ClientInfo, Filter, Options, Session])
    end).

unsubscribe_waiting() ->
    receive
        {unsubscribe_waiting, Channel} -> Channel
    after 5000 -> ct:fail(missing_unsubscribe_attempt)
    end.

subscription_waiting() ->
    receive
        {subscription_waiting, Channel} -> Channel
    after 5000 -> ct:fail(missing_subscription_attempt)
    end.

assert_service_subscriptions(Channel, Key) ->
    Installed = maps:from_list(emqx_broker:subscriptions(Channel)),
    lists:foreach(fun(Topic) -> ?assert(maps:is_key(Topic, Installed)) end, [
        <<"$service/", Key/binary>>,
        <<"$service/", Key/binary, "/apply/+">>,
        <<"$service/", Key/binary, "/retract/+">>
    ]).

controlled(Id, Topics) ->
    {ok, Pid} = emqx_mqtt_components_test_service:start_link(Id, Topics),
    emqx_common_test_helpers:on_exit(fun() -> emqx_mqtt_components_test_service:stop(Pid) end),
    {Pid, emqx_mqtt_components_test_service:mqtt(Pid)}.

complete_apply(Service, Msg, Caller) ->
    ok = emqx_mqtt_components_test_service:complete(Service, Msg, applied),
    ?assertMatch(#{payload := <<"accepted">>}, mqtt(Caller)).

assert_effects(Service, Applies) ->
    Expected = maps:from_list([{effect_id(M), P} || M = #{payload := P} <- Applies]),
    ?assertEqual(Expected, emqx_mqtt_components_test_service:effects(Service)).

failed_retraction(Outcome) ->
    Id = atom_to_binary(Outcome),
    {Service, P} = controlled(Id, [<<"$provide/service/", Id/binary>>]),
    event(P, initialize),
    ready(P),
    CallerId = <<Id/binary, "-caller">>,
    C = client(CallerId, [<<"$consume/service/", Id/binary>>]),
    event(C, initialize),
    Apply1 = request(C, CallerId, P, Id, <<"one">>),
    complete_apply(Service, Apply1, C),
    Apply2 = request(C, CallerId, P, Id, <<"two">>),
    complete_apply(Service, Apply2, C),
    ready(C),
    ok = emqtt:disconnect(C),
    R2 = mqtt(P),
    ?assertEqual(retract_topic(Apply2), maps:get(topic, R2)),
    assert_effects(Service, [Apply1, Apply2]),
    no_mqtt(P),
    ok = emqx_mqtt_components_test_service:complete(Service, R2, Outcome),
    R1 = mqtt(P),
    ?assertEqual(retract_topic(Apply1), maps:get(topic, R1)),
    ok = emqx_mqtt_components_test_service:complete(Service, R1, retracted),
    #{effects := Results, local_cleanup := unknown} = cleanup_result(CallerId),
    ?assertEqual([Outcome, retracted], [R || #{cleanup := R} <- Results]),
    assert_effects(Service, [Apply2]).

transitive_initialization(Disconnect) ->
    %% E stays connected so the test can observe retractions after C disappears.
    E = provider(<<"e">>, <<"$provide/service/e">>),
    C = provider(<<"c">>, <<"$provide/service/c">>),
    B = client(<<"b">>, [
        <<"$provide/service/b">>, <<"$consume/service/c">>, <<"$consume/service/e">>
    ]),
    event(B, initialize),
    BC = request(B, <<"b">>, C, <<"c">>, <<"b-c">>),
    complete_request(C, BC, B),
    BE = request(B, <<"b">>, E, <<"e">>, <<"b-e">>),
    complete_request(E, BE, B),
    ready(B),
    A = client(<<"a">>, [<<"$consume/service/b">>, <<"$consume/service/e">>]),
    D = client(<<"d">>, [<<"$consume/service/c">>, <<"$consume/service/e">>]),
    event(A, initialize),
    event(D, initialize),
    AB1 = request(A, <<"a">>, B, <<"b">>, <<"a-b-1">>),
    complete_request(B, AB1, A),
    AE1 = request(A, <<"a">>, E, <<"e">>, <<"a-e-1">>),
    complete_request(E, AE1, A),
    AB2 = request(A, <<"a">>, B, <<"b">>, <<"a-b-2">>),
    AE2 = request(A, <<"a">>, E, <<"e">>, <<"a-e-2">>),
    DC = request(D, <<"d">>, C, <<"c">>, <<"d-c">>),
    DE1 = request(D, <<"d">>, E, <<"e">>, <<"d-e-1">>),
    complete_request(E, DE1, D),
    DE2 = request(D, <<"d">>, E, <<"e">>, <<"d-e-2">>),
    lists:foreach(
        fun(P) ->
            publish(P, <<"$component/ready">>, <<>>),
            error_event(P, initialization_pending)
        end,
        [A, D]
    ),

    ok = Disconnect(C),
    event(A, deactivated),
    event(D, deactivated),
    event(B, deactivated),
    no_mqtt(E),
    no_mqtt(B),
    lists:foreach(
        fun(P) ->
            publish(P, <<"$service/e">>, <<"stopped">>),
            error_event(P, invalid_state_or_operation),
            publish(P, <<"$component/ready">>, <<>>),
            error_event(P, invalid_state_or_operation)
        end,
        [A, B, D]
    ),
    %% Late applies must finish before their inverses start.
    respond(B, AB2, <<"late apply">>),
    respond(E, AE2, <<"late apply">>),
    respond(E, DE2, <<"late apply">>),
    cleanup_complete(A),
    cleanup_complete(D),
    Retracts = [mqtt(E), mqtt(E)],
    ?assertEqual(
        lists:sort([retract_topic(AE2), retract_topic(DE2)]),
        lists:sort([T || #{topic := T} <- Retracts])
    ),
    [RA] = [M || M = #{topic := T} <- Retracts, T =:= retract_topic(AE2)],
    [RD] = [M || M = #{topic := T} <- Retracts, T =:= retract_topic(DE2)],
    %% B must preserve its service until A finishes cleanup.
    publish(B, <<"$component/cleanup_complete">>, <<>>),
    error_event(B, invalid_state_or_operation),
    Conflict = client(<<"replacement">>, [<<"$provide/service/c">>]),
    error_event(Conflict, provider_conflict),
    no_mqtt(B),
    no_mqtt(E),
    %% D can finish while A's first inverse is still blocked.
    respond(E, RD, <<"{\"status\":\"retracted\"}">>),
    retract(E, DE1),
    event(D, stopped),
    #{effects := DResults} = cleanup_result(<<"d">>),
    ?assertEqual([retracted, retracted, unknown], [R || #{cleanup := R} <- DResults]),
    ?assertEqual(effect_id(DC), maps:get(id, lists:last(DResults))),
    no_mqtt(D),
    %% A retracts across B and E in reverse acceptance order.
    respond(E, RA, <<"{\"status\":\"retracted\"}">>),
    retract(B, AB2),
    retract(E, AE1),
    retract(B, AB1),
    no_mqtt(E),
    event(A, stopped),
    cleanup_complete(B),
    RBE = mqtt(E),
    ?assertEqual(retract_topic(BE), maps:get(topic, RBE)),
    {ok, _, _} = emqtt:subscribe(Conflict, <<"$provide/service/c">>, 1),
    error_event(Conflict, provider_conflict),
    no_mqtt(B),
    respond(E, RBE, <<"{\"status\":\"retracted\"}">>),
    event(B, stopped),
    cleanup_result(<<"c">>),
    C2 = provider(<<"c">>, <<"$provide/service/c">>),
    event(D, initialize),
    DC2 = request(D, <<"d">>, C2, <<"c">>, <<"d-c-new">>),
    ?assertNotEqual(maps:get(topic, DC), maps:get(topic, DC2)),
    complete_request(C2, DC2, D),
    ready(D),
    event(B, initialize),
    BC2 = request(B, <<"b">>, C2, <<"c">>, <<"b-c-new">>),
    complete_request(C2, BC2, B),
    ready(B),
    event(A, initialize),
    AB3 = request(A, <<"a">>, B, <<"b">>, <<"a-b-new">>),
    ?assertNotEqual(maps:get(topic, AB2), maps:get(topic, AB3)),
    respond(B, AB2, <<"stale response">>),
    publish(A, <<"$component/ready">>, <<>>),
    error_event(A, initialization_pending),
    complete_request(B, AB3, A),
    ready(A).

request(Caller, CallerId, Provider, Service, Payload) ->
    {ok, _} = emqtt:publish(
        Caller,
        <<"$service/", Service/binary>>,
        #{
            'Response-Topic' => <<"test/", CallerId/binary, "/reply">>,
            'Correlation-Data' => Payload
        },
        Payload,
        [{qos, 1}]
    ),
    Msg = mqtt(Provider),
    ?assertEqual(Payload, maps:get(payload, Msg)),
    Msg.

complete_request(Provider, Apply, Caller) ->
    respond(Provider, Apply, <<"accepted">>),
    ?assertMatch(#{payload := <<"accepted">>}, mqtt(Caller)).

cleanup_complete(C) ->
    event(C, cleanup_requested),
    publish(C, <<"$component/cleanup_complete">>, <<>>).

ready(C) ->
    publish(C, <<"$component/ready">>, <<>>),
    event(C, activated).

retract(C, Apply) ->
    Msg = mqtt(C),
    ?assertEqual(retract_topic(Apply), maps:get(topic, Msg)),
    ?assertEqual(<<>>, maps:get(payload, Msg)),
    respond(C, Msg, <<"{\"status\":\"retracted\"}">>).

no_mqtt(C) ->
    receive
        {mqtt, C, Msg} -> ct:fail({unexpected_mqtt_message, C, Msg})
    after 100 -> ok
    end.

cleanup_result(ClientId) ->
    ?retry(20, 100, begin
        [Result | _] = [
            R
         || R = #{clientid := Id} <- emqx_mqtt_components:cleanup_results(), Id =:= ClientId
        ],
        Result
    end).

effect_id(#{topic := Topic}) ->
    lists:last(binary:split(Topic, <<"/">>, [global])).

demo(Id, Role) ->
    {ok, Pid} = emqx_mqtt_components_demo:start_link(Id, Role),
    emqx_common_test_helpers:on_exit(fun() -> emqx_mqtt_components_demo:stop(Pid) end),
    Pid.

await(Pid, Event) ->
    receive
        {component, Pid, Event} -> ok
    after 5000 -> ct:fail({missing_component_event, Pid, Event})
    end.

registered(Pid) ->
    receive
        {component, Pid, {registered, Effect}} -> Effect
    after 5000 -> ct:fail(missing_registration)
    end.

http(Port, Method, Path, Body) ->
    URL = "http://127.0.0.1:" ++ integer_to_list(Port) ++ Path,
    Request =
        case Method of
            get -> {URL, []};
            post -> {URL, [], "text/plain", Body}
        end,
    {ok, {{_, Status, _}, _Headers, Response}} = httpc:request(
        Method,
        Request,
        [{timeout, 5000}],
        [{body_format, binary}]
    ),
    case Response of
        <<>> -> {Status, <<>>};
        _ -> {Status, emqx_utils_json:decode(Response)}
    end.

client(Id, Topics) ->
    Owner = self(),
    {ok, C} = emqtt:start_link([
        {clientid, Id},
        {proto_ver, v5},
        {msg_handler, #{publish => fun(Msg) -> Owner ! {mqtt, self(), Msg} end}}
    ]),
    {ok, _} = emqtt:connect(C),
    All = [<<"test/", Id/binary, "/reply">>, <<"$component/", Id/binary, "/events">> | Topics],
    {ok, _, _} = emqtt:subscribe(C, [{T, 1} || T <- All]),
    C.

provider(Id, Topic) ->
    C = client(Id, [Topic]),
    event(C, initialize),
    publish(C, <<"$component/ready">>, <<>>),
    event(C, activated),
    C.

publish(C, Topic, Payload) ->
    {ok, _} = emqtt:publish(C, Topic, Payload, [{qos, 1}]),
    ok.

mqtt(C) ->
    receive
        {mqtt, C, Msg} -> Msg
    after 5000 -> ct:fail({missing_mqtt_message, C})
    end.

event(C, Event) ->
    #{payload := Payload} = mqtt(C),
    ?assertEqual(#{<<"event">> => atom_to_binary(Event)}, emqx_utils_json:decode(Payload)).

error_event(C, Reason) ->
    #{payload := Payload} = mqtt(C),
    ?assertEqual(
        #{<<"event">> => <<"error">>, <<"reason">> => atom_to_binary(Reason)},
        emqx_utils_json:decode(Payload)
    ).

respond(C, #{properties := Props = #{'Response-Topic' := Topic}}, Payload) ->
    {ok, _} = emqtt:publish(C, Topic, maps:with(['Correlation-Data'], Props), Payload, [{qos, 1}]),
    ok.

retract_topic(#{topic := Topic}) ->
    binary:replace(Topic, <<"/apply/">>, <<"/retract/">>).
