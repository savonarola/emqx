%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_backend_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../include/emqx_omp.hrl").

all() ->
    [
        {group, postgresql},
        {group, mysql},
        {group, mariadb},
        t_simultaneous,
        t_topic_move,
        t_invalid_change,
        t_invalid_redis_change,
        t_all_backend_overlaps,
        t_config
    ].
groups() ->
    [
        {Id, [],
            [
                t_storage_contract,
                t_large_properties,
                t_batch_append,
                t_long_client_id,
                t_append_serialization,
                t_subscribe_share_lock,
                t_provision,
                t_mqtt_progress,
                t_failure,
                t_retention,
                t_wildcard_live,
                t_wildcard_restore,
                t_wildcard_batch,
                t_wildcard_retry,
                t_wildcard_missing_reply,
                t_wildcard_config_change,
                t_idle_flush
            ] ++ backend_cases(Id)}
     || Id <- [postgresql, mysql, mariadb]
    ].

-doc """
Run MySQL adapter cases against both MySQL and MariaDB. The PostgreSQL suite already
covers the delivery and cleanup cases for PostgreSQL.
""".
backend_cases(postgresql) ->
    [];
backend_cases(Id) when Id =:= mysql; Id =:= mariadb ->
    [
        t_legacy_conversion,
        t_provision_failure,
        t_missing_schema_version,
        t_append_binary_values,
        t_append_sql_text,
        t_append_failure_unlocks,
        t_schema_lock_timeout,
        t_consumed_cleanup,
        t_consumed_cleanup_race,
        t_async_thresholds,
        t_append_timeout,
        t_live_recovery,
        t_cluster_routing
    ].

init_per_suite(Config) ->
    Config0 = emqx_offline_messages_pg_SUITE:init_per_suite(Config),
    lists:foreach(fun drop_mysql_schema/1, [mysql, mariadb]),
    Config0.

drop_mysql_schema(Id) ->
    {ok, Conn} = emqx_omp_mysql_sql:connect(parse(Id, raw(Id, <<"setup">>))),
    {ok, _} = emqx_omp_mysql_sql:query(Conn, """
    DROP TABLE IF EXISTS mqtt_omp_subscription, mqtt_omp_message, mqtt_omp_topic, mqtt_omp_schema
    """, []),
    ok = emqx_omp_mysql_sql:close(Conn).

end_per_suite(Config) -> emqx_offline_messages_pg_SUITE:end_per_suite(Config).
init_per_group(Id, Config) -> [{backend_id, Id} | Config].
end_per_group(_Id, _Config) -> ok.

init_per_testcase(Case, Config) ->
    Id = proplists:get_value(backend_id, Config, mysql),
    Raw = raw(Id, atom_to_binary(Case)),
    Parsed = parse(Id, Raw),
    #{backend := Backend} = Parsed,
    ok = Backend:install(Parsed),
    {ok, Conn} = Backend:connect(Parsed),
    ok = emqx_omp:start(Parsed),
    ok = snabbkaffe:start_trace(),
    [{raw, Raw}, {parsed, Parsed}, {backend, Backend}, {db, Conn} | Config].

end_per_testcase(_Case, Config) ->
    emqx_common_test_helpers:call_janitor(),
    meck:unload(),
    snabbkaffe:stop(),
    ok = emqx_omp:stop(mysql),
    ok = emqx_omp:stop(postgresql),
    Backend = ?config(backend, Config),
    Backend:close(?config(db, Config)).

-doc "Check page boundaries, binary fields, rollback and monotonic subscriber progress on both backends.".
t_storage_contract(Config) ->
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace} = ?config(parsed, Config),
    Topic = <<"contract/storage">>,
    {ok, #{topic_id := Id, cursor := 0}} = Backend:subscribe(
        Conn, Namespace, <<"a">>, Topic, #{qos => 1}, earliest
    ),
    ?assertMatch(
        {ok, [{0, null, null, null, null, null, null, null, null, null}]},
        Backend:page(Conn, Id, 0, 2)
    ),
    Props = #{
        'Correlation-Data' => <<0, 255>>,
        'User-Property' => [{<<"k">>, <<"1">>}, {<<"k">>, <<"2">>}]
    },
    Msg0 = emqx_message:set_header(
        properties, Props, emqx_message:make(<<"p">>, 2, Topic, <<0, 255>>)
    ),
    Msg = Msg0#message{flags = #{retain => true}},
    Encoded = emqx_omp_store:encode(Msg),
    ?assertException(error, _, Backend:append(Conn, Namespace, Topic, [Encoded#{qos := 3}])),
    {ok, {Id, 1}} = Backend:append(Conn, Namespace, Topic, [Encoded, Encoded, Encoded]),
    {ok, Rows0} = Backend:page(Conn, Id, 0, 2),
    ?assertEqual([1, 2], [element(2, Row) || Row <- Rows0]),
    ?assertEqual([3, 3], [element(1, Row) || Row <- Rows0]),
    {ok, Decoded} = emqx_omp_store:decode(Topic, {{mysql, Namespace}, Id}, hd(Rows0)),
    ?assertEqual(<<0, 255>>, Decoded#message.payload),
    ?assertEqual(#{retain => true}, Decoded#message.flags),
    ?assertEqual(Props, emqx_message:get_header(properties, Decoded)),
    {ok, [Row1]} = Backend:page(Conn, Id, 2, 2),
    ?assertEqual(3, element(2, Row1)),
    {ok, #{cursor := 3}} = Backend:subscribe(Conn, Namespace, <<"b">>, Topic, #{qos => 1}, latest),
    {ok, _} = Backend:checkpoint(Conn, Namespace, <<"a">>, Id, 2),
    {ok, _} = Backend:checkpoint(Conn, Namespace, <<"a">>, Id, 1),
    {ok, #{cursor := 2}} = Backend:subscribe(Conn, Namespace, <<"a">>, Topic, #{qos => 1}, latest),
    ?assertEqual({ok, 3}, Backend:consumed_cutoff(Conn, Id, 4)),
    {ok, [{Topic, _}]} = Backend:restore(Conn, Namespace, <<"a">>),
    {ok, 1} = Backend:unsubscribe(Conn, Namespace, <<"a">>, Topic),
    ?assertEqual({ok, []}, Backend:restore(Conn, Namespace, <<"a">>)),
    %% Topics and namespaces use byte-exact identity, including case and trailing spaces.
    {ok, {OtherId, 1}} = Backend:append(Conn, Namespace, <<"contract/Storage ">>, [Encoded]),
    ?assertNotEqual(Id, OtherId),
    {ok, {OtherNamespaceId, 1}} = Backend:append(Conn, <<Namespace/binary, " ">>, Topic, [Encoded]),
    ?assertNotEqual(Id, OtherNamespaceId).

-doc "Round-trip a maximum-length MQTT correlation value through encoded properties.".
t_large_properties(Config) ->
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace, backend_id := BackendId} = ?config(parsed, Config),
    Topic = <<"contract/large-properties">>,
    Props = #{'Correlation-Data' => binary:copy(<<0, 255, 1>>, 21845)},
    Msg = emqx_message:set_header(properties, Props, emqx_message:make(<<"p">>, 1, Topic, <<>>)),
    {ok, {Id, 1}} = Backend:append(Conn, Namespace, Topic, [emqx_omp_store:encode(Msg)]),
    {ok, [Row]} = Backend:page(Conn, Id, 0, 1),
    {ok, Decoded} = emqx_omp_store:decode(Topic, {{BackendId, Namespace}, Id}, Row),
    ?assertEqual(Props, emqx_message:get_header(properties, Decoded)).

-doc "Store a 100-message batch in one append in order at consecutive offsets.".
t_batch_append(Config) ->
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace} = ?config(parsed, Config),
    Topic = <<"contract/batch">>,
    Payloads = [<<N:32, 0, 255>> || N <- lists:seq(1, 100)],
    Encoded = [
        emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, Payload))
     || Payload <- Payloads
    ],
    {ok, {Id, 1}} = Backend:append(Conn, Namespace, Topic, Encoded),
    {ok, Rows} = Backend:page(Conn, Id, 0, 200),
    ?assertEqual(lists:seq(1, 100), [element(2, Row) || Row <- Rows]),
    ?assertEqual(Payloads, [element(7, Row) || Row <- Rows]),
    ?assertEqual({ok, {Id, 101}}, Backend:append(Conn, Namespace, Topic, [hd(Encoded)])).

-doc "Deny MySQL-managed subscriptions for client IDs longer than 2048 bytes.".
t_long_client_id(Config) ->
    #{backend_id := BackendId} = ?config(parsed, Config),
    Topic = <<"contract/long-client-id">>,
    lists:foreach(
        fun(#{length := Length, expected := Expected}) ->
            ClientInfo = #{clientid => binary:copy(<<"c">>, Length), protocol => mqtt},
            ok = emqx_omp:on_session(ClientInfo, #{impl => emqx_session_mem}),
            Context = #{
                message_target => make_ref(), clientinfo => ClientInfo, subopts => #{qos => 1}
            },
            Authorize = emqx_omp:on_authorize(
                ClientInfo, #{action_type => subscribe, qos => 1}, Topic, #{result => allow}
            ),
            case maps:get(BackendId, Expected) of
                allow ->
                    ?assertEqual({ok, #{result => allow}}, Authorize);
                deny ->
                    ?assertMatch({stop, #{result := deny}}, Authorize),
                    ?assertEqual(
                        ignore,
                        emqx_omp_extsub:handle_subscribe(subscribe, Context, undefined, Topic)
                    )
            end
        end,
        [
            #{length => 2048, expected => #{mysql => allow, postgresql => allow}},
            #{length => 2049, expected => #{mysql => deny, postgresql => allow}}
        ]
    ).

-doc "Block a competing append until the topic lock is released and preserve consecutive offsets.".
t_append_serialization(Config) ->
    Backend = ?config(backend, Config),
    Conn0 = ?config(db, Config),
    Parsed = ?config(parsed, Config),
    #{namespace := Namespace, backend_id := Id} = Parsed,
    {ok, Conn1} = Backend:connect(Parsed),
    emqx_common_test_helpers:on_exit(fun() -> Backend:close(Conn1) end),
    Topic = <<"contract/locked">>,
    Encoded = emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, <<"data">>)),
    {ok, {TopicId, 1}} = Backend:append(Conn0, Namespace, Topic, [Encoded]),
    {ok, _} = Backend:query(Conn0, "BEGIN", []),
    {Table, Parameter} =
        case Id of
            postgresql -> {"mqtt_pg_topic", "$1"};
            mysql -> {"mqtt_omp_topic", "?"}
        end,
    {ok, _} = Backend:query(
        Conn0,
        ["SELECT next_offset FROM ", Table, " WHERE topic_id = ", Parameter, " FOR UPDATE"],
        [TopicId]
    ),
    Parent = self(),
    spawn_link(fun() ->
        Parent ! {appended, Backend:append(Conn1, Namespace, Topic, [Encoded, Encoded])}
    end),
    ?assertNotReceive({appended, _}, 100),
    {ok, _} = Backend:query(Conn0, "ROLLBACK", []),
    ?assertReceive({appended, {ok, {TopicId, 2}}}, 5000),
    {ok, Rows} = Backend:page(Conn0, TopicId, 0, 10),
    ?assertEqual([1, 2, 3], [element(2, Row) || Row <- Rows]).

-doc "Let subscriptions share the topic lock with each other, while appends wait for it.".
t_subscribe_share_lock(Config) ->
    Backend = ?config(backend, Config),
    Conn0 = ?config(db, Config),
    Parsed = ?config(parsed, Config),
    #{namespace := Namespace, backend_id := Id} = Parsed,
    {ok, Conn1} = Backend:connect(Parsed),
    {ok, Conn2} = Backend:connect(Parsed),
    emqx_common_test_helpers:on_exit(fun() ->
        Backend:close(Conn1),
        Backend:close(Conn2)
    end),
    Topic = <<"contract/shared-lock">>,
    Encoded = emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, <<"data">>)),
    {ok, {TopicId, 1}} = Backend:append(Conn0, Namespace, Topic, [Encoded]),
    %% Hold a share lock on the topic row, as an uncommitted subscription does.
    {ok, _} = Backend:query(Conn0, "BEGIN", []),
    SQL =
        case Id of
            postgresql -> "SELECT next_offset FROM mqtt_pg_topic WHERE topic_id = $1 FOR SHARE";
            mysql -> "SELECT next_offset FROM mqtt_omp_topic WHERE topic_id = ? LOCK IN SHARE MODE"
        end,
    {ok, _} = Backend:query(Conn0, SQL, [TopicId]),
    Parent = self(),
    %% Another subscription does not wait for the lock holder.
    _ = spawn(fun() ->
        Parent !
            {subscribed,
                Backend:subscribe(Conn1, Namespace, <<"second">>, Topic, #{qos => 1}, latest)}
    end),
    ?assertReceive({subscribed, {ok, #{topic_id := TopicId, cursor := 1}}}, 2000),
    %% An append waits until the lock holder ends its transaction.
    spawn_link(fun() -> Parent ! {appended, Backend:append(Conn2, Namespace, Topic, [Encoded])} end),
    ?assertNotReceive({appended, _}, 200),
    {ok, _} = Backend:query(Conn0, "ROLLBACK", []),
    ?assertReceive({appended, {ok, {TopicId, 2}}}, 5000).

-doc "Repeat provisioning without removing messages and check schema health.".
t_provision(Config) ->
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace} = Parsed = ?config(parsed, Config),
    Msg = emqx_omp_store:encode(
        emqx_message:make(<<"p">>, 1, <<"contract/schema">>, <<"retained">>)
    ),
    {ok, {Id, 1}} = Backend:append(Conn, Namespace, <<"contract/schema">>, [Msg]),
    ok = Backend:install(Parsed),
    ok = Backend:install(Parsed),
    Parent = self(),
    lists:foreach(
        fun(_) ->
            spawn_link(fun() ->
                ok = Backend:install(Parsed),
                Parent ! installed
            end)
        end,
        [1, 2]
    ),
    ?assertReceive(installed, 5000),
    ?assertReceive(installed, 5000),
    ?assertEqual(ok, Backend:health(Conn)),
    ?assertMatch(
        {ok, [{1, 1, _, _, _, _, <<"retained">>, _, _, _}]}, Backend:page(Conn, Id, 0, 10)
    ).

-doc "Replay independently to two clients and resume a clean-start client after its checkpoint.".
t_mqtt_progress(Config) ->
    Topic = <<"contract/progress">>,
    Pub = client(Config, <<"contract-pub">>, []),
    {ok, _} = emqtt:publish(Pub, Topic, <<"one">>, 1),
    {ok, _} = emqtt:publish(Pub, Topic, <<"two">>, 2),
    Slow = client(Config, <<"contract-slow">>, [{auto_ack, false}]),
    {ok, _, [1]} = emqtt:subscribe(Slow, Topic, 1),
    ?assertEqual([<<"one">>, <<"two">>], payloads(receive_messages(2))),
    Fast0 = client(Config, <<"contract-fast">>, []),
    {ok, _, [1]} = emqtt:subscribe(Fast0, Topic, 1),
    ?assertEqual([<<"one">>, <<"two">>], payloads(receive_messages(2))),
    ?retry(20, 100, ?assertEqual(2, cursor(Config, <<"contract-fast">>, Topic))),
    ?assertEqual(0, cursor(Config, <<"contract-slow">>, Topic)),
    ok = emqtt:disconnect(Fast0),
    ?retry(20, 100, ?assertEqual([], emqx_cm_registry:lookup_all_channels(<<"contract-fast">>))),
    {ok, _} = emqtt:publish(Pub, Topic, <<"three">>, 1),
    [_] = receive_messages(1),
    Fast1 = client(Config, <<"contract-fast">>, [{clean_start, true}]),
    ?assertEqual([<<"three">>], payloads(receive_messages(1))),
    ok = emqtt:disconnect(Slow),
    ok = emqtt:disconnect(Fast1),
    ok = emqtt:disconnect(Pub).

-doc "Fail managed publishes and health checks when the backend pools are unavailable.".
t_failure(Config) ->
    #{backend_id := Id} = ?config(parsed, Config),
    ok = emqx_omp_sup:stop(Id),
    ?assertMatch({error, _}, emqx_omp:on_health_check(Id, ?config(raw, Config))),
    ?assertMatch(
        {error, disconnect, _},
        emqx_broker:publish2(
            emqx_message:make(<<"p">>, 1, <<"contract/failure">>, <<"not-routed">>)
        )
    ).

-doc "Trim consumed and old prefixes without reusing offsets or crossing namespaces.".
t_retention(Config) ->
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace} = ?config(parsed, Config),
    Topic = <<"contract/retention">>,
    Msg = emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, <<"data">>)),
    Now = erlang:system_time(millisecond),
    {ok, {Id, 1}} = Backend:append(Conn, Namespace, Topic, [
        Msg#{published_at := Now - 10000}, Msg, Msg
    ]),
    {ok, {OtherId, 1}} = Backend:append(Conn, <<"other">>, Topic, [Msg]),
    ?assertEqual({ok, 2}, Backend:age_cutoff(Conn, Id, 4, Now - 5000)),
    {ok, _} = Backend:subscribe(Conn, Namespace, <<"a">>, Topic, #{qos => 1}, earliest),
    {ok, _} = Backend:checkpoint(Conn, Namespace, <<"a">>, Id, 2),
    ?assertEqual({ok, 3}, Backend:consumed_cutoff(Conn, Id, 4)),
    ?assertEqual({ok, 2}, Backend:trim(Conn, Id, 3, 1)),
    ?assertMatch({ok, [{3, 3, _, _, _, _, _, _, _, _}]}, Backend:page(Conn, Id, 0, 10)),
    ?assertMatch({ok, [{1, 1, _, _, _, _, _, _, _, _}]}, Backend:page(Conn, OtherId, 0, 10)),
    ?assertEqual({ok, 1}, Backend:trim(Conn, Id, 4, 1)),
    ?assertEqual({ok, {Id, 4}}, Backend:append(Conn, Namespace, Topic, [Msg])).

-doc "Deliver live copies once per wildcard subscription with its QoS, and never from storage.".
t_wildcard_live(Config) ->
    Pub = client(Config, <<"wildcard-pub">>, []),
    %% A message published before the subscriptions is stored but not delivered to wildcards.
    {ok, _} = emqtt:publish(Pub, <<"contract/live/a">>, <<"before">>, 1),
    Monitor = client(Config, <<"wildcard-monitor">>, []),
    {ok, _, [0, 1, 1]} = emqtt:subscribe(Monitor, [
        {<<"contract/live/+">>, 0}, {<<"contract/live/#">>, 1}, {<<"contract/live/a">>, 1}
    ]),
    %% Only the exact subscription replays the stored message.
    ?assertEqual([{1, <<"before">>}], qos_payloads(receive_messages(1))),
    no_messages(),
    %% Each subscription delivers one copy with its own QoS, although the channel receives
    %% the live copy once for each of its three matching routes.
    {ok, _} = emqtt:publish(Pub, <<"contract/live/a">>, <<"live">>, 1),
    ?assertEqual(
        [{0, <<"live">>}, {1, <<"live">>}, {1, <<"live">>}], qos_payloads(receive_messages(3))
    ),
    no_messages(),
    %% A filter over managed and unmanaged topics receives both kinds of messages.
    All = client(Config, <<"wildcard-all">>, []),
    {ok, _, [1]} = emqtt:subscribe(All, <<"#">>, 1),
    {ok, _} = emqtt:publish(Pub, <<"contract/other">>, <<"managed">>, 1),
    {ok, _} = emqtt:publish(Pub, <<"unmanaged/topic">>, <<"unmanaged">>, 1),
    ?assertEqual([<<"managed">>, <<"unmanaged">>], lists:sort(payloads(receive_messages(2)))),
    no_messages(),
    ok = emqtt:disconnect(All),
    ok = emqtt:disconnect(Monitor),
    ok = emqtt:disconnect(Pub).

-doc "Restore a wildcard subscription after reconnect without delivering messages published offline.".
t_wildcard_restore(Config) ->
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace} = ?config(parsed, Config),
    ClientId = <<"wildcard-restore">>,
    Filter = <<"contract/restore/#">>,
    %% Store the wildcard subscription.
    Monitor0 = client(Config, ClientId, []),
    {ok, _, [1]} = emqtt:subscribe(Monitor0, Filter, 1),
    ?retry(50, 100, ?assertMatch({ok, [{Filter, _}]}, Backend:restore(Conn, Namespace, ClientId))),
    ok = emqtt:disconnect(Monitor0),
    ?retry(20, 100, ?assertEqual([], emqx_cm_registry:lookup_all_channels(ClientId))),
    %% A message published while the client is offline is not kept for the wildcard.
    Pub = client(Config, <<"wildcard-restore-pub">>, []),
    {ok, _} = emqtt:publish(Pub, <<"contract/restore/a">>, <<"offline">>, 1),
    %% A new in-memory session gets the stored subscription back and receives live messages.
    Monitor1 = client(Config, ClientId, [{clean_start, true}]),
    ?retry(50, 100, ?assert(is_subscribed(ClientId, Filter))),
    {ok, _} = emqtt:publish(Pub, <<"contract/restore/a">>, <<"online">>, 1),
    ?assertEqual([<<"online">>], payloads(receive_messages(1))),
    no_messages(),
    %% Unsubscribe removes the stored subscription.
    {ok, _, _} = emqtt:unsubscribe(Monitor1, Filter),
    ?retry(20, 100, ?assertEqual({ok, []}, Backend:restore(Conn, Namespace, ClientId))),
    ok = emqtt:disconnect(Monitor1),
    ok = emqtt:disconnect(Pub).

-doc "Queue an entire committed wildcard batch while the receive window allows one message.".
t_wildcard_batch(Config) ->
    Sub = client(Config, <<"wildcard-batch">>, [{properties, #{'Receive-Maximum' => 1}}]),
    Filter = <<"contract/batch/+">>,
    Topic = <<"contract/batch/a">>,
    {ok, _, [1]} = emqtt:subscribe(Sub, Filter, 1),
    #{namespace := Namespace, backend_id := BackendId} = ?config(parsed, Config),
    Messages = [
        emqx_message:make(<<"p">>, 1, Topic, integer_to_binary(N))
     || N <- lists:seq(1, 200)
    ],
    {ok, {Id, Offset}} = emqx_omp_store:append_batch(
        Namespace, Topic, [emqx_omp_store:encode(M) || M <- Messages], ?config(parsed, Config)
    ),
    ok = emqx_omp_store:dispatch(Messages, {{BackendId, Namespace}, Id}, Offset),
    ?assertEqual([M#message.payload || M <- Messages], payloads(receive_messages(200))),
    no_messages(),
    ok = emqtt:disconnect(Sub).

-doc "Retry failed wildcard subscription writes without requiring the client to resubscribe.".
t_wildcard_retry(Config) ->
    wildcard_retry(Config, error).

-doc "Retry wildcard subscription writes when the pool sends no reply.".
t_wildcard_missing_reply(Config) ->
    wildcard_retry(Config, missing).

wildcard_retry(Config, Failure) ->
    Parsed = (?config(parsed, Config))#{request_timeout := 100},
    ok = emqx_omp:start(Parsed),
    ClientId = <<"wildcard-retry">>,
    Filter = <<"contract/retry/#">>,
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace} = Parsed,
    Test = self(),
    ok = meck:new(emqx_omp_store, [passthrough, no_link]),
    ok = meck:expect(emqx_omp_store, subscribe_async, fun(_, _, _, _, _, Target, Ref, _) ->
        Test ! subscribe_attempt,
        case Failure of
            error -> emqx_omp_store:reply({done, {error, unavailable}}, Target, Ref);
            missing -> ok
        end
    end),
    Sub = client(Config, ClientId, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Filter, 1),
    ?assertReceive(subscribe_attempt),
    ok = meck:unload(emqx_omp_store),
    ?retry(50, 100, ?assertMatch({ok, [{Filter, _}]}, Backend:restore(Conn, Namespace, ClientId))),
    ok = emqtt:disconnect(Sub).

-doc "Store connected wildcard subscriptions in the replacement namespace.".
t_wildcard_config_change(Config) ->
    ClientId = <<"wildcard-config">>,
    Filter = <<"contract/config/#">>,
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace, backend_id := BackendId} = ?config(parsed, Config),
    Raw0 = ?config(raw, Config),
    Sub = client(Config, ClientId, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Filter, 1),
    ?retry(50, 100, ?assertMatch({ok, [{Filter, _}]}, Backend:restore(Conn, Namespace, ClientId))),
    NewNamespace = <<Namespace/binary, "-replacement">>,
    ok = emqx_omp:start(parse(BackendId, Raw0#{<<"namespace">> := NewNamespace})),
    ?retry(
        50, 100, ?assertMatch({ok, [{Filter, _}]}, Backend:restore(Conn, NewNamespace, ClientId))
    ),
    {ok, _, _} = emqtt:unsubscribe(Sub, Filter),
    ?retry(20, 100, ?assertEqual({ok, []}, Backend:restore(Conn, NewNamespace, ClientId))),
    ok = emqtt:disconnect(Sub).

-doc "Run the checkpoint flush timer only after an acknowledgement, not for an idle subscription.".
t_idle_flush(Config) ->
    Raw = ?config(raw, Config),
    #{backend_id := Id} = ?config(parsed, Config),
    Checkpoint = #{<<"interval">> => 50, <<"max_pending">> => 10},
    ok = emqx_omp:start(parse(Id, Raw#{<<"checkpoint">> => Checkpoint})),
    Topic = <<"contract/idle">>,
    ClientId = <<"idle-sub">>,
    Sub = client(Config, ClientId, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    %% An idle subscription runs no flush timer.
    timer:sleep(300),
    ?assertEqual(0, flush_count(ClientId)),
    %% One acknowledgement arms the timer once, and its flush saves the progress.
    Pub = client(Config, <<"idle-pub">>, []),
    {ok, _} = emqtt:publish(Pub, Topic, <<"one">>, 1),
    ?assertEqual([<<"one">>], payloads(receive_messages(1))),
    ?retry(20, 100, ?assertEqual(1, cursor(Config, ClientId, Topic))),
    timer:sleep(300),
    ?assertEqual(1, flush_count(ClientId)),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

-doc "Check that cleanup keeps acknowledged history with `earliest` and removes it with `latest`.".
t_consumed_cleanup(Config) ->
    %% Give two subscriptions different acknowledged offsets in the same topic.
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    Raw = ?config(raw, Config),
    #{backend_id := Id, namespace := Namespace} = ?config(parsed, Config),
    Topic = <<"contract/consumed">>,
    {ok, {TopicId, 1}} = Backend:append(Conn, Namespace, Topic, encoded(Topic, [1, 2, 3])),
    {ok, _} = Backend:subscribe(Conn, Namespace, <<"a">>, Topic, #{qos => 1}, earliest),
    {ok, _} = Backend:subscribe(Conn, Namespace, <<"b">>, Topic, #{qos => 1}, earliest),
    {ok, _} = Backend:checkpoint(Conn, Namespace, <<"a">>, TopicId, 3),
    {ok, _} = Backend:checkpoint(Conn, Namespace, <<"b">>, TopicId, 1),
    Retention = #{<<"interval">> => 100},
    %% With `earliest`, acknowledged messages within retention stay readable.
    ok = emqx_omp:start(parse(Id, Raw#{<<"retention">> => Retention})),
    timer:sleep(500),
    ?assertEqual([1, 2, 3], offsets(Config, Topic)),
    %% With `latest`, messages acknowledged by every subscriber are removed.
    Latest = Raw#{<<"retention">> => Retention, <<"new_subscription_start">> => <<"latest">>},
    ok = emqx_omp:start(parse(Id, Latest)),
    ?retry(50, 100, ?assertEqual([2, 3], offsets(Config, Topic))),
    %% A topic without subscriptions keeps no messages with `latest`.
    {ok, _} = Backend:query(
        Conn, "DELETE FROM mqtt_omp_subscription WHERE namespace = ?", [Namespace]
    ),
    ?retry(50, 100, ?assertEqual([], offsets(Config, Topic))),
    ?assertEqual({ok, 4}, Backend:consumed_cutoff(Conn, TopicId, 4)).

-doc "Check that the topic lock orders a `latest` subscription before later appends.".
t_consumed_cleanup_race(Config) ->
    %% Acknowledge the existing history before creating a concurrent latest subscription.
    Backend = ?config(backend, Config),
    Conn0 = ?config(db, Config),
    #{namespace := Namespace} = Parsed = ?config(parsed, Config),
    {ok, Conn1} = Backend:connect(Parsed),
    emqx_common_test_helpers:on_exit(fun() -> Backend:close(Conn1) end),
    Topic = <<"contract/race">>,
    {ok, {TopicId, 1}} = Backend:append(Conn0, Namespace, Topic, encoded(Topic, [1, 2])),
    {ok, _} = Backend:subscribe(Conn0, Namespace, <<"old">>, Topic, #{qos => 1}, earliest),
    {ok, _} = Backend:checkpoint(Conn0, Namespace, <<"old">>, TopicId, 2),
    %% Hold the share lock as a subscribe transaction does after its tip read.
    {ok, _} = Backend:query(Conn1, "BEGIN", []),
    {ok, [{3}]} = Backend:query(
        Conn1, "SELECT next_offset FROM mqtt_omp_topic WHERE topic_id = ? LOCK IN SHARE MODE", [
            TopicId
        ]
    ),
    Parent = self(),
    spawn_link(fun() ->
        Parent ! {appended, Backend:append(Conn0, Namespace, Topic, encoded(Topic, [3]))}
    end),
    ?assertNotReceive({appended, _}, 200),
    {ok, _} = Backend:query(Conn1, """
    INSERT INTO mqtt_omp_subscription
      (namespace, client_id, topic_id, subopts, ack_offset)
    VALUES (?, 'new', ?, '{}', 2)
    """, [Namespace, TopicId]),
    {ok, _} = Backend:query(Conn1, "COMMIT", []),
    ?assertReceive({appended, {ok, {TopicId, 3}}}, 5000),
    %% The old subscriber acknowledges the new offset, and the cutoff stays at the new start.
    {ok, _} = Backend:checkpoint(Conn0, Namespace, <<"old">>, TopicId, 3),
    ?assertEqual({ok, 3}, Backend:consumed_cutoff(Conn0, TopicId, 4)).

-doc "Check that count and byte thresholds flush async batches and dispatch each as one term.".
t_async_thresholds(Config) ->
    Raw = ?config(raw, Config),
    #{backend_id := Id} = ?config(parsed, Config),
    lists:foreach(
        fun(#{name := Name, count := Count, bytes := Bytes}) ->
            %% Keep timers beyond the test deadline so only the size threshold can flush.
            Writer = #{
                <<"pool_size">> => 1,
                <<"min_flush_timeout">> => 10000,
                <<"max_flush_timeout">> => 10000,
                <<"flush_max_messages">> => Count,
                <<"flush_max_bytes">> => Bytes
            },
            ok = emqx_omp:start(
                parse(Id, Raw#{<<"async_batched">> => true, <<"writer">> => Writer})
            ),
            Pub = client(Config, Name, []),
            Topic = <<"contract/", Name/binary>>,
            ok = emqx:subscribe(Topic),
            %% Acknowledge both publish QoS levels before dispatching their single topic batch.
            {ok, _} = emqtt:publish(Pub, Topic, <<"abc">>, 1),
            ?assertEqual([], stored(Config, Topic)),
            {ok, _} = emqtt:publish(Pub, Topic, <<"def">>, 2),
            ?retry(
                20, 100, ?assertEqual([{1, <<"abc">>}, {2, <<"def">>}], stored(Config, Topic))
            ),
            ?assertReceive(
                #dispatch_info{
                    topic = Topic,
                    info = #omp_live{
                        offset = 1,
                        messages = [
                            #message{payload = <<"abc">>}, #message{payload = <<"def">>}
                        ]
                    }
                }
            ),
            ?assertNotReceive(#dispatch_info{info = #omp_live{}}),
            ok = emqx:unsubscribe(Topic),
            ok = emqtt:disconnect(Pub)
        end,
        [
            #{name => <<"count">>, count => 2, bytes => 10000},
            #{name => <<"bytes">>, count => 100, bytes => 6}
        ]
    ).

-doc "Check that appends over the query timeout disconnect MQTT 3.1.1 and MQTT 5 publishers.".
t_append_timeout(Config) ->
    %% Shorten the query timeout so a held topic row lock fails each MQTT publish.
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    Raw = ?config(raw, Config),
    #{backend_id := Id, namespace := Namespace} = ?config(parsed, Config),
    Topic = <<"contract/failure">>,
    {ok, {TopicId, 1}} = Backend:append(Conn, Namespace, Topic, encoded(Topic, [1])),
    ok = emqx_omp:start(parse(Id, Raw#{<<"query_timeout">> => 200})),
    lists:foreach(
        fun(Version) ->
            %% Check publisher disconnect without a successful acknowledgement.
            Pub = client(Config, atom_to_binary(Version), [{proto_ver, Version}]),
            unlink(Pub),
            Monitor = monitor(process, Pub),
            {ok, _} = Backend:query(Conn, "BEGIN", []),
            {ok, _} = Backend:query(
                Conn, "SELECT next_offset FROM mqtt_omp_topic WHERE topic_id = ? FOR UPDATE", [
                    TopicId
                ]
            ),
            ?assertNotMatch({ok, _}, catch emqtt:publish(Pub, Topic, <<"lost">>, 1)),
            receive
                {'DOWN', Monitor, process, Pub, _} -> ok
            after 5000 -> ct:fail(not_disconnected)
            end,
            {ok, _} = Backend:query(Conn, "ROLLBACK", [])
        end,
        [v4, v5]
    ),
    %% The failed appends are not stored, also after the lock is released.
    timer:sleep(500),
    ?assertEqual([<<"1">>], [Payload || {_, Payload} <- stored(Config, Topic)]).

-doc "Check that live delivery needs no page reads and idle polling recovers a lost final copy.".
t_live_recovery(Config) ->
    %% Wait for initial catch-up with idle polling outside the live-delivery test interval.
    Raw = ?config(raw, Config),
    #{backend_id := Id} = ?config(parsed, Config),
    Read = #{<<"page_max_messages">> => 2, <<"idle_poll_interval">> => 1000},
    ok = emqx_omp:start(parse(Id, Raw#{<<"read">> => Read})),
    Topic = <<"contract/live-recovery">>,
    Sub = client(Config, <<"live-recovery">>, []),
    ?assertWaitEvent(
        emqtt:subscribe(Sub, Topic, 1),
        #{?snk_kind := omp_page, clientid := <<"live-recovery">>, dirty := false},
        5000
    ),
    %% Record database reads after the initial snapshot finishes.
    _ = snabbkaffe:collect_trace(),
    Pub = client(Config, <<"live-recovery-pub">>, []),
    lists:foreach(
        fun(N) ->
            {ok, _} = emqtt:publish(Pub, Topic, integer_to_binary(N), 1),
            ?assertEqual([integer_to_binary(N)], payloads(receive_messages(1)))
        end,
        lists:seq(1, 10)
    ),
    ?assertEqual([], ?of_kind(omp_read_page, snabbkaffe:collect_trace())),
    %% Lose the live copy of a committed message and recover it from the database.
    ok = meck:new(emqx_omp_store, [passthrough, no_link]),
    ok = meck:expect(emqx_omp_store, dispatch, fun(_Msg, _Identity, _Offset) -> ok end),
    {ok, _} = emqtt:publish(Pub, Topic, <<"lost-copy">>, 1),
    ok = meck:delete(emqx_omp_store, dispatch, 3),
    ?assertEqual([<<"lost-copy">>], payloads(receive_messages(1))),
    ?retry(20, 100, ?assertEqual(11, cursor(Config, <<"live-recovery">>, Topic))),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

-doc "Check cross-node live dispatch and ordered delivery with publishers on two nodes.".
t_cluster_routing(Config) ->
    %% Start two broker nodes with the same namespace and live-delivery hooks.
    Apps = [
        {emqx_conf, #{config => #{authorization => #{no_match => allow}}}},
        emqx,
        emqx_extsub,
        emqx_offline_messages
    ],
    Nodes =
        [Node0, Node1] = emqx_cth_cluster:start(
            [{omp_backend_0, #{apps => Apps}}, {omp_backend_1, #{apps => Apps}}],
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    emqx_common_test_helpers:on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    Raw = ?config(raw, Config),
    #{backend_id := Id} = ?config(parsed, Config),
    Read = #{<<"page_max_messages">> => 2, <<"idle_poll_interval">> => 60000},
    lists:foreach(
        fun(Node) ->
            ok = rpc:call(Node, emqx_omp, start, [parse(Id, Raw#{<<"read">> => Read})]),
            ok = rpc:call(
                Node,
                emqx_hooks,
                put,
                ['message.delivered', {?MODULE, cluster_delivered, [self()]}, 0]
            )
        end,
        Nodes
    ),
    Config0 = [{port, emqx_cth_cluster:get_tcp_mqtt_port(Node0)} | Config],
    %% Subscribe on the second node and check a remote publish without recovery reads.
    Config1 = [{port, emqx_cth_cluster:get_tcp_mqtt_port(Node1)} | Config],
    Topic = <<"contract/cluster">>,
    Sub = client(Config1, <<"cluster-sub">>, []),
    ?assertWaitEvent(
        emqtt:subscribe(Sub, Topic, 1),
        #{?snk_kind := omp_page, clientid := <<"cluster-sub">>, dirty := false},
        5000
    ),
    ok = emqx_cth_cluster:sync_routes(Nodes),
    _ = snabbkaffe:collect_trace(),
    Pub0 = client(Config0, <<"cluster-pub0">>, []),
    Pub1 = client(Config1, <<"cluster-pub1">>, []),
    {ok, _} = emqtt:publish(Pub0, Topic, <<"remote">>, 1),
    ?assertEqual([<<"remote">>], payloads(receive_messages(1))),
    ?assertReceive({cluster_delivered, {_, _, 1}}, 5000),
    ?assertEqual([], ?of_kind(omp_read_page, snabbkaffe:collect_trace())),
    %% Publish concurrently on both nodes and compare delivery order with database offsets.
    Parent = self(),
    lists:foreach(
        fun({Pub, Prefix}) ->
            spawn_link(fun() ->
                lists:foreach(
                    fun(N) ->
                        {ok, _} = emqtt:publish(
                            Pub, Topic, <<Prefix/binary, (integer_to_binary(N))/binary>>, 1
                        )
                    end,
                    lists:seq(1, 20)
                ),
                Parent ! {publisher_done, Pub}
            end)
        end,
        [{Pub0, <<"a">>}, {Pub1, <<"b">>}]
    ),
    Received = payloads(receive_messages(40)),
    ?assertReceive({publisher_done, Pub0}, 5000),
    ?assertReceive({publisher_done, Pub1}, 5000),
    ?assertEqual(
        [Payload || {Offset, Payload} <- stored(Config, Topic), Offset > 1], Received
    ),
    Delivered = [
        receive
            {cluster_delivered, {_, _, Offset}} -> Offset
        after 5000 -> ct:fail(missing_cluster_delivery)
        end
     || _ <- lists:seq(1, 40)
    ],
    ?assertEqual(lists:seq(2, 41), Delivered),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub0),
    ok = emqtt:disconnect(Pub1).

cluster_delivered(#{clientid := <<"cluster-sub">>}, Msg, Parent) ->
    Parent ! {cluster_delivered, emqx_message:get_header(?OMP_POSITION, Msg)},
    ok;
cluster_delivered(_ClientInfo, _Msg, _Parent) ->
    ok.

-doc "Run both SQL backends with the same namespace and client and reload only MySQL.".
t_simultaneous(Config) ->
    Mysql0 = ?config(raw, Config),
    Pg = raw(postgresql, <<"t_simultaneous">>),
    Mysql = Mysql0#{<<"topics">> := [<<"mysql/#">>], <<"async_batched">> => true},
    PgRaw = Pg#{<<"topics">> := [<<"pg/#">>], <<"async_batched">> => true},
    Both = #{<<"mysql">> => Mysql, <<"postgresql">> => PgRaw},
    ok = emqx_offline_messages:on_config_changed(#{<<"mysql">> => Mysql0}, Both),
    PgConfig = emqx_omp:config(postgresql),
    Sub = client(Config, <<"both-sub">>, []),
    {ok, _, [1, 1]} = emqtt:subscribe(Sub, [{<<"pg/a">>, 1}, {<<"mysql/a">>, 1}]),
    Pub = client(Config, <<"both-pub">>, []),
    {ok, _} = emqtt:publish(Pub, <<"pg/a">>, <<"pg">>, 1),
    {ok, _} = emqtt:publish(Pub, <<"mysql/a">>, <<"mysql">>, 1),
    ?assertEqual([<<"mysql">>, <<"pg">>], lists:sort(payloads(receive_messages(2)))),
    MysqlConfig0 = emqx_omp:config(mysql),
    Mysql1 = Mysql#{<<"message_read_pool_size">> => 3},
    ok = emqx_offline_messages:on_config_changed(Both, Both#{<<"mysql">> := Mysql1}),
    ?assertNotEqual(MysqlConfig0, emqx_omp:config(mysql)),
    ?assertEqual(PgConfig, emqx_omp:config(postgresql)),
    ok = emqx_omp:stop(mysql),
    {ok, _} = emqtt:publish(Pub, <<"pg/a">>, <<"still-running">>, 1),
    ?assertEqual([<<"still-running">>], payloads(receive_messages(1))),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

-doc "Move a topic from PostgreSQL to MySQL while publishing, and store or reject each publish.".
t_topic_move(Config) ->
    Topic = <<"move/", (integer_to_binary(erlang:system_time(microsecond)))/binary>>,
    Mysql0 = ?config(raw, Config),
    Pg = raw(postgresql, <<"t_topic_move">>),
    Old = #{
        <<"mysql">> => Mysql0#{<<"topics">> := [<<"mysql-only/#">>]},
        <<"postgresql">> => Pg#{<<"topics">> := [<<"move/#">>]}
    },
    New = #{
        <<"mysql">> => Mysql0#{<<"topics">> := [<<"move/#">>]},
        <<"postgresql">> => Pg#{<<"topics">> := [<<"pg-only/#">>]}
    },
    ok = emqx_offline_messages:on_config_changed(#{<<"mysql">> => Mysql0}, Old),
    %% Observe ordinary routing, which a managed publish must never reach.
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    %% Publish continuously while the topic moves to MySQL.
    Parent = self(),
    Publisher = spawn_link(fun() -> publish_loop(Parent, Topic, 0, 0) end),
    ct:sleep(100),
    ok = emqx_offline_messages:on_config_changed(Old, New),
    ct:sleep(100),
    Publisher ! stop,
    {Published, Rejected} =
        receive
            {published, Count, RejectedCount} -> {Count, RejectedCount}
        after 5000 -> ct:fail(publisher_timeout)
        end,
    %% Each publish is stored in one backend or rejected, and none is routed without storage.
    ?assertNotReceive({deliver, Topic, _}),
    Stored = count_messages(postgresql, Pg, Topic) + count_messages(mysql, Mysql0, Topic),
    ?assertEqual(Published, Stored + Rejected),
    ?assert(count_messages(mysql, Mysql0, Topic) > 0).

-doc "Reject an invalid PostgreSQL configuration without changing the running MySQL backend.".
t_invalid_change(Config) ->
    Mysql0 = ?config(raw, Config),
    Before = emqx_omp:configs(),
    Mysql1 = Mysql0#{<<"message_read_pool_size">> => 3},
    Pg = raw(postgresql, <<"t_invalid_change">>),
    BadPg = Pg#{
        <<"topics">> => [<<"pg/#">>],
        <<"writer">> => #{<<"min_flush_timeout">> => 100, <<"max_flush_timeout">> => 10}
    },
    ?assertMatch(
        {error, {invalid_config, _}},
        emqx_offline_messages:on_config_changed(
            #{<<"mysql">> => Mysql0}, #{<<"mysql">> => Mysql1, <<"postgresql">> => BadPg}
        )
    ),
    ?assertEqual(Before, emqx_omp:configs()).

-doc "Reject an invalid Redis configuration without changing the running MySQL backend.".
t_invalid_redis_change(Config) ->
    Mysql0 = ?config(raw, Config),
    Before = emqx_omp:configs(),
    Mysql1 = Mysql0#{<<"message_read_pool_size">> => 3},
    BadRedis = #{
        <<"enable">> => true,
        <<"redis_type">> => <<"unknown">>,
        <<"servers">> => <<"redis:6379">>
    },
    ?assertMatch(
        {error, {invalid_config, _}},
        emqx_offline_messages:on_config_changed(
            #{<<"mysql">> => Mysql0}, #{<<"mysql">> => Mysql1, <<"redis">> => BadRedis}
        )
    ),
    ?assertEqual(Before, emqx_omp:configs()).

-doc "Convert default legacy tables in ID order and preserve their binary payloads.".
t_legacy_conversion(Config) ->
    Conn = ?config(db, Config),
    SQL = fun(Query) -> emqx_omp_mysql_sql:query(Conn, Query, []) end,
    {ok, _} = SQL("DROP TABLE IF EXISTS mqtt_msg, mqtt_sub"),
    {ok, _} = SQL("""
    CREATE TABLE mqtt_msg(id BIGINT PRIMARY KEY, msgid VARCHAR(64), topic VARCHAR(180),
        sender VARCHAR(64), qos TINYINT, retain TINYINT, payload BLOB, arrived DATETIME)
    """),
    {ok, _} = SQL("CREATE TABLE mqtt_sub(clientid VARCHAR(64), topic VARCHAR(180), qos TINYINT)"),
    %% Remove the simplified legacy tables, so that later suites can create the real ones.
    emqx_common_test_helpers:on_exit(fun() -> SQL("DROP TABLE mqtt_msg, mqtt_sub") end),
    {ok, _} = SQL("""
    INSERT INTO mqtt_msg VALUES
        (9, '', 'legacy/topic', 'p', 1, 0, X'00FF', '2026-11-06 00:00:00'),
        (3, '', 'legacy/topic', 'p', 2, 1, X'0102', '2026-11-06 00:00:00')
    """),
    {ok, _} = SQL(
        "INSERT INTO mqtt_sub VALUES ('legacy', 'legacy/topic', 2), ('legacy', 'legacy/#', 0)"
    ),
    {ok, Script} = file:read_file(
        filename:join(code:priv_dir(emqx_offline_messages), "mysql-convert-legacy.sql")
    ),
    Convert = fun() ->
        lists:foreach(
            fun(Statement) -> {ok, _} = SQL(Statement) end,
            [S || S <- binary:split(Script, <<";">>, [global]), string:trim(S) =/= <<>>]
        )
    end,
    %% Legacy rows store 2026-11-06 00:00:00 in the zone of the legacy connections.
    Midnight =
        (calendar:datetime_to_gregorian_seconds({{2026, 11, 6}, {0, 0, 0}}) - 62167219200) * 1000,
    %% The exact topic has two messages. The wildcard filter's topic row has none.
    TopicId = fun(Namespace) ->
        {ok, Topics} = emqx_omp_mysql_sql:topics(Conn, Namespace, 0, 10),
        [Id] = [Id || {Id, 3} <- Topics],
        Id
    end,
    PublishedAt = fun(Namespace) ->
        {ok, PageRows} = emqx_omp_mysql_sql:page(Conn, TopicId(Namespace), 0, 10),
        lists:usort([element(9, Row) || Row <- PageRows])
    end,
    %% Read legacy values in the server's global time zone by default.
    {ok, [{GlobalZone}]} = SQL("SELECT @@GLOBAL.time_zone"),
    {ok, _} = SQL("SET GLOBAL time_zone = '+03:00'"),
    emqx_common_test_helpers:on_exit(fun() ->
        SQL(["SET GLOBAL time_zone = '", GlobalZone, "'"])
    end),
    {ok, _} = SQL("SET @omp_namespace = 'converted'"),
    Convert(),
    ?assertEqual([Midnight - 3 * 3600000], PublishedAt(<<"converted">>)),
    {ok, Rows} = emqx_omp_mysql_sql:page(Conn, TopicId(<<"converted">>), 0, 10),
    ?assertEqual([<<1, 2>>, <<0, 255>>], [element(7, Row) || Row <- Rows]),
    %% The exact subscription uses QoS 1, and the wildcard subscription keeps its QoS.
    {ok, Subscriptions} = emqx_omp_mysql_sql:restore(Conn, <<"converted">>, <<"legacy">>),
    ?assertEqual(
        [{<<"legacy/#">>, #{<<"qos">> => 0}}, {<<"legacy/topic">>, #{<<"qos">> => 1}}],
        lists:sort([{Topic, emqx_utils_json:decode(Options)} || {Topic, Options} <- Subscriptions])
    ),
    %% Read legacy values in an explicitly selected source time zone.
    {ok, _} = SQL("SET @omp_namespace = 'converted-tz'"),
    {ok, _} = SQL("SET @omp_source_time_zone = '+08:00'"),
    Convert(),
    ?assertEqual([Midnight - 8 * 3600000], PublishedAt(<<"converted-tz">>)),
    {ok, [{2}]} = SQL("SELECT count(*) FROM mqtt_msg").

-doc "Fail self-provisioning without CREATE privileges and reject managed publishes afterward.".
t_provision_failure(Config) ->
    Conn = ?config(db, Config),
    SQL = fun(Query) -> emqx_omp_mysql_sql:query(Conn, Query, []) end,
    {ok, _} = SQL("DROP USER IF EXISTS 'omp_readonly'@'%'"),
    {ok, _} = SQL("CREATE USER 'omp_readonly'@'%' IDENTIFIED BY 'public'"),
    emqx_common_test_helpers:on_exit(fun() ->
        SQL("DROP USER 'omp_readonly'@'%'")
    end),
    {ok, _} = SQL("GRANT SELECT ON mqtt.* TO 'omp_readonly'@'%'"),
    %% Without the schema version, the plugin installs the schema again.
    {ok, _} = SQL("DELETE FROM mqtt_omp_schema"),
    Raw = ?config(raw, Config),
    ReadOnly = Raw#{<<"username">> => <<"omp_readonly">>},
    ok = emqx_omp:apply_changes([
        #config_change{
            id = mysql, module = emqx_omp_mysql_config, old = Raw, new = ReadOnly
        }
    ]),
    ?assertMatch({error, _}, emqx_omp:on_health_check(mysql, ReadOnly)),
    ?assertMatch(
        {error, disconnect, _},
        emqx_broker:publish2(
            emqx_message:make(<<"p">>, 1, <<"contract/no-schema">>, <<"not-stored">>)
        )
    ).

-doc "Fail the MySQL health check without the schema version and restore it on installation.".
t_missing_schema_version(Config) ->
    Conn = ?config(db, Config),
    #{namespace := Namespace} = Parsed = ?config(parsed, Config),
    Topic = <<"contract/schema-version">>,
    Msg = emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, <<"data">>)),
    {ok, {Id, 1}} = emqx_omp_mysql_sql:append(Conn, Namespace, Topic, [Msg]),
    %% A missing schema version fails the health check.
    {ok, _} = emqx_omp_mysql_sql:query(Conn, "DELETE FROM mqtt_omp_schema", []),
    ?assertException(error, _, emqx_omp_mysql_sql:health(Conn)),
    %% Installation restores the version and keeps the stored messages.
    ok = emqx_omp_mysql_sql:install(Parsed),
    ok = emqx_omp_mysql_sql:health(Conn),
    ?assertEqual({ok, {Id, 2}}, emqx_omp_mysql_sql:append(Conn, Namespace, Topic, [Msg])).

-doc "Store every byte value and SQL quote characters in MySQL binary fields unchanged.".
t_append_binary_values(Config) ->
    Conn = ?config(db, Config),
    #{namespace := Namespace, backend_id := BackendId} = ?config(parsed, Config),
    Topic = <<"contract/binary-values">>,
    Payload = <<<<Byte>> || Byte <- lists:seq(0, 255)>>,
    Sender = <<"p'\"\\;", 0>>,
    Props = #{'User-Property' => [{<<"k'\\">>, <<"v\";">>}]},
    Msg = emqx_message:set_header(
        properties, Props, emqx_message:make(Sender, 1, Topic, Payload)
    ),
    {ok, {Id, 1}} = emqx_omp_mysql_sql:append(
        Conn, Namespace, Topic, [emqx_omp_store:encode(Msg)]
    ),
    {ok, [Row]} = emqx_omp_mysql_sql:page(Conn, Id, 0, 1),
    ?assertEqual(Sender, element(4, Row)),
    ?assertEqual(Payload, element(7, Row)),
    {ok, Decoded} = emqx_omp_store:decode(Topic, {{BackendId, Namespace}, Id}, Row),
    ?assertEqual(Props, emqx_message:get_header(properties, Decoded)).

-doc "Reject SQL text in fields that must be base64, and store the same text as message data.".
t_append_sql_text(Config) ->
    Conn = ?config(db, Config),
    #{namespace := Namespace} = ?config(parsed, Config),
    Topic = <<"contract/sql-text">>,
    Text = <<"'); DROP TABLE mqtt_omp_message; -- \\">>,
    Msg = emqx_omp_store:encode(emqx_message:make(Text, 1, Topic, Text)),
    {ok, {Id, 1}} = emqx_omp_mysql_sql:append(Conn, Namespace, Topic, [Msg]),
    %% Text in a base64 field fails the append before any SQL is sent.
    lists:foreach(
        fun(Field) ->
            ?assertException(
                error, _, emqx_omp_mysql_sql:append(Conn, Namespace, Topic, [Msg#{Field := Text}])
            )
        end,
        [message_id, payload]
    ),
    {ok, Rows} = emqx_omp_mysql_sql:page(Conn, Id, 0, 10),
    ?assertMatch([{1, 1, _, Text, _, _, Text, _, _, _}], Rows),
    ?assertEqual({ok, {Id, 2}}, emqx_omp_mysql_sql:append(Conn, Namespace, Topic, [Msg])).

-doc "Release the topic row lock after a failed append and keep its offsets unused.".
t_append_failure_unlocks(Config) ->
    Conn = ?config(db, Config),
    #{namespace := Namespace} = Parsed = ?config(parsed, Config),
    {ok, Other} = emqx_omp_mysql_sql:connect(Parsed),
    emqx_common_test_helpers:on_exit(fun() -> emqx_omp_mysql_sql:close(Other) end),
    Topic = <<"contract/append-failure">>,
    Msg = emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, <<"data">>)),
    {ok, {Id, 1}} = emqx_omp_mysql_sql:append(Conn, Namespace, Topic, [Msg]),
    %% The CHECK constraint fails the insert after the topic row is locked and updated.
    ?assertException(error, _, emqx_omp_mysql_sql:append(Conn, Namespace, Topic, [Msg#{qos := 3}])),
    ?assertNot(mysql:in_transaction(Conn)),
    Lock = "SELECT next_offset FROM mqtt_omp_topic WHERE topic_id = ? FOR UPDATE NOWAIT",
    {ok, _} = emqx_omp_mysql_sql:query(Other, "BEGIN", []),
    ?assertEqual({ok, [{2}]}, emqx_omp_mysql_sql:query(Other, Lock, [Id])),
    {ok, _} = emqx_omp_mysql_sql:query(Other, "ROLLBACK", []),
    ?assertEqual({ok, {Id, 2}}, emqx_omp_mysql_sql:append(Conn, Namespace, Topic, [Msg])).

-doc "Fail MySQL installation after `query_timeout` while another connection installs.".
t_schema_lock_timeout(Config) ->
    Conn = ?config(db, Config),
    Raw = ?config(raw, Config),
    Parsed = parse(mysql, Raw#{<<"query_timeout">> => 1000}),
    %% Hold the installation lock as another node would during its installation.
    LockName = "SHA2(CONCAT(DATABASE(), ':mqtt_omp_schema'), 256)",
    {ok, [{1}]} = emqx_omp_mysql_sql:query(Conn, ["SELECT GET_LOCK(", LockName, ", 0)"], []),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_omp_mysql_sql:query(Conn, ["SELECT RELEASE_LOCK(", LockName, ")"], [])
    end),
    %% The installation waits about one second instead of thirty.
    {Time, Result} = timer:tc(fun() -> catch emqx_omp_mysql_sql:install(Parsed) end, millisecond),
    ?assertMatch({'EXIT', {schema_lock_timeout, _}}, Result),
    ?assert(Time < 5000).

-doc "Check overlap validation for every pair, including disabled and disjoint backends.".
t_all_backend_overlaps(_Config) ->
    Enabled = #{<<"enable">> => true, <<"topics">> => [<<"shared/#">>]},
    lists:foreach(
        fun({Left, Right}) ->
            ?assertMatch(
                {error, {overlapping_backend_topics, [_]}},
                emqx_offline_messages:validate_backends(#{Left => Enabled, Right => Enabled})
            ),
            ?assertEqual(
                ok,
                emqx_offline_messages:validate_backends(#{
                    Left => Enabled, Right => Enabled#{<<"topics">> := [<<"other/#">>]}
                })
            ),
            ?assertEqual(
                ok,
                emqx_offline_messages:validate_backends(#{
                    Left => Enabled, Right => Enabled#{<<"enable">> := false}
                })
            )
        end,
        [
            {<<"mysql">>, <<"postgresql">>},
            {<<"mysql">>, <<"redis">>},
            {<<"postgresql">>, <<"redis">>}
        ]
    ).

-doc "Round-trip MySQL defaults through Avro and preserve secure TLS connection options.".
t_config(_Config) ->
    PrivDir = code:priv_dir(emqx_offline_messages),
    {ok, Schema} = file:read_file(filename:join(PrivDir, "config_schema.avsc")),
    {ok, Defaults} = hocon:load(filename:join(PrivDir, "config.hocon")),
    Store = avro_schema_store:import_schema_json(
        <<"offline_messages">>, Schema, avro_schema_store:new([map])
    ),
    Encoded = avro_json_encoder:encode(Store, <<"offline_messages">>, Defaults),
    Options = avro:make_decoder_options([{map_type, map}, {record_type, map}, {encoding, avro_json}]),
    Decoded = avro_json_decoder:decode_value(Encoded, <<"offline_messages">>, Store, Options#{
        is_wrapped => false
    }),
    Parsed0 = parse(mysql, maps:get(<<"mysql">>, Defaults)),
    Parsed1 = parse(mysql, maps:get(<<"mysql">>, Decoded)),
    ?assertEqual(maps:remove(instance, Parsed0), maps:remove(instance, Parsed1)),
    #{connection := Connection} = parse(mysql, #{
        <<"server">> => <<"database.example.net:3306">>,
        <<"ssl">> => #{<<"enable">> => true}
    }),
    SSL = proplists:get_value(ssl, Connection),
    ?assertEqual(verify_peer, proplists:get_value(verify, SSL)),
    ?assertEqual("database.example.net", proplists:get_value(server_name_indication, SSL)).

raw(Id, Namespace) ->
    Common = #{
        <<"enable">> => true,
        <<"namespace">> => Namespace,
        <<"topics">> => [<<"contract/#">>],
        <<"init_default_schema">> => true,
        <<"message_publish_pool_size">> => 2,
        <<"message_read_pool_size">> => 2,
        <<"subscription_meta_pool_size">> => 2,
        <<"read">> => #{<<"page_max_messages">> => 2, <<"idle_poll_interval">> => 100},
        <<"checkpoint">> => #{<<"interval">> => 50, <<"max_pending">> => 1}
    },
    case Id of
        postgresql -> Common#{<<"host">> => <<"pgsql">>};
        mysql -> Common#{<<"server">> => <<"mysql:3306">>};
        mariadb -> Common#{<<"server">> => <<"mariadb:3306">>}
    end.

parse(postgresql, Raw) -> emqx_omp_pg_config:parse(Raw);
parse(Id, Raw) when Id =:= mysql; Id =:= mariadb -> emqx_omp_mysql_config:parse(Raw).

publish_loop(Parent, Topic, Published, Rejected) ->
    receive
        stop -> Parent ! {published, Published, Rejected}
    after 0 ->
        Msg = emqx_message:make(<<"mover">>, 1, Topic, integer_to_binary(Published)),
        case emqx_broker:publish2(Msg) of
            {error, disconnect, _} -> publish_loop(Parent, Topic, Published + 1, Rejected + 1);
            _ -> publish_loop(Parent, Topic, Published + 1, Rejected)
        end
    end.

count_messages(Id, Raw, Topic) ->
    #{backend := Backend, namespace := Namespace} = Parsed = parse(Id, Raw),
    {ok, Conn} = Backend:connect(Parsed),
    SQL =
        case Id of
            postgresql ->
                """
                SELECT count(*) FROM mqtt_pg_message JOIN mqtt_pg_topic USING (topic_id)
                WHERE namespace = $1 AND topic = $2
                """;
            mysql ->
                """
                SELECT count(*) FROM mqtt_omp_message JOIN mqtt_omp_topic USING (topic_id)
                WHERE namespace = ? AND topic = ?
                """
        end,
    {ok, [{Count}]} = Backend:query(Conn, SQL, [Namespace, Topic]),
    ok = Backend:close(Conn),
    Count.

cursor(Config, ClientId, Topic) ->
    Backend = ?config(backend, Config),
    #{namespace := Namespace} = ?config(parsed, Config),
    {ok, #{cursor := Cursor}} = Backend:subscribe(
        ?config(db, Config), Namespace, ClientId, Topic, #{qos => 1}, earliest
    ),
    Cursor.

client(Config, Id, Opts) -> emqx_offline_messages_pg_SUITE:client(Config, Id, Opts).
receive_messages(N) -> emqx_offline_messages_pg_SUITE:receive_messages(N).
payloads(Messages) -> emqx_offline_messages_pg_SUITE:payloads(Messages).
no_messages() -> emqx_offline_messages_pg_SUITE:no_messages().

encoded(Topic, Payloads) ->
    [
        emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, integer_to_binary(Payload)))
     || Payload <- Payloads
    ].

-doc "Return the stored offsets and payloads of a MySQL topic in the test namespace.".
stored(Config, Topic) ->
    Backend = ?config(backend, Config),
    #{namespace := Namespace} = ?config(parsed, Config),
    {ok, Rows} = Backend:query(?config(db, Config), """
    SELECT m.msg_offset, m.payload FROM mqtt_omp_message m JOIN mqtt_omp_topic t USING (topic_id)
    WHERE t.namespace = ? AND t.topic = ? ORDER BY m.msg_offset
    """, [Namespace, Topic]),
    Rows.

offsets(Config, Topic) ->
    [Offset || {Offset, _} <- stored(Config, Topic)].

flush_count(ClientId) ->
    length([
        Event
     || #{clientid := EventClientId} = Event <- ?of_kind(
            omp_checkpoint_flush, snabbkaffe:collect_trace(0)
        ),
        EventClientId =:= ClientId
    ]).

qos_payloads(Messages) ->
    lists:sort([{QoS, Payload} || #{qos := QoS, payload := Payload} <- Messages]).

is_subscribed(ClientId, Filter) ->
    lists:any(
        fun(Pid) -> emqx_broker:subscribed(Pid, Filter) end, emqx_cm:lookup_channels(ClientId)
    ).
