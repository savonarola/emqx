%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_offline_messages.hrl").
-include("emqx_omp.hrl").

all() -> [{group, postgresql}, {group, mysql}, {group, mariadb}].

groups() ->
    Cases = emqx_common_test_helpers:all(?MODULE),
    [{Id, [], Cases} || Id <- [postgresql, mysql, mariadb]].

init_per_group(Id, Config) ->
    init_sql_group(Id, Config).

end_per_group(_Id, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_sql_group(Id, Config) ->
    [{sql_fixture, Id} | start_broker(Config)].

init_per_testcase(Case, Config) ->
    init_sql_case(Case, ?config(sql_fixture, Config), Config).

init_sql_case(Case, Id, Config) ->
    Suffix = integer_to_binary(erlang:system_time(microsecond)),
    Namespace = <<(atom_to_binary(Case))/binary, "-", Suffix/binary>>,
    Raw = raw(Id, Namespace),
    Parsed = parse(Id, Raw),
    #{backend := Backend} = Parsed,
    ok = Backend:install(Parsed),
    {ok, Conn} = Backend:connect(Parsed),
    ok = emqx_omp:start(Parsed),
    ok = snabbkaffe:start_trace(),
    [
        {namespace, Namespace},
        {raw, Raw},
        {parsed, Parsed},
        {backend, Backend},
        {db, Conn}
        | Config
    ].

end_per_testcase(_Case, Config) ->
    end_sql_case(Config).

end_sql_case(Config) ->
    emqx_common_test_helpers:call_janitor(),
    meck:unload(),
    snabbkaffe:stop(),
    ok = emqx_omp:stop(mysql),
    ok = emqx_omp:stop(postgresql),
    Backend = ?config(backend, Config),
    Backend:close(?config(db, Config)).

backend_id(Config) ->
    #{backend_id := Id} = ?config(parsed, Config),
    Id.

other_backend(Config) ->
    case backend_id(Config) of
        postgresql -> mysql;
        mysql -> postgresql
    end.

config_module(Config) ->
    case backend_id(Config) of
        postgresql -> emqx_omp_pg_config;
        mysql -> emqx_omp_mysql_config
    end.

bad_endpoint(Config, Raw) ->
    case backend_id(Config) of
        postgresql -> Raw#{<<"port">> => 1};
        mysql -> Raw#{<<"server">> => <<"mysql:1">>}
    end.

messages(Config, Topic) ->
    SQL = """
    SELECT m.msg_offset, m.payload FROM mqtt_pg_message m JOIN mqtt_pg_topic t USING (topic_id)
    WHERE t.namespace = $1 AND t.topic = $2 ORDER BY m.msg_offset
    """,
    sql(Config, ?config(db, Config), SQL, [?config(namespace, Config), Topic]).

stored(Config, Topic) -> messages(Config, Topic).

offsets(Config, Topic) ->
    [Offset || {Offset, _} <- messages(Config, Topic)].

offsets(Messages) ->
    [Offset || #message{headers = #{omp_position := {_, _, Offset}}} <- Messages].

cursor(Config, ClientId, Topic) ->
    Backend = ?config(backend, Config),
    Result = Backend:subscription_cursor(
        ?config(db, Config), ?config(namespace, Config), ClientId, Topic
    ),
    case Result of
        {ok, #{cursor := Cursor}} -> Cursor;
        {ok, not_found} -> undefined
    end.

delete_subscriptions(Config) ->
    SQL = "DELETE FROM mqtt_pg_subscription WHERE namespace = $1",
    sql(Config, ?config(db, Config), SQL, [?config(namespace, Config)]).

sql(Config, Conn, PostgreSQL, Params) ->
    Backend = ?config(backend, Config),
    SQL =
        case Backend of
            emqx_omp_pg_sql ->
                PostgreSQL;
            emqx_omp_mysql_sql ->
                %% The test SQL uses parameters in ascending order with no repeats.
                Renamed = binary:replace(
                    iolist_to_binary(PostgreSQL), <<"mqtt_pg_">>, <<"mqtt_omp_">>, [global]
                ),
                re:replace(Renamed, "\\$[1-9][0-9]*", "?", [global, {return, binary}])
        end,
    {ok, Result} = Backend:query(Conn, SQL, Params),
    Result.

start_broker(Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{
                config => #{
                    authorization => #{no_match => allow},
                    listeners => #{
                        tcp => #{default => #{bind => Port}},
                        ssl => #{default => #{bind => 0}},
                        ws => #{default => #{bind => 0}},
                        wss => #{default => #{bind => 0}}
                    }
                }
            }},
            {emqx, #{config => false}},
            emqx_extsub,
            emqx_offline_messages
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps}, {port, Port} | Config].

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
        {error, blocked, _},
        emqx_broker:publish2(
            emqx_message:make(<<"p">>, 1, <<"contract/failure">>, <<"not-routed">>),
            #{hook_prohibition_as_error => true}
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
    {ok, {OtherId, 1}} = Backend:append(Conn, <<Namespace/binary, "-other">>, Topic, [Msg]),
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

-doc "Save and delete wildcard subscriptions in the current namespace on explicit requests.".
t_wildcard_config_change(Config) ->
    ClientId = <<"wildcard-config">>,
    Filter = <<"contract/config/#">>,
    Backend = ?config(backend, Config),
    Conn = ?config(db, Config),
    #{namespace := Namespace, backend_id := BackendId} = ?config(parsed, Config),
    Raw0 = ?config(raw, Config),
    Sub = client(Config, ClientId, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Filter, 1),
    ?assertMatch({ok, [{Filter, _}]}, Backend:restore(Conn, Namespace, ClientId)),
    NewNamespace = <<Namespace/binary, "-replacement">>,
    ok = emqx_omp:start(parse(BackendId, Raw0#{<<"namespace">> := NewNamespace})),
    ?assertEqual({ok, []}, Backend:restore(Conn, NewNamespace, ClientId)),
    {ok, _, [1]} = emqtt:subscribe(Sub, Filter, 1),
    ?assertMatch({ok, [{Filter, _}]}, Backend:restore(Conn, NewNamespace, ClientId)),
    {ok, _, _} = emqtt:unsubscribe(Sub, Filter),
    ?assertEqual({ok, []}, Backend:restore(Conn, NewNamespace, ClientId)),
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
    delete_subscriptions(Config),
    ?retry(50, 100, ?assertEqual([], offsets(Config, Topic))),
    ?assertEqual({ok, 4}, Backend:consumed_cutoff(Conn, TopicId, 4)).

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

raw(Id, Namespace) ->
    Common = #{
        <<"enable">> => true,
        <<"namespace">> => Namespace,
        <<"topics">> => [<<"contract/#">>, <<"offline/#">>],
        <<"init_default_schema">> => true,
        <<"message_publish_pool_size">> => 2,
        <<"message_read_pool_size">> => 2,
        <<"subscription_meta_pool_size">> => 2,
        <<"read">> => #{<<"page_max_messages">> => 2, <<"idle_poll_interval">> => 100},
        <<"checkpoint">> => #{<<"interval">> => 50, <<"max_pending">> => 2}
    },
    case Id of
        postgresql -> Common#{<<"host">> => <<"pgsql">>};
        mysql -> Common#{<<"server">> => <<"mysql:3306">>};
        mariadb -> Common#{<<"server">> => <<"mariadb:3306">>}
    end.

parse(postgresql, Raw) -> emqx_omp_pg_config:parse(Raw);
parse(Id, Raw) when Id =:= mysql; Id =:= mariadb -> emqx_omp_mysql_config:parse(Raw).

encoded(Topic, Payloads) ->
    [
        emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, integer_to_binary(Payload)))
     || Payload <- Payloads
    ].

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

-doc "Check that stored QoS 1/2 publishes are acknowledged and sent as one live term after commit.".
t_publish_success(Config) ->
    %% Subscribe to live terms and publish at both supported persistence QoS levels.
    Topic = <<"offline/success">>,
    BackendId = backend_id(Config),
    Namespace = ?config(namespace, Config),
    ok = emqx:subscribe(Topic),
    Pub = client(Config, <<"publisher">>, []),
    {ok, #{reason_code := 0}} = emqtt:publish(Pub, Topic, <<"one">>, 1),
    {ok, #{reason_code := 0}} = emqtt:publish(Pub, Topic, <<"two">>, 2),
    ?assertEqual([{1, <<"one">>}, {2, <<"two">>}], messages(Config, Topic)),
    ?assertReceive(#dispatch_info{
        topic = Topic,
        info = #omp_live{
            namespace = {BackendId, Namespace},
            offset = 1,
            messages = [#message{payload = <<"one">>}]
        }
    }),
    ?assertReceive(#dispatch_info{
        topic = Topic,
        info = #omp_live{
            namespace = {BackendId, Namespace},
            offset = 2,
            messages = [#message{payload = <<"two">>}]
        }
    }),
    %% The broker does not route the original message to subscribers.
    ?assertNotReceive({deliver, Topic, _}),
    ok = emqx:unsubscribe(Topic),
    ok = emqtt:disconnect(Pub).

-doc "Check that QoS 0 uses regular dispatch, keeps hook changes and creates no PostgreSQL rows.".
t_qos0_publish(Config) ->
    %% Observe the transformed publish and its regular broker delivery.
    Topic = <<"offline/continuation">>,
    Action = {?MODULE, transform_publish, []},
    ok = emqx_hooks:add('message.publish', Action, 0),
    emqx_common_test_helpers:on_exit(fun() -> emqx_hooks:del('message.publish', Action) end),
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    Before = emqx_metrics:val_global('messages.publish'),
    {ok, _, _} = emqx_broker:publish2(emqx_message:make(<<"p">>, 0, Topic, <<"original">>), #{}),
    %% Check that persistence neither intercepts nor duplicates the message.
    ?assertReceive({deliver, Topic, #message{qos = 0, payload = <<"transformed">>}}),
    ?assertNotReceive(#dispatch_info{info = #omp_live{}}),
    ?assertEqual([], messages(Config, Topic)),
    ?assertEqual(Before + 1, emqx_metrics:val_global('messages.publish')),
    ?assertMatch(
        [#{topic := Topic}],
        [
            Event
         || #{topic := EventTopic} = Event <-
                ?of_kind(emqx_trace_publish, snabbkaffe:collect_trace()),
            EventTopic =:= Topic
        ]
    ).

-doc "Check that publishes rejected or disconnected by earlier hooks are not stored at any QoS.".
t_rejected_publish_not_stored(Config) ->
    %% Reject messages before the persistence hook at every publish QoS.
    Topic = <<"offline/rejected">>,
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    lists:foreach(
        fun(QoS) ->
            Msg = emqx_message:make(<<"p">>, QoS, Topic, <<"rejected">>),
            ?assertMatch(
                {ok, [], _},
                emqx_broker:publish2(emqx_message:set_header(allow_publish, false, Msg))
            ),
            ?assertMatch(
                {error, disconnect, _},
                emqx_broker:publish2(emqx_message:set_header(should_disconnect, true, Msg))
            )
        end,
        [0, 1, 2]
    ),
    %% Accept a later QoS 1 publish and check that it receives the first stored offset.
    {ok, [persisted], _} = emqx_broker:publish2(
        emqx_message:make(<<"p">>, 1, Topic, <<"accepted">>)
    ),
    ?assertReceive(#dispatch_info{
        topic = Topic, info = #omp_live{offset = 1, messages = [#message{payload = <<"accepted">>}]}
    }),
    ?assertEqual([{1, <<"accepted">>}], messages(Config, Topic)).

transform_publish(Msg) ->
    {ok, Msg#message{payload = <<"transformed">>}}.

-doc "QoS 0 bypasses the asynchronous writer even when the SQL runtime is stopped.".
t_qos0_without_runtime(Config) ->
    %% Enable batching and stop only the runtime so the publish hook remains active.
    Raw = ?config(raw, Config),
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = Raw,
            new = Raw#{<<"async_batched">> => true}
        }
    ]),
    ok = emqx_omp_sup:stop(backend_id(Config)),
    Topic = <<"offline/qos0-without-runtime">>,
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    %% Check regular delivery without a storage write or live batch.
    {ok, _, _} = emqx_broker:publish2(emqx_message:make(<<"p">>, 0, Topic, <<"ordinary">>)),
    ?assertReceive({deliver, Topic, #message{qos = 0, payload = <<"ordinary">>}}),
    ?assertNotReceive(#dispatch_info{info = #omp_live{}}),
    ?assertEqual([], messages(Config, Topic)).

-doc "Check ordered pagination and that each client's cursor follows its own acknowledgements.".
t_independent_progress(Config) ->
    %% Store more messages than the slow client's receive maximum and connect both subscribers.
    Topic = <<"offline/progress">>,
    append(Config, Topic, [integer_to_binary(N) || N <- lists:seq(1, 6)]),
    Slow = client(Config, <<"slow">>, [
        {auto_ack, false}, {properties, #{'Receive-Maximum' => 4}}
    ]),
    Fast = client(Config, <<"fast">>, []),
    {ok, _, [1]} = emqtt:subscribe(Slow, Topic, 1),
    SlowMessages = receive_messages(4),
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>, <<"4">>], payloads(SlowMessages)),
    {ok, _, [1]} = emqtt:subscribe(Fast, Topic, 1),
    ?assertEqual([integer_to_binary(N) || N <- lists:seq(1, 6)], payloads(receive_messages(6))),
    ?retry(50, 100, ?assertEqual(6, cursor(Config, <<"fast">>, Topic))),
    ?assertEqual(0, cursor(Config, <<"slow">>, Topic)),
    %% Acknowledge the slow client's inflight messages and check that its cursor moves independently.
    [First | Later] = SlowMessages,
    emqtt:puback(Slow, maps:get(packet_id, First)),
    ?retry(50, 100, ?assertEqual(1, cursor(Config, <<"slow">>, Topic))),
    lists:foreach(fun(#{packet_id := Id}) -> emqtt:puback(Slow, Id) end, Later),
    Last = receive_messages(2),
    ?assertEqual([<<"5">>, <<"6">>], payloads(Last)),
    lists:foreach(fun(#{packet_id := Id}) -> emqtt:puback(Slow, Id) end, Last),
    ?retry(50, 100, ?assertEqual(6, cursor(Config, <<"slow">>, Topic))),
    ?assertEqual(6, length(messages(Config, Topic))),
    ok = emqtt:disconnect(Slow),
    ok = emqtt:disconnect(Fast).

-doc "Check clean-start restoration, repeated subscribe and removal by explicit unsubscribe.".
t_restore_and_unsubscribe(Config) ->
    %% Acknowledge one message and repeat SUBSCRIBE without replaying it.
    Topic = <<"offline/restore">>,
    Sub0 = client(Config, <<"restored">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub0, Topic, 1),
    append(Config, Topic, [<<"first">>]),
    ?assertEqual([<<"first">>], payloads(receive_messages(1))),
    ?retry(50, 100, ?assertEqual(1, cursor(Config, <<"restored">>, Topic))),
    {ok, _, [1]} = emqtt:subscribe(Sub0, Topic, 1),
    no_messages(),
    ok = emqtt:disconnect(Sub0),
    %% Reconnect with clean start and recover the message published while disconnected.
    append(Config, Topic, [<<"second">>]),
    Sub1 = client(Config, <<"restored">>, [{clean_start, true}]),
    ?assertEqual([<<"second">>], payloads(receive_messages(1))),
    %% Unsubscribe explicitly and verify that the next connection restores nothing.
    {ok, _, _} = emqtt:unsubscribe(Sub1, Topic),
    ?retry(50, 100, ?assertEqual(undefined, cursor(Config, <<"restored">>, Topic))),
    ok = emqtt:disconnect(Sub1),
    Sub2 = client(Config, <<"restored">>, []),
    no_messages(),
    ok = emqtt:disconnect(Sub2).

-doc "Check that a failed or empty append rolls back the counter and message rows.".
t_append_failure(Config) ->
    %% Fail appends with invalid QoS and an empty batch.
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/invalid">>,
    Message = emqx_omp_store:encode(emqx_message:make(<<"p">>, 1, Topic, <<"data">>)),
    ?assertException(
        error, _, (?config(backend, Config)):append(Conn, Namespace, Topic, [Message#{qos := 3}])
    ),
    ?assertException(error, _, (?config(backend, Config)):append(Conn, Namespace, Topic, [])),
    %% Check that rollback leaves the first offset available for a valid append.
    ?assertEqual([], messages(Config, Topic)),
    ?assertMatch({ok, {_, 1}}, store_append(Conn, Config, Topic, [<<"valid">>])).

-doc "Check that QoS 1/2 share a stored sequence while QoS 0 uses regular dispatch.".
t_mixed_qos(Config) ->
    %% Subscribe without automatic acknowledgements to observe stored progress.
    Topic = <<"offline/mixed">>,
    Sub = client(Config, <<"mixed">>, [{auto_ack, false}]),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    Pub = client(Config, <<"mixed-pub">>, []),
    {ok, _} = emqtt:publish(Pub, Topic, <<"one">>, 1),
    [#{packet_id := First, qos := 1, payload := <<"one">>}] = receive_messages(1),
    ok = emqtt:publish(Pub, Topic, <<"zero">>, 0),
    [#{qos := 0, payload := <<"zero">>}] = receive_messages(1),
    {ok, _} = emqtt:publish(Pub, Topic, <<"two">>, 2),
    [#{packet_id := Second, qos := 1, payload := <<"two">>}] = receive_messages(1),
    %% Only QoS 1/2 consume offsets in SQL.
    ?assertEqual(
        [{1, 1}, {2, 2}],
        sql(
            Config,
            ?config(db, Config),
            "SELECT m.msg_offset, m.qos FROM mqtt_pg_message m JOIN mqtt_pg_topic t USING (topic_id) WHERE t.namespace = $1 ORDER BY 1",
            [?config(namespace, Config)]
        )
    ),
    %% The checkpoint waits for the in-order acknowledgements.
    ?assertEqual(0, cursor(Config, <<"mixed">>, Topic)),
    lists:foreach(fun(PacketId) -> emqtt:puback(Sub, PacketId) end, [First, Second]),
    ?retry(50, 100, ?assertEqual(2, cursor(Config, <<"mixed">>, Topic))),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

-doc "Check that managed topics accept only QoS 1 subscriptions and keep QoS 1 with `upgrade_qos`.".
t_delivery_qos(Config) ->
    %% Reject subscriptions at QoS 0 and QoS 2.
    Topic = <<"offline/qos">>,
    Sub0 = client(Config, <<"qos-denied">>, []),
    ?assertMatch({ok, _, [16#87]}, emqtt:subscribe(Sub0, Topic, 0)),
    ?assertMatch({ok, _, [16#87]}, emqtt:subscribe(Sub0, Topic, 2)),
    ok = emqtt:disconnect(Sub0),
    %% Enable QoS upgrade and verify that stored delivery remains QoS 1.
    Mqtt = emqx_config:get_zone_conf(default, [mqtt]),
    ok = emqx_config:put_zone_conf(default, [mqtt], Mqtt#{upgrade_qos := true}),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_config:put_zone_conf(default, [mqtt], Mqtt)
    end),
    Pub = client(Config, <<"qos-pub">>, []),
    {ok, _} = emqtt:publish(Pub, Topic, <<"one">>, 1),
    ?assertEqual([{1, <<"one">>}], messages(Config, Topic)),
    Sub1 = client(Config, <<"qos-sub">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub1, Topic, 1),
    ?assertMatch([#{qos := 1, payload := <<"one">>}], receive_messages(1)),
    {ok, _} = emqtt:publish(Pub, Topic, <<"two">>, 2),
    ?assertMatch([#{qos := 1, payload := <<"two">>}], receive_messages(1)),
    ok = emqtt:disconnect(Sub1),
    ok = emqtt:disconnect(Pub).

-doc "Check that no-local, expired, malformed and rejected messages do not block later deliveries.".
t_settlement(Config) ->
    %% Store no-local, expired, malformed and deliverable messages in one sequence.
    Topic = <<"offline/settlement">>,
    Conn = ?config(db, Config),
    Now = erlang:system_time(millisecond),
    Own = emqx_message:make(<<"settler">>, 1, Topic, <<"own">>),
    Expired = Own#message{
        from = <<"publisher">>,
        timestamp = Now - 5000,
        headers = #{properties => #{'Message-Expiry-Interval' => 1}}
    },
    {ok, {_, 1}} = (?config(backend, Config)):append(Conn, ?config(namespace, Config), Topic, [
        emqx_omp_store:encode(Msg)
     || Msg <- [Own, Expired]
    ]),
    append(Config, Topic, [<<"invalid">>, <<"rejected">>, <<"valid">>]),
    sql(
        Config,
        Conn,
        "UPDATE mqtt_pg_message SET properties = '{}' WHERE msg_offset = 3 AND topic_id IN (SELECT topic_id FROM mqtt_pg_topic WHERE namespace = $1)",
        [?config(namespace, Config)]
    ),
    %% Reject one delivery and acknowledge the next to acknowledge every preceding offset.
    Sub = client(Config, <<"settler">>, [{auto_ack, false}]),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, [{qos, 1}, {nl, true}]),
    [#{packet_id := Rejected}] = receive_messages(1),
    emqtt:puback(Sub, Rejected, 16#80, #{}),
    [#{packet_id := Valid, payload := <<"valid">>}] = receive_messages(1),
    emqtt:puback(Sub, Valid),
    ?retry(50, 100, ?assertEqual(5, cursor(Config, <<"settler">>, Topic))),
    ok = emqtt:disconnect(Sub).

-doc "Check that replay skips rows deleted by count retention or by hand.".
t_retention_and_gaps(Config) ->
    %% Create stored history and remove rows through retention and a manual deletion.
    Topic = <<"offline/gaps">>,
    Conn = ?config(db, Config),
    {ok, _} = (?config(backend, Config)):subscribe(
        Conn, ?config(namespace, Config), <<"gaps">>, Topic, #{qos => 1}, earliest
    ),
    {ok, {Id, 1}} = store_append(
        Conn, Config, Topic, [integer_to_binary(N) || N <- lists:seq(1, 5)]
    ),
    ?assertEqual({ok, 2}, (?config(backend, Config)):trim(Conn, Id, 3, 1000)),
    ?assertEqual([{3, <<"3">>}, {4, <<"4">>}, {5, <<"5">>}], messages(Config, Topic)),
    sql(
        Config,
        Conn,
        "DELETE FROM mqtt_pg_message WHERE msg_offset = 4 AND topic_id IN (SELECT topic_id FROM mqtt_pg_topic WHERE namespace = $1)",
        [?config(namespace, Config)]
    ),
    %% Restore the subscription and verify that replay skips the deleted offsets.
    Sub = client(Config, <<"gaps">>, []),
    ?assertEqual([<<"3">>, <<"5">>], payloads(receive_messages(2))),
    ?retry(50, 100, ?assertEqual(5, cursor(Config, <<"gaps">>, Topic))),
    ok = emqtt:disconnect(Sub).

-doc "Check that a `latest` subscription skips history and receives only later messages.".
t_latest_start(Config) ->
    %% Create a latest subscription after an existing message.
    Topic = <<"offline/latest">>,
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    append(Config, Topic, [<<"old">>]),
    {ok, #{cursor := 1}} =
        (?config(backend, Config)):subscribe(
            Conn, Namespace, <<"latest">>, Topic, #{qos => 1}, latest
        ),
    Sub = client(Config, <<"latest">>, []),
    %% Deliver only the message appended after the subscription's starting position.
    append(Config, Topic, [<<"new">>]),
    ?assertEqual([<<"new">>], payloads(receive_messages(1))),
    ok = emqtt:disconnect(Sub).

-doc """
Check that unsupported managed subscriptions are denied, wildcard filters are accepted at any
QoS, and shared ones receive nothing.
""".
t_subscription_rejections(Config) ->
    %% Reject unsupported managed filters while allowing an unrelated QoS 0 subscription.
    Sub = client(Config, <<"denied">>, []),
    lists:foreach(
        fun({Topic, QoS}) ->
            ?assertMatch({ok, _, [16#87]}, emqtt:subscribe(Sub, Topic, QoS))
        end,
        [
            {<<"offline/", (binary:copy(<<"x">>, 1024))/binary>>, 1},
            {<<"offline/exact">>, 0},
            {<<"offline/exact">>, 2}
        ]
    ),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(Sub, <<"ordinary/topic">>, 0)),
    ?assertMatch({ok, _, [1]}, emqtt:subscribe(Sub, <<"offline/exact">>, 1)),
    %% Accept wildcard filters over managed topics for live delivery.
    ?assertMatch({ok, _, [1]}, emqtt:subscribe(Sub, <<"offline/#">>, 1)),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(Sub, <<"#">>, 0)),
    ok = emqtt:disconnect(Sub),
    %% Authorization cannot see the share prefix. The subscription succeeds and gets no messages.
    Shared = client(Config, <<"shared">>, []),
    ?assertMatch({ok, _, [1]}, emqtt:subscribe(Shared, <<"$share/group/offline/shared">>, 1)),
    append(Config, <<"offline/shared">>, [<<"history">>]),
    Pub = client(Config, <<"shared-pub">>, []),
    {ok, _} = emqtt:publish(Pub, <<"offline/shared">>, <<"live">>, 1),
    no_messages(),
    ok = emqtt:disconnect(Shared),
    ok = emqtt:disconnect(Pub).

-doc "Check that binary correlation data and duplicate user properties survive SQL encoding.".
t_properties(Config) ->
    %% Store binary payload data and MQTT properties through the SQL encoding.
    Topic = <<"offline/properties">>,
    Props = #{
        'Correlation-Data' => <<0, 255, 128>>,
        'Content-Type' => <<"application/octet-stream">>,
        'User-Property' => [{<<"a">>, <<"1">>}, {<<"a">>, <<"2">>}]
    },
    Msg = emqx_message:set_header(
        properties, Props, emqx_message:make(<<"p">>, 1, Topic, <<0, 255>>)
    ),
    {ok, {_, 1}} = (?config(backend, Config)):append(
        ?config(db, Config), ?config(namespace, Config), Topic, [
            emqx_omp_store:encode(Msg)
        ]
    ),
    Sub = client(Config, <<"properties">>, []),
    %% Replay the stored message and compare the decoded properties.
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    [#{payload := Payload, properties := Received}] = receive_messages(1),
    ?assertEqual(<<0, 255>>, Payload),
    ?assertEqual(Props, maps:with(maps:keys(Props), Received)),
    ok = emqtt:disconnect(Sub).

-doc "Check that PUBACK waits while another transaction holds the topic counter lock.".
t_ack_after_commit(Config) ->
    %% Lock the topic counter before the MQTT publisher tries to append.
    Topic = <<"offline/commit">>,
    append(Config, Topic, [<<"initial">>]),
    Pub = client(Config, <<"commit-publisher">>, []),
    Conn = ?config(db, Config),
    sql(Config, Conn, "BEGIN", []),
    sql(
        Config,
        Conn,
        "SELECT topic_id FROM mqtt_pg_topic WHERE namespace = $1 AND topic = $2 FOR UPDATE",
        [?config(namespace, Config), Topic]
    ),
    Parent = self(),
    spawn_link(fun() -> Parent ! {published, emqtt:publish(Pub, Topic, <<"committed">>, 1)} end),
    receive
        {published, _} -> ct:fail(ack_before_commit)
    after 100 -> ok
    end,
    ?assertEqual([{1, <<"initial">>}], messages(Config, Topic)),
    %% Release the lock and check that the acknowledgement follows the stored row.
    sql(Config, Conn, "COMMIT", []),
    receive
        {published, Result} -> ?assertMatch({ok, #{reason_code := 0}}, Result)
    after 5000 -> ct:fail(no_ack)
    end,
    ?assertEqual([{1, <<"initial">>}, {2, <<"committed">>}], messages(Config, Topic)),
    ok = emqtt:disconnect(Pub).

-doc "Check that `async_batched` acknowledges before commit and dispatches one batch after commit.".
t_async_ack_before_commit(Config) ->
    %% Buffer two messages in one batch while a separate transaction holds the topic lock.
    Raw = ?config(raw, Config),
    Writer = #{
        <<"pool_size">> => 1,
        <<"min_flush_timeout">> => 60000,
        <<"max_flush_timeout">> => 60000,
        <<"flush_max_messages">> => 2
    },
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = Raw,
            new = Raw#{<<"async_batched">> => true, <<"writer">> => Writer}
        }
    ]),
    Topic = <<"offline/async-commit">>,
    append(Config, Topic, [<<"initial">>]),
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    Pub = client(Config, <<"async-publisher">>, []),
    Conn = ?config(db, Config),
    sql(Config, Conn, "BEGIN", []),
    sql(
        Config,
        Conn,
        "SELECT topic_id FROM mqtt_pg_topic WHERE namespace = $1 AND topic = $2 FOR UPDATE",
        [?config(namespace, Config), Topic]
    ),
    %% Both MQTT handshakes finish before the lock is released.
    ?assertMatch({ok, #{reason_code := 0}}, emqtt:publish(Pub, Topic, <<"one">>, 1)),
    ?assertMatch({ok, #{reason_code := 0}}, emqtt:publish(Pub, Topic, <<"two">>, 2)),
    ?assertEqual([{1, <<"initial">>}], messages(Config, Topic)),
    ?assertNotReceive(#dispatch_info{info = #omp_live{}}),
    %% Commit the lock transaction and verify one dispatch for the committed batch.
    sql(Config, Conn, "COMMIT", []),
    ?assertReceive(
        #dispatch_info{
            topic = Topic,
            info = #omp_live{
                offset = 2,
                messages = [
                    #message{payload = <<"one">>}, #message{payload = <<"two">>}
                ]
            }
        }
    ),
    ?assertEqual([{1, <<"initial">>}, {2, <<"one">>}, {3, <<"two">>}], messages(Config, Topic)),
    ?assertNotReceive(#dispatch_info{info = #omp_live{}}),
    ok = emqtt:disconnect(Pub).

-doc "Check that authorization mounts the topic once with and without `include_mountpoint`.".
t_authorize_include_mountpoint(Config) ->
    %% Manage a mounted topic filter and prepare an unsupported QoS 0 subscribe action.
    Raw = ?config(raw, Config),
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = Raw,
            new = Raw#{<<"topics">> => [<<"tenant/offline/#">>]}
        }
    ]),
    ClientInfo = #{protocol => mqtt, mountpoint => <<"tenant/">>},
    Subscribe = #{action_type => subscribe, qos => 0},
    Authorize = fun(Topic) ->
        emqx_omp:on_authorize(ClientInfo, Subscribe, Topic, #{result => allow})
    end,
    Authz = emqx_config:get([authorization]),
    emqx_common_test_helpers:on_exit(fun() -> emqx_config:put([authorization], Authz) end),
    %% Without the option, the plugin adds the mountpoint to the client topic.
    emqx_config:put([authorization], Authz#{include_mountpoint => false}),
    ?assertMatch({stop, #{result := deny}}, Authorize(<<"offline/a">>)),
    %% With the option, the broker has already added the mountpoint.
    emqx_config:put([authorization], Authz#{include_mountpoint => true}),
    ?assertMatch({stop, #{result := deny}}, Authorize(<<"tenant/offline/a">>)).

-doc "Check that a failed start keeps the hooks and rejects managed QoS 1 publishes.".
t_start_failure(Config) ->
    %% Keep the hooks active while starting against an unreachable database port.
    Raw = ?config(raw, Config),
    Bad = bad_endpoint(Config, Raw#{<<"init_default_schema">> => false}),
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config), module = config_module(Config), old = Raw, new = Bad
        }
    ]),
    ?assertMatch({error, _}, emqx_omp:on_health_check(backend_id(Config), Bad)),
    %% Check that a failed publish neither disconnects nor routes the message.
    Topic = <<"offline/start-failure">>,
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    Pub = client(Config, <<"start-failure">>, []),
    ?assertMatch(
        {ok, #{reason_code := ?RC_IMPLEMENTATION_SPECIFIC_ERROR}},
        emqtt:publish(Pub, Topic, <<"lost">>, 1)
    ),
    ?assertEqual(pong, emqtt:ping(Pub)),
    ok = emqtt:disconnect(Pub),
    ?assertNotReceive({deliver, Topic, _}),
    ?assertNotReceive(#dispatch_info{info = #omp_live{}}),
    %% A later config change starts the backend.
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config), module = config_module(Config), old = Bad, new = Raw
        }
    ]),
    ?assertEqual(ok, emqx_omp:on_health_check(backend_id(Config), Raw)),
    ?assertEqual([], messages(Config, Topic)).

-doc "Check that stopping the backend stores and dispatches buffered messages once.".
t_writer_shutdown(Config) ->
    %% Buffer a message with flush timers beyond the test deadline.
    Raw = ?config(raw, Config),
    Writer = #{
        <<"pool_size">> => 1, <<"min_flush_timeout">> => 60000, <<"max_flush_timeout">> => 60000
    },
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = #{},
            new = Raw#{<<"async_batched">> => true, <<"writer">> => Writer}
        }
    ]),
    Topic = <<"offline/shutdown">>,
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    {ok, [persisted], _} = emqx_broker:publish2(
        emqx_message:make(<<"p">>, 1, Topic, <<"buffered">>)
    ),
    ?assertEqual([], messages(Config, Topic)),
    %% Stop the backend and verify one committed dispatch.
    ok = emqx_omp:stop(backend_id(Config)),
    ?assertEqual([{1, <<"buffered">>}], messages(Config, Topic)),
    ?assertReceive(#dispatch_info{
        topic = Topic, info = #omp_live{offset = 1, messages = [#message{payload = <<"buffered">>}]}
    }),
    ?assertNotReceive(#dispatch_info{info = #omp_live{}}).

-doc "Check that a writer crash counts buffered messages as failed and stores no batch twice.".
t_writer_crash(Config) ->
    %% Configure one writer to flush after two messages.
    Raw = ?config(raw, Config),
    Writer = #{
        <<"pool_size">> => 1,
        <<"min_flush_timeout">> => 60000,
        <<"max_flush_timeout">> => 60000,
        <<"flush_max_messages">> => 2
    },
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = #{},
            new = Raw#{<<"async_batched">> => true, <<"writer">> => Writer}
        }
    ]),
    %% The second append of the flush crashes the writer after the first one committed.
    Appends = counters:new(1, []),
    ok = meck:new(emqx_omp_store, [passthrough, no_link]),
    ok = meck:expect(emqx_omp_store, append_batch, fun(Namespace, Topic, Messages, PgConfig) ->
        counters:add(Appends, 1, 1),
        case counters:get(Appends, 1) of
            2 -> error(test_crash);
            _ -> meck:passthrough([Namespace, Topic, Messages, PgConfig])
        end
    end),
    Failed = write_failed(Config),
    %% Fill two topic batches and verify that the crash does not append either batch twice.
    {ok, [persisted], _} = emqx_broker:publish2(
        emqx_message:make(<<"p">>, 1, <<"offline/crash-a">>, <<"a">>)
    ),
    {ok, [persisted], _} = emqx_broker:publish2(
        emqx_message:make(<<"p">>, 2, <<"offline/crash-b">>, <<"b">>)
    ),
    ?retry(20, 100, ?assertEqual(Failed + 2, write_failed(Config))),
    Stored = messages(Config, <<"offline/crash-a">>) ++ messages(Config, <<"offline/crash-b">>),
    ?assertMatch([{1, _}], Stored).

write_failed(Config) ->
    emqx_metrics_worker:get(?METRICS_WORKER, backend_id(Config), write_failed).

-doc "Check that a takeover handler resumes from durable progress and keeps later delivery.".
t_takeover(Config) ->
    %% Leave two delivered messages unacknowledged in a persistent MQTT session.
    Topic = <<"offline/takeover">>,
    Session = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 60}}],
    Sub0 = client(Config, <<"takeover">>, [{auto_ack, false} | Session]),
    unlink(Sub0),
    {ok, _, [1]} = emqtt:subscribe(Sub0, Topic, 1),
    append(Config, Topic, [<<"first">>, <<"second">>]),
    ?assertEqual([<<"first">>, <<"second">>], payloads(receive_messages(2))),
    %% Take over the session and acknowledge both replayed offsets before later delivery.
    Sub1 = client(Config, <<"takeover">>, Session),
    Replayed = payloads(receive_messages(4)),
    ?assertEqual([<<"first">>, <<"first">>, <<"second">>, <<"second">>], lists:sort(Replayed)),
    ?retry(50, 100, ?assertEqual(2, cursor(Config, <<"takeover">>, Topic))),
    append(Config, Topic, [<<"third">>]),
    ?assertEqual([<<"third">>], payloads(receive_messages(1))),
    ok = emqtt:disconnect(Sub1).

-doc "Check that live copies sent during takeover reach the client and acknowledge every offset.".
t_takeover_live(Config) ->
    %% Start a persistent subscriber and publish concurrently with its replacement connection.
    Topic = <<"offline/takeover-live">>,
    Session = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 60}}],
    Sub0 = client(Config, <<"takeover-live">>, Session),
    unlink(Sub0),
    {ok, _, [1]} = emqtt:subscribe(Sub0, Topic, 1),
    Pub = client(Config, <<"takeover-pub">>, []),
    Parent = self(),
    spawn_link(fun() ->
        lists:foreach(
            fun(N) -> {ok, _} = emqtt:publish(Pub, Topic, integer_to_binary(N), 1) end,
            lists:seq(1, 30)
        ),
        Parent ! published
    end),
    Sub1 = client(Config, <<"takeover-live">>, Session),
    %% Wait for publishing and verify that takeover preserves every committed offset.
    receive
        published -> ok
    after 10000 -> ct:fail(publisher_timeout)
    end,
    ?retry(50, 100, ?assertEqual(30, cursor(Config, <<"takeover-live">>, Topic))),
    Received = [binary_to_integer(P) || P <- payloads(drain_messages())],
    %% Every offset reaches one of the clients, and the new handler acknowledges all of them.
    ?assertEqual(lists:seq(1, 30), lists:usort(Received)),
    ok = emqtt:disconnect(Sub1),
    ok = emqtt:disconnect(Pub).

-doc "Check that a request to a missing pool fails at once so the handler can retry.".
t_async_missing_pool(Config) ->
    %% Stop the pools, send a page read and check its immediate error callback.
    ok = emqx_omp:stop(backend_id(Config)),
    Ref = make_ref(),
    Target = test_reader_target(),
    ok = emqx_omp_store:page_async(key, 1, 0, 1, Target, Ref, ?config(parsed, Config)),
    ?assertReceive({reader_event, {result, Ref, {error, no_such_pool}}}).

-doc "Check that a configuration reload keeps durable progress and resumes live delivery.".
t_reload(Config) ->
    %% Acknowledge one message before replacing the database pools.
    Topic = <<"offline/reload">>,
    Sub = client(Config, <<"reload">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    append(Config, Topic, [<<"before">>]),
    ?assertEqual([<<"before">>], payloads(receive_messages(1))),
    ?retry(50, 100, ?assertEqual(1, cursor(Config, <<"reload">>, Topic))),
    %% Reload configuration and verify continued delivery through the recreated reader.
    Raw = ?config(raw, Config),
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = Raw,
            new = Raw#{<<"message_read_pool_size">> => 2}
        }
    ]),
    append(Config, Topic, [<<"after">>]),
    ?assertEqual([<<"after">>], payloads(receive_messages(1))),
    ?assertEqual(ok, emqx_omp:on_health_check(backend_id(Config), Raw)),
    ok = emqtt:disconnect(Sub).

-doc "Check that the HTTP publish handler reports a committed message as persisted.".
t_http_publish(Config) ->
    %% Publish through the HTTP handler and check the committed database row.
    Topic = <<"offline/http">>,
    ?assertMatch(
        {200, _},
        emqx_mgmt_api_publish:publish(
            post,
            #{body => #{<<"topic">> => Topic, <<"payload">> => <<"http">>, <<"qos">> => 1}}
        )
    ),
    ?assertEqual([{1, <<"http">>}], messages(Config, Topic)).

-doc "Check that managed subscriptions from durable sessions are denied.".
t_durable_session_policy(_Config) ->
    %% Reject a durable session's managed-topic subscription.
    ClientInfo = #{clientid => <<"durable">>, protocol => mqtt},
    ok = emqx_omp:on_session(ClientInfo, #{impl => emqx_persistent_session_ds}),
    ?assertMatch(
        {stop, #{result := deny}},
        emqx_omp:on_authorize(
            ClientInfo,
            #{action_type => subscribe, qos => 1},
            <<"offline/durable">>,
            #{result => allow}
        )
    ),
    ok = emqx_omp:on_session(ClientInfo, #{impl => emqx_session_mem}),
    %% Allow the same subscription after selecting an in-memory session.
    ?assertMatch(
        {ok, #{result := allow}},
        emqx_omp:on_authorize(
            ClientInfo,
            #{action_type => subscribe, qos => 1},
            <<"offline/durable">>,
            #{result => allow}
        )
    ).

-doc "Check that managed subscriptions from gateways are denied without caching the denial.".
t_gateway_policy(_Config) ->
    %% Reject each gateway protocol even when the session implementation is in memory.
    ok = emqx_omp:on_session(#{}, #{impl => emqx_session_mem}),
    lists:foreach(
        fun(Protocol) ->
            ?assertMatch(
                {stop, #{result := deny, is_cacheable := false}},
                emqx_omp:on_authorize(
                    #{protocol => Protocol},
                    #{action_type => subscribe, qos => 1},
                    <<"offline/gateway">>,
                    #{result => allow}
                )
            )
        end,
        [stomp, mqttsn, coap, nats]
    ).

-doc "Check that one retention run removes an expired prefix larger than one SQL batch.".
t_retention_batches(Config) ->
    %% Store more than two cleanup batches and retain only the last two messages.
    Topic = <<"offline/retention-batches">>,
    append(Config, Topic, lists:duplicate(2100, <<"payload">>)),
    Raw = ?config(raw, Config),
    Retention = #{<<"max_messages">> => 2, <<"interval">> => 100},
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = Raw,
            new = Raw#{<<"retention">> => Retention}
        }
    ]),
    ?retry(50, 100, ?assertEqual([2099, 2100], [N || {N, _} <- messages(Config, Topic)])).

-doc "Check that age retention removes the expired prefix and stops at the first recent row.".
t_age_retention(Config) ->
    %% Store recent and expired rows with publish times out of offset order.
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/age">>,
    Now = erlang:system_time(millisecond),
    Messages = [
        emqx_omp_store:encode(
            published_at(emqx_message:make(<<"p">>, 1, Topic, integer_to_binary(N)), T)
        )
     || {N, T} <- [{1, Now - 10000}, {2, Now}, {3, Now - 10000}, {4, Now}]
    ],
    {ok, {Id, 1}} = (?config(backend, Config)):append(Conn, Namespace, Topic, Messages),
    %% Remove only the expired prefix and stop at the first recent row.
    {ok, Cutoff} = (?config(backend, Config)):age_cutoff(Conn, Id, 5, Now - 5000),
    ?assertEqual(2, Cutoff),
    {ok, 1} = (?config(backend, Config)):trim(Conn, Id, Cutoff, 1000),
    ?assertEqual([2, 3, 4], [N || {N, _} <- messages(Config, Topic)]),
    ?assertEqual({ok, 5}, (?config(backend, Config)):age_cutoff(Conn, Id, 5, Now + 5000)).

published_at(Msg, Timestamp) ->
    Msg#message{timestamp = Timestamp}.

-doc "Check that replay keeps offset order under an MQTT receive maximum of one.".
t_inflight_backpressure(Config) ->
    %% Set receive maximum one and append three messages for replay.
    Topic = <<"offline/inflight">>,
    Sub = client(Config, <<"inflight">>, [
        {auto_ack, false}, {properties, #{'Receive-Maximum' => 1}}
    ]),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    append(Config, Topic, [<<"first">>, <<"second">>, <<"third">>]),
    %% Acknowledge each packet before the next message can arrive.
    lists:foreach(
        fun(Payload) ->
            [#{payload := Received, packet_id := PacketId}] = receive_messages(1),
            ?assertEqual(Payload, Received),
            no_messages(),
            emqtt:puback(Sub, PacketId)
        end,
        [<<"first">>, <<"second">>, <<"third">>]
    ),
    ?retry(50, 100, ?assertEqual(3, cursor(Config, <<"inflight">>, Topic))),
    ok = emqtt:disconnect(Sub).

-doc "Check that the delivery rate limit cannot advance the cursor past unsent messages.".
t_rate_backpressure(Config) ->
    %% Limit the listener to one delivery so later replay rows remain unsettled.
    {ok, _} = emqx:update_config(
        [listeners, tcp, default],
        {update, #{<<"delivery_messages_rate">> => <<"1/1h">>}}
    ),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx:update_config(
            [listeners, tcp, default],
            {update, #{<<"delivery_messages_rate">> => <<"infinity">>}}
        )
    end),
    Topic = <<"offline/rate">>,
    Sub = client(Config, <<"rate">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    append(Config, Topic, [<<"first">>, <<"second">>, <<"third">>]),
    ?assertEqual([<<"first">>], payloads(receive_messages(1))),
    no_messages(),
    ?retry(50, 100, ?assertEqual(1, cursor(Config, <<"rate">>, Topic))),
    ok = emqtt:disconnect(Sub),
    {ok, _} = emqx:update_config(
        [listeners, tcp, default],
        {update, #{<<"delivery_messages_rate">> => <<"infinity">>}}
    ),
    %% Reconnect without the rate limit and replay the messages left unsettled.
    Resumed = client(Config, <<"rate">>, []),
    ?assertEqual([<<"second">>, <<"third">>], payloads(receive_messages(2))),
    ok = emqtt:disconnect(Resumed).

-doc "Check that cleanup does not remove another namespace's messages or subscriptions.".
t_retention_namespace(Config) ->
    %% Store independent history and a subscription in another namespace.
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/namespaces">>,
    OtherNamespace = <<Namespace/binary, "-other">>,
    OtherConfig = [{namespace, OtherNamespace} | Config],
    append(Config, Topic, [<<"one">>, <<"two">>]),
    append(OtherConfig, Topic, [<<"other-one">>, <<"other-two">>]),
    {ok, _} = (?config(backend, Config)):subscribe(
        Conn, OtherNamespace, <<"other">>, Topic, #{qos => 1}, earliest
    ),
    %% Clean the current namespace and check that the other namespace is unaffected.
    {ok, [{Id, 3}]} = (?config(backend, Config)):topics(Conn, Namespace, 0, 100),
    {ok, 1} = (?config(backend, Config)):trim(Conn, Id, 2, 1000),
    ?assertEqual([{2, <<"two">>}], messages(Config, Topic)),
    ?assertEqual([{1, <<"other-one">>}, {2, <<"other-two">>}], messages(OtherConfig, Topic)),
    ?assertEqual(0, cursor(OtherConfig, <<"other">>, Topic)).

-doc "Check that live delivery needs no page reads and idle polling recovers a lost final copy.".
t_live_mqtt(Config) ->
    %% Wait for initial catch-up with idle polling outside the live-delivery test interval.
    Topic = <<"offline/live-mqtt">>,
    Raw = ?config(raw, Config),
    Read = #{<<"page_max_messages">> => 2, <<"idle_poll_interval">> => 1000},
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = Raw,
            new = Raw#{<<"read">> => Read}
        }
    ]),
    Sub = client(Config, <<"live-mqtt">>, []),
    ?assertWaitEvent(
        emqtt:subscribe(Sub, Topic, 1),
        #{?snk_kind := omp_page, clientid := <<"live-mqtt">>, dirty := false},
        5000
    ),
    %% Record database reads after the initial snapshot finishes.
    _ = snabbkaffe:collect_trace(),
    Pub = client(Config, <<"live-pub">>, []),
    lists:foreach(
        fun(N) ->
            {ok, _} = emqtt:publish(Pub, Topic, integer_to_binary(N), 1),
            ?assertEqual([integer_to_binary(N)], payloads(receive_messages(1)))
        end,
        lists:seq(1, 10)
    ),
    ?assertEqual([], ?of_kind(omp_read_page, snabbkaffe:collect_trace())),
    %% Lose the live copy of a committed message.
    ok = meck:new(emqx_omp_store, [passthrough, no_link]),
    ok = meck:expect(emqx_omp_store, dispatch, fun(_Msg, _Identity, _Offset) -> ok end),
    {ok, _} = emqtt:publish(Pub, Topic, <<"lost-copy">>, 1),
    ok = meck:delete(emqx_omp_store, dispatch, 3),
    ?assertEqual([<<"lost-copy">>], payloads(receive_messages(1))),
    ?retry(20, 100, ?assertEqual(11, cursor(Config, <<"live-mqtt">>, Topic))),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

cluster_delivered_apply_changes(#{clientid := <<"cluster-sub">>}, Msg, Parent) ->
    Parent ! {cluster_delivered, emqx_message:get_header(omp_position, Msg)},
    ok;
cluster_delivered_apply_changes(_ClientInfo, _Msg, _Parent) ->
    ok.

-doc "Check that the mailbox limit still shuts down a stalled managed-topic subscriber.".
t_mailbox_overload(Config) ->
    %% Suspend a caught-up channel with a low mailbox shutdown limit.
    Topic = <<"offline/overload">>,
    Policy = emqx_config:get_zone_conf(default, [force_shutdown]),
    ok = emqx_config:put_zone_conf(default, [force_shutdown], Policy#{max_mailbox_size := 20}),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_config:put_zone_conf(default, [force_shutdown], Policy)
    end),
    Sub = client(Config, <<"overload">>, []),
    unlink(Sub),
    ?assertWaitEvent(
        emqtt:subscribe(Sub, Topic, 1),
        #{?snk_kind := omp_page, clientid := <<"overload">>, dirty := false},
        5000
    ),
    [Channel] = emqx_cm:lookup_channels(<<"overload">>),
    Monitor = monitor(process, Channel),
    ok = sys:suspend(Channel),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Channel) end),
    %% The passive-socket boundary checks overload before the queued copies run.
    Channel ! {tcp_passive, undefined},
    lists:foreach(
        fun(_) ->
            {ok, [persisted], _} = emqx_broker:publish2(
                emqx_message:make(<<"p">>, 1, Topic, <<"queued">>)
            )
        end,
        lists:seq(1, 100)
    ),
    {message_queue_len, QueueLength} = process_info(Channel, message_queue_len),
    ?assert(QueueLength >= 100),
    ?assertWaitEvent(
        sys:resume(Channel),
        #{?snk_kind := check_oom_shutdown, shutdown := #{reason := mailbox_overflow}},
        5000
    ),
    receive
        {'DOWN', Monitor, process, Channel, _} -> ok
    after 5000 -> ct:fail(channel_still_running)
    end,
    ?assertEqual(100, length(messages(Config, Topic))).

-doc "Check that reordered live copies drain in offset order without database reads.".
t_live_reorder_buffer(Config) ->
    %% Buffer reordered copies before the missing first offset arrives.
    {Handler0, Context, Identity} = handler(Config, #{}),
    [Live1, Live2, Live3] = live(Identity, [1, 2, 3]),
    {ok, Handler1, []} = live_info(Handler0, Context, [Live3, Live2]),
    %% Drain the buffered copies in offset order after the missing offset arrives.
    {ok, Handler2, Messages} = live_info(Handler1, Context, [Live1]),
    ?assertEqual([1, 2, 3], offsets(Messages)),
    ?assertEqual([1, 1, 1], [QoS || #message{qos = QoS} <- Messages]),
    no_read(),
    ok = emqx_omp_extsub:handle_terminate(Handler2).

-doc "Check that a live batch is limited by ExtSub demand and the rest is recovered from storage.".
t_live_batch_demand(Config) ->
    %% Deliver five consecutive copies when ExtSub wants only three messages.
    {Handler0, Context0, {Namespace, Id} = Identity} = handler(Config, #{}),
    Context1 = Context0#{desired_message_count := 3},
    Terms = live(Identity, lists:seq(1, 5)),
    Batch = [Msg || #omp_live{messages = [Msg]} <- Terms],
    {ok, Handler1, Messages} = live_info(Handler0, Context1, [
        #omp_live{namespace = Namespace, topic_id = Id, offset = 1, messages = Batch}
    ]),
    ?assertEqual([1, 2, 3], offsets(Messages)),
    %% Restore demand and check that recovery starts after the accepted prefix.
    {Handler2, Ref, {page, [Id, 3, 2]}} = handler_request(Handler1, Context0),
    {ok, Handler3, Recovered} = handler_result(
        Handler2, Context0, Ref, {ok, [db_row(5, Term) || Term <- lists:nthtail(3, Terms)]}
    ),
    ?assertEqual([4, 5], offsets(Recovered)),
    %% A repeated batch cannot return offsets already delivered or recovered.
    {ok, Handler4, []} = live_info(Handler3, Context0, [
        #omp_live{namespace = Namespace, topic_id = Id, offset = 1, messages = Batch}
    ]),
    no_read(),
    ok = emqx_omp_extsub:handle_terminate(Handler4).

-doc "Check that copies from another namespace or topic and unrelated terms are ignored.".
t_live_identity(Config) ->
    %% Ignore foreign namespaces, foreign topic IDs and unrelated terms.
    {Handler0, Context, {{BackendId, Namespace}, Id} = Identity} = handler(Config, #{}),
    Foreign =
        live({{BackendId, <<Namespace/binary, "-other">>}, Id}, [1]) ++
            live({{BackendId, Namespace}, Id + 1}, [1]) ++
            live({{other_backend(Config), Namespace}, Id}, [1]),
    {ok, Handler1, []} = live_info(Handler0, Context, Foreign ++ [unrelated_term]),
    %% Accept the first offset only from the handler's own topic identity.
    {ok, Handler2, Messages} = live_info(Handler1, Context, live(Identity, [1])),
    ?assertEqual([1], offsets(Messages)),
    no_read(),
    ok = emqx_omp_extsub:handle_terminate(Handler2).

-doc "Check that a missing offset starts one recovery page after the reorder timeout.".
t_live_gap(Config) ->
    %% Receive a later offset and wait for the reorder timer to request recovery.
    {Handler0, Context, Identity} = handler(Config, #{}),
    [Live1, Live2, Live3] = live(Identity, [1, 2, 3]),
    {ok, Handler1, []} = live_info(Handler0, Context, [Live3]),
    no_read(),
    {ok, Handler2} = handler_event(Handler1, Context, reorder),
    {Handler3, Ref, {page, [_, 0, 2]}} = handler_request(Handler2, Context),
    %% Fill the missing prefix from storage and drain the buffered live copy once.
    {ok, Handler4, Messages} = handler_result(
        Handler3, Context, Ref, {ok, [db_row(3, Live1), db_row(3, Live2)]}
    ),
    ?assertEqual([1, 2, 3], offsets(Messages)),
    no_read(),
    ok = emqx_omp_extsub:handle_terminate(Handler4).

-doc "Check that live copies received during a read stay available after the read completes.".
t_read_overlap(Config) ->
    {Handler0, Context, Identity} = handler(Config, #{}),
    %% Force a recovery read through a large gap, independently of wall-clock time.
    {ok, Handler1, []} = live_info(Handler0, Context, live(Identity, [40])),
    {Handler2, Ref, {page, [_, 0, 2]}} = handler_request(Handler1, Context),
    [Live1, Live2, Live3, Live4] = live(Identity, [1, 2, 3, 4]),
    {ok, Handler3, []} = live_info(Handler2, Context, [Live3]),
    %% Without ExtSub demand, the next copy is dropped instead of buffered.
    {ok, Handler4, []} = live_info(Handler3, Context#{desired_message_count := 0}, [Live4]),
    {ok, Handler5, Messages} = handler_result(
        Handler4, Context, Ref, {ok, [db_row(2, Live1), db_row(2, Live2)]}
    ),
    ?assertEqual([1, 2, 3], offsets(Messages)),
    %% The dropped copy is recovered from the cursor.
    {Handler6, _Ref1, {page, [_, 3, 2]}} = handler_request(Handler5, Context),
    ok = emqx_omp_extsub:handle_terminate(Handler6).

-doc "Check that failed reads keep consumed offsets and stale results do not replace new requests.".
t_read_retry(Config) ->
    %% Deliver one offset and fail the recovery read for a later gap.
    {Handler0, Context, Identity} = handler(Config, #{}),
    {ok, Handler1, [_]} = live_info(Handler0, Context, live(Identity, [1])),
    {ok, Handler2, []} = live_info(Handler1, Context, live(Identity, [40])),
    {Handler3, Ref0, {page, [_, 1, 2]}} = handler_request(Handler2, Context),
    {ok, Handler4} = handler_result(Handler3, Context, Ref0, {error, disconnected}),
    {ok, Handler5} = emqx_omp_extsub:handle_info(Handler4, Context, retry),
    Ref1 =
        receive
            {handler_request, Ref, {page, [_, 1, 2]}} -> Ref
        after 1000 -> ct:fail(no_retry)
        end,
    [Live2] = live(Identity, [2]),
    %% Ignore the stale reply and accept only the replacement request's result.
    {ok, Handler5} = handler_result(Handler5, Context, Ref0, {ok, [db_row(2, Live2)]}),
    {ok, Handler6, Messages} = handler_result(Handler5, Context, Ref1, {ok, [db_row(2, Live2)]}),
    ?assertEqual([2], offsets(Messages)),
    ok = emqx_omp_extsub:handle_terminate(Handler6).

-doc "Check that the retry delay grows with jitter and stays within the idle poll interval.".
t_retry_backoff(Config) ->
    %% Fail four consecutive reads and collect their scheduled retry delays.
    {Handler0, Context, Identity} = handler(Config, #{<<"idle_poll_interval">> => 4000}),
    {ok, Handler1, []} = live_info(Handler0, Context, live(Identity, [40])),
    Delays = retry_delays(Handler1, Context, 4),
    %% Delays are 1000, 2000, 4000 and 4000 milliseconds, each with jitter of +-50%.
    lists:foreach(
        fun({Delay, Base}) -> ?assert(Delay >= Base div 2 andalso Delay =< Base * 3 div 2) end,
        lists:zip(Delays, [1000, 2000, 4000, 4000])
    ).

retry_delays(_Handler, _Context, 0) ->
    [];
retry_delays(Handler0, Context, N) ->
    {Handler1, Ref, _} = handler_request(Handler0, Context),
    {{ok, Handler2}, {ok, #{timeout := Delay}}} = ?wait_async_action(
        handler_result(Handler1, Context, Ref, {error, disconnected}),
        #{?snk_kind := omp_retry},
        1000
    ),
    {ok, Handler3} = emqx_omp_extsub:handle_info(Handler2, Context, retry),
    self() ! {reader_event, tick},
    [Delay | retry_delays(Handler3, Context, N - 1)].

-doc "Check that recovery waits for ExtSub demand and starts after an acknowledgement restores it.".
t_no_demand(Config) ->
    %% Deliver the first offset and drop the second while ExtSub has no demand.
    {Handler0, Context, Identity} = handler(Config, #{}),
    NoDemand = Context#{desired_message_count := 0},
    {ok, Handler1, [Msg1]} = live_info(Handler0, Context, live(Identity, [1])),
    {ok, Handler2, []} = live_info(Handler1, NoDemand, live(Identity, [2])),
    %% The scheduled tick starts no read while demand is zero.
    receive
        {reader_event, tick} -> ok
    after 1000 -> ct:fail(no_tick)
    end,
    {ok, Handler3} = emqx_omp_extsub:handle_info(Handler2, NoDemand, tick),
    no_read(),
    %% An acknowledgement with restored demand starts recovery after the delivered offset.
    {ok, Handler4} = emqx_omp_extsub:handle_delivered(Handler3, Context, Msg1, 0),
    {Handler5, _Ref, {page, [_, 1, 2]}} = handler_request(Handler4, Context),
    ok = emqx_omp_extsub:handle_terminate(Handler5).

-doc "Check that no-local and expiry drops advance the checkpoint.".
t_drop_ack(Config) ->
    %% Deliver two live messages and report their no-local and expiry drops.
    {Handler0, Context, Identity} = handler(Config, #{}),
    {ok, Handler1, [Msg1, Msg2]} = live_info(Handler0, Context, live(Identity, [1, 2])),
    {ok, Handler2} = emqx_omp_extsub:handle_delivered(
        Handler1, Context, Msg1, {dropped, no_local}
    ),
    {ok, Handler3} = emqx_omp_extsub:handle_delivered(
        Handler2, Context, Msg2, {dropped, expired}
    ),
    ok = emqx_omp_extsub:handle_terminate(Handler3),
    %% Check that both drop reasons count as acknowledged progress.
    ?retry(20, 100, ?assertEqual(2, cursor(Config, <<"handler">>, <<"offline/handler">>))).

-doc "Reject overlapping backend ownership without restarting the running PostgreSQL backend.".
t_backend_overlap(Config) ->
    Raw = ?config(raw, Config),
    Id = backend_id(Config),
    Key = atom_to_binary(Id),
    OtherKey = atom_to_binary(other_backend(Config)),
    Before = emqx_omp:config(Id),
    lists:foreach(
        fun(Filter) ->
            ?assertMatch(
                {error, {overlapping_backend_topics, [_ | _]}},
                emqx_offline_messages:on_config_changed(#{Key => Raw}, #{
                    Key => Raw,
                    OtherKey => #{<<"enable">> => true, <<"topics">> => [Filter]}
                })
            ),
            ?assertEqual(Before, emqx_omp:config(Id))
        end,
        [<<"offline/#">>, <<"offline/+">>, <<"offline/exact">>, <<"#">>]
    ).

-doc "Keep backend-scoped pools, writers and readers independent when one runtime stops.".
t_backend_runtime_isolation(Config) ->
    %% Use the same SQL adapter for both identities to isolate runtime behavior from SQL dialects.
    Parsed = ?config(parsed, Config),
    OtherId = other_backend(Config),
    Other = Parsed#{
        backend_id := OtherId,
        topics := [<<"other/#">>],
        instance := make_ref(),
        async_batched := true
    },
    ok = emqx_omp:start(Other),
    emqx_common_test_helpers:on_exit(fun() -> emqx_omp:stop(OtherId) end),
    ?assertNotEqual(
        emqx_omp_store:pool(message_read, Parsed), emqx_omp_store:pool(message_read, Other)
    ),
    Sub = client(Config, <<"isolated-sub">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"other/topic">>, 1),
    Pub = client(Config, <<"isolated-pub">>, []),
    {ok, _} = emqtt:publish(Pub, <<"other/topic">>, <<"before">>, 1),
    ?assertEqual([<<"before">>], payloads(receive_messages(1))),
    ok = emqx_omp:stop(backend_id(Config)),
    ?assertEqual(Other, emqx_omp:config(OtherId)),
    {ok, _} = emqtt:publish(Pub, <<"other/topic">>, <<"after">>, 1),
    ?assertEqual([<<"after">>], payloads(receive_messages(1))),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

-doc "Initialize a caught-up reader with database requests controlled by the test.".
handler(Config, ReadOptions) ->
    Raw = ?config(raw, Config),
    Read = maps:merge(
        #{
            <<"page_max_messages">> => 2,
            <<"reorder_timeout">> => 100,
            <<"idle_poll_interval">> => 60000
        },
        ReadOptions
    ),
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = Raw,
            new = Raw#{<<"read">> => Read}
        }
    ]),
    Parent = self(),
    ok = meck:new(emqx_omp_store, [passthrough, no_link]),
    %% Capture requests as `{Statement, Args}` so the test can run them on its own connection.
    ok = meck:expect(
        emqx_omp_store,
        subscription_cursor_async,
        fun(Namespace, ClientId, Topic, _Target, Ref, _BackendConfig) ->
            Parent !
                {handler_request, Ref, {subscription_cursor, [Namespace, ClientId, Topic]}},
            ok
        end
    ),
    ok = meck:expect(emqx_omp_store, page_async, fun(
        _Key, Id, Cursor, Count, _Target, Ref, _BackendConfig
    ) ->
        Parent ! {handler_request, Ref, {page, [Id, Cursor, Count]}},
        ok
    end),
    Context = #{
        can_receive_acks => true,
        message_target => test_reader_target(),
        clientinfo => #{clientid => <<"handler">>},
        subopts => #{qos => 1},
        delivering_count => 0,
        desired_message_count => 100
    },
    Topic = <<"offline/handler">>,
    {ok, _} = run_request(
        Config,
        {
            subscribe, [?config(namespace, Config), <<"handler">>, Topic, #{qos => 1}, earliest]
        }
    ),
    {ok, Handler0} = emqx_omp_extsub:handle_subscribe(subscribe, Context, undefined, Topic),
    {Handler1, Ref0, Init} = handler_request(Handler0, Context),
    {ok, #{topic_id := Id}} = InitResult = run_request(Config, Init),
    {ok, Handler2} = handler_result(Handler1, Context, Ref0, InitResult),
    {Handler3, Ref1, Page} = handler_request(Handler2, Context),
    {ok, Handler4, []} = handler_result(
        Handler3,
        Context,
        Ref1,
        run_request(Config, Page)
    ),
    {Handler4, Context, {{backend_id(Config), ?config(namespace, Config)}, Id}}.

test_reader_target() ->
    Parent = self(),
    Target = emqx_extsub_handler:message_target(make_ref()),
    ok = meck:new(emqx_extsub_handler, [passthrough, no_link]),
    ok = meck:expect(emqx_extsub_handler, send, fun
        (To, Event) when To =:= Target ->
            Parent ! {reader_event, Event},
            ok;
        (To, Event) ->
            meck:passthrough([To, Event])
    end),
    ok = meck:expect(emqx_extsub_handler, send_after, fun
        (Delay, To, Event) when To =:= Target ->
            erlang:send_after(Delay, Parent, {reader_event, Event});
        (Delay, To, Event) ->
            meck:passthrough([Delay, To, Event])
    end),
    Target.

-doc "Process the scheduled reader tick and return the database request it starts.".
handler_request(Handler0, Context) ->
    receive
        {reader_event, tick} -> ok
    after 1000 -> ct:fail(no_tick)
    end,
    {ok, Handler} = emqx_omp_extsub:handle_info(Handler0, Context, tick),
    receive
        {handler_request, Ref, Operation} -> {Handler, Ref, Operation}
    after 1000 -> ct:fail(no_read_request)
    end.

run_request(Config, {Statement, Args}) ->
    apply(?config(backend, Config), Statement, [?config(db, Config) | Args]).

handler_event(Handler, Context, reorder) ->
    receive
        {reader_event, {reorder, _} = Event} ->
            emqx_omp_extsub:handle_info(Handler, Context, Event)
    after 1000 -> ct:fail(no_reorder_timer)
    end.

handler_result(Handler, Context, Ref, Result) ->
    emqx_omp_extsub:handle_info(Handler, Context, {result, Ref, Result}).

-doc "Pass live terms one at a time, as the channel does, and collect returned messages.".
live_info(Handler0, Context, Terms) ->
    lists:foldl(
        fun(Term, {ok, Handler, Acc}) ->
            Routed = #dispatch_info{topic = <<"offline/handler">>, info = Term},
            case emqx_omp_extsub:handle_info(Handler, Context, {generic, Routed}) of
                {ok, Handler1} -> {ok, Handler1, Acc};
                {ok, Handler1, Messages} -> {ok, Handler1, Acc ++ Messages}
            end
        end,
        {ok, Handler0, []},
        Terms
    ).

-doc "Build one-message live batches with payloads that identify their assigned offsets.".
live({Namespace, Id}, Offsets) ->
    [
        #omp_live{
            namespace = Namespace,
            topic_id = Id,
            offset = Offset,
            messages = [
                emqx_message:make(
                    <<"publisher">>, 1, <<"offline/handler">>, integer_to_binary(Offset)
                )
            ]
        }
     || Offset <- Offsets
    ].

db_row(Tip, #omp_live{
    offset = Offset, messages = [#message{id = Id, payload = Payload, timestamp = Published}]
}) ->
    {Tip, Offset, Id, <<"publisher">>, 1, false, Payload,
        emqx_utils_json:encode(#{version => 1, values => #{}}), Published, null}.

no_read() ->
    receive
        {handler_request, _, _} = Request -> ct:fail({unexpected_read, Request});
        {reader_event, tick} -> ct:fail(unexpected_read_tick)
    after 20 -> ok
    end.

client(Config, Id, Options) ->
    {ok, Client} = emqtt:start_link(
        proplists:delete(proto_ver, Options) ++
            [
                {clientid, Id},
                {host, "127.0.0.1"},
                {port, ?config(port, Config)},
                {proto_ver, proplists:get_value(proto_ver, Options, v5)}
            ]
    ),
    {ok, _} = emqtt:connect(Client),
    emqx_common_test_helpers:on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

append(Config, Topic, Payloads) ->
    {ok, _} = store_append(?config(db, Config), Config, Topic, Payloads),
    ok.

store_append(Conn, Config, Topic, Payloads) ->
    Messages = [
        emqx_omp_store:encode(emqx_message:make(<<"publisher">>, 1, Topic, Payload))
     || Payload <- Payloads
    ],
    (?config(backend, Config)):append(Conn, ?config(namespace, Config), Topic, Messages).

receive_messages(0) ->
    [];
receive_messages(N) ->
    receive
        {publish, Message} -> [Message | receive_messages(N - 1)]
    after 10000 -> ct:fail({missing_messages, N})
    end.

drain_messages() ->
    receive
        {publish, Message} -> [Message | drain_messages()]
    after 500 -> []
    end.

payloads(Messages) -> [Payload || #{payload := Payload} <- Messages].

no_messages() ->
    receive
        {publish, Msg} -> ct:fail({unexpected_publish, Msg})
    after 150 -> ok
    end.

-doc "Check that count and byte thresholds flush async batches and dispatch each as one term.".
t_async_threshold_payloads(Config) ->
    lists:foreach(
        fun(#{name := Name, count := Count, bytes := Bytes}) ->
            %% Keep timers beyond the test deadline so only the size threshold can flush.
            Raw = ?config(raw, Config),
            Writer = #{
                <<"pool_size">> => 1,
                <<"min_flush_timeout">> => 10000,
                <<"max_flush_timeout">> => 10000,
                <<"flush_max_messages">> => Count,
                <<"flush_max_bytes">> => Bytes
            },
            ok = emqx_omp:apply_changes([
                #config_change{
                    id = backend_id(Config),
                    module = config_module(Config),
                    old = #{},
                    new = Raw#{<<"async_batched">> => true, <<"writer">> => Writer}
                }
            ]),
            Pub = client(Config, Name, []),
            Topic = <<"offline/", Name/binary>>,
            ok = emqx:subscribe(Topic),
            %% Acknowledge both publish QoS levels before dispatching their single topic batch.
            {ok, _} = emqtt:publish(Pub, Topic, <<"abc">>, 1),
            ?assertEqual([], messages(Config, Topic)),
            {ok, _} = emqtt:publish(Pub, Topic, <<"def">>, 2),
            ?retry(
                20, 100, ?assertEqual([{1, <<"abc">>}, {2, <<"def">>}], messages(Config, Topic))
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

-doc "Check cross-node live dispatch and ordered delivery with publishers on two nodes.".
t_cluster_apply_changes(Config) ->
    %% Start two broker nodes with the same SQL namespace and live-delivery hooks.
    Apps = [
        {emqx_conf, #{config => #{authorization => #{no_match => allow}}}},
        emqx,
        emqx_extsub,
        emqx_offline_messages
    ],
    Nodes =
        [Node0, Node1] = emqx_cth_cluster:start(
            [{omp_pg_0, #{apps => Apps}}, {omp_pg_1, #{apps => Apps}}],
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    emqx_common_test_helpers:on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    Raw = ?config(raw, Config),
    Read = #{<<"page_max_messages">> => 2, <<"idle_poll_interval">> => 60000},
    lists:foreach(
        fun(Node) ->
            ok = rpc:call(Node, emqx_omp, apply_changes, [
                [
                    #config_change{
                        id = backend_id(Config),
                        module = config_module(Config),
                        old = #{},
                        new = Raw#{<<"read">> => Read}
                    }
                ]
            ]),
            ok = rpc:call(
                Node,
                emqx_hooks,
                put,
                ['message.delivered', {?MODULE, cluster_delivered_apply_changes, [self()]}, 0]
            )
        end,
        Nodes
    ),
    Config0 = [{port, emqx_cth_cluster:get_tcp_mqtt_port(Node0)} | Config],
    %% Subscribe on the second node and check a remote publish without recovery reads.
    Config1 = [{port, emqx_cth_cluster:get_tcp_mqtt_port(Node1)} | Config],
    Topic = <<"offline/cluster">>,
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
    receive
        {cluster_delivered, {_, _, 1}} -> ok
    after 5000 -> ct:fail(missing_remote_position)
    end,
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
    lists:foreach(
        fun(Pub) ->
            receive
                {publisher_done, Pub} -> ok
            after 5000 -> ct:fail(publisher_timeout)
            end
        end,
        [Pub0, Pub1]
    ),
    ?assertEqual([Payload || {Offset, Payload} <- messages(Config, Topic), Offset > 1], Received),
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

-doc "Check that cleanup keeps acknowledged history with `earliest` and removes it with `latest`.".
t_consumed_cleanup_reload(Config) ->
    %% Give two subscriptions different acknowledged offsets in the same topic.
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/consumed">>,
    {ok, {Id, 1}} = store_append(Conn, Config, Topic, [<<"1">>, <<"2">>, <<"3">>]),
    {ok, _} = (?config(backend, Config)):subscribe(
        Conn, Namespace, <<"a">>, Topic, #{qos => 1}, earliest
    ),
    {ok, _} = (?config(backend, Config)):subscribe(
        Conn, Namespace, <<"b">>, Topic, #{qos => 1}, earliest
    ),
    {ok, _} = (?config(backend, Config)):checkpoint(Conn, Namespace, <<"a">>, Id, 3),
    {ok, _} = (?config(backend, Config)):checkpoint(Conn, Namespace, <<"b">>, Id, 1),
    Raw = ?config(raw, Config),
    Retention = #{<<"interval">> => 100},
    %% With `earliest`, acknowledged messages within retention stay readable.
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config),
            module = config_module(Config),
            old = Raw,
            new = Raw#{<<"retention">> => Retention}
        }
    ]),
    timer:sleep(500),
    ?assertEqual([1, 2, 3], [N || {N, _} <- messages(Config, Topic)]),
    %% With `latest`, messages acknowledged by every subscriber are removed.
    Latest = Raw#{<<"retention">> => Retention, <<"new_subscription_start">> => <<"latest">>},
    ok = emqx_omp:apply_changes([
        #config_change{
            id = backend_id(Config), module = config_module(Config), old = Raw, new = Latest
        }
    ]),
    ?retry(50, 100, ?assertEqual([2, 3], [N || {N, _} <- messages(Config, Topic)])),
    %% A topic without subscriptions keeps no messages with `latest`.
    delete_subscriptions(Config),
    ?retry(50, 100, ?assertEqual([], messages(Config, Topic))),
    ?assertEqual({ok, 4}, (?config(backend, Config)):consumed_cutoff(Conn, Id, 4)).
