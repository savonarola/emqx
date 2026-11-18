%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_pg_SUITE).
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

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
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
    ok = drop_schema(),
    [{apps, Apps}, {port, Port} | Config].

end_per_suite(Config) -> emqx_cth_suite:stop(?config(apps, Config)).

-doc "Development databases are recreated when the schema definitions change.".
drop_schema() ->
    Parsed = emqx_omp_pg_config:parse(#{
        <<"host">> => <<"pgsql">>, <<"ssl">> => #{<<"enable">> => false}
    }),
    {ok, Conn} = emqx_omp_store:connect(Parsed),
    _ = epgsql:squery(Conn, """
    DROP TABLE IF EXISTS mqtt_pg_subscription, mqtt_pg_message, mqtt_pg_topic, mqtt_pg_schema CASCADE;
    DROP FUNCTION IF EXISTS omp_append(text, text, text, jsonb);
    DROP FUNCTION IF EXISTS omp_append(text, text, jsonb);
    """),
    ok = epgsql:close(Conn).

init_per_testcase(Case, Config) ->
    ok = snabbkaffe:start_trace(),
    Namespace = atom_to_binary(Case),
    Raw = #{
        <<"enable">> => true,
        <<"host">> => <<"pgsql">>,
        <<"namespace">> => Namespace,
        <<"topics">> => [<<"offline/#">>],
        <<"init_default_schema">> => true,
        <<"read">> => #{
            <<"page_max_messages">> => 2, <<"idle_poll_interval">> => 200
        },
        <<"checkpoint">> => #{<<"interval">> => 50, <<"max_pending">> => 2}
    },
    Parsed = emqx_omp_pg_config:parse(Raw),
    {ok, Conn} = emqx_omp_store:connect(Parsed),
    ok = emqx_omp_store:install(Parsed),
    _ = sql(
        Conn, "TRUNCATE mqtt_pg_subscription, mqtt_pg_message, mqtt_pg_topic RESTART IDENTITY", []
    ),
    ok = emqx_offline_messages:on_config_changed(#{}, #{<<"postgresql">> => Raw}),
    [{namespace, Namespace}, {raw, Raw}, {db, Conn}, {pg_config, Parsed} | Config].

end_per_testcase(_Case, Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok = emqx_omp:stop(postgresql),
    epgsql:close(?config(db, Config)),
    meck:unload(),
    snabbkaffe:stop(),
    ok.

-doc "Check counter locks, rollback with consecutive offsets, independent topics and paging.".
t_transaction_ordering(Config) ->
    %% Hold an uncommitted append and start a competing writer for the same topic.
    Conn0 = ?config(db, Config),
    {ok, Conn1} = emqx_omp_store:connect(?config(pg_config, Config)),
    emqx_common_test_helpers:on_exit(fun() -> epgsql:close(Conn1) end),
    Topic = <<"offline/transactions">>,
    sql(Conn0, "BEGIN", []),
    {ok, {_, 1}} = store_append(Conn0, Config, Topic, [<<"rolled-back">>]),
    Parent = self(),
    spawn_link(fun() ->
        Parent !
            {writer,
                store_append(Conn1, Config, Topic, [<<"1">>, <<"2">>, <<"3">>, <<"4">>, <<"5">>])}
    end),
    %% A separate topic can commit while the first topic's counter remains locked.
    append(Config, <<"offline/other">>, [<<"independent">>]),
    receive
        {writer, _} -> ct:fail(writer_did_not_wait)
    after 100 -> ok
    end,
    sql(Conn0, "ROLLBACK", []),
    receive
        {writer, Result} -> ?assertMatch({ok, {_, 1}}, Result)
    after 5000 -> ct:fail(writer_timeout)
    end,
    ?assertEqual(lists:seq(1, 5), [N || {N, _} <- messages(Config, Topic)]),
    %% An unrelated transaction does not hide committed rows from the reader.
    sql(Conn0, "BEGIN", []),
    Sub = client(Config, <<"pages">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>, <<"4">>, <<"5">>], payloads(receive_messages(5))),
    sql(Conn0, "ROLLBACK", []),
    ok = emqtt:disconnect(Sub).

-doc "Check that the session statement timeout is set only with `set_statement_timeout`.".
t_statement_timeout_option(Config) ->
    %% Compare the configured timeout with the database default on separate connections.
    PgConfig = ?config(pg_config, Config),
    lists:foreach(
        fun(#{set := Set, expected := Expected}) ->
            {ok, Conn} = emqx_omp_store:connect(PgConfig#{set_statement_timeout := Set}),
            ?assertEqual([{Expected}], sql(Conn, "SHOW statement_timeout", [])),
            ok = epgsql:close(Conn)
        end,
        [
            #{set => true, expected => <<"5s">>},
            #{set => false, expected => <<"0">>}
        ]
    ).

-doc "Check that idle polls deliver SQL commits without broker traffic or notification sessions.".
t_idle_recovery(Config) ->
    %% Append directly through SQL and wait for idle polling to deliver each message.
    Topic = <<"offline/idle">>,
    Sub = client(Config, <<"idle">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    append(Config, Topic, [<<"before">>]),
    ?assertEqual([<<"before">>], payloads(receive_messages(1))),
    append(Config, Topic, [<<"during">>]),
    ?assertEqual([<<"during">>], payloads(receive_messages(1))),
    %% Check that recovery requires no PostgreSQL LISTEN session.
    ?assertEqual(
        [{0}],
        sql(
            ?config(db, Config),
            "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'LISTEN omp_%'",
            []
        )
    ),
    ok = emqtt:disconnect(Sub).

-doc "Check that statement timeouts do not disconnect publishers.".
t_write_failure(Config) ->
    %% Shorten the query timeout so a held table lock fails each MQTT publish.
    Raw = ?config(raw, Config),
    ok = emqx_offline_messages:on_config_changed(
        #{<<"postgresql">> => Raw},
        #{<<"postgresql">> => Raw#{<<"query_timeout">> => 200}}
    ),
    Failed = write_failed(),
    lists:foreach(
        fun({Version, ExpectedCode}) ->
            Pub = client(Config, atom_to_binary(Version), [{proto_ver, Version}]),
            Conn = ?config(db, Config),
            sql(Conn, "BEGIN", []),
            sql(Conn, "LOCK TABLE mqtt_pg_topic IN ACCESS EXCLUSIVE MODE", []),
            lists:foreach(
                fun(QoS) ->
                    ?assertMatch(
                        {ok, #{reason_code := ExpectedCode}},
                        emqtt:publish(Pub, <<"offline/failure">>, <<"lost">>, QoS)
                    )
                end,
                [1, 2]
            ),
            sql(Conn, "ROLLBACK", []),
            ?assertEqual(pong, emqtt:ping(Pub)),
            ok = emqtt:disconnect(Pub)
        end,
        [{v3, ?RC_SUCCESS}, {v4, ?RC_SUCCESS}, {v5, ?RC_IMPLEMENTATION_SPECIFIC_ERROR}]
    ),
    ?assertEqual(Failed + 6, write_failed()),
    ?assertEqual([], messages(Config, <<"offline/failure">>)).

write_failed() ->
    emqx_metrics_worker:get(?METRICS_WORKER, postgresql, write_failed).

-doc "Check that the topic lock orders a `latest` subscription before later appends.".
t_consumed_cleanup_race(Config) ->
    %% Acknowledge the existing history before creating a concurrent latest subscription.
    Conn0 = ?config(db, Config),
    {ok, Conn1} = emqx_omp_store:connect(?config(pg_config, Config)),
    emqx_common_test_helpers:on_exit(fun() -> epgsql:close(Conn1) end),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/race">>,
    {ok, {Id, 1}} = store_append(Conn0, Config, Topic, [<<"1">>, <<"2">>]),
    {ok, _} = emqx_omp_pg_sql:subscribe(Conn0, Namespace, <<"old">>, Topic, #{qos => 1}, earliest),
    {ok, _} = emqx_omp_pg_sql:checkpoint(Conn0, Namespace, <<"old">>, Id, 2),
    %% Hold the share lock as a subscribe transaction does after its tip read.
    sql(Conn1, "BEGIN", []),
    [{3}] = sql(Conn1, "SELECT next_offset FROM mqtt_pg_topic WHERE topic_id = $1 FOR SHARE", [Id]),
    Parent = self(),
    spawn_link(fun() -> Parent ! {appended, store_append(Conn0, Config, Topic, [<<"3">>])} end),
    receive
        {appended, _} -> ct:fail(append_did_not_wait)
    after 200 -> ok
    end,
    sql(
        Conn1,
        """
        INSERT INTO mqtt_pg_subscription(namespace, client_id, topic_id, subopts, ack_offset)
        VALUES ($1, 'new', $2, '{}', 2)
        """,
        [Namespace, Id]
    ),
    sql(Conn1, "COMMIT", []),
    receive
        {appended, Result} -> ?assertEqual({ok, {Id, 3}}, Result)
    after 5000 -> ct:fail(append_timeout)
    end,
    %% The old subscriber acknowledges the new offset, and the cutoff stays at the new start.
    {ok, _} = emqx_omp_pg_sql:checkpoint(Conn0, Namespace, <<"old">>, Id, 3),
    ?assertEqual({ok, 3}, emqx_omp_pg_sql:consumed_cutoff(Conn0, Id, 4)).

-doc "Check that the packaged defaults round-trip through the Avro configuration schema.".
t_config_schema(_Config) ->
    %% Round-trip the packaged defaults through Avro and compare the parsed configuration.
    PrivDir = code:priv_dir(emqx_offline_messages),
    {ok, Schema} = file:read_file(filename:join(PrivDir, "config_schema.avsc")),
    {ok, Defaults} = hocon:load(filename:join(PrivDir, "config.hocon")),
    Store = avro_schema_store:import_schema_json(
        <<"offline_messages">>,
        Schema,
        avro_schema_store:new([map])
    ),
    Encoded = avro_json_encoder:encode(Store, <<"offline_messages">>, Defaults),
    Options = avro:make_decoder_options([{map_type, map}, {record_type, map}, {encoding, avro_json}]),
    Decoded = avro_json_decoder:decode_value(Encoded, <<"offline_messages">>, Store, Options#{
        is_wrapped => false
    }),
    Parsed0 = emqx_omp_pg_config:parse(maps:get(<<"postgresql">>, Defaults)),
    Parsed1 = emqx_omp_pg_config:parse(maps:get(<<"postgresql">>, Decoded)),
    ?assertEqual(maps:remove(instance, Parsed0), maps:remove(instance, Parsed1)),
    %% Check the default TLS verification and hostname used for PostgreSQL connections.
    #{connection := #{ssl_opts := SSLOpts}} = emqx_omp_pg_config:parse(
        #{<<"host">> => <<"database.example.net">>, <<"ssl">> => #{<<"enable">> => true}}
    ),
    ?assertEqual(verify_peer, proplists:get_value(verify, SSLOpts)),
    ?assertEqual("database.example.net", proplists:get_value(server_name_indication, SSLOpts)).

-doc "Check that SQL appends of any QoS share one sequence, both start positions and trimming.".
t_storage(Config) ->
    %% Append mixed-QoS SQL batches and check their consecutive offsets.
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/storage">>,
    Encode = fun(QoS) ->
        emqx_omp_store:encode(emqx_message:make(<<"p">>, QoS, Topic, <<"data">>))
    end,
    {ok, {Id, 1}} = emqx_omp_pg_sql:append(
        Conn, Namespace, Topic, [Encode(0), Encode(1), Encode(2)]
    ),
    {ok, {Id, 4}} = emqx_omp_pg_sql:append(Conn, Namespace, Topic, [Encode(0)]),
    ?assertEqual(
        [{1, 0}, {2, 1}, {3, 2}, {4, 0}],
        sql(Conn, "SELECT msg_offset, qos FROM mqtt_pg_message ORDER BY 1", [])
    ),
    ?assertMatch(
        {ok, #{cursor := 4}},
        emqx_omp_pg_sql:subscribe(Conn, Namespace, <<"latest">>, Topic, #{qos => 1}, latest)
    ),
    %% Trim history and check the remaining row and earliest starting position.
    {ok, 3} = emqx_omp_pg_sql:trim(Conn, Id, 5 - 1, 1000),
    {ok, Rows} = emqx_omp_pg_sql:page(Conn, Id, 0, 10),
    ?assertEqual([4], [element(2, Row) || Row <- Rows]),
    ?assertMatch(
        {ok, #{cursor := 0}},
        emqx_omp_pg_sql:subscribe(Conn, Namespace, <<"earliest">>, Topic, #{qos => 1}, earliest)
    ),
    ?assertEqual(
        [{<<"mqtt_pg_subscription_topic">>}],
        sql(
            Conn,
            "SELECT indexname FROM pg_indexes WHERE tablename = 'mqtt_pg_subscription' AND indexname <> 'mqtt_pg_subscription_pkey'",
            []
        )
    ),
    ?assertEqual(
        [{<<"mqtt_pg_message_pkey">>}],
        sql(Conn, "SELECT indexname FROM pg_indexes WHERE tablename = 'mqtt_pg_message'", [])
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

store_append(Conn, Config, Topic, Payloads) ->
    Messages = [
        emqx_omp_store:encode(emqx_message:make(<<"publisher">>, 1, Topic, Payload))
     || Payload <- Payloads
    ],
    emqx_omp_pg_sql:append(Conn, ?config(namespace, Config), Topic, Messages).

append(Config, Topic, Payloads) ->
    {ok, _} = store_append(?config(db, Config), Config, Topic, Payloads),
    ok.

messages(Config, Topic) ->
    sql(?config(db, Config), """
    SELECT m.msg_offset, m.payload FROM mqtt_pg_message m JOIN mqtt_pg_topic t USING(topic_id)
    WHERE t.namespace = $1 AND t.topic = $2 ORDER BY m.msg_offset
    """, [?config(namespace, Config), Topic]).

sql(Conn, SQL, Params) ->
    {ok, Result} = emqx_omp_pg_sql:query(Conn, SQL, Params),
    Result.

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

receive_messages(0) ->
    [];
receive_messages(N) ->
    receive
        {publish, Message} -> [Message | receive_messages(N - 1)]
    after 10000 -> ct:fail({missing_messages, N})
    end.

payloads(Messages) -> [Payload || #{payload := Payload} <- Messages].

no_messages() ->
    receive
        {publish, Msg} -> ct:fail({unexpected_publish, Msg})
    after 150 -> ok
    end.

cursor(Config, ClientId, Topic) ->
    case sql(?config(db, Config), """
    SELECT s.ack_offset FROM mqtt_pg_subscription s JOIN mqtt_pg_topic t USING(topic_id)
    WHERE s.namespace = $1 AND s.client_id = $2 AND t.topic = $3
    """, [?config(namespace, Config), ClientId, Topic]) of
        [{Offset}] -> Offset;
        [] -> undefined
    end.
