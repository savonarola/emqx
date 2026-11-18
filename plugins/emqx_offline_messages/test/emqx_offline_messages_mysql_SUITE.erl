%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_mysql_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_omp.hrl").

all() -> [{group, mysql}, {group, mariadb}].

groups() ->
    [{Id, [], cases()} || Id <- [mysql, mariadb]].

cases() ->
    [
        t_legacy_conversion,
        t_provision_failure,
        t_missing_schema_version,
        t_append_binary_values,
        t_append_sql_text,
        t_append_failure_unlocks,
        t_schema_lock_timeout,
        t_consumed_cleanup_race,
        t_append_timeout,
        t_config
    ].

init_per_group(Id, Config) ->
    emqx_offline_messages_SUITE:init_sql_group(Id, Config).

end_per_group(Id, Config) ->
    emqx_offline_messages_SUITE:end_per_group(Id, Config).

init_per_testcase(Case, Config) ->
    emqx_offline_messages_SUITE:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    emqx_offline_messages_SUITE:end_per_testcase(Case, Config).

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
        {error, blocked, _},
        emqx_broker:publish2(
            emqx_message:make(<<"p">>, 1, <<"contract/no-schema">>, <<"not-stored">>),
            #{hook_prohibition_as_error => true}
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

-doc "Check that appends over the query timeout do not disconnect publishers.".
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
        fun({Version, ExpectedCode}) ->
            Pub = client(Config, atom_to_binary(Version), [{proto_ver, Version}]),
            {ok, _} = Backend:query(Conn, "BEGIN", []),
            {ok, _} = Backend:query(
                Conn, "SELECT next_offset FROM mqtt_omp_topic WHERE topic_id = ? FOR UPDATE", [
                    TopicId
                ]
            ),
            ?assertMatch(
                {ok, #{reason_code := ExpectedCode}},
                emqtt:publish(Pub, Topic, <<"lost">>, 1)
            ),
            {ok, _} = Backend:query(Conn, "ROLLBACK", []),
            ?assertEqual(pong, emqtt:ping(Pub)),
            ok = emqtt:disconnect(Pub)
        end,
        [{v4, ?RC_SUCCESS}, {v5, ?RC_IMPLEMENTATION_SPECIFIC_ERROR}]
    ),
    %% The failed appends are not stored, also after the lock is released.
    timer:sleep(500),
    ?assertEqual([<<"1">>], [Payload || {_, Payload} <- stored(Config, Topic)]).

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

client(Config, Id, Opts) -> emqx_offline_messages_SUITE:client(Config, Id, Opts).
parse(Id, Raw) -> emqx_offline_messages_SUITE:parse(Id, Raw).
encoded(Topic, Payloads) -> emqx_offline_messages_SUITE:encoded(Topic, Payloads).
stored(Config, Topic) -> emqx_offline_messages_SUITE:messages(Config, Topic).
