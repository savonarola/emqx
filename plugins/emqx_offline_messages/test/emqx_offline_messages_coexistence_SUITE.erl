%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_coexistence_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-import(emqx_offline_messages_SUITE, [client/3, parse/2, payloads/1, raw/2, receive_messages/1]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) -> emqx_offline_messages_SUITE:init_sql_group(mysql, Config).
end_per_suite(Config) -> emqx_offline_messages_SUITE:end_per_group(mysql, Config).
init_per_testcase(Case, Config) -> emqx_offline_messages_SUITE:init_per_testcase(Case, Config).
end_per_testcase(Case, Config) -> emqx_offline_messages_SUITE:end_per_testcase(Case, Config).

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

publish_loop(Parent, Topic, Published, Rejected) ->
    receive
        stop -> Parent ! {published, Published, Rejected}
    after 0 ->
        Msg = emqx_message:make(<<"mover">>, 1, Topic, integer_to_binary(Published)),
        case emqx_broker:publish2(Msg, #{hook_prohibition_as_error => true}) of
            {error, blocked, _} -> publish_loop(Parent, Topic, Published + 1, Rejected + 1);
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
