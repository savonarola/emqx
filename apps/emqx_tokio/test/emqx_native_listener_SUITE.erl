%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_native_listener_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{config => base_config()}},
            {emqx_tokio, #{
                override_env => [
                    {representatives, 2},
                    {worker_threads, 2},
                    {batch_size, 8},
                    {queue_limit, 16},
                    {max_packet_size, 4096}
                ]
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    ct:timetrap({seconds, 60}),
    {ok, _} = emqx:update_config([listeners], base_listeners()),
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Case, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    snabbkaffe:stop(),
    ok.

-doc "Skip native subscribers only on the native message's origin node".
t_broker_origin(_Config) ->
    Topic = <<"native/origin">>,
    ok = emqx_broker:subscribe(Topic, undefined, #{native => true}),
    Msg0 = emqx_message:make(<<"native">>, Topic, <<"payload">>),
    Msg1 = emqx_message:set_header(native_origin, node(), Msg0),
    ?assertMatch([{_, _, {ok, 1}}], emqx_broker:publish(Msg1)),
    receive
        #deliver{message = Msg1} -> ct:fail(duplicate_native_delivery)
    after 100 -> ok
    end,
    Msg2 = emqx_message:set_header(native_origin, 'remote@localhost', Msg0),
    emqx_broker:publish(Msg2),
    receive
        #deliver{topic = Topic, message = Msg2} -> ok
    after 1000 -> ct:fail(no_remote_delivery)
    end,
    ok = emqx_broker:unsubscribe(Topic),
    ?retry(20, 100, ?assertEqual([], emqx_router:match_routes(Topic))).

-doc "Deliver locally across listeners and slots without broker publication or duplicate membership".
t_local_delivery(_Config) ->
    A = connect(18883),
    B = connect(18884),
    C = connect(18883),
    ?assertEqual([0, 0], subscribe(A, [<<"native/#">>, <<"native/+">>])),
    ?assertEqual([0], subscribe(A, [<<"native/#">>])),
    ?assertEqual([0], subscribe(B, [<<"native/#">>])),
    publish(C, <<"native/test">>, <<"local">>),
    expect_publish(A, <<"native/test">>, <<"local">>),
    expect_publish(A, <<"native/test">>, <<"local">>),
    expect_publish(B, <<"native/test">>, <<"local">>),
    assert_empty(A),
    assert_empty(B),
    Trace = snabbkaffe:collect_trace(),
    ?assertEqual([], [E || #{publishes := N} = E <- ?of_kind(native_publish_batch, Trace), N > 0]),
    send_packet(A, 16#A2, <<1:16, (utf(<<"native/+">>))/binary>>),
    ?assertEqual({16#B0, <<1:16>>}, recv_packet(A)),
    publish(C, <<"native/test">>, <<"once">>),
    expect_publish(A, <<"native/test">>, <<"once">>),
    assert_empty(A).

-doc "Deliver native and regular publishes in both directions without returning the native broker copy".
t_mixed_delivery(_Config) ->
    enable_regular(),
    A = connect(18883),
    B = connect(18884),
    C = connect(18885),
    [subscribe(S, [<<"mixed/#">>]) || S <- [A, B, C]],
    await_subscribers(<<"mixed/#">>, 3),
    publish(A, <<"mixed/topic">>, <<"native">>),
    [expect_publish(S, <<"mixed/topic">>, <<"native">>) || S <- [A, B, C]],
    [assert_empty(S) || S <- [A, B, C]],
    publish(C, <<"mixed/topic">>, <<"regular">>),
    [expect_publish(S, <<"mixed/topic">>, <<"regular">>) || S <- [A, B, C]],
    Trace = snabbkaffe:collect_trace(),
    ?assert(lists:any(fun(#{publishes := N}) -> N > 0 end, ?of_kind(native_publish_batch, Trace))),
    ?assertMatch([_ | _], ?of_kind(native_delivery_batch, Trace)).

-doc "Keep a regular route when native subscribers leave and remove it after the last subscriber leaves".
t_route_cleanup(_Config) ->
    enable_regular(),
    A = connect(18883),
    B = connect(18885),
    subscribe(A, [<<"cleanup">>]),
    subscribe(B, [<<"cleanup">>]),
    await_subscribers(<<"cleanup">>, 2),
    gen_tcp:close(A),
    ?retry(20, 100, ?assertEqual(1, length(emqx_broker:subscribers(<<"cleanup">>)))),
    ?assertMatch([_], emqx_router:match_routes(<<"cleanup">>)),
    gen_tcp:close(B),
    ?retry(20, 100, ?assertEqual([], emqx_router:match_routes(<<"cleanup">>))).

-doc "Publish wills on socket close and listener stop, and discard them on DISCONNECT".
t_will(_Config) ->
    enable_regular(),
    A = connect(18884),
    B = connect(18885),
    [subscribe(S, [<<"will">>]) || S <- [A, B]],
    await_subscribers(<<"will">>, 2),
    C = connect(18883, #{will => {<<"will">>, <<"gone">>}}),
    gen_tcp:close(C),
    [expect_publish(S, <<"will">>, <<"gone">>) || S <- [A, B]],
    D = connect(18883, #{will => {<<"will">>, <<"silent">>}}),
    send_packet(D, 16#E0, <<>>),
    gen_tcp:close(D),
    [assert_empty(S) || S <- [A, B]],
    _E = connect(18883, #{will => {<<"will">>, <<"stopped">>}}),
    ok = emqx_listeners:stop_listener('native:a'),
    [expect_publish(S, <<"will">>, <<"stopped">>) || S <- [A, B]],
    ?assertEqual(false, emqx_listeners:is_running('native:a')),
    ?assertEqual(true, emqx_listeners:is_running('native:b')).

-doc "Handle fragmented and coalesced packets, rejected filters, protocol errors, and keepalive".
t_protocol(_Config) ->
    A = socket(18883),
    Wire = packet(16#10, connect_body(#{client => <<>>})),
    [ok = gen_tcp:send(A, <<Byte>>) || <<Byte>> <= Wire],
    ?assertEqual({16#20, <<0, 0>>}, recv_packet(A)),
    ?assertEqual([0, 16#80, 16#80], subscribe(A, [<<"ok/+">>, <<"bad/#/x">>, <<"$share/g/t">>])),
    ok = gen_tcp:send(A, <<16#C0, 0, 16#C0, 0>>),
    ?assertEqual({16#D0, <<>>}, recv_packet(A)),
    ?assertEqual({16#D0, <<>>}, recv_packet(A)),
    send_packet(A, 16#34, <<0, 1, $t, 0, 1, $x>>),
    ?assertEqual({error, closed}, gen_tcp:recv(A, 0, 2000)),
    B = socket(18883),
    send_packet(B, 16#10, connect_body(#{level => 6})),
    ?assertEqual({16#20, <<0, 1>>}, recv_packet(B)),
    ?assertEqual({error, closed}, gen_tcp:recv(B, 0, 2000)),
    C = connect(18883, #{keepalive => 1}),
    ?assertEqual({error, closed}, gen_tcp:recv(C, 0, 2500)),
    D = connect(18883),
    send_packet(D, 16#30, <<(utf(<<"large">>))/binary, (binary:copy(<<0>>, 4096))/binary>>),
    ?assertEqual({error, closed}, gen_tcp:recv(D, 0, 2000)).

-doc "Keep a connection open past several keepalive intervals while it sends PINGREQ".
t_keepalive_activity(_Config) ->
    A = connect(18883, #{keepalive => 1}),
    lists:foreach(
        fun(_) ->
            timer:sleep(500),
            send_packet(A, 16#C0, <<>>),
            ?assertEqual({16#D0, <<>>}, recv_packet(A))
        end,
        lists:seq(1, 6)
    ).

-doc "Close only a failed representative's channels and restart the runtime from configuration".
t_restarts(_Config) ->
    A = connect(18883),
    B = connect(18884),
    subscribe(A, [<<"restart">>]),
    subscribe(B, [<<"restart">>]),
    await_subscribers(<<"restart">>, 2),
    [{_, Rep, _, _} | _] = supervisor:which_children(emqx_tokio_pool),
    exit(Rep, kill),
    Results = [gen_tcp:recv(S, 0, 300) || S <- [A, B]],
    ?assertEqual(1, length([ok || {error, closed} <- Results])),
    ?retry(
        20, 100, ?assertNot(lists:keymember(Rep, 2, supervisor:which_children(emqx_tokio_pool)))
    ),
    OldOwner = whereis(emqx_tokio_runtime),
    exit(OldOwner, kill),
    ?retry(
        50,
        100,
        ?assert(
            is_pid(whereis(emqx_tokio_runtime)) andalso whereis(emqx_tokio_runtime) =/= OldOwner
        )
    ),
    ?retry(50, 100, ?assertEqual(true, emqx_listeners:is_running('native:a'))),
    C = connect(18883),
    send_packet(C, 16#C0, <<>>),
    ?assertEqual({16#D0, <<>>}, recv_packet(C)).

-doc "Pause a publisher while its representative queue exceeds its limit and resume it without loss".
t_queue_overflow(_Config) ->
    enable_regular(),
    S = connect(18885),
    subscribe(S, [<<"overflow">>]),
    await_subscribers(<<"overflow">>, 1),
    A = connect(18883),
    Reps = [Pid || {_, Pid, _, _} <- supervisor:which_children(emqx_tokio_pool)],
    [sys:suspend(Pid) || Pid <- Reps],
    emqx_common_test_helpers:on_exit(fun() -> [catch sys:resume(Pid) || Pid <- Reps] end),
    %% Fill the representative queue past its limit of 16
    Wire = packet(16#30, <<(utf(<<"overflow">>))/binary, "payload">>),
    ok = gen_tcp:send(A, lists:duplicate(100, Wire)),
    %% The publisher stays connected, and its later packets are not read.
    %% Wait first so that PINGREQ does not arrive in the same read as the publishes.
    timer:sleep(200),
    send_packet(A, 16#C0, <<>>),
    ?assertEqual({error, timeout}, gen_tcp:recv(A, 0, 500)),
    %% Draining the queue resumes the publisher and delivers every broker copy
    [sys:resume(Pid) || Pid <- Reps],
    ?assertEqual({16#D0, <<>>}, recv_packet(A)),
    [expect_publish(S, <<"overflow">>, <<"payload">>) || _ <- lists:seq(1, 100)],
    assert_empty(S).

-doc "Reconcile listener settings and preserve the old listener when certificate validation fails".
t_configuration(_Config) ->
    A = connect(18883),
    Conf0 = emqx:get_config([listeners, native, a]),
    ?assertMatch(
        {error, _},
        emqx_tokio_runtime:start_listener('native:a', Conf0#{enable_unauthenticated => false})
    ),
    ?assertEqual(true, emqx_listeners:is_running('native:a')),
    BadTLS = #{
        enable => true, certfile => "/nonexistent/native.pem", keyfile => "/nonexistent/native.key"
    },
    ?assertMatch(
        {error, _}, emqx_tokio_runtime:start_listener('native:a', Conf0#{ssl_options => BadTLS})
    ),
    send_packet(A, 16#C0, <<>>),
    ?assertEqual({16#D0, <<>>}, recv_packet(A)),
    ?assertMatch(
        {error, _},
        emqx_tokio_runtime:start_listener('native:a', Conf0#{bind => {{127, 0, 0, 1}, 18884}})
    ),
    ?assertEqual(false, emqx_listeners:is_running('native:a')),
    ?assertEqual(true, emqx_listeners:is_running('native:b')),
    ok = emqx_listeners:start(),
    ?assertEqual(true, emqx_listeners:is_running('native:a')),
    {ok, _} = emqx:update_config(
        [listeners, native, a], {update, #{<<"bind">> => <<"127.0.0.1:18886">>}}
    ),
    _B = connect(18886),
    {ok, _} = emqx:update_config([listeners, native, a], {update, #{<<"enable">> => false}}),
    ?assertEqual(false, emqx_listeners:is_running('native:a')),
    Root0 = emqx:get_raw_config([listeners]),
    Native0 = maps:get(<<"native">>, Root0),
    {ok, _} = emqx:update_config([listeners], Root0#{<<"native">> := maps:remove(<<"a">>, Native0)}),
    ?assertEqual({error, not_found}, emqx_listeners:is_running('native:a')).

-doc "Serve MQTT over TLS with the configured certificate and private key".
t_tls(Config) ->
    Dir = ?config(priv_dir, Config),
    emqx_common_test_helpers:gen_ca(Dir, "native-ca"),
    emqx_common_test_helpers:gen_host_cert("native-server", "native-ca", Dir, #{}),
    SSL = #{
        <<"enable">> => true,
        <<"certfile">> => list_to_binary(filename:join(Dir, "native-server.pem")),
        <<"keyfile">> => list_to_binary(filename:join(Dir, "native-server.key"))
    },
    {ok, _} = emqx:update_config([listeners, native, a], {update, #{<<"ssl_options">> => SSL}}),
    {ok, Socket} = ssl:connect(
        "127.0.0.1", 18883, [{verify, verify_none}, {active, false}, binary], 5000
    ),
    ok = ssl:send(Socket, packet(16#10, connect_body(#{}))),
    ?assertEqual({ok, <<16#20, 2, 0, 0>>}, ssl:recv(Socket, 4, 2000)),
    ok = ssl:send(Socket, <<16#C0, 0>>),
    ?assertEqual({ok, <<16#D0, 0>>}, ssl:recv(Socket, 2, 2000)),
    ssl:close(Socket).

-doc "Turn forwarding on before a regular listener starts and off after it is disabled".
t_forwarding_updates(_Config) ->
    A = connect(18883),
    B = connect(18884),
    subscribe(B, [<<"forwarding">>]),
    publish(A, <<"forwarding">>, <<"local">>),
    expect_publish(B, <<"forwarding">>, <<"local">>),
    enable_regular(),
    C = connect(18885),
    subscribe(C, [<<"forwarding">>]),
    publish(A, <<"forwarding">>, <<"forwarded">>),
    expect_publish(B, <<"forwarding">>, <<"forwarded">>),
    expect_publish(C, <<"forwarding">>, <<"forwarded">>),
    {ok, _} = emqx:update_config([listeners, tcp, regular], {update, #{<<"enable">> => false}}),
    _ = snabbkaffe:collect_trace(),
    publish(A, <<"forwarding">>, <<"local-again">>),
    expect_publish(B, <<"forwarding">>, <<"local-again">>),
    assert_empty(B),
    Trace = snabbkaffe:collect_trace(),
    ?assertEqual([], [E || #{publishes := N} = E <- ?of_kind(native_publish_batch, Trace), N > 0]).

-doc "Close a slow subscriber without blocking other channels".
t_output_overflow(_Config) ->
    A = connect(18883),
    B = connect(18884),
    ok = inet:setopts(B, [{recbuf, 1024}]),
    subscribe(B, [<<"slow">>]),
    await_subscribers(<<"slow">>, 1),
    Wire = packet(16#30, <<(utf(<<"slow">>))/binary, (binary:copy(<<0>>, 3500))/binary>>),
    ok = gen_tcp:send(A, lists:duplicate(4000, Wire)),
    ?retry(50, 100, ?assertEqual([], emqx_broker:subscribers(<<"slow">>))),
    send_packet(A, 16#C0, <<>>),
    ?assertEqual({16#D0, <<>>}, recv_packet(A)),
    gen_tcp:close(B).

-doc "Stop the application without wills and reject native starts while the runtime is unavailable".
t_shutdown(_Config) ->
    enable_regular(),
    B = connect(18885),
    subscribe(B, [<<"shutdown-will">>]),
    _A = connect(18883, #{will => {<<"shutdown-will">>, <<"unexpected">>}}),
    emqx_common_test_helpers:on_exit(fun() -> application:ensure_all_started(emqx_tokio) end),
    ok = application:stop(emqx_tokio),
    ?assertEqual({error, not_available}, emqx_listeners:start_listener('native:a')),
    assert_empty(B),
    {ok, _} = application:ensure_all_started(emqx_tokio).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

base_config() -> #{<<"listeners">> => base_listeners()}.

base_listeners() ->
    Disabled = #{<<"default">> => #{<<"enable">> => false}},
    #{
        <<"tcp">> => Disabled,
        <<"ssl">> => Disabled,
        <<"ws">> => Disabled,
        <<"wss">> => Disabled,
        <<"native">> => #{<<"a">> => native_conf(18883), <<"b">> => native_conf(18884)}
    }.

native_conf(Port) ->
    #{
        <<"bind">> => iolist_to_binary(["127.0.0.1:", integer_to_list(Port)]),
        <<"enable">> => true,
        <<"enable_unauthenticated">> => true,
        <<"ssl_options">> => #{<<"enable">> => false}
    }.

enable_regular() ->
    Root0 = emqx:get_raw_config([listeners]),
    TCP0 = maps:get(<<"tcp">>, Root0),
    TCP = TCP0#{
        <<"regular">> => #{<<"bind">> => <<"127.0.0.1:18885">>, <<"enable_authn">> => false}
    },
    {ok, _} = emqx:update_config([listeners], Root0#{<<"tcp">> := TCP}),
    ok.

socket(Port) ->
    {ok, Socket} = gen_tcp:connect(
        "127.0.0.1", Port, [binary, {active, false}, {nodelay, true}], 5000
    ),
    emqx_common_test_helpers:on_exit(fun() -> gen_tcp:close(Socket) end),
    Socket.

connect(Port) -> connect(Port, #{}).
connect(Port, Opts) ->
    Socket = socket(Port),
    send_packet(Socket, 16#10, connect_body(Opts)),
    ?assertEqual({16#20, <<0, 0>>}, recv_packet(Socket)),
    Socket.

connect_body(Opts) ->
    ClientId = maps:get(client, Opts, integer_to_binary(erlang:unique_integer([positive]))),
    Level = maps:get(level, Opts, 4),
    Keepalive = maps:get(keepalive, Opts, 30),
    {Flags, Payload} =
        case Opts of
            #{will := {Topic, Message}} ->
                {6, <<(utf(ClientId))/binary, (utf(Topic))/binary, (utf(Message))/binary>>};
            _ ->
                {2, utf(ClientId)}
        end,
    <<0, 4, "MQTT", Level, Flags, Keepalive:16, Payload/binary>>.

subscribe(Socket, Filters) ->
    Body = iolist_to_binary([<<1:16>> | [<<(utf(Filter))/binary, 0>> || Filter <- Filters]]),
    send_packet(Socket, 16#82, Body),
    {16#90, <<1:16, Codes/binary>>} = recv_packet(Socket),
    binary_to_list(Codes).

publish(Socket, Topic, Payload) ->
    send_packet(Socket, 16#30, <<(utf(Topic))/binary, Payload/binary>>).
utf(Bin) -> <<(byte_size(Bin)):16, Bin/binary>>.
send_packet(Socket, Header, Body) -> ok = gen_tcp:send(Socket, packet(Header, Body)).
packet(Header, Body) -> iolist_to_binary([Header, remaining_length(byte_size(Body)), Body]).
remaining_length(N) when N < 128 -> [N];
remaining_length(N) -> [(N rem 128) bor 128 | remaining_length(N div 128)].

recv_packet(Socket) ->
    {ok, <<Header>>} = gen_tcp:recv(Socket, 1, 5000),
    Size = recv_length(Socket, 0, 0),
    Body =
        case Size of
            0 ->
                <<>>;
            _ ->
                {ok, Bin} = gen_tcp:recv(Socket, Size, 5000),
                Bin
        end,
    {Header, Body}.

recv_length(Socket, Shift, Acc) ->
    {ok, <<Byte>>} = gen_tcp:recv(Socket, 1, 5000),
    Value = Acc bor ((Byte band 127) bsl Shift),
    case Byte band 128 of
        0 -> Value;
        _ -> recv_length(Socket, Shift + 7, Value)
    end.

expect_publish(Socket, Topic, Payload) ->
    ?assertEqual({16#30, <<(utf(Topic))/binary, Payload/binary>>}, recv_packet(Socket)).

assert_empty(Socket) -> ?assertEqual({error, timeout}, gen_tcp:recv(Socket, 0, 100)).

await_subscribers(Filter, Count) ->
    ?retry(20, 100, ?assertEqual(Count, length(emqx_broker:subscribers(Filter)))).
