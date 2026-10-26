%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_native_mqtt5_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).
init_per_suite(Config) -> emqx_native_listener_SUITE:init_per_suite(Config).
end_per_suite(Config) -> emqx_native_listener_SUITE:end_per_suite(Config).
init_per_testcase(Case, Config) -> emqx_native_listener_SUITE:init_per_testcase(Case, Config).
end_per_testcase(Case, Config) -> emqx_native_listener_SUITE:end_per_testcase(Case, Config).

-doc "Preserve MQTT 5 properties and deliver the granted QoS on native connections".
t_native_properties(_Config) ->
    A = client(18883, #{}),
    B = client(18884, #{}),
    C = client(18884, #{}),
    {ok, _, [1]} = emqtt:subscribe(B, <<"props">>, 1),
    {ok, _, [0]} = emqtt:subscribe(C, <<"props">>, 0),
    {ok, _} = emqtt:publish(A, <<"props">>, properties(), <<"value">>, [{qos, 1}]),
    ?assertMatch(
        #{qos := 1, properties := #{'User-Property' := [{<<"k">>, <<"1">>}, {<<"k">>, <<"2">>}]}},
        message(B)
    ),
    ?assertMatch(#{qos := 0, payload := <<"value">>}, message(C)),
    [emqtt:disconnect(Pid) || Pid <- [A, B, C]].

-doc "Preserve properties and QoS through the broker in both directions".
t_broker_properties(_Config) ->
    emqx_native_listener_SUITE:enable_regular(),
    A = client(18883, #{}),
    B = client(18885, #{}),
    [
        begin
            {ok, _, [1]} = emqtt:subscribe(Pid, <<"mixed5">>, 1)
        end
     || Pid <- [A, B]
    ],
    emqx_native_listener_SUITE:await_subscribers(<<"mixed5">>, 2),
    lists:foreach(
        fun(Pub) ->
            {ok, _} = emqtt:publish(Pub, <<"mixed5">>, properties(), <<"value">>, [{qos, 1}]),
            [
                begin
                    #{qos := 1, properties := Props} = message(Sub),
                    assert_properties(Props)
                end
             || Sub <- [A, B]
            ]
        end,
        [A, B]
    ),
    [emqtt:disconnect(Pid) || Pid <- [A, B]].

-doc "Encode MQTT 3.1.1 and MQTT 5 deliveries independently on the same topic".
t_mixed_versions(_Config) ->
    A = client(18883, #{}),
    B = client(18884, #{proto_ver => v4}),
    [
        begin
            {ok, _, [1]} = emqtt:subscribe(Pid, <<"versions">>, 2)
        end
     || Pid <- [A, B]
    ],
    lists:foreach(
        fun(Pub) ->
            {ok, _} = emqtt:publish(Pub, <<"versions">>, <<"value">>, 1),
            [?assertMatch(#{qos := 1, payload := <<"value">>}, message(Sub)) || Sub <- [A, B]]
        end,
        [A, B]
    ),
    [emqtt:disconnect(Pid) || Pid <- [A, B]].

-doc "Resume pending QoS 1 deliveries after PUBACK and keep control packets flowing".
t_receive_maximum(_Config) ->
    A = client(18883, #{}),
    B = raw_connect(#{properties => <<16#21, 0, 1>>}),
    raw_subscribe(B, <<"window">>, 1),
    [{ok, _} = emqtt:publish(A, <<"window">>, integer_to_binary(N), 1) || N <- [1, 2]],
    {16#32, <<6:16, "window", Id:16, 0, "1">>} = recv(B),
    send(B, 16#C0, <<>>),
    ?assertEqual({16#D0, <<>>}, recv(B)),
    emqx_native_listener_SUITE:assert_empty(B),
    send(B, 16#40, <<65535:16>>),
    emqx_native_listener_SUITE:assert_empty(B),
    send(B, 16#40, <<Id:16, 0, 0>>),
    ?assertMatch({16#32, <<6:16, "window", _:16, 0, "2">>}, recv(B)),
    emqtt:disconnect(A).

-doc "Close a subscriber when pending QoS 1 deliveries reach the output bound".
t_pending_overflow(_Config) ->
    A = client(18883, #{}),
    B = raw_connect(#{properties => <<16#21, 0, 1>>}),
    raw_subscribe(B, <<"pending">>, 1),
    [{ok, _} = emqtt:publish(A, <<"pending">>, <<"x">>, 1) || _ <- lists:seq(1, 1030)],
    ?assertMatch({16#32, _}, recv(B)),
    ?assertEqual({error, closed}, gen_tcp:recv(B, 0, 2000)),
    emqtt:disconnect(A).

-doc "Apply No Local and update subscription options when a client subscribes again".
t_no_local(_Config) ->
    A = client(18883, #{}),
    {ok, _, [1]} = emqtt:subscribe(A, <<"nolocal">>, [{qos, 1}, {nl, true}, {rap, true}, {rh, 2}]),
    {ok, _} = emqtt:publish(A, <<"nolocal">>, <<"first">>, 1),
    receive
        {publish, #{client_pid := A}} -> ct:fail(local_delivery)
    after 100 -> ok
    end,
    {ok, _, [0]} = emqtt:subscribe(A, <<"nolocal">>, 0),
    {ok, _} = emqtt:publish(A, <<"nolocal">>, <<"second">>, 1),
    ?assertMatch(#{qos := 0, payload := <<"second">>}, message(A)),
    emqtt:disconnect(A).

-doc "Assign an empty client ID and advertise the supported MQTT 5 features".
t_connack(_Config) ->
    {ok, A} = emqtt:start_link(#{
        host => "127.0.0.1",
        port => 18883,
        proto_ver => v5,
        clientid => <<>>,
        properties => #{'Session-Expiry-Interval' => 60}
    }),
    {ok, Props} = emqtt:connect(A),
    ?assertMatch(
        #{
            'Assigned-Client-Identifier' := <<"native-", _/binary>>,
            'Maximum-QoS' := 1,
            'Retain-Available' := 0,
            'Maximum-Packet-Size' := 4096,
            'Shared-Subscription-Available' := 0,
            'Subscription-Identifier-Available' := 0,
            'Session-Expiry-Interval' := 0
        },
        Props
    ),
    emqtt:disconnect(A),
    B = emqx_native_listener_SUITE:socket(18883),
    send(B, 16#10, <<0, 4, "MQTT", 5, 2, 0, 30, 5, 16#27, 10:32, 1:16, "c">>),
    ?assertEqual({16#20, <<0, 16#95, 0>>}, recv(B)),
    ?assertEqual({error, closed}, gen_tcp:recv(B, 0, 2000)).

-doc "Drop an oversized delivery only for the subscriber with the smaller packet limit".
t_maximum_packet_size(_Config) ->
    A = client(18883, #{}),
    B = client(18884, #{properties => #{'Maximum-Packet-Size' => 64}}),
    C = client(18884, #{}),
    [
        begin
            {ok, _, [1]} = emqtt:subscribe(Pid, <<"size">>, 1)
        end
     || Pid <- [B, C]
    ],
    {ok, _} = emqtt:publish(A, <<"size">>, binary:copy(<<0>>, 100), 1),
    ?assertMatch(#{qos := 1}, message(C)),
    receive
        {publish, #{client_pid := B}} -> ct:fail(oversized_delivery)
    after 100 -> ok
    end,
    {ok, _} = emqtt:publish(A, <<"size">>, <<"small">>, 1),
    [?assertMatch(#{payload := <<"small">>}, message(Pid)) || Pid <- [B, C]],
    [emqtt:disconnect(Pid) || Pid <- [A, B, C]].

-doc "Publish a QoS 1 will with properties on DISCONNECT reason 0x04".
t_disconnect_will(_Config) ->
    A = client(18884, #{}),
    {ok, _, [1]} = emqtt:subscribe(A, <<"will5">>, 1),
    B = client(18883, #{
        will_topic => <<"will5">>,
        will_payload => <<"gone">>,
        will_qos => 1,
        will_props => #{'Content-Type' => <<"text/plain">>, 'Will-Delay-Interval' => 60}
    }),
    ok = emqtt:disconnect(B, 4),
    ?assertMatch(
        #{
            qos := 1,
            payload := <<"gone">>,
            properties := #{'Content-Type' := <<"text/plain">>}
        },
        message(A)
    ),
    emqtt:disconnect(A).

-doc "Return MQTT 5 errors and subscription reason codes for unsupported features".
t_reason_codes(_Config) ->
    lists:foreach(
        fun(#{header := Header, body := Body, reason := Reason}) ->
            A = raw_connect(#{}),
            send(A, Header, Body),
            ?assertEqual({16#E0, <<Reason, 0>>}, recv(A)),
            ?assertEqual({error, closed}, gen_tcp:recv(A, 0, 2000))
        end,
        [
            #{header => 16#34, body => <<1:16, "t", 1:16, 0>>, reason => 16#9B},
            #{header => 16#31, body => <<1:16, "t", 0>>, reason => 16#9A},
            #{header => 16#30, body => <<1:16, "t", 3, 16#23, 0, 1>>, reason => 16#94},
            #{header => 16#30, body => <<1:16, "t", 2, 16#0B, 1>>, reason => 16#82},
            #{header => 16#82, body => <<1:16, 2, 16#0B, 1, 1:16, "t", 0>>, reason => 16#A1},
            #{header => 16#F0, body => <<>>, reason => 16#82},
            #{header => 16#30, body => <<1:16, "t", 0, 0:32768>>, reason => 16#95}
        ]
    ),
    B = raw_connect(#{}),
    send(B, 16#82, <<1:16, 0, 10:16, "$share/g/t", 0, 5:16, "a/#/b", 0>>),
    ?assertEqual({16#90, <<1:16, 0, 16#9E, 16#8F>>}, recv(B)),
    raw_subscribe(B, <<"unsub">>, 1),
    send(B, 16#A2, <<2:16, 0, 5:16, "unsub", 6:16, "absent">>),
    ?assertEqual({16#B0, <<2:16, 0, 0, 16#11>>}, recv(B)).

-doc "Clear RETAIN on a live retained broker publish delivered to a native MQTT 3.1.1 client".
t_broker_retain(_Config) ->
    emqx_native_listener_SUITE:enable_regular(),
    A = client(18883, #{proto_ver => v4}),
    B = client(18885, #{proto_ver => v4}),
    {ok, _, _} = emqtt:subscribe(A, <<"retain5">>, 0),
    emqx_native_listener_SUITE:await_subscribers(<<"retain5">>, 1),
    ok = emqtt:publish(B, <<"retain5">>, <<"value">>, [{qos, 0}, {retain, true}]),
    ?assertMatch(#{retain := false, payload := <<"value">>}, message(A)),
    [emqtt:disconnect(Pid) || Pid <- [A, B]].

-doc "Send server-shutting-down to MQTT 5 clients before stopping the listener".
t_server_disconnect(_Config) ->
    A = raw_connect(#{}),
    ok = emqx_listeners:stop_listener('native:a'),
    ?assertEqual({16#E0, <<16#8B, 0>>}, recv(A)),
    ?assertEqual({error, closed}, gen_tcp:recv(A, 0, 2000)),
    ok = emqx_listeners:start_listener('native:a'),
    B = raw_connect(#{}),
    emqx_common_test_helpers:on_exit(fun() -> application:ensure_all_started(emqx_tokio) end),
    ok = application:stop(emqx_tokio),
    ?assertEqual({16#E0, <<16#8B, 0>>}, recv(B)),
    {ok, _} = application:ensure_all_started(emqx_tokio).

-doc "Reject invalid CONNECT properties and unsupported will features with CONNACK reason codes".
t_connect_errors(_Config) ->
    lists:foreach(
        fun(#{properties := Props, flags := Flags, payload := Payload, reason := Reason}) ->
            A = emqx_native_listener_SUITE:socket(18883),
            send(
                A,
                16#10,
                <<0, 4, "MQTT", 5, Flags, 0, 30, (byte_size(Props)), Props/binary, Payload/binary>>
            ),
            ?assertEqual({16#20, <<0, Reason, 0>>}, recv(A)),
            ?assertEqual({error, closed}, gen_tcp:recv(A, 0, 2000))
        end,
        [
            #{properties => <<16#21, 0, 0>>, flags => 2, payload => <<1:16, "c">>, reason => 16#82},
            #{properties => <<16#27, 0:32>>, flags => 2, payload => <<1:16, "c">>, reason => 16#82},
            #{
                properties => <<16#15, 1:16, "m">>,
                flags => 2,
                payload => <<1:16, "c">>,
                reason => 16#8C
            },
            #{
                properties => <<16#21, 0, 1, 16#21, 0, 1>>,
                flags => 2,
                payload => <<1:16, "c">>,
                reason => 16#82
            },
            #{
                properties => <<>>,
                flags => 16#26,
                payload => <<1:16, "c", 0, 1:16, "w", 1:16, "p">>,
                reason => 16#9A
            },
            #{
                properties => <<>>,
                flags => 16#16,
                payload => <<1:16, "c", 0, 1:16, "w", 1:16, "p">>,
                reason => 16#9B
            }
        ]
    ).

-doc "Limit MQTT 3.1.1 deliveries to the configured inflight window".
t_server_window(_Config) ->
    A = client(18883, #{}),
    B = emqx_native_listener_SUITE:connect(18884),
    send(B, 16#82, <<1:16, 6:16, "window", 1>>),
    ?assertEqual({16#90, <<1:16, 1>>}, recv(B)),
    [{ok, _} = emqtt:publish(A, <<"window">>, <<"x">>, 1) || _ <- lists:seq(1, 33)],
    [?assertEqual({16#32, <<6:16, "window", Id:16, "x">>}, recv(B)) || Id <- lists:seq(1, 32)],
    emqx_native_listener_SUITE:assert_empty(B),
    send(B, 16#40, <<1:16>>),
    ?assertEqual({16#32, <<6:16, "window", 33:16, "x">>}, recv(B)),
    emqtt:disconnect(A).

-doc "Return the keepalive reason code before closing an idle MQTT 5 connection".
t_keepalive_reason(_Config) ->
    A = emqx_native_listener_SUITE:socket(18883),
    send(A, 16#10, <<0, 4, "MQTT", 5, 2, 0, 1, 0, 1:16, "c">>),
    ?assertMatch({16#20, <<0, 0, _/binary>>}, recv(A)),
    ?assertEqual({16#E0, <<16#8D, 0>>}, recv(A)).

-doc "Deliver MQTT 5 QoS 1 and properties over TLS 1.3".
t_tls_mqtt5(Config) ->
    emqx_native_listener_SUITE:t_tls(Config),
    Options = #{ssl => true, ssl_opts => [{verify, verify_none}, {versions, ['tlsv1.3']}]},
    A = client(18883, Options),
    B = client(18883, Options),
    {ok, _, [1]} = emqtt:subscribe(B, <<"tls5">>, 1),
    {ok, _} = emqtt:publish(A, <<"tls5">>, properties(), <<"encrypted">>, [{qos, 1}]),
    #{qos := 1, payload := <<"encrypted">>, properties := Properties} = message(B),
    ?assertEqual(properties(), Properties),
    [emqtt:disconnect(Pid) || Pid <- [A, B]].

-doc "Accept a DISCONNECT session expiry only when CONNECT requested a non-zero expiry".
t_disconnect_session_expiry(_Config) ->
    A = client(18884, #{}),
    {ok, _, [0]} = emqtt:subscribe(A, <<"expiry5">>, 0),
    Disconnect = <<0, 5, 16#11, 30:32>>,
    %% A client that requested an expiry disconnects normally and its will is discarded
    B = emqx_native_listener_SUITE:socket(18883),
    send(
        B,
        16#10,
        <<0, 4, "MQTT", 5, 6, 0, 30, 5, 16#11, 60:32, 2:16, "c1", 0, 7:16, "expiry5", 4:16, "gone">>
    ),
    ?assertMatch({16#20, <<0, 0, _/binary>>}, recv(B)),
    send(B, 16#E0, Disconnect),
    ?assertEqual({error, closed}, gen_tcp:recv(B, 0, 2000)),
    receive
        {publish, #{client_pid := A}} -> ct:fail(will_published)
    after 200 -> ok
    end,
    %% A client that requested no expiry gets a protocol error
    C = raw_connect(#{}),
    send(C, 16#E0, Disconnect),
    ?assertEqual({16#E0, <<16#82, 0>>}, recv(C)),
    emqtt:disconnect(A).

-doc "Skip broker message properties that MQTT cannot encode and deliver the rest".
t_broker_invalid_properties(_Config) ->
    A = client(18883, #{}),
    {ok, _, [1]} = emqtt:subscribe(A, <<"badprops">>, 1),
    emqx_native_listener_SUITE:await_subscribers(<<"badprops">>, 1),
    Props = #{
        'Payload-Format-Indicator' => 5,
        'Content-Type' => <<"text/plain">>,
        'Response-Topic' => response,
        'User-Property' => [{<<"k">>, 1}, {<<"a">>, <<"b">>}]
    },
    Msg = emqx_message:make(<<"regular">>, 1, <<"badprops">>, <<"value">>, #{}, #{
        properties => Props
    }),
    _ = emqx_broker:publish(Msg),
    #{payload := <<"value">>, properties := Received} = message(A),
    ?assertEqual(
        #{'Content-Type' => <<"text/plain">>, 'User-Property' => [{<<"a">>, <<"b">>}]}, Received
    ),
    emqtt:disconnect(A).

-doc "Drop expired broker messages instead of delivering them to native subscribers".
t_broker_expired(_Config) ->
    A = client(18883, #{}),
    {ok, _, [1]} = emqtt:subscribe(A, <<"expired">>, 1),
    emqx_native_listener_SUITE:await_subscribers(<<"expired">>, 1),
    Props = #{properties => #{'Message-Expiry-Interval' => 1}},
    Msg0 = emqx_message:make(<<"regular">>, 1, <<"expired">>, <<"old">>, #{}, Props),
    Msg1 = Msg0#message{timestamp = erlang:system_time(millisecond) - 5000},
    _ = emqx_broker:publish(Msg1),
    Msg2 = emqx_message:make(<<"regular">>, 1, <<"expired">>, <<"new">>, #{}, Props),
    _ = emqx_broker:publish(Msg2),
    ?assertMatch(#{payload := <<"new">>}, message(A)),
    emqtt:disconnect(A).

-doc "Close without CONNACK when CONNECT is malformed or another packet comes first".
t_connect_close(_Config) ->
    lists:foreach(
        fun(#{header := Header, body := Body}) ->
            A = emqx_native_listener_SUITE:socket(18883),
            send(A, Header, Body),
            ?assertEqual({error, closed}, gen_tcp:recv(A, 0, 2000))
        end,
        [
            #{header => 16#10, body => <<0, 4, "MQTT", 4, 3, 0, 30, 1:16, "c">>},
            #{header => 16#C0, body => <<>>}
        ]
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

%% The broker lowers the message expiry interval by the time the message waited.
assert_properties(Received) ->
    #{'Message-Expiry-Interval' := Interval} = Received,
    ?assert(Interval > 590 andalso Interval =< 600),
    Expected = maps:remove('Message-Expiry-Interval', properties()),
    ?assertEqual(Expected, maps:remove('Message-Expiry-Interval', Received)).

client(Port, Options) ->
    {ok, Pid} = emqtt:start_link(
        maps:merge(#{host => "127.0.0.1", port => Port, proto_ver => v5}, Options)
    ),
    {ok, _} = emqtt:connect(Pid),
    Pid.

message(Pid) ->
    receive
        {publish, #{client_pid := Pid} = Message} -> Message
    after 5000 -> ct:fail({missing_publish, Pid})
    end.

properties() ->
    #{
        'Payload-Format-Indicator' => 1,
        'Message-Expiry-Interval' => 600,
        'Content-Type' => <<"text/plain">>,
        'Response-Topic' => <<"response">>,
        'Correlation-Data' => <<0, 1, 255>>,
        'User-Property' => [{<<"k">>, <<"1">>}, {<<"k">>, <<"2">>}]
    }.

raw_connect(Options) ->
    Socket = emqx_native_listener_SUITE:socket(18883),
    Props = maps:get(properties, Options, <<>>),
    send(Socket, 16#10, <<0, 4, "MQTT", 5, 2, 0, 30, (byte_size(Props)), Props/binary, 1:16, "c">>),
    ?assertMatch({16#20, <<0, 0, _/binary>>}, recv(Socket)),
    Socket.

raw_subscribe(Socket, Topic, Options) ->
    send(Socket, 16#82, <<1:16, 0, (byte_size(Topic)):16, Topic/binary, Options>>),
    ?assertEqual({16#90, <<1:16, 0, (min(Options band 3, 1))>>}, recv(Socket)).

send(Socket, Header, Body) -> emqx_native_listener_SUITE:send_packet(Socket, Header, Body).
recv(Socket) -> emqx_native_listener_SUITE:recv_packet(Socket).
