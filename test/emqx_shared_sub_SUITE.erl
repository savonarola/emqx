%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_shared_sub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SUITE, ?MODULE).

-define(wait(For, Timeout),
        emqx_ct_helpers:wait_for(
          ?FUNCTION_NAME, ?LINE, fun() -> For end, Timeout)).

-define(ack, shared_sub_ack).
-define(no_ack, no_ack).

all() -> emqx_ct:all(?SUITE).

init_per_suite(Config) ->
    net_kernel:start(['master@127.0.0.1', longnames]),
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_is_ack_required(_) ->
    ?assertEqual(false, emqx_shared_sub:is_ack_required(#message{headers = #{}})).

t_maybe_nack_dropped(_) ->
    ?assertEqual(ok, emqx_shared_sub:maybe_nack_dropped(#message{headers = #{}})),
    Msg = #message{headers = #{shared_dispatch_ack => {self(), for_test}}},
    ?assertEqual(ok, emqx_shared_sub:maybe_nack_dropped(Msg)),
    ?assertEqual(ok,receive {for_test, {shared_sub_nack, dropped}} -> ok after 100 -> timeout end).

t_nack_no_connection(_) ->
    Msg = #message{headers = #{shared_dispatch_ack => {self(), for_test}}},
    ?assertEqual(ok, emqx_shared_sub:nack_no_connection(Msg)),
    ?assertEqual(ok,receive {for_test, {shared_sub_nack, no_connection}} -> ok
                    after 100 -> timeout end).

t_maybe_ack(_) ->
    ?assertEqual(#message{headers = #{}}, emqx_shared_sub:maybe_ack(#message{headers = #{}})),
    Msg = #message{headers = #{shared_dispatch_ack => {self(), for_test}}},
    ?assertEqual(#message{headers = #{shared_dispatch_ack => ?no_ack}},
                 emqx_shared_sub:maybe_ack(Msg)),
    ?assertEqual(ok,receive {for_test, ?ack} -> ok after 100 -> timeout end).

% t_subscribers(_) ->
%     error('TODO').

t_random_basic(_) ->
    ok = ensure_config(random),
    ClientId = <<"ClientId">>,
    Topic = <<"foo">>,
    Payload = <<"hello">>,
    emqx:subscribe(Topic, #{qos => 2, share => <<"group1">>}),
    MsgQoS2 = emqx_message:make(ClientId, 2, Topic, Payload),
    %% wait for the subscription to show up
    ct:sleep(200),
    ?assertEqual(true, subscribed(<<"group1">>, Topic, self())),
    emqx:publish(MsgQoS2),
    receive
        {deliver, Topic0, #message{from = ClientId0,
                                   payload = Payload0}} = M->
        ct:pal("==== received: ~p", [M]),
        ?assertEqual(Topic, Topic0),
        ?assertEqual(ClientId, ClientId0),
        ?assertEqual(Payload, Payload0)
    after 1000 -> ct:fail(waiting_basic_failed)
    end,
    ok.

%% Start two subscribers share subscribe to "$share/g1/foo/bar"
%% Set 'sticky' dispatch strategy, send 1st message to find
%% out which member it picked, then close its connection
%% send the second message, the message should be 'nack'ed
%% by the sticky session and delivered to the 2nd session.
%% After the connection for the 2nd session is also closed,
%% i.e. when all clients are offline, the following message(s)
%% should be delivered randomly.
t_no_connection_nack(_) ->
    ok = ensure_config(sticky),
    Publisher = <<"publisher">>,
    Subscriber1 = <<"Subscriber1">>,
    Subscriber2 = <<"Subscriber2">>,
    QoS = 1,
    Group = <<"g1">>,
    Topic = <<"foo/bar">>,
    ShareTopic = <<"$share/", Group/binary, $/, Topic/binary>>,

    ExpProp = [{properties, #{'Session-Expiry-Interval' => timer:seconds(30)}}],
    {ok, SubConnPid1} = emqtt:start_link([{clientid, Subscriber1}] ++ ExpProp),
    {ok, _Props} = emqtt:connect(SubConnPid1),
    {ok, SubConnPid2} = emqtt:start_link([{clientid, Subscriber2}] ++ ExpProp),
    {ok, _Props} = emqtt:connect(SubConnPid2),
    emqtt:subscribe(SubConnPid1, ShareTopic, QoS),
    emqtt:subscribe(SubConnPid1, ShareTopic, QoS),

    %% wait for the subscriptions to show up
    ct:sleep(200),
    MkPayload = fun(PacketId) ->
                    iolist_to_binary(["hello-", integer_to_list(PacketId)])
                end,
    SendF = fun(PacketId) ->
                M = emqx_message:make(Publisher, QoS, Topic, MkPayload(PacketId)),
                emqx:publish(M#message{id = PacketId})
            end,
    SendF(1),
    timer:sleep(200),
    %% This is the connection which was picked by broker to dispatch (sticky) for 1st message

    ?assertMatch([#{packet_id := 1}], recv_msgs(1)),
    %% Now kill the connection, expect all following messages to be delivered to the other
    %% subscriber.
    %emqx_mock_client:stop(ConnPid),
    %% sleep then make synced calls to session processes to ensure that
    %% the connection pid's 'EXIT' message is propagated to the session process
    %% also to be sure sessions are still alive
    %   timer:sleep(2),
    %   _ = emqx_session:info(SPid1),
    %   _ = emqx_session:info(SPid2),
    %   %% Now we know what is the other still alive connection
    %   [TheOtherConnPid] = [SubConnPid1, SubConnPid2] -- [ConnPid],
    %   %% Send some more messages
    %   PacketIdList = lists:seq(2, 10),
    %   lists:foreach(fun(Id) ->
    %                         SendF(Id),
    %                         ?wait(Received(Id, TheOtherConnPid), 1000)
    %                 end, PacketIdList),
    %   %% Now close the 2nd (last connection)
    %   emqx_mock_client:stop(TheOtherConnPid),
    %   timer:sleep(2),
    %   %% both sessions should have conn_pid = undefined
    %   ?assertEqual({conn_pid, undefined}, lists:keyfind(conn_pid, 1, emqx_session:info(SPid1))),
    %   ?assertEqual({conn_pid, undefined}, lists:keyfind(conn_pid, 1, emqx_session:info(SPid2))),
    %   %% send more messages, but all should be queued in session state
    %   lists:foreach(fun(Id) -> SendF(Id) end, PacketIdList),
    %   {_, L1} = lists:keyfind(mqueue_len, 1, emqx_session:info(SPid1)),
    %   {_, L2} = lists:keyfind(mqueue_len, 1, emqx_session:info(SPid2)),
    %   ?assertEqual(length(PacketIdList), L1 + L2),
    %   %% clean up
    %   emqx_mock_client:close_session(PubConnPid),
    %   emqx_sm:close_session(SPid1),
    %   emqx_sm:close_session(SPid2),
    ok.

t_random(_) ->
    test_two_messages(random).

t_round_robin(_) ->
    test_two_messages(round_robin).

t_sticky(_) ->
    test_two_messages(sticky).

t_hash(_) ->
    test_two_messages(hash, false).

t_hash_clinetid(_) ->
    test_two_messages(hash_clientid, false).

t_hash_topic(_) ->
    ok = ensure_config(hash_topic, false),
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}]),
    {ok, _} = emqtt:connect(ConnPid2),

    Topic1 = <<"foo/bar1">>,
    Topic2 = <<"foo/bar2">>,
    ?assert(erlang:phash2(Topic1) rem 2 =/= erlang:phash2(Topic2) rem 2),
    Message1 = emqx_message:make(ClientId1, 0, Topic1, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 0, Topic2, <<"hello2">>),
    emqtt:subscribe(ConnPid1, {<<"$share/group1/foo/#">>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/group1/foo/#">>, 0}),
    ct:sleep(100),
    emqx:publish(Message1),
    Me = self(),
    WaitF = fun(ExpectedPayload) ->
                    case last_message(ExpectedPayload, [ConnPid1, ConnPid2]) of
                        {true, Pid} ->
                            Me ! {subscriber, Pid},
                            true;
                        Other ->
                            Other
                    end
            end,
    WaitF(<<"hello1">>),
    UsedSubPid1 = receive {subscriber, P1} -> P1 end,
    emqx_broker:publish(Message2),
    WaitF(<<"hello2">>),
    UsedSubPid2 = receive {subscriber, P2} -> P2 end,
    ?assert(UsedSubPid1 =/= UsedSubPid2),
    emqtt:stop(ConnPid1),
    emqtt:stop(ConnPid2),
    ok.

%% if the original subscriber dies, change to another one alive
t_not_so_sticky(_) ->
    ok = ensure_config(sticky),
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, C1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link([{clientid, ClientId2}]),
    {ok, _} = emqtt:connect(C2),

    emqtt:subscribe(C1, {<<"$share/group1/foo/bar">>, 0}),
    timer:sleep(50),
    emqtt:publish(C2, <<"foo/bar">>, <<"hello1">>),
    ?assertMatch([#{payload := <<"hello1">>}], recv_msgs(1)),

    emqtt:unsubscribe(C1, <<"$share/group1/foo/bar">>),
    timer:sleep(50),
    emqtt:subscribe(C1, {<<"$share/group1/foo/#">>, 0}),
    timer:sleep(50),
    emqtt:publish(C2, <<"foo/bar">>, <<"hello2">>),
    ?assertMatch([#{payload := <<"hello2">>}], recv_msgs(1)),
    emqtt:disconnect(C1),
    emqtt:disconnect(C2),
    ok.

test_two_messages(Strategy) ->
    test_two_messages(Strategy, _WithAck = true).

test_two_messages(Strategy, WithAck) ->
    ok = ensure_config(Strategy, WithAck),
    Topic = <<"foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}]),
    {ok, _} = emqtt:connect(ConnPid2),

    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 0, Topic, <<"hello2">>),
    emqtt:subscribe(ConnPid1, {<<"$share/group1/foo/bar">>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/group1/foo/bar">>, 0}),
    ct:sleep(100),
    emqx:publish(Message1),
    Me = self(),
    WaitF = fun(ExpectedPayload) ->
                    case last_message(ExpectedPayload, [ConnPid1, ConnPid2]) of
                        {true, Pid} ->
                            Me ! {subscriber, Pid},
                            true;
                        Other ->
                            Other
                    end
            end,
    WaitF(<<"hello1">>),
    UsedSubPid1 = receive {subscriber, P1} -> P1 end,
    emqx_broker:publish(Message2),
    WaitF(<<"hello2">>),
    UsedSubPid2 = receive {subscriber, P2} -> P2 end,
    case Strategy of
        sticky -> ?assert(UsedSubPid1 =:= UsedSubPid2);
        round_robin -> ?assert(UsedSubPid1 =/= UsedSubPid2);
        hash -> ?assert(UsedSubPid1 =:= UsedSubPid2);
        _ -> ok
    end,
    emqtt:stop(ConnPid1),
    emqtt:stop(ConnPid2),
    ok.

last_message(ExpectedPayload, Pids) ->
    receive
        {publish, #{client_pid := Pid, payload := ExpectedPayload}} ->
            ct:pal("~p ====== ~p", [Pids, Pid]),
            {true, Pid}
    after 100 ->
        <<"not yet?">>
    end.

t_dispatch(_) ->
    ok = ensure_config(random),
    Topic = <<"foo">>,
    ?assertEqual({error, no_subscribers},
                 emqx_shared_sub:dispatch(<<"group1">>, Topic, #delivery{message = #message{}})),
    emqx:subscribe(Topic, #{qos => 2, share => <<"group1">>}),
    ?assertEqual({ok, 1},
                 emqx_shared_sub:dispatch(<<"group1">>, Topic, #delivery{message = #message{}})).

% t_unsubscribe(_) ->
%     error('TODO').

% t_subscribe(_) ->
%     error('TODO').
t_uncovered_func(_) ->
    ignored = gen_server:call(emqx_shared_sub, ignored),
    ok = gen_server:cast(emqx_shared_sub, ignored),
    ignored = emqx_shared_sub ! ignored,
    {mnesia_table_event, []} = emqx_shared_sub ! {mnesia_table_event, []}.

t_local(_) ->
    ok = ensure_group_config([{<<"local_group">>, local}]),

    Me = self(),
    Topic = <<"local_foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    Node = start_slave('local_shared_sub_test', [emqx_modules, emqx_management]),

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),

    ct:pal("Here"),
    erlang:spawn(Node, fun() ->
                               Res = emqtt:start_link([{clientid, ClientId2}, {owner, Me}]),
                               (Me ! Res),
                               _ = receive
                                   _ -> ok
                               end
                       end),
    ct:pal("Here"),

    ConnPid2 = receive
                   {ok, ReceivedConnPid} -> ReceivedConnPid
               after 5000 -> exit(fuck)
               end,

    ct:pal("ConnPid2: ~p", [ConnPid2]),
    ct:pal("ConnPid2 alive? ~p", [rpc:call(Node, erlang, is_process_alive, [ConnPid2])]),
    ct:sleep(10),
    ct:pal("ConnPid2 alive? another ~p", [rpc:call(Node, erlang, is_process_alive, [ConnPid2])]),
    ct:sleep(5000),
    ct:pal("ConnPid2 alive? second ~p", [rpc:call(Node, erlang, is_process_alive, [ConnPid2])]),

    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = rpc:call(Node, emqtt, connect, [ConnPid2]),

    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 0, Topic, <<"hello2">>),

    emqtt:subscribe(ConnPid1, {<<"$share/local_group/local_foo/bar">>, 0}),

    rpc:call(Node, emqtt, subscribe, [ConnPid2, {<<"$share/local_group/local_foo/bar">>, 0}]),

    ct:pal("ConnPid2 alive? third ~p", [rpc:call(Node, erlang, is_process_alive, [ConnPid2])]),
    ct:sleep(100),

    WaitF = fun(ExpectedPayload) ->
                    case last_message(ExpectedPayload, [ConnPid1, ConnPid2]) of
                        {true, Pid} ->
                            Me ! {subscriber, Pid},
                            true;
                        Other ->
                            Other
                    end
            end,

    emqx:publish(Message1),
    WaitF(<<"hello1">>),
    UsedSubPid1 = receive {subscriber, P1} -> P1 end,

    rpc:call(Node, emqx, publish, [Message2]),
    WaitF(<<"hello2">>),
    UsedSubPid2 = receive {subscriber, P2} -> P2 end,

    ?assert(UsedSubPid1 =/= UsedSubPid2),

    emqtt:stop(ConnPid1),
    emqtt:stop(ConnPid2),
    ok.

%%--------------------------------------------------------------------
%% help functions
%%--------------------------------------------------------------------

ensure_config(Strategy) ->
    ensure_config(Strategy, _AckEnabled = true).

ensure_config(Strategy, AckEnabled) ->
    application:set_env(emqx, shared_subscription_strategy, Strategy),
    application:set_env(emqx, shared_dispatch_ack_enabled, AckEnabled),
    ok.

ensure_group_config(Group2Strategy) ->
    application:set_env(emqx, shared_subscription_strategy_per_group, Group2Strategy),
    ok.

subscribed(Group, Topic, Pid) ->
    lists:member(Pid, emqx_shared_sub:subscribers(Group, Topic)).

recv_msgs(Count) ->
    recv_msgs(Count, []).

recv_msgs(0, Msgs) ->
    Msgs;
recv_msgs(Count, Msgs) ->
    receive
        {publish, Msg} ->
            recv_msgs(Count-1, [Msg|Msgs]);
        _Other -> recv_msgs(Count, Msgs) %%TODO:: remove the branch?
    after 100 ->
        Msgs
    end.

start_slave(Name, Apps) ->
    {ok, Node} = ct_slave:start(list_to_atom(atom_to_list(Name) ++ "@" ++ host()),
                                [{kill_if_fail, true},
                                 {monitor_master, true},
                                 {init_timeout, 10000},
                                 {startup_timeout, 10000},
                                 {erl_flags, ebin_path()}]),

    pong = net_adm:ping(Node),
    setup_node(Node, Apps),
    Node.

stop_slave(Node, Apps) ->
    [ok = Res || Res <- rpc:call(Node, emqx_ct_helpers, stop_apps, [Apps])],
    rpc:call(Node, ekka, leave, []),
    ct_slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

setup_node(Node, Apps) ->
    EnvHandler =
        fun(emqx) ->
                application:set_env(emqx, listeners, []),
                application:set_env(gen_rpc, port_discovery, manual),
                ok;
           (emqx_management) ->
                application:set_env(emqx_management, listeners, []),
                ok;
           (emqx_dashboard) ->
                application:set_env(emqx_dashboard, listeners, []),
                ok;
           (_) ->
                ok
        end,

    [ok = rpc:call(Node, application, load, [App]) || App <- [gen_rpc, emqx | Apps]],
    ok = rpc:call(Node, emqx_ct_helpers, start_apps, [Apps, EnvHandler]),

    rpc:call(Node, ekka, join, [node()]),
    rpc:call(Node, application, stop, [emqx_dashboard]),
    rpc:call(Node, application, start, [emqx_dashboard]),

    ok.
