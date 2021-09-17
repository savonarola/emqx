%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% SUITE boilerplate
%%--------------------------------------------------------------------

all() ->
    [ {group, kill_connection_process}
    , {group, no_kill_connection_process}
    , {group, snabbkaffe}
    ].

%% A persistent session can be resumed in two ways:
%%    1. The old connection process is still alive, and the session is taken
%%       over by the new connection.
%%    2. The old session process has died (e.g., because of node down).
%%       The new process resumes the session from the stored state, and finds
%%       any subscribed messages from the persistent message store.
%%
%% We want to test both these implementations, which is done through the top
%% level groups {no_}kill_connection_process.
%%
%% In addition, we test both tcp and quic connections for both scenarios.

groups() ->
    TCs = emqx_ct:all(?MODULE),
    SnabbkaffeTCs = [TC || TC <- TCs, is_snabbkaffe_tc(TC)],
    OtherTCs = TCs -- SnabbkaffeTCs,
    [ {no_kill_connection_process, [], [{group, tcp}, {group, quic}]}
    , {   kill_connection_process, [], [{group, tcp}, {group, quic}]}
    , {snabbkaffe, [], [{group, tcp_snabbkaffe}, {group, quic_snabbkaffe}]}
    , {tcp,  [], OtherTCs}
    , {quic, [], OtherTCs}
    , {tcp_snabbkaffe,  [], SnabbkaffeTCs}
    , {quic_snabbkaffe, [], SnabbkaffeTCs}
    ].

is_snabbkaffe_tc(TC) ->
    re:run(atom_to_list(TC), "snabbkaffe") /= nomatch.

init_per_group(Group, Config) when Group == tcp; Group == tcp_snabbkaffe ->
    [ {port, 1883}, {conn_fun, connect}| Config];
init_per_group(Group, Config) when Group == quic; Group == quic_snabbkaffe ->
    [ {port, 14567}, {conn_fun, quic_connect} | Config];
init_per_group(no_kill_connection_process, Config) ->
    [ {kill_connection_process, false} | Config];
init_per_group(kill_connection_process, Config) ->
    [ {kill_connection_process, true} | Config];
init_per_group(snabbkaffe, Config) ->
    [ {kill_connection_process, true} | Config].


init_per_suite(Config) ->
    %% Start Apps
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([emqx], fun set_special_confs/1),
    Config.

set_special_confs(emqx) ->
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"));
set_special_confs(_) ->
    ok.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Config1 = preconfig_per_testcase(TestCase, Config),
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config1);
        _ -> Config1
    end.

end_per_testcase(TestCase, Config) ->
    case is_snabbkaffe_tc(TestCase) of
        true  -> snabbkaffe:stop();
        false -> skip
    end,
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

preconfig_per_testcase(TestCase, Config) ->
    {BaseName, Config1} =
        case ?config(tc_group_properties, Config) of
            [] ->
                %% We are running a single testcase
                {atom_to_binary(TestCase),
                 init_per_group(tcp, init_per_group(kill_connection_process, Config))};
            [{name, GroupName}] ->
                [[{name,TopName}]] = ?config(tc_group_path, Config),
                {iolist_to_binary([atom_to_binary(TopName), "_",
                                   atom_to_binary(GroupName), "_",
                                   atom_to_binary(TestCase)]),
                 Config}
        end,
    [ {topic, iolist_to_binary([BaseName, "/foo"])}
    , {stopic, iolist_to_binary([BaseName, "/+"])}
    , {stopic_alt, iolist_to_binary([BaseName, "/foo"])}
    , {client_id, BaseName}
    | Config1].

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_info(Key, Client) ->
    maps:get(Key, maps:from_list(emqtt:info(Client)), undefined).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count-1, [Msg|Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 1000 ->
        Msgs
    end.

maybe_kill_connection_process(ClientId, Config) ->
    case ?config(kill_connection_process, Config) of
        true ->
            [ConnectionPid] = emqx_cm:lookup_channels(ClientId),
            ?assert(is_pid(ConnectionPid)),
            ConnectionPid ! die_if_test,
            ok;
        false ->
            ok
    end.

publish(Topic, Payloads = [_|_], Config) ->
    %% Publish from another process to avoid connection confusion.
    {Pid, Ref} =
        spawn_monitor(
          fun() ->
                  ConnFun = ?config(conn_fun, Config),
                  {ok, Client} = emqtt:start_link([ {proto_ver, v5}
                                                  | Config]),
                  {ok, _} = emqtt:ConnFun(Client),
                  lists:foreach(fun(Payload) ->
                                        {ok, _} = emqtt:publish(Client, Topic, Payload, 2)
                                end, Payloads),
                  ok = emqtt:disconnect(Client)
          end),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok;
        {'DOWN', Ref, process, Pid, What} -> error({failed_publish, What})
    end;
publish(Topic, Payload, Config) ->
    publish(Topic, [Payload], Config).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

%% [MQTT-3.1.2-23]
t_connect_session_expiry_interval(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload = <<"test message">>,
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload, Config),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    [Msg | _ ] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg)),
    ok = emqtt:disconnect(Client2).

t_without_client_id(Config) ->
    process_flag(trap_exit, true), %% Emqtt client dies
    ConnFun = ?config(conn_fun, Config),
    {ok, Client0} = emqtt:start_link([ {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {error, {client_identifier_not_valid, _}} = emqtt:ConnFun(Client0),
    ok.

t_assigned_clientid_persistent_session(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),

    AssignedClientId = client_info(clientid, Client1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(AssignedClientId, Config),

    {ok, Client2} = emqtt:start_link([ {clientid, AssignedClientId},
                                       {proto_ver, v5},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(1, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_cancel_on_disconnect(Config) ->
    %% Open a persistent session, but cancel the persistence when
    %% shutting down the connection.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 0}),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {clean_start, false},
                                       {properties, #{'Session-Expiry-Interval' => 30}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_persist_on_disconnect(Config) ->
    %% Open a non-persistent session, but add the persistence when
    %% shutting down the connection. This is a protocol error, and
    %% should not convert the session into a persistent session.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 0}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),

    %% Strangely enough, the disconnect is reported as successful by emqtt.
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 30}),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {clean_start, false},
                                       {properties, #{'Session-Expiry-Interval' => 30}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    %% The session should not be known, since it wasn't persisted because of the
    %% changed expiry interval in the disconnect call.
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_process_dies_session_expires(Config) ->
    %% Emulate an error in the connect process,
    %% or that the node of the process goes down.
    %% A persistent session should eventually expire.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 1}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    timer:sleep(1000),

    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    emqtt:disconnect(Client2).

t_publish_while_client_is_gone(Config) ->
    %% A persistent session should receive messages in its
    %% subscription even if the process owning the session dies.
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    ClientId = ?config(client_id, Config),
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ok = publish(Topic, [Payload1, Payload2], Config),

    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    [Msg1] = receive_messages(1),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload1)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),

    ok = emqtt:disconnect(Client2).

t_clean_start_drops_subscriptions(Config) ->
    %% 1. A persistent session is started and disconnected.
    %% 2. While disconnected, a message is published and persisted.
    %% 3. When connecting again, the clean start flag is set, the subscription is renewed,
    %%    then we disconnect again.
    %% 4. Finally, a new connection is made with clean start set to false.
    %% The original message should not be delivered.

    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    Payload3 = <<"hello3">>,
    ClientId = ?config(client_id, Config),

    %% 1.
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    %% 2.
    ok = publish(Topic, Payload1, Config),

    %% 3.
    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    {ok, _, [2]} = emqtt:subscribe(Client2, STopic, qos2),

    ok = publish(Topic, Payload2, Config),
    [Msg1] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg1)),

    ok = emqtt:disconnect(Client2),
    maybe_kill_connection_process(ClientId, Config),

    %% 4.
    {ok, Client3} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client3),

    ok = publish(Topic, Payload3, Config),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload3)}, maps:find(payload, Msg2)),

    ok = emqtt:disconnect(Client3).


t_multiple_subscription_matches(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic1 = ?config(stopic, Config),
    STopic2 = ?config(stopic_alt, Config),
    Payload = <<"test message">>,
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic1, qos2),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic2, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload, Config),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),

    %% We will receive the same message twice because it matches two subscriptions.
    [Msg1, Msg2] = receive_messages(2),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg2)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),
    ok = emqtt:disconnect(Client2).



%%--------------------------------------------------------------------
%% Snabbkaffe helpers
%%--------------------------------------------------------------------

check_snabbkaffe_vanilla(Trace) ->
    ResumeTrace = [T || #{?snk_kind := K} = T <- Trace,
                        re:run(atom_to_list(K), "^ps_") /= nomatch],
    ?assertMatch([_|_], ResumeTrace),
    [_Sid] = lists:usort(?projection(sid, ResumeTrace)),
    %% Check internal flow of the emqx_cm resuming
    ?assert(?strict_causality(#{ ?snk_kind := ps_resuming },
                              #{ ?snk_kind := ps_initial_pendings },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_initial_pendings },
                              #{ ?snk_kind := ps_persist_pendings },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_persist_pendings },
                              #{ ?snk_kind := ps_notify_writers },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_notify_writers },
                              #{ ?snk_kind := ps_resume_session },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_resume_session },
                              #{ ?snk_kind := ps_marker_pendings },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_marker_pendings },
                              #{ ?snk_kind := ps_resume_end },
                              ResumeTrace)),

    %% Check flow between worker and emqx_cm
    ?assert(?strict_causality(#{ ?snk_kind := ps_notify_writers },
                              #{ ?snk_kind := ps_worker_started },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_marker_pendings },
                              #{ ?snk_kind := ps_worker_resume_end },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_worker_resume_end },
                              #{ ?snk_kind := ps_worker_shutdown },
                              ResumeTrace)).

%%--------------------------------------------------------------------
%% Snabbkaffe tests
%%--------------------------------------------------------------------

t_snabbkaffe_vanilla_stages(Config) ->
    %% Test that all stages of session resume works ok in the simplest case
    process_flag(trap_exit, true),
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    EmqttOpts = [ {proto_ver, v5},
                  {clientid, ClientId},
                  {properties, #{'Session-Expiry-Interval' => 30}}
                | Config],
    {ok, Client1} = emqtt:start_link([{clean_start, true}|EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ?check_trace(
       begin
           {ok, Client2} = emqtt:start_link([{clean_start, false}|EmqttOpts]),
           {ok, _} = emqtt:ConnFun(Client2),
           ok = emqtt:disconnect(Client2)
       end,
       fun(ok, Trace) ->
               check_snabbkaffe_vanilla(Trace)
       end),
    ok.

t_snabbkaffe_pending_messages(Config) ->
    %% Make sure there are pending messages are fetched during the init stage.
    process_flag(trap_exit, true),
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payloads = [<<"test", (integer_to_binary(X))/binary>> || X <- [1,2,3,4,5]],
    EmqttOpts = [ {proto_ver, v5},
                  {clientid, ClientId},
                  {properties, #{'Session-Expiry-Interval' => 30}}
                | Config],
    {ok, Client1} = emqtt:start_link([{clean_start, true} | EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payloads, Config),

    ?check_trace(
       begin
           {ok, Client2} = emqtt:start_link([{clean_start, false} | EmqttOpts]),
           {ok, _} = emqtt:ConnFun(Client2),
           Msgs = receive_messages(length(Payloads)),
           ReceivedPayloads = [P || #{ payload := P } <- Msgs],
           ?assertEqual(lists:sort(ReceivedPayloads),
                        lists:sort(Payloads)),
           ok = emqtt:disconnect(Client2)
       end,
       fun(ok, Trace) ->
               check_snabbkaffe_vanilla(Trace),
               %% Check that all messages was delivered in the initial pendings sweep
               [Delivers] = ?projection(msgs, ?of_kind(ps_persist_pendings_msgs, Trace)),
               ?assertEqual(length(Delivers), length(Payloads))
       end),
    ok.

t_snabbkaffe_buffered_messages(Config) ->
    %% Make sure to buffer messages during startup.
    process_flag(trap_exit, true),
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payloads1 = [<<"test", (integer_to_binary(X))/binary>> || X <- [1, 2, 3]],
    Payloads2 = [<<"test", (integer_to_binary(X))/binary>> || X <- [4, 5, 6]],
    EmqttOpts = [ {proto_ver, v5},
                  {clientid, ClientId},
                  {properties, #{'Session-Expiry-Interval' => 30}}
                | Config],
    {ok, Client1} = emqtt:start_link([{clean_start, true} | EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payloads1, Config),

    ?check_trace(
       begin
           %% Make the resume init phase wait until the first message is delivered.
           ?force_ordering( #{ ?snk_kind := ps_worker_deliver },
                            #{ ?snk_kind := ps_resume_end }),
           spawn_link(fun() ->
                              ?block_until(#{ ?snk_kind := ps_marker_pendings_msgs }, infinity, 5000),
                              publish(Topic, Payloads2, Config)
                      end),
           {ok, Client2} = emqtt:start_link([{clean_start, false} | EmqttOpts]),
           {ok, _} = emqtt:ConnFun(Client2),
           Msgs = receive_messages(length(Payloads1) + length(Payloads2) + 1),
           ReceivedPayloads = [P || #{ payload := P } <- Msgs],
           ?assertEqual(lists:sort(Payloads1 ++ Payloads2),
                        lists:sort(ReceivedPayloads)),
           ok = emqtt:disconnect(Client2)
       end,
       fun(ok, Trace) ->
               check_snabbkaffe_vanilla(Trace),
               %% Check that some messages was buffered in the writer process
               [Msgs] = ?projection(msgs, ?of_kind(ps_writer_pendings, Trace)),
               ?assertMatch(X when 0 < X andalso X =< length(Payloads2),
                            length(Msgs))
       end),
    ok.
