%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_eviction_agent]).

init_per_testcase(Config) ->
    _ = emqx_eviction_agent:disable(foo),
    Config.

end_per_testcase(_Config) ->
    _ = emqx_eviction_agent:disable(foo).

t_enable_disable(_Config) ->
    erlang:process_flag(trap_exit, true),

    ?assertMatch(
       disabled,
       emqx_eviction_agent:status()),

    {ok, C0} = emqtt_connect(),
    ok = emqtt:disconnect(C0),

    ok = emqx_eviction_agent:enable(foo, undefined),

    ?assertMatch(
       {error, eviction_agent_busy},
       emqx_eviction_agent:enable(bar, undefined)),

    ?assertMatch(
       ok,
       emqx_eviction_agent:enable(foo, <<"srv">>)),

    ?assertMatch(
        {enabled, #{}},
        emqx_eviction_agent:status()),

    ?assertMatch(
       {error, {use_another_server, #{}}},
       emqtt_connect()),

    ?assertMatch(
        {error, eviction_agent_busy},
        emqx_eviction_agent:disable(bar)),

    ?assertMatch(
        ok,
        emqx_eviction_agent:disable(foo)),

    ?assertMatch(
        {error, disabled},
        emqx_eviction_agent:disable(foo)),

    ?assertMatch(
       disabled,
       emqx_eviction_agent:status()),

    {ok, C1} = emqtt_connect(),
    ok = emqtt:disconnect(C1).


t_evict_connections_status(_Config) ->
    erlang:process_flag(trap_exit, true),

    {ok, _C} = emqtt_connect(),

    {error, disabled} = emqx_eviction_agent:evict_connections(1),

    ok = emqx_eviction_agent:enable(foo, undefined),

    ?assertMatch(
       {enabled, #{connections := 1, sessions := _}},
       emqx_eviction_agent:status()),

    ok = emqx_eviction_agent:evict_connections(1),

    ct:sleep(100),

    ?assertMatch(
       {enabled, #{connections := 0, sessions := _}},
       emqx_eviction_agent:status()),

    ok = emqx_eviction_agent:disable(foo).


t_explicit_session_takeover(_Config) ->
    erlang:process_flag(trap_exit, true),

    {ok, C0} = emqtt_connect(<<"client_with_session">>, false),
    {ok, _, _} = emqtt:subscribe(C0, <<"t1">>),

    ok = emqx_eviction_agent:enable(foo, undefined),

    ?assertEqual(
       1,
       emqx_eviction_agent:connection_count()),

    ok = emqx_eviction_agent:evict_connections(1),

    receive
        {'EXIT', C0, {disconnected, ?RC_USE_ANOTHER_SERVER, _}} -> ok
    after 1000 ->
              ?assert(false, "Connection not evicted")
    end,

    ?assertEqual(
       0,
       emqx_eviction_agent:connection_count()),

    ?assertEqual(
       1,
       emqx_eviction_agent:session_count()),

    ?check_trace(
       ?wait_async_action(
          emqx_eviction_agent:evict_sessions(1, node()),
          #{?snk_kind := emqx_eviction_agent_channel_session_opened},
          1000),
       fun(_Result, Trace) ->
        ?assertMatch(
           [#{clientid := <<"client_with_session">>} | _ ],
           ?of_kind(emqx_eviction_agent_channel_session_opened, Trace))
       end),

    ?assertEqual(
       0,
       emqx_eviction_agent:session_count()),

    ok = emqx_eviction_agent:disable(foo),

    {ok, C1} = emqtt_connect(),
    emqtt:publish(C1, <<"t1">>, <<"MessageToEvictedSession">>),
    ok = emqtt:disconnect(C1),

    {ok, C2} = emqtt_connect(<<"client_with_session">>, false),

    receive
        {publish, #{payload := <<"MessageToEvictedSession">>,
                    topic := <<"t1">>}} -> ok
    after 1000 ->
              ?assert(false, "Message `MessageToEvictedSession` is lost")
    end,
    ok = emqtt:disconnect(C2).

emqtt_connect() ->
    emqtt_connect(<<"client1">>, true).

emqtt_connect(ClientId, CleanStart) ->
    {ok, C} = emqtt:start_link(
                [{clientid, ClientId},
                 {clean_start, CleanStart},
                 {proto_ver, v5},
                 {properties, #{'Session-Expiry-Interval' => 60}}
                ]),
    case emqtt:connect(C) of
        {ok, _} -> {ok, C};
        {error, _} = Error -> Error
    end.
