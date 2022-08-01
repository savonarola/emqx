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

-module(emqx_dispatch_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(OTHER_NODE_PORT, 11885).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    GenRpcOpts = emqx_node_helpers:init_gen_rpc(),
    emqx_ct_helpers:start_apps([]),
    GenRpcOpts ++ Config.

end_per_suite(Config) ->
    _ = emqx_node_helpers:deinit_gen_rpc(Config),
    emqx_ct_helpers:stop_apps([]).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

t_session_takeover(_Config) ->
    erlang:process_flag(trap_exit, true),

    Listeners = [#{listen_on => {{127,0,0,1}, ?OTHER_NODE_PORT},
                   name => "internal",
                   opts => [{zone,internal}],
                   proto => tcp}],
    Node = emqx_node_helpers:start_slave(
             test1,
             #{listeners => Listeners}),

    % Create a session on the remote node
    {ok, C0} = connect_with_session(<<"client_with_session">>, ?OTHER_NODE_PORT),
    {ok, _, _} = emqtt:subscribe(C0, <<"t1">>),
    ok = emqtt:stop(C0),

    % Publish message to the session
    ok = connect_and_publish(<<"t1">>, <<"Message">>),

    %% If uncomment, the test will always pass
    %% ct:sleep(100),

    % Start takeover immediately
    {ok, C1} = connect_with_session(<<"client_with_session">>),

    receive
        {publish, #{payload := <<"Message">>,
                    topic := <<"t1">>}} -> ok
    after 1000 ->
                ct:fail("Message is lost")
    end,

    ok = emqtt:disconnect(C1),
    _ = emqx_node_helpers:stop_slave(Node).

connect_and_publish(Topic, Message) ->
    {ok, C} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C),
    _ = emqtt:publish(C, Topic, Message),
    ok = emqtt:stop(C).

connect_with_session(ClientId) ->
    connect_with_session(ClientId, []).

connect_with_session(ClientId, Port) when is_integer(Port) ->
    connect_with_session(ClientId, [{port, Port}]);

connect_with_session(ClientId, Opts) ->
    {ok, C} = emqtt:start_link(
                [{clientid, ClientId},
                 {clean_start, false},
                 {proto_ver, v5},
                 {properties, #{'Session-Expiry-Interval' => 600}}
                ] ++ Opts),
    case emqtt:connect(C) of
        {ok, _} -> {ok, C};
        {error, _} = Error -> Error
    end.
