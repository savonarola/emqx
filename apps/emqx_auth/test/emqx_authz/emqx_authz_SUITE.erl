%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create_local, fun(_, _, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, remove_local, fun(_) -> ok end),
    meck:expect(
        emqx_authz_file,
        acl_conf_file,
        fun() ->
            emqx_common_test_helpers:deps_path(emqx_auth_file, "etc/acl.conf")
        end
    ),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf,
                "authorization { cache { enable = false }, no_match = deny, sources = [] }"},
            emqx_auth,
            emqx_auth_file,
            emqx_auth_http,
            emqx_auth_mnesia,
            emqx_auth_redis,
            emqx_auth_postgresql,
            emqx_auth_mysql,
            emqx_auth_mongodb
        ],
        #{
            work_dir => filename:join(?config(priv_dir, Config), ?MODULE)
        }
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    meck:unload(emqx_resource),
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_subscribe_deny_disconnect_publishes_last_will_testament;
    TestCase =:= t_publish_last_will_testament_banned_client_connecting;
    TestCase =:= t_publish_deny_disconnect_publishes_last_will_testament
->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, []),
    {ok, _} = emqx:update_config([authorization, deny_action], disconnect),
    Config;
init_per_testcase(_TestCase, Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, []),
    Config.

end_per_testcase(TestCase, _Config) when
    TestCase =:= t_subscribe_deny_disconnect_publishes_last_will_testament;
    TestCase =:= t_publish_last_will_testament_banned_client_connecting;
    TestCase =:= t_publish_deny_disconnect_publishes_last_will_testament
->
    {ok, _} = emqx:update_config([authorization, deny_action], ignore),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, []),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

-define(SOURCE1, #{
    <<"type">> => <<"http">>,
    <<"enable">> => true,
    <<"url">> => <<"https://example.com:443/a/b?c=d">>,
    <<"headers">> => #{},
    <<"ssl">> => #{<<"enable">> => true},
    <<"method">> => <<"get">>,
    <<"request_timeout">> => <<"5s">>
}).
-define(SOURCE2, #{
    <<"type">> => <<"mongodb">>,
    <<"enable">> => true,
    <<"mongo_type">> => <<"single">>,
    <<"server">> => <<"127.0.0.1:27017">>,
    <<"w_mode">> => <<"unsafe">>,
    <<"pool_size">> => 1,
    <<"database">> => <<"mqtt">>,
    <<"ssl">> => #{<<"enable">> => false},
    <<"collection">> => <<"authz">>,
    <<"filter">> => #{<<"a">> => <<"b">>}
}).
-define(SOURCE3, #{
    <<"type">> => <<"mysql">>,
    <<"enable">> => true,
    <<"server">> => <<"127.0.0.1:27017">>,
    <<"pool_size">> => 1,
    <<"database">> => <<"mqtt">>,
    <<"username">> => <<"xx">>,
    <<"password">> => <<"ee">>,
    <<"auto_reconnect">> => true,
    <<"ssl">> => #{<<"enable">> => false},
    <<"query">> => <<"abcb">>
}).
-define(SOURCE4, #{
    <<"type">> => <<"postgresql">>,
    <<"enable">> => true,
    <<"server">> => <<"127.0.0.1:27017">>,
    <<"pool_size">> => 1,
    <<"database">> => <<"mqtt">>,
    <<"username">> => <<"xx">>,
    <<"password">> => <<"ee">>,
    <<"auto_reconnect">> => true,
    <<"ssl">> => #{<<"enable">> => false},
    <<"query">> => <<"abcb">>
}).
-define(SOURCE5, #{
    <<"type">> => <<"redis">>,
    <<"redis_type">> => <<"single">>,
    <<"enable">> => true,
    <<"server">> => <<"127.0.0.1:27017">>,
    <<"pool_size">> => 1,
    <<"database">> => 0,
    <<"password">> => <<"ee">>,
    <<"auto_reconnect">> => true,
    <<"ssl">> => #{<<"enable">> => false},
    <<"cmd">> => <<"HGETALL mqtt_authz:", ?PH_USERNAME/binary>>
}).

-define(FILE_SOURCE(Rules), #{
    <<"type">> => <<"file">>,
    <<"enable">> => true,
    <<"rules">> => Rules
}).

-define(SOURCE6,
    ?FILE_SOURCE(
        <<
            "{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}."
            "\n{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}."
        >>
    )
).
-define(SOURCE7,
    ?FILE_SOURCE(
        <<
            "{allow,{username,\"some_client\"},publish,[\"some_client/lwt\"]}.\n"
            "{deny, all}."
        >>
    )
).

-define(BAD_FILE_SOURCE2, #{
    <<"type">> => <<"file">>,
    <<"enable">> => true,
    <<"rules">> =>
        <<
            "{not_allow,{username,\"some_client\"},publish,[\"some_client/lwt\"]}."
        >>
}).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

-define(UPDATE_ERROR(Err), {error, {pre_config_update, emqx_authz, Err}}).

t_bad_file_source(_) ->
    BadContent = ?FILE_SOURCE(<<"{allow,{username,\"bar\"}, publish, [\"test\"]}">>),
    BadContentErr = {bad_acl_file_content, {1, erl_parse, ["syntax error before: ", []]}},
    BadRule = ?FILE_SOURCE(<<"{allow,{username,\"bar\"},publish}.">>),
    BadRuleErr = {invalid_authorization_rule, {allow, {username, "bar"}, publish}},
    BadPermission = ?FILE_SOURCE(<<"{not_allow,{username,\"bar\"},publish,[\"test\"]}.">>),
    BadPermissionErr = {invalid_authorization_permission, not_allow},
    BadAction = ?FILE_SOURCE(<<"{allow,{username,\"bar\"},pubsub,[\"test\"]}.">>),
    BadActionErr = {invalid_authorization_action, pubsub},
    lists:foreach(
        fun({Source, Error}) ->
            File = emqx_authz_file:acl_conf_file(),
            {ok, Bin1} = file:read_file(File),
            ?assertEqual(?UPDATE_ERROR(Error), emqx_authz:update(?CMD_REPLACE, [Source])),
            ?assertEqual(?UPDATE_ERROR(Error), emqx_authz:update(?CMD_PREPEND, Source)),
            ?assertEqual(?UPDATE_ERROR(Error), emqx_authz:update(?CMD_APPEND, Source)),
            %% Check file content not changed if update failed
            {ok, Bin2} = file:read_file(File),
            ?assertEqual(Bin1, Bin2)
        end,
        [
            {BadContent, BadContentErr},
            {BadRule, BadRuleErr},
            {BadPermission, BadPermissionErr},
            {BadAction, BadActionErr}
        ]
    ),
    ?assertMatch(
        [],
        emqx_conf:get([authorization, sources], [])
    ).

t_update_source(_) ->
    %% replace all
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE3]),
    {ok, _} = emqx_authz:update(?CMD_PREPEND, ?SOURCE2),
    {ok, _} = emqx_authz:update(?CMD_PREPEND, ?SOURCE1),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE4),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE5),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE6),

    ?assertMatch(
        [
            #{type := http, enable := true},
            #{type := mongodb, enable := true},
            #{type := mysql, enable := true},
            #{type := postgresql, enable := true},
            #{type := redis, enable := true},
            #{type := file, enable := true}
        ],
        emqx_conf:get([authorization, sources], [])
    ),

    {ok, _} = emqx_authz:update({?CMD_REPLACE, http}, ?SOURCE1#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mongodb}, ?SOURCE2#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mysql}, ?SOURCE3#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, postgresql}, ?SOURCE4#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, redis}, ?SOURCE5#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, file}, ?SOURCE6#{<<"enable">> := true}),

    {ok, _} = emqx_authz:update({?CMD_REPLACE, http}, ?SOURCE1#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mongodb}, ?SOURCE2#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mysql}, ?SOURCE3#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, postgresql}, ?SOURCE4#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, redis}, ?SOURCE5#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, file}, ?SOURCE6#{<<"enable">> := false}),

    ?assertMatch(
        [
            #{type := http, enable := false},
            #{type := mongodb, enable := false},
            #{type := mysql, enable := false},
            #{type := postgresql, enable := false},
            #{type := redis, enable := false},
            #{type := file, enable := false}
        ],
        emqx_conf:get([authorization, sources], [])
    ),
    ?assertMatch(
        [
            #{type := http, enable := false},
            #{type := mongodb, enable := false},
            #{type := mysql, enable := false},
            #{type := postgresql, enable := false},
            #{type := redis, enable := false},
            #{type := file, enable := false}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:update(?CMD_REPLACE, []).

t_replace_all(_) ->
    RootKey = [<<"authorization">>],
    Conf = emqx:get_raw_config(RootKey),
    emqx_authz_utils:update_config(RootKey, Conf#{
        <<"sources">> => [
            ?SOURCE6, ?SOURCE5, ?SOURCE4, ?SOURCE3, ?SOURCE2, ?SOURCE1
        ]
    }),
    %% config
    ?assertMatch(
        [
            #{type := file, enable := true},
            #{type := redis, enable := true},
            #{type := postgresql, enable := true},
            #{type := mysql, enable := true},
            #{type := mongodb, enable := true},
            #{type := http, enable := true}
        ],
        emqx_conf:get([authorization, sources], [])
    ),
    %% hooks status
    ?assertMatch(
        [
            #{type := file, enable := true},
            #{type := redis, enable := true},
            #{type := postgresql, enable := true},
            #{type := mysql, enable := true},
            #{type := mongodb, enable := true},
            #{type := http, enable := true}
        ],
        emqx_authz:lookup()
    ),
    Ids = [http, mongodb, mysql, postgresql, redis, file],
    %% metrics
    lists:foreach(
        fun(Id) ->
            ?assert(emqx_metrics_worker:has_metrics(authz_metrics, Id), Id)
        end,
        Ids
    ),

    ?assertMatch(
        {ok, _},
        emqx_authz_utils:update_config(
            RootKey,
            Conf#{<<"sources">> => [?SOURCE1#{<<"enable">> => false}]}
        )
    ),
    %% hooks status
    ?assertMatch([#{type := http, enable := false}], emqx_authz:lookup()),
    %% metrics
    ?assert(emqx_metrics_worker:has_metrics(authz_metrics, http)),
    lists:foreach(
        fun(Id) ->
            ?assertNot(emqx_metrics_worker:has_metrics(authz_metrics, Id), Id)
        end,
        Ids -- [http]
    ),
    ok.

t_delete_source(_) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE1]),

    ?assertMatch([#{type := http, enable := true}], emqx_conf:get([authorization, sources], [])),

    {ok, _} = emqx_authz:update({?CMD_DELETE, http}, #{}),

    ?assertMatch([], emqx_conf:get([authorization, sources], [])).

t_move_source(_) ->
    {ok, _} = emqx_authz:update(
        ?CMD_REPLACE,
        [
            ?SOURCE1,
            ?SOURCE2,
            ?SOURCE3,
            ?SOURCE4,
            ?SOURCE5,
            ?SOURCE6
        ]
    ),
    ?assertMatch(
        [
            #{type := http},
            #{type := mongodb},
            #{type := mysql},
            #{type := postgresql},
            #{type := redis},
            #{type := file}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:move(postgresql, ?CMD_MOVE_FRONT),
    ?assertMatch(
        [
            #{type := postgresql},
            #{type := http},
            #{type := mongodb},
            #{type := mysql},
            #{type := redis},
            #{type := file}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:move(http, ?CMD_MOVE_REAR),
    ?assertMatch(
        [
            #{type := postgresql},
            #{type := mongodb},
            #{type := mysql},
            #{type := redis},
            #{type := file},
            #{type := http}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:move(mysql, ?CMD_MOVE_BEFORE(postgresql)),
    ?assertMatch(
        [
            #{type := mysql},
            #{type := postgresql},
            #{type := mongodb},
            #{type := redis},
            #{type := file},
            #{type := http}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:move(mongodb, ?CMD_MOVE_AFTER(http)),
    ?assertMatch(
        [
            #{type := mysql},
            #{type := postgresql},
            #{type := redis},
            #{type := file},
            #{type := http},
            #{type := mongodb}
        ],
        emqx_authz:lookup()
    ),

    ok.

t_get_enabled_authzs_none_enabled(_Config) ->
    ?assertEqual([], emqx_authz:get_enabled_authzs()).

t_get_enabled_authzs_some_enabled(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE4, ?SOURCE5#{<<"enable">> := false}]),
    ?assertEqual([postgresql], emqx_authz:get_enabled_authzs()).

t_subscribe_deny_disconnect_publishes_last_will_testament(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE7]),
    {ok, C} = emqtt:start_link([
        {username, <<"some_client">>},
        {will_topic, <<"some_client/lwt">>},
        {will_payload, <<"should be published">>}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = emqx:subscribe(<<"some_client/lwt">>),
    process_flag(trap_exit, true),

    try
        emqtt:subscribe(C, <<"unauthorized">>),
        error(should_have_disconnected)
    catch
        exit:{{shutdown, tcp_closed}, _} ->
            ok
    end,

    receive
        {deliver, <<"some_client/lwt">>, #message{payload = <<"should be published">>}} ->
            ok
    after 2_000 ->
        error(lwt_not_published)
    end,

    ok.

t_publish_deny_disconnect_publishes_last_will_testament(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE7]),
    {ok, C} = emqtt:start_link([
        {username, <<"some_client">>},
        {will_topic, <<"some_client/lwt">>},
        {will_payload, <<"should be published">>}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = emqx:subscribe(<<"some_client/lwt">>),
    process_flag(trap_exit, true),

    %% disconnect is async
    Ref = monitor(process, C),
    emqtt:publish(C, <<"some/topic">>, <<"unauthorized">>),
    receive
        {'DOWN', Ref, process, C, _} ->
            ok
    after 1_000 ->
        error(client_should_have_been_disconnected)
    end,
    receive
        {deliver, <<"some_client/lwt">>, #message{payload = <<"should be published">>}} ->
            ok
    after 2_000 ->
        error(lwt_not_published)
    end,

    ok.

t_publish_last_will_testament_denied_topic(_Config) ->
    {ok, C} = emqtt:start_link([
        {will_topic, <<"$SYS/lwt">>},
        {will_payload, <<"should not be published">>}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = emqx:subscribe(<<"$SYS/lwt">>),
    unlink(C),
    ok = snabbkaffe:start_trace(),
    {true, {ok, _}} = ?wait_async_action(
        exit(C, kill),
        #{?snk_kind := last_will_testament_publish_denied},
        1_000
    ),
    ok = snabbkaffe:stop(),

    receive
        {deliver, <<"$SYS/lwt">>, #message{payload = <<"should not be published">>}} ->
            error(lwt_should_not_be_published_to_forbidden_topic)
    after 1_000 ->
        ok
    end,

    ok.

%% client is allowed by ACL to publish to its LWT topic, is connected,
%% and then gets banned and kicked out while connected.  Should not
%% publish LWT.
t_publish_last_will_testament_banned_client_connecting(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE7]),
    Username = <<"some_client">>,
    ClientId = <<"some_clientid">>,
    LWTPayload = <<"should not be published">>,
    LWTTopic = <<"some_client/lwt">>,
    ok = emqx:subscribe(<<"some_client/lwt">>),
    {ok, C} = emqtt:start_link([
        {clientid, ClientId},
        {username, Username},
        {will_topic, LWTTopic},
        {will_payload, LWTPayload}
    ]),
    ?assertMatch({ok, _}, emqtt:connect(C)),

    %% Now we ban the client while it is connected.
    Now = erlang:system_time(second),
    Who = {username, Username},
    emqx_banned:create(#{
        who => Who,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),
    on_exit(fun() -> emqx_banned:delete(Who) end),
    %% Now kick it as we do in the ban API.
    process_flag(trap_exit, true),
    ?check_trace(
        begin
            ok = emqx_cm:kick_session(ClientId),
            receive
                {deliver, LWTTopic, #message{payload = LWTPayload}} ->
                    error(lwt_should_not_be_published_to_forbidden_topic)
            after 2_000 -> ok
            end,
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        client_banned := true,
                        publishing_disallowed := false
                    }
                ],
                ?of_kind(last_will_testament_publish_denied, Trace)
            ),
            ok
        end
    ),
    ok = snabbkaffe:stop(),

    ok.

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
