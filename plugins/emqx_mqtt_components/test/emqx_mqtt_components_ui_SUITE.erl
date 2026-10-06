%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_components_ui_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_plugins,
            emqx_mqtt_components,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    ok = meck:new(emqx_plugins_fs, [passthrough, no_link, no_history]),
    {ok, Version} = application:get_key(emqx_mqtt_components, vsn),
    NameVsn = iolist_to_binary(["emqx_mqtt_components-", Version]),
    ok = meck:expect(emqx_plugins_fs, list_name_vsn, fun() -> [NameVsn] end),
    ok = meck:expect(emqx_plugins_fs, read_info, fun(NameVsn0) when NameVsn0 =:= NameVsn ->
        {ok, #{
            name => <<"emqx_mqtt_components">>,
            rel_vsn => list_to_binary(Version),
            rel_apps => [NameVsn],
            description => <<"MQTT components demo">>
        }}
    end),
    Config.

end_per_testcase(_Case, _Config) ->
    meck:unload(emqx_plugins_fs).

t_demo_page(_Config) ->
    URL = emqx_mgmt_api_test_util:uri([plugin_api, emqx_mqtt_components, ui]),
    {ok, {{_, 200, _}, Headers, Body}} = httpc:request(
        get,
        {URL, [emqx_mgmt_api_test_util:auth_header_()]},
        [{timeout, 5000}],
        [{body_format, binary}]
    ),
    ?assertEqual("text/html; charset=utf-8", proplists:get_value("content-type", Headers)),
    ?assertEqual("no-store", proplists:get_value("cache-control", Headers)),
    ?assertMatch(<<"<!DOCTYPE html>", _/binary>>, Body),
    ?assertNotEqual(nomatch, binary:match(Body, <<"MQTT Components Demo">>)),
    ?assertNotEqual(nomatch, binary:match(Body, <<"mqtt@5/dist/mqtt.min.js">>)).
