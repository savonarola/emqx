%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_ctx_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    emqx_gateway_test_utils:load_all_gateway_apps(),
    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun
            (#{clientid := bad_client}) ->
                {error, bad_username_or_password};
            (#{clientid := admin}) ->
                {ok, #{is_superuser => true}};
            (#{clientid := <<"original-clientid">>}) ->
                {ok, #{
                    clientid_override => <<"overridden-clientid">>,
                    trusted_attrs => #{clientid => true}
                }};
            (#{clientid := <<"client-with-auth-attrs">>}) ->
                {ok, #{
                    client_attrs => #{<<"tenant">> => <<"tenant-1">>},
                    trusted_attrs => #{clientid => true, username => true}
                }};
            (#{clientid := <<"expiring">>}) ->
                {ok, #{expire_at => erlang:system_time(millisecond) + 10_000}};
            (#{clientid := <<"zone-override">>}) ->
                {ok, #{zone_override => <<"default">>, custom_authn => value}};
            (_) ->
                {ok, #{trusted_attrs => #{clientid => true}}}
        end
    ),
    Conf.

end_per_suite(_Conf) ->
    meck:unload(emqx_access_control),
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_authenticate(_) ->
    Ctx = #{gwname => mqttsn, cm => self()},
    Info1 = #{
        mountpoint => undefined,
        clientid => <<"user1">>
    },
    {ok, NInfo1} = emqx_gateway_ctx:authenticate(Ctx, Info1),
    ?assertEqual(default, maps:get(zone, NInfo1)),
    ?assertEqual(false, maps:is_key(is_superuser, NInfo1)),
    ?assertEqual(false, maps:is_key(auth_expire_at, NInfo1)),
    ?assertEqual({ok, false}, emqx_clientinfo:get_trusted(NInfo1, is_superuser)),
    ?assertEqual({ok, undefined}, emqx_clientinfo:get_trusted(NInfo1, auth_expire_at)),

    Info2 = #{
        mountpoint => <<"mqttsn/${clientid}/">>,
        clientid => <<"user1">>
    },
    {ok, NInfo2} = emqx_gateway_ctx:authenticate(Ctx, Info2),
    ?assertEqual(<<"mqttsn/user1/">>, maps:get(mountpoint, NInfo2)),

    Info3 = #{
        mountpoint => <<"mqttsn/${clientid}/">>,
        clientid => bad_client
    },
    {error, bad_username_or_password} =
        emqx_gateway_ctx:authenticate(Ctx, Info3),

    Info4 = #{
        mountpoint => undefined,
        clientid => admin
    },
    {ok, NInfo4} = emqx_gateway_ctx:authenticate(Ctx, Info4),
    ?assertEqual(false, maps:is_key(is_superuser, NInfo4)),
    ?assertEqual({ok, true}, emqx_clientinfo:get_trusted(NInfo4, is_superuser)),

    Info5 = #{mountpoint => undefined, clientid => <<"zone-override">>},
    {ok, NInfo5} = emqx_gateway_ctx:authenticate(Ctx, Info5),
    ?assertEqual(default, maps:get(zone, NInfo5)),
    ?assertEqual(false, maps:is_key(zone_override, NInfo5)),
    ?assertEqual(false, maps:is_key(custom_authn, NInfo5)),
    ?assertEqual({ok, value}, emqx_clientinfo:get_trusted(NInfo5, custom_authn)),
    ok.

t_clientid_override_ignored(_) ->
    Ctx = #{gwname => mqttsn, cm => self()},
    Info = #{
        mountpoint => <<"mqttsn/${clientid}/">>,
        clientid => <<"original-clientid">>
    },
    Reports = emqx_cth_log_capture:capture(warning, fun() ->
        {ok, NInfo} = emqx_gateway_ctx:authenticate(Ctx, Info),
        ?assertEqual(<<"original-clientid">>, maps:get(clientid, NInfo)),
        ?assertEqual(<<"mqttsn/original-clientid/">>, maps:get(mountpoint, NInfo)),
        ?assertEqual(false, maps:is_key(clientid_override, NInfo)),
        ?assertEqual(
            {ok, <<"original-clientid">>}, emqx_clientinfo:get_trusted(NInfo, clientid)
        )
    end),
    ?assertMatch(
        [
            #{
                msg := "gateway_authn_clientid_override_not_supported",
                gateway := mqttsn,
                clientid := <<"original-clientid">>,
                clientid_override := <<"overridden-clientid">>
            }
        ],
        Reports
    ),
    ok.

t_mountpoint_after_authn(_) ->
    Ctx = #{gwname => mqttsn, cm => self()},
    Info = #{
        mountpoint => <<"mqttsn/${client_attrs.tenant}/${clientid}/">>,
        clientid => <<"client-with-auth-attrs">>,
        username => <<"user">>,
        client_attrs => #{<<"old">> => <<"value">>}
    },
    {ok, NInfo} = emqx_gateway_ctx:authenticate(Ctx, Info),
    ?assertEqual(
        <<"mqttsn/tenant-1/client-with-auth-attrs/">>,
        maps:get(mountpoint, NInfo)
    ),
    ?assertEqual(#{<<"tenant">> => <<"tenant-1">>}, maps:get(client_attrs, NInfo)),
    ?assertEqual({ok, <<"user">>}, emqx_clientinfo:get_trusted(NInfo, username)),
    ?assertEqual(
        {ok, <<"tenant-1">>},
        emqx_clientinfo:get_trusted(NInfo, [client_attrs, <<"tenant">>])
    ),
    ok.

%% Verify that a missing trusted gateway mountpoint variable rejects authentication.
t_mountpoint_missing_trusted_variable(_) ->
    Ctx = #{gwname => mqttsn, cm => self()},
    Info = #{
        mountpoint => <<"mqttsn/${username}/${clientid}/">>,
        clientid => <<"missing-username">>
    },
    ?assertMatch(
        {error, {unresolved_mountpoint_placeholders, [_]}},
        emqx_gateway_ctx:authenticate(Ctx, Info)
    ).

%% Verify that gateway expiry reads the relocated trusted authn value.
t_connection_expire_interval(_) ->
    Ctx = #{gwname => mqttsn, cm => self()},
    Info = #{mountpoint => undefined, clientid => <<"expiring">>},
    {ok, NInfo} = emqx_gateway_ctx:authenticate(Ctx, Info),
    Interval = emqx_gateway_ctx:connection_expire_interval(Ctx, NInfo),
    ?assert(Interval > 0),
    ?assert(Interval =< 10_000).
