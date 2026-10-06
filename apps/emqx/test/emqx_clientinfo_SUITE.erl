%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_clientinfo_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => """
        listeners.tcp.default.enable = false
        listeners.ssl.default.enable = false
        listeners.ws.default.enable = false
        listeners.wss.default.enable = false
        """}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Roots = [authorization, mqtt, multi_tenancy, zones],
    SavedConfig = [{Root, emqx_config:get([Root], #{})} || Root <- Roots],
    emqx_config:put([authorization, require_trusted_attributes], true),
    [{saved_config, SavedConfig} | Config].

end_per_testcase(_TestCase, Config) ->
    [emqx_config:put([Root], Value) || {Root, Value} <- proplists:get_value(saved_config, Config)],
    ok.

%% Verify that disabled projection returns the original client info term.
t_maybe_trusted_disabled(_) ->
    ClientInfo = #{clientid => <<"client">>, trusted_attrs => #{clientinfo => #{}}},
    ?assert(ClientInfo =:= emqx_clientinfo:maybe_trusted(ClientInfo, false)),
    ?assertNot(ClientInfo =:= emqx_clientinfo:maybe_trusted(ClientInfo, true)).

%% Verify that authn composition keeps known outputs at the top level and trusts returned values.
t_merge_authn_result(_) ->
    ClientInfo0 = #{
        zone => default,
        protocol => mqtt,
        clientid => <<"original">>,
        username => <<"user">>,
        client_attrs => #{<<"existing">> => <<"old">>, <<"tenant">> => <<"input">>}
    },
    AuthResult = #{
        is_superuser => true,
        acl => [rule],
        expire_at => 123,
        trusted_attrs => #{username => true},
        client_attrs => #{<<"tenant">> => <<"authenticated">>},
        clientid_override => <<"overridden">>,
        zone_override => <<"default">>
    },
    ClientInfo = emqx_clientinfo:merge_authn_result(ClientInfo0, AuthResult, merge),
    ?assertMatch(#{is_superuser := true, acl := [rule], auth_expire_at := 123}, ClientInfo),
    ?assertEqual(false, maps:is_key(clientid_override, ClientInfo)),
    ?assertEqual(false, maps:is_key(zone_override, ClientInfo)),
    ?assertEqual({ok, true}, emqx_clientinfo:get_trusted(ClientInfo, is_superuser)),
    ?assertEqual({ok, [rule]}, emqx_clientinfo:get_trusted(ClientInfo, acl)),
    ?assertEqual({ok, 123}, emqx_clientinfo:get_trusted(ClientInfo, auth_expire_at)),
    ?assertEqual({ok, <<"overridden">>}, emqx_clientinfo:get_trusted(ClientInfo, clientid)),
    ?assertEqual({ok, <<"user">>}, emqx_clientinfo:get_trusted(ClientInfo, username)),
    ?assertEqual(
        {ok, <<"authenticated">>},
        emqx_clientinfo:get_trusted(ClientInfo, [client_attrs, <<"tenant">>])
    ),
    ?assertEqual([clientinfo], maps:keys(maps:get(trusted_attrs, ClientInfo))),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, [client_attrs, <<"existing">>])).

%% Verify that reauthentication replaces stale authn data and the previous input trust mask.
t_replace_authn_result(_) ->
    ClientInfo0 = #{
        zone => default,
        clientid => <<"client">>,
        username => <<"user">>,
        client_attrs => #{<<"old">> => <<"value">>}
    },
    ClientInfo1 = emqx_clientinfo:merge_authn_result(
        ClientInfo0,
        #{
            is_superuser => true,
            acl => [rule],
            expire_at => 123,
            custom_authn => old,
            trusted_attrs => #{username => true}
        },
        merge
    ),
    ClientInfo = emqx_clientinfo:merge_authn_result(
        ClientInfo1,
        #{client_attrs => #{<<"new">> => <<"value">>}},
        replace
    ),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, acl)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, custom_authn)),
    ?assertNot(maps:is_key(authn, maps:get(trusted_attrs, ClientInfo))),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, username)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, [client_attrs, <<"old">>])),
    ?assertEqual(
        {ok, <<"value">>},
        emqx_clientinfo:get_trusted(ClientInfo, [client_attrs, <<"new">>])
    ),
    ?assertEqual({ok, false}, emqx_clientinfo:get_trusted(ClientInfo, is_superuser)),
    ?assertEqual({ok, undefined}, emqx_clientinfo:get_trusted(ClientInfo, auth_expire_at)).

%% Verify that the trusted projection combines static fields, masked input, and authn output.
t_trusted_projection(_) ->
    ClientInfo = #{
        zone => default,
        protocol => mqtt,
        clientid => <<"client">>,
        username => <<"user">>,
        password => <<"secret">>,
        is_superuser => false,
        auth_expire_at => undefined,
        acl => [rule],
        client_attrs => #{<<"tns">> => <<"tenant">>, <<"untrusted">> => <<"value">>},
        trusted_attrs => #{
            authn => #{custom_authn => value},
            clientinfo => #{
                clientid => true,
                client_attrs => #{<<"tns">> => true}
            }
        }
    },
    ?assertEqual(
        #{
            zone => default,
            protocol => mqtt,
            is_superuser => false,
            auth_expire_at => undefined,
            acl => [rule],
            clientid => <<"client">>,
            client_attrs => #{<<"tns">> => <<"tenant">>},
            trusted_attrs => #{
                authn => #{custom_authn => value},
                clientinfo => #{
                    clientid => true,
                    client_attrs => #{<<"tns">> => true}
                }
            }
        },
        emqx_clientinfo:trusted(ClientInfo)
    ),
    ?assertEqual({ok, <<"client">>}, emqx_clientinfo:get_trusted(ClientInfo, clientid)),
    ?assertEqual(
        {ok, <<"tenant">>},
        emqx_clientinfo:get_trusted(ClientInfo, [client_attrs, <<"tns">>])
    ),
    ?assertEqual({ok, false}, emqx_clientinfo:get_trusted(ClientInfo, is_superuser)),
    ?assertEqual({ok, value}, emqx_clientinfo:get_trusted(ClientInfo, custom_authn)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, username)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, password)).

%% Verify that trusted and untrusted setters update trust without changing unrelated fields.
t_setters(_) ->
    ClientInfo0 = #{
        zone => default,
        clientid => <<"old">>,
        username => <<"user">>,
        client_attrs => #{<<"tns">> => <<"tenant">>, <<"region">> => <<"eu">>},
        trusted_attrs => #{
            clientinfo => #{
                clientid => true,
                client_attrs => #{<<"tns">> => true, <<"region">> => true}
            }
        }
    },
    ClientInfo1 = emqx_clientinfo:set(ClientInfo0, clientid, <<"new">>),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo1, clientid)),
    ?assertEqual(
        {ok, <<"tenant">>},
        emqx_clientinfo:get_trusted(ClientInfo1, [client_attrs, <<"tns">>])
    ),
    ClientInfo2 = emqx_clientinfo:set(
        ClientInfo1, [client_attrs, <<"tns">>], <<"untrusted-tenant">>
    ),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo2, [client_attrs, <<"tns">>])),
    ?assertEqual(
        {ok, <<"eu">>},
        emqx_clientinfo:get_trusted(ClientInfo2, [client_attrs, <<"region">>])
    ),
    ClientInfo3 = emqx_clientinfo:set_trusted(
        ClientInfo2, [client_attrs, <<"tns">>], <<"trusted-tenant">>
    ),
    ?assertEqual(
        {ok, <<"trusted-tenant">>},
        emqx_clientinfo:get_trusted(ClientInfo3, [client_attrs, <<"tns">>])
    ),
    ClientInfo4 = emqx_clientinfo:set(ClientInfo3, zone, other),
    ?assertEqual({ok, other}, emqx_clientinfo:get_trusted(ClientInfo4, zone)).

%% Verify that setters preserve unconditional trust without changing the trust metadata.
t_universal_mask_setters(_) ->
    ClientInfo0 = #{
        zone => default,
        clientid => <<"client">>,
        username => <<"user">>,
        client_attrs => #{<<"tns">> => <<"tenant">>},
        trusted_attrs => #{clientinfo => true}
    },
    ?assertEqual({ok, <<"user">>}, emqx_clientinfo:get_trusted(ClientInfo0, username)),
    ClientInfo1 = emqx_clientinfo:set(ClientInfo0, username, <<"new-user">>),
    ?assertEqual(ClientInfo0#{username := <<"new-user">>}, ClientInfo1),
    ?assertEqual({ok, <<"new-user">>}, emqx_clientinfo:get_trusted(ClientInfo1, username)),
    ClientInfo2 = emqx_clientinfo:set(ClientInfo1, zone, other),
    ?assertEqual(ClientInfo1#{zone := other}, ClientInfo2),
    ?assertEqual({ok, other}, emqx_clientinfo:get_trusted(ClientInfo2, zone)),
    ClientInfo3 = emqx_clientinfo:set_trusted(ClientInfo2, zone, trusted_zone),
    ?assertEqual(ClientInfo2#{zone := trusted_zone}, ClientInfo3),
    ?assertEqual({ok, trusted_zone}, emqx_clientinfo:get_trusted(ClientInfo3, zone)),
    ClientInfo4 = emqx_clientinfo:set(ClientInfo3, [client_attrs, <<"tns">>], <<"new-tenant">>),
    ?assertEqual(
        ClientInfo3#{client_attrs := #{<<"tns">> => <<"new-tenant">>}}, ClientInfo4
    ),
    ?assertEqual(
        {ok, <<"new-tenant">>}, emqx_clientinfo:get_trusted(ClientInfo4, [client_attrs, <<"tns">>])
    ),
    ClientInfo5 = emqx_clientinfo:set(ClientInfo4, [client_attrs, <<"new">>], <<"value">>),
    ?assertEqual(
        {ok, <<"value">>}, emqx_clientinfo:get_trusted(ClientInfo5, [client_attrs, <<"new">>])
    ),
    ?assertEqual(maps:get(trusted_attrs, ClientInfo0), maps:get(trusted_attrs, ClientInfo5)).

%% Verify that reauthentication replaces unconditional trust with an explicit input mask.
t_replace_universal_mask(_) ->
    ClientInfo0 = #{clientid => <<"client">>, username => <<"user">>},
    ClientInfo1 = emqx_clientinfo:merge_authn_result(
        ClientInfo0, #{trusted_attrs => true}, merge
    ),
    ?assertEqual(
        #{clientinfo => true},
        maps:get(trusted_attrs, ClientInfo1)
    ),
    ClientInfo2 = emqx_clientinfo:merge_authn_result(
        ClientInfo1, #{trusted_attrs => #{clientid => true}}, merge
    ),
    ?assertEqual({ok, <<"client">>}, emqx_clientinfo:get_trusted(ClientInfo2, clientid)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo2, username)).

%% Verify that all disabled consumers skip trust composition and keep legacy authentication output.
t_disabled_authn_metadata(_) ->
    configure_enforcement(false, false, false),
    emqx_config:put_zone_conf(default, [mqtt, trusted_client_attributes], [invalid]),
    ClientInfo0 = #{
        zone => default,
        clientid => <<"original">>,
        username => <<"user">>,
        client_attrs => #{<<"existing">> => <<"value">>},
        is_superuser => true,
        acl => [old_rule],
        auth_expire_at => 123,
        trusted_attrs => #{clientinfo => true, authn => #{old_custom => value}}
    },
    ClientInfo = emqx_clientinfo:merge_authn_result(
        ClientInfo0,
        #{
            trusted_attrs => invalid,
            clientid_override => <<"overridden">>,
            client_attrs => #{<<"returned">> => <<"trusted-value">>},
            custom_authn => value
        },
        merge
    ),
    ?assertEqual(
        #{
            zone => default,
            clientid => <<"overridden">>,
            username => <<"user">>,
            client_attrs => #{
                <<"existing">> => <<"value">>, <<"returned">> => <<"trusted-value">>
            },
            is_superuser => false,
            auth_expire_at => undefined,
            custom_authn => value
        },
        ClientInfo
    ),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, custom_authn)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, clientid)),
    ?assertEqual({ok, false}, emqx_clientinfo:get_trusted(ClientInfo, is_superuser)),
    ?assertEqual(ClientInfo, emqx_authz_context:make(ClientInfo)),
    ClientInfo1 = emqx_clientinfo:set(ClientInfo, username, <<"new-user">>),
    ClientInfo2 = emqx_clientinfo:set_trusted(ClientInfo1, [client_attrs, <<"tns">>], <<"tenant">>),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo1)),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo2)),
    ?assertEqual(<<"tenant">>, emqx_utils_maps:deep_get([client_attrs, <<"tns">>], ClientInfo2)),
    ClientInfo3 = emqx_clientinfo:merge_authn_result(
        ClientInfo2, #{is_superuser => true, acl => [new_rule], expire_at => 456}, merge
    ),
    ?assertMatch(#{is_superuser := true, acl := [new_rule], auth_expire_at := 456}, ClientInfo3),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo3)).

%% Verify that each enforcement switch independently retains the input trust mask.
t_independent_enforcement(_) ->
    lists:foreach(
        fun({Authz, MultiTenancy, MQTT}) ->
            configure_enforcement(Authz, MultiTenancy, MQTT),
            ClientInfo = emqx_clientinfo:merge_authn_result(
                #{zone => default, clientid => <<"client">>},
                #{trusted_attrs => #{clientid => true}},
                merge
            ),
            ?assertEqual(#{clientinfo => #{clientid => true}}, maps:get(trusted_attrs, ClientInfo)),
            ?assertEqual({ok, <<"client">>}, emqx_clientinfo:get_trusted(ClientInfo, clientid))
        end,
        [{true, false, false}, {false, true, false}, {false, false, true}]
    ).

%% Verify that zone overrides select metadata retention before configured paths are evaluated.
t_zone_override_metadata(_) ->
    configure_enforcement(false, false, false),
    emqx_config:put([zones, trusted_zone], #{
        mqtt => #{
            require_trusted_attributes => true,
            trusted_client_attributes => [<<"username">>]
        }
    }),
    ClientInfo0 = #{zone => default, clientid => <<"client">>, username => <<"user">>},
    ClientInfo1 = emqx_clientinfo:merge_authn_result(
        ClientInfo0, #{zone_override => <<"trusted_zone">>}, merge
    ),
    ?assertEqual({ok, <<"user">>}, emqx_clientinfo:get_trusted(ClientInfo1, username)),
    ClientInfo2 = emqx_clientinfo:merge_authn_result(
        ClientInfo1, #{zone_override => <<"default">>}, merge
    ),
    ?assertEqual(default, maps:get(zone, ClientInfo2)),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo2)).

%% Verify that enabling enforcement does not recover input trust discarded during authentication.
t_enable_enforcement_without_metadata(_) ->
    configure_enforcement(false, false, false),
    ClientInfo0 = emqx_clientinfo:merge_authn_result(
        #{zone => default, clientid => <<"client">>, username => <<"user">>},
        #{is_superuser => true, acl => [rule], trusted_attrs => true},
        merge
    ),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo0)),
    configure_enforcement(true, false, false),
    Context = emqx_authz_context:make(ClientInfo0),
    ?assertEqual(
        #{zone => default, is_superuser => true, acl => [rule], auth_expire_at => undefined},
        Context
    ),
    ?assertEqual(true, emqx_authz_context:get_authn(Context, is_superuser, false)),
    ClientInfo1 = emqx_clientinfo:set(ClientInfo0, username, <<"new-user">>),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo1)),
    ClientInfo2 = emqx_clientinfo:set_trusted(ClientInfo1, [client_attrs, <<"tns">>], <<"tenant">>),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo2, username)),
    ?assertEqual(
        {ok, <<"tenant">>}, emqx_clientinfo:get_trusted(ClientInfo2, [client_attrs, <<"tns">>])
    ),
    ClientInfo3 = emqx_clientinfo:merge_authn_result(
        ClientInfo2, #{trusted_attrs => #{username => true}}, merge
    ),
    ?assertEqual({ok, <<"new-user">>}, emqx_clientinfo:get_trusted(ClientInfo3, username)),
    ?assertEqual({ok, false}, emqx_clientinfo:get_trusted(ClientInfo3, is_superuser)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo3, acl)).

%% Verify that custom authentication outputs remain optional and setters remove stale custom values.
t_custom_authn_outputs(_) ->
    ClientInfo0 = emqx_clientinfo:merge_authn_result(
        #{zone => default, clientid => <<"client">>}, #{custom_authn => old}, merge
    ),
    ?assertEqual(
        #{clientinfo => #{}, authn => #{custom_authn => old}}, maps:get(trusted_attrs, ClientInfo0)
    ),
    ?assertNot(maps:is_key(custom_authn, ClientInfo0)),
    ClientInfo1 = emqx_clientinfo:set(ClientInfo0, custom_authn, new),
    ?assertEqual(#{clientinfo => #{}}, maps:get(trusted_attrs, ClientInfo1)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo1, custom_authn)),
    ClientInfo2 = emqx_clientinfo:set_trusted(ClientInfo0, custom_authn, new),
    ?assertEqual(#{clientinfo => #{custom_authn => true}}, maps:get(trusted_attrs, ClientInfo2)),
    ?assertEqual({ok, new}, emqx_clientinfo:get_trusted(ClientInfo2, custom_authn)),
    ClientInfo3 = emqx_clientinfo:merge_authn_result(ClientInfo0, #{other_custom => value}, merge),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo3, custom_authn)),
    ?assertEqual({ok, value}, emqx_clientinfo:get_trusted(ClientInfo3, other_custom)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

configure_enforcement(Authz, MultiTenancy, MQTT) ->
    emqx_config:put([authorization, require_trusted_attributes], Authz),
    emqx_config:put([multi_tenancy, require_trusted_attributes], MultiTenancy),
    emqx_config:put_zone_conf(default, [mqtt, require_trusted_attributes], MQTT).
