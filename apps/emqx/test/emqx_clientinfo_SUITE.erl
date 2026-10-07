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
    emqx_common_test_helpers:call_janitor().

%% Verify that disabled projection returns the original client info term.
t_maybe_trusted_disabled(_) ->
    ClientInfo = #{clientid => <<"client">>, trusted_attrs => #{}},
    ?assert(ClientInfo =:= emqx_clientinfo:maybe_trusted(ClientInfo, false)),
    ?assertNot(ClientInfo =:= emqx_clientinfo:maybe_trusted(ClientInfo, true)).

%% Verify that the MQTT shortcut projects client info only when trust enforcement is enabled.
t_maybe_trusted_for_mqtt(_) ->
    ClientInfo = #{
        zone => default,
        clientid => <<"client">>,
        username => <<"untrusted-user">>,
        trusted_attrs => #{clientid => true}
    },
    %% Keep all client info when enforcement is disabled.
    emqx_config:put([mqtt, require_trusted_attributes], false),
    ?assertEqual(ClientInfo, emqx_clientinfo:maybe_trusted_for_mqtt(ClientInfo)),
    %% Remove untrusted inputs when enforcement is enabled.
    emqx_config:put([mqtt, require_trusted_attributes], true),
    ?assertEqual(
        maps:remove(username, ClientInfo), emqx_clientinfo:maybe_trusted_for_mqtt(ClientInfo)
    ).

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
    ?assertEqual(
        #{username => true, clientid => true, client_attrs => #{<<"tenant">> => true}},
        maps:get(trusted_attrs, ClientInfo)
    ),
    ?assertNot(maps:is_key(authn, ClientInfo)),
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
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, [authn, custom_authn])),
    ?assertNot(maps:is_key(authn, ClientInfo)),
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
        authn => #{custom_authn => value},
        trusted_attrs => #{
            clientid => true,
            client_attrs => #{<<"tns">> => true}
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
            authn => #{custom_authn => value},
            trusted_attrs => #{
                clientid => true,
                client_attrs => #{<<"tns">> => true}
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
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, custom_authn)),
    ?assertEqual({ok, value}, emqx_clientinfo:get_trusted(ClientInfo, [authn, custom_authn])),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, username)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, password)),
    %% Check raw values and their trust status without filtering the values.
    ?assertEqual({ok, default, true}, emqx_clientinfo:get(ClientInfo, [zone])),
    ?assertEqual({ok, false, true}, emqx_clientinfo:get(ClientInfo, is_superuser)),
    ?assertEqual({ok, <<"user">>, false}, emqx_clientinfo:get(ClientInfo, username)),
    ?assertEqual(
        {ok, <<"tenant">>, true}, emqx_clientinfo:get(ClientInfo, [client_attrs, <<"tns">>])
    ),
    ?assertEqual(
        {ok, <<"value">>, false}, emqx_clientinfo:get(ClientInfo, [client_attrs, <<"untrusted">>])
    ),
    ?assertEqual({ok, value, true}, emqx_clientinfo:get(ClientInfo, [authn, custom_authn])),
    ?assertEqual(error, emqx_clientinfo:get(ClientInfo, custom_authn)),
    ?assertEqual({<<"user">>, false}, emqx_clientinfo:get(ClientInfo, username, fallback)),
    ?assertEqual({undefined, true}, emqx_clientinfo:get(ClientInfo, auth_expire_at, fallback)),
    ?assertEqual({fallback, false}, emqx_clientinfo:get(ClientInfo, missing, fallback)),
    ?assertEqual(
        {fallback, false}, emqx_clientinfo:get(maps:remove(zone, ClientInfo), zone, fallback)
    ),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo, zone)),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo, [zone])),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo, clientid)),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo, [client_attrs, <<"tns">>])),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo, [authn, custom_authn])),
    ?assertNot(emqx_clientinfo:is_trusted(ClientInfo, client_attrs)),
    ?assertNot(emqx_clientinfo:is_trusted(ClientInfo, [client_attrs, <<"untrusted">>])),
    ?assertNot(emqx_clientinfo:is_trusted(ClientInfo, username)),
    ?assertNot(emqx_clientinfo:is_trusted(ClientInfo, missing)),
    ParentTrusted = ClientInfo#{trusted_attrs := #{client_attrs => true}},
    ?assert(emqx_clientinfo:is_trusted(ParentTrusted, client_attrs)),
    ?assert(emqx_clientinfo:is_trusted(ParentTrusted, [client_attrs, <<"tns">>])).

%% Verify that trusted and untrusted setters update trust without changing unrelated fields.
t_setters(_) ->
    ClientInfo0 = #{
        zone => default,
        clientid => <<"old">>,
        username => <<"user">>,
        client_attrs => #{<<"tns">> => <<"tenant">>, <<"region">> => <<"eu">>},
        trusted_attrs => #{
            clientid => true,
            client_attrs => #{<<"tns">> => true, <<"region">> => true}
        }
    },
    ClientInfo1 = emqx_clientinfo:set(ClientInfo0, clientid, <<"new">>),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo1, clientid)),
    ?assertNot(emqx_clientinfo:is_trusted(ClientInfo1, clientid)),
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
    ?assert(emqx_clientinfo:is_trusted(ClientInfo3, [client_attrs, <<"tns">>])),
    ?assertError(
        {statically_trusted_attribute, [zone]},
        emqx_clientinfo:set(ClientInfo3, zone, other)
    ),
    ClientInfo4 = emqx_clientinfo:set_trusted(ClientInfo3, zone, other),
    ?assertEqual(ClientInfo3#{zone := other}, ClientInfo4),
    ?assertEqual({ok, other}, emqx_clientinfo:get_trusted(ClientInfo4, zone)).

%% Verify that setters preserve unconditional trust without changing the trust metadata.
t_universal_mask_setters(_) ->
    ClientInfo0 = #{
        zone => default,
        clientid => <<"client">>,
        username => <<"user">>,
        client_attrs => #{<<"tns">> => <<"tenant">>},
        trusted_attrs => true
    },
    ?assertEqual({ok, <<"user">>}, emqx_clientinfo:get_trusted(ClientInfo0, username)),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo0, username)),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo0, [client_attrs, <<"tns">>])),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo0, [client_attrs, <<"missing">>])),
    ?assertEqual(
        {<<"tenant">>, true}, emqx_clientinfo:get(ClientInfo0, [client_attrs, <<"tns">>], undefined)
    ),
    ?assertEqual(error, emqx_clientinfo:get(ClientInfo0, [client_attrs, <<"missing">>])),
    ?assertEqual(
        {undefined, false},
        emqx_clientinfo:get(ClientInfo0, [client_attrs, <<"missing">>], undefined)
    ),
    ClientInfo1 = emqx_clientinfo:set(ClientInfo0, username, <<"new-user">>),
    ?assertEqual(ClientInfo0#{username := <<"new-user">>}, ClientInfo1),
    ?assertEqual({ok, <<"new-user">>}, emqx_clientinfo:get_trusted(ClientInfo1, username)),
    ?assertError(
        {statically_trusted_attribute, [zone]},
        emqx_clientinfo:set(ClientInfo1, [zone], other)
    ),
    ClientInfo2 = emqx_clientinfo:set_trusted(ClientInfo1, [zone], other),
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
    ?assertEqual(true, maps:get(trusted_attrs, ClientInfo1)),
    ClientInfo2 = emqx_clientinfo:merge_authn_result(
        ClientInfo1, #{trusted_attrs => #{clientid => true}}, merge
    ),
    ?assertEqual({ok, <<"client">>}, emqx_clientinfo:get_trusted(ClientInfo2, clientid)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo2, username)).

%% Verify that disabled consumers skip trust composition and keep custom output in its own namespace.
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
        trusted_attrs => true,
        authn => #{old_custom => value}
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
            authn => #{custom_authn => value}
        },
        ClientInfo
    ),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, custom_authn)),
    ?assertEqual({ok, value}, emqx_clientinfo:get_trusted(ClientInfo, [authn, custom_authn])),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, clientid)),
    ?assertNot(emqx_clientinfo:is_trusted(ClientInfo, clientid)),
    ?assertNot(emqx_clientinfo:is_trusted(ClientInfo, [client_attrs, <<"tns">>])),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo, is_superuser)),
    ?assert(emqx_clientinfo:is_trusted(ClientInfo, [authn, custom_authn])),
    ?assertEqual({ok, false}, emqx_clientinfo:get_trusted(ClientInfo, is_superuser)),
    ?assertEqual(ClientInfo, emqx_authz_context:make(ClientInfo)),
    ?assertEqual(
        ClientInfo#{is_superuser := true},
        emqx_clientinfo:set_trusted(ClientInfo, is_superuser, true)
    ),
    ?assertError(
        {statically_trusted_attribute, [is_superuser]},
        emqx_clientinfo:set(ClientInfo, is_superuser, true)
    ),
    ClientInfo1 = emqx_clientinfo:set(ClientInfo, username, <<"new-user">>),
    ClientInfo2 = emqx_clientinfo:set_trusted(ClientInfo1, [client_attrs, <<"tns">>], <<"tenant">>),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo1)),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo2)),
    ?assertEqual(<<"tenant">>, emqx_utils_maps:deep_get([client_attrs, <<"tns">>], ClientInfo2)),
    ClientInfo3 = emqx_clientinfo:merge_authn_result(
        ClientInfo2, #{is_superuser => true, acl => [new_rule], expire_at => 456}, merge
    ),
    ?assertMatch(#{is_superuser := true, acl := [new_rule], auth_expire_at := 456}, ClientInfo3),
    ?assertNot(maps:is_key(authn, ClientInfo3)),
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
            ?assertEqual(#{clientid => true}, maps:get(trusted_attrs, ClientInfo)),
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
    ?assertEqual(ClientInfo0, emqx_clientinfo:set_trusted(ClientInfo0, [zone], default)),
    ?assertError(
        {statically_trusted_attribute, [zone]},
        emqx_clientinfo:set(ClientInfo0, zone, default)
    ),
    Context = emqx_authz_context:make(ClientInfo0),
    ?assertEqual(
        #{zone => default, is_superuser => true, acl => [rule], auth_expire_at => undefined},
        Context
    ),
    ?assertEqual(true, maps:get(is_superuser, Context)),
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

%% Verify that custom output remains separate from client-info setters and resets on reauthentication.
t_custom_authn_outputs(_) ->
    ClientInfo0 = emqx_clientinfo:merge_authn_result(
        #{zone => default, clientid => <<"client">>}, #{custom_authn => old}, merge
    ),
    ?assertEqual(#{}, maps:get(trusted_attrs, ClientInfo0)),
    ?assertEqual(#{custom_authn => old}, maps:get(authn, ClientInfo0)),
    ?assertNot(maps:is_key(custom_authn, ClientInfo0)),
    ClientInfo1 = emqx_clientinfo:set(ClientInfo0, custom_authn, new),
    ?assertEqual(#{}, maps:get(trusted_attrs, ClientInfo1)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo1, custom_authn)),
    ?assertEqual({ok, old}, emqx_clientinfo:get_trusted(ClientInfo1, [authn, custom_authn])),
    ClientInfo2 = emqx_clientinfo:set_trusted(ClientInfo0, custom_authn, new),
    ?assertEqual(#{custom_authn => true}, maps:get(trusted_attrs, ClientInfo2)),
    ?assertEqual({ok, new}, emqx_clientinfo:get_trusted(ClientInfo2, custom_authn)),
    ?assertEqual({ok, old}, emqx_clientinfo:get_trusted(ClientInfo2, [authn, custom_authn])),
    ClientInfo3 = emqx_clientinfo:merge_authn_result(ClientInfo0, #{other_custom => value}, merge),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo3, custom_authn)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo3, other_custom)),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo3, [authn, custom_authn])),
    ?assertEqual({ok, value}, emqx_clientinfo:get_trusted(ClientInfo3, [authn, other_custom])),
    ClientInfo4 = emqx_clientinfo:set_trusted(ClientInfo0, [authn, custom_authn], new),
    ?assertEqual(ClientInfo0#{authn := #{custom_authn => new}}, ClientInfo4),
    ?assertError(
        {statically_trusted_attribute, [authn, custom_authn]},
        emqx_clientinfo:set(ClientInfo0, [authn, custom_authn], new)
    ).

%% Verify that colliding unknown outputs stay in authn and cannot replace any client-info field.
t_authn_result_reserved_keys(_) ->
    ClientInfo0 = #{
        zone => default,
        protocol => mqtt,
        peerhost => {127, 0, 0, 1},
        sockport => 1883,
        is_bridge => false,
        cert_pem => <<"broker-cert">>,
        cn => <<"broker-cn">>,
        dn => <<"broker-dn">>,
        listener => tcp_default,
        peername => {{127, 0, 0, 1}, 1234},
        peerport => 1234,
        clientid => <<"client">>,
        username => <<"user">>,
        password => <<"secret">>,
        mountpoint => <<"broker/">>,
        ws_cookie => [],
        peersni => <<"broker-host">>,
        enable_authn => true,
        old_zone => default,
        auth_result => success,
        anonymous => false,
        broker_extension => broker_value
    },
    ReservedKeys = maps:keys(ClientInfo0),
    Collisions = maps:map(fun(_Key, _Value) -> external end, ClientInfo0),
    AuthResult = Collisions#{
        is_superuser => true,
        acl => [rule],
        expire_at => 123,
        custom_authn => output
    },
    ExpectedAuthn = Collisions#{custom_authn => output},
    lists:foreach(
        fun(RequireTrusted) ->
            configure_enforcement(RequireTrusted, RequireTrusted, RequireTrusted),
            ClientInfo = emqx_clientinfo:merge_authn_result(ClientInfo0, AuthResult, merge),
            ?assertEqual(ClientInfo0, maps:with(ReservedKeys, ClientInfo)),
            ?assertMatch(#{is_superuser := true, acl := [rule], auth_expire_at := 123}, ClientInfo),
            ?assertEqual(ExpectedAuthn, maps:get(authn, ClientInfo)),
            ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, custom_authn)),
            ?assertEqual(
                {ok, output}, emqx_clientinfo:get_trusted(ClientInfo, [authn, custom_authn])
            ),
            case RequireTrusted of
                true ->
                    ?assertEqual(#{}, maps:get(trusted_attrs, ClientInfo)),
                    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, username));
                false ->
                    ?assertNot(maps:is_key(trusted_attrs, ClientInfo))
            end
        end,
        [true, false]
    ).

%% Verify that authn fields never shadow present or absent top-level statically trusted fields.
t_authn_metadata_static_keys(_) ->
    StaticFields = #{
        zone => default,
        protocol => mqtt,
        peerhost => {127, 0, 0, 1},
        sockport => 1883,
        is_bridge => false,
        cert_pem => <<"broker-cert">>,
        cn => <<"broker-cn">>,
        dn => <<"broker-dn">>,
        listener => tcp_default,
        peername => {{127, 0, 0, 1}, 1234},
        peerport => 1234,
        is_superuser => false,
        auth_expire_at => undefined,
        acl => [broker_rule]
    },
    Authn = maps:map(fun(_Key, _Value) -> external end, StaticFields),
    lists:foreach(
        fun(Mask) ->
            ClientInfo = StaticFields#{authn => Authn, trusted_attrs => Mask},
            maps:foreach(
                fun(Key, Value) ->
                    ?assertEqual({ok, Value}, emqx_clientinfo:get_trusted(ClientInfo, Key)),
                    ?assertEqual({ok, Value}, emqx_clientinfo:get_trusted(ClientInfo, [Key])),
                    ?assertEqual(Value, maps:get(Key, ClientInfo)),
                    ?assertEqual(
                        {ok, external}, emqx_clientinfo:get_trusted(ClientInfo, [authn, Key])
                    ),
                    ClientInfoWithoutKey = maps:remove(Key, ClientInfo),
                    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfoWithoutKey, Key)),
                    ?assertEqual(missing, maps:get(Key, ClientInfoWithoutKey, missing))
                end,
                StaticFields
            )
        end,
        [#{}, true]
    ).

%% Verify that input and derived fields retain their own trust when authn contains the same key.
t_authn_metadata_reserved_inputs(_) ->
    ClientInfo0 = #{username => <<"user">>, mountpoint => <<"broker/">>},
    Authn = #{username => <<"external-user">>, mountpoint => <<"external/">>},
    ClientInfo = ClientInfo0#{authn => Authn, trusted_attrs => #{}},
    lists:foreach(
        fun(Key) ->
            ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, Key)),
            ?assertEqual(maps:get(Key, ClientInfo0), maps:get(Key, ClientInfo)),
            ?assertEqual(
                {ok, maps:get(Key, Authn)}, emqx_clientinfo:get_trusted(ClientInfo, [authn, Key])
            ),
            TrustedClientInfo = emqx_clientinfo:set_trusted(
                ClientInfo, Key, maps:get(Key, ClientInfo0)
            ),
            ?assertEqual(
                {ok, maps:get(Key, ClientInfo0)},
                emqx_clientinfo:get_trusted(TrustedClientInfo, Key)
            )
        end,
        [username, mountpoint]
    ).

%% Verify that disabled consumers preserve the original map without mask traversal or projections.
t_authn_lookup_skips_fallback(_) ->
    configure_enforcement(false, false, false),
    ok = meck:new(emqx_utils_maps, [passthrough, no_link]),
    emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_utils_maps) end),
    ClientInfo = #{authn => #{custom_authn => output}, trusted_attrs => invalid},
    ?assertEqual(ClientInfo, emqx_clientinfo:maybe_trusted(ClientInfo, false)),
    Context = emqx_authz_context:make(ClientInfo),
    ?assertEqual(ClientInfo, Context),
    #{authn := #{custom_authn := output}} = Context,
    ?assertNot(meck:called(emqx_utils_maps, deep_find, '_')),
    ?assertNot(meck:called(emqx_utils_maps, deep_merge, '_')).

%% Verify that disabled reauthentication replaces custom outputs and drops empty authn maps.
t_replace_custom_authn_disabled(_) ->
    configure_enforcement(false, false, false),
    ClientInfo0 = emqx_clientinfo:merge_authn_result(
        #{zone => default}, #{custom_authn => old}, merge
    ),
    ClientInfo1 = emqx_clientinfo:merge_authn_result(ClientInfo0, #{other_custom => new}, merge),
    ?assertEqual(#{other_custom => new}, maps:get(authn, ClientInfo1)),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo1)),
    ClientInfo2 = emqx_clientinfo:merge_authn_result(ClientInfo1, #{}, merge),
    ?assertNot(maps:is_key(authn, ClientInfo2)),
    ?assertNot(maps:is_key(trusted_attrs, ClientInfo2)).

%% Verify that known authn reads preserve values and defaults without config checks or projections.
t_known_authn_trusted_lookup(_) ->
    Modules = [emqx_config, emqx_utils_maps],
    ok = meck:new(Modules, [passthrough, no_link]),
    emqx_common_test_helpers:on_exit(fun() -> meck:unload(Modules) end),
    KnownOutputs = #{is_superuser => false, auth_expire_at => undefined, acl => #{rules => []}},
    Authn = maps:map(fun(_Key, _Value) -> external end, KnownOutputs),
    ClientInfo0 = KnownOutputs#{authn => Authn},
    lists:foreach(
        fun(ClientInfo) ->
            maps:foreach(
                fun(Key, Value) ->
                    lists:foreach(
                        fun(Path) ->
                            ?assertEqual(
                                {ok, Value}, emqx_clientinfo:get_trusted(ClientInfo, Path)
                            ),
                            ?assertEqual(
                                Value, emqx_clientinfo:get_trusted(ClientInfo, Path, missing)
                            ),
                            WithoutKey = maps:remove(Key, ClientInfo),
                            ?assertEqual(error, emqx_clientinfo:get_trusted(WithoutKey, Path)),
                            ?assertEqual(
                                missing, emqx_clientinfo:get_trusted(WithoutKey, Path, missing)
                            )
                        end,
                        [Key, [Key]]
                    )
                end,
                KnownOutputs
            )
        end,
        [ClientInfo0 | [ClientInfo0#{trusted_attrs => Mask} || Mask <- [#{}, true]]]
    ),
    ?assertNot(meck:called(emqx_config, get, '_')),
    ?assertNot(meck:called(emqx_config, get_zone_conf, '_')),
    ?assertNot(meck:called(emqx_utils_maps, deep_find, '_')),
    ?assertNot(meck:called(emqx_utils_maps, deep_merge, '_')).

%% Verify that default-value access rejects untrusted input and reads custom output only via authn.
t_get_trusted_defaults(_) ->
    ClientInfo = #{
        username => <<"untrusted-user">>,
        clientid => <<"trusted-client">>,
        custom_authn => input,
        client_attrs => #{<<"tns">> => <<"tenant">>, <<"other">> => <<"untrusted">>},
        authn => #{username => <<"authn-user">>, custom_authn => output},
        trusted_attrs => #{clientid => true, client_attrs => #{<<"tns">> => true}}
    },
    ?assertEqual(missing, emqx_clientinfo:get_trusted(ClientInfo, username, missing)),
    ?assertEqual(missing, emqx_clientinfo:get_trusted(ClientInfo, [username], missing)),
    ?assertEqual(missing, emqx_clientinfo:get_trusted(ClientInfo, custom_authn, missing)),
    ?assertEqual(missing, emqx_clientinfo:get_trusted(ClientInfo, missing_key, missing)),
    ?assertEqual(<<"trusted-client">>, emqx_clientinfo:get_trusted(ClientInfo, clientid, missing)),
    ?assertEqual(
        <<"tenant">>, emqx_clientinfo:get_trusted(ClientInfo, [client_attrs, <<"tns">>], missing)
    ),
    ?assertEqual(
        missing, emqx_clientinfo:get_trusted(ClientInfo, [client_attrs, <<"other">>], missing)
    ),
    ?assertEqual(output, emqx_clientinfo:get_trusted(ClientInfo, [authn, custom_authn], missing)),
    ?assertEqual(
        <<"authn-user">>, emqx_clientinfo:get_trusted(ClientInfo, [authn, username], missing)
    ),
    ?assertEqual(
        missing,
        emqx_clientinfo:get_trusted(maps:remove(trusted_attrs, ClientInfo), clientid, missing)
    ),
    ?assertEqual(
        <<"untrusted-user">>,
        emqx_clientinfo:get_trusted(ClientInfo#{trusted_attrs := true}, username, missing)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

configure_enforcement(Authz, MultiTenancy, MQTT) ->
    emqx_config:put([authorization, require_trusted_attributes], Authz),
    emqx_config:put([multi_tenancy, require_trusted_attributes], MultiTenancy),
    emqx_config:put_zone_conf(default, [mqtt, require_trusted_attributes], MQTT).
