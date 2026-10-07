%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_context_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor().

t_security_boundary(_) ->
    ClientInfo = #{
        zone => default,
        username => <<"user">>,
        password => <<"passwd">>,
        custom_authz_field => custom_value,
        is_superuser => true,
        trusted_attrs => #{username => true}
    },
    emqx_common_test_helpers:with_security_profile("legacy", fun() ->
        ?assertEqual(ClientInfo, emqx_authz_context:make(ClientInfo))
    end),
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        Context = emqx_authz_context:make(ClientInfo),
        ?assertEqual(default, maps:get(zone, Context)),
        ?assertEqual(<<"user">>, maps:get(username, Context)),
        ?assertNot(maps:is_key(password, Context)),
        ?assertNot(maps:is_key(custom_authz_field, Context)),
        ?assertEqual(true, maps:get(is_superuser, Context)),
        Persisted = emqx_authz_context:make_persist(Context),
        ?assertEqual(true, maps:get(is_superuser, Persisted))
    end).

%% Verify that persistence retains the authn namespace and the direct input trust mask in both profiles.
t_authn_persistence(_) ->
    ClientInfo = #{
        zone => default,
        is_superuser => true,
        acl => #{rules => []},
        password => <<"secret">>,
        authn => #{custom_authn => output},
        trusted_attrs => #{username => true}
    },
    lists:foreach(
        fun(Profile) ->
            emqx_common_test_helpers:with_security_profile(Profile, fun() ->
                Context = emqx_authz_context:make(ClientInfo),
                Persisted = emqx_authz_context:make_persist(Context),
                ?assertEqual(maps:remove(password, ClientInfo), Persisted),
                ?assertEqual(
                    {ok, output}, emqx_clientinfo:get_trusted(Persisted, [authn, custom_authn])
                )
            end)
        end,
        [legacy, hardened]
    ),
    ?assertEqual(#{}, emqx_authz_context:make_persist(#{})).

%% Verify that custom outputs use authn paths and do not establish trust for same-name client inputs.
t_authn_namespace(_) ->
    Context = #{
        username => <<"untrusted-user">>,
        trusted_input => verified,
        custom_authn => input,
        trusted_attrs => #{trusted_input => true},
        authn => #{custom_authn => output}
    },
    ContextWithoutMetadata = maps:remove(trusted_attrs, Context),
    emqx_common_test_helpers:with_security_profile(hardened, fun() ->
        Restricted = emqx_authz_context:make(Context),
        ?assertNot(maps:is_key(username, Restricted)),
        ?assertEqual(verified, maps:get(trusted_input, Restricted)),
        ?assertNot(maps:is_key(custom_authn, Restricted)),
        ?assertEqual({ok, output}, emqx_clientinfo:get_trusted(Restricted, [authn, custom_authn])),
        ?assertEqual(
            #{authn => #{custom_authn => output}}, emqx_authz_context:make(ContextWithoutMetadata)
        )
    end),
    emqx_common_test_helpers:with_security_profile(legacy, fun() ->
        ?assertEqual(Context, emqx_authz_context:make(Context)),
        ?assertEqual(ContextWithoutMetadata, emqx_authz_context:make(ContextWithoutMetadata)),
        #{custom_authn := input, authn := #{custom_authn := output}} = Context
    end).

%% Verify that authn fields do not shadow broker fields or trusted inputs in either profile.
t_authn_cannot_shadow_clientinfo(_) ->
    Context = #{
        zone => default,
        protocol => mqtt,
        username => <<"user">>,
        is_superuser => false,
        acl => [broker_rule],
        trusted_attrs => #{},
        authn => #{
            zone => external_zone,
            protocol => external_protocol,
            listener => external_listener,
            username => <<"external-user">>,
            is_superuser => true,
            acl => [external_rule]
        }
    },
    lists:foreach(
        fun(Profile) ->
            emqx_common_test_helpers:with_security_profile(Profile, fun() ->
                AuthzContext = emqx_authz_context:make(Context),
                ?assertEqual(default, maps:get(zone, AuthzContext)),
                ?assertEqual(mqtt, maps:get(protocol, AuthzContext)),
                ?assertNot(maps:is_key(listener, AuthzContext)),
                ?assertEqual(false, maps:get(is_superuser, AuthzContext)),
                ?assertEqual([broker_rule], maps:get(acl, AuthzContext)),
                ?assertEqual(
                    {ok, external_zone}, emqx_clientinfo:get_trusted(AuthzContext, [authn, zone])
                ),
                case Profile of
                    hardened ->
                        ?assertNot(maps:is_key(username, AuthzContext));
                    legacy ->
                        ?assertEqual(
                            <<"user">>, maps:get(username, AuthzContext)
                        )
                end
            end)
        end,
        [legacy, hardened]
    ).
