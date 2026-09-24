%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_context_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_security_boundary(_) ->
    ClientInfo = #{
        zone => default,
        username => <<"user">>,
        password => <<"passwd">>,
        custom_authz_field => custom_value,
        trusted_attrs => #{
            authn => #{is_superuser => true},
            clientinfo => #{username => true}
        }
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
        ?assertNot(maps:is_key(is_superuser, Context)),
        ?assertEqual(true, emqx_authz_context:get_authn(Context, is_superuser, false)),
        Persisted = emqx_authz_context:make_persist(Context),
        ?assertEqual(true, emqx_authz_context:get_authn(Persisted, is_superuser, false))
    end).
