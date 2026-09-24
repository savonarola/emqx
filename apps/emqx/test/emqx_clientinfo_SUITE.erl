%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_clientinfo_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

%% Verify that authn composition relocates outputs and trusts returned values and overrides.
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
    ?assertEqual(false, maps:is_key(is_superuser, ClientInfo)),
    ?assertEqual(false, maps:is_key(acl, ClientInfo)),
    ?assertEqual(false, maps:is_key(auth_expire_at, ClientInfo)),
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
        #{acl => [rule], trusted_attrs => #{username => true}},
        merge
    ),
    ClientInfo = emqx_clientinfo:merge_authn_result(
        ClientInfo1,
        #{client_attrs => #{<<"new">> => <<"value">>}},
        replace
    ),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo, acl)),
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
        client_attrs => #{<<"tns">> => <<"tenant">>, <<"untrusted">> => <<"value">>},
        trusted_attrs => #{
            authn => #{is_superuser => false, acl => [rule]},
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
            clientid => <<"client">>,
            client_attrs => #{<<"tns">> => <<"tenant">>},
            trusted_attrs => #{
                authn => #{is_superuser => false, acl => [rule]},
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
    ).

%% Verify that an explicit exclusion takes precedence over static and universal trust.
t_universal_mask_and_exclusion(_) ->
    ClientInfo0 = #{
        zone => default,
        clientid => <<"client">>,
        username => <<"user">>,
        trusted_attrs => #{clientinfo => true}
    },
    ?assertEqual({ok, <<"user">>}, emqx_clientinfo:get_trusted(ClientInfo0, username)),
    ClientInfo1 = emqx_clientinfo:set(ClientInfo0, username, <<"new-user">>),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo1, username)),
    ClientInfo2 = emqx_clientinfo:set(ClientInfo1, zone, other),
    ?assertEqual(error, emqx_clientinfo:get_trusted(ClientInfo2, zone)),
    ClientInfo3 = emqx_clientinfo:set_trusted(ClientInfo2, zone, trusted_zone),
    ?assertEqual({ok, trusted_zone}, emqx_clientinfo:get_trusted(ClientInfo3, zone)).
