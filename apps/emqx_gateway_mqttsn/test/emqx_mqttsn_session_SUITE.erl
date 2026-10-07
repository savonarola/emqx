%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqttsn_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

%% Verify that session resume preserves top-level authentication output with or without metadata.
t_resume_clientinfo_preserves_authn(_) ->
    NewClientInfo = #{
        clientid => <<"client">>,
        peerhost => {127, 0, 0, 2},
        is_superuser => false,
        auth_expire_at => undefined,
        acl => [stale_rule],
        authn => #{custom_authn => stale}
    },
    OldClientInfo = #{
        clientid => <<"client">>,
        peerhost => {127, 0, 0, 1},
        username => <<"user">>,
        is_superuser => true,
        auth_expire_at => 123,
        acl => [rule],
        authn => #{custom_authn => output}
    },
    Expected = OldClientInfo#{peerhost := {127, 0, 0, 2}},
    ?assertEqual(Expected, emqx_mqttsn_session:resume_clientinfo(NewClientInfo, OldClientInfo)),
    TrustedAttrs = #{username => true},
    ?assertEqual(
        Expected#{trusted_attrs => TrustedAttrs},
        emqx_mqttsn_session:resume_clientinfo(
            NewClientInfo, OldClientInfo#{trusted_attrs => TrustedAttrs}
        )
    ),
    NoAuthnClientInfo = maps:without([authn, is_superuser, auth_expire_at, acl], OldClientInfo),
    ?assertEqual(
        NoAuthnClientInfo#{peerhost := {127, 0, 0, 2}},
        emqx_mqttsn_session:resume_clientinfo(NewClientInfo, NoAuthnClientInfo)
    ).
