%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The gateway instance context
-module(emqx_gateway_ctx).

-export_type([context/0]).

-include("emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").

%% @doc The running context for a Connection/Channel process.
%%
%% The `Context` encapsulates a complex structure of contextual information.
%% It is convenient to use it directly in Channel/Connection to read
%% configuration, register devices and other common operations.
%%
-type context() ::
    #{
        %% Gateway Name
        gwname := gateway_name(),
        %% FIXME: use process name instead of pid()
        %% The ConnectionManager PID
        cm := pid(),
        %% Cached metrics table for hot-path updates
        metrics_tab := ets:table()
    }.

%% Authentication circle
-export([
    authenticate/2,
    connection_expire_interval/2,
    open_session/5,
    open_session/6,
    resume_session/4,
    insert_channel_info/4,
    set_chan_info/3,
    set_chan_stats/3,
    connection_closed/2
]).

%% Message circle
-export([
    authorize/4,
    authorize_publish/3
    % Needless for pub/sub
    %, publish/3
    %, subscribe/4
]).

%% Metrics & Stats
-export([
    metrics_inc/2,
    metrics_inc/3
]).

%%--------------------------------------------------------------------
%% Authentication circle

%% @doc Authenticate whether the client has access to the Broker.
-spec authenticate(context(), emqx_types:clientinfo()) ->
    {ok, emqx_types:clientinfo()}
    | {error, any()}.
authenticate(#{gwname := GwName}, ClientInfo0) ->
    ClientInfo = emqx_clientinfo:set_trusted(ClientInfo0, zone, default),
    case emqx_access_control:authenticate(ClientInfo) of
        {ok, AuthResult} ->
            handle_auth_result(GwName, ClientInfo, AuthResult);
        {error, Reason} ->
            {error, Reason}
    end.

-spec connection_expire_interval(context(), emqx_types:clientinfo()) ->
    undefined | non_neg_integer().
connection_expire_interval(_Ctx, ClientInfo) ->
    case emqx_clientinfo:get_trusted(ClientInfo, auth_expire_at) of
        {ok, undefined} -> undefined;
        {ok, ExpireAt} -> max(0, ExpireAt - erlang:system_time(millisecond));
        error -> undefined
    end.

%% @doc Register the session to the cluster.
%%
%%  This function should be called after the client has authenticated
%%  successfully so that the client can be managed in the cluster.
-spec open_session(
    context(),
    boolean(),
    emqx_types:clientinfo(),
    emqx_types:conninfo(),
    fun(
        (
            emqx_types:clientinfo(),
            emqx_types:conninfo()
        ) -> Session
    )
) ->
    {ok, #{
        session := Session,
        present := boolean(),
        pendings => list()
    }}
    | {error, any()}.
open_session(Ctx, CleanStart, ClientInfo, ConnInfo, CreateSessionFun) ->
    open_session(
        Ctx,
        CleanStart,
        ClientInfo,
        ConnInfo,
        CreateSessionFun,
        emqx_session
    ).

open_session(
    _Ctx = #{gwname := GwName},
    CleanStart,
    ClientInfo,
    ConnInfo,
    CreateSessionFun,
    SessionMod
) ->
    emqx_gateway_cm:open_session(
        GwName,
        CleanStart,
        ClientInfo,
        ConnInfo,
        CreateSessionFun,
        SessionMod
    ).

resume_session(
    _Ctx = #{gwname := GwName},
    ClientInfo,
    ConnInfo,
    SessionMod
) ->
    emqx_gateway_cm:resume_session(
        GwName,
        ClientInfo,
        ConnInfo,
        SessionMod
    ).

-spec insert_channel_info(
    context(),
    emqx_types:clientid(),
    emqx_types:infos(),
    emqx_types:stats()
) -> ok.
insert_channel_info(_Ctx = #{gwname := GwName}, ClientId, Infos, Stats) ->
    emqx_gateway_cm:insert_channel_info(GwName, ClientId, Infos, Stats).

%% @doc Set the Channel Info to the ConnectionManager for this client
-spec set_chan_info(
    context(),
    emqx_types:clientid(),
    emqx_types:infos()
) -> boolean().
set_chan_info(_Ctx = #{gwname := GwName}, ClientId, Infos) ->
    emqx_gateway_cm:set_chan_info(GwName, ClientId, Infos).

-spec set_chan_stats(
    context(),
    emqx_types:clientid(),
    emqx_types:stats()
) -> boolean().
set_chan_stats(_Ctx = #{gwname := GwName}, ClientId, Stats) ->
    emqx_gateway_cm:set_chan_stats(GwName, ClientId, Stats).

-spec connection_closed(context(), emqx_types:clientid()) -> boolean().
connection_closed(_Ctx = #{gwname := GwName}, ClientId) ->
    emqx_gateway_cm:connection_closed(GwName, ClientId).

%%--------------------------------------------------------------------
%% Message circle

-spec authorize(
    context(),
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic()
) ->
    allow | deny.
authorize(_Ctx, ClientInfo, Action, Topic) ->
    AuthzContext = emqx_authz_context:make(ClientInfo),
    emqx_access_control:authorize(AuthzContext, Action, Topic).

-spec authorize_publish(
    context(),
    emqx_types:clientinfo(),
    emqx_types:message()
) -> {allow, emqx_types:message()} | deny | {error, term()}.
authorize_publish(_Ctx, ClientInfo, Msg) ->
    emqx_message_ingress:ingress_and_authorize(ClientInfo, Msg).

%%--------------------------------------------------------------------
%% Metrics & Stats

metrics_inc(_Ctx = #{metrics_tab := Tab}, Name) ->
    emqx_gateway_metrics:inc_tab(Tab, Name).

metrics_inc(_Ctx = #{metrics_tab := Tab}, Name, Oct) ->
    emqx_gateway_metrics:inc_tab(Tab, Name, Oct).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

eval_mountpoint(ClientInfo = #{mountpoint := undefined}) ->
    {ok, ClientInfo};
eval_mountpoint(ClientInfo = #{mountpoint := MountPoint}) ->
    RequireTrustedAttrs = emqx_clientinfo:mqtt_require_trusted_attributes(ClientInfo),
    MountpointClientInfo = emqx_clientinfo:maybe_trusted(ClientInfo, RequireTrustedAttrs),
    case RequireTrustedAttrs of
        false ->
            MountPoint1 = emqx_mountpoint:replvar(MountPoint, MountpointClientInfo),
            {ok, ClientInfo#{mountpoint := MountPoint1}};
        true ->
            case emqx_mountpoint:replvar_strict(MountPoint, MountpointClientInfo) of
                {ok, MountPoint1} ->
                    {ok, emqx_clientinfo:set(ClientInfo, mountpoint, MountPoint1)};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

handle_auth_result(GwName, ClientInfo, AuthResult0) ->
    AuthResult = maybe_drop_clientid_override(GwName, ClientInfo, AuthResult0),
    ClientInfo1 = emqx_clientinfo:merge_authn_result(ClientInfo, AuthResult, replace),
    eval_mountpoint(ClientInfo1).

maybe_drop_clientid_override(GwName, ClientInfo, AuthResult) ->
    case maps:take(clientid_override, AuthResult) of
        {ClientIdOverride, AuthResult1} ->
            ?SLOG(warning, #{
                msg => "gateway_authn_clientid_override_not_supported",
                gateway => GwName,
                clientid => maps:get(clientid, ClientInfo, undefined),
                clientid_override => ClientIdOverride
            }),
            AuthResult1;
        error ->
            AuthResult
    end.
