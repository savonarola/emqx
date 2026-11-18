%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp).
-moduledoc "Shared offline message lifecycle and MQTT hooks.".
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_offline_messages.hrl").
-include("emqx_omp.hrl").

-export([
    apply_changes/1,
    parse_changes/1,
    stop_changes/1,
    start_changes/1,
    on_health_check/2,
    config/1,
    configs/0,
    find/1,
    is_managed/2,
    is_overlapping/2,
    is_client_id_supported/2,
    count/3,
    start/1,
    stop/1
]).
-export([
    on_publish/1,
    on_publish_ack/4,
    on_connected/2,
    on_session/2,
    on_authorize/4,
    on_info/3,
    guard_publish/1,
    on_subscribed/3,
    on_unsubscribed/3
]).

-export_type([backend_id/0, config/0, update/0]).

-define(SQL_BACKENDS, [postgresql, mysql]).

-type backend_id() :: postgresql | mysql | redis.
-type config() :: emqx_omp_backend:config() | emqx_offline_messages_redis:config().
%% A changed backend with its new configuration, or `undefined` when it is disabled.
-type update() :: {backend_id(), config() | undefined}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc """
Apply raw configuration changes of several backends as one operation.
A parse error changes nothing.
A failed start keeps the hooks, logs the error and fails the health check.
""".
-spec apply_changes([#config_change{}]) -> ok | {error, term()}.
apply_changes(Changes) ->
    case parse_changes(Changes) of
        {ok, Updates} ->
            ok = stop_changes(Updates),
            _ = start_changes(Updates),
            ok;
        {error, _} = Error ->
            Error
    end.

-doc """
Parse the changed configurations without changing any backend.
A disabled backend gets the configuration `undefined`.
""".
-spec parse_changes([#config_change{}]) -> {ok, [update()]} | {error, term()}.
parse_changes(Changes) ->
    try
        {ok, [
            {Id, parse_enabled(Module, New)}
         || #config_change{id = Id, module = Module, old = Old, new = New} <- Changes,
            Old =/= New
        ]}
    catch
        Class:Reason -> {error, {invalid_config, {Class, Reason}}}
    end.

-doc """
Stop the runtimes of the changed backends.
Their old configurations stay active, so managed publishes fail instead of being routed
without storage.
""".
-spec stop_changes([update()]) -> ok.
stop_changes(Updates) ->
    lists:foreach(fun({BackendId, _}) -> stop_runtime(BackendId) end, Updates).

-doc """
Replace all configurations in one step, update the hooks and start the enabled runtimes.
Return the start result of each enabled backend.
""".
-spec start_changes([update()]) -> [{backend_id(), ok | {error, {atom(), term()}}}].
start_changes(Updates) ->
    %% Create the metrics before any hook can use the new configurations.
    lists:foreach(
        fun({BackendId, _}) ->
            emqx_metrics_worker:create_metrics(
                ?METRICS_WORKER, BackendId, [write_failed, rejected, skipped]
            )
        end,
        [Update || {_, #{}} = Update <- Updates]
    ),
    %% Replace all configurations in one step, so that no topic has two owners at any time.
    Configs = lists:foldl(fun update_config/2, configs_map(), Updates),
    case map_size(Configs) of
        0 -> persistent_term:erase(?MODULE);
        _ -> persistent_term:put(?MODULE, Configs)
    end,
    ok = update_hooks(Configs),
    [{BackendId, start_runtime(Config)} || {BackendId, #{} = Config} <- Updates].

-doc """
Start or restart the backend for a parsed `Config`.
For SQL backends, the config and hooks are active before the runtime starts. Managed QoS 1/2
publishes fail until the runtime is running, and they keep failing when the start fails.
""".
-spec start(config()) -> ok | {error, {atom(), term()}}.
start(#{backend_id := BackendId} = Config) ->
    Updates = [{BackendId, Config}],
    ok = stop_changes(Updates),
    [{BackendId, Result}] = start_changes(Updates),
    Result.

-doc "Stop the backend runtime, then remove its configuration and the unused hooks.".
-spec stop(backend_id()) -> ok.
stop(BackendId) ->
    Updates = [{BackendId, undefined}],
    ok = stop_changes(Updates),
    [] = start_changes(Updates),
    ok.

-doc "Report an unavailable database or an incompatible schema as a failed health check.".
-spec on_health_check(atom(), map()) -> ok | {error, binary()}.
on_health_check(BackendId, #{<<"enable">> := true}) ->
    Healthy =
        try
            ok = emqx_omp_store:health(config(BackendId)),
            true
        catch
            _:_ -> false
        end,
    case Healthy of
        true -> ok;
        false -> {error, <<(atom_to_binary(BackendId))/binary, " storage is unavailable">>}
    end;
on_health_check(_, _) ->
    ok.

-doc "Return the configuration of the running backend.".
-spec config(backend_id()) -> config() | undefined.
config(BackendId) ->
    case configs_map() of
        #{BackendId := Config} -> Config;
        #{} -> undefined
    end.

-doc "Return the SQL delivery configurations from one consistent snapshot.".
-spec configs() -> [emqx_omp_backend:config()].
configs() ->
    Configs = configs_map(),
    [Config || Id <- ?SQL_BACKENDS, #{Id := Config} <- [Configs]].

-doc "Find the backend that owns an exact topic.".
-spec find(emqx_types:topic()) -> emqx_omp_backend:config() | undefined.
find(Topic) ->
    case [Config || Config <- configs(), is_managed(Topic, Config)] of
        [Config] -> Config;
        [] -> undefined
    end.

-doc "Check whether a topic matches one of the configured topic filters.".
-spec is_managed(emqx_types:topic(), emqx_omp_backend:config() | undefined) -> boolean().
is_managed(Topic, #{topics := Filters}) when is_binary(Topic) ->
    lists:any(fun(Filter) -> emqx_topic:match(Topic, Filter) end, Filters);
is_managed(_, _) ->
    false.

-doc "Check whether a topic filter intersects one of the backend's managed topic filters.".
-spec is_overlapping(emqx_types:topic(), emqx_omp_backend:config() | undefined) -> boolean().
is_overlapping(Topic, #{topics := Filters}) ->
    lists:any(fun(Filter) -> emqx_topic:intersection(Topic, Filter) =/= false end, Filters);
is_overlapping(_, _) ->
    false.

-doc "Check whether the backend can store subscription progress for the client ID.".
-spec is_client_id_supported(emqx_types:clientid(), emqx_omp_backend:config()) -> boolean().
is_client_id_supported(ClientId, #{backend := Backend}) ->
    case Backend:max_client_id_length() of
        infinity -> true;
        Max -> byte_size(ClientId) =< Max
    end.

-doc "Increase a metric of the selected backend.".
-spec count(write_failed | rejected | skipped, pos_integer(), emqx_omp_backend:config()) -> ok.
count(Name, N, #{backend_id := BackendId}) ->
    emqx_metrics_worker:inc(?METRICS_WORKER, BackendId, Name, N).

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

-doc "Reject Redis-managed publishes while its runtime is being replaced or failed to start.".
guard_publish(Msg) ->
    case config(redis) of
        #{runtime_ready := true} ->
            {ok, Msg};
        #{context := #{topic_filters := Filters}} = Config ->
            case emqx_offline_messages_utils:need_persist_message(Msg, Filters) of
                true ->
                    publish_result({error, unavailable}, Msg, Config);
                false ->
                    {ok, Msg}
            end;
        undefined ->
            {ok, Msg}
    end.

-doc "Pass committed wildcard batches to normal session delivery with their matching filter.".
on_info(
    _ClientInfo,
    #dispatch_info{topic = Filter, info = #omp_live{messages = Batch}},
    #{deliver := Delivers} = Acc
) when is_binary(Filter) ->
    case emqx_topic:wildcard(Filter) of
        true ->
            {ok, Acc#{
                deliver := [#deliver{topic = Filter, message = Msg} || Msg <- Batch] ++ Delivers
            }};
        false ->
            {ok, Acc}
    end;
on_info(_ClientInfo, _Info, Acc) ->
    {ok, Acc}.

-doc "Store managed QoS 1/2 publishes, either before acknowledgement or through the batch writer.".
-spec on_publish(emqx_types:message()) -> {ok | stop, emqx_types:message()}.
on_publish(#message{qos = ?QOS_0} = Msg) ->
    {ok, Msg};
on_publish(#message{headers = #{allow_publish := false}} = Msg) ->
    {ok, Msg};
on_publish(#message{headers = #{should_disconnect := true}} = Msg) ->
    {ok, Msg};
on_publish(#message{topic = Topic} = Msg) ->
    Config = find(Topic),
    case is_managed(Topic, Config) of
        false ->
            {ok, Msg};
        true ->
            #{namespace := Namespace, max_topic_length := Max, async_batched := Async} = Config,
            Result =
                case byte_size(Topic) =< Max of
                    false ->
                        {error, topic_too_long};
                    true when Async -> emqx_omp_store:append(Namespace, Msg, Config);
                    true ->
                        emqx_omp_store:append_sync(Namespace, Msg, Config)
                end,
            publish_result(Result, Msg, Config)
    end.

-doc "Suppress regular dispatch after admission or commit, or reject a failed publish.".
publish_result(ok, Msg, _Config) ->
    {stop, persisted(Msg)};
publish_result({error, Reason}, Msg, #{backend_id := BackendId} = Config) ->
    count(write_failed, 1, Config),
    ?SLOG_THROTTLE(error, #{
        msg => offline_messages_write_failed, backend => BackendId, reason => Reason
    }),
    {stop, emqx_message:set_headers(#{allow_publish => false, omp_publish_failed => true}, Msg)}.

on_publish_ack(_PacketId, #message{headers = #{omp_publish_failed := true}}, _PubRes, _RC) ->
    {stop, ?RC_IMPLEMENTATION_SPECIFIC_ERROR};
on_publish_ack(_PacketId, _Msg, _PubRes, RC) ->
    {ok, RC}.

persisted(Msg) ->
    emqx_message:set_headers(#{allow_publish => false, message_persisted => true}, Msg).

-doc "Remember the session implementation for the authorization and connect hooks.".
-spec on_session(emqx_types:clientinfo(), #{impl := module(), _ => _}) -> ok.
on_session(_ClientInfo, #{impl := Impl}) ->
    put(omp_session_impl, Impl),
    ok.

-doc """
Restore stored subscriptions for MQTT clients with in-memory sessions.
A wildcard filter over the topics of both backends is stored in both and restored once.
""".
-spec on_connected(emqx_types:clientinfo(), emqx_types:conninfo()) -> ok.
on_connected(#{clientid := ClientId, protocol := mqtt} = ClientInfo, _ConnInfo) ->
    Subs = lists:append([restore(ClientId, ClientInfo, Config) || Config <- configs()]),
    emqx_offline_messages_utils:induce_subscriptions(lists:ukeysort(1, Subs));
on_connected(_ClientInfo, _ConnInfo) ->
    ok.

on_subscribed(ClientInfo, Topic, Options) ->
    #{clientid := ClientId} = ClientInfo,
    SubOpts = maps:with([qos, nl, rap, rh, subid], Options),
    lists:foreach(
        fun(#{namespace := Namespace, new_subscription_start := Start} = Config) ->
            case is_unsupported(ClientInfo, Options, Topic, Config) of
                false ->
                    Position =
                        case emqx_topic:wildcard(Topic) of
                            true -> latest;
                            false -> Start
                        end,
                    subscription_result(
                        subscribe,
                        Config,
                        emqx_omp_store:subscribe(
                            Namespace, ClientId, Topic, SubOpts, Position, Config
                        )
                    );
                true ->
                    ok
            end
        end,
        subscription_backends(ClientInfo, Topic)
    ).

on_unsubscribed(#{clientid := ClientId} = ClientInfo, Topic, _Options) ->
    lists:foreach(
        fun(#{namespace := Namespace} = Config) ->
            subscription_result(
                unsubscribe, Config, emqx_omp_store:unsubscribe(Namespace, ClientId, Topic, Config)
            )
        end,
        subscription_backends(ClientInfo, Topic)
    ).

subscription_backends(#{clientid := ClientId, protocol := mqtt}, Topic) when is_binary(Topic) ->
    case get(omp_session_impl) of
        emqx_session_mem ->
            [
                Config
             || Config <- configs(),
                is_client_id_supported(ClientId, Config),
                is_restorable(Topic, Config)
            ];
        _ ->
            []
    end;
subscription_backends(_, _) ->
    [].

subscription_result(_Operation, _Config, {ok, _}) ->
    ok;
subscription_result(Operation, #{backend_id := BackendId}, {error, Reason}) ->
    ?SLOG_THROTTLE(error, #{
        msg => offline_messages_query_failed,
        operation => Operation,
        backend => BackendId,
        reason => Reason
    }).

-doc "Return the stored subscriptions of in-memory sessions whose client ID the backend can store.".
restore(ClientId, ClientInfo, #{namespace := Namespace, backend_id := BackendId} = Config) ->
    CanRestore =
        get(omp_session_impl) =:= emqx_session_mem andalso
            is_client_id_supported(ClientId, Config),
    case CanRestore of
        true ->
            case emqx_omp_store:restore(Namespace, ClientId, Config) of
                {ok, Rows} ->
                    %% Skip stored subscriptions that no longer overlap the managed topics.
                    [
                        {unmount(ClientInfo, Topic), subopts(Options)}
                     || {Topic, Options} <- Rows,
                        is_restorable(Topic, Config)
                    ];
                {error, Reason} ->
                    ?SLOG_THROTTLE(error, #{
                        msg => offline_messages_restore_failed,
                        backend => BackendId,
                        reason => Reason
                    }),
                    []
            end;
        false ->
            []
    end.

-doc "Restore managed exact topics and wildcard filters that overlap managed topics.".
is_restorable(Topic, Config) ->
    case emqx_topic:wildcard(Topic) of
        true -> is_overlapping(Topic, Config);
        false -> is_managed(Topic, Config)
    end.

-doc "Deny unsupported subscriptions to managed topics.".
-spec on_authorize(
    emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic(), map()
) -> {ok, map()} | {stop, map()}.
on_authorize(ClientInfo, #{action_type := subscribe} = Action, Topic0, Result) ->
    Topic = authz_topic(ClientInfo, Topic0),
    Denied = lists:any(
        fun(Config) ->
            is_overlapping(Topic, Config) andalso is_unsupported(ClientInfo, Action, Topic, Config)
        end,
        configs()
    ),
    case Denied of
        true -> {stop, #{result => deny, from => ?MODULE, is_cacheable => false}};
        false -> {ok, Result}
    end;
on_authorize(_ClientInfo, _Action, _Topic, Result) ->
    {ok, Result}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

parse_enabled(Parser, #{<<"enable">> := true} = New) -> Parser:parse(New);
parse_enabled(_Parser, _New) -> undefined.

update_config({BackendId, undefined}, Configs) -> maps:remove(BackendId, Configs);
update_config({redis, Config}, Configs) -> Configs#{redis => Config#{runtime_ready => false}};
update_config({BackendId, Config}, Configs) -> Configs#{BackendId => Config}.

configs_map() -> persistent_term:get(?MODULE, #{}).

-doc "Install the hooks while any backend is configured, and remove them otherwise.".
update_hooks(Configs) when map_size(Configs) =:= 0 ->
    lists:foreach(
        fun({Hook, Function, _}) -> emqx_hooks:del(Hook, {?MODULE, Function}) end, hooks()
    ),
    emqx_extsub_handler_registry:unregister(emqx_omp_extsub),
    ok;
update_hooks(_Configs) ->
    Options = #{multi_topic => false, handle_generic_messages => true},
    case emqx_extsub_handler_registry:register(emqx_omp_extsub, Options) of
        ok -> ok;
        {error, extsub_handler_already_registered} -> ok
    end,
    lists:foreach(
        fun({Hook, Function, Priority}) ->
            ok = emqx_hooks:put(Hook, {?MODULE, Function, []}, Priority)
        end,
        hooks()
    ).

stop_runtime(redis) ->
    case config(redis) of
        undefined ->
            ok;
        #{} = Config ->
            persistent_term:put(?MODULE, (configs_map())#{redis := Config#{runtime_ready => false}}),
            emqx_offline_messages_redis:stop()
    end;
stop_runtime(BackendId) ->
    case whereis(emqx_omp_sup) of
        undefined -> ok;
        _ -> emqx_omp_sup:stop(BackendId)
    end.

-doc "Start the pools, writers and cleanup worker. Log and return a failure.".
start_runtime(#{backend_id := BackendId} = Config) ->
    try
        do_start_runtime(Config)
    catch
        Class:Reason ->
            ?SLOG(error, #{
                msg => offline_messages_start_failed,
                backend => BackendId,
                class => Class,
                reason => Reason
            }),
            {error, {Class, Reason}}
    end.

-doc "Install the schema when requested and check the started database pools.".
do_start_runtime(#{backend_id := redis} = Config) ->
    ok = emqx_offline_messages_redis:start(Config),
    persistent_term:put(?MODULE, (configs_map())#{redis := Config#{runtime_ready => true}});
do_start_runtime(#{init_default_schema := Install} = Config) ->
    case Install of
        true -> ok = emqx_omp_store:install(Config);
        false -> ok
    end,
    {ok, _} = emqx_omp_sup:start(Config),
    ok = emqx_omp_store:health(Config).

hooks() ->
    [
        {'message.publish', guard_publish, ?HP_HIGHEST + 1},
        {'message.publish', on_publish, ?HP_LOWEST - 1},
        {'message.puback', on_publish_ack, ?HP_LOWEST},
        {'message.pubrec', on_publish_ack, ?HP_LOWEST},
        {'session.subscribed', on_subscribed, ?HP_HIGHEST},
        {'session.unsubscribed', on_unsubscribed, ?HP_HIGHEST},
        {'client.handle_info', on_info, ?HP_LOWEST},
        {'client.connected', on_connected, ?HP_LOWEST},
        {'session.created', on_session, ?HP_LOWEST + 1},
        {'session.resumed', on_session, ?HP_LOWEST + 1},
        {'client.authorize', on_authorize, ?HP_AUTHZ + 1}
    ].

-doc """
Return `true` for a subscription that a managed topic does not support.
Managed topics accept subscriptions from in-memory MQTT sessions whose client ID the backend
can store. Exact topics need QoS 1, because their delivery is acknowledged and replayed.
Wildcard filters get only live delivery, so they accept any QoS.
Authorization cannot see the share prefix, so shared subscriptions pass but receive
no managed messages.
""".
is_unsupported(
    #{protocol := mqtt, clientid := ClientId},
    #{qos := QoS},
    Topic,
    #{max_topic_length := Max} = Config
) ->
    (QoS =/= ?QOS_1 andalso not emqx_topic:wildcard(Topic)) orelse
        get(omp_session_impl) =/= emqx_session_mem orelse byte_size(Topic) > Max orelse
        not is_client_id_supported(ClientId, Config);
is_unsupported(_ClientInfo, _Action, _Topic, _Config) ->
    true.

-doc "Return the mounted topic. With `include_mountpoint`, authorization passes it mounted.".
authz_topic(ClientInfo, Topic) ->
    case emqx:get_config([authorization, include_mountpoint], false) of
        true -> Topic;
        false -> mount(ClientInfo, Topic)
    end.

mount(#{mountpoint := Mountpoint}, Topic) -> emqx_mountpoint:mount(Mountpoint, Topic);
mount(_, Topic) -> Topic.

unmount(#{mountpoint := Mountpoint}, Topic) -> emqx_mountpoint:unmount(Mountpoint, Topic);
unmount(_, Topic) -> Topic.

-doc "Restore only the MQTT subscription options understood by the broker.".
subopts(JSON) ->
    Raw = emqx_utils_json:decode(JSON),
    maps:from_list([
        {Key, Value}
     || Key <- [qos, nl, rap, rh, subid],
        {ok, Value} <- [maps:find(atom_to_binary(Key), Raw)]
    ]).
