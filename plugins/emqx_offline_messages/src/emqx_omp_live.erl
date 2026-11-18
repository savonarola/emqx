%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_live).
-moduledoc """
Subscription persistence for wildcard filters that overlap managed topics.

The subscription is stored in each overlapping backend, so that it is restored when the
client connects again. The channel delivers committed messages through the normal session.
""".
-behaviour(emqx_extsub_handler).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

%% The handler checks configuration changes at least this often.
-ifdef(TEST).
-define(CONFIG_CHECK_INTERVAL, 500).
-else.
-define(CONFIG_CHECK_INTERVAL, 300000).
-endif.
%% Delay before a failed subscription write is repeated.
-define(RETRY_DELAY, 1000).

-export([
    handle_subscribe/4, handle_unsubscribe/4, handle_terminate/1, handle_info/3, handle_delivered/4
]).

-record(live, {
    filter :: emqx_types:topic(),
    clientid :: emqx_types:clientid(),
    target :: emqx_extsub_handler:message_target(),
    subopts :: emqx_types:subopts(),
    requests = #{} :: #{emqx_omp:backend_id() => map()},
    sync_timer :: reference() | undefined
}).

%%------------------------------------------------------------------------------
%% Handler callbacks
%%------------------------------------------------------------------------------

-doc "Store the subscription for a wildcard filter over managed topics.".
handle_subscribe(_Type, _Context, _State, #share{}) ->
    ignore;
handle_subscribe(_Type, Context, undefined, Filter) ->
    #{message_target := Target, clientinfo := #{clientid := ClientId}, subopts := Options} =
        Context,
    case emqx_topic:wildcard(Filter) andalso configs(Filter, ClientId) of
        [_ | _] = Configs ->
            SubOpts = maps:with([qos, nl, rap, rh, subid], Options),
            Live = #live{filter = Filter, clientid = ClientId, target = Target, subopts = SubOpts},
            {ok, sync(Live, Configs)};
        _ ->
            ignore
    end.

-doc "Remove the stored subscription on unsubscribe and keep it on disconnect.".
handle_unsubscribe(unsubscribe, _Context, #live{filter = Filter, clientid = ClientId} = Live, _) ->
    lists:foreach(
        fun(#{namespace := Namespace} = Config) ->
            log_error(unsubscribe, emqx_omp_store:unsubscribe(Namespace, ClientId, Filter, Config))
        end,
        configs(Filter, ClientId)
    ),
    Live;
handle_unsubscribe(disconnect, _Context, Live, _Filter) ->
    Live.

-doc "Cancel the sync timer.".
handle_terminate(#live{sync_timer = Timer}) ->
    emqx_utils:cancel_timer(Timer),
    ok.

-doc "Ignore acknowledgements, because live delivery keeps no progress.".
handle_delivered(Live, _Context, _Msg, _Ack) ->
    {ok, Live}.

-doc "Handle subscription replies, and retry failed writes and check configuration on the timer.".
handle_info(#live{requests = Requests} = Live, _Context, {result, Ref, Result}) ->
    Updated = maps:map(
        fun
            (_, #{ref := RequestRef, instance := Instance}) when RequestRef =:= Ref ->
                case Result of
                    {ok, _} ->
                        #{instance => Instance};
                    _ ->
                        log_error(subscribe, Result),
                        #{instance => Instance, due => now_ms() + ?RETRY_DELAY}
                end;
            (_, Request) ->
                Request
        end,
        Requests
    ),
    {ok, schedule(Live#live{requests = Updated})};
handle_info(#live{filter = Filter, clientid = ClientId} = Live, _Context, sync) ->
    {ok, sync(Live#live{sync_timer = undefined}, configs(Filter, ClientId))};
handle_info(Live, _Context, _Info) ->
    {ok, Live}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

subscribe(Filter, ClientId, SubOpts, Target, #{namespace := Namespace} = Config) ->
    Ref = make_ref(),
    ok = emqx_omp_store:subscribe_async(
        Namespace, ClientId, Filter, SubOpts, latest, Target, Ref, Config
    ),
    Ref.

-doc """
Store the subscription in backends that do not have it yet, and repeat failed or unanswered
writes. A new configuration instance stores the subscription again.
""".
sync(#live{requests = Requests} = Live, Configs) ->
    Now = now_ms(),
    Updated = maps:from_list([
        {BackendId, sync_request(Live, Config, maps:get(BackendId, Requests, #{}), Now)}
     || #{backend_id := BackendId} = Config <- Configs
    ]),
    schedule(Live#live{requests = Updated}).

-doc "Schedule the next run at the earliest due request, or after the configuration check interval.".
schedule(#live{requests = Requests, target = Target, sync_timer = Timer} = Live) ->
    emqx_utils:cancel_timer(Timer),
    Now = now_ms(),
    Next = lists:min([
        Now + ?CONFIG_CHECK_INTERVAL | [Due || #{due := Due} <- maps:values(Requests)]
    ]),
    Live#live{sync_timer = emqx_extsub_handler:send_after(max(1, Next - Now), Target, sync)}.

-doc """
Keep a stored subscription and a request that is not due yet. A pending request is due when
its reply is overdue.
""".
sync_request(_Live, #{instance := Instance}, #{instance := Instance} = Request, Now) when
    not is_map_key(due, Request);
    map_get(due, Request) > Now
->
    Request;
sync_request(
    #live{filter = Filter, clientid = ClientId, subopts = SubOpts, target = Target},
    #{instance := Instance, request_timeout := Timeout} = Config,
    _Request,
    Now
) ->
    #{
        instance => Instance,
        ref => subscribe(Filter, ClientId, SubOpts, Target, Config),
        due => Now + Timeout + ?RETRY_DELAY
    }.

now_ms() -> erlang:monotonic_time(millisecond).

-doc "Return the running backends whose managed topics overlap the filter and accept the client.".
configs(Filter, ClientId) ->
    [
        Config
     || Config <- emqx_omp:configs(),
        emqx_omp:is_overlapping(Filter, Config),
        emqx_omp:is_client_id_supported(ClientId, Config)
    ].

log_error(_Operation, {ok, _}) ->
    ok;
log_error(Operation, Error) ->
    ?SLOG_THROTTLE(error, #{
        msg => offline_messages_query_failed, operation => Operation, reason => Error
    }).
