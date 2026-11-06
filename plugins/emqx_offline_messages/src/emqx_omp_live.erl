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

-export([
    handle_subscribe/4, handle_unsubscribe/4, handle_terminate/1, handle_info/3, handle_delivered/4
]).

-record(live, {
    filter :: emqx_types:topic(),
    clientid :: emqx_types:clientid(),
    target :: emqx_extsub_handler:message_target(),
    subopts :: emqx_types:subopts(),
    requests = #{} :: #{emqx_omp:backend_id() => map()},
    renew_timer :: reference() | undefined
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
            {ok, renew(Live, Configs)};
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

-doc "Cancel the renewal timer.".
handle_terminate(#live{renew_timer = Timer}) ->
    emqx_utils:cancel_timer(Timer),
    ok.

-doc "Ignore acknowledgements, because live delivery keeps no progress.".
handle_delivered(Live, _Context, _Msg, _Ack) ->
    {ok, Live}.

-doc "Handle subscription replies and retry or renew writes on the maintenance timer.".
handle_info(#live{requests = Requests} = Live, _Context, {result, Ref, Result}) ->
    Updated = maps:map(
        fun
            (_, #{ref := RequestRef, interval := Interval} = Request) when RequestRef =:= Ref ->
                Delay =
                    case Result of
                        {ok, _} ->
                            Interval;
                        _ ->
                            log_error(subscribe, Result),
                            1000
                    end,
                maps:remove(ref, Request#{due := now_ms() + Delay});
            (_, Request) ->
                Request
        end,
        Requests
    ),
    {ok, Live#live{requests = Updated}};
handle_info(#live{filter = Filter, clientid = ClientId} = Live, _Context, renew) ->
    {ok, renew(Live#live{renew_timer = undefined}, configs(Filter, ClientId))};
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

-doc "Upsert due subscriptions and replace requests from earlier configuration instances.".
renew(#live{requests = Requests, target = Target} = Live, Configs) ->
    Now = now_ms(),
    Updated = maps:from_list([
        {BackendId, renew_request(Live, Config, maps:get(BackendId, Requests, #{}), Now)}
     || #{backend_id := BackendId} = Config <- Configs
    ]),
    Delay = lists:min([1000 | [renew_interval(C) || C <- Configs]]),
    Live#live{
        requests = Updated,
        renew_timer = emqx_extsub_handler:send_after(Delay, Target, renew)
    }.

renew_request(
    _Live, #{instance := Instance}, #{instance := Instance, due := Due} = Request, Now
) when
    Due > Now
->
    Request;
renew_request(
    #live{filter = Filter, clientid = ClientId, subopts = SubOpts, target = Target},
    #{instance := Instance, request_timeout := Timeout} = Config,
    _Request,
    Now
) ->
    #{
        instance => Instance,
        ref => subscribe(Filter, ClientId, SubOpts, Target, Config),
        due => Now + Timeout + 1000,
        interval => renew_interval(Config)
    }.

renew_interval(#{subscription_expiry := Expiry}) -> max(1, Expiry div 3).

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
