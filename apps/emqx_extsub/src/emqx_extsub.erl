%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub).

-include_lib("emqx/include/emqx_hooks.hrl").
-include("emqx_extsub_internal.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_client_authorize/4,
    on_session_subscribed/3,
    on_session_unsubscribed/3,
    on_session_resumed/2,
    on_session_disconnected/2,
    on_delivery_completed/2,
    on_message_nack/2,
    on_client_handle_info/3
]).

-export([
    inspect/2
]).

-define(tp_extsub(KIND, EVENT), ?tp_debug(KIND, EVENT)).
-define(ST_PD_KEY, {?MODULE, st}).

-record(st, {
    registry :: emqx_extsub_handler_registry:t(),
    buffer :: emqx_extsub_buffer:t(),
    deliver_retry_tref :: reference() | undefined,
    unacked :: #{emqx_extsub_types:message_id() => true}
}).

-record(extsub_info, {
    subscriber_ref :: emqx_extsub_types:subscriber_ref(),
    message_id :: emqx_extsub_types:message_id()
}).

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:add('client.authorize', {?MODULE, on_client_authorize, []}, ?HP_AUTHZ + 1),
    ok = emqx_hooks:add('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.disconnected', {?MODULE, on_session_disconnected, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('message.nack', {?MODULE, on_message_nack, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_LOWEST).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('client.authorize', {?MODULE, on_client_authorize}),
    emqx_hooks:del('message.delivered', {?MODULE, on_message_delivered}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    emqx_hooks:del('session.disconnected', {?MODULE, on_session_disconnected}),
    emqx_hooks:del('message.nack', {?MODULE, on_message_nack}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}).

%%--------------------------------------------------------------------
%% Hooks callbacks
%%--------------------------------------------------------------------

on_delivery_completed(Msg, Info) ->
    with_sub_handler(
        Msg,
        fun(
            #st{unacked = Unacked0, buffer = Buffer0} = St0,
            Handler0,
            #extsub_info{message_id = MessageId, subscriber_ref = SubscriberRef}
        ) ->
            OldDeliveringCount = emqx_extsub_buffer:delivering_count(Buffer0, SubscriberRef),
            Buffer1 = emqx_extsub_buffer:set_delivered(Buffer0, SubscriberRef, MessageId),
            NewDeliveringCount = emqx_extsub_buffer:delivering_count(Buffer1, SubscriberRef),
            BufferSize = emqx_extsub_buffer:size(Buffer1),
            ct:print("on_delivery_completed[~p]: delivering_count: ~p -> ~p~nBufferSize: ~p", [
                SubscriberRef, OldDeliveringCount, NewDeliveringCount, BufferSize
            ]),
            %% Request more data if buffer size for the subscriber is now below the threshold
            case NewDeliveringCount of
                N when N < ?MIN_SUB_DELIVERING ->
                    case emqx_extsub_handler:handle_need_data(Handler0, ?MIN_SUB_DELIVERING) of
                        {ok, Handler1} ->
                            Buffer2 = Buffer1;
                        {ok, Handler1, Messages} ->
                            Buffer2 = emqx_extsub_buffer:add_new(Buffer1, SubscriberRef, Messages)
                    end;
                _ ->
                    Handler1 = Handler0,
                    Buffer2 = Buffer1
            end,
            St = St0#st{buffer = Buffer2},

            %% Update the unacked window
            case emqx_message:qos(Msg) of
                ?QOS_0 ->
                    Handler = emqx_extsub_handler:handle_ack(Handler1, MessageId, undefined),
                    {ok, St, Handler};
                _ ->
                    ReasonCode = maps:get(reason_code, Info, ?RC_SUCCESS),
                    Handler = emqx_extsub_handler:handle_ack(Handler1, MessageId, ReasonCode),
                    Unacked = maps:remove(MessageId, Unacked0),
                    ct:print("on_delivery_completed[~p]: unacked: ~p -> ~p", [
                        SubscriberRef, map_size(Unacked0), map_size(Unacked)
                    ]),
                    {ok, ensure_deliver_retry_timer(0, St#st{unacked = Unacked}), Handler}
            end
        end
    ).

on_message_nack(Msg, false) ->
    with_sub_handler(
        Msg,
        fun(
            #st{unacked = Unacked0, buffer = Buffer0} = St0,
            Handler,
            #extsub_info{message_id = MessageId, subscriber_ref = SubscriberRef}
        ) ->
            Unacked = maps:remove(MessageId, Unacked0),
            UnackedCnt = map_size(Unacked),
            Buffer = emqx_extsub_buffer:add_back(Buffer0, SubscriberRef, MessageId, Msg),
            St = St0#st{unacked = Unacked, buffer = Buffer},
            case UnackedCnt of
                0 ->
                    {ok, ensure_deliver_retry_timer(St), Handler, true};
                _ ->
                    {ok, St, Handler, true}
            end
        end,
        ok
    );
%% Already nacked by some other hook
on_message_nack(_Msg, true) ->
    ok.

on_session_subscribed(ClientInfo, TopicFilter, _SubOpts) ->
    on_init(subscribe, ClientInfo, TopicFilter).

on_session_resumed(ClientInfo, #{subscriptions := Subs} = _SessionInfo) ->
    ok = maps:foreach(
        fun(TopicFilter, _SubOpts) ->
            on_init(resume, ClientInfo, TopicFilter)
        end,
        Subs
    ).

on_init(InitType, ClientInfo, TopicFilter) ->
    with_st(fun(St) ->
        {ok, add_handler(St, InitType, ClientInfo, TopicFilter)}
    end).

on_session_unsubscribed(ClientInfo, TopicFilter, _SubOpts) ->
    on_terminate(unsubscribe, ClientInfo, TopicFilter).

on_terminate(TerminateType, _ClientInfo, TopicFilter) ->
    with_st(fun(St) ->
        {ok, remove_handler(St, TerminateType, TopicFilter)}
    end).

on_session_disconnected(ClientInfo, #{subscriptions := Subs} = _SessionInfo) ->
    ?tp_extsub(extsub_on_session_disconnected, #{subscriptions => Subs}),
    ok = maps:foreach(
        fun(TopicFilter, _SubOpts) ->
            on_terminate(disconnect, ClientInfo, TopicFilter)
        end,
        Subs
    ).

on_client_handle_info(
    #info_extsub_inspect{receiver = Receiver, topic_filter = _TopicFilter}, _HookContext, Acc
) ->
    %% TODO: Implement inspect
    Info = todo,
    erlang:send(Receiver, {Receiver, Info}),
    {ok, Acc};
on_client_handle_info(
    #info_to_extsub{subscriber_ref = SubscriberRef, info = InfoMsg},
    #{session_info_fn := SessionInfoFn, chan_info_fn := ChanInfoFn} = _HookContext,
    #{deliver := Delivers} = Acc
) ->
    InfoHandleResult = with_sub_handler(
        SubscriberRef,
        fun(#st{buffer = Buffer0} = St, Handler0) ->
            case emqx_extsub_handler:handle_info(Handler0, InfoMsg) of
                {ok, Handler} ->
                    {ok, St, Handler, ok};
                {ok, Handler, Messages} ->
                    Buffer = emqx_extsub_buffer:add_new(Buffer0, SubscriberRef, Messages),
                    {ok, St#st{buffer = Buffer}, Handler, try_deliver};
                recreate ->
                    {ok, St, Handler0, recreate}
            end
        end,
        not_found
    ),
    case InfoHandleResult of
        try_deliver ->
            {ok, Acc#{deliver => try_deliver(SessionInfoFn) ++ Delivers}};
        recreate ->
            ok = with_st(fun(St) ->
                ClientInfo = ChanInfoFn(clientinfo),
                {ok, recreate_handler(St, ClientInfo, SubscriberRef)}
            end),
            {ok, Acc};
        ok ->
            {ok, Acc};
        not_found ->
            {ok, Acc}
    end;
on_client_handle_info(
    #info_extsub_try_deliver{},
    #{session_info_fn := SessionInfoFn} = _HookContext,
    #{deliver := Delivers} = Acc
) ->
    {ok, Acc#{deliver => try_deliver(SessionInfoFn) ++ Delivers}};
on_client_handle_info(_Info, _HookContext, Acc) ->
    ?tp_extsub(extsub_on_client_handle_info_unknown, #{info => _Info}),
    {ok, Acc}.

on_client_authorize(
    ClientInfo, #{action_type := subscribe} = _Action, TopicFilter, Result
) ->
    case emqx_extsub_handler:handle_allow_subscribe(ClientInfo, TopicFilter) of
        true ->
            {ok, Result};
        false ->
            %% TODO
            %% make from field more specific
            {stop, #{result => deny, from => extsub}}
    end;
on_client_authorize(_ClientInfo, _Action, _TopicFilter, Result) ->
    {ok, Result}.

%%
%% Introspection
%%

inspect(ChannelPid, TopicFilter) ->
    Self = alias([reply]),
    erlang:send(ChannelPid, #info_extsub_inspect{receiver = Self, topic_filter = TopicFilter}),
    receive
        {Self, Info} ->
            Info
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

add_handler(
    #st{registry = HandlerRegistry0} = St,
    InitType,
    ClientInfo,
    TopicFilter
) ->
    case emqx_extsub_handler:handle_init(InitType, ClientInfo, TopicFilter) of
        ignore ->
            St;
        {ok, SubscriberRef, Handler} ->
            HandlerRegistry = emqx_extsub_handler_registry:register(
                HandlerRegistry0, TopicFilter, SubscriberRef, Handler
            ),

            St#st{registry = HandlerRegistry}
    end.

remove_handler(#st{registry = HandlerRegistry} = St, TerminateType, TopicFilter) when
    is_binary(TopicFilter)
->
    case emqx_extsub_handler_registry:subscriber_ref(HandlerRegistry, TopicFilter) of
        undefined ->
            {ok, St};
        SubscriberRef ->
            remove_handler(St, TerminateType, SubscriberRef)
    end;
remove_handler(#st{registry = HandlerRegistry0} = St, TerminateType, SubscriberRef) ->
    case emqx_extsub_handler_registry:find(HandlerRegistry0, SubscriberRef) of
        undefined ->
            {ok, St};
        Handler ->
            ok = emqx_extsub_handler:handle_terminate(TerminateType, Handler),
            HandlerRegistry = emqx_extsub_handler_registry:delete(
                HandlerRegistry0, SubscriberRef
            ),
            {ok, St#st{registry = HandlerRegistry}}
    end.

recreate_handler(#st{registry = HandlerRegistry0} = St0, ClientInfo, OldSubscriberRef) ->
    case emqx_extsub_handler_registry:topic_filter(HandlerRegistry0, OldSubscriberRef) of
        undefined ->
            St0;
        TopicFilter ->
            St1 = remove_handler(St0, disconnect, OldSubscriberRef),
            add_handler(St1, resume, ClientInfo, TopicFilter)
    end.

try_deliver(SessionInfoFn) ->
    with_st(fun(#st{buffer = Buffer0, unacked = Unacked0} = St) ->
        BufferSize = emqx_extsub_buffer:size(Buffer0),
        UnackedCnt = map_size(Unacked0),
        case deliver_count(SessionInfoFn, UnackedCnt) of
            0 when BufferSize > 0 andalso UnackedCnt =:= 0 ->
                {ok, ensure_deliver_retry_timer(St), []};
            0 ->
                %% * No room but noting to deliver (BufferSize =:= 0)
                %% * No room and there are unacked messages (UnackedCnt > 0) — we will retry
                %% on their ack callback.
                {ok, St, []};
            Room ->
                {MessageEntries, Buffer} = emqx_extsub_buffer:take(Buffer0, Room),
                Unacked = add_unacked(Unacked0, MessageEntries),
                {ok, cancel_deliver_retry_timer(St#st{buffer = Buffer, unacked = Unacked}),
                    delivers(MessageEntries)}
        end
    end).

add_unacked(Unacked, MessageEntries) ->
    lists:foldl(
        fun({_SubscriberRef, MessageId, _Msg}, UnackedAcc) ->
            UnackedAcc#{MessageId => true}
        end,
        Unacked,
        MessageEntries
    ).

delivers(MessageEntries) ->
    lists:map(
        fun({SubscriberRef, MessageId, Msg0}) ->
            Topic = emqx_message:topic(Msg0),
            Msg = emqx_message:set_headers(
                #{
                    ?EXTSUB_HEADER_INFO => #extsub_info{
                        subscriber_ref = SubscriberRef, message_id = MessageId
                    }
                },
                Msg0
            ),
            {deliver, Topic, Msg}
        end,
        MessageEntries
    ).

with_st(Fun) ->
    with_st(Fun, ok).

with_st(Fun, DefaultResult) ->
    St0 = get_st(),
    case Fun(St0) of
        {ok, St} ->
            ok = put_st(St),
            DefaultResult;
        {ok, St, Result} ->
            ok = put_st(St),
            Result
    end.

with_sub_handler(SubscriberRef, Fun) ->
    with_sub_handler(SubscriberRef, Fun, ok).

with_sub_handler(SubscriberRef, Fun, DefaultResult) when is_reference(SubscriberRef) ->
    with_sub_handler(SubscriberRef, Fun, [], DefaultResult);
with_sub_handler(#message{} = Msg, Fun, DefaultResult) ->
    case emqx_message:get_header(?EXTSUB_HEADER_INFO, Msg) of
        undefined ->
            DefaultResult;
        #extsub_info{subscriber_ref = SubscriberRef} = ExtSubInfo ->
            with_sub_handler(SubscriberRef, Fun, [ExtSubInfo], DefaultResult)
    end.

with_sub_handler(SubscriberRef, Fun, Args, DefaultResult) ->
    with_st(
        fun(#st{registry = HandlerRegistry0} = St0) ->
            case emqx_extsub_handler_registry:find(HandlerRegistry0, SubscriberRef) of
                undefined ->
                    {ok, St0};
                Handler0 ->
                    case erlang:apply(Fun, [St0, Handler0 | Args]) of
                        {ok, St} ->
                            {ok, St};
                        {ok, St, Handler} ->
                            HandlerRegistry = emqx_extsub_handler_registry:update(
                                HandlerRegistry0, SubscriberRef, Handler
                            ),
                            {ok, St#st{registry = HandlerRegistry}};
                        {ok, St, Handler, Result} ->
                            HandlerRegistry = emqx_extsub_handler_registry:update(
                                HandlerRegistry0, SubscriberRef, Handler
                            ),
                            {ok, St#st{registry = HandlerRegistry}, Result}
                    end
            end
        end,
        DefaultResult
    ).

deliver_count(SessionInfoFn, UnackedCnt) ->
    InflightMax = SessionInfoFn(inflight_max),
    InflightCnt = SessionInfoFn(inflight_cnt),
    case InflightMax of
        0 -> ?EXTSUB_MAX_UNACKED - UnackedCnt;
        _ -> max(?EXTSUB_MAX_UNACKED - UnackedCnt, InflightMax - InflightCnt)
    end.

ensure_deliver_retry_timer(St) ->
    ensure_deliver_retry_timer(?EXTSUB_DELIVER_RETRY_INTERVAL, St).

ensure_deliver_retry_timer(Interval, #st{deliver_retry_tref = undefined, buffer = Buffer} = St) ->
    case emqx_extsub_buffer:size(Buffer) > 0 of
        true ->
            ct:print("ensure_deliver_retry_timer: schedule try_deliver with interval ~p", [Interval]),
            TRef = erlang:send_after(Interval, self(), #info_extsub_try_deliver{}),
            St#st{deliver_retry_tref = TRef};
        false ->
            ct:print("ensure_deliver_retry_timer: no buffer", []),
            St
    end;
ensure_deliver_retry_timer(_Interval, #st{deliver_retry_tref = TRef} = St) when
    is_reference(TRef)
->
    ct:print("ensure_deliver_retry_timer: already scheduled", []),
    St.

cancel_deliver_retry_timer(#st{deliver_retry_tref = TRef} = St) ->
    ok = emqx_utils:cancel_timer(TRef),
    St#st{deliver_retry_tref = undefined}.

get_st() ->
    case erlang:get(?ST_PD_KEY) of
        undefined -> new_st();
        St -> St
    end.

new_st() ->
    #st{
        registry = emqx_extsub_handler_registry:new(),
        buffer = emqx_extsub_buffer:new(),
        deliver_retry_tref = undefined,
        unacked = #{}
    }.

put_st(St) ->
    _ = erlang:put(?ST_PD_KEY, St),
    ok.
