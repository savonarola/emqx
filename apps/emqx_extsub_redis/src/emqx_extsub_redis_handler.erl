%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_handler).

-behaviour(emqx_extsub_handler).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_extsub_redis_internal.hrl").

-export([
    handle_allow_subscribe/2,
    handle_init/3,
    handle_terminate/2,
    handle_ack/5,
    handle_info/3
]).

-record(fetch_data, {}).

handle_allow_subscribe(_ClientInfo, _TopicFilter) ->
    true.

handle_init(
    _InitType,
    #{send_after := SendAfterFn, send := SendFn} = _Ctx,
    <<"$redis/", _/binary>> = TopicFilter
) ->
    ok = emqx_extsub_redis_sub:subscribe(#{topic => TopicFilter, pub_fn => SendFn}),
    ok = SendFn(#fetch_data{}),
    {ok, #{
        topic_filter => TopicFilter,
        send_after => SendAfterFn,
        send => SendFn,
        last_id => 0,
        has_data => true
    }};
handle_init(_InitType, _Ctx, _TopicFilter) ->
    ignore.

handle_terminate(_TerminateType, #{topic_filter := TopicFilter} = _State) ->
    ok = emqx_extsub_redis_sub:unsubscribe(#{topic => TopicFilter}),
    ok.

handle_ack(State, #{desired_message_count := DesiredCount} = _AckCtx, MessageId, _Message, Ack) ->
    ?tp_debug(handle_ack, #{message_id => MessageId, ack => Ack}),
    ok = maybe_schedule_fetch(State, DesiredCount),
    State.

handle_info(State, #{desired_message_count := 0} = _InfoCtx, #fetch_data{}) ->
    {ok, State};
handle_info(
    #{has_data := false} = State,
    #{desired_message_count := _DesiredMessageCount} = _InfoCtx,
    #fetch_data{}
) ->
    {ok, State};
handle_info(
    #{has_data := true} = State0,
    #{desired_message_count := DesiredMessageCount} = _InfoCtx,
    #fetch_data{}
) ->
    {Messages, State} = fetch_data(State0, DesiredMessageCount),
    case Messages of
        [] ->
            {ok, State};
        _ ->
            {ok, State, Messages}
    end;
handle_info(#{has_data := true} = State, _InfoCtx, {redis_pub, _Topic, _Notification}) ->
    {ok, State};
handle_info(
    #{has_data := false} = State,
    #{desired_message_count := 0} = _InfoCtx,
    {redis_pub, _Topic, _Notification}
) ->
    {ok, State#{has_data => true}};
handle_info(
    #{has_data := false, last_id := LastId, send := SendFn} = State,
    _InfoCtx,
    {redis_pub, _Topic, Notification}
) ->
    {Id, Message} = parse_pub_message(State, Notification),
    case Id of
        N when N =:= LastId + 1 ->
            %% New next message, send it
            {ok, State#{has_data => true, last_id => Id}, [{Id, Message}]};
        N when N =< LastId ->
            %% Stale message, ignore
            {ok, State};
        _ ->
            %% Next notification from the future, we missed something and have to read.
            ok = SendFn(#fetch_data{}),
            {ok, State#{has_data => true}}
    end;
handle_info(State, _CallbackCtx, _Info) ->
    ?tp(warning, unknown_extsub_redis_handler_info, #{info => _Info, state => State}),
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

maybe_schedule_fetch(_State, 0) ->
    ok;
maybe_schedule_fetch(#{has_data := false} = _State, _DesiredCount) ->
    ok;
maybe_schedule_fetch(#{has_data := true, send := SendFn} = _State, DesiredCount) when
    DesiredCount > 0
->
    ok = SendFn(#fetch_data{}).

fetch_data(#{last_id := LastId, topic_filter := TopicFilter} = State, Limit) ->
    Results = emqx_extsub_redis_read:query(
        q,
        [
            [
                <<"ZRANGEBYSCORE">>,
                emqx_extsub_redis_persist:messages_key(TopicFilter),
                integer_to_binary(LastId + 1),
                <<"+inf">>,
                <<"WITHSCORES">>,
                <<"LIMIT">>,
                <<"0">>,
                integer_to_binary(Limit)
            ]
        ]
    ),
    case Results of
        {ok, []} ->
            {[], State#{has_data => false}};
        {ok, MessagesRaw} when length(MessagesRaw) < Limit ->
            {Messages, NewLastId} = to_messages(State, MessagesRaw),
            {Messages, State#{has_data => false, last_id => NewLastId}};
        {ok, MessagesRaw} ->
            {Messages, NewLastId} = to_messages(State, MessagesRaw),
            {Messages, State#{has_data => true, last_id => NewLastId}}
    end.

to_messages(State, MessageRows) ->
    to_messages(State, MessageRows, [], 0).

to_messages(State, [MessageBin, IdBin | Rest], Acc, MaxId) ->
    Message = emqx_mq_message_db:decode_message(MessageBin),
    Id = binary_to_integer(IdBin),
    to_messages(State, Rest, [{Id, Message} | Acc], max(MaxId, Id));
to_messages(_State, [], Acc, MaxId) ->
    {lists:reverse(Acc), MaxId}.

parse_pub_message(_State, Bin) ->
    [IdBin, MessageBin] = binary:split(Bin, <<":">>),
    Message = emqx_mq_message_db:decode_message(MessageBin),
    Id = binary_to_integer(IdBin),
    {Id, Message}.
