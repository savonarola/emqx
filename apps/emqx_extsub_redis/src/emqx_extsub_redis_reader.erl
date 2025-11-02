%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_reader).

-behaviour(gen_server).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_extsub_redis_internal.hrl").

-export([
    start_link/2,
    block/1,
    unblock/1,
    next/1,
    stop/1
]).

-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(REDIS_READ_LIMIT, 100).
-define(REDIS_READ_LIMIT_BIN, <<"100">>).

-record(unblock, {}).
-record(next, {}).
-record(block, {}).
-record(stop, {}).

start_link(TopicFilter, SendFn) ->
    gen_server:start_link(?MODULE, [TopicFilter, SendFn], []).

%%--------------------------------------------------------------------
%% Gen Server Callbacks (Reader)
%%--------------------------------------------------------------------

block(Pid) ->
    _ = erlang:send(Pid, #block{}),
    ok.

unblock(Pid) ->
    _ = erlang:send(Pid, #unblock{}),
    ok.

next(Pid) ->
    _ = erlang:send(Pid, #next{}),
    ok.

stop(Pid) ->
    gen_server:call(Pid, #stop{}, infinity).

init([TopicFilter, SendFn]) ->
    process_flag(trap_exit, true),
    Self = self(),
    PubFn = fun(Message) ->
        _ = erlang:send(Self, Message),
        ok
    end,
    ok = emqx_extsub_redis_sub:subscribe(#{topic => TopicFilter, pub_fn => PubFn}),
    State = #{
        topic_filter => TopicFilter,
        send_fn => SendFn,
        last_id => 0,
        has_data => true,
        blocked => false
    },
    {ok, State, {continue, fetch_data}}.

handle_continue(fetch_data, #{blocked := true} = State) ->
    {noreply, State};
handle_continue(fetch_data, #{has_data := false} = State) ->
    {noreply, State};
handle_continue(
    fetch_data,
    #{
        topic_filter := TopicFilter,
        last_id := LastId
    } = State
) ->
    ?tp_debug(fetch_data, #{topic_filter => TopicFilter, last_id => LastId, state => State}),
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
                ?REDIS_READ_LIMIT_BIN
            ]
        ]
    ),
    case Results of
        {ok, []} ->
            {noreply, State#{has_data => false}};
        {ok, MessagesRaw} when length(MessagesRaw) < ?REDIS_READ_LIMIT ->
            {Messages, NewLastId} = to_messages(State, MessagesRaw),
            send_messages(State, Messages),
            {noreply, State#{has_data => false, last_id => NewLastId}};
        {ok, MessagesRaw} ->
            {Messages, NewLastId} = to_messages(State, MessagesRaw),
            send_messages(State, Messages),
            {noreply, State#{has_data => true, last_id => NewLastId}}
    end.

handle_info(#next{}, State) ->
    {noreply, State, {continue, fetch_data}};
handle_info(#block{}, State) ->
    {noreply, State#{blocked => true}};
handle_info(#unblock{}, State) ->
    {noreply, State#{blocked => false}, {continue, fetch_data}};
handle_info({redis_pub, _Topic, Notification}, #{last_id := LastId} = State) ->
    {Id, Message} = parse_pub_message(State, Notification),
    ?tp_debug(redis_pub, #{notification => Notification, new_id => Id, state => State}),
    case LastId + 1 of
        Id ->
            send_messages(State, [{Id, Message}]),
            {noreply, State#{last_id => Id}};
        _ ->
            {noreply, State#{has_data => true}, {continue, fetch_data}}
    end;
handle_info(Info, State) ->
    ?tp_debug(unknown_info, #{info => Info, state => State}),
    {noreply, State}.

handle_cast(Info, State) ->
    ?tp_debug(mq_consumer_streams_pg_unknown_cast, #{info => Info}),
    {noreply, State}.

handle_call(#stop{}, _From, State) ->
    {stop, normal, ok, State};
handle_call(Info, _From, State) ->
    ?tp_debug(mq_consumer_streams_pg_unknown_call, #{info => Info}),
    {reply, {error, {unknown_call, Info}}, State}.

terminate(_Reason, #{topic_filter := TopicFilter} = _State) ->
    ?tp_debug(terminate, #{topic_filter => TopicFilter, reason => _Reason}),
    ok = emqx_extsub_redis_sub:unsubscribe(#{topic => TopicFilter}),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

to_messages(State, MessageRows) ->
    to_messages(State, MessageRows, [], 0).

to_messages(State, [MessageBin, IdBin | Rest], Acc, MaxId) ->
    Message = emqx_mq_message_db:decode_message(MessageBin),
    Id = binary_to_integer(IdBin),
    to_messages(State, Rest, [{Id, Message} | Acc], max(MaxId, Id));
to_messages(_State, [], Acc, MaxId) ->
    {lists:reverse(Acc), MaxId}.

send_messages(#{send_fn := SendFn} = _State, Messages) ->
    SendFn({redis_data, Messages}).

parse_pub_message(_State, Bin) ->
    [IdBin, MessageBin] = binary:split(Bin, <<":">>),
    Message = emqx_mq_message_db:decode_message(MessageBin),
    Id = binary_to_integer(IdBin),
    {Id, Message}.
