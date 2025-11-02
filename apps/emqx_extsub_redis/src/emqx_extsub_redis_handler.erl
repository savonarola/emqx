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
    handle_ack/3,
    handle_need_data/2,
    handle_info/2
]).

-define(BUFFER_MAX_LENGTH, 5000).

handle_allow_subscribe(_ClientInfo, _TopicFilter) ->
    true.

handle_init(
    _InitType, #{send_after := SendAfterFn, send := SendFn} = _Ctx, <<"$redis/", _/binary>> = TopicFilter
) ->
    {ok, ReaderPid} = gen_server:start_link(emqx_extsub_redis_reader, [TopicFilter, SendFn], []),
    {ok, #{
        buffer => buffer_new(),
        blocked => false,
        wants_data => true,
        send_after => SendAfterFn,
        send => SendFn,
        reader_pid => ReaderPid
    }};
handle_init(_InitType, _Ctx, _TopicFilter) ->
    ignore.

handle_terminate(_TerminateType, #{reader_pid := ReaderPid} = _State) ->
    ok = emqx_extsub_redis_reader:stop(ReaderPid),
    ok.

handle_ack(State, MessageId, Ack) ->
    ?tp_debug(handle_ack, #{message_id => MessageId, ack => Ack}),
    State.

handle_need_data(#{buffer := Buffer0, blocked := Blocked, reader_pid := ReaderPid} = State, DesiredCount) ->
    case Blocked of
        false -> ok;
        true -> emqx_extsub_redis_reader:unblock(ReaderPid)
    end,
    case is_buffer_empty(Buffer0) of
        true ->
            ?tp_debug(handle_next_buffer_empty, #{}),
            {ok, State#{blocked => false, wants_data => true}};
        false ->
            ?tp_debug(handle_next_buffer_not_empty, #{buffer_length => buffer_length(Buffer0)}),
            {MessageEntries, Buffer1} = buffer_out(Buffer0, DesiredCount),
            {ok, State#{blocked => false, wants_data => false, buffer => Buffer1}, MessageEntries}
    end.

handle_info(#{wants_data := true, buffer := Buffer0, reader_pid := ReaderPid} = State, {redis_data, Messages} = _Info) ->
    % ?tp_debug(handle_info_wants_data_true, #{buffer => Buffer0}),
    Buffer = buffer_in(Buffer0, Messages),
    ok = emqx_extsub_redis_reader:next(ReaderPid),
    {ok, State#{wants_data => false, buffer => buffer_new()}, buffer_all(Buffer)};
handle_info(#{wants_data := false, buffer := Buffer0, reader_pid := ReaderPid} = State, {redis_data, Messages} = _Info) ->
    % ?tp_debug(handle_info_wants_data_false, #{buffer => Buffer0}),
    Buffer = buffer_in(Buffer0, Messages),
    case buffer_length(Buffer) > ?BUFFER_MAX_LENGTH of
        true ->
            emqx_extsub_redis_reader:block(ReaderPid),
            {ok, State#{blocked => true, buffer => Buffer}};
        false ->
            emqx_extsub_redis_reader:next(ReaderPid),
            {ok, State#{buffer => Buffer}}
    end;
handle_info(State, Info) ->
    ?tp_debug(handle_info_unknown, #{info => Info, state => State}),
    {ok, State}.

%% Toy buffer functions

buffer_new() ->
    {0, queue:new()}.

buffer_in({BufferSize, Q0}, MessageEntries) ->
    Q = lists:foldl(
        fun({MessageId, Msg}, QAcc) ->
            queue:in({MessageId, Msg}, QAcc)
        end,
        Q0,
        MessageEntries
    ),
    {BufferSize + length(MessageEntries), Q}.

buffer_all({BufferSize, Q0}) ->
    case queue:out(Q0) of
        {{value, MessageEntry}, Q} ->
            [MessageEntry | buffer_all({BufferSize - 1, Q})];
        {empty, _} ->
            []
    end.

buffer_out(Buf, N) ->
    buffer_out(Buf, N, []).

buffer_out(Buf, 0, Acc) ->
    {lists:reverse(Acc), Buf};
buffer_out({BufferSize, Q}, N, Acc) ->
    case queue:out(Q) of
        {{value, MessageEntry}, Q1} ->
            buffer_out({BufferSize - 1, Q1}, N - 1, [MessageEntry | Acc]);
        {empty, Q1} ->
            {lists:reverse(Acc), {BufferSize, Q1}}
    end.

is_buffer_empty({BufferSize, _Q}) ->
    BufferSize =:= 0.

buffer_length({BufferSize, _Q}) ->
    BufferSize.
