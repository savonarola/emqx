%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_handler).

-behaviour(emqx_extsub_handler).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    handle_allow_subscribe/2,
    handle_init/3,
    handle_terminate/2,
    handle_ack/3,
    handle_need_data/2,
    handle_info/2
]).

-record(fake_msg, {
    n :: integer()
}).

handle_allow_subscribe(_ClientInfo, _TopicFilter) ->
    true.

handle_init(
    _InitType, #{send_after := SendAfterFn} = _Ctx, <<"extsub_test/", Rest/binary>> = TopicFilter
) ->
    try
        [BatchCountBin, BatchSizeBin, IntervalMsBin] = binary:split(Rest, <<"/">>, [global]),
        BatchCount = binary_to_integer(BatchCountBin),
        BatchSize = binary_to_integer(BatchSizeBin),
        IntervalMs = binary_to_integer(IntervalMsBin),
        ok = lists:foreach(
            fun(I) ->
                SendAfterFn(IntervalMs * I, #fake_msg{n = I})
            end,
            lists:seq(0, BatchCount - 1)
        ),
        {ok, #{
            batch_count => BatchCount,
            interval_ms => IntervalMs,
            buffer => queue:new(),
            wants_data => true,
            topic_filter => TopicFilter,
            batch_size => BatchSize
        }}
    catch
        Class:Reason:Stacktrace ->
            ?tp(warning, handle_init_error, #{
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ignore
    end;
handle_init(_InitType, _Ctx, TopicFilter) ->
    ?tp(warning, handle_init_ignore, #{topic_filter => TopicFilter}),
    ignore.

handle_terminate(_TerminateType, _State) ->
    ?tp(warning, handle_terminate, #{state => _State}),
    ok.

handle_ack(State, MessageId, Ack) ->
    ?tp(warning, handle_ack, #{message_id => MessageId, ack => Ack}),
    State.

handle_need_data(#{buffer := Buffer0} = State, DesiredCount) ->
    case is_buffer_empty(Buffer0) of
        true ->
            ?tp(warning, handle_next_buffer_empty, #{}),
            {ok, State#{wants_data => true}};
        false ->
            ?tp(warning, handle_next_buffer_not_empty, #{buffer_length => buffer_length(Buffer0)}),
            {MessageEntries, Buffer1} = buffer_out(Buffer0, DesiredCount),
            {ok, State#{wants_data => false, buffer => Buffer1}, MessageEntries}
    end.

handle_info(#{wants_data := true, buffer := Buffer0} = State, #fake_msg{} = Msg) ->
    ?tp(warning, handle_info_wants_data_true, #{buffer_length => buffer_length(Buffer0), msg => Msg}),
    Buffer = buffer_in(Buffer0, make_messages(State, Msg)),
    {ok, State#{wants_data => false, buffer => buffer_new()}, buffer_all(Buffer)};
handle_info(#{wants_data := false, buffer := Buffer0} = State, #fake_msg{} = Msg) ->
    ?tp(warning, handle_info_wants_data_false, #{buffer_length => buffer_length(Buffer0), msg => Msg}),
    Buffer = buffer_in(Buffer0, make_messages(State, Msg)),
    {ok, State#{buffer => Buffer}};
handle_info(State, Info) ->
    ?tp(warning, handle_info_unknown, #{info => Info, state => State}),
    {ok, State}.

%% Fake message generation functions

make_messages(#{batch_size := BatchSize} = State, #fake_msg{n = BatchN}) ->
    lists:reverse(
        lists:map(
            fun(I) ->
                make_message(State, BatchN, I, BatchSize)
            end,
            lists:seq(0, BatchSize - 1)
        )
    ).

make_message(#{topic_filter := TopicFilter} = _State, BatchN, I, _BatchSize) ->
    Body = iolist_to_binary(io_lib:format("fake msg batch_n=~p, n in batch=~p", [BatchN, I])),
    Msg = emqx_message:make(<<"from">>, ?QOS_1, TopicFilter, Body),
    {{BatchN, I}, Msg}.

%% Toy buffer functions

buffer_new() ->
    queue:new().

buffer_in(Q, MessageEntries) ->
    lists:foldl(
        fun({MessageId, Msg}, QAcc) ->
            queue:in({MessageId, Msg}, QAcc)
        end,
        Q,
        MessageEntries
    ).

buffer_all(Q) ->
    case queue:out(Q) of
        {{value, MessageEntry}, Q1} ->
            [MessageEntry | buffer_all(Q1)];
        {empty, _} ->
            []
    end.

buffer_out(Q, N) ->
    buffer_out(Q, N, []).

buffer_out(Q, 0, Acc) ->
    {lists:reverse(Acc), Q};
buffer_out(Q, N, Acc) ->
    case queue:out(Q) of
        {{value, MessageEntry}, Q1} ->
            buffer_out(Q1, N - 1, [MessageEntry | Acc]);
        {empty, Q1} ->
            {lists:reverse(Acc), Q1}
    end.

is_buffer_empty(Q) ->
    queue:is_empty(Q).

buffer_length(Q) ->
    queue:len(Q).



    % -module(emqx_mq_consumer_streams_redis).

    % -include("../emqx_mq_internal.hrl").
    % -include_lib("snabbkaffe/include/snabbkaffe.hrl").

    % -export([
    %     new/2,
    %     fetch_progress_updates/1,
    %     handle_ds_info/2,
    %     handle_ack/2,
    %     inspect/1
    % ]).

    % -behaviour(gen_server).

    % -export([
    %     init/1,
    %     handle_continue/2,
    %     handle_call/3,
    %     handle_cast/2,
    %     handle_info/2,
    %     terminate/2
    % ]).

    % -define(REDIS_READ_LIMIT, 1000).
    % -define(REDIS_READ_LIMIT_BIN, <<"1000">>).

    % -define(REDIS_MAX_UNACKED, 5000).
    % -define(SLAB, {<<"0">>, 1}).
    % -define(MESSAGE_ID(I), {?SLAB, I}).

    % -record(redis_messages, {
    %     messages = []
    % }).
    % -record(unblock, {}).
    % -record(inspect, {}).

    % -type t() :: #{
    %     reader_pid := pid(),
    %     unacked := #{non_neg_integer() => true}
    % }.

    % -export_type([t/0]).

    % new(MQ, _Progress) ->
    %     {ok, ReaderPid} = gen_server:start_link(?MODULE, [MQ, self()], []),
    %     #{reader_pid => ReaderPid, unacked => #{}, mq => MQ}.

    % handle_ds_info(#{unacked := Unacked0} = State, #redis_messages{messages = MessagesWithIds}) ->
    %     Unacked = lists:foldl(
    %         fun({MessageId, _Message}, UnackedAcc) ->
    %             UnackedAcc#{MessageId => true}
    %         end,
    %         Unacked0,
    %         MessagesWithIds
    %     ),
    %     {ok, MessagesWithIds, maybe_unblock(State#{unacked => Unacked})}.

    % handle_ack(#{unacked := Unacked0} = State, MessageId) ->
    %     Unacked = maps:remove(MessageId, Unacked0),
    %     maybe_unblock(State#{unacked => Unacked}).

    % fetch_progress_updates(State) ->
    %     {undefined, State}.

    % inspect(#{unacked := Unacked, reader_pid := ReaderPid} = _State) ->
    %     ReaderState = gen_server:call(ReaderPid, #inspect{}, infinity),
    %     ReaderState#{unacked => map_size(Unacked)}.

    % %%--------------------------------------------------------------------
    % %% Gen Server Callbacks (Reader)
    % %%--------------------------------------------------------------------

    % init([MQ, ConsumerPid]) ->
    %     {ok, Conn} = eredis:start_link(
    %         emqx_mq_message_db_redis:redis_opts([])
    %     ),
    %     {ok, ListenConn} = eredis_sub:start_link(
    %         emqx_mq_message_db_redis:redis_opts([])
    %     ),
    %     ListenChannel = channel(MQ),
    %     ok = eredis_sub:controlling_process(ListenConn, self()),
    %     ok = eredis_sub:subscribe(ListenConn, [ListenChannel]),
    %     State = #{
    %         mq => MQ,
    %         consumer_pid => ConsumerPid,
    %         connection => Conn,
    %         listen_connection => ListenConn,
    %         has_data => true,
    %         blocked => false,
    %         last_id => {0, 0},
    %         listen_channel => ListenChannel
    %     },
    %     ?tp_debug(mq_consumer_streams_redis_init, #{state => State}),
    %     {ok, State, {continue, fetch_data}}.

    % handle_continue(fetch_data, #{blocked := true} = State) ->
    %     {noreply, State};
    % handle_continue(fetch_data, #{has_data := false} = State) ->
    %     {noreply, State};
    % handle_continue(
    %     fetch_data,
    %     #{
    %         mq := MQ,
    %         connection := Conn
    %     } = State
    % ) ->
    %     Results = eredis:q(Conn, [
    %         <<"XRANGE">>,
    %         key(MQ),
    %         start_id(State),
    %         <<"+">>,
    %         <<"COUNT">>,
    %         ?REDIS_READ_LIMIT_BIN
    %     ]),
    %     case Results of
    %         {ok, []} ->
    %             ?tp_debug(mq_consumer_streams_pg_fetch_data, #{messages => 0}),
    %             {noreply, State#{has_data => false}};
    %         {ok, MessagesRaw} when length(MessagesRaw) < ?REDIS_READ_LIMIT ->
    %             {Messages, NewLastId} = to_messages(State, MessagesRaw),
    %             send_messages(State, Messages),
    %             ?tp_debug(mq_consumer_streams_redis_fetch_data, #{
    %                 messages => N, new_last_id => NewLastId
    %             }),
    %             {noreply, State#{has_data => false, blocked => true, last_id => NewLastId}};
    %         {ok, MessagesRaw} ->
    %             {Messages, NewLastId} = to_messages(State, MessagesRaw),
    %             send_messages(State, Messages),
    %             ?tp_debug(mq_consumer_streams_redis_fetch_data, #{
    %                 messages => _N, new_last_id => NewLastId
    %             }),
    %             {noreply, State#{has_data => true, blocked => true, last_id => NewLastId}}
    %     end.

    % handle_info(#unblock{}, State) ->
    %     {noreply, State#{blocked => false}, {continue, fetch_data}};
    % handle_info(
    %     {subscribed, ListenChannel, _Pid},
    %     #{listen_channel := ListenChannel, listen_connection := ListenConnection} = State
    % ) ->
    %     ?tp_debug(mq_consumer_streams_redis_subscribed, #{}),
    %     ok = eredis_sub:ack_message(ListenConnection),
    %     {noreply, State};
    % handle_info(
    %     {message, ListenChannel, _Notification, _Pid},
    %     #{listen_channel := ListenChannel, listen_connection := ListenConnection} = State
    % ) ->
    %     ?tp_debug(mq_consumer_streams_redis_message, #{notification => _Notification, pid => _Pid}),
    %     ok = eredis_sub:ack_message(ListenConnection),
    %     {noreply, State#{has_data => true}, {continue, fetch_data}};
    % handle_info(Info, State) ->
    %     ?tp(warning, mq_consumer_streams_redis_unknown_info, #{info => Info}),
    %     {noreply, State}.

    % handle_cast(Info, State) ->
    %     ?tp(warning, mq_consumer_streams_pg_unknown_cast, #{info => Info}),
    %     {noreply, State}.

    % handle_call(
    %     #inspect{}, _From, #{has_data := HasData, blocked := Blocked, last_id := LastId} = State
    % ) ->
    %     {reply, #{has_data => HasData, blocked => Blocked, last_id => LastId}, State};
    % handle_call(Info, _From, State) ->
    %     ?tp(warning, mq_consumer_streams_pg_unknown_call, #{info => Info}),
    %     {reply, {error, {unknown_call, Info}}, State}.

    % terminate(_Reason, _State) ->
    %     ok.

    % %%--------------------------------------------------------------------
    % %% Internal functions
    % %%--------------------------------------------------------------------

    % to_messages(State, MessageRows) ->
    %     lists:mapfoldl(
    %         fun(MessageRow, _) ->
    %             parse_message(State, MessageRow)
    %         end,
    %         0,
    %         MessageRows
    %     ).

    % send_messages(#{consumer_pid := ConsumerPid} = _State, Messages) ->
    %     erlang:send(ConsumerPid, #redis_messages{messages = Messages}).

    % %% {Message, Id}
    % parse_message(_State, [IdBin, [<<"m">>, MessageBin]]) ->
    %     Message = emqx_mq_message_db:decode_message(MessageBin),
    %     Id = bin_to_id(IdBin),
    %     {{?MESSAGE_ID({Id}), Message}, Id}.

    % maybe_unblock(
    %     #{unacked := Unacked, reader_pid := ReaderPid, mq := #{stream_max_buffer_size := MaxUnacked}} =
    %         State
    % ) ->
    %     case maps:size(Unacked) < MaxUnacked of
    %         true ->
    %             erlang:send(ReaderPid, #unblock{});
    %         false ->
    %             ok
    %     end,
    %     State.

    % channel(MQ) ->
    %     emqx_mq_message_db_redis:channel(MQ).

    % key(MQ) ->
    %     emqx_mq_message_db_redis:key(MQ).

    % start_id(#{last_id := LastId} = _State) ->
    %     <<"(", (id_to_bin(LastId))/binary>>.

    % bin_to_id(BinId) when is_binary(BinId) ->
    %     [Ts, Seq] = binary:split(BinId, <<"-">>, [global]),
    %     {binary_to_integer(Ts), binary_to_integer(Seq)}.

    % id_to_bin({Ts, Seq}) when is_integer(Ts) andalso is_integer(Seq) ->
    %     <<(integer_to_binary(Ts))/binary, "-", (integer_to_binary(Seq))/binary>>.
