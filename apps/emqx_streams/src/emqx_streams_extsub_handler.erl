%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_extsub_handler).

-moduledoc """
Handler for the external subscription to the streams.

Important: `stream' here means EMQX Streams, a feature of EMQX MQTT Broker and the corresponding
data structure associated with it.
DS streams are explicity called `DS streams' here.
""".

-behaviour(emqx_extsub_handler).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
% -include_lib("emqx_extsub/include/emqx_extsub_internal.hrl").

%% ExtSub Handler callbacks

-export([
    handle_subscribe/4,
    handle_unsubscribe/3,
    handle_terminate/1,
    handle_delivered/4,
    handle_info/3
]).

%% DS Client callbacks

-export([
    get_current_generation/3,
    on_advance_generation/4,
    get_iterator/4,
    on_new_iterator/5,
    on_unrecoverable_error/5,
    on_subscription_down/4
]).

-record(stream_status_unblocked, {}).
-record(stream_status_blocked, {
    unblock_fn :: fun((handler()) -> handler())
}).

-type stream_status() :: #stream_status_unblocked{} | #stream_status_blocked{}.

-record(status_blocked, {}).
-record(status_unblocked, {}).

-type status() :: #status_blocked{} | #status_unblocked{}.

%% DS Client subscription ID
-type sub_id() :: reference().

-record(stream_state, {
    stream :: emqx_streams_types:stream(),
    iterator :: emqx_ds_iterator:iterator(),
    slab :: emqx_ds_slab:slab(),
    current_generation :: emqx_ds:generation(),
    filter :: fun((emqx_ds:time()) -> boolean()),
    sub_id :: sub_id(),
    status :: stream_status()
}).

-type stream_state() :: #stream_state{}.

-record(state, {
    send_fn :: function(),
    send_after_fn :: function(),
    status :: status(),
    subs :: #{sub_id() => stream_state()},
    topic_filters :: #{emqx_types:topic() => sub_id()}
}).

-type state() :: #state{}.

-record(h, {
    state :: state(),
    ds_client :: emqx_ds_client:client()
}).

-type handler() :: #h{}.

-record(complete_stream, {
    sub_id :: sub_id(),
    slab :: {emqx_ds:shard(), emqx_ds:generation()},
    ds_stream :: emqx_ds:stream()
}).

%%------------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------------

%% ExtSub Handler callbacks

handle_subscribe(
    _SubscribeType,
    SubscribeCtx,
    Handler0,
    <<"$s/", Rest0/binary>> = FullTopicFilter
) ->
    maybe
        [Partition, Rest1] ?= binary:split(Rest0, <<"/">>),
        [OffsetBin, TopicFilter] ?= binary:split(Rest1, <<"/">>),
        {ok, Stream} = emqx_streams_registry:find(TopicFilter),
        true ?= lists:member(Partition, emqx_streams_message_db:partitions(Stream)),
        {ok, Offset} ?= parse_timestamp(OffsetBin),
        {ok, Iterator, {_, Generation} = Slab, Filter} ?=
            emqx_streams_message_db:iterator(Stream, Partition, Offset),
        false ?= topic_present(Handler0, FullTopicFilter),
        #h{
            state = #state{topic_filters = TopicFilters, subs = Subs} = State0,
            ds_client = DSClient0
        } = Handler = init_handler(Handler0, SubscribeCtx),
        SubId = make_ref(),
        StreamState = #stream_state{
            stream = Stream,
            iterator = Iterator,
            slab = Slab,
            current_generation = Generation,
            filter = Filter,
            sub_id = SubId,
            status = #stream_status_unblocked{}
        },
        State1 = State0#state{
            topic_filters = TopicFilters#{FullTopicFilter => SubId}, subs = Subs#{SubId => StreamState}
        },
        {ok, DSClient, State} = emqx_streams_message_db:subscribe(Stream, DSClient0, SubId, State1),
        {ok, Handler#h{state = State, ds_client = DSClient}}
    else
        Error ->
            ?tp(error, streams_handler_subscribe_error, #{error => Error, topic_filter => FullTopicFilter}),
            ignore
    end;
handle_subscribe(_SubscribeType, _SubscribeCtx, _Handler, _FullTopicFilter) ->
    ignore.

handle_unsubscribe(
    _UnsubscribeType,
    #h{state = #state{topic_filters = TopicFilters0} = State0, ds_client = DSClient0} = Handler,
    FullTopicFilter
) ->
    % -spec unsubscribe(t(), sub_id(), HostState) -> {ok, t(), HostState} | {error, not_found}.
    case TopicFilters0 of
        #{FullTopicFilter := SubId} ->
            {ok, DSClient, #state{topic_filters = TopicFilters, subs = Subs} = State1} = emqx_ds_client:unsubscribe(
                DSClient0, SubId, State0
            ),
            State = State1#state{
                topic_filters = maps:remove(FullTopicFilter, TopicFilters),
                subs = maps:remove(SubId, Subs)
            },
            Handler#h{state = State, ds_client = DSClient};
        _ ->
            Handler
    end.

handle_terminate(#h{state = State, ds_client = DSClient}) ->
    ?tp(debug, handle_terminate, #{state => State}),
    _ = emqx_ds_client:destroy(DSClient, State),
    ok.

handle_delivered(
    HState,
    #{desired_message_count := DesiredCount} = _DeliveredCtx,
    _Message,
    _Ack
) ->
    update_blocking_status(HState, DesiredCount).

handle_info(HState, #{desired_message_count := DesiredCount} = _InfoCtx, Info) ->
    handle_info(update_blocking_status(HState, DesiredCount), Info).

%% DS Client callbacks

get_current_generation(SubId, _Shard, #state{subs = Subs}) ->
    case Subs of
        #{SubId := #stream_state{current_generation = Generation}} ->
            Generation;
        _ ->
            error(stream_not_found)
    end.

on_advance_generation(
    SubId, Shard, Generation, #state{subs = Subs} = State
) ->
    case Subs of
        #{SubId := #stream_state{slab = {Shard, _}} = StreamState} ->
            update_stream_state(State, SubId, StreamState#stream_state{
                current_generation = Generation
            });
        _ ->
            error(stream_not_found)
    end;
on_advance_generation(_SubId, _Shard, _Generation, State) ->
    State.

get_iterator(SubId, {Shard, Generation}, DSStream, #state{subs = Subs, send_fn = SendFn}) ->
    case Subs of
        #{SubId := #stream_state{iterator = Iterator, slab = {Shard, StartGen}}} when
            StartGen =:= Generation
        ->
            {subscribe, Iterator};
        #{SubId := #stream_state{slab = {Shard, StartGen}}} when StartGen < Generation ->
            undefined;
        _ ->
            SendFn(#complete_stream{
                sub_id = SubId, slab = {Shard, Generation}, ds_stream = DSStream
            }),
            {ok, end_of_stream}
    end;
get_iterator(_SubId, _Slab, _DSStream, _State) ->
    undefined.

on_new_iterator(SubId, {Shard, Generation}, _Stream, _It, #state{subs = Subs} = State) ->
    case Subs of
        #{SubId := #stream_state{slab = {Shard, StartGeneration}}} when
            StartGeneration =< Generation
        ->
            {subscribe, State};
        _ ->
            %% Should never happen
            {ignore, State}
    end.

on_unrecoverable_error(_SubId, Slab, _DSStream, Error, State) ->
    ?tp(error, unrecoverable_error, #{slab => Slab, error => Error}),
    %% TODO
    %% Handle unrecoverable error
    State.

on_subscription_down(_SubId, Slab, _DSStream, State) ->
    ?tp(error, subscription_down, #{slab => Slab}),
    %% TODO
    %% Handle subscription down
    State.

%% Internal functions

handle_info(
    #h{state = #state{subs = Subs} = State0, ds_client = DSClient0} = HState, {generic, Info}
) ->
    case emqx_ds_client:dispatch_message(Info, DSClient0, State0) of
        ignore ->
            {ok, HState};
        {DSClient, State} ->
            {ok, HState#h{ds_client = DSClient, state = State}};
        {data, SubId, DSStream, SubHandle, DSReply} ->
            case Subs of
                #{SubId := StreamState} ->
                    handle_ds_info(HState, StreamState, SubId, DSStream, SubHandle, DSReply);
                _ ->
                    {ok, HState}
            end
    end;
handle_info(
    #h{state = #state{subs = Subs} = State0, ds_client = DSClient0} = HState0, #complete_stream{
        sub_id = SubId, slab = Slab, ds_stream = DSStream
    }
) ->
    case Subs of
        #{SubId := _} ->
            {DSClient, State} = emqx_ds_client:complete_stream(
                DSClient0, SubId, Slab, DSStream, State0
            ),
            {ok, HState0#h{ds_client = DSClient, state = State}};
        _ ->
            {ok, HState0}
    end.

handle_ds_info(
    #h{state = #state{status = Status}} = HState, StreamState0, SubId, _DSStream, SubHandle, DSReply
) ->
    case DSReply of
        #ds_sub_reply{payload = {ok, end_of_stream}, ref = SubRef} ->
            case Status of
                #status_blocked{} ->
                    StreamStatus = #stream_status_blocked{
                        unblock_fn = fun(HSt) ->
                            complete_ds_stream(HSt, SubRef)
                        end
                    },
                    StreamState = StreamState0#stream_state{status = StreamStatus},
                    {ok, update_stream_state(HState, SubId, StreamState)};
                #status_unblocked{} ->
                    {ok, complete_ds_stream(HState, SubRef)}
            end;
        #ds_sub_reply{payload = {ok, _It, TTVs}, seqno = SeqNo, size = _Size} ->
            case Status of
                #status_blocked{} ->
                    StreamStatus = #stream_status_blocked{
                        unblock_fn = fun(HSt) ->
                            ok = suback(HSt, SubId, SubHandle, SeqNo),
                            HSt
                        end
                    },
                    StreamState = StreamState0#stream_state{status = StreamStatus},
                    {ok, update_stream_state(HState, SubId, StreamState),
                        to_messages(StreamState, TTVs)};
                #status_unblocked{} ->
                    ok = suback(HState, SubId, SubHandle, SeqNo),
                    {ok, HState, to_messages(StreamState0, TTVs)}
            end
    end.

update_blocking_status(#h{state = #state{status = Status} = State} = HState, DesiredCount) ->
    case {Status, DesiredCount} of
        {#status_unblocked{}, 0} ->
            HState#h{state = State#state{status = #status_blocked{}}};
        {#status_unblocked{}, N} when N > 0 ->
            HState;
        {#status_blocked{}, 0} ->
            HState;
        {#status_blocked{}, N} when N > 0 ->
            unblock_streams(HState)
    end.

unblock_streams(#h{state = #state{subs = Subs}} = HState) ->
    maps:fold(
        fun
            (_SubId, #stream_state{status = #stream_status_unblocked{}}, HStateAcc) ->
                HStateAcc;
            (
                SubId,
                #stream_state{status = #stream_status_blocked{unblock_fn = UnblockFn}} =
                    StreamState0,
                HStateAcc0
            ) ->
                HStateAcc = UnblockFn(HStateAcc0),
                StreamState = StreamState0#stream_state{status = #stream_status_unblocked{}},
                update_stream_state(HStateAcc, SubId, StreamState)
        end,
        HState,
        Subs
    ).

to_messages(#stream_state{filter = Filter}, TTVs) ->
    lists:filtermap(
        fun({_Topic, Time, Payload}) ->
            case Filter(Time) of
                true -> {true, emqx_streams_message_db:decode_message(Payload)};
                false -> false
            end
        end,
        TTVs
    ).

complete_ds_stream(#h{state = State0, ds_client = DSClient0} = HState, SubRef) ->
    {DSClient, State} = emqx_ds_client:complete_stream(DSClient0, SubRef, State0),
    HState#h{ds_client = DSClient, state = State}.

suback(#h{state = #state{subs = Subs}}, SubId, SubHandle, SeqNo) ->
    case Subs of
        #{SubId := #stream_state{stream = Stream}} ->
            ok = emqx_streams_message_db:suback(Stream, SubHandle, SeqNo);
        _ ->
            ok
    end.

parse_timestamp(<<"earliest">>) ->
    {ok, earliest};
parse_timestamp(<<"latest">>) ->
    {ok, latest};
parse_timestamp(OffsetBin) ->
    maybe
        [GenerationBin, TimestampBin] ?= binary:split(OffsetBin, <<"-">>, [global]),
        {Generation, <<>>} ?= string:to_integer(GenerationBin),
        {Timestamp, <<>>} ?= string:to_integer(TimestampBin),
        {ok, {Generation, Timestamp}}
    else
        _ ->
            {error, invalid_offset}
    end.

init_handler(undefined, #{send_after := SendAfterFn, send := SendFn} = _Ctx) ->
    State = #state{
        send_fn = SendFn,
        send_after_fn = SendAfterFn,
        status = #status_unblocked{},
        subs = #{},
        topic_filters = #{}
    },
    DSClient = emqx_streams_message_db:create_client(?MODULE),
    #h{state = State, ds_client = DSClient};
init_handler(#h{} = Handler, _Ctx) ->
    Handler.

topic_present(undefined, _TopicFilter) ->
    false;
topic_present(#h{state = #state{topic_filters = TopicFilters}}, FullTopicFilter) ->
    maps:is_key(FullTopicFilter, TopicFilters).

update_stream_state(#h{state = State} = HState, SubId, StreamState) ->
    HState#h{state = update_stream_state(State, SubId, StreamState)};
update_stream_state(#state{subs = Subs} = State, SubId, StreamState) ->
    State#state{subs = Subs#{SubId => StreamState}}.
