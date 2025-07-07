%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_worker).
-moduledoc """
A process that is responsible for execution of the timers.

## State machine
```

         .--(type = active)--> active
        /
 [start]
        \
         `--(leader = down)--> {candidate, Type} --> ...

```
""".

-behavior(gen_statem).

%% API:
-export([start_link/5, apply_after/5, dead_hand/5, cancel/3]).

%% behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([dead_hand_topic/3, started_topic/3]).

-export_type([]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type type() :: active | dead_hand | active_replay.

-define(via(TYPE, EPOCH, SHARD), {via, gproc, {n, l, {?MODULE, TYPE, EPOCH, SHARD}}}).

-record(pending_trans, {key, from}).

-record(call_apply_after, {
    k :: emqx_durable_timer:key(),
    v :: emqx_durable_timer:value(),
    t :: integer()
}).

%% States:
-define(s_active, active).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(
    emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_ds:shard(), module(), type()
) ->
    {ok, pid()}.
start_link(Type, Epoch, Shard, CBM, WorkerType) ->
    gen_statem:start_link(
        ?via(Type, Epoch, Shard), ?MODULE, [Type, CBM, Epoch, Shard, WorkerType], []
    ).

-spec apply_after(
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value(),
    emqx_durable_timer:delay()
) -> ok.
apply_after(Type, Epoch, Key, Val, NotEarlierThan) ->
    Shard = emqx_ds:shard_of(?DB_GLOB, Key),
    gen_statem:call(
        ?via(Type, Epoch, Shard),
        #call_apply_after{k = Key, v = Val, t = NotEarlierThan},
        infinity
    ).

dead_hand(Type, Epoch, Key, Value, Delay) ->
    Result = emqx_ds:trans(
        trans_opts(Key, #{}),
        fun() ->
            tx_del_dead_hand(Type, Key),
            tx_del_started(Type, Key),
            emqx_ds:tx_write({
                dead_hand_topic(Type, Epoch, Key),
                Delay,
                Value
            })
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

-spec cancel(emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_durable_timer:key()) -> ok.
cancel(Type, _Epoch, Key) ->
    Result = emqx_ds:trans(
        trans_opts(Key, #{}),
        fun() ->
            tx_del_dead_hand(Type, Key),
            tx_del_started(Type, Key)
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%%
%% --offset-->[--tail--]
%% [rrrrrrrrrr|uuuuuuuu]
%% ^                 ^
%% it_begin       it_end
%%
%% Legend:
%% r: replayed timers
%% u: not replayed yet
-record(replay_pos, {
    it_begin :: emqx_ds:iterator(),
    it_end :: emqx_ds:iterator() | undefined,
    offset :: non_neg_integer(),
    tail :: list()
}).

-record(replay_pos_checkpoint, {
    it :: emqx_ds:iterator(),
    offset :: non_neg_integer()
}).

-record(s, {
    type :: emqx_durable_timer:type(),
    cbm :: module(),
    epoch :: emqx_durable_timer:epoch(),
    shard :: emqx_ds:shard(),
    pending_tab :: ets:tid(),
    replay_pos :: #replay_pos{} | #replay_pos_checkpoint{} | undefined
}).

callback_mode() -> [handle_event_function, state_enter].

init([Type, CBM, Epoch, Shard, active]) ->
    process_flag(trap_exit, true),
    S = #s{
        type = Type,
        cbm = CBM,
        epoch = Epoch,
        shard = Shard,
        pending_tab = addq_new(),
        replay_pos = 0
    },
    {ok, ?s_active, S}.

%% Active:
handle_event(enter, _, ?s_active, _) ->
    keep_state_and_data;
handle_event({call, From}, #call_apply_after{k = Key, v = Value, t = NotEarlierThan}, ?s_active, S) ->
    handle_apply_after(From, Key, Value, NotEarlierThan, S);
handle_event(_ET, ?ds_tx_commit_reply(Ref, Reply), ?s_active, Data) ->
    handle_ds_reply(Ref, Reply, Data);
%% Common:
handle_event(info, wake_up, State, Data) ->
    handle_replay(State, Data);
handle_event(ET, Event, State, Data) ->
    ?tp(error, ?tp_unknown_event, #{m => ?MODULE, ET => Event, state => State, data => Data}),
    keep_state_and_data.

terminate(_Reason, _State, _D) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

handle_replay(State, S = #s{replay_pos = ReplayPos0}) ->
    Bound = replay_bound(State, S),
    ?tp_ignore_side_effects_in_prod(?tp_replay_start, #{start => ReplayPos0, bound => Bound}),
    maybe
        {ok, ReplayPos1} ?= get_iterator(State, S),
        {ok, ReplayPos} ?= replay_interval(State, S, ReplayPos1, Bound),
        {keep_state, S#s{replay_pos = ReplayPos}}
    else
        end_of_stream ->
            %% FIXME:
            error(fixme);
        {retry, ReplayPos, Reason} ->
            ?tp(warning, ?tp_replay_failed, #{from => ReplayPos0, to => ReplayPos, reason => Reason}),
            timer:send_after(emqx_durable_timer:cfg_replay_retry_interval(), self(), wake_up),
            {keep_state, S#s{replay_pos = ReplayPos}}
    end.

get_iterator(State, #s{replay_pos = undefined, shard = Shard, type = Type, epoch = Epoch}) ->
    %% This is the first start of the worker. Start from the beginning:
    maybe
        TopicFilter =
            case State of
                ?s_active ->
                    started_topic(Type, Epoch, '+')
            end,
        {[{_, Stream}], []} ?= emqx_ds:get_streams(?DB_GLOB, TopicFilter, 0, #{shard => Shard}),
        {ok, It} ?= emqx_ds:make_iterator(?DB_GLOB, Stream, TopicFilter, 0),
        {ok, #replay_pos{
            it_begin = It,
            it_end = It,
            offset = 0,
            tail = []
        }}
    else
        Reason ->
            {retry, undefined, Reason}
    end;
get_iterator(_State, #s{replay_pos = P0 = #replay_pos_checkpoint{it = It0, offset = Offset}}) ->
    %% Restarting from the checkpointed position:
    maybe
        %% Skip the replayed parts of the topic by fetching and
        %% discarding a batch of size equal to the offset:
        {ok, It, Batch} ?=
            case Offset of
                0 ->
                    {ok, It0, []};
                _ ->
                    emqx_ds:next(?DB_GLOB, It0, Offset)
            end,
        {length_mismatch, Offset} ?= {length_mismatch, length(Batch)},
        Pos = #replay_pos{it_begin = It, offset = 0, it_end = It, tail = []},
        {ok, Pos}
    else
        Reason ->
            {retry, P0, Reason}
    end;
get_iterator(_State, #s{replay_pos = ReplayPos = #replay_pos{}}) ->
    %% Replay is ongoing:
    {ok, ReplayPos}.

replay_interval(?s_active, S, RP, Bound) ->
    %% Active workers should not reuse the tail, since new data can be inserted there:
    replay_loop(S, RP#replay_pos{tail = []}, Bound).

replay_loop(S, P0 = #replay_pos{it = It0, offset = O0, tail = Tl0}, Bound) ->
    case replay_batch(S, Tl0, Bound) of
        more ->
            Result = emqx_ds:next(?DB_GLOB, It0, max(100, O0)),
            ?tp(warning, emqx_durable_timer_replay_iteration, #{
                                                                start => Start, bound => Bound, result => Result
                                                               }),
            case Result of
                {ok, end_of_stream} ->
                    {ok, Start, []};
                {ok, _It, []} ->
                    %% TODO: handle case where data from DS is unavailable due
                    %% to replication lag.
                    %%
                    %% This may manifest as an empty batch with end time is
                    %% less than time of the last known timer fire.
                    {ok, Start, []};
                {ok, It, Batch} ->
                    case replay_batch(S, Start, Bound, Batch) of
                        {more, ReplayEndedAt} ->
                            replay_loop(S, It, ReplayEndedAt, Bound);
                        {ok, _, _} = Ok ->
                            Ok
                    end
            end
    end.

replay_loop(S, It0, Start, Bound) ->
    Result = emqx_ds:next(?DB_GLOB, It0, 100),
    ?tp(warning, emqx_durable_timer_replay_iteration, #{
        start => Start, bound => Bound, result => Result
    }),
    case Result of
        {ok, end_of_stream} ->
            {ok, Start, []};
        {ok, _It, []} ->
            %% TODO: handle case where data from DS is unavailable due
            %% to replication lag.
            %%
            %% This may manifest as an empty batch with end time is
            %% less than time of the last known timer fire.
            {ok, Start, []};
        {ok, It, Batch} ->
            case replay_batch(S, Start, Bound, Batch) of
                {more, ReplayEndedAt} ->
                    replay_loop(S, It, ReplayEndedAt, Bound);
                {ok, _, _} = Ok ->
                    Ok
            end
    end.

replay_batch(_S, _Bound, []) ->
    more;
replay_batch(S, Bound, L = [{_DSKey, Payload} | Rest]) ->
    {[_Root, _Type, _Epoch, Key], Time, Val} = Payload,
    case Time =< Bound of
        false ->
            {ok, L};
        true ->
            handle_timeout(S#s.type, S#s.cbm, Key, Val),
            replay_batch(S, Bound, Rest)
    end.

replay_bound(?s_active, #s{pending_tab = Tab}) ->
    case addq_first(Tab) of
        undefined ->
            emqx_durable_timer:now_ms();
        Bound ->
            Bound
    end.

handle_apply_after(From, Key, Val, NotEarlierThan, #s{
    type = Type, epoch = Epoch, pending_tab = Tab
}) ->
    %% Start a transaction that inserts the timer to the started queue for the epoch.
    %%
    %% Invarinat: Time > replay_bound(..)
    Time = max(emqx_durable_timer:now_ms() + 1, NotEarlierThan),
    {async, Ref, _Ret} =
        emqx_ds:trans(
            trans_opts(Key, #{sync => false}),
            fun() ->
                %% Clear previous data:
                tx_del_dead_hand(Type, Key),
                tx_del_started(Type, Key),
                %% Insert the new data:
                emqx_ds:tx_write({started_topic(Type, Epoch, Key), Time, Val})
            end
        ),
    ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_begin, #{
        key => Key, val => Val, time => Time, epoch => Epoch, ref => Ref
    }),
    addq_push(Ref, Time, From, Tab),
    keep_state_and_data.

%% This function is only used in the active worker. It's called on the
%% transaction reply from DS.
handle_ds_reply(Ref, DSReply, #s{pending_tab = Tab}) ->
    case addq_pop(Ref, Tab) of
        {Time, From} ->
            case emqx_ds:tx_commit_outcome(?DB_GLOB, Ref, DSReply) of
                {ok, _Serial} ->
                    ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_ok, #{ref => Ref}),
                    WakeUpAfter = max(0, Time - emqx_durable_timer:now_ms()),
                    timer:send_after(WakeUpAfter, self(), wake_up),
                    Reply = ok;
                Reply ->
                    ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_fail, #{
                        ref => Ref, reason => Reply
                    }),
                    ok
            end,
            {keep_state_and_data, [{reply, From, Reply}]};
        undefined ->
            ?tp(error, ?tp_unknown_event, #{m => ?MODULE, event => DSReply}),
            keep_state_and_data
    end.

-spec handle_timeout(
    emqx_durable_timer:type(), module(), emqx_durable_timer:key(), emqx_durable_timer:value()
) -> ok.
handle_timeout(Type, CBM, Key, Value) ->
    ?tp(debug, ?tp_fire, #{type => Type, key => Key, val => Value}),
    try
        CBM:handle_durable_timeout(Key, Value),
        ok
    catch
        _:_ ->
            ok
    end.

tx_del_dead_hand(Type, Key) ->
    emqx_ds:tx_del_topic(dead_hand_topic(Type, '+', Key)).

tx_del_started(Type, Key) ->
    emqx_ds:tx_del_topic(started_topic(Type, '+', Key)).

dead_hand_topic(Type, NodeEpochId, Key) ->
    [?top_deadhand, <<Type:?type_bits>>, NodeEpochId, Key].

started_topic(Type, NodeEpochId, Key) ->
    [?top_started, <<Type:?type_bits>>, NodeEpochId, Key].

trans_opts(Key, Other) ->
    Other#{
        db => ?DB_GLOB,
        generation => emqx_durable_timer:generation(?DB_GLOB),
        shard => {auto, Key}
    }.

addq_new() ->
    ets:new(pending_trans, [private, ordered_set, {keypos, 1}]).

addq_push(Ref, Time, From, Tab) ->
    ets:insert(Tab, {{Time, Ref}}),
    ets:insert(Tab, {Ref, Time, From}).

addq_first(Tab) ->
    case ets:first(Tab) of
        {{Time, _Ref}} ->
            Time;
        '$end_of_table' ->
            undefined
    end.

addq_pop(Ref, Tab) ->
    case ets:take(Tab, Ref) of
        [{Ref, Time, From}] ->
            ets:delete(Tab, {Time, Ref}),
            {Time, From};
        [] ->
            undefined
    end.
