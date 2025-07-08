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

-export_type([type/0]).

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
-record(cast_wake_up, {t :: integer()}).

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

-record(s, {
    type :: emqx_durable_timer:type(),
    cbm :: module(),
    epoch :: emqx_durable_timer:epoch(),
    shard :: emqx_ds:shard(),
    pending_tab :: ets:tid(),
    %% Iterator pointing at the beginning of un-replayed timers:
    replay_pos :: emqx_ds:iterator() | undefined,
    next_wakeup = -1 :: integer()
}).

callback_mode() -> [handle_event_function, state_enter].

init([Type, CBM, Epoch, Shard, active]) ->
    process_flag(trap_exit, true),
    logger:update_process_metadata(
        #{worker_type => active, epoch => Epoch, shard => Shard}
    ),
    S = #s{
        type = Type,
        cbm = CBM,
        epoch = Epoch,
        shard = Shard,
        pending_tab = addq_new(),
        replay_pos = undefined
    },
    {ok, ?s_active, S}.

%% Active:
handle_event(enter, _, ?s_active, _) ->
    keep_state_and_data;
handle_event({call, From}, #call_apply_after{k = Key, v = Value, t = NotEarlierThan}, ?s_active, S) ->
    handle_apply_after(From, Key, Value, NotEarlierThan, S);
handle_event(_ET, ?ds_tx_commit_reply(Ref, Reply), ?s_active, Data) ->
    handle_ds_reply(Ref, Reply, Data);
handle_event(info, #cast_wake_up{t = Treached}, ?s_active, Data) ->
    replay_active(Data, Treached);
%% Common:
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

replay_active(S, Treached) ->
    Bound = active_replay_bound(S, Treached),
    ?tp_ignore_side_effects_in_prod(?tp_replay_start, #{start => S#s.replay_pos, bound => Bound}),
    maybe
        {ok, It0} ?= get_iterator(?s_active, S),
        {ok, It} ?= do_replay_active(S, It0, Bound),
        {keep_state, S#s{replay_pos = It}}
    else
        {retry, It1, Reason} ->
            ?tp(warning, ?tp_replay_failed, #{
                from => S#s.replay_pos, to => It1, reason => Reason
            }),
            erlang:send_after(
                emqx_durable_timer:cfg_replay_retry_interval(),
                self(),
                #cast_wake_up{t = S#s.next_wakeup}
            ),
            {keep_state, S#s{replay_pos = It1}}
    end.

do_replay_active(S = #s{type = Type, cbm = CBM}, It0, Bound) ->
    BS = 100,
    case emqx_ds:next(?DB_GLOB, It0, {time, Bound, BS}) of
        {ok, It, Batch} ->
            lists:foreach(
                fun({[_Root, _Type, _Epoch, Key], Time, Val}) ->
                    %% %% Assert:
                    %% _ = Time < Bound orelse error({Time, '<', Bound}),
                    handle_timeout(Type, CBM, Time, Key, Val)
                end,
                Batch
            ),
            case length(Batch) =:= BS of
                true ->
                    do_replay_active(S, It, Bound);
                false ->
                    {ok, It}
            end;
        ?err_rec(Reason) ->
            {retry, It0, Reason}
    end.

-doc """
Get starting position for the replay.
Either create a new iterator at the very beginning of the timer stream,
or return the existing iterator that points at the beginning of un-replayed timers.
""".
get_iterator(State, #s{replay_pos = undefined, shard = Shard, type = Type, epoch = Epoch}) ->
    %% This is the first start of the worker. Start from the beginning:
    maybe
        TopicFilter =
            case State of
                ?s_active ->
                    started_topic(Type, Epoch, '+')
            end,
        {[{_, Stream}], []} ?= emqx_ds:get_streams(?DB_GLOB, TopicFilter, 0, #{shard => Shard}),
        {ok, It} ?= emqx_ds:make_iterator(?DB_GLOB, Stream, TopicFilter, 0)
    else
        Reason ->
            {retry, undefined, Reason}
    end;
get_iterator(_State, #s{replay_pos = ReplayPos}) ->
    %% Iterator already exists:
    {ok, ReplayPos}.

%% Active replayer should be careful to not jump over timers that are
%% still being added via pending transactions. Consider the following
%% race condition:
%%
%% 1. Replay iterator is at t0. Next known timer fires at t2. At t1
%% we start a transaction tx to add a new timer at t1':
%%
%%                   tx
%%                ........
%%                .      v
%% ----------|----------------|-----------------------> t
%%          t0   t1     t1'   t2
%%           ^
%%
%% 2. While the transaction is pending, t2 timer fires and replay
%% iterator moves to t2:
%%
%%                  tx
%%               ........
%%               .      v
%% ----------|----------------|----------------------> t
%%          t0  t1     t1'    t2
%%           `- replay batch -^
%%
%%
%% 3. Transaction tx finally commits, but the iterator is already at
%% t2:
%%
%%                  tx
%%               +------+
%%               |      v
%% ----------|----------|-----|----------------------> t
%%          t0  t1     t1'    t2
%%                            ^
%%
%% ...We missed the event at t1'.
%%
%% This function prevents iterator from advancing past the earliest
%% timestamp that is being added via a pending transaction.
active_replay_bound(#s{pending_tab = Tab}, Treached) ->
    case addq_first(Tab) of
        Bound when is_integer(Bound), Bound < Treached ->
            %% There are pending transactions preventing replay up to
            %% the current time. Retry replay later:
            erlang:send_after(
                emqx_durable_timer:cfg_transaction_timeout(),
                self(),
                #cast_wake_up{t = Treached}
            ),
            Bound;
        _ ->
            Treached
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
handle_ds_reply(Ref, DSReply, S = #s{pending_tab = Tab}) ->
    case addq_pop(Ref, Tab) of
        {Time, From} ->
            case emqx_ds:tx_commit_outcome(?DB_GLOB, Ref, DSReply) of
                {ok, _Serial} ->
                    ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_ok, #{ref => Ref}),
                    handle_add_timer(Time, From, S);
                Reply ->
                    ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_fail, #{
                        ref => Ref, reason => Reply
                    }),
                    {keep_state_and_data, [{reply, From, Reply}]}
            end;
        undefined ->
            ?tp(error, ?tp_unknown_event, #{m => ?MODULE, event => DSReply}),
            keep_state_and_data
    end.

handle_add_timer(Time, From, #s{next_wakeup = NextWakeUp}) when Time =< NextWakeUp ->
    %% No need to set up a new wake up timer. Just report success to
    %% the client:
    {keep_state_and_data, {reply, From, ok}};
handle_add_timer(Time, From, S) ->
    %% This is the new maximum:
    Delay = max(0, Time - emqx_durable_timer:now_ms()),
    erlang:send_after(Delay, self(), #cast_wake_up{t = Time}),
    {keep_state, S#s{next_wakeup = Time}, {reply, From, ok}}.

-spec handle_timeout(
    emqx_durable_timer:type(),
    module(),
    integer(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value()
) -> ok.
handle_timeout(Type, CBM, Time, Key, Value) ->
    ?tp(debug, ?tp_fire, #{type => Type, key => Key, val => Value, t => Time}),
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
            ?tp(foo, #{fst => Time, ref => _Ref}),
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
