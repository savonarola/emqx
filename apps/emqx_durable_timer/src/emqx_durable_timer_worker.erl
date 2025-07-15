%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_worker).
-moduledoc """
A process responsible for execution of the timers.

## State machine
```

         .--(type = active)--> active
        /
 [start]                      ,-->(leader, Kind)
        \                    /
         `--(candidate, Kind)
                  ^          \
                  |           `-->(backup, Kind)
                   \                   /
                    `---leader down---'
```

### Active worker's timeline

```                                         pending add apply_after
     pending gc trans                         ..................
      ..............     wake up timers      :   ..........     :
     :              v   v     v       v      :  :          v    v
-----=----=---=--=--=---=--=--=---=-=-=------------------------------> t
     |              |                 |                    |
 del_up_to  fully_replayed_ts   next_wake_up    active_safe_replay_pos
```

- `del_up_to` tracks the last garbage collection.
   It's used to avoid running GC unnecessarily.

- `fully_replayed_ts` is used to

Invariants:

- `del_up_to` =< `fully_replayed_ts`
- `fully_replayed_ts` =< `next_wake_up`
- `next_wake_up` < `safe_active_replay_pos`
- All pending transactions that add new timers insert entries with timestamp >= `active_safe_replay_pos`

""".

-behavior(gen_statem).

%% API:
-export([start_link/4, apply_after/5]).

%% behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([ls/0]).

-export_type([kind/0]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type kind() :: active | {closed, started | dead_hand}.

-define(name(WORKER_KIND, TYPE, EPOCH, SHARD), {n, l, {?MODULE, WORKER_KIND, TYPE, EPOCH, SHARD}}).
-define(via(WORKER_KIND, TYPE, EPOCH, SHARD), {via, gproc, ?name(WORKER_KIND, TYPE, EPOCH, SHARD)}).

-record(call_apply_after, {
    k :: emqx_durable_timer:key(),
    v :: emqx_durable_timer:value(),
    t :: integer()
}).
%% Note: here t is local time:
-record(cast_wake_up, {t :: integer()}).
-define(elect_leader, elect_leader).
-define(replay_complete, replay_complete).

%% States:
-define(s_active, active).
-define(s_candidate(KIND), {candidate, KIND}).
-define(s_backup(KIND, REF), {backup, KIND, REF}).
-define(s_leader(KIND), {leader, KIND}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(
    kind(), emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_ds:shard()
) ->
    {ok, pid()}.
start_link(WorkerKind, Type, Epoch, Shard) ->
    gen_statem:start_link(
        ?via(WorkerKind, Type, Epoch, Shard), ?MODULE, [WorkerKind, Type, Epoch, Shard], []
    ).

-spec apply_after(
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value(),
    emqx_durable_timer:delay()
) -> ok.
apply_after(Type, Epoch, Key, Val, NotEarlierThan) when
    ?is_valid_timer(Type, Key, Val, NotEarlierThan) andalso is_binary(Epoch)
->
    Shard = emqx_ds:shard_of(?DB_GLOB, Key),
    gen_statem:call(
        ?via(active, Type, Epoch, Shard),
        #call_apply_after{k = Key, v = Val, t = NotEarlierThan},
        infinity
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    type :: emqx_durable_timer:type(),
    cbm :: module(),
    epoch :: emqx_durable_timer:epoch(),
    shard :: emqx_ds:shard(),
    pending_tab :: ets:tid() | undefined,
    %% Iterator pointing at the beginning of un-replayed timers:
    replay_pos :: emqx_ds:iterator() | undefined,
    %% Remaining entries from the last replay (relevent for closed)
    tail = [] :: [emqx_ds:ttv()],
    %% Reference to the pending transaction that async-ly cleans up
    %% the replayed data:
    pending_del_tx :: reference() | undefined,
    topic :: emqx_ds:topic(),
    %% This value should be added to the timestamp of the timer TTV to
    %% convert it to the local time:
    time_delta :: integer(),
    %% Timeline:
    del_up_to = 0 :: integer(),
    fully_replayed_ts = 0 :: integer(),
    next_wakeup = -1 :: integer()
}).

-type data() :: #s{}.

callback_mode() -> [handle_event_function, state_enter].

init([WorkerKind, Type, Epoch, Shard]) ->
    process_flag(trap_exit, true),
    logger:update_process_metadata(
        #{kind => WorkerKind, type => Type, epoch => Epoch, shard => Shard}
    ),
    CBM = emqx_durable_timer:get_cbm(Type),
    ?tp(?tp_worker_started, #{}),
    case WorkerKind of
        active ->
            emqx_durable_timer_dl:insert_epoch_marker(Shard, Epoch),
            D = #s{
                type = Type,
                cbm = CBM,
                epoch = Epoch,
                shard = Shard,
                pending_tab = addq_new(),
                time_delta = 0,
                topic = emqx_durable_timer_dl:started_topic(Type, Epoch, '+')
            },
            {ok, ?s_active, D};
        {closed, Closed} ->
            pg:join(?workers_pg, {Closed, Type, Epoch, Shard}, self()),
            case Closed of
                started ->
                    Topic = emqx_durable_timer_dl:started_topic(Type, Epoch, '+'),
                    %% TODO: try to account for clock skew?
                    Delta = 0;
                dead_hand ->
                    {ok, LastHeartbeat} = emqx_durable_timer_dl:last_heartbeat(Epoch),
                    %% Upper margin for error:
                    Delta = LastHeartbeat + emqx_durable_timer:cfg_heartbeat_interval(),
                    Topic = emqx_durable_timer_dl:dead_hand_topic(Type, Epoch, '+')
            end,
            D = #s{
                type = Type,
                cbm = CBM,
                epoch = Epoch,
                shard = Shard,
                time_delta = Delta,
                topic = Topic
            },
            {ok, ?s_candidate(Closed), D}
    end.

%% Active:
handle_event(enter, _, ?s_active, _) ->
    keep_state_and_data;
handle_event({call, From}, #call_apply_after{k = Key, v = Value, t = NotEarlierThan}, ?s_active, S) ->
    handle_apply_after(From, Key, Value, NotEarlierThan, S);
%% Candidate:
handle_event(enter, _, ?s_candidate(_Kind), _Data) ->
    {keep_state_and_data, {state_timeout, 0, ?elect_leader}};
handle_event(state_timeout, ?elect_leader, ?s_candidate(Kind), Data) ->
    handle_election(Kind, Data);
handle_event(enter, _, State = ?s_leader(_Kind), Data) ->
    init_leader(State, Data);
handle_event(info, ?replay_complete, ?s_candidate(_), _Data) ->
    {stop, normal};
%% Backup:
handle_event(enter, _, ?s_backup(_, _), _) ->
    keep_state_and_data;
handle_event(info, {'DOWN', Ref, _, _, Reason}, ?s_backup(Kind, Ref), Data) ->
    handle_leader_down(Kind, Data, Reason);
handle_event(info, ?replay_complete, ?s_backup(_, _), _Data) ->
    {stop, normal};
%% Common:
handle_event(_ET, ?ds_tx_commit_reply(Ref, Reply), _State, Data) ->
    handle_ds_reply(Ref, Reply, Data);
handle_event(info, #cast_wake_up{t = Treached}, State, Data) ->
    handle_wake_up(State, Data, Treached);
handle_event(ET, Event, State, Data) ->
    ?tp(error, ?tp_unknown_event, #{m => ?MODULE, ET => Event, state => State, data => Data}),
    keep_state_and_data.

terminate(_Reason, _State, _D) ->
    ?tp(?tp_terminate, #{m => ?MODULE, reason => _Reason, s => _State}),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%% @doc Display all active workers:
-spec ls() -> [{emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_ds:shard()}].
ls() ->
    MS = {{?name('$1', '$2', '$3', '$4'), '_', '_'}, [], [{{'$1', '$2', '$3', '$4'}}]},
    gproc:select({local, names}, [MS]).

%%================================================================================
%% Internal functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Leader election
%%--------------------------------------------------------------------------------

handle_leader_down(_Kind, _Data, _Reason) ->
    %% FIXME
    keep_state_and_data.

handle_election(Kind, Data = #s{type = T, epoch = E, shard = S}) ->
    Name = ?name(Kind, T, E, S),
    case global:whereis_name(Name) of
        Leader when is_pid(Leader) ->
            MRef = monitor(process, Leader),
            {next_state, ?s_backup(Kind, MRef), Data};
        undefined ->
            %% FIXME: do something smarter
            case global:register_name(Name, self()) of
                yes ->
                    {next_state, ?s_leader(Kind), Data};
                no ->
                    {next_state, ?s_candidate(Kind), Data,
                        {state_timeout, rand:uniform(1000), ?elect_leader}}
            end
    end.

init_leader(State, Data0) ->
    case get_iterator(Data0) of
        {ok, It} ->
            Data = Data0#s{
                replay_pos = It
            },
            self() ! #cast_wake_up{t = emqx_durable_timer:now_ms()},
            {keep_state, Data};
        undefined ->
            complete_replay(State, Data0, 0)
    end.

%%--------------------------------------------------------------------------------
%% Replay
%%--------------------------------------------------------------------------------

handle_wake_up(
    State, Data0 = #s{time_delta = Delta, fully_replayed_ts = FullyReplayedTS0}, Treached
) ->
    Bound = Treached + 1,
    DSBatchSize =
        case State of
            ?s_active ->
                {time, Bound + Delta, emqx_durable_timer:cfg_batch_size()};
            _ ->
                emqx_durable_timer:cfg_batch_size()
        end,
    Result = ?tp_span(
        ?tp_replay,
        #{bound => Bound, fully_replayed_ts => FullyReplayedTS0},
        replay_timers(State, Data0, Bound, DSBatchSize)
    ),
    case Result of
        {end_of_stream, FullyReplayedTS} ->
            ?s_leader(_) = State,
            complete_replay(State, Data0, FullyReplayedTS);
        {ok, Data} ->
            clean_replayed(schedule_next_wake_up(State, Data));
        {retry, Data, Reason} ->
            ?tp(warning, ?tp_replay_failed, #{
                from => Data0#s.replay_pos, to => Bound, reason => Reason
            }),
            erlang:send_after(
                emqx_durable_timer:cfg_replay_retry_interval(),
                self(),
                #cast_wake_up{t = Treached}
            ),
            {keep_state, Data}
    end.

schedule_next_wake_up(?s_active, Data) ->
    Data;
schedule_next_wake_up(?s_leader(_), Data = #s{tail = Tail, time_delta = Delta}) ->
    [{_, T, _} | _] = Tail,
    After = max(0, (T + Delta) - emqx_durable_timer:now_ms()),
    erlang:send_after(
        After,
        self(),
        #cast_wake_up{t = T}
    ),
    Data.

complete_replay(
    ?s_leader(Closed), #s{shard = Shard, type = Type, topic = Topic, epoch = Epoch}, FullyReplayedTS
) ->
    emqx_durable_timer_dl:clean_replayed(Shard, Topic, FullyReplayedTS),
    lists:foreach(
        fun(Peer) ->
            Peer ! ?replay_complete
        end,
        pg:get_members(?workers_pg, {Closed, Type, Epoch, Shard})
    ),
    _ = emqx_durable_timer_dl:delete_epoch_if_empty(Epoch),
    {stop, normal}.

replay_timers(
    State,
    Data = #s{fully_replayed_ts = FullyReplayedTS0, replay_pos = It0, tail = Tail0},
    Bound,
    DSBatchSize
) ->
    case do_replay_timers(Data, It0, Bound, DSBatchSize, FullyReplayedTS0, Tail0) of
        {ok, FullyReplayedTS, _, []} when State =/= ?s_active ->
            {end_of_stream, FullyReplayedTS};
        {ok, FullyReplayedTS, It, Tail} ->
            {ok, Data#s{replay_pos = It, fully_replayed_ts = FullyReplayedTS, tail = Tail}};
        {retry, FullyReplayedTS1, It1, Reason} ->
            {retry, Data#s{replay_pos = It1, fully_replayed_ts = FullyReplayedTS1, tail = []},
                Reason}
    end.

do_replay_timers(D, It0, Bound, DSBatchSize, FullyReplayedTS, []) ->
    case emqx_ds:next(?DB_GLOB, It0, DSBatchSize) of
        {ok, It, []} ->
            {ok, FullyReplayedTS, It, []};
        {ok, It, Batch} ->
            do_replay_timers(D, It, Bound, DSBatchSize, FullyReplayedTS, Batch);
        ?err_rec(Reason) ->
            {retry, FullyReplayedTS, It0, Reason}
    end;
do_replay_timers(D, It, Bound, DSBatchSize, FullyReplayedTS0, Batch) ->
    {FullyReplayedTS, Tail} = apply_timers_from_batch(D, Bound, FullyReplayedTS0, Batch),
    case Tail of
        [] ->
            do_replay_timers(D, It, Bound, DSBatchSize, FullyReplayedTS, []);
        _ ->
            {ok, FullyReplayedTS, It, Tail}
    end.

apply_timers_from_batch(
    D = #s{type = Type, cbm = CBM, time_delta = Delta}, Bound, FullyReplayedTS, L
) ->
    case L of
        [] ->
            {FullyReplayedTS, []};
        [{[_Root, _Type, _Epoch, Key], Time, Val} | Rest] when Time + Delta =< Bound ->
            handle_timeout(Type, CBM, Time, Key, Val),
            apply_timers_from_batch(D, Bound, Time, Rest);
        _ ->
            {FullyReplayedTS, L}
    end.

-spec clean_replayed(data()) -> data().
clean_replayed(
    D = #s{
        pending_del_tx = Pending,
        fully_replayed_ts = FullyReplayed,
        del_up_to = DelUpTo
    }
) when is_reference(Pending); FullyReplayed =:= DelUpTo ->
    %% Clean up is already in progress or there's nothing to clean:
    {keep_state, D};
clean_replayed(
    D = #s{
        shard = Shard,
        topic = Topic,
        pending_del_tx = undefined,
        fully_replayed_ts = DelUpTo
    }
) ->
    Ref = emqx_durable_timer_dl:clean_replayed_async(Shard, Topic, DelUpTo),
    {keep_state, D#s{pending_del_tx = Ref, del_up_to = DelUpTo}}.

-spec on_clean_complete(reference(), _, data()) -> data().
on_clean_complete(Ref, Reply, Data) ->
    case emqx_ds:tx_commit_outcome(?DB_GLOB, Ref, Reply) of
        {ok, _} ->
            ok;
        Other ->
            ?tp(info, "Failed to clean up expired timers", #{reason => Other})
    end,
    clean_replayed(Data#s{pending_del_tx = undefined}).

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

-doc """
Get starting position for the replay.
Either create a new iterator at the very beginning of the timer stream,
or return the existing iterator that points at the beginning of un-replayed timers.
""".
get_iterator(#s{shard = Shard, topic = Topic}) ->
    %% This is the first start of the worker. Start from the beginning:
    case emqx_ds:get_streams(?DB_GLOB, Topic, 0, #{shard => Shard}) of
        {[{_, Stream}], []} ->
            emqx_ds:make_iterator(?DB_GLOB, Stream, Topic, 0);
        {[], []} ->
            undefined;
        {_, Errors} ->
            error({failed_to_make_iterator, Errors})
    end.

%%--------------------------------------------------------------------------------
%% Adding a new timer to the active queue
%%--------------------------------------------------------------------------------

%% Active replayer should be careful to not jump over timers that are
%% still being added via pending transactions. Consider the following
%% race condition:
%%
%% 1. Replay iterator is at t0. Next known timer fires at t2. At t1
%% we start a transaction tx to add a new timer at t1':
%%
%%                   tx
%%                 ......
%%                :      v
%% ----------|----------------|-----------------------> t
%%          t0   t1     t1'   t2
%%           ^
%%
%% 2. While the transaction is pending, t2 timer fires and replay
%% iterator moves to t2:
%%
%%                  tx
%%                ......
%%               :      v
%% ----------|----------------|----------------------> t
%%          t0  t1     t1'    t2
%%           `- replay batch -^
%%
%%
%% 3. Transaction tx finally commits, but the iterator is already at
%% t2:
%%
%%                  tx
%%                ______
%%               |      v
%% ----------|----------|-----|----------------------> t
%%          t0  t1     t1'    t2
%%                            ^
%%
%% ...We missed the event at t1'.
%%
%% This function prevents iterator from advancing past the earliest
%% timestamp that is being added via a pending transaction.
active_safe_replay_pos(#s{pending_tab = Tab, fully_replayed_ts = FullyReplayed}, T) ->
    %% 1. We should not attempt to move `fully_replayed_ts' backwards:
    max(
        FullyReplayed,
        case addq_first(Tab) of
            undefined ->
                T;
            MinPending when is_integer(MinPending) ->
                %% 2. We cannot wake up earlier than `MinPending':
                max(MinPending, T)
        end
    ).

active_ensure_iterator(Data = #s{replay_pos = undefined}) ->
    {ok, It} = get_iterator(Data),
    Data#s{
        replay_pos = It
    };
active_ensure_iterator(Data) ->
    Data.

handle_apply_after(From, Key, Val, NotEarlierThan, #s{
    type = Type, epoch = Epoch, pending_tab = Tab
}) ->
    %% Start a transaction that inserts the timer to the started queue for the epoch.
    %%
    %% Invarinat: Time > replay_bound(..)
    Time = max(emqx_durable_timer:now_ms() + 1, NotEarlierThan),
    Ref = emqx_durable_timer_dl:insert_started_async(Type, Epoch, Key, Val, Time),
    ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_begin, #{
        key => Key, val => Val, time => Time, epoch => Epoch, ref => Ref
    }),
    addq_push(Ref, Time, From, Tab),
    keep_state_and_data.

handle_ds_reply(Ref, DSReply, D = #s{pending_del_tx = Ref}) ->
    on_clean_complete(Ref, DSReply, D);
handle_ds_reply(Ref, DSReply, D = #s{pending_tab = Tab}) ->
    case addq_pop(Ref, Tab) of
        {Time, From} ->
            Result =
                case emqx_ds:tx_commit_outcome(?DB_GLOB, Ref, DSReply) of
                    {ok, _} ->
                        ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_ok, #{ref => Ref}),
                        ok;
                    Err ->
                        ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_fail, #{
                            ref => Ref, reason => Err
                        }),
                        Err
                end,
            active_schedule_wake_up(Time, active_ensure_iterator(D), {reply, From, Result});
        undefined ->
            ?tp(error, ?tp_unknown_event, #{m => ?MODULE, event => DSReply}),
            keep_state_and_data
    end.

active_schedule_wake_up(Time, D0 = #s{next_wakeup = NextWakeUp}, Effects) ->
    D =
        case active_safe_replay_pos(D0, Time) of
            TMax when TMax > NextWakeUp ->
                %% New maximum wait time reached:
                Delay = max(0, TMax - emqx_durable_timer:now_ms()),
                erlang:send_after(Delay, self(), #cast_wake_up{t = TMax}),
                D0#s{next_wakeup = TMax};
            _ ->
                %% Wake up timer already exists
                D0
        end,
    {keep_state, D, Effects}.

%%--------------------------------------------------------------------------------
%% Pending transaction queue
%%--------------------------------------------------------------------------------

addq_new() ->
    ets:new(pending_trans, [private, ordered_set, {keypos, 1}]).

addq_push(Ref, Time, From, Tab) ->
    ets:insert(Tab, {{Time, Ref}}),
    ets:insert(Tab, {{ref, Ref}, Time, From}).

addq_first(Tab) ->
    case ets:first(Tab) of
        {Time, _Ref} ->
            ?tp(foo, #{fst => Time, ref => _Ref}),
            Time;
        '$end_of_table' ->
            undefined
    end.

addq_pop(Ref, Tab) ->
    case ets:take(Tab, {ref, Ref}) of
        [{_, Time, From}] ->
            ets:delete(Tab, {Time, Ref}),
            {Time, From};
        [] ->
            undefined
    end.

%%--------------------------------------------------------------------------------
%% Misc
%%--------------------------------------------------------------------------------
