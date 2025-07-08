%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer).
-moduledoc """
Low-level API for the durable timers.

Business-level applications should not use this.

## Timer types

Nodes ignore any timers with the types that aren't locally registered.

Callback modules that implement timer types should be backward- and forward-compatible.
If data stored in the timer key or value changes, a new timer type should be created.

## State machine

```
                         (failed heartbeat)                       ,-----.
                             |    |                              |       |
                             |    v                              v       |
 dormant -(register_type)-> isolated -(successful heartbeat)-> normal --<
                               ^                                         |
                               |                                         |
                               `--(failed multple heartbeats in a row)--'
```

## Down detection

Naturally, "dead hand" functionality requires a method for determining when a remote node goes down.
Currently we use a very simple method that relies entirely on heartbeats.
Updating the heartbeat and closing the epoch involve writing data into a DS DB with `builtin_raft` backend,
so we piggyback on the Raft consensus to protect against network splits.
More specifically, a network-isolated node cannot update its own heartbeat or mark others' epochs as closed.

We pay for this simplicity by slow detection time, so this is something that can be improved.

### Local view

Every node periodically writes its local monotonic time to the DS topic associated with the current epoch.

If the node doesn't succeed in updating heartbeat for its current epoch at least once over `missed_heartbeats / 2` milliseconds,
then it considers itself isolated.

When node enters isolated state, it changes its current epoch and continues to try to update the heartbeat for the new epoch.
When this succeeds, it enters normal state.

While the node is in isolated state, all local operations with the timers hang until the node enters normal state.

### Remote view

All nodes monitor heartbeats for every known epoch, except their own.
When the remote fails to update the epoch for `missed_heartbeats` milliseconds,
node tries to close the epoch using `update_epoch`.

If this operation succeeds, the remote is considered down.
If it fails it may be because the local node itself is isolated.

""".

-behavior(gen_statem).

%% API:
-export([start_link/0, register_type/1, dead_hand/4, apply_after/4, cancel/2]).

-ifdef(TEST).
-export([drop_tables/0]).
-endif.

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([
    lts_threshold_cb/2,
    ls_epochs/0, ls_epochs/1,
    last_heartbeat/1,
    now_ms/0,
    epoch/0,
    generation/1,
    cfg_heartbeat_interval/0,
    cfg_missed_heartbeats/0,
    cfg_replay_retry_interval/0,
    cfg_transaction_timeout/0
]).

-export_type([
    type/0,
    key/0,
    value/0,
    epoch/0,
    delay/0
]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(APP, emqx_durable_timer).
-define(epoch_optvar, {?MODULE, epoch}).

-record(s, {
    regs = #{} :: #{type() => module()},
    this_epoch :: epoch() | undefined,
    my_last_heartbeat :: non_neg_integer() | undefined,
    epochs :: #{node() => #{epoch() => epoch_lifetime()}}
}).

-doc "Unique ID of the timer event".
-type type() :: 0..?max_type.

-doc "Additional key of the timer".
-type key() :: binary().

-doc "Payload of the timer".
-type value() :: binary().

-type delay() :: non_neg_integer().

-type epoch() :: binary().

-doc """
Note: _End is an approximate moment when epoch end was detected.
It's not equal to the last epoch heartbeat.
""".
-type epoch_lifetime() :: {_Begin :: integer(), _End :: integer() | undefined}.

-callback durable_timer_type() -> type().

-callback timer_introduced_in() -> string().

-callback timer_deprected_since() -> string().

-callback handle_durable_timeout(key(), value()) -> ok.

-optional_callbacks([timer_deprected_since/0]).

%% States:
%%
%%  Unless at least one timer type is registered, this application
%%  lies dormant:
-define(s_dormant, dormant).
%%  There's no current epoch:
-define(s_isolated, isolated).
%%  Normal operation:
-define(s_normal, normal).

%% Timeouts:
-define(heartbeat, heartbeat).

-define(epoch_start, <<"s">>).
-define(epoch_end, <<"e">>).

%% Calls/casts:
-record(call_register, {module :: module()}).

-define(retry_fold(N, SLEEP, BODY), retry_fold(N, SLEEP, fun() -> BODY end)).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register_type(module()) -> ok | {error, _}.
register_type(Module) ->
    gen_statem:call(?SERVER, #call_register{module = Module}, infinity).

-doc """
Create a timer that activates when the Erlang node that created it
dies.
""".
-spec dead_hand(type(), key(), value(), delay()) -> ok | emqx_ds:error(_).
dead_hand(Type, Key, Value, Delay) when
    Type >= 0,
    Type =< ?max_type,
    is_binary(Key),
    is_binary(Value),
    is_integer(Delay),
    Delay >= 0
->
    ?tp(debug, ?tp_new_dead_hand, #{type => Type, key => Key, val => Value, delay => Delay}),
    Epoch = epoch(),
    emqx_durable_timer_worker:dead_hand(Type, Epoch, Key, Value, Delay).

-spec apply_after(type(), key(), value(), delay()) -> ok | emqx_ds:error(_).
apply_after(Type, Key, Value, Delay) when
    Type >= 0,
    Type =< ?max_type,
    is_binary(Key),
    is_binary(Value),
    is_integer(Delay),
    Delay >= 0
->
    ?tp(debug, ?tp_new_apply_after, #{type => Type, key => Key, val => Value, delay => Delay}),
    NotEarlierThan = now_ms() + Delay,
    Epoch = epoch(),
    %% This function may crash. Add a retry mechanism?
    emqx_durable_timer_worker:apply_after(Type, Epoch, Key, Value, NotEarlierThan).

-spec cancel(type(), key()) -> ok | emqx_ds:error(_).
cancel(Type, Key) when Type >= 0, Type =< ?max_type, is_binary(Key) ->
    ?tp(debug, ?tp_delete, #{type => Type, key => Key}),
    Epoch = epoch(),
    emqx_durable_timer_worker:cancel(Type, Epoch, Key).

%%================================================================================
%% behavior callbacks
%%================================================================================

callback_mode() ->
    [handle_event_function, state_enter].

init(_) ->
    {ok, ?s_dormant, undefined}.

%% Dormant:
handle_event(enter, PrevState, ?s_dormant, _) ->
    ?tp(debug, ?tp_state_change, #{from => PrevState, to => ?s_dormant}),
    keep_state_and_data;
handle_event(EventType, Event, ?s_dormant, D) ->
    ?tp(debug, ?tp_app_activation, #{type => EventType, event => Event}),
    {next_state, ?s_isolated, D, postpone};
%% Isolated:
handle_event(enter, PrevState, ?s_isolated, S0) ->
    %% Need lazy initialization?
    S =
        case PrevState of
            ?s_dormant ->
                ?tp(debug, ?tp_state_change, #{from => PrevState, to => ?s_isolated}),
                real_init();
            _ ->
                ?tp(error, ?tp_state_change, #{from => PrevState, to => ?s_isolated}),
                S0
        end,
    %% Shut down the workers.
    emqx_durable_timer_sup:stop_worker_sup(),
    %% Erase epoch:
    optvar:unset(?epoch_optvar),
    {keep_state, S, {state_timeout, 0, ?heartbeat}};
%% Normal:
handle_event(enter, PrevState, ?s_normal, S = #s{this_epoch = Epoch}) ->
    ?tp(debug, ?tp_state_change, #{from => PrevState, to => ?s_normal, epoch => Epoch}),
    %% Restart workers:
    ok = emqx_durable_timer_sup:start_worker_sup(),
    start_workers(S),
    %% Set epoch:
    optvar:set(?epoch_optvar, Epoch),
    {keep_state_and_data, {state_timeout, cfg_heartbeat_interval(), ?heartbeat}};
%% Common:
handle_event({call, From}, #call_register{module = Module}, State, Data) ->
    handle_register(State, Module, Data, From);
handle_event(state_timeout, ?heartbeat, State, Data) ->
    handle_heartbeat(State, Data);
handle_event(state_enter, From, To, _Data) ->
    ?tp(debug, ?tp_state_change, #{from => From, to => To}),
    keep_state_and_data;
handle_event(ET, Event, State, Data) ->
    ?tp(error, ?tp_unknown_event, #{m => ?MODULE, ET => Event, state => State, data => Data}),
    keep_state_and_data.

terminate(_Reason, ?s_normal, #s{this_epoch = Epoch}) ->
    _ = update_epoch(node(), Epoch, ?epoch_end),
    ok;
terminate(_Reason, _State, _D) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

lts_threshold_cb(0, _) ->
    infinity;
lts_threshold_cb(N, Root) when Root =:= ?top_deadhand; Root =:= ?top_started ->
    %% [<<"d">>, Type, Epoch, Key]
    if
        N =:= 1 -> infinity;
        true -> 0
    end;
lts_threshold_cb(_, _) ->
    0.

-spec ls_epochs(node()) -> #{epoch() => epoch_lifetime()}.
ls_epochs(Node) ->
    NodeBin = atom_to_binary(Node),
    FoldOpts = #{
        db => ?DB_GLOB,
        generation => generation(?DB_GLOB),
        errors => crash
    },
    #{Node := Ret} = ?retry_fold(
        10,
        100,
        emqx_ds:fold_topic(
            fun traverse_epochs/4,
            #{Node => #{}},
            nodes_topic(NodeBin, '+'),
            FoldOpts
        )
    ),
    Ret.

-spec ls_epochs() -> #{node() => #{epoch() => epoch_lifetime()}}.
ls_epochs() ->
    FoldOpts = #{
        db => ?DB_GLOB,
        generation => generation(?DB_GLOB),
        errors => report
    },
    element(
        1,
        emqx_ds:fold_topic(
            fun traverse_epochs/4,
            #{},
            nodes_topic('+', '+'),
            FoldOpts
        )
    ).

-spec last_heartbeat(epoch()) -> {ok, integer()} | undefined.
last_heartbeat(Epoch) when is_binary(Epoch) ->
    Result = ?retry_fold(
        10,
        100,
        emqx_ds:dirty_read(
            #{
                db => ?DB_GLOB,
                generation => generation(?DB_GLOB),
                errors => crash,
                shard => emqx_ds:shard_of(?DB_GLOB, Epoch)
            },
            heartbeat_topic(Epoch)
        )
    ),
    case Result of
        [{_, _, <<Time:64>>}] ->
            {ok, Time};
        [] ->
            undefined
    end.

-spec now_ms() -> integer().
now_ms() ->
    erlang:monotonic_time(millisecond) + erlang:time_offset(millisecond).

-spec epoch() -> epoch().
epoch() ->
    optvar:read(?epoch_optvar).

%%================================================================================
%% Internal functions
%%================================================================================

real_init() ->
    ?tp_span(
        debug,
        ?tp_init,
        #{},
        begin
            ok = ensure_tables(),
            maybe_close_prev_epoch(),
            #s{
                epochs = ls_epochs(),
                my_last_heartbeat = now_ms()
            }
        end
    ).

handle_register(State, Module, Data0 = #s{regs = Regs}, From) ->
    try
        Type = Module:durable_timer_type(),
        case Regs of
            #{Type := OldMod} ->
                Reply = {error, {already_registered, OldMod}},
                Data = Data0;
            #{} ->
                ok =
                    case State of
                        ?s_normal -> start_workers(Type, Data0);
                        ?s_isolated -> ok
                    end,
                Reply = ok,
                Data = Data0#s{regs = Regs#{Type => Module}}
        end,
        {keep_state, Data, {reply, From, Reply}}
    catch
        EC:Err:Stack ->
            {keep_state_and_data, {reply, From, {error, {EC, Err, Stack}}}}
    end.

handle_heartbeat(State, S0 = #s{this_epoch = LastEpoch, my_last_heartbeat = MLH}) ->
    %% Should we try to close the previous epoch?
    _ =
        case State of
            ?s_isolated when is_binary(LastEpoch) ->
                update_epoch(node(), LastEpoch, ?epoch_end);
            _ ->
                ok
        end,
    %% Which epoch to update? If isolated, create a new one:
    Epoch =
        case State of
            ?s_isolated ->
                crypto:strong_rand_bytes(8);
            ?s_normal ->
                LastEpoch
        end,
    Result = epoch_trans(
        Epoch,
        fun() ->
            Time = now_ms(),
            case State of
                ?s_isolated -> tx_update_epoch(node(), Epoch, ?epoch_start);
                _ -> ok
            end,
            emqx_ds:tx_write({heartbeat_topic(Epoch), 0, <<Time:64>>}),
            Time
        end
    ),
    Timeout = {state_timeout, cfg_heartbeat_interval(), ?heartbeat},
    DeadlineMissed = now_ms() >= (MLH + cfg_missed_heartbeats() / 2),
    case Result of
        {atomic, _, LEO} ->
            ?tp(debug, ?tp_heartbeat, #{epoch => Epoch, state => State}),
            S = S0#s{my_last_heartbeat = LEO, this_epoch = Epoch},
            {next_state, ?s_normal, S, Timeout};
        {error, EC, Err} when DeadlineMissed ->
            ?tp(error, ?tp_missed_heartbeat, #{epoch => Epoch, EC => Err, state => State}),
            {next_state, ?s_isolated, S0, Timeout};
        {error, EC, Err} ->
            ?tp(info, ?tp_missed_heartbeat, #{epoch => Epoch, EC => Err, state => State}),
            {keep_state_and_data, Timeout}
    end.

ensure_tables() ->
    %% FIXME: config
    NShards = 1,
    NSites = 1,
    RFactor = 5,
    Storage =
        {emqx_ds_storage_skipstream_lts_v2, #{
            lts_threshold_spec => {mf, ?MODULE, lts_threshold_cb},
            timestamp_bytes => 8
        }},
    Transaction = #{
        flush_interval => 1000, idle_flush_interval => 1, conflict_window => 5000
    },
    ok = emqx_ds:open_db(
        ?DB_GLOB,
        #{
            backend => builtin_raft,
            store_ttv => true,
            transaction => Transaction,
            storage => Storage,
            replication => #{},
            n_shards => NShards,
            n_sites => NSites,
            replication_factor => RFactor,
            atomic_batches => true,
            append_only => false,
            reads => leader_preferred
        }
    ).

maybe_close_prev_epoch() ->
    %% This function is called during initialization of the node,
    %% before creation of a new epoch. It closes any epoch for the
    %% node.
    maps:foreach(
        fun(Epoch, Lifetime) ->
            case Lifetime of
                {_, undefined} ->
                    %% This epoch has been left by us, and no other
                    %% node has closed it:
                    _ = update_epoch(node(), Epoch, ?epoch_end);
                {_, End} when is_integer(End) ->
                    ok
            end
        end,
        ls_epochs(node())
    ).

traverse_epochs(_Slab, _Stream, {[_, NodeBin, Epoch], Time, Val}, Acc0) ->
    Node = binary_to_atom(NodeBin),
    Acc = maps:merge(#{Node => #{}}, Acc0),
    maps:update_with(
        Node,
        fun(Epochs0) ->
            Epochs = maps:merge(#{Epoch => {undefined, undefined}}, Epochs0),
            maps:update_with(
                Epoch,
                fun
                    ({_, End}) when Val =:= ?epoch_start ->
                        {Time, End};
                    ({Start, _}) when Val =:= ?epoch_end ->
                        {Start, Time}
                end,
                Epochs
            )
        end,
        Acc
    ).

update_epoch(Node, Epoch, Val) ->
    case epoch_trans(Epoch, fun() -> tx_update_epoch(Node, Epoch, Val) end) of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

tx_update_epoch(Node, Epoch, Val) ->
    NodeBin = atom_to_binary(Node),
    emqx_ds:tx_write({
        nodes_topic(NodeBin, Epoch),
        ?ds_tx_ts_monotonic,
        Val
    }),
    ?ds_tx_on_success(
        case Val of
            ?epoch_start ->
                ?tp(debug, ?tp_open_epoch, #{epoch => Epoch});
            ?epoch_end ->
                ?tp(debug, ?tp_close_epoch, #{epoch => Epoch})
        end
    ).

generation(_DB) ->
    1.

heartbeat_topic(NodeEpochId) ->
    [?top_heartbeat, NodeEpochId].

nodes_topic(Node, NodeEpochId) ->
    [?top_nodes, Node, NodeEpochId].

retry_fold(0, _Sleep, Fun) ->
    Fun();
retry_fold(N, Sleep, Fun) ->
    try
        Fun()
    catch
        error:L when is_list(L) ->
            timer:sleep(Sleep),
            retry_fold(N - 1, Sleep, Fun)
    end.

cfg_heartbeat_interval() ->
    application:get_env(?APP, heartbeat_interval, 5_000).

cfg_missed_heartbeats() ->
    application:get_env(?APP, missed_heartbeats, 30_000).

cfg_replay_retry_interval() ->
    application:get_env(?APP, replay_retry_interval, 500).

cfg_transaction_timeout() ->
    application:get_env(?APP, transaction_timeout, 1000).

-doc """
A wrapper for transactions that operate on a particular epoch.
""".
epoch_trans(Epoch, Fun) ->
    %% FIXME: config
    emqx_ds:trans(
        #{
            db => ?DB_GLOB,
            shard => {auto, Epoch},
            generation => generation(?DB_GLOB),
            retries => 10,
            retry_interval => 100
        },
        Fun
    ).

start_workers(S = #s{regs = Regs}) ->
    maps:foreach(
        fun(Type, _CBM) ->
            start_workers(Type, S)
        end,
        Regs
    ).

start_workers(Type, #s{regs = Regs, this_epoch = Epoch}) ->
    #{Type := CBM} = Regs,
    lists:foreach(
        fun(Shard) ->
            emqx_durable_timer_sup:start_worker(Type, Epoch, Shard, CBM, active)
        end,
        shards()
    ).

shards() ->
    Gen = generation(?DB_GLOB),
    maps:fold(
        fun({Shard, G}, _Info, Acc) ->
            case G of
                Gen -> [Shard | Acc];
                _ -> Acc
            end
        end,
        [],
        emqx_ds:list_generations_with_lifetimes(?DB_GLOB)
    ).

-ifdef(TEST).
drop_tables() ->
    emqx_ds:close_db(?DB_GLOB),
    emqx_ds:drop_db(?DB_GLOB).
-endif.
