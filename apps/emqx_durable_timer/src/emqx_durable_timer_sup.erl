%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_sup).

-behavior(supervisor).

%% API:
-export([start_link/0, start_workers/0, stop_workers/0, start_type/3]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_link_types_sup/0, start_link_type_sup/3]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(TOP, ?MODULE).
-define(TYPES_SUP, emqx_durable_timer_types_sup).
-record(type_sup, {
    type :: emqx_durable_timer:type(), cbm :: module(), epoch :: emqx_durable_timer:epoch()
}).

-define(type_name(TYPE), {n, l, {emqx_durable_timer_type_sup, TYPE}}).
-define(via(NAME), {via, gproc, NAME}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?TOP}, ?MODULE, ?TOP).

-spec start_workers() -> ok.
start_workers() ->
    case supervisor:restart_child(?TOP, types_sup) of
        {ok, _} ->
            ok;
        {ok, _, _} ->
            ok;
        {error, running} ->
            ok
    end.

-spec stop_workers() -> ok.
stop_workers() ->
    ok = supervisor:terminate_child(?TOP, types_sup).

-spec start_type(emqx_ds:type(), module(), emqx_durable_timer:epoch()) -> ok.
start_type(Type, CBM, Epoch) ->
    case supervisor:start_child(?TYPES_SUP, [Type, CBM, Epoch]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        Err ->
            Err
    end.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_types_sup() -> supervisor:startlink_ret().
start_link_types_sup() ->
    supervisor:start_link({local, ?TYPES_SUP}, ?MODULE, ?TYPES_SUP).

-spec start_link_type_sup(emqx_durable_timer:type(), module(), emqx_durable_timer:epoch()) ->
    supervisor:startlink_ret().
start_link_type_sup(Type, CBM, Epoch) ->
    supervisor:start_link(?via(?type_name(Type)), ?MODULE, #type_sup{
        type = Type, cbm = CBM, epoch = Epoch
    }).

%%================================================================================
%% behavior callbacks
%%================================================================================

init(?TOP) ->
    Children = [
        #{
            id => types_sup,
            start => {?MODULE, start_link_types_sup, []},
            shutdown => infinity,
            restart => permanent,
            type => supervisor
        },
        #{
            id => main,
            start => {emqx_durable_timer, start_link, []},
            shutdown => 5_000,
            restart => permanent,
            type => worker
        }
    ],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, Children}};
init(?TYPES_SUP) ->
    Children = [
        #{
            id => type,
            start => {?MODULE, start_link_type_sup, []},
            shutdown => infinity,
            type => supervisor
        }
    ],
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, Children}};
init(#type_sup{type = Type, cbm = CBM, epoch = Epoch}) ->
    Children = [
        #{
            id => worker,
            start => {emqx_durable_timer_worker, start_link, [Type, CBM, Epoch]},
            shutdown => brutal_kill,
            intensity => 0,
            period => 0
        }
    ],
    SupFlags = #{
        stratgey => one_for_all,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================
