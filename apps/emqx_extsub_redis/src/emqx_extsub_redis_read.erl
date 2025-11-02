%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_read).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_extsub_redis_internal.hrl").

-behaviour(gen_server).

-export([
    start/0,
    stop/0
]).

-export([
    query/2
]).

-export([
    connect/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(REDIS_POOL_NAME, ?MODULE).
-define(REDIS_POOL_SIZE, 8).

-define(AUTO_RECONNECT_INTERVAL, 2).

-record(query, {
    op :: atom(),
    args :: list(iodata()),
    from :: reference()
}).

start() ->
    Options = [
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, ?REDIS_POOL_SIZE},
        {redis_opts, emqx_extsub_redis_persist:redis_opts([])},
        % {pool_type, hash}
        {pool_type, random}
    ],
    {ok, _} = ecpool:start_sup_pool(?MODULE, ?REDIS_POOL_NAME, Options),
    ok.

stop() ->
    ecpool:stop_sup_pool(?REDIS_POOL_NAME).

query(Op, Args) ->
    From = alias([reply]),
    Query = #query{op = Op, args = Args, from = From},
    ecpool:pick_and_do(?REDIS_POOL_NAME, {erlang, send, [Query]}, no_handover),
    wait_resp(From).

%%--------------------------------------------------------------------
%% ecpool API
%%--------------------------------------------------------------------

connect(Opts) ->
    EredisOpts = proplists:get_value(redis_opts, Opts),
    start_link(EredisOpts).

start_link(EredisOpts) ->
    gen_server:start_link(?MODULE, [EredisOpts], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([EredisOpts]) ->
    {ok, Conn} = eredis:start_link(EredisOpts),
    {ok, #{conn => Conn}}.

handle_info(
    #query{op = Op, args = Args, from = From},
    State = #{conn := Conn}
) ->
    ?tp_debug(query, #{op => Op, args => Args, from => From, state => State}),
    Results = erlang:apply(eredis, Op, [Conn | Args]),
    erlang:send(From, {From, Results}),
    {noreply, State};
handle_info(_Info, State) ->
    ?tp_debug(unknown_info, #{info => _Info, state => State}),
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, {unknown_call, _Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

wait_resp(From) ->
    receive
        {From, Result} ->
            Result
    after 5000 ->
        unalias(From),
        receive
            {From, Result} ->
                Result
        after 0 ->
            ok
        end,
        {error, timeout}
    end.
