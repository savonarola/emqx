%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_sub).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-export([
    start/0,
    stop/0
]).

-export([
    redis_opts/1,
    insert/2
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


-record(subscribe, {
    topic :: binary(),
    from :: reference(),
    pid :: pid()
}).
-record(unsubscribe, {
    topic :: binary(),
    from :: reference(),
    pid :: pid()
}).

start() ->
    Options = [
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, ?REDIS_POOL_SIZE},
        {redis_opts, emqx_extsub_redis_persist:redis_opts([])},
        {pool_type, hash}
    ],
    {ok, _} = ecpool:start_sup_pool(?MODULE, ?REDIS_POOL_NAME, Options),
    ok.

stop() ->
    ecpool:stop_sup_pool(?REDIS_POOL_NAME).

subscribe(#{topic := Topic} = _InsertHandle) ->
    Key = Topic,
    From = alias([reply]),
    Subscribe = #subscribe{topic = Topic, from = From, pid = self()},
    ok = ecpool:pick_and_do({?REDIS_POOL_NAME, Key}, {erlang, send, [Subscribe]}, no_handover),
    wait_resp(From).

unsubscribe(#{topic := Topic} = _InsertHandle) ->
    Key = Topic,
    From = alias([reply]),
    Unsubscribe = #unsubscribe{topic = Topic, from = From, pid = self()},
    ok = ecpool:pick_and_do({?REDIS_POOL_NAME, Key}, {erlang, send, [Unsubscribe]}, no_handover),
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
    {ok, ListenConn} = eredis_sub:start_link(EredisOpts),
    ok = eredis_sub:controlling_process(ListenConn, self()),
    {ok, #{by_pid => #{}, by_topic => #{}, conn => ListenConn}}.

handle_info(#subscribe{from = From, pid = Pid, topic = Topic}, State = #{by_pid := ByPid0, by_topic := ByTopic0, conn := Conn}) ->
    PidInfo = case ByPid0 of
        #{Pid := #{topics := Topics} = Info} ->
            Info#{topics => Topics#{Topic => true}};
        _ ->
            MRef = erlang:monitor(process, Pid),
            #{mref => MRef, topics => #{Topic => true}}
    end,
    ByPid = ByPid0#{Pid => PidInfo},
    ByTopic = case ByTopic0 of
        #{Topic := Pids} ->
            #{Topic => Pids#{Pid => true}};
        _ ->
            ok = eredis_sub:subscribe(Conn, [channel(Topic)]),
            #{Topic => #{Pid => true}}
    end,
    erlang:send(From, {From, ok}),
    {noreply, State#{by_pid => ByPid, by_topic => ByTopic}};
handle_info(#unsubscribe{pid = Pid, topic = Topic, from = From}, State0 = #{by_pid := ByPid0, by_topic := ByTopic0, conn := Conn}) ->
    case ByPid0 of
        #{Pid := #{topics := Topics0, mref := MRef} = PidInfo} ->
            Topics = maps:remove(Topic, Topics0),
            ByPid = case maps:size(Topics) of
                0 ->
                    erlang:demonitor(MRef, [flush]),
                    maps:remove(Pid, ByPid0);
                _ ->
                    ByPid0#{Pid => PidInfo#{topics => Topics}}
            end,
            #{Topic := Pids0} = ByTopic0,
            Pids = maps:remove(Pid, Pids0),
            ByTopic = case maps:size(Pids) of
                0 ->
                    ok = eredis_sub:unsubscribe(Conn, [channel(Topic)]),
                    maps:remove(Topic, ByTopic0);
                _ ->
                    ByTopic0#{Topic => Pids}
            end,
            erlang:send(From, {From, ok}),
            {noreply, State0#{by_pid => ByPid, by_topic => ByTopic}};
        _ ->
            erlang:send(From, {From, {error, not_found}}),
            {noreply, State0}
    end;
handle_info({'DOWN', MRef, process, Pid, Reason}, State = #{by_pid := ByPid0}) ->
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------


flush(State = #{buffer := []}) ->
    State;
flush(#{conn := Connection, buffer := Buffer0, store_sha := SHA} = State) ->
    Buffer = lists:reverse(Buffer0),
    Commands = lists:map(
        fun(#insert{topic = Topic, message = MessageBin}) ->
            [<<"EVALSHA">>, SHA, <<"3">>, id_key(Topic), messages_key(Topic), channel(Topic), MessageBin]
        end,
        Buffer
    ),
    Results = eredis:qp(Connection, Commands),
    %% TODO
    %% Ignore errors for now
    ?tp(debug, mq_message_db_redis_flush, #{
        results => Results,
        commands => Commands
    }),
    send_replies(ok, Buffer),
    State#{buffer := []}.

send_replies(Result, Buffer) ->
    lists:foreach(
        fun(#insert{from = From}) ->
            erlang:send(From, {From, Result})
        end,
        Buffer
    ).

channel(Topic) ->
    <<"ch:", Topic/binary>>.

messages_key(Topic) ->
    <<"m:", Topic/binary>>.

id_key(Topic) ->
    <<"id:", Topic/binary>>.

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
