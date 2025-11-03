%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_sub).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_extsub_redis_internal.hrl").

-behaviour(gen_server).

-export([
    start/0,
    stop/0
]).

-export([
    subscribe/1,
    unsubscribe/1
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
    pub_fn :: function(),
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

subscribe(#{topic := Topic, pub_fn := PubFn}) ->
    Key = Topic,
    From = alias([reply]),
    Subscribe = #subscribe{topic = Topic, from = From, pid = self(), pub_fn = PubFn},
    _ = ecpool:pick_and_do({?REDIS_POOL_NAME, Key}, {erlang, send, [Subscribe]}, no_handover),
    wait_resp(From).

unsubscribe(#{topic := Topic}) ->
    Key = Topic,
    From = alias([reply]),
    Unsubscribe = #unsubscribe{topic = Topic, from = From, pid = self()},
    _ = ecpool:pick_and_do({?REDIS_POOL_NAME, Key}, {erlang, send, [Unsubscribe]}, no_handover),
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

handle_info(
    #subscribe{from = From, pid = Pid, topic = Topic, pub_fn = PubFn},
    State = #{by_pid := ByPid0, by_topic := ByTopic0, conn := Conn}
) ->
    ?tp_debug(subscribe, #{
        from => From, pid => Pid, topic => Topic, pub_fn => PubFn, state => State
    }),
    PidInfo =
        case ByPid0 of
            #{Pid := #{topics := Topics} = Info} ->
                Info#{topics => Topics#{Topic => true}};
            _ ->
                MRef = erlang:monitor(process, Pid),
                #{mref => MRef, topics => #{Topic => true}}
        end,
    ByPid = ByPid0#{Pid => PidInfo},
    ByTopic =
        case ByTopic0 of
            #{Topic := Pids} ->
                ByTopic0#{Topic => Pids#{Pid => PubFn}};
            _ ->
                ok = eredis_sub:subscribe(Conn, [channel(Topic)]),
                ByTopic0#{Topic => #{Pid => PubFn}}
        end,
    erlang:send(From, {From, ok}),
    {noreply, State#{by_pid => ByPid, by_topic => ByTopic}};
handle_info(#unsubscribe{pid = Pid, topic = Topic, from = From}, State0) ->
    ?tp_debug(unsubscribe, #{pid => Pid, topic => Topic, from => From, state => State0}),
    case remove(Pid, Topic, State0) of
        {true, State} ->
            erlang:send(From, {From, ok}),
            {noreply, State};
        {false, State} ->
            erlang:send(From, {From, {error, not_found}}),
            {noreply, State}
    end;
handle_info({'DOWN', _MRef, process, Pid, _Reason}, #{by_pid := ByPid} = State0) ->
    ?tp_debug('DOWN', #{pid => Pid, reason => _Reason, state => State0}),
    case ByPid of
        #{Pid := #{topics := Topics}} ->
            State = lists:foldl(
                fun(Topic, StateAcc0) ->
                    {true, StateAcc} = remove(Pid, Topic, StateAcc0),
                    StateAcc
                end,
                State0,
                maps:keys(Topics)
            ),
            {noreply, State};
        _ ->
            {noreply, State0}
    end;
handle_info(
    {message, <<"ch:", Topic/binary>>, Notification, _Pid},
    #{conn := Conn, by_topic := ByTopic} = State0
) ->
    ?tp_debug(message, #{topic => Topic, notification => Notification, pid => _Pid}),
    {Id, Message} = parse_pub_message(State0, Notification),
    ok = eredis_sub:ack_message(Conn),
    case ByTopic of
        #{Topic := Pids} ->
            lists:foreach(
                fun(PubFn) ->
                    PubFn({redis_pub, Topic, Id, Message})
                end,
                maps:values(Pids)
            );
        _ ->
            ok
    end,
    {noreply, State0};
handle_info(
    {subscribed, _Channel, _Pid},
    #{conn := Conn} = State
) ->
    ?tp_debug(redis_subscribed, #{channel => _Channel, pid => _Pid}),
    ok = eredis_sub:ack_message(Conn),
    {noreply, State};
handle_info({unsubscribed, _Channel, _Pid}, #{conn := Conn} = State) ->
    ?tp_debug(redis_unsubscribed, #{channel => _Channel, pid => _Pid}),
    ok = eredis_sub:ack_message(Conn),
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

remove(Pid, Topic, #{by_pid := ByPid0, by_topic := ByTopic0, conn := Conn} = State0) ->
    case ByPid0 of
        #{Pid := #{topics := Topics0, mref := MRef} = PidInfo} ->
            Topics = maps:remove(Topic, Topics0),
            ByPid =
                case maps:size(Topics) of
                    0 ->
                        erlang:demonitor(MRef, [flush]),
                        maps:remove(Pid, ByPid0);
                    _ ->
                        ByPid0#{Pid => PidInfo#{topics => Topics}}
                end,
            #{Topic := Pids0} = ByTopic0,
            Pids = maps:remove(Pid, Pids0),
            ByTopic =
                case maps:size(Pids) of
                    0 ->
                        ok = eredis_sub:unsubscribe(Conn, [channel(Topic)]),
                        maps:remove(Topic, ByTopic0);
                    _ ->
                        ByTopic0#{Topic => Pids}
                end,
            {true, State0#{by_pid => ByPid, by_topic => ByTopic}};
        _ ->
            {false, State0}
    end.

channel(Topic) ->
    <<"ch:", Topic/binary>>.

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

parse_pub_message(_State, Bin) ->
    [IdBin, MessageBin] = binary:split(Bin, <<":">>),
    Message = emqx_mq_message_db:decode_message(MessageBin),
    Id = binary_to_integer(IdBin),
    {Id, Message}.
