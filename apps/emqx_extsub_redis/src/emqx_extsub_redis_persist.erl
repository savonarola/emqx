%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_persist).

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
    connect/1,
    do_insert/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(REDIS_HOST, "127.0.0.1").
-define(REDIS_PORT, 6379).
-define(REDIS_DATABASE, 1).

-define(REDIS_POOL_NAME, ?MODULE).
-define(REDIS_POOL_SIZE, 8).

-define(MAX_BUFFER_SIZE, 100).
-define(FLUSH_INTERVAL, 10).

-define(AUTO_RECONNECT_INTERVAL, 2).


%% EVALSHA SHA 3 id:TOPIC m:TOPIC ch:TOPIC MESSAGE_BIN
-define(STORE_LUA, """
    local new_weight = redis.call('INCR', KEYS[1]);
    redis.call('ZADD', KEYS[2], new_weight, ARGV[1]);
    local publish_payload = new_weight .. ':' .. ARGV[1];
    redis.call('PUBLISH', KEYS[3], publish_payload);
    return new_weight
""").

-record(insert, {
    topic :: binary(),
    message :: binary(),
    from :: reference()
}).
-record(flush, {}).

redis_opts(Overrides) ->
    Default = [
        {host, ?REDIS_HOST},
        {port, ?REDIS_PORT},
        {database, ?REDIS_DATABASE}
    ],
    Default ++ Overrides.

start() ->
    Options = [
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, ?REDIS_POOL_SIZE},
        {redis_opts, redis_opts([])}
    ],
    {ok, _} = ecpool:start_sup_pool(?MODULE, ?REDIS_POOL_NAME, Options),
    ok.

stop() ->
    ecpool:stop_sup_pool(?REDIS_POOL_NAME).

insert(#{topic := Topic} = _InsertHandle, Message) ->
    MessageBin = emqx_mq_message_db:encode_message(Message),
    From = alias([reply]),
    Insert = #insert{
        topic = Topic, message = MessageBin, from = From
    },
    ok = ecpool:pick_and_do(?REDIS_POOL_NAME, {?MODULE, do_insert, [Insert]}, no_handover),
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
    {ok, SHA} = eredis:q(Conn, [<<"SCRIPT">>, <<"LOAD">>, ?STORE_LUA]),
    ?tp(warning, extsub_redis_persist_init, #{sha => SHA}),
    {ok, #{conn => Conn, buffer => [], flush_tref => undefined, store_sha => SHA}}.

handle_info(#insert{} = Insert, State0 = #{buffer := Buffer0}) ->
    Buffer = [Insert | Buffer0],
    State1 = State0#{buffer => Buffer},
    State2 = maybe_flush(State1),
    State = ensure_flush_timer(State2),
    {noreply, State};
handle_info(#flush{}, State0) ->
    State1 = flush(State0),
    State = ensure_flush_timer(State1),
    {noreply, State};
handle_info(Info, State) ->
    ?tp(warning, mq_message_db_redis_info, #{info => Info}),
    {noreply, State}.

handle_cast(Info, State) ->
    ?tp(warning, mq_message_db_redis_cast, #{info => Info}),
    {noreply, State}.

handle_call(Info, _From, State) ->
    ?tp(warning, mq_message_db_redis_call, #{info => Info}),
    {reply, {error, {unknown_call, Info}}, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

maybe_flush(State = #{buffer := Buffer}) when length(Buffer) >= ?MAX_BUFFER_SIZE ->
    flush(State);
maybe_flush(State) ->
    State.

ensure_flush_timer(State = #{flush_tref := undefined, buffer := []}) ->
    State;
ensure_flush_timer(State = #{flush_tref := undefined, buffer := _Buffer}) ->
    TRef = erlang:send_after(?FLUSH_INTERVAL, self(), #flush{}),
    State#{flush_tref := TRef};
ensure_flush_timer(State = #{flush_tref := TRef, buffer := []}) ->
    _ = emqx_utils:cancel_timer(TRef),
    State#{flush_tref := undefined};
ensure_flush_timer(State) ->
    State.

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

do_insert(BufferPid, Insert) ->
    _ = erlang:send(BufferPid, Insert),
    ok.
