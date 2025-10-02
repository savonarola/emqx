%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_db_redis).

-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-export([
    start/0,
    stop/0
]).

-export([
    redis_opts/1,
    channel/1,
    key/1,
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

-define(REDIS_STORE_LIMIT_BIN, <<"10000000">>).

-record(insert, {
    key :: binary(),
    message :: binary(),
    from :: reference(),
    channel :: string()
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

connect(Opts) ->
    EredisOpts = proplists:get_value(redis_opts, Opts),
    start_link(EredisOpts).

start_link(EredisOpts) ->
    gen_server:start_link(?MODULE, [EredisOpts], []).

init([EredisOpts]) ->
    {ok, Conn} = eredis:start_link(EredisOpts),
    {ok, #{conn => Conn, buffer => [], flush_tref => undefined}}.

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
flush(#{conn := Connection, buffer := Buffer0} = State) ->
    Buffer = lists:reverse(Buffer0),
    CommandsXADD = lists:map(
        fun(#insert{key = Key, message = MessageBin}) ->
            [<<"XADD">>, Key, <<"MAXLEN">>, ?REDIS_STORE_LIMIT_BIN, <<"*">>, <<"m">>, MessageBin]
        end,
        Buffer
    ),
    Channels0 = lists:map(
        fun(#insert{channel = Channel}) ->
            Channel
        end,
        Buffer
    ),
    Channels = lists:usort(Channels0),
    CommandsNOTIFY = lists:map(
        fun(Channel) ->
            [<<"PUBLISH">>, Channel, <<"1">>]
        end,
        Channels
    ),
    Commands = [[<<"MULTI">>] | CommandsXADD ++ CommandsNOTIFY ++ [[<<"EXEC">>]]],
    Results = eredis:qp(Connection, Commands),
    %% TODO
    %% Ignore errors for now
    EXECResult = lists:last(Results),
    ?tp(debug, mq_message_db_redis_flush, #{
        results => EXECResult,
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

insert(MQ, Message) ->
    MessageBin = emqx_mq_message_db:encode_message(Message),
    From = alias([reply]),
    Insert = #insert{
        key = key(MQ), message = MessageBin, from = From, channel = channel(MQ)
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

channel(#{topic_filter := TopicFilter} = _MQ) ->
    <<"l_", TopicFilter/binary>>.

key(#{topic_filter := TopicFilter} = _MQ) ->
    <<"d_", TopicFilter/binary>>.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_insert(BufferPid, Insert) ->
    _ = erlang:send(BufferPid, Insert),
    ok.
