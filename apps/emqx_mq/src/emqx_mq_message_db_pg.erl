%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_db_pg).

-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-export([
    start/0,
    stop/0
]).

-export([
    epgsql_opts/1,
    channel/1,
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

-define(PG_HOST, "127.0.0.1").
-define(PG_PORT, 5432).
-define(PG_USER, "av").
-define(PG_PASSWORD, "public").
-define(PG_DATABASE, "av").

-define(PG_POOL_NAME, ?MODULE).
-define(PG_POOL_SIZE, 8).

-define(MAX_BUFFER_SIZE, 100).
-define(FLUSH_INTERVAL, 100).

-define(AUTO_RECONNECT_INTERVAL, 2).

-define(PG_INSERT_SQL, "INSERT INTO messages (topic, created_at, message) VALUES ($1, now(), $2)").
-define(PG_INSERT_PREPARE_NAME, "insert_message").

-record(insert, {
    topic_filter :: binary(),
    message :: binary(),
    from :: reference(),
    channel :: string()
}).
-record(flush, {}).

epgsql_opts(Overrides) ->
    Default = #{
        host => ?PG_HOST,
        port => ?PG_PORT,
        username => ?PG_USER,
        password => ?PG_PASSWORD,
        database => ?PG_DATABASE
    },
    maps:merge(Default, Overrides).

start() ->
    Options = [
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, ?PG_POOL_SIZE},

        {epgsql_opts, epgsql_opts(#{})}
    ],
    {ok, _} = ecpool:start_sup_pool(?MODULE, ?PG_POOL_NAME, Options),
    ok.

stop() ->
    ecpool:stop_sup_pool(?PG_POOL_NAME).

connect(Opts) ->
    EpgsqlOpts = proplists:get_value(epgsql_opts, Opts),
    start_link(EpgsqlOpts).

start_link(EpgsqlOpts) ->
    gen_server:start_link(?MODULE, [EpgsqlOpts], []).

init([EpgsqlOpts]) ->
    {ok, Conn} = epgsql:connect(EpgsqlOpts),
    {ok, _Stmt} = epgsql:parse(Conn, ?PG_INSERT_PREPARE_NAME, ?PG_INSERT_SQL, []),
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
    ?tp(warning, mq_message_db_pg_info, #{info => Info}),
    {noreply, State}.

handle_cast(Info, State) ->
    ?tp(warning, mq_message_db_pg_cast, #{info => Info}),
    {noreply, State}.

handle_call(Info, _From, State) ->
    ?tp(warning, mq_message_db_pg_call, #{info => Info}),
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

flush(#{conn := Connection, buffer := Buffer0} = State) ->
    Buffer = lists:reverse(Buffer0),
    TxResult = epgsql:with_transaction(
        Connection,
        fun(Conn) ->
            do_insert_tx(Conn, Buffer)
        end,
        []
    ),
    Result =
        case TxResult of
            {rollback, Reason} ->
                {error, Reason};
            ok ->
                ok
        end,
    send_replies(Result, Buffer),
    State#{buffer := []}.

send_replies(Result, Buffer) ->
    lists:foreach(
        fun(#insert{from = From}) ->
            erlang:send(From, {From, Result})
        end,
        Buffer
    ).

insert(#{topic_filter := TopicFilter} = MQ, Message) ->
    MessageBin = emqx_mq_message_db:encode_message(Message),
    Channel = binary_to_list(channel(MQ)),
    From = alias([reply]),
    Insert = #insert{
        topic_filter = TopicFilter, message = MessageBin, from = From, channel = Channel
    },
    ok = ecpool:pick_and_do(?PG_POOL_NAME, {?MODULE, do_insert, [Insert]}, no_handover),
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
    <<"t_", (emqx_utils:bin_to_hexstr(crypto:hash(sha, TopicFilter), lower))/binary>>.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_insert(BufferPid, Insert) ->
    _ = erlang:send(BufferPid, Insert),
    ok.

do_insert_tx(Conn, Buffer) ->
    ok = lists:foreach(
        fun(#insert{topic_filter = TopicFilter, message = MessageBin}) ->
            {ok, _} = epgsql:prepared_query(Conn, ?PG_INSERT_PREPARE_NAME, [
                TopicFilter, MessageBin
            ])
        end,
        Buffer
    ),
    Channels0 = lists:map(fun(#insert{channel = Channel}) -> Channel end, Buffer),
    Channels = lists:usort(Channels0),
    ok = lists:foreach(
        fun(Channel) ->
            {ok, [], []} = epgsql:squery(Conn, "NOTIFY " ++ Channel)
        end,
        Channels
    ).
