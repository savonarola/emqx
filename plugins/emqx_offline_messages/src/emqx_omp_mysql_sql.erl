%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_mysql_sql).
-moduledoc "Fixed MySQL statements for ordered offline message storage.".
-behaviour(emqx_omp_backend).
-export([
    connect/1,
    close/1,
    install/1,
    health/1,
    append/4,
    subscribe/6,
    restore/3,
    unsubscribe/4,
    checkpoint/5,
    page/4,
    topics/4,
    age_cutoff/4,
    consumed_cutoff/3,
    trim/4,
    expire/4,
    max_client_id_length/0,
    query/3
]).

%% Restarts of a transaction or an append call after a deadlock.
-define(TRANSACTION_RETRIES, 3).
%% MySQL error code of a deadlock.
-define(ER_LOCK_DEADLOCK, 1213).
%% Size of the `client_id` column.
-define(MAX_CLIENT_ID_LENGTH, 2048).

-type config() :: emqx_omp_backend:config().
-type namespace() :: emqx_omp_backend:namespace().
-type topic_id() :: emqx_omp_backend:topic_id().
-type offset() :: emqx_omp_backend:offset().
-type connection() :: mysql:connection().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Open a MySQL connection in read-committed isolation.".
-spec connect(config()) -> {ok, connection()} | {error, term()}.
connect(#{connection := Options}) -> mysql:start_link(Options).

-doc "Close a MySQL connection.".
-spec close(connection()) -> ok.
close(Conn) ->
    unlink(Conn),
    mysql:stop(Conn).

-doc """
Install the packaged schema unless it is already complete.
Installation is serialized across nodes. A node waits at most `query_timeout` for another
node's installation. A complete schema is never changed, because other nodes can be calling
its procedure.
""".
-spec install(config()) -> ok.
install(#{query_timeout := Timeout} = Config) ->
    {ok, Conn} = connect(Config),
    try
        ok = lock_schema(Conn, Timeout),
        case is_installed(Conn) of
            true ->
                ok;
            false ->
                {ok, SQL} = file:read_file(
                    filename:join(code:priv_dir(emqx_offline_messages), "mysql.sql")
                ),
                lists:foreach(
                    fun(Statement) -> {ok, _} = query(Conn, Statement, []) end,
                    statements(SQL)
                ),
                health(Conn)
        end
    after
        %% Closing the connection also releases its installation lock.
        close(Conn)
    end.

-doc "Check the schema version and that the account can see the append procedure.".
-spec health(connection()) -> ok.
health(Conn) ->
    {ok, [{1}]} = query(Conn, "SELECT version FROM mqtt_omp_schema", []),
    {ok, [{1}]} = query(Conn, """
    SELECT COUNT(*) FROM information_schema.ROUTINES
    WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'omp_append'
      AND ROUTINE_TYPE = 'PROCEDURE'
    """, []),
    ok.

-doc """
Commit a batch with consecutive offsets in one call of the `omp_append` procedure.
The procedure holds the topic row lock only on the server. A deadlock rolls the call back,
so the call is repeated.
""".
-spec append(connection(), namespace(), emqx_types:topic(), [
    emqx_omp_backend:encoded_message(), ...
]) ->
    {ok, {topic_id(), offset()}}.
append(Conn, Namespace, Topic, [_ | _] = Messages) ->
    %% A text query needs no separate statement preparation round trip.
    Batch = emqx_utils_json:encode([append_message(Message) || Message <- Messages]),
    SQL = [
        "CALL omp_append(",
        lists:join(", ", [mysql:encode(Conn, Value) || Value <- [Namespace, Topic, Batch]]),
        ")"
    ],
    call_append(Conn, SQL, ?TRANSACTION_RETRIES).

-doc "Create or renew a subscription while serializing its starting position with appends.".
-spec subscribe(
    connection(),
    namespace(),
    emqx_types:clientid(),
    emqx_types:topic(),
    emqx_types:subopts(),
    emqx_omp_backend:start_position()
) ->
    {ok, #{topic_id := topic_id(), cursor := offset()}}.
subscribe(Conn, Namespace, ClientId, Topic, SubOpts, Start) ->
    transaction(Conn, fun() ->
        {Id, Next} = share_lock_topic_tx(Conn, Namespace, Topic),
        Ack =
            case Start of
                earliest -> 0;
                latest -> Next - 1
            end,
        Options = emqx_utils_json:encode(SubOpts),
        {ok, _} = query(Conn, """
        INSERT INTO mqtt_omp_subscription
          (namespace, client_id, topic_id, subopts, ack_offset, updated_at)
        VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(3))
        ON DUPLICATE KEY UPDATE subopts = ?, updated_at = UTC_TIMESTAMP(3)
        """, [Namespace, ClientId, Id, Options, Ack, Options]),
        {ok, [{Cursor}]} = query(Conn, """
        SELECT ack_offset FROM mqtt_omp_subscription
        WHERE namespace = ? AND client_id = ? AND topic_id = ?
        """, [Namespace, ClientId, Id]),
        {ok, #{topic_id => Id, cursor => Cursor}}
    end).

-doc "Renew and return a client's subscriptions in one transaction.".
-spec restore(connection(), namespace(), emqx_types:clientid()) ->
    {ok, [emqx_omp_backend:subscription_row()]}.
restore(Conn, Namespace, ClientId) ->
    transaction(Conn, fun() ->
        {ok, _} = query(Conn, """
        UPDATE mqtt_omp_subscription SET updated_at = UTC_TIMESTAMP(3)
        WHERE namespace = ? AND client_id = ?
        """, [Namespace, ClientId]),
        query(Conn, """
        SELECT t.topic, s.subopts FROM mqtt_omp_subscription s
        JOIN mqtt_omp_topic t USING (topic_id) WHERE s.namespace = ? AND s.client_id = ?
        """, [Namespace, ClientId])
    end).

-doc "Remove an explicitly unsubscribed topic.".
-spec unsubscribe(connection(), namespace(), emqx_types:clientid(), emqx_types:topic()) ->
    {ok, non_neg_integer()}.
unsubscribe(Conn, Namespace, ClientId, Topic) ->
    query(Conn, """
    DELETE s FROM mqtt_omp_subscription s JOIN mqtt_omp_topic t USING (topic_id)
    WHERE s.namespace = ? AND s.client_id = ? AND t.topic = ?
    """, [Namespace, ClientId, Topic]).

-doc "Renew a subscription and save progress without moving it backwards.".
-spec checkpoint(connection(), namespace(), emqx_types:clientid(), topic_id(), offset()) ->
    {ok, non_neg_integer()}.
checkpoint(Conn, Namespace, ClientId, TopicId, Offset) ->
    query(Conn, """
    UPDATE mqtt_omp_subscription
    SET ack_offset = GREATEST(ack_offset, ?), updated_at = UTC_TIMESTAMP(3)
    WHERE namespace = ? AND client_id = ? AND topic_id = ?
    """, [Offset, Namespace, ClientId, TopicId]).

-doc "Read an ordered page and its tip from one statement snapshot.".
-spec page(connection(), topic_id(), offset(), pos_integer()) ->
    {ok, [emqx_omp_backend:page_row()]}.
page(Conn, TopicId, Cursor, Count) ->
    {ok, Rows} = query(Conn, """
    SELECT t.next_offset - 1, m.msg_offset, m.message_id, m.sender, m.qos, m.retain,
           m.payload, m.properties, m.published_at, m.expires_at
    FROM mqtt_omp_topic t LEFT JOIN (
      SELECT * FROM mqtt_omp_message WHERE topic_id = ? AND msg_offset > ?
      ORDER BY msg_offset LIMIT ?
    ) m ON m.topic_id = t.topic_id WHERE t.topic_id = ? ORDER BY m.msg_offset
    """, [TopicId, Cursor, Count, TopicId]),
    {ok, [normalize_row(Row) || Row <- Rows]}.

-doc "Enumerate the namespace's topics in topic ID order.".
-spec topics(connection(), namespace(), topic_id() | 0, pos_integer()) ->
    {ok, [{topic_id(), offset()}]}.
topics(Conn, Namespace, After, Limit) ->
    query(Conn, """
    SELECT topic_id, next_offset FROM mqtt_omp_topic
    WHERE namespace = ? AND topic_id > ? ORDER BY topic_id LIMIT ?
    """, [Namespace, After, Limit]).

-doc "Find the first offset outside the expired prefix.".
-spec age_cutoff(connection(), topic_id(), offset(), integer()) -> {ok, offset()}.
age_cutoff(Conn, TopicId, Next, Oldest) ->
    case query(Conn, """
    SELECT msg_offset FROM mqtt_omp_message
    WHERE topic_id = ? AND published_at >= ? ORDER BY msg_offset LIMIT 1
    """, [TopicId, Oldest]) of
        {ok, [{Offset}]} -> {ok, Offset};
        {ok, []} -> {ok, Next}
    end.

-doc "Find the first offset that some subscription has not acknowledged.".
-spec consumed_cutoff(connection(), topic_id(), offset()) -> {ok, offset()}.
consumed_cutoff(Conn, TopicId, Next) ->
    case
        query(
            Conn,
            "SELECT min(ack_offset) FROM mqtt_omp_subscription WHERE topic_id = ?",
            [TopicId]
        )
    of
        {ok, [{null}]} -> {ok, Next};
        {ok, [{Ack}]} -> {ok, Ack + 1}
    end.

-doc "Delete the retained prefix in bounded batches.".
-spec trim(connection(), topic_id(), offset(), pos_integer()) -> {ok, non_neg_integer()}.
trim(Conn, TopicId, Cutoff, Batch) -> trim(Conn, TopicId, Cutoff, Batch, 0).

-doc "Delete a bounded batch of inactive subscriptions.".
-spec expire(connection(), namespace(), pos_integer(), pos_integer()) -> {ok, non_neg_integer()}.
expire(Conn, Namespace, Expiry, Batch) ->
    query(Conn, """
    DELETE FROM mqtt_omp_subscription
    WHERE namespace = ? AND updated_at < TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3))
    LIMIT ?
    """, [Namespace, -Expiry * 1000, Batch]).

-doc "Return the largest client ID, in bytes, that the subscription table can store.".
-spec max_client_id_length() -> pos_integer().
max_client_id_length() -> ?MAX_CLIENT_ID_LENGTH.

-doc "Run a fixed statement and normalize driver results.".
-spec query(connection(), mysql:query(), [mysql:query_param()]) ->
    {ok, [tuple()] | non_neg_integer()}.
query(Conn, SQL, Params) ->
    Result =
        case Params of
            [] -> mysql:query(Conn, SQL);
            _ -> mysql:query(Conn, SQL, Params)
        end,
    case Result of
        {ok, _Columns, Rows} -> {ok, [list_to_tuple(Row) || Row <- Rows]};
        ok -> {ok, mysql:affected_rows(Conn)};
        {error, Reason} -> error({mysql, Reason})
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

-doc "Run `Fun` in a transaction. The driver restarts it after a deadlock.".
transaction(Conn, Fun) ->
    case mysql:transaction(Conn, Fun, ?TRANSACTION_RETRIES) of
        {atomic, Result} -> Result;
        {aborted, Reason} -> error({mysql_transaction, Reason})
    end.

-doc """
Lock the topic row in share mode, and create the row for a new topic first.
Like `FOR SHARE` in PostgreSQL, the lock blocks appends to the topic until the transaction
commits, but not other subscriptions. An upsert of an existing row would take an exclusive
lock, so the row is read first and inserted only when it is missing.
""".
share_lock_topic_tx(Conn, Namespace, Topic) ->
    case select_topic_tx(Conn, Namespace, Topic) of
        {ok, Row} ->
            Row;
        not_found ->
            %% A concurrent creation of the same row turns this insert into a no-op.
            {ok, _} = query(Conn, """
            INSERT INTO mqtt_omp_topic(namespace, topic) VALUES (?, ?)
            ON DUPLICATE KEY UPDATE topic_id = topic_id
            """, [Namespace, Topic]),
            {ok, Row} = select_topic_tx(Conn, Namespace, Topic),
            Row
    end.

select_topic_tx(Conn, Namespace, Topic) ->
    case query(Conn, """
    SELECT topic_id, next_offset FROM mqtt_omp_topic
    WHERE namespace = ? AND topic = ? LOCK IN SHARE MODE
    """, [Namespace, Topic]) of
        {ok, [{Id, Next}]} -> {ok, {Id, Next}};
        {ok, []} -> not_found
    end.

-doc "Call `omp_append` and repeat the call after a deadlock rolled it back.".
call_append(Conn, SQL, Retries) ->
    case mysql:query(Conn, SQL) of
        {ok, _Columns, [[TopicId, First]]} -> {ok, {TopicId, First}};
        {error, {?ER_LOCK_DEADLOCK, _, _}} when Retries > 0 -> call_append(Conn, SQL, Retries - 1);
        {error, Reason} -> error({mysql, Reason})
    end.

-doc "Flatten an encoded message to the scalar-only JSON object read by `omp_append`.".
append_message(#{
    message_id := MessageId,
    sender := Sender,
    qos := QoS,
    retain := Retain,
    payload := Payload,
    properties := Properties,
    published_at := Published,
    expires_at := Expiry
}) ->
    Message = #{
        message_id => MessageId,
        sender => Sender,
        qos => QoS,
        retain => retain_value(Retain),
        payload => Payload,
        properties => emqx_utils_json:encode(Properties),
        published_at => Published
    },
    case Expiry of
        null -> Message;
        _ -> Message#{expires_at => Expiry}
    end.

retain_value(true) -> 1;
retain_value(false) -> 0.

-doc """
Take the database-wide installation lock, waiting at most `Timeout` milliseconds.
GET_LOCK waits whole seconds, so the wait is rounded up. The statement gets one more second,
so that the driver does not time out before the lock wait ends.
""".
lock_schema(Conn, Timeout) ->
    Seconds = max(1, ceil(Timeout / 1000)),
    SQL = "SELECT GET_LOCK(SHA2(CONCAT(DATABASE(), ':mqtt_omp_schema'), 256), ?)",
    case mysql:query(Conn, SQL, [Seconds], Seconds * 1000 + 1000) of
        {ok, _Columns, [[1]]} -> ok;
        {ok, _Columns, [[_TimeoutOrNull]]} -> error(schema_lock_timeout);
        {error, Reason} -> error({mysql, Reason})
    end.

-doc "Return `true` when the schema version and the append procedure are present.".
is_installed(Conn) ->
    try health(Conn) of
        ok -> true
    catch
        error:_ -> false
    end.

-doc """
Split a schema file into statements like the mysql client.
A statement ends with the current delimiter at the end of a line.
A `DELIMITER` line changes the delimiter.
""".
statements(SQL) ->
    statements(binary:split(SQL, <<"\n">>, [global]), <<";">>, [], []).

statements([], _Delimiter, Lines, Statements) ->
    <<>> = string:trim(iolist_to_binary(Lines)),
    lists:reverse(Statements);
statements([Line | Rest], Delimiter, Lines, Statements) ->
    case string:trim(Line) of
        <<"DELIMITER ", New/binary>> ->
            statements(Rest, string:trim(New), Lines, Statements);
        Trimmed ->
            case binary:longest_common_suffix([Trimmed, Delimiter]) =:= byte_size(Delimiter) of
                true ->
                    Body = binary:part(Trimmed, 0, byte_size(Trimmed) - byte_size(Delimiter)),
                    Statement = iolist_to_binary(
                        lists:join(<<"\n">>, lists:reverse([Body | Lines]))
                    ),
                    statements(Rest, Delimiter, [], [Statement | Statements]);
                false ->
                    statements(Rest, Delimiter, [Line | Lines], Statements)
            end
    end.

normalize_row({_, null, _, _, _, _, _, _, _, _} = Row) ->
    Row;
normalize_row({Tip, Offset, Id, Sender, QoS, Retain, Payload, Properties, Published, Expiry}) ->
    {Tip, Offset, Id, Sender, QoS, Retain =:= 1, Payload, Properties, Published, Expiry}.

trim(Conn, TopicId, Cutoff, Batch, Removed) ->
    {ok, Count} = query(Conn, """
    DELETE FROM mqtt_omp_message WHERE topic_id = ? AND msg_offset < ?
    ORDER BY msg_offset LIMIT ?
    """, [TopicId, Cutoff, Batch]),
    case Count < Batch of
        true -> {ok, Removed + Count};
        false -> trim(Conn, TopicId, Cutoff, Batch, Removed + Count)
    end.
