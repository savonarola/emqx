%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_backend).
-moduledoc "Storage contract for ordered offline messages and independent subscriber progress.".

-export_type([
    config/0,
    namespace/0,
    topic_id/0,
    offset/0,
    start_position/0,
    encoded_message/0,
    page_row/0,
    subscription_row/0
]).

-type config() :: #{backend := module(), backend_id := postgresql | mysql, atom() => term()}.
-type namespace() :: binary().
-type topic_id() :: pos_integer().
-type offset() :: non_neg_integer().
-type start_position() :: earliest | latest.
-type encoded_message() :: #{
    message_id := binary(),
    sender := binary(),
    qos := emqx_types:qos(),
    retain := boolean(),
    payload := binary(),
    properties := map(),
    published_at := integer(),
    expires_at := integer() | null
}.
%% An empty page has the current tip and null message fields.
-type page_row() :: {
    _Tip :: offset(),
    _Offset :: offset() | null,
    _MessageId :: binary() | null,
    _Sender :: binary() | null,
    _QoS :: emqx_types:qos() | null,
    _Retain :: boolean() | null,
    _Payload :: binary() | null,
    _Properties :: binary() | null,
    _PublishedAt :: integer() | null,
    _ExpiresAt :: integer() | null
}.
-type subscription_row() :: {emqx_types:topic(), binary()}.
-type connection() :: pid().
-type result(T) :: {ok, T} | {error, term()}.

-callback connect(config()) -> result(connection()).
-callback close(connection()) -> term().
-callback install(config()) -> ok.
-callback health(connection()) -> ok.
%% Appends commit atomically and allocate consecutive offsets. Rollbacks do not consume offsets.
-callback append(connection(), namespace(), emqx_types:topic(), [encoded_message(), ...]) ->
    result({topic_id(), offset()}).
%% Subscription creation serializes with appends so consumed cleanup cannot pass its start cursor.
-callback subscribe(
    connection(),
    namespace(),
    emqx_types:clientid(),
    emqx_types:topic(),
    emqx_types:subopts(),
    start_position()
) -> result(#{topic_id := topic_id(), cursor := offset()}).
-callback subscription_cursor(connection(), namespace(), emqx_types:clientid(), emqx_types:topic()) ->
    result(#{topic_id := topic_id(), cursor := offset()} | not_found).
-callback restore(connection(), namespace(), emqx_types:clientid()) -> result([subscription_row()]).
-callback unsubscribe(connection(), namespace(), emqx_types:clientid(), emqx_types:topic()) ->
    result(non_neg_integer()).
%% Checkpoints never move progress backwards.
-callback checkpoint(connection(), namespace(), emqx_types:clientid(), topic_id(), offset()) ->
    result(non_neg_integer()).
-callback page(connection(), topic_id(), offset(), pos_integer()) -> result([page_row()]).
-callback topics(connection(), namespace(), topic_id() | 0, pos_integer()) ->
    result([{topic_id(), offset()}]).
-callback age_cutoff(connection(), topic_id(), offset(), integer()) -> result(offset()).
-callback consumed_cutoff(connection(), topic_id(), offset()) -> result(offset()).
-callback trim(connection(), topic_id(), offset(), pos_integer()) -> result(non_neg_integer()).
%% The largest client ID, in bytes, that the backend can store.
-callback max_client_id_length() -> pos_integer() | infinity.
