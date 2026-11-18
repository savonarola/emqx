%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_store).
-moduledoc """
Backend-scoped connection pools, storage operations and versioned MQTT row encoding.

The backend uses three pools:

- `message_publish` appends messages. The topic selects the connection.
- `message_read` reads message pages. The subscription handler selects the connection.
- `subscription_meta` changes subscription rows. The client ID selects the connection,
  so the operations of one client run in order.
""".

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_omp.hrl").

-export([pool_specs/1, connect/1, install/1, encode/1, decode/3, dispatch/3, pool/2]).
-export([
    health/1,
    append/3,
    append_sync/3,
    append_batch/4,
    restore/3,
    unsubscribe/4,
    checkpoint/5,
    subscribe_async/8,
    page_async/7
]).
-export([exec/4, exec_async/4, reply/3]).

-define(MESSAGE_PUBLISH_POOL, message_publish).
-define(MESSAGE_READ_POOL, message_read).
-define(SUBSCRIPTION_META_POOL, subscription_meta).

-type config() :: emqx_omp_backend:config().
-type namespace() :: emqx_omp_backend:namespace().
-type topic_id() :: emqx_omp_backend:topic_id().
-type offset() :: emqx_omp_backend:offset().
-type error() :: {error, term()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Return the child specs of the publish, read and subscription connection pools.".
-spec pool_specs(config()) -> [supervisor:child_spec()].
pool_specs(
    #{
        message_publish_pool_size := Publish,
        message_read_pool_size := Read,
        subscription_meta_pool_size := Meta
    } = Config
) ->
    [
        pool_spec(?MESSAGE_PUBLISH_POOL, Publish, Config),
        pool_spec(?MESSAGE_READ_POOL, Read, Config),
        pool_spec(?SUBSCRIPTION_META_POOL, Meta, Config)
    ].

-doc "Open a connection and set its statement timeout. The pools call this to start workers.".
-spec connect(config() | [{config, config()} | term()]) -> {ok, pid()} | error().
connect(Options) when is_list(Options) ->
    connect(proplists:get_value(config, Options));
connect(#{backend := Backend} = Config) ->
    Backend:connect(Config).

-doc "Install the packaged schema using a connection that closes after the operation.".
-spec install(config()) -> ok.
install(#{backend := Backend} = Config) ->
    Backend:install(Config).

-doc "Check that the database is reachable and has the expected schema version.".
-spec health(config()) -> ok | error().
health(Config) ->
    call(?SUBSCRIPTION_META_POOL, health, health, [], Config).

-doc """
Admit one message to the topic's batch writer. The writer stores the batch and sends
its live copies after the commit.
""".
-spec append(namespace(), emqx_types:message(), config()) -> ok | error().
append(Namespace, Msg, Config) ->
    emqx_omp_writer:append(Namespace, Msg, Config).

-doc "Store one message, wait for the commit and send its live copy.".
-spec append_sync(namespace(), emqx_types:message(), config()) -> ok | error().
append_sync(Namespace, #message{topic = Topic} = Msg, #{backend_id := BackendId} = Config) ->
    case append_batch(Namespace, Topic, [encode(Msg)], Config) of
        {ok, {Id, Offset}} -> dispatch([Msg], {{BackendId, Namespace}, Id}, Offset);
        {error, _} = Error -> Error
    end.

-doc "Append encoded messages to `Topic`, wait for the commit and return the first offset.".
-spec append_batch(
    namespace(), emqx_types:topic(), [emqx_omp_backend:encoded_message()], config()
) -> {ok, {topic_id(), offset()}} | error().
append_batch(Namespace, Topic, Messages, Config) ->
    call(?MESSAGE_PUBLISH_POOL, Topic, append, [Namespace, Topic, Messages], Config).

-doc "Return the subscriptions of a client with their topics and options.".
-spec restore(namespace(), emqx_types:clientid(), config()) ->
    {ok, [emqx_omp_backend:subscription_row()]} | error().
restore(Namespace, ClientId, Config) ->
    call(?SUBSCRIPTION_META_POOL, ClientId, restore, [Namespace, ClientId], Config).

-doc "Delete the subscription of a client to a topic and wait for the result.".
-spec unsubscribe(namespace(), emqx_types:clientid(), emqx_types:topic(), config()) ->
    {ok, non_neg_integer()} | error().
unsubscribe(Namespace, ClientId, Topic, Config) ->
    call(?SUBSCRIPTION_META_POOL, ClientId, unsubscribe, [Namespace, ClientId, Topic], Config).

-doc "Save acknowledged progress without waiting for the result.".
-spec checkpoint(namespace(), emqx_types:clientid(), topic_id(), offset(), config()) -> ok.
checkpoint(Namespace, ClientId, TopicId, Offset, Config) ->
    cast(
        ?SUBSCRIPTION_META_POOL,
        ClientId,
        checkpoint,
        [Namespace, ClientId, TopicId, Offset],
        Config
    ).

-doc "Create or update a subscription and send the result to the handler.".
-spec subscribe_async(
    namespace(),
    emqx_types:clientid(),
    emqx_types:topic(),
    emqx_types:subopts(),
    emqx_omp_backend:start_position(),
    emqx_extsub_handler:message_target(),
    reference(),
    config()
) -> ok.
subscribe_async(Namespace, ClientId, Topic, SubOpts, Start, Target, Ref, Config) ->
    Args = [Namespace, ClientId, Topic, SubOpts, Start],
    async(?SUBSCRIPTION_META_POOL, ClientId, subscribe, Args, Target, Ref, Config).

-doc "Read a message page and send the result to the handler.".
-spec page_async(
    term(),
    topic_id(),
    offset(),
    pos_integer(),
    emqx_extsub_handler:message_target(),
    reference(),
    config()
) -> ok.
page_async(Key, TopicId, Cursor, Count, Target, Ref, Config) ->
    ?tp(omp_read_page, #{topic_id => TopicId, cursor => Cursor, count => Count}),
    async(?MESSAGE_READ_POOL, Key, page, [TopicId, Cursor, Count], Target, Ref, Config).

-doc "Run a statement on a pool connection and return errors instead of raising them.".
-spec exec(pid(), module(), atom(), [term()]) -> term().
exec(Conn, Backend, Fun, Args) ->
    try
        apply(Backend, Fun, [Conn | Args])
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

-doc "Run a statement and wrap its result, because the pool skips the callback for `{error, _}`.".
-spec exec_async(pid(), module(), atom(), [term()]) -> {done, term()}.
exec_async(Conn, Backend, Fun, Args) ->
    {done, exec(Conn, Backend, Fun, Args)}.

-doc "Send the result of an asynchronous statement to the handler.".
-spec reply({done, term()}, emqx_extsub_handler:message_target(), reference()) -> ok.
reply({done, Result}, Target, Ref) ->
    emqx_extsub_handler:send(Target, {result, Ref, Result}).

-doc "Send a committed topic batch as one term, with its first assigned offset.".
-spec dispatch([emqx_types:message(), ...], {{atom(), namespace()}, topic_id()}, offset()) -> ok.
dispatch([#message{topic = Topic} | _] = Messages, {Namespace, Id}, Offset) ->
    emqx_broker:dispatch_info(Topic, #omp_live{
        namespace = Namespace, topic_id = Id, offset = Offset, messages = Messages
    }).

-doc "Encode MQTT fields and binary properties for the versioned JSON append format.".
-spec encode(emqx_types:message()) -> emqx_omp_backend:encoded_message().
encode(#message{
    id = Id,
    from = From,
    qos = QoS,
    flags = Flags,
    payload = Payload,
    timestamp = Published,
    headers = Headers
}) ->
    Props = maps:get(properties, Headers, #{}),
    Expiry =
        case Props of
            #{'Message-Expiry-Interval' := Seconds} -> Published + Seconds * 1000;
            _ -> null
        end,
    #{
        message_id => base64:encode(Id),
        sender => sender(From),
        qos => QoS,
        retain => maps:get(retain, Flags, false),
        payload => base64:encode(Payload),
        properties => #{version => 1, values => encode_properties(Props)},
        published_at => Published,
        expires_at => Expiry
    }.

-doc "Decode a stored row and validate its MQTT properties before replay.".
-spec decode(emqx_types:topic(), {{atom(), namespace()}, topic_id()}, emqx_omp_backend:page_row()) ->
    {ok, emqx_types:message()} | {error, invalid_row}.
decode(
    Topic,
    {Namespace, TopicId},
    {_, Offset, Id, Sender, _QoS, Retain, Payload, Properties, Published, Expiry}
) ->
    try
        #{<<"version">> := 1, <<"values">> := Values} = emqx_utils_json:decode(Properties),
        Props0 = decode_properties(Values),
        Props =
            case Expiry of
                null -> Props0;
                _ -> Props0#{'Message-Expiry-Interval' => max(0, (Expiry - Published) div 1000)}
            end,
        ok = emqx_mqtt_props:validate(Props),
        MsgId =
            case Id of
                <<_:128>> -> Id;
                _ -> emqx_guid:gen()
            end,
        Msg = #message{
            id = MsgId,
            from = Sender,
            qos = ?QOS_1,
            topic = Topic,
            payload = Payload,
            timestamp = Published,
            flags = #{retain => Retain},
            headers = #{
                properties => Props,
                sub_topic => Topic,
                ?OMP_POSITION => {Namespace, TopicId, Offset}
            }
        },
        {ok, Msg}
    catch
        _:_ -> {error, invalid_row}
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

pool_spec(Role, Size, Config) ->
    Pool = pool(Role, Config),
    Options = [{pool_size, Size}, {pool_type, hash}, {auto_reconnect, 1}, {config, Config}],
    ecpool:pool_spec(Pool, Pool, ?MODULE, Options).

-doc "Run a statement on the pool worker selected by `Key` and wait for the result.".
call(Role, Key, Fun, Args, #{request_timeout := Timeout, backend := Backend} = Config) ->
    Pool = pool(Role, Config),
    try
        ecpool:pick_and_do(
            {Pool, Key}, {?MODULE, exec, [Backend, Fun, Args]}, {handover, Timeout + 1000}
        )
    catch
        exit:Reason -> {error, Reason}
    end.

cast(Role, Key, Fun, Args, #{backend := Backend} = Config) ->
    Pool = pool(Role, Config),
    _ = ecpool:pick_and_do({Pool, Key}, {?MODULE, exec, [Backend, Fun, Args]}, handover_async),
    ok.

-doc """
Send `{result, Ref, Result}` to the handler. A missing or empty pool fails at once.
The pool sends no result when its connection is down, so the caller needs a timeout.
""".
async(Role, Key, Fun, Args, Target, Ref, #{backend := Backend} = Config) ->
    Pool = pool(Role, Config),
    Callback = {?MODULE, reply, [Target, Ref]},
    case
        ecpool:pick_and_do({Pool, Key}, {?MODULE, exec_async, [Backend, Fun, Args]}, {
            handover_async, Callback
        })
    of
        {error, _} = Error -> reply({done, Error}, Target, Ref);
        _ -> ok
    end.

-doc "Return the backend-scoped pool name.".
-spec pool(atom(), config()) -> binary().
pool(Role, #{backend_id := BackendId}) ->
    <<"omp_", (atom_to_binary(BackendId))/binary, "_", (atom_to_binary(Role))/binary>>.

sender(From) when is_atom(From) -> atom_to_binary(From);
sender(From) when is_binary(From) -> From.

-doc "Encode supported MQTT properties as JSON, including binary and repeated user properties.".
encode_properties(Props) ->
    maps:fold(
        fun
            ('Correlation-Data', Value, Acc) ->
                Acc#{<<"Correlation-Data">> => base64:encode(Value)};
            ('User-Property', Values, Acc) ->
                Acc#{<<"User-Property">> => [[K, V] || {K, V} <- Values]};
            (Key, Value, Acc) ->
                case lists:member(Key, property_keys()) of
                    true -> Acc#{atom_to_binary(Key) => Value};
                    false -> Acc
                end
        end,
        #{},
        Props
    ).

-doc "Restore binary data and ordered user properties from the stored JSON values.".
decode_properties(Values) ->
    maps:fold(
        fun
            (<<"Correlation-Data">>, Value, Acc) ->
                Acc#{'Correlation-Data' => base64:decode(Value)};
            (<<"User-Property">>, Values0, Acc) ->
                Acc#{'User-Property' => lists:map(fun([K, V]) -> {K, V} end, Values0)};
            (Key, Value, Acc) ->
                [Atom] = [K || K <- property_keys(), atom_to_binary(K) =:= Key],
                Acc#{Atom => Value}
        end,
        #{},
        Values
    ).

property_keys() ->
    ['Payload-Format-Indicator', 'Content-Type', 'Response-Topic'].
