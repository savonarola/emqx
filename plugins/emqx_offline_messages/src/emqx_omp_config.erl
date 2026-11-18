%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_config).
-moduledoc "Shared delivery configuration for offline message storage backends.".
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-export([fields/1, desc/1, parse/2]).

desc(_) -> undefined.

fields(config) ->
    [
        {namespace, mk(binary(), <<>>)},
        {topics, mk(hoconsc:array(binary()), [])},
        {init_default_schema, mk(boolean(), false)},
        {async_batched, mk(boolean(), false)},
        {max_topic_length, mk(range(1, 2048), 1024)},
        {new_subscription_start, mk(hoconsc:enum([earliest, latest]), earliest)},
        {query_timeout, mk(pos_integer(), 5000)},
        {message_publish_pool_size, mk(range(1, 128), 8)},
        {message_read_pool_size, mk(range(1, 128), 8)},
        {subscription_meta_pool_size, mk(range(1, 128), 8)},
        {writer, mk(hoconsc:ref(?MODULE, writer), #{})},
        {read, mk(hoconsc:ref(?MODULE, read), #{})},
        {checkpoint, mk(hoconsc:ref(?MODULE, checkpoint), #{})},
        {retention, mk(hoconsc:ref(?MODULE, retention), #{})}
    ];
fields(writer) ->
    [
        {pool_size, mk(range(1, 128), 4)},
        {min_flush_timeout, mk(pos_integer(), 5)},
        {max_flush_timeout, mk(pos_integer(), 50)},
        {flush_max_messages, mk(pos_integer(), 1000)},
        {flush_max_bytes, mk(pos_integer(), 1048576)},
        {call_timeout, mk(pos_integer(), 5000)}
    ];
fields(read) ->
    [
        {page_max_messages, mk(pos_integer(), 100)},
        {reorder_max_distance, mk(pos_integer(), 32)},
        {reorder_timeout, mk(pos_integer(), 50)},
        {idle_poll_interval, mk(pos_integer(), 30000)}
    ];
fields(checkpoint) ->
    [{interval, mk(pos_integer(), 5000)}, {max_pending, mk(pos_integer(), 100)}];
fields(retention) ->
    [
        {max_age, mk(pos_integer(), 604800000)},
        {max_messages, mk(pos_integer(), 1000000)},
        {interval, mk(pos_integer(), 60000)}
    ].

mk(Type, Default) -> hoconsc:mk(Type, #{default => Default}).

-doc "Validate shared limits and use peer and hostname verification by default.".
-spec parse(module(), map()) -> map().
parse(Schema, Raw) ->
    #{<<"ssl">> := SSL0} = Raw0 = emqx_offline_messages_utils:fix_ssl_config(Raw),
    SSL = maps:merge(
        #{<<"verify">> => <<"verify_peer">>, <<"server_name_indication">> => <<"auto">>}, SSL0
    ),
    Config = emqx_offline_messages_utils:check_config(Schema, Raw0#{<<"ssl">> := SSL}),
    #{
        namespace := Namespace,
        topics := Topics,
        writer := #{min_flush_timeout := Min, max_flush_timeout := Max}
    } = Config,
    true = byte_size(Namespace) =< 256,
    true = Min =< Max,
    lists:foreach(fun(Topic) -> true = emqx_topic:validate(filter, Topic) end, Topics),
    Config.
