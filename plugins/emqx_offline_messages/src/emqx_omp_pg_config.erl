%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_config).
-moduledoc "Validate the PostgreSQL plugin configuration.".
-behaviour(hocon_schema).
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-export([namespace/0, roots/0, fields/1, desc/1, parse/1]).

-export_type([config/0, raw_config/0]).

%% Configuration as stored in the plugin configuration, with binary keys.
-type raw_config() :: #{binary() => term()}.

-type writer_config() :: #{
    pool_size := pos_integer(),
    min_flush_timeout := pos_integer(),
    max_flush_timeout := pos_integer(),
    flush_max_messages := pos_integer(),
    flush_max_bytes := pos_integer(),
    call_timeout := pos_integer()
}.

-type read_config() :: #{
    page_max_messages := pos_integer(),
    reorder_max_distance := pos_integer(),
    reorder_timeout := pos_integer(),
    idle_poll_interval := pos_integer()
}.

-type checkpoint_config() :: #{interval := pos_integer(), max_pending := pos_integer()}.

-type retention_config() :: #{
    max_age := pos_integer(), max_messages := pos_integer(), interval := pos_integer()
}.

%% Validated configuration. `instance` changes on every parse and identifies the running backend.
-type config() :: #{
    enable := boolean(),
    host := binary(),
    port := inet:port_number(),
    database := binary(),
    username := binary(),
    password := binary(),
    ssl := map(),
    namespace := emqx_omp_pg_sql:namespace(),
    topics := [emqx_types:topic()],
    init_default_schema := boolean(),
    async_batched := boolean(),
    max_topic_length := pos_integer(),
    new_subscription_start := emqx_omp_pg_sql:start_position(),
    query_timeout := pos_integer(),
    %% How long the client waits for a statement reply.
    request_timeout := pos_integer(),
    set_statement_timeout := boolean(),
    message_publish_pool_size := pos_integer(),
    message_read_pool_size := pos_integer(),
    subscription_meta_pool_size := pos_integer(),
    writer := writer_config(),
    read := read_config(),
    checkpoint := checkpoint_config(),
    retention := retention_config(),
    subscription_expiry := pos_integer(),
    connection := epgsql:connect_opts_map(),
    backend := emqx_omp_pg_sql,
    backend_id := postgresql,
    instance := reference()
}.

namespace() -> omp_pg.
roots() -> [config].
desc(_) -> undefined.

fields(config) ->
    [
        {enable, mk(boolean(), false)},
        {host, mk(binary(), <<"127.0.0.1">>)},
        {port, mk(range(1, 65535), 5432)},
        {database, mk(binary(), <<"mqtt">>)},
        {username, mk(binary(), <<"root">>)},
        {password, hoconsc:mk(binary(), #{default => <<"public">>, sensitive => true})},
        {ssl, hoconsc:mk(hoconsc:ref(emqx_schema, "ssl_client_opts"), #{})},
        {set_statement_timeout, mk(boolean(), true)}
    ] ++ emqx_omp_config:fields(config).

mk(Type, Default) -> hoconsc:mk(Type, #{default => Default}).

-doc "Validate limits and build the database connection options with secure TLS defaults.".
-spec parse(raw_config()) -> config().
parse(Raw) ->
    Config0 = emqx_omp_config:parse(?MODULE, Raw),
    #{
        host := Host,
        port := Port,
        username := Username,
        password := Password,
        database := Database,
        ssl := SSL,
        query_timeout := Timeout
    } = Config0,
    %% Build the epgsql connection options.
    SSLMode =
        case SSL of
            #{enable := true} -> required;
            _ -> false
        end,
    ClientSSL =
        case SSL of
            #{server_name_indication := "auto"} -> SSL#{server_name_indication := Host};
            _ -> SSL
        end,
    Connection = #{
        host => binary_to_list(Host),
        port => Port,
        username => binary_to_list(Username),
        password => binary_to_list(Password),
        database => binary_to_list(Database),
        ssl => SSLMode,
        ssl_opts => emqx_tls_lib:to_client_opts(ClientSSL),
        timeout => Timeout
    },
    Config0#{
        connection => Connection,
        %% The server statement timeout ends slow statements, so the client waits as long.
        request_timeout => Timeout,
        instance => make_ref(),
        backend => emqx_omp_pg_sql,
        backend_id => postgresql
    }.
