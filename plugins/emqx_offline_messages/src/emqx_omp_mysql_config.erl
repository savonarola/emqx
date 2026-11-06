%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_mysql_config).
-moduledoc "MySQL connection configuration for the shared offline message runtime.".
-behaviour(hocon_schema).
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-export([namespace/0, roots/0, fields/1, desc/1, parse/1]).

%% Time for a statement to finish its work and reply after its row-lock wait ends.
-define(REPLY_MARGIN, 5000).

namespace() -> omp_mysql.
roots() -> [config].
desc(_) -> undefined.

fields(config) ->
    [
        {enable, mk(boolean(), false)},
        {server, mk(binary(), <<"127.0.0.1:3306">>)},
        {database, mk(binary(), <<"mqtt">>)},
        {username, mk(binary(), <<"root">>)},
        {password, hoconsc:mk(binary(), #{default => <<"public">>, sensitive => true})},
        {ssl, hoconsc:mk(hoconsc:ref(emqx_schema, "ssl_client_opts"), #{})}
    ] ++ emqx_omp_config:fields(config).

mk(Type, Default) -> hoconsc:mk(Type, #{default => Default}).

-doc "Validate MySQL configuration and build driver options with secure TLS defaults.".
-spec parse(map()) -> emqx_omp_backend:config().
parse(Raw) ->
    Config = emqx_omp_config:parse(?MODULE, Raw),
    #{
        server := Server,
        database := Database,
        username := Username,
        password := Password,
        ssl := SSL,
        query_timeout := Timeout
    } = Config,
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, #{default_port => 3306}),
    ClientSSL =
        case SSL of
            #{server_name_indication := "auto"} -> SSL#{server_name_indication := Host};
            _ -> SSL
        end,
    SSLOptions =
        case SSL of
            #{enable := true} -> [{ssl, emqx_tls_lib:to_client_opts(ClientSSL)}];
            _ -> []
        end,
    %% MySQL has no server-side timeout for writes. The server ends row-lock waits after whole
    %% seconds and rolls the statement back. The client waits longer, so that it receives
    %% that rollback instead of giving up without knowing the outcome.
    LockWait = max(1, ceil(Timeout / 1000)),
    RequestTimeout = LockWait * 1000 + ?REPLY_MARGIN,
    Connection = [
        {host, Host},
        {port, Port},
        {user, Username},
        {password, Password},
        {database, Database},
        {connect_timeout, Timeout},
        {query_timeout, RequestTimeout},
        {query_cache_time, 0},
        {try_kill_slow_query, false},
        {queries, [
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET SESSION innodb_lock_wait_timeout = " ++ integer_to_list(LockWait)
        ]}
        | SSLOptions
    ],
    Config#{
        connection => Connection,
        request_timeout => RequestTimeout,
        backend => emqx_omp_mysql_sql,
        backend_id => mysql,
        instance => make_ref()
    }.
