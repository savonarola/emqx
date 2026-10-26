%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_tokio_runtime).
-behaviour(gen_server).
-include_lib("emqx/include/logger.hrl").
-export([start_link/0, resource/0, limits/0, stop_listeners/0]).
-export([start_listener/2, stop_listener/2, restart_listener/2, is_running/1, config_update/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-spec start_link() -> gen_server:start_ret().
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec resource() -> reference().
resource() -> persistent_term:get({?MODULE, resource}).

-spec limits() -> map().
limits() ->
    gen_server:call(?MODULE, limits, 5000).

configured_limits() ->
    Defaults = #{
        representatives => erlang:system_info(schedulers),
        queue_limit => 8192,
        batch_size => 256,
        output_limit => 1024,
        max_packet_size => 1048576,
        worker_threads => erlang:system_info(schedulers)
    },
    maps:map(fun(Key, Default) -> application:get_env(emqx_tokio, Key, Default) end, Defaults).

-spec start_listener(atom(), map()) -> ok | {error, term()}.
start_listener(Id, Conf) -> call({apply, Id, Conf, false}).

-spec restart_listener(atom(), map()) -> ok | {error, term()}.
restart_listener(Id, Conf) -> call({apply, Id, Conf, true}).

-spec stop_listener(atom(), map()) -> ok | {error, term()}.
stop_listener(Id, Conf) -> call({apply, Id, Conf#{enable => false}, false}).

-spec is_running(atom()) -> boolean() | {error, not_available}.
is_running(Id) -> call({is_running, Id}).

-spec stop_listeners() -> ok | {error, term()}.
stop_listeners() -> call(stop_listeners).

-spec config_update(before | 'after', listeners | gateway, map()) -> ok | {error, not_available}.
config_update(Phase, Root, Conf) -> call({config_update, Phase, Root, Conf}).

call(Request) ->
    case whereis(?MODULE) of
        undefined -> {error, not_available};
        Pid -> gen_server:call(Pid, Request, 30000)
    end.

init([]) ->
    process_flag(trap_exit, true),
    ok = stop_previous_runtime(),
    Limits = configured_limits(),
    {ok, Runtime} = emqx_tokio_nif:native_new(Limits),
    persistent_term:put({?MODULE, resource}, Runtime),
    ok = emqx_broker:set_native_dispatch_enabled(true),
    ok = emqx_listeners:register_native(?MODULE),
    ok = ekka:monitor(membership),
    ok = update_forwarding(Runtime, current_config()),
    maps:foreach(
        fun
            (Name, Conf) when is_map(Conf) ->
                _ = apply_listener(Runtime, emqx_listeners:listener_id(native, Name), Conf, false);
            (_Name, _Tombstone) ->
                ok
        end,
        emqx:get_config([listeners, native], #{})
    ),
    {ok, #{runtime => Runtime, limits => Limits}}.

handle_call(limits, _From, #{limits := Limits} = State) ->
    {reply, Limits, State};
handle_call({apply, Id, Conf, Restart}, _From, #{runtime := Runtime} = State) ->
    {reply, apply_listener(Runtime, Id, Conf, Restart), State};
handle_call({is_running, Id}, _From, #{runtime := Runtime} = State) ->
    Running = await(emqx_tokio_nif:listener_status(Runtime, self())),
    {reply, lists:member(atom_to_binary(Id), Running), State};
handle_call(stop_listeners, _From, #{runtime := Runtime} = State) ->
    {reply, await(emqx_tokio_nif:listeners_stop(Runtime, self())), State};
handle_call({config_update, Phase, Root, Conf}, _From, #{runtime := Runtime} = State) ->
    Config0 = current_config(),
    Config = Config0#{Root => Conf},
    Enabled = need_forwarding(Config),
    case Phase =:= 'after' orelse Enabled of
        true -> ok = emqx_tokio_nif:forwarding(Runtime, Enabled);
        false -> ok
    end,
    {reply, ok, State}.

handle_cast(_Request, State) -> {noreply, State}.

handle_info({membership, _Event}, #{runtime := Runtime} = State) ->
    ok = update_forwarding(Runtime, current_config()),
    {noreply, State};
handle_info({native_listener_error, Id, Reason}, State) ->
    ?SLOG(error, #{msg => "native_listener_stopped", listener => Id, reason => Reason}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{runtime := Runtime}) ->
    ok = emqx_broker:set_native_dispatch_enabled(false),
    ok = emqx_listeners:unregister_native(?MODULE),
    persistent_term:erase({?MODULE, resource}),
    ekka:unmonitor(membership),
    ok = emqx_tokio_nif:runtime_stop(Runtime, self()),
    receive
        runtime_stopped -> ok
    after 10000 -> ok
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

apply_listener(Runtime, Id, Conf, Restart) ->
    Result =
        case normalize(Id, Conf) of
            {ok, Normalized} ->
                await(emqx_tokio_nif:listener_apply(Runtime, Normalized, Restart, self()));
            {error, _} = Error ->
                Error
        end,
    log_listener(Id, Result, Conf),
    Result.

normalize(_Id, #{enable := true} = Conf) when not is_map_key(enable_unauthenticated, Conf) ->
    {error, enable_unauthenticated_required};
normalize(_Id, #{enable := true, enable_unauthenticated := false}) ->
    {error, enable_unauthenticated_required};
normalize(Id, #{bind := Bind, enable := Enable} = Conf) ->
    SSL0 = maps:get(ssl_options, Conf, #{}),
    case ensure_certs(Enable, SSL0) of
        {ok, SSL} ->
            ListenOn = emqx_default_address:listen_on(mqtt, Bind),
            {ok, #{
                id => atom_to_binary(Id),
                bind => iolist_to_binary(emqx_listeners:format_bind(ListenOn)),
                enable => Enable,
                tls => maps:get(enable, SSL, false),
                certfile => path(maps:get(certfile, SSL, "")),
                keyfile => path(maps:get(keyfile, SSL, ""))
            }};
        Error ->
            Error
    end.

ensure_certs(true, #{enable := true} = SSL) -> emqx_tls_lib:ensure_default_certs(SSL);
ensure_certs(_Enable, SSL) -> {ok, SSL}.

path(Path) -> unicode:characters_to_binary(emqx_schema:naive_env_interpolation(Path)).

stop_previous_runtime() ->
    case persistent_term:get({?MODULE, resource}, undefined) of
        undefined ->
            ok;
        Runtime ->
            ok = emqx_tokio_nif:runtime_stop(Runtime, self()),
            receive
                runtime_stopped -> ok
            after 10000 -> exit(runtime_stop_timeout)
            end
    end.

await(Ref) ->
    receive
        {native_result, Ref, Result} -> Result
    after 20000 ->
        exit(native_listener_timeout)
    end.

log_listener(Id, ok, #{enable := Enabled}) ->
    ?SLOG(info, #{msg => "native_listener_updated", listener => Id, enable => Enabled});
log_listener(Id, {error, Reason}, _Conf) ->
    ?SLOG(error, #{msg => "native_listener_update_failed", listener => Id, reason => Reason}).

current_config() ->
    #{listeners => emqx:get_config([listeners], #{}), gateway => emqx:get_config([gateway], #{})}.

update_forwarding(Runtime, Config) ->
    emqx_tokio_nif:forwarding(Runtime, need_forwarding(Config)).

need_forwarding(#{listeners := Listeners, gateway := Gateways}) ->
    enabled_listeners(maps:remove(native, Listeners)) orelse
        lists:any(fun enabled_gateway/1, maps:values(Gateways)) orelse
        emqx:cluster_nodes(running) -- [node()] =/= [].

enabled_gateway(#{enable := false}) -> false;
enabled_gateway(#{listeners := Listeners}) -> enabled_listeners(Listeners);
enabled_gateway(_) -> false.

enabled_listeners(Listeners) ->
    lists:any(
        fun(Names) ->
            lists:any(
                fun
                    (#{enable := true}) -> true;
                    (_) -> false
                end,
                maps:values(Names)
            )
        end,
        maps:values(Listeners)
    ).
