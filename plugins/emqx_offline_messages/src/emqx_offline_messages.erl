%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages).

-include("emqx_offline_messages.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(gen_server).

%% API
-export([
    current_config/0,
    start_link/0,
    child_spec/0,
    validate_backends/1
]).

%% Plugin callbacks
-export([
    on_config_changed/2,
    on_health_check/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 15000).

%%--------------------------------------------------------------------
%% Events
%%--------------------------------------------------------------------

-record(state, {}).

-record(on_config_changed, {
    old_conf :: map(),
    new_conf :: map()
}).

-record(on_health_check, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?SERVER,
        start => {?MODULE, start_link, []},
        type => worker,
        modules => [?MODULE],
        restart => permanent,
        shutdown => ?TIMEOUT
    }.

%%--------------------------------------------------------------------
%% EMQX Plugin callbacks
%%--------------------------------------------------------------------

on_config_changed(OldConf, NewConf) ->
    try
        gen_server:call(
            ?SERVER, #on_config_changed{old_conf = OldConf, new_conf = NewConf}, ?TIMEOUT
        )
    catch
        exit:{noproc, _} ->
            ok
    end.

on_health_check() ->
    try
        gen_server:call(?SERVER, #on_health_check{}, ?TIMEOUT)
    catch
        exit:{noproc, _} ->
            {error, <<"Plugin is not running">>}
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(map()) -> {ok, map()}.
init([]) ->
    erlang:process_flag(trap_exit, true),
    ok = init_metrics(),
    ok = handle_on_config_changed(#{}, current_config()),
    {ok, #state{}}.

handle_call(#on_config_changed{old_conf = OldConf, new_conf = NewConf}, _From, State) ->
    {reply, handle_on_config_changed(OldConf, NewConf), State};
handle_call(#on_health_check{}, _From, State) ->
    {reply, handle_on_health_check(), State};
handle_call(Request, From, State) ->
    ?SLOG(error, #{
        msg => "offline_messages_unexpected_call", request => Request, from => From
    }),
    {reply, {error, unexpected_call}, State}.

handle_cast(Request, State) ->
    ?SLOG(error, #{
        msg => "offline_messages_unexpected_cast", request => Request
    }),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{
        msg => "offline_messages_unexpected_info", info => Info
    }),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = handle_on_config_changed(current_config(), #{}),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec handle_on_config_changed(map(), map()) -> ok | {error, term()}.
handle_on_config_changed(OldConf, NewConf) ->
    case validate_backends(NewConf) of
        ok -> apply_config_changed(OldConf, NewConf);
        {error, _} = Error -> Error
    end.

-doc "Reject intersecting topic filters before changing either storage runtime.".
-spec validate_backends(map()) -> ok | {error, term()}.
validate_backends(Config) ->
    Enabled = [
        {Id, maps:get(<<"topics">>, Raw, [])}
     || Id <- [<<"postgresql">>, <<"mysql">>],
        #{<<"enable">> := true} = Raw <- [maps:get(Id, Config, #{})]
    ],
    Overlaps = [
        {Left, Right, A, B}
     || {Left, FiltersA} <- Enabled,
        {Right, FiltersB} <- Enabled,
        Left < Right,
        A <- FiltersA,
        B <- FiltersB,
        emqx_topic:intersection(A, B) =/= false
    ],
    case Overlaps of
        [] -> ok;
        _ -> {error, {overlapping_backend_topics, Overlaps}}
    end.

apply_config_changed(OldConf, NewConf) ->
    ?SLOG(info, #{
        msg => "offline_messages_config_changed", old_conf => OldConf, new_conf => NewConf
    }),
    DefaultConf = #{<<"enable">> => false},
    Section = fun(Key, Conf) -> maps:get(Key, Conf, DefaultConf) end,
    SQLChanges = [
        {emqx_omp_pg_config, Section(<<"postgresql">>, OldConf), Section(<<"postgresql">>, NewConf)}
    ],
    %% Parse every backend first, so that an invalid configuration changes nothing.
    maybe
        {ok, SQLUpdates} ?= emqx_omp:parse_changes(SQLChanges),
        {ok, RedisChange} ?=
            emqx_offline_messages_redis:parse_change(
                Section(<<"redis">>, OldConf), Section(<<"redis">>, NewConf)
            ),
        %% Stop every changed backend before any starts, so that a topic moved between
        %% backends never has two owners.
        ok = emqx_omp:stop_changes(SQLUpdates),
        ok = emqx_offline_messages_redis:stop_change(RedisChange),
        _ = emqx_omp:start_changes(SQLUpdates),
        ok = emqx_offline_messages_redis:start_change(RedisChange),
        emqx_offline_messages_mysql:on_config_changed(
            Section(<<"mysql">>, OldConf), Section(<<"mysql">>, NewConf)
        )
    end.

handle_on_health_check() ->
    Config = current_config(),
    DefaultConf = #{<<"enable">> => false},
    MysqlConf = maps:get(<<"mysql">>, Config, DefaultConf),
    RedisConf = maps:get(<<"redis">>, Config, DefaultConf),
    MysqlStatus = emqx_offline_messages_mysql:on_health_check(MysqlConf),
    RedisStatus = emqx_offline_messages_redis:on_health_check(RedisConf),
    PgStatus = emqx_offline_messages_pg:on_health_check(
        maps:get(<<"postgresql">>, Config, DefaultConf)
    ),
    Errors =
        status_to_error_list(MysqlStatus) ++ status_to_error_list(RedisStatus) ++
            status_to_error_list(PgStatus),
    case Errors of
        [] ->
            ok;
        Errors ->
            {error, iolist_to_binary(lists:join(",", Errors))}
    end.

status_to_error_list(ok) -> [];
status_to_error_list({error, Error}) -> [Error].

current_config() ->
    emqx_plugins:get_config(name_vsn(), #{}).

name_vsn() ->
    {ok, Vsn} = application:get_key(?PLUGIN_NAME, vsn),
    iolist_to_binary([atom_to_binary(?PLUGIN_NAME), <<"-">>, Vsn]).

init_metrics() ->
    ?SLOG(info, #{msg => "offline_messages_init_metrics"}),
    emqx_metrics_worker:create_metrics(
        ?METRICS_WORKER, message_acked, [success, fail]
    ),
    emqx_metrics_worker:create_metrics(
        ?METRICS_WORKER, session_subscribed, [success, fail]
    ),
    ok.
