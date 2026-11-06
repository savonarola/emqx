%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_sup).
-moduledoc "Supervise independent offline message backend runtimes.".
-behaviour(supervisor).
-export([start_link/0, start_link/1, start/1, stop/1, init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> supervisor:startlink_ret().
start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).
-spec start_link(emqx_omp_backend:config()) -> supervisor:startlink_ret().
start_link(Config) -> supervisor:start_link(?MODULE, Config).

-doc "Start the backend processes for `Config`.".
-spec start(emqx_omp_backend:config()) -> supervisor:startchild_ret().
start(#{backend_id := BackendId} = Config) ->
    supervisor:start_child(?MODULE, #{
        id => BackendId,
        start => {?MODULE, start_link, [Config]},
        type => supervisor,
        shutdown => infinity
    }).

-doc "Stop the backend processes. The child spec is deleted so that `start/1` can run again.".
-spec stop(atom()) -> ok.
stop(BackendId) ->
    _ = supervisor:terminate_child(?MODULE, BackendId),
    _ = supervisor:delete_child(?MODULE, BackendId),
    ok.

%%------------------------------------------------------------------------------
%% Callbacks
%%------------------------------------------------------------------------------

init([]) ->
    {ok, {#{strategy => one_for_one}, []}};
init(
    #{
        async_batched := Async,
        writer := #{pool_size := Writers, call_timeout := Shutdown}
    } = Config
) ->
    Children =
        emqx_omp_store:pool_specs(Config) ++
            [
                emqx_omp_worker_sup:child_spec(
                    writers, emqx_omp_writer, Writers, Shutdown, Config
                )
             || Async
            ] ++
            [#{id => maintenance, start => {emqx_omp_maintenance, start_link, [Config]}}],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 10}, Children}}.
