%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_pg).
-moduledoc "PostgreSQL configuration entry point for the shared offline message runtime.".
-export([on_config_changed/2, on_health_check/1, config/0, start/1, stop/0]).

on_config_changed(Old, New) -> emqx_omp:on_config_changed(emqx_omp_pg_config, Old, New).
on_health_check(Raw) -> emqx_omp:on_health_check(postgresql, Raw).
config() -> emqx_omp:config(postgresql).
start(Config) -> emqx_omp:start(Config).
stop() -> emqx_omp:stop(postgresql).
