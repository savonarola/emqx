%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_tokio_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        #{
            id => emqx_tokio_runtime,
            start => {emqx_tokio_runtime, start_link, []},
            shutdown => 15000
        },
        #{
            id => emqx_tokio_pool,
            start => {emqx_tokio_pool, start_link, []},
            type => supervisor,
            shutdown => infinity
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
