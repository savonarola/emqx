%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_tokio_pool).
-behaviour(supervisor).
-export([start_link/0, init/1]).

-spec start_link() -> supervisor:startlink_ret().
start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    #{representatives := Count, batch_size := BatchSize} = emqx_tokio_runtime:limits(),
    Children = [
        #{id => Slot, start => {emqx_tokio_representative, start_link, [Slot, BatchSize]}}
     || Slot <- lists:seq(0, Count - 1)
    ],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 10}, Children}}.
