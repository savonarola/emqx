%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_app).

-behaviour(application).

-export([start/2, stop/1]).

%% Behaviour callbacks

start(_StartType, _StartArgs) ->
    ok = emqx_extsub_redis_persist:start(),
    {ok, Sup} = emqx_extsub_redis_sup:start_link(),
    ok = emqx_extsub_redis:register_hooks(),
    ok = emqx_extsub_handler:register(emqx_extsub_redis_handler),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_extsub_redis_persist:stop(),
    ok = emqx_extsub_handler:unregister(emqx_extsub_redis_handler),
    ok = emqx_extsub_redis:unregister_hooks(),
    ok.
