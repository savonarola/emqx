%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_app).

-behaviour(application).

-export([start/2, stop/1]).

%% Behaviour callbacks

start(_StartType, _StartArgs) ->
    ct:print("emqx_extsub_app: start~n"),
    ok = emqx_extsub_handler:init(),
    {ok, Sup} = emqx_extsub_sup:start_link(),
    ok = emqx_extsub:register_hooks(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_extsub:unregister_hooks(),
    ok.
