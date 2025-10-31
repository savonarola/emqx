%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_app).

-behaviour(application).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start/2, stop/1]).

%% Behaviour callbacks

start(_StartType, _StartArgs) ->
    ?tp(warning, emqx_extsub_app_start, #{}),
    ok = emqx_extsub_handler:init(),
    {ok, Sup} = emqx_extsub_sup:start_link(),
    ok = emqx_extsub:register_hooks(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_extsub:unregister_hooks(),
    ok.
