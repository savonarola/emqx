%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_components_app).
-behaviour(application).
-emqx_plugin(?MODULE).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_mqtt_components_sup:start_link(),
    ok = emqx_mqtt_components:hook(),
    {ok, Sup}.

stop(_State) ->
    emqx_mqtt_components:unhook().
