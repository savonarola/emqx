%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_components_app).
-behaviour(application).
-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_handle_api_call/4]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_mqtt_components_sup:start_link(),
    ok = emqx_mqtt_components:hook(),
    {ok, Sup}.

stop(_State) ->
    emqx_mqtt_components:unhook().

on_handle_api_call(get, [<<"ui">>], _Request, _Context) ->
    File = filename:join(code:priv_dir(emqx_mqtt_components), "index.html"),
    case file:read_file(File) of
        {ok, Html} ->
            {ok, 200,
                #{
                    <<"content-type">> => <<"text/html; charset=utf-8">>,
                    <<"cache-control">> => <<"no-store">>
                },
                Html};
        {error, _Reason} ->
            {error, 500, #{<<"content-type">> => <<"text/plain; charset=utf-8">>},
                <<"Unable to load the demo page">>}
    end;
on_handle_api_call(_Method, _Path, _Request, _Context) ->
    {error, not_found}.
