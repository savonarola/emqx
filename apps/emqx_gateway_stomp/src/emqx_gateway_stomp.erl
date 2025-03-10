%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc The Stomp Gateway implement
-module(emqx_gateway_stomp).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/include/emqx_gateway.hrl").

%% define a gateway named stomp
-gateway(#{
    name => stomp,
    callback_module => ?MODULE,
    config_schema_module => emqx_stomp_schema
}).

%% callback_module must implement the emqx_gateway_impl behaviour
-behaviour(emqx_gateway_impl).

%% callback for emqx_gateway_impl
-export([
    on_gateway_load/2,
    on_gateway_update/3,
    on_gateway_unload/2
]).

-import(
    emqx_gateway_utils,
    [
        normalize_config/1,
        start_listeners/4,
        stop_listeners/2,
        update_listeners/5
    ]
).

-define(MOD_CFG, #{
    frame_mod => emqx_stomp_frame,
    chann_mod => emqx_stomp_channel
}).

%%--------------------------------------------------------------------
%% emqx_gateway_impl callbacks
%%--------------------------------------------------------------------

on_gateway_load(
    _Gateway = #{
        name := GwName,
        config := Config
    },
    Ctx
) ->
    Listeners = normalize_config(Config),
    case
        start_listeners(
            Listeners, GwName, Ctx, ?MOD_CFG
        )
    of
        {ok, ListenerPids} ->
            %% FIXME: How to throw an exception to interrupt the restart logic ?
            %% FIXME: Assign ctx to GwState
            {ok, ListenerPids, _GwState = #{ctx => Ctx}};
        {error, {Reason, Listener}} ->
            throw(
                {badconf, #{
                    key => listeners,
                    value => Listener,
                    reason => Reason
                }}
            )
    end.

on_gateway_update(Config, Gateway = #{config := OldConfig}, GwState = #{ctx := Ctx}) ->
    GwName = maps:get(name, Gateway),
    try
        OldListeners = normalize_config(OldConfig),
        NewListeners = normalize_config(Config),
        Res = update_listeners(NewListeners, OldListeners, GwName, Ctx, ?MOD_CFG),
        ?SLOG(info, #{
            msg => "update_gateway_result",
            result => Res
        }),
        NewPids = lists:map(fun({_, Pid}) -> Pid end, maps:get(added, Res, [])),
        {ok, NewPids, GwState}
    catch
        Class:Reason:Stk ->
            logger:error(
                "Failed to update ~ts; "
                "reason: {~0p, ~0p} stacktrace: ~0p",
                [GwName, Class, Reason, Stk]
            ),
            {error, Reason}
    end.

on_gateway_unload(
    _Gateway = #{
        name := GwName,
        config := Config
    },
    _GwState
) ->
    Listeners = normalize_config(Config),
    stop_listeners(GwName, Listeners).
