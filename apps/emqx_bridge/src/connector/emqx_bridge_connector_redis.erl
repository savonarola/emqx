%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_connector_redis).

-include_lib("emqx/include/logger.hrl").

-export([
    on_start/2,
    on_stop/2,
    on_query/4,
    on_get_status/2
]).

on_start(
    InstId,
    #{egress := Egress} = Config
) ->
    ?SLOG(info, #{
        msg => "starting_bridge_redis_connector",
        connector => InstId,
        config => Config
    }),
    case emqx_connector_redis:on_start(InstId, Config) of
        {ok, State} -> {ok, State#{egress => Egress}};
        {error, _} = Err -> Err
    end.

on_stop(InstId, State) ->
    ?SLOG(info, #{
        msg => "stopping_redis_connector",
        connector => InstId
    }),
    emqx_connector_redis:on_stop(InstId, State).

on_query(InstId, {send_message, Msg}, AfterCommand, #{egress := Egress} = State) ->
    ?TRACE(
        "QUERY",
        "redis_bridge_connector_msg_received",
        #{connector => InstId, msg => Msg, state => State}
    ),
    #{payload := PayloadTpl, egress_command := EgressCmd} = Egress,
    PayloadTks = emqx_plugin_libs_rule:preproc_tmpl(PayloadTpl),
    Payload = emqx_plugin_libs_rule:proc_tmpl(PayloadTks, Msg, #{return => full_binary}),
    Cmd = EgressCmd ++ [Payload],
    on_query(InstId, {cmd, Cmd}, AfterCommand, State);
on_query(InstId, Query, AfterCommand, State) ->
    ?TRACE(
        "QUERY",
        "redis_bridge_connector_query_received",
        #{connector => InstId, query => Query, state => State}
    ),
    emqx_connector_redis:on_query(InstId, Query, AfterCommand, State).

on_get_status(InstId, State) ->
    emqx_connector_redis:on_get_status(InstId, State).
