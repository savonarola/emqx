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

-module(emqx_bridge_connector_redis_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([connector_type/0]).

namespace() -> "bridge-connector-redis".

roots() -> [].

fields("egress") ->
    [
        {egress_command,
            sc(
                list(binary()),
                #{
                    default => undefined,
                    % ?DESC("egress_command")
                    desc => ""
                }
            )},
        {payload,
            sc(
                binary(),
                #{
                    default => undefined,
                    % ?DESC("payload")
                    desc => ""
                }
            )}
    ];
fields("ingress") ->
    [].

connector_type() ->
    hoconsc:union(
        [
            hoconsc:ref(emqx_connector_redis, cluster),
            hoconsc:ref(emqx_connector_redis, single),
            hoconsc:ref(emqx_connector_redis, sentinel)
        ]
    ).

% fields("replayq") ->
%     [
%         {dir,
%             sc(
%                 hoconsc:union([boolean(), string()]),
%                 #{desc => ?DESC("dir")}
%             )},
%         {seg_bytes,
%             sc(
%                 emqx_schema:bytesize(),
%                 #{
%                     default => "100MB",
%                     desc => ?DESC("seg_bytes")
%                 }
%             )},
%         {offload,
%             sc(
%                 boolean(),
%                 #{
%                     default => false,
%                     desc => ?DESC("offload")
%                 }
%             )}
%     ].

desc(_) ->
    "".

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
% ref(Field) -> hoconsc:ref(?MODULE, Field).
