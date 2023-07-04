%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_cluster_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(emqx_dashboard_api_test_helpers, [host/0, uri/1]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = ?config(priv_dir, Config),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, #{}}
        ],
        #{work_dir => WorkDir}
    ),
    [{group_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(group_apps, Config)).

t_gen_rpc(_Config) ->
    ?assertMatch(
        ok,
        emqx_rpc:call(node(), emqx, running_nodes, [])
    ).
