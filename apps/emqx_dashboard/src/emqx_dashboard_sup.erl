%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, ShutDown), {I, {I, start_link, []}, permanent, ShutDown, worker, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% supervisor owns the cache table
    ok = emqx_dashboard_desc_cache:init(),
    {ok,
        {{one_for_one, 5, 100}, [
            ?CHILD(emqx_dashboard_listener, brutal_kill),
            ?CHILD(emqx_dashboard_token, 5000),
            ?CHILD(emqx_dashboard_monitor, 5000)
        ]}}.
