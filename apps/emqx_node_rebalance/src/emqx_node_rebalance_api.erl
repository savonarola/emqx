%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_node_rebalance_api).

-rest_api(#{name   => load_rebalance_status,
            method => 'GET',
            path   => "/load_rebalance/status",
            func   => status,
            descr  => "Get load rebalance status"}).

-rest_api(#{name   => load_rebalance_availability_check,
            method => 'GET',
            path   => "/load_rebalance/availability_check",
            func   => availability_check,
            descr  => "Node rebalance availability check"}).

-export([status/2,
        availability_check/2]).

status(_Bindings, _Params) ->
    case emqx_node_rebalance_evacuation:status() of
        disabled ->
            {ok, #{status => disabled}};
        {enabled, Stats} ->
            {ok, format_stats(Stats)}
    end.

availability_check(_Bindings, _Params) ->
    case emqx_node_rebalance_evacuation:status() of
        disabled ->
            {200, #{}};
        {enabled, _Stats} ->
            {503, #{}}
    end.

format_stats(Stats) ->
    #{
      status => maps:get(status, Stats),
      connection_eviction_rate => maps:get(conn_evict_rate, Stats),
      session_eviction_rate => maps:get(sess_evict_rate, Stats),
      %% for evacuation
      connection_goal => 0,
      session_goal => 0,
      stats => #{
        initial_connected => maps:get(initial_conns, Stats),
        current_connected => maps:get(current_conns, Stats),
        initial_sessions => maps:get(initial_sessions, Stats),
        current_sessions => maps:get(current_sessions, Stats)
      }
     }.
