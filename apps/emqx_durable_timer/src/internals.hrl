%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-ifndef(EMQX_DURABLE_TIMER_HRL).
-define(EMQX_DURABLE_TIMER_HRL, true).

-include_lib("snabbkaffe/include/trace.hrl").

-define(DB_GLOB, global_timers).

%% Topics:
-define(top_deadhand, <<"d">>).
-define(top_started, <<"s">>).
-define(top_heartbeat, <<"h">>).
-define(top_nodes, <<"n">>).

-define(type_bits, 32).
-define(max_type, (1 bsl ?type_bits - 1)).

%% Tracepoints:
-define(tp_init, emqx_durable_timer_init).
-define(tp_open_epoch, emqx_durable_timer_open_epoch).
-define(tp_close_epoch, emqx_durable_timer_close_epoch).
-define(tp_heartbeat, emqx_durable_timer_heartbeat).
-define(tp_missed_heartbeat, emqx_durable_timer_missed_heartbeat).
-define(tp_new_apply_after, emqx_durable_timer_apply_after).
-define(tp_new_dead_hand, emqx_durable_timer_dead_hand).
-define(tp_delete, emqx_durable_timer_delete).
-define(tp_fire, emqx_durable_timer_fire).
-define(tp_unknown_event, emqx_durable_timer_unknown_event).
-define(tp_app_activation, emqx_durable_timer_app_activated).
-define(tp_state_change, emqx_durable_timer_state_change).
-define(tp_replay_start, emqx_durable_timer_replay_start).
-define(tp_replay_failed, emqx_durable_timer_replay_failed).

-define(tp_apply_after_write_begin, emqx_durable_timer_apply_write_begin).
-define(tp_apply_after_write_ok, emqx_durable_timer_apply_write_ok).
-define(tp_apply_after_write_fail, emqx_durable_timer_apply_write_fail).

-define(tp_test_register, emqx_durable_timer_test_register).
-define(tp_test_fire, emqx_durable_timer_test_fire).

-endif.
