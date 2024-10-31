%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_persistent_session_ds_node_heartbeat_worker).

-behaviour(gen_server).

-include("session_internals.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    create_tables/0,
    start_link/0,
    get_node_epoch_id/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% call/cast/info records
-record(update_last_alive_at, {}).

-define(epoch_id_pt_key, {?MODULE, epoch_id}).
-define(node_epoch, node_epoch).
-define(tab, ?node_epoch).

-record(?node_epoch, {
    epoch_id :: reference(),
    node :: node(),
    last_alive_at :: pos_integer()
}).

-type epoch_id() :: reference().

%%--------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    create_tables(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_node_epoch_id() -> epoch_id().
get_node_epoch_id() ->
    persistent_term:get(?epoch_id_pt_key).

%%--------------------------------------------------------------------------------
%% `gen_server' API
%%--------------------------------------------------------------------------------

init(_Opts) ->
    erlang:process_flag(trap_exit, true),
    ok = generate_node_epoch_id(),
    ok = update_last_alive_at(),
    ok = ensure_heartbeat_timer(),
    State = #{},
    {ok, State}.

handle_call(_Call, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#update_last_alive_at{}, State) ->
    ok = update_last_alive_at(),
    ok = ensure_heartbeat_timer(),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = delete_last_alive_at(),
    ok.

%%--------------------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?tab, [
        {rlog_shard, ?DS_MRIA_SHARD},
        {type, set},
        {storage, disc_copies},
        {record_name, ?node_epoch},
        {attributes, record_info(fields, ?node_epoch)}
    ]),
    mria:wait_for_tables([?tab]).

generate_node_epoch_id() ->
    EpochId = erlang:make_ref(),
    persistent_term:put(?epoch_id_pt_key, EpochId),
    ok.

ensure_heartbeat_timer() ->
    _ = erlang:send_after(bump_interval(), self(), #update_last_alive_at{}),
    ok.

update_last_alive_at() ->
    EpochId = get_node_epoch_id(),
    LastAliveAt = erlang:system_time(mill),
    ok = mria:dirty_write(?tab, #?node_epoch{epoch_id=EpochId, node=node(), last_alive_at=LastAliveAt}),
    ok.

delete_last_alive_at() ->
    EpochId = get_node_epoch_id(),
    ok = mria:dirty_delete(?tab, EpochId).

%% Note: we don't allow overriding `heartbeat_interval' per
%% zone, since the GC process is responsible for all sessions
%% regardless of the zone.
bump_interval() ->
    emqx_config:get([durable_sessions, heartbeat_interval]).

now_ms() ->
    erlang:system_time(millisecond).
