%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Schema for EMQX_DS databases.
-module(emqx_ds_schema).

%% API:
-export([schema/0, storage_schema/1, translate_builtin_raft/1, translate_builtin_local/1]).
-export([db_config/1]).

%% Behavior callbacks:
-export([fields/1, desc/1, namespace/0]).

-include("emqx_schema.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("hocon/include/hocon_types.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-ifndef(EMQX_RELEASE_EDITION).
-define(EMQX_RELEASE_EDITION, ce).
-endif.

-if(?EMQX_RELEASE_EDITION == ee).
-define(DEFAULT_BACKEND, builtin_raft).
-define(BUILTIN_BACKENDS, [ref(builtin_raft), ref(builtin_local)]).
-else.
-define(DEFAULT_BACKEND, builtin_local).
-define(BUILTIN_BACKENDS, [ref(builtin_local)]).
-endif.

%%================================================================================
%% API
%%================================================================================

-spec db_config(emqx_config:runtime_config_key_path()) -> emqx_ds:create_db_opts().
db_config(Path) ->
    ConfigTree = #{'_config_handler' := {Module, Function}} = emqx_config:get(Path),
    apply(Module, Function, [ConfigTree]).

translate_builtin_raft(
    Backend = #{
        backend := builtin_raft,
        n_shards := NShards,
        n_sites := NSites,
        replication_factor := ReplFactor,
        layout := Layout
    }
) ->
    #{
        backend => builtin_raft,
        n_shards => NShards,
        n_sites => NSites,
        replication_factor => ReplFactor,
        replication_options => maps:get(replication_options, Backend, #{}),
        storage => translate_layout(Layout)
    }.

translate_builtin_local(
    #{
        backend := builtin_local,
        n_shards := NShards,
        layout := Layout
    }
) ->
    #{
        backend => builtin_local,
        n_shards => NShards,
        storage => translate_layout(Layout)
    }.

%%================================================================================
%% Behavior callbacks
%%================================================================================

namespace() ->
    durable_storage.

schema() ->
    [
        {messages,
            storage_schema(#{
                importance => ?IMPORTANCE_MEDIUM,
                desc => ?DESC(messages)
            })}
    ].

storage_schema(ExtraOptions) ->
    Options = #{
        default => #{<<"backend">> => ?DEFAULT_BACKEND}
    },
    sc(
        hoconsc:union(
            ?BUILTIN_BACKENDS ++ emqx_schema_hooks:injection_point('durable_storage.backends', [])
        ),
        maps:merge(Options, ExtraOptions)
    ).

fields(builtin_local) ->
    %% Schema for the builtin_raft backend:
    [
        {backend,
            sc(
                builtin_local,
                #{
                    'readOnly' => true,
                    default => builtin_local,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(backend_type)
                }
            )},
        {'_config_handler',
            sc(
                {module(), atom()},
                #{
                    'readOnly' => true,
                    default => {?MODULE, translate_builtin_local},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
        | common_builtin_fields()
    ];
fields(builtin_raft) ->
    %% Schema for the builtin_raft backend:
    [
        {backend,
            sc(
                builtin_raft,
                #{
                    'readOnly' => true,
                    default => builtin_raft,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(backend_type)
                }
            )},
        {'_config_handler',
            sc(
                {module(), atom()},
                #{
                    'readOnly' => true,
                    default => {?MODULE, translate_builtin_raft},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {replication_factor,
            sc(
                pos_integer(),
                #{
                    default => 3,
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {n_sites,
            sc(
                pos_integer(),
                #{
                    default => 1,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(builtin_n_sites)
                }
            )},
        %% TODO: Elaborate.
        {replication_options,
            sc(
                hoconsc:map(name, any()),
                #{
                    default => #{},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
        | common_builtin_fields()
    ];
fields(builtin_write_buffer) ->
    [
        {max_items,
            sc(
                pos_integer(),
                #{
                    default => 1000,
                    mapping => "emqx_durable_storage.egress_batch_size",
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(builtin_write_buffer_max_items)
                }
            )},
        {flush_interval,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => 100,
                    mapping => "emqx_durable_storage.egress_flush_interval",
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(builtin_write_buffer_flush_interval)
                }
            )}
    ];
fields(layout_builtin_wildcard_optimized) ->
    [
        {type,
            sc(
                wildcard_optimized,
                #{
                    'readOnly' => true,
                    default => wildcard_optimized,
                    desc => ?DESC(layout_builtin_wildcard_optimized_type)
                }
            )},
        {bits_per_topic_level,
            sc(
                range(1, 64),
                #{
                    default => 64,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {epoch_bits,
            sc(
                range(0, 64),
                #{
                    default => 20,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(wildcard_optimized_epoch_bits)
                }
            )},
        {topic_index_bytes,
            sc(
                pos_integer(),
                #{
                    default => 4,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields(layout_builtin_wildcard_optimized_v2) ->
    [
        {type,
            sc(
                wildcard_optimized_v2,
                #{
                    'readOnly' => true,
                    default => wildcard_optimized_v2,
                    desc => ?DESC(layout_builtin_wildcard_optimized_type)
                }
            )},
        {bytes_per_topic_level,
            sc(
                range(1, 16),
                #{
                    default => 8,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {topic_index_bytes,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {serialization_schema,
            sc(
                emqx_ds_msg_serializer:schema(),
                #{
                    default => v1,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields(layout_builtin_reference) ->
    [
        {type,
            sc(
                reference,
                #{
                    'readOnly' => true,
                    importance => ?IMPORTANCE_LOW,
                    default => reference,
                    desc => ?DESC(layout_builtin_reference_type)
                }
            )}
    ].

common_builtin_fields() ->
    [
        {data_dir,
            sc(
                string(),
                #{
                    mapping => "emqx_durable_storage.db_data_dir",
                    required => false,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_data_dir)
                }
            )},
        {n_shards,
            sc(
                pos_integer(),
                #{
                    default => 16,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_n_shards)
                }
            )},
        {local_write_buffer,
            sc(
                ref(builtin_write_buffer),
                #{
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(builtin_write_buffer)
                }
            )},
        {layout,
            sc(
                hoconsc:union(builtin_layouts()),
                #{
                    desc => ?DESC(builtin_layout),
                    importance => ?IMPORTANCE_MEDIUM,
                    default =>
                        #{
                            <<"type">> => wildcard_optimized_v2
                        }
                }
            )}
    ].

desc(builtin_raft) ->
    ?DESC(builtin_raft);
desc(builtin_local) ->
    ?DESC(builtin_local);
desc(builtin_write_buffer) ->
    ?DESC(builtin_write_buffer);
desc(layout_builtin_wildcard_optimized) ->
    ?DESC(layout_builtin_wildcard_optimized);
desc(layout_builtin_wildcard_optimized_v2) ->
    ?DESC(layout_builtin_wildcard_optimized);
desc(layout_builtin_reference) ->
    ?DESC(layout_builtin_reference);
desc(_) ->
    undefined.

%%================================================================================
%% Internal functions
%%================================================================================

translate_layout(
    #{
        type := wildcard_optimized_v2,
        bytes_per_topic_level := BytesPerTopicLevel,
        topic_index_bytes := TopicIndexBytes,
        serialization_schema := SSchema
    }
) ->
    {emqx_ds_storage_skipstream_lts, #{
        wildcard_hash_bytes => BytesPerTopicLevel,
        topic_index_bytes => TopicIndexBytes,
        serialization_schema => SSchema
    }};
translate_layout(
    #{
        type := wildcard_optimized,
        bits_per_topic_level := BitsPerTopicLevel,
        epoch_bits := EpochBits,
        topic_index_bytes := TIBytes
    }
) ->
    {emqx_ds_storage_bitfield_lts, #{
        bits_per_topic_level => BitsPerTopicLevel,
        topic_index_bytes => TIBytes,
        epoch_bits => EpochBits
    }};
translate_layout(#{type := reference}) ->
    {emqx_ds_storage_reference, #{}}.

builtin_layouts() ->
    %% Reference layout stores everything in one stream, so it's not
    %% suitable for production use. However, it's very simple and
    %% produces a very predictabale replay order, which can be useful
    %% for testing and debugging:
    [
        ref(layout_builtin_wildcard_optimized_v2),
        ref(layout_builtin_wildcard_optimized),
        ref(layout_builtin_reference)
    ].

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(StructName) -> hoconsc:ref(?MODULE, StructName).
