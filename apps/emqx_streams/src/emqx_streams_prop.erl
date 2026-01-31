%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_prop).

-moduledoc """
The module contains accessor functions for the Streams.
""".

-export([
    id/1,
    is_limited/1,
    is_lastvalue/1,
    is_append_only/1,
    topic_filter/1,
    quota_index_opts/1,
    max_unacked/1,
    data_retention_period/1,
    prompt/1,
    tools/1,
    provider/1,
    model/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec prompt(emqx_streams_types:stream()) -> binary().
prompt(#{prompt := Prompt} = _Stream) ->
    Prompt.

-spec tools(emqx_streams_types:stream()) -> [term()].
tools(#{tools := Tools} = _Stream) ->
    Tools.

-spec provider(emqx_streams_types:stream()) -> binary().
provider(#{provider := Provider} = _Stream) ->
    Provider.

-spec model(emqx_streams_types:stream()) -> binary().
model(#{model := Model} = _Stream) ->
    Model.

-spec is_limited(emqx_streams_types:stream()) -> boolean().
is_limited(
    #{limits := #{max_shard_message_count := infinity, max_shard_message_bytes := infinity}} =
        _Stream
) ->
    false;
is_limited(_Stream) ->
    true.

-spec is_lastvalue(emqx_streams_types:stream()) -> boolean().
is_lastvalue(#{is_lastvalue := IsLastValue} = _Stream) ->
    IsLastValue.

-spec topic_filter(emqx_streams_types:stream()) ->
    emqx_types:topic().
topic_filter(#{topic_filter := TopicFilter} = _Stream) ->
    TopicFilter.

-spec id(emqx_streams_types:stream()) ->
    emqx_streams_types:stream_id().
id(#{id := ID} = _Stream) ->
    ID.

-spec is_append_only(emqx_streams_types:stream()) -> boolean().
is_append_only(Stream) ->
    (not is_limited(Stream)) andalso (not is_lastvalue(Stream)).

-spec quota_index_opts(emqx_streams_types:stream()) ->
    emqx_mq_quota_index:opts().
quota_index_opts(#{
    limits := #{
        max_shard_message_count := MaxShardMessageCount,
        max_shard_message_bytes := MaxShardMessageBytes
    }
}) ->
    #{
        bytes => MaxShardMessageBytes,
        count => MaxShardMessageCount,
        threshold_percentage => emqx_streams_config:quota_option(threshold_percentage)
    }.

-spec max_unacked(emqx_streams_types:stream()) ->
    non_neg_integer().
max_unacked(#{read_max_unacked := ReadMaxUnacked} = _Stream) ->
    ReadMaxUnacked.

-spec data_retention_period(emqx_streams_types:stream()) ->
    non_neg_integer().
data_retention_period(#{data_retention_period := DataRetentionPeriod} = _Stream) ->
    DataRetentionPeriod.
