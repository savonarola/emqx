%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ai_completion_registry).

-export([
    matching_completions/1
]).

-type completion() :: emqx_ai_completion:completion().

-spec matching_completions(binary()) -> [completion()].
matching_completions(<<"foo">>) ->
    [
        #{
            prompt =>
                <<"""
                The user provides MQTT messages in JSON format.
                For user's message, if the payload has comma-separated integer values
                * calculate the sum of the values
                * use the sum as the payload of a new message
                * publish the new message to the topic ai_completion/{original_topic}
                * use the qos of the original message as the qos of the new message.

                Do nothing for other messages.
                """>>,
            tools => emqx_ai_completion_tool_publish:tool_specs()
        }
    ];
matching_completions(_Topic) ->
    [].
