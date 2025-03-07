%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_tool_publish).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_ai_completion_tool).
-export([
    tool_specs/0
]).

-export([
    namespace/0,
    roots/0,
    fields/1
]).

namespace() -> ai_completion.

roots() -> [].

fields(publish) ->
    [
        {topic,
            hoconsc:mk(
                binary(),
                #{
                    desc => "MQTT topic to publish the message to",
                    required => true
                }
            )},
        {payload,
            hoconsc:mk(
                binary(),
                #{
                    desc => "MQTT message payload to publish",
                    required => true
                }
            )},
        {qos,
            hoconsc:mk(
                integer(),
                #{
                    desc => "MQTT QoS to publish the message with",
                    required => true
                }
            )}
    ].

tool_specs() ->
    [
        #{
            name => publish,
            description => <<"Publish a message with a given payload and qos to a topic">>,
            function => fun publish/1,
            parameters => ?R_REF(?MODULE, publish)
        }
    ].

publish(#{topic := Topic, payload := Payload, qos := QoS}) ->
    ?tp(warning, emqx_ai_completion_tool_publish, #{
        topic => Topic,
        payload => Payload,
        qos => QoS
    }),
    emqx_broker:publish(emqx_message:make(<<"ai_completion">>, QoS, Topic, Payload)).
