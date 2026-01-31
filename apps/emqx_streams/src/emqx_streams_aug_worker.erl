%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_aug_worker).

-behaviour(gen_server).

-include("emqx_streams_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-export([
    start_link/1,
    child_spec/1,
    handle_new_message/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(st, {
    stream :: emqx_streams_types:stream()
}).

start_link(Stream) ->
    gen_server:start_link(?MODULE, Stream, []).

child_spec(#{id := Id} = Stream) ->
    #{
        id => Id,
        start => {?MODULE, start_link, [Stream]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

handle_new_message(Stream) ->
    case emqx_streams_sup:start_aug_worker(Stream) of
        {ok, Pid} ->
            erlang:send(Pid, new_message),
            ok;
        {error, Reason} ->
            ?tp(error, streams_aug_worker_start_error, #{stream => Stream, reason => Reason}),
            {error, Reason}
    end.

init(Stream) ->
    ?tp(warning, streams_aug_worker_init, #{stream => Stream}),
    {ok, #st{stream = Stream}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

% Stream:
% ```
%     #{
%         id => <<0, 6, 73, 177, 20, 124, 69, 236, 244, 69, 0, 0, 26, 241, 0, 0>>,
%         tools =>
%             [
%                 #{
%                     <<"args">> =>
%                         [
%                             #{
%                                 <<"desc">> => <<"Device Id">>,
%                                 <<"name">> => <<"device_id">>
%                             }
%                         ],
%                     <<"desc">> => <<"get device info">>,
%                     <<"name">> => <<"get_device_info">>,
%                     <<"resource">> =>
%                         #{
%                             <<"method">> => <<"GET">>,
%                             <<"query_template">> =>
%                                 <<"http://localhost:20000/devices/${device_id}">>,
%                             <<"type">> => <<"http">>
%                         }
%                 },
%                 #{
%                     <<"args">> =>
%                         [
%                             #{
%                                 <<"desc">> => <<"message payload">>,
%                                 <<"name">> => <<"message_payload">>
%                             }
%                         ],
%                     <<"desc">> => <<"publish message to a predefined topic">>,
%                     <<"name">> => <<"publish_message">>,
%                     <<"resource">> =>
%                         #{
%                             <<"type">> => <<"publish">>,
%                             <<"topic">> => <<"warnings">>,
%                             <<"body_template">> => <<"${message_payload}">>
%                         }
%                 }
%             ],
%         prompt => <<"xxxx">>,
%         topic_filter => <<"t/2/#">>,
%         limits =>
%             #{
%                 max_shard_message_bytes => infinity,
%                 max_shard_message_count => infinity
%             },
%         is_lastvalue => true,
%         key_expression =>
%             #{expr => "message.from", form => {var, "message.from"}},
%         data_retention_period => 604800000,
%         read_max_unacked => 1000
%     }.
% ```

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(new_message, #st{stream = Stream} = State) ->
    %% Flush all other new_message events
    flush_new_messages(),

    %% Start agent loop with max iterations
    agent_loop(Stream, _MaxIterations = 10),

    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%% Helpers

read_context(Stream) ->
    TTVs0 = [
        {Time, Payload}
     || {_Topic, Time, Payload} <- emqx_streams_message_db:dirty_read_all(Stream)
    ],
    TTVs = lists:sort(TTVs0),

    lists:map(
        fun({Timestamp, Payload}) ->
            Message = emqx_streams_message_db:decode_message(Payload),
            {format_timestamp(Timestamp), emqx_message:from(Message), emqx_message:payload(Message)}
        end,
        TTVs
    ).

format_timestamp(Microsecond) ->
    Seconds = Microsecond div 1000000,
    Milliseconds = (Microsecond rem 1000000) div 1000,
    {{Year, Month, Day}, {Hour, Minute, Second}} =
        calendar:system_time_to_universal_time(Seconds, second),
    io_lib:format(
        "~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B.~3..0B",
        [Year, Month, Day, Hour, Minute, Second, Milliseconds]
    ).

add_to_context(Stream, From, Text) ->
    Topic = emqx_streams_prop:topic_filter(Stream),
    Message = emqx_message:make(From, ?QOS_1, Topic, Text),
    Res = emqx_streams_message_db:insert(Stream, Message),
    ?tp(warning, streams_aug_worker_add_to_context, #{
        from => From,
        text => Text,
        result => Res
    }),
    Res.

%% Agent orchestrator functions

flush_new_messages() ->
    receive
        new_message ->
            flush_new_messages()
    after 0 ->
        ok
    end.

agent_loop(Stream, MaxIterations) ->
    agent_loop(Stream, MaxIterations, 0).

agent_loop(_Stream, MaxIterations, CurrentIteration) when CurrentIteration >= MaxIterations ->
    ?tp(warning, streams_aug_worker_max_iterations_reached, #{
        max_iterations => MaxIterations,
        current_iteration => CurrentIteration
    }),
    ok;
agent_loop(Stream, MaxIterations, CurrentIteration) ->
    Context = read_context(Stream),
    SystemPrompt = maps:get(prompt, Stream, <<"You are a helpful assistant.">>),
    Tools = maps:get(tools, Stream, []),

    %% Convert tools to OpenAI format
    OpenAITools = convert_tools_to_openai_format(Tools),

    %% Format context as messages
    Messages = format_context_as_messages(Context, SystemPrompt),

    ?tp(warning, streams_aug_worker_calling_llm, #{
        messages => Messages,
        tools => OpenAITools,
        iteration => CurrentIteration
    }),

    %% Call LLM API (OpenAI or Together)
    Provider = emqx_streams_prop:provider(Stream),
    Model = emqx_streams_prop:model(Stream),
    case call_llm(Provider, Model, Messages, OpenAITools) of
        {ok, Response} ->
            handle_llm_response(Stream, Response, Tools, MaxIterations, CurrentIteration);
        {error, Reason} ->
            ?tp(error, streams_aug_worker_llm_error, #{reason => Reason}),
            ok
    end.

handle_llm_response(Stream, Response, Tools, MaxIterations, CurrentIteration) ->
    %% Extract the assistant's response
    FinishReason = maps:get(<<"finish_reason">>, Response, undefined),
    Message = maps:get(<<"message">>, Response, #{}),
    Content = maps:get(<<"content">>, Message, null),
    ToolCalls = maps:get(<<"tool_calls">>, Message, []),

    ?tp(warning, streams_aug_worker_llm_response, #{
        response => Response
    }),

    %% Handle tool calls
    case {FinishReason, ToolCalls} of
        {<<"tool_calls">>, [_ | _]} ->
            %% Store the assistant message with tool_calls as structured JSON
            MessageJson = emqx_utils_json:encode(#{
                <<"type">> => <<"assistant_with_tools">>,
                <<"message">> => Message
            }),
            add_to_context(Stream, <<"llm">>, MessageJson),

            %% Execute tools
            execute_tools(Stream, ToolCalls, Tools),

            %% Flush any new messages that arrived during tool execution
            %% (so the LLM sees fresh user input when processing tool results)
            flush_new_messages(),

            %% Continue the loop with incremented iteration counter
            agent_loop(Stream, MaxIterations, CurrentIteration + 1);
        _ ->
            %% Regular assistant message - store content if present
            case Content of
                null -> ok;
                <<>> -> ok;
                _ -> add_to_context(Stream, <<"llm">>, Content)
            end,
            ok
    end.

execute_tools(Stream, ToolCalls, AvailableTools) ->
    lists:foreach(
        fun(ToolCall) ->
            ToolCallId = maps:get(<<"id">>, ToolCall),
            Function = maps:get(<<"function">>, ToolCall),
            ToolName = maps:get(<<"name">>, Function),
            Arguments = maps:get(<<"arguments">>, Function),

            ?tp(warning, streams_aug_worker_executing_tool, #{
                tool_call_id => ToolCallId,
                tool_name => ToolName,
                arguments => Arguments
            }),

            %% Parse arguments JSON
            Args = emqx_utils_json:decode(Arguments),

            %% Find tool definition
            case find_tool(ToolName, AvailableTools) of
                {ok, ToolDef} ->
                    Result = execute_tool(ToolDef, Args, Stream),

                    %% Store tool result with tool_call_id for proper message reconstruction
                    ToolResultJson = emqx_utils_json:encode(#{
                        <<"type">> => <<"tool_result">>,
                        <<"tool_call_id">> => ToolCallId,
                        <<"name">> => ToolName,
                        <<"content">> => Result
                    }),
                    add_to_context(Stream, <<"tool:", ToolName/binary>>, ToolResultJson);
                {error, not_found} ->
                    ErrorJson = emqx_utils_json:encode(#{
                        <<"type">> => <<"tool_result">>,
                        <<"tool_call_id">> => ToolCallId,
                        <<"name">> => ToolName,
                        <<"content">> => #{error => <<"Tool not found">>}
                    }),
                    add_to_context(Stream, <<"tool:", ToolName/binary>>, ErrorJson)
            end
        end,
        ToolCalls
    ).

find_tool(ToolName, Tools) ->
    case
        lists:filter(
            fun(Tool) -> maps:get(<<"name">>, Tool) =:= ToolName end,
            Tools
        )
    of
        [Tool | _] -> {ok, Tool};
        [] -> {error, not_found}
    end.

execute_tool(ToolDef, Args, Stream) ->
    Resource = maps:get(<<"resource">>, ToolDef),
    Type = maps:get(<<"type">>, Resource),

    case Type of
        <<"http">> ->
            execute_http_tool(Resource, Args);
        <<"publish">> ->
            execute_publish_tool(Resource, Args, Stream);
        _ ->
            #{error => <<"Unknown tool type">>, type => Type}
    end.

execute_http_tool(Resource, Args) ->
    Method = binary_to_existing_atom(
        string:lowercase(maps:get(<<"method">>, Resource, <<"get">>)),
        utf8
    ),
    QueryTemplate = maps:get(<<"query_template">>, Resource),

    %% Parse and render template with arguments
    ParsedTemplate = emqx_template:parse(QueryTemplate),
    URL =
        case emqx_template:render(ParsedTemplate, Args) of
            {Rendered, []} ->
                iolist_to_binary(Rendered);
            {Rendered, Errors} ->
                ?tp(warning, streams_aug_worker_template_errors, #{
                    errors => Errors,
                    template => QueryTemplate
                }),
                iolist_to_binary(Rendered)
        end,

    ?tp(warning, streams_aug_worker_http_request, #{
        method => Method,
        url => URL
    }),

    %% Make HTTP request with hackney (30s connect, 60s receive timeout)
    Options = [
        with_body,
        {connect_timeout, 30000},
        {recv_timeout, 60000}
    ],

    case hackney:request(Method, URL, [], <<>>, Options) of
        {ok, StatusCode, _Headers, Body} ->
            #{
                status_code => StatusCode,
                body => Body
            };
        {error, Reason} ->
            #{
                error => <<"HTTP request failed">>,
                reason => iolist_to_binary(io_lib:format("~p", [Reason]))
            }
    end.

execute_publish_tool(Resource, Args, _Stream) ->
    Topic = maps:get(<<"topic">>, Resource),
    BodyTemplate = maps:get(<<"body_template">>, Resource),

    %% Parse and render template with arguments
    ParsedTemplate = emqx_template:parse(BodyTemplate),
    Payload =
        case emqx_template:render(ParsedTemplate, Args) of
            {Rendered, []} ->
                iolist_to_binary(Rendered);
            {Rendered, Errors} ->
                ?tp(warning, streams_aug_worker_template_errors, #{
                    errors => Errors,
                    template => BodyTemplate
                }),
                iolist_to_binary(Rendered)
        end,

    ?tp(warning, streams_aug_worker_publish, #{
        topic => Topic,
        payload => Payload
    }),

    %% Create and publish message
    Message = emqx_message:make(<<"aug_worker">>, Topic, Payload),
    case emqx_broker:publish(Message) of
        [] ->
            #{success => true, topic => Topic};
        _ ->
            #{success => true, topic => Topic, delivered => true}
    end.

format_context_as_messages(Context, SystemPrompt) ->
    SystemMsg = #{
        <<"role">> => <<"system">>,
        <<"content">> => SystemPrompt
    },

    UserMessages = lists:flatmap(
        fun({Timestamp, From, Payload}) ->
            _TimestampBin = iolist_to_binary(Timestamp),

            %% Try to parse as structured message
            case catch emqx_utils_json:decode(Payload) of
                #{<<"type">> := <<"assistant_with_tools">>, <<"message">> := Message} ->
                    %% Assistant message with tool calls - use original structure
                    [Message];
                #{
                    <<"type">> := <<"tool_result">>,
                    <<"tool_call_id">> := ToolCallId,
                    <<"name">> := Name,
                    <<"content">> := Content
                } ->
                    %% Tool result - format as tool message
                    ContentStr =
                        case Content of
                            C when is_binary(C) -> C;
                            C -> emqx_utils_json:encode(C)
                        end,
                    [
                        #{
                            <<"role">> => <<"tool">>,
                            <<"tool_call_id">> => ToolCallId,
                            <<"name">> => Name,
                            <<"content">> => ContentStr
                        }
                    ];
                _ ->
                    %% Regular message - determine role based on source
                    Role =
                        case From of
                            <<"llm">> -> <<"assistant">>;
                            _ -> <<"user">>
                        end,
                    [
                        #{
                            <<"role">> => Role,
                            <<"content">> => Payload
                        }
                    ]
            end
        end,
        Context
    ),

    [SystemMsg | UserMessages].

convert_tools_to_openai_format(Tools) ->
    lists:map(
        fun(Tool) ->
            Name = maps:get(<<"name">>, Tool),
            Desc = maps:get(<<"desc">>, Tool, <<"">>),
            Args = maps:get(<<"args">>, Tool, []),

            %% Build JSON schema for parameters
            Properties = lists:foldl(
                fun(Arg, Acc) ->
                    ArgName = maps:get(<<"name">>, Arg),
                    ArgDesc = maps:get(<<"desc">>, Arg, <<"">>),
                    Acc#{
                        ArgName => #{
                            <<"type">> => <<"string">>,
                            <<"description">> => ArgDesc
                        }
                    }
                end,
                #{},
                Args
            ),

            RequiredArgs = [maps:get(<<"name">>, Arg) || Arg <- Args],

            #{
                <<"type">> => <<"function">>,
                <<"function">> => #{
                    <<"name">> => Name,
                    <<"description">> => Desc,
                    <<"parameters">> => #{
                        <<"type">> => <<"object">>,
                        <<"properties">> => Properties,
                        <<"required">> => RequiredArgs
                    }
                }
            }
        end,
        Tools
    ).

call_llm(Provider, Model, Messages, Tools) ->
    %% Get API key and URL based on provider
    {ApiKeyEnvVar, URL} =
        case Provider of
            openai ->
                {
                    "OPENAI_API_KEY",
                    <<"https://api.openai.com/v1/chat/completions">>
                };
            together ->
                {
                    "TOGETHER_API_KEY",
                    <<"https://api.together.xyz/v1/chat/completions">>
                };
            _ ->
                ?tp(error, streams_aug_worker_unknown_provider, #{provider => Provider}),
                throw({error, {unknown_provider, Provider}})
        end,

    %% Get API key from environment
    ApiKey =
        case os:getenv(ApiKeyEnvVar) of
            false ->
                ?tp(error, streams_aug_worker_no_api_key, #{env_var => ApiKeyEnvVar}),
                throw({error, no_api_key});
            Key ->
                list_to_binary(Key)
        end,

    %% Build request body
    RequestBody = #{
        <<"model">> => Model,
        <<"messages">> => Messages
    },

    %% Add tools if available
    FinalBody =
        case Tools of
            [] ->
                RequestBody;
            _ ->
                RequestBody#{<<"tools">> => Tools}
        end,

    BodyJson = emqx_utils_json:encode(FinalBody),

    %% Make API request
    Headers = [
        {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"Content-Type">>, <<"application/json">>}
    ],

    ?tp(warning, streams_aug_worker_llm_request, #{
        provider => Provider,
        model => Model,
        url => URL,
        body => BodyJson
    }),

    %% Set timeouts: 30s connect, 2 minutes receive (for thinking models)
    Options = [
        with_body,
        {connect_timeout, 30000},
        {recv_timeout, 120000}
    ],

    case hackney:request(post, URL, Headers, BodyJson, Options) of
        {ok, 200, _RespHeaders, RespBody} ->
            case emqx_utils_json:decode(RespBody, [return_maps]) of
                #{<<"choices">> := [Choice | _]} ->
                    {ok, Choice};
                Other ->
                    ?tp(error, streams_aug_worker_unexpected_response, #{response => Other}),
                    {error, unexpected_response}
            end;
        {ok, StatusCode, _RespHeaders, RespBody} ->
            ?tp(error, streams_aug_worker_llm_error, #{
                provider => Provider,
                status_code => StatusCode,
                body => RespBody
            }),
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            ?tp(error, streams_aug_worker_llm_request_failed, #{
                provider => Provider,
                reason => Reason
            }),
            {error, {request_failed, Reason}}
    end.
