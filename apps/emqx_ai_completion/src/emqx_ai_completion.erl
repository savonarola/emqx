%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ai_completion).

-feature(maybe_expr, enable).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").

%% `emqx_hooks' API
-export([
    register_hooks/0,
    unregister_hooks/0,

    on_message_publish/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type completion() :: #{
    prompt := binary(),
    tools := [emqx_ai_completion_tool:tool_spec()]
}.

-export_type([completion/0]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    emqx_hooks:put(
        'message.publish', {?MODULE, on_message_publish, []}, ?HP_AI_COMPLETION
    ).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

-spec on_message_publish(emqx_types:message()) ->
    {ok, emqx_types:message()} | {stop, emqx_types:message()}.
on_message_publish(#message{topic = <<"$SYS/", _/binary>>} = Message) ->
    {ok, Message};
on_message_publish(Message = #message{topic = Topic}) ->
    ?tp(debug, emqx_ai_completion_on_message_publish, #{
        message => Message
    }),
    case emqx_ai_completion_registry:matching_completions(Topic) of
        [] ->
            ok;
        Completions ->
            run_completions(Completions, Message)
    end.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

-spec run_completions([completion()], emqx_types:message()) ->
    {ok, emqx_types:message()}. %% | {stop, emqx_types:message()}.
run_completions(Completions, MessageIn) ->
    ok = lists:foreach(fun(Completion) ->
        ok = run_completion(Completion, MessageIn)
    end, Completions),
    {ok, MessageIn}.

-spec run_completion(completion(), emqx_types:message()) -> ok.
run_completion(#{prompt := Prompt, tools := Tools}, MessageIn) ->
    MessageJson = emqx_utils_json:encode(message_to_map(MessageIn)),
    Request = #{
        model => <<"gpt-4o">>,
        messages => [
            #{role => <<"system">>, content => Prompt},
            #{role => <<"user">>, content => MessageJson}
        ],
        tools => emqx_ai_completion_tool:tools_to_openai_spec(Tools)
    },
    ?tp(warning, emqx_ai_completion_on_message_publish_request, #{
        request => Request
    }),
    case emqx_ai_completion_client:api_post({chat, completions}, Request) of
        {ok, #{<<"choices">> := [#{<<"message">> := #{<<"tool_calls">> := ToolCalls}}]}} ->
            ?tp(warning, emqx_ai_completion_on_message_publish_result, #{
                result => ToolCalls
            }),
            ok = apply_tool_calls(ToolCalls, Tools),
            ok;
        {ok, Result} ->
            ?tp(warning, emqx_ai_completion_on_message_publish_result_no_action, #{
                result => Result
            }),
            ok;
        {error, Reason} ->
            ?tp(error, emqx_ai_completion_on_message_publish_error, #{
                reason => Reason
            }),
            ok
    end.

apply_tool_calls(ToolCalls, Tools) ->
    lists:foreach(fun(ToolCall) ->
        apply_tool_call(ToolCall, Tools)
    end, ToolCalls).

apply_tool_call(#{<<"type">> := <<"function">>, <<"function">> := #{<<"name">> := Name, <<"arguments">> := ArgumentsJson}}, Tools) ->
    ArgumentsBin = emqx_utils_json:decode(ArgumentsJson),
    MatchingTool = [
        Tool || Tool = #{name := ToolName} <- Tools, atom_to_binary(ToolName, utf8) =:= Name
    ],
    ?tp(warning, emqx_ai_completion_apply_tool_call, #{
        matching_tool => MatchingTool
    }),
    case MatchingTool of
        [#{function := Function, parameters := Parameters}] ->
            Arguments = convert_arguments(ArgumentsBin, Parameters),
            ?tp(warning, emqx_ai_completion_convert_arguments, #{
                arguments => Arguments
            }),
            _ = Function(Arguments),
            ok;
        [] ->
            ok
    end.

message_to_map(Message) ->
    #{
        topic => emqx_message:topic(Message),
        payload => emqx_message:payload(Message),
        qos => emqx_message:qos(Message),
        retain => emqx_message:get_flag(retain, Message)
    }.

convert_arguments(ArgumentsBin, Parameters) ->
    Schema = #{roots => [{args, hoconsc:mk(Parameters, #{})}]},
    case emqx_hocon:check(Schema, #{<<"args">> => ArgumentsBin}) of
        {ok, #{args := Args}} ->
            Args;
        {error, Error} ->
            ?tp(error, emqx_ai_completion_convert_arguments_error, #{
                error => Error
            }),
            error(Error)
    end.
