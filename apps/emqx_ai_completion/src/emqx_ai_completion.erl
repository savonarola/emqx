%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ai_completion).

-feature(maybe_expr, enable).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    action/3,
    pre_process_action_args/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type completion() :: #{
    prompt := binary(),
    tool_specs := [emqx_ai_completion_tool_format:openai_spec()]
}.

-export_type([completion/0]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec pre_process_action_args(action, map()) -> #{completion := completion()}.
pre_process_action_args(action, #{prompt := Prompt, tool_names := ToolNames}) ->
    ToolSpecs = emqx_ai_completion_tool:specs(ToolNames),
    #{completion => #{prompt => Prompt, tool_specs => ToolSpecs}}.

action(Selected, _Envs, #{completion := Completion} = _Args) ->
    ?tp(debug, emqx_ai_completion_action, #{
        selected => Selected
    }),
    run_completion(Completion, Selected).

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

-spec run_completion(completion(), map()) -> ok.
run_completion(#{prompt := Prompt, tool_specs := ToolSpecs}, Selected) ->
    SelectedJson = emqx_utils_json:encode(Selected),
    Request = #{
        model => <<"gpt-4o">>,
        messages => [
            #{role => <<"system">>, content => Prompt},
            #{role => <<"user">>, content => SelectedJson}
        ],
        tools => ToolSpecs
    },
    ?tp(warning, emqx_ai_completion_on_message_publish_request, #{
        request => Request
    }),
    case emqx_ai_completion_client:api_post({chat, completions}, Request) of
        {ok, #{<<"choices">> := [#{<<"message">> := #{<<"tool_calls">> := ToolCalls}}]}} ->
            ?tp(warning, emqx_ai_completion_on_message_publish_result, #{
                result => ToolCalls
            }),
            ok = apply_tool_calls(ToolCalls),
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

apply_tool_calls(ToolCalls) ->
    lists:foreach(
        fun(ToolCall) ->
            emqx_ai_completion_tool:run(ToolCall)
        end,
        ToolCalls
    ).

