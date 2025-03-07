%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_tool).

-include_lib("snabbkaffe/include/trace.hrl").

-export([
    specs/1,
    run/1
]).

-type parameters() :: term().

-type tool_spec() :: #{
    name := atom(),
    description := iolist(),
    parameters := parameters(),
    function := fun((list()) -> ok)
}.

-callback tool_specs() -> [tool_spec()].

-define(TOOL_PROVIDERS, [
    emqx_ai_completion_tool_publish
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

specs(BinNames0) ->
    ToolsByName = tools_by_name(),
    lists:map(
        fun(BinName) ->
            case ToolsByName of
                #{BinName := Tool} ->
                    emqx_ai_completion_tool_format:to_openai_spec(Tool);
                _ ->
                    error({tool_not_found, BinName})
            end
        end,
        BinNames0
    ).

run(
    #{
        <<"type">> := <<"function">>,
        <<"function">> := #{<<"name">> := BinName, <<"arguments">> := ArgumentsJson}
    } = _CallSpec
) ->
    case tools_by_name() of
        #{BinName := #{function := Function, parameters := Parameters} = Tool} ->
            ArgumentsBin = emqx_utils_json:decode(ArgumentsJson),
            ?tp(warning, emqx_ai_completion_apply_tool_call, #{
                tool => Tool
            }),
            Arguments = convert_arguments(ArgumentsBin, Parameters),
            ?tp(warning, emqx_ai_completion_convert_arguments, #{
                arguments => Arguments
            }),
            _ = Function(Arguments),
            ok;
        _ ->
            error({tool_not_found, BinName})
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

tools_by_name() ->
    %% TODO
    %% cache in PT
    maps:from_list(
        lists:flatmap(
            fun(Provider) ->
                [
                    {atom_to_binary(Name, utf8), Tool}
                 || #{name := Name} = Tool <- Provider:tool_specs()
                ]
            end,
            ?TOOL_PROVIDERS
        )
    ).

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
