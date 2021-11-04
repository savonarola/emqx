-module(emqx_swagger_parameter_int_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-behaviour(minirest_api).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2]).

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

-define(assertOk(Method, Path),
        (fun() ->
            {ok, 200, _} = request(Method, uri(Path))
         end)()).

-define(assertOk(Method, Path, Body),
        (fun() ->
            {ok, 200, _} = request(Method, uri(Path), Body)
         end)()).

-define(assertBadRequest(Method, Path),
        (fun() ->
            {ok, 400, _} = request(Method, uri(Path))
         end)()).

-define(assertBadRequest(Method, Path, Body),
        (fun() ->
            {ok, 400, _} = request(Method, uri(Path), Body)
         end)()).


all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_dashboard], fun set_special_configs/1),
    meck:unload(application),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_dashboard]),
    ok.

set_special_configs(emqx_dashboard) ->
    ok = meck:new(application, [passthrough, unstick, no_history, no_link]),
    ok = meck:expect(application, get_key,
                     fun(emqx_dashboard, modules) -> {ok, [?MODULE]};
                        (App, Key) -> meck:passthrough([App, Key])
                     end),

    Config = #{
        default_username => <<"admin">>,
        default_password => <<"public">>,
        listeners => [#{
            protocol => http,
            port => 18083
        }]
    },
    emqx_config:put([emqx_dashboard], Config),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

test_path_param_schema() ->
    {
        "/test/path/param/:filter",
        #{
            operationId => test_path_param_handler,
            get => #{
                parameters => [
                    {filter,
                        mk(hoconsc:enum([assigned, created, mentioned, all]),
                            #{in => path, desc => <<"desc">>, example => "all"})}
                ],
                responses => #{200 => <<"ok">>, 400 => <<"bad request">>}
            }
        }
    }.

test_path_param_handler(get, #{bindings := #{filter := assigned}}) ->
    {200}.

t_test_path_param(_) ->
    ?assertOk(get, "/test/path/param/assigned"),
    ?assertBadRequest(get, "/test/path/param/unknown").


test_query_param_schema() ->
    {
        "/test/query/param",
        #{
            operationId => test_query_param_handler,
            get => #{
                parameters => [
                    {per_page,
                        mk(range(1, 100),
                            #{in => query, desc => <<"desc">>, example => 1})}
                ],
                responses => #{200 => <<"ok">>, 400 => <<"bad request">>}
            }
        }
    }.

test_query_param_handler(get, #{query_string := #{<<"per_page">> := 25}}) ->
    {200}.

t_test_in_filter(_) ->
    ?assertOk(get, "/test/query/param?per_page=25"),
    ?assertBadRequest(get, "/test/query/param?per_page=notanum"),
    ?assertBadRequest(get, "/test/query/param?per_page=0"),
    ?assertBadRequest(get, "/test/query/param?per_page=134").



%%------------------------------------------------------------------------------
%% Swagger
%%------------------------------------------------------------------------------

api_spec() -> emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

test_schemas() ->
    [
       test_path_param_schema(),
       test_query_param_schema()
    ].

paths() -> [Path || {Path, _} <- test_schemas()].

schema(Path) ->
    [Schema] = [S || {P, S} <- test_schemas(), P =:= Path],
    Schema.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

response_data(Response) ->
    #{<<"data">> := Data} = jiffy:decode(Response, [return_maps]),
    Data.

request(Method, Url) ->
    request(Method, Url, []).

request(Method, Url, Body) ->
    Request =
        case Body of
            [] ->
                {Url, [auth_header()]};
            _ ->
                {Url, [auth_header()], "application/json", to_json(Body)}
    end,
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return} } ->
            {ok, Code, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

uri(Path) ->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION]) ++ Path.

auth_header() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

to_json(Map) ->
    jiffy:encode(Map).
