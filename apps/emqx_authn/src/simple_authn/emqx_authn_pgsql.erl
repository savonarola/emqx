%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_pgsql).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([ namespace/0
        , roots/0
        , fields/1
        ]).

-export([ refs/0
        , create/1
        , update/2
        , authenticate/2
        , destroy/1
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-postgresql".

roots() -> [config].

fields(config) ->
    [ {mechanism,               {enum, ['password-based']}}
    , {backend,                 {enum, [postgresql]}}
    , {password_hash_algorithm, fun password_hash_algorithm/1}
    , {salt_position,           fun salt_position/1}
    , {query,                   fun query/1}
    ] ++ emqx_authn_schema:common_fields()
    ++ emqx_connector_schema_lib:relational_db_fields()
    ++ emqx_connector_schema_lib:ssl_fields().

password_hash_algorithm(type) -> {enum, [plain, md5, sha, sha256, sha512, bcrypt]};
password_hash_algorithm(default) -> sha256;
password_hash_algorithm(_) -> undefined.

salt_position(type) -> {enum, [prefix, suffix]};
salt_position(default) -> prefix;
salt_position(_) -> undefined.

query(type) -> string();
query(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, config)].

create(
  #{query := Query0,
    password_hash_algorithm := Algorithm,
    salt_position := SaltPosition} = Config) ->
    {Query, PlaceHolders} = parse_query(Query0),
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    State = #{query => Query,
              placeholders => PlaceHolders,
              password_hash_algorithm => Algorithm,
              salt_position => SaltPosition,
              resource_id => ResourceId},
    case emqx_resource:create_local(ResourceId, emqx_connector_pgsql, Config) of
        {ok, already_created} ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

update(Config, State) ->
    case create(Config) of
        {ok, NewState} ->
            ok = destroy(State),
            {ok, NewState};
        {error, Reason} ->
            {error, Reason}
    end.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := Password} = Credential,
             #{query := Query,
               placeholders := PlaceHolders,
               resource_id := ResourceId} = State) ->
    Params = emqx_authn_utils:replace_placeholders(PlaceHolders, Credential),
    case emqx_resource:query(ResourceId, {sql, Query, Params}) of
        {ok, _Columns, []} -> ignore;
        {ok, Columns, [Row | _]} ->
            NColumns = [Name || #column{name = Name} <- Columns],
            Selected = maps:from_list(lists:zip(NColumns, erlang:tuple_to_list(Row))),
            case emqx_authn_utils:check_password(Password, Selected, State) of
                ok ->
                    {ok, emqx_authn_utils:is_superuser(Selected)};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            ?SLOG(error, #{msg => "postgresql_query_failed",
                           resource => ResourceId,
                           reason => Reason}),
            ignore
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% TODO: Support prepare
parse_query(Query) ->
    case re:run(Query, ?RE_PLACEHOLDER, [global, {capture, all, binary}]) of
        {match, Captured} ->
            PlaceHolders = [PlaceHolder || [PlaceHolder] <- Captured],
            Replacements = ["$" ++ integer_to_list(I) || I <- lists:seq(1, length(Captured))],
            NQuery = lists:foldl(fun({PlaceHolder, Replacement}, Query0) ->
                                     re:replace(Query0, "\\" ++ PlaceHolder, Replacement, [{return, binary}])
                                 end, Query, lists:zip(PlaceHolders, Replacements)),
            {NQuery, PlaceHolders};
        nomatch ->
            {Query, []}
    end.
