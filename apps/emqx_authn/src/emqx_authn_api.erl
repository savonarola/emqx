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

-module(emqx_authn_api).

-behaviour(minirest_api).
-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include("emqx_authn.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').
-define(CONFLICT, 'CONFLICT').

% Swagger

-export([ api_spec/0
        , paths/0
        , schema/1
        ]).

% Hocon

-export([ roots/0
        , fields/1
        ]).

% minirest handlers

-export([ authenticators/2
        , authenticator/2
        , listener_authenticators/2
        , listener_authenticator/2
        , authenticator_move/2
        , listener_authenticator_move/2
        , authenticator_import_users/2
        , listener_authenticator_import_users/2
        , authenticator_users/2
        , authenticator_user/2
        , listener_authenticator_users/2
        , listener_authenticator_user/2
        ]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() -> [ "/authentication"
           , "/authentication/:id"
           , "/authentication/:id/move"
           , "/authentication/:id/import_users"
           , "/authentication/:id/users"

           , "/listeners/:listener_id/authentication"
           , "/listeners/:listener_id/authentication/:id"
           , "/listeners/:listener_id/authentication/:id/move"
           , "/listeners/:listener_id/authentication/:id/import_users"
           , "/listeners/:listener_id/authentication/:id/users"
           , "/listeners/:listener_id/authentication/:id/users/:user_id"
           ].

roots() -> [ request_user_create
           , request_user_update
           , request_move
           , request_import_users
           , response_user
           , response_no_content
           ].

fields(request_user_create) ->
    [
        {user_id, string()},
        {password, string()},
        {is_superuser, mk(boolean(), #{default => false})}
    ];

fields(request_user_update) ->
    [
        {password, string()},
        {is_superuser, mk(boolean(), #{default => false})}
    ];

fields(request_move) ->
    [{position, string()}];

fields(request_import_users) ->
    [{filename, string()}];

fields(response_user) ->
    [
        {user_id, string()},
        {is_superuser, mk(boolean(), #{default => false})}
    ];

fields(response_no_content) ->
    [{description, string()}].

error_response(Codes, MsgExample) ->
    emqx_dashboard_swagger:error_codes(Codes, MsgExample).

no_content_response() ->
    ref(?MODULE, response_no_content).

%% TODO: tags

schema("/authentication") ->
    #{
        operationId => authenticators,
        get => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"List authenticators for global authentication">>,
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_authentication:authenticator_sc()),
                    authenticator_array_example()
                )
            }
        },
        post => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Create authenticator for global authentication">>,
            requestBody => emqx_authentication:authenticator_sc(),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authentication:authenticator_sc(),
                    authenticator_examples()
                ),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                409 => error_response([?CONFLICT], <<"Conflict">>)
            }
        }
    };

schema("/authentication/:id") ->
    #{
        operationId => authenticator,
        get => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Get authenticator from global authentication chain">>,
            parameters => [{id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authentication:authenticator_sc(),
                    authenticator_examples()
                ),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Update authenticator from global authentication chain">>,
            parameters => [{id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}],
            requestBody => emqx_dashboard_swagger:schema_with_examples(
                emqx_authentication:authenticator_sc(),
                authenticator_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authentication:authenticator_sc(),
                    authenticator_examples()
                ),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>),
                409 => error_response([?CONFLICT], <<"Conflict">>)
            }
        },
        delete => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Delete authenticator from global authentication chain">>,
            parameters => [{id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}],
            requestBody => emqx_dashboard_swagger:schema_with_examples(
                emqx_authentication:authenticator_sc(),
                authenticator_examples()
            ),
            responses => #{
                200 => no_content_response(),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };

schema("/listeners/:listener_id/authentication") ->
    #{
        operationId => listener_authenticators,
        get => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"List authenticators for listener authentication">>,
            parameters => [{listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})}],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_authentication:authenticator_sc()),
                    authenticator_array_example()
                )
            }
        },
        post => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Create authenticator for listener authentication">>,
            parameters => [{listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})}],
            requestBody => emqx_dashboard_swagger:schema_with_examples(
                emqx_authentication:authenticator_sc(),
                authenticator_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authentication:authenticator_sc(),
                    authenticator_examples()
                ),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                409 => error_response([?CONFLICT], <<"Conflict">>)
            }
        }
    };

schema("/listeners/:listener_id/authentication/:id") ->
    #{
        operationId => listener_authenticator,
        get => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Get authenticator from listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}
            ],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authentication:authenticator_sc(),
                    authenticator_examples()
                ),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Update authenticator from listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}
            ],
            requestBody => emqx_dashboard_swagger:schema_with_examples(
                emqx_authentication:authenticator_sc(),
                authenticator_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authentication:authenticator_sc(),
                    authenticator_examples()
                ),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>),
                409 => error_response([?CONFLICT], <<"Conflict">>)
            }
        },
        delete => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Delete authenticator from listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}
            ],
            requestBody => emqx_dashboard_swagger:schema_with_examples(
                emqx_authentication:authenticator_sc(),
                authenticator_examples()
            ),
            responses => #{
                204 => no_content_response(),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };


schema("/authentication/:id/move") ->
    #{
        operationId => authenticator_move,
        post => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Move authenticator in global authentication chain">>,
            parameters => [{id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}],
            requestBody => ref(?MODULE, request_move),
            responses => #{
                204 => no_content_response(),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };

schema("/listeners/:listener_id/authentication/:id/move") ->
    #{
        operationId => listener_authenticator_move,
        post => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Move authenticator in listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}
            ],
            requestBody => ref(?MODULE, request_move),
            responses => #{
                204 => no_content_response(),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };

schema("/authentication/:id/import_users") ->
    #{
        operationId => authenticator_import_users,
        post => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Import users into authenticator in global authentication chain">>,
            parameters => [{id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}],
            requestBody => ref(?MODULE, request_import_users),
            responses => #{
                204 => no_content_response(),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };

schema("/listeners/:listener_id/authentication/:id/import_users") ->
    #{
        operationId => listener_authenticator_import_users,
        post => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Import users into authenticator in listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}
            ],
            requestBody => ref(?MODULE, request_import_users),
            responses => #{
                204 => no_content_response(),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };

schema("/authentication/:id/users") ->
    #{
        operationId => authenticator_users,
        post => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Create users for authenticator in global authentication chain">>,
            parameters => [{id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}],
            requestBody => ref(?MODULE, request_user_create),
            responses => #{
                201 => ref(?MODULE, response_user),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        },
        get => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"List users in authenticator in global authentication chain">>,
            parameters => [{id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}],
            responses => #{
                200 => mk(hoconsc:array(ref(response_user)), #{}),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }

        }
    };

schema("/listeners/:listener_id/authentication/:id/users") ->
    #{
        operationId => listener_authenticator_users,
        post => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Create users for authenticator in global authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}
            ],
            requestBody => ref(?MODULE, request_user_create),
            responses => #{
                201 => ref(?MODULE, response_user),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        },
        get => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"List users in authenticator in listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})}
            ],
            responses => #{
                200 => mk(hoconsc:array(ref(response_user)), #{}),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }

        }
    };

schema("/authentication/:id/users/:user_id") ->
    #{
        operationId => authenticator_user,
        get => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Get user from authenticator in global authentication chain">>,
            parameters => [
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})},
                {user_id, mk(binary(), #{in => path, desc => <<"User ID">>})}
            ],
            responses => #{
                200 => ref(?MODULE, response_user),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Update user in authenticator in global authentication chain">>,
            parameters => [
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})},
                {user_id, mk(binary(), #{in => path, desc => <<"User ID">>})}
            ],
            requestBody => ref(?MODULE, request_user_update),
            responses => #{
                200 => hoconsc:array(ref(response_user), #{}),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        },
        delete => #{
            tags => [<<"authentication">>, <<"global">>],
            description => <<"Update user in authenticator in global authentication chain">>,
            parameters => [
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})},
                {user_id, mk(binary(), #{in => path, desc => <<"User ID">>})}
            ],
            requestBody => ref(?MODULE, request_user_update),
            responses => #{
                204 => no_content_response(),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };

schema("/listeners/:listener_id/authentication/:id/users/:user_id") ->
    #{
        operationId => listener_authenticator_user,
        get => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Get user from authenticator in listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})},
                {user_id, mk(binary(), #{in => path, desc => <<"User ID">>})}
            ],
            responses => #{
                200 => ref(?MODULE, response_user),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Update user in authenticator in listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})},
                {user_id, mk(binary(), #{in => path, desc => <<"User ID">>})}
            ],
            requestBody => ref(?MODULE, request_user_update),
            responses => #{
                200 => mk(hoconsc:array(ref(response_user)), #{}),
                400 => error_response([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }

        },
        delete => #{
            tags => [<<"authentication">>, <<"listener">>],
            description => <<"Update user in authenticator in listener authentication chain">>,
            parameters => [
                {listener_id, mk(binary(), #{in => path, desc => <<"Listener ID">>})},
                {id, mk(binary(), #{in => path, desc => <<"Authenticator ID">>})},
                {user_id, mk(binary(), #{in => path, desc => <<"User ID">>})}
            ],
            requestBody => ref(?MODULE, request_user_update),
            responses => #{
                204 => no_content_response(),
                404 => error_response([?NOT_FOUND], <<"Not Found">>)
            }
        }
    }.

authenticators(post, #{body := Config}) ->
    create_authenticator([authentication], ?GLOBAL, Config);

authenticators(get, _Params) ->
    list_authenticators([authentication]).

authenticator(get, #{bindings := #{id := AuthenticatorID}}) ->
    list_authenticator([authentication], AuthenticatorID);

authenticator(put, #{bindings := #{id := AuthenticatorID}, body := Config}) ->
    update_authenticator([authentication], ?GLOBAL, AuthenticatorID, Config);

authenticator(delete, #{bindings := #{id := AuthenticatorID}}) ->
    delete_authenticator([authentication], ?GLOBAL, AuthenticatorID).

listener_authenticators(post, #{bindings := #{listener_id := ListenerID}, body := Config}) ->
    with_listener(ListenerID,
                  fun(Type, Name) ->
                        create_authenticator([listeners, Type, Name, authentication],
                                          ListenerID,
                                          Config)
                  end);

listener_authenticators(get, #{bindings := #{listener_id := ListenerID}}) ->
    with_listener(ListenerID,
                  fun(Type, Name) ->
                        list_authenticators([listeners, Type, Name, authentication])
                  end).

listener_authenticator(get, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}}) ->
    with_listener(ListenerID,
                  fun(Type, Name) ->
                        list_authenticator([listeners, Type, Name, authentication],
                                       AuthenticatorID)
                  end);
listener_authenticator(put, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}, body := Config}) ->
    with_listener(ListenerID,
                  fun(Type, Name) ->
                        update_authenticator([listeners, Type, Name, authentication],
                                             ListenerID,
                                             AuthenticatorID,
                                             Config)
                  end);
listener_authenticator(delete, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}}) ->
    with_listener(ListenerID,
                  fun(Type, Name) ->
                        delete_authenticator([listeners, Type, Name, authentication],
                                             ListenerID,
                                             AuthenticatorID)
                  end).

authenticator_move(post, #{bindings := #{id := AuthenticatorID}, body := #{<<"position">> := Position}}) ->
    move_authenitcator([authentication], ?GLOBAL, AuthenticatorID, Position);
authenticator_move(post, #{bindings := #{id := _}, body := _}) ->
    serialize_error({missing_parameter, position}).

listener_authenticator_move(post, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}, body := #{<<"position">> := Position}}) ->
    with_listener(ListenerID,
                  fun(Type, Name) ->
                        move_authenitcator([listeners, Type, Name, authentication],
                                           ListenerID,
                                           AuthenticatorID,
                                           Position)
                  end);
listener_authenticator_move(post, #{bindings := #{listener_id := _, id := _}, body := _}) ->
    serialize_error({missing_parameter, position}).

authenticator_import_users(post, #{bindings := #{id := AuthenticatorID}, body := #{<<"filename">> := Filename}}) ->
    case ?AUTHN:import_users(?GLOBAL, AuthenticatorID, Filename) of
        ok -> {204};
        {error, Reason} -> serialize_error(Reason)
    end;
authenticator_import_users(post, #{bindings := #{id := _}, body := _}) ->
    serialize_error({missing_parameter, filename}).

listener_authenticator_import_users(post, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}, body := #{<<"filename">> := Filename}}) ->
    case ?AUTHN:import_users(ListenerID, AuthenticatorID, Filename) of
        ok -> {204};
        {error, Reason} -> serialize_error(Reason)
    end;
listener_authenticator_import_users(post, #{bindings := #{listener_id := _, id := _}, body := _}) ->
    serialize_error({missing_parameter, filename}).

authenticator_users(post, #{bindings := #{id := AuthenticatorID}, body := UserInfo}) ->
    add_user(?GLOBAL, AuthenticatorID, UserInfo);
authenticator_users(get, #{bindings := #{id := AuthenticatorID}}) ->
    list_users(?GLOBAL, AuthenticatorID).

authenticator_user(put, #{bindings := #{id := AuthenticatorID,
                            user_id := UserID}, body := UserInfo}) ->
    update_user(?GLOBAL, AuthenticatorID, UserID, UserInfo);
authenticator_user(get, #{bindings := #{id := AuthenticatorID, user_id := UserID}}) ->
    find_user(?GLOBAL, AuthenticatorID, UserID);
authenticator_user(delete, #{bindings := #{id := AuthenticatorID, user_id := UserID}}) ->
    delete_user(?GLOBAL, AuthenticatorID, UserID).

listener_authenticator_users(post, #{bindings := #{listener_id := ListenerID,
                             id := AuthenticatorID}, body := UserInfo}) ->
    add_user(ListenerID, AuthenticatorID, UserInfo);
listener_authenticator_users(get, #{bindings := #{listener_id := ListenerID,
                            id := AuthenticatorID}}) ->
    list_users(ListenerID, AuthenticatorID).

listener_authenticator_user(put, #{bindings := #{listener_id := ListenerID,
                            id := AuthenticatorID,
                            user_id := UserID}, body := UserInfo}) ->
    update_user(ListenerID, AuthenticatorID, UserID, UserInfo);
listener_authenticator_user(get, #{bindings := #{listener_id := ListenerID,
                            id := AuthenticatorID,
                            user_id := UserID}}) ->
    find_user(ListenerID, AuthenticatorID, UserID);
listener_authenticator_user(delete, #{bindings := #{listener_id := ListenerID,
                               id := AuthenticatorID,
                               user_id := UserID}}) ->
    delete_user(ListenerID, AuthenticatorID, UserID).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

with_listener(ListenerID, Fun) ->
    case find_listener(ListenerID) of
        {ok, {Type, Name}} ->
           Fun(Type, Name);
        {error, Reason} ->
            serialize_error(Reason)
    end.

find_listener(ListenerID) ->
    {Type, Name} = emqx_listeners:parse_listener_id(ListenerID),
    case emqx_config:find([listeners, Type, Name]) of
        {not_found, _, _} ->
            {error, {not_found, {listener, ListenerID}}};
        {ok, _} ->
            {ok, {Type, Name}}
    end.

create_authenticator(ConfKeyPath, ChainName, Config) ->
    case update_config(ConfKeyPath, {create_authenticator, to_atom(ChainName), Config}) of
        {ok, #{post_config_update := #{?AUTHN := #{id := ID}},
            raw_config := AuthenticatorsConfig}} ->
            {ok, AuthenticatorConfig} = find_config(ID, AuthenticatorsConfig),
            {200, maps:put(id, ID, convert_certs(fill_defaults(AuthenticatorConfig)))};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end.

list_authenticators(ConfKeyPath) ->
    AuthenticatorsConfig = get_raw_config_with_defaults(ConfKeyPath),
    NAuthenticators = [maps:put(id, ?AUTHN:generate_id(AuthenticatorConfig), convert_certs(AuthenticatorConfig))
                        || AuthenticatorConfig <- AuthenticatorsConfig],
    {200, NAuthenticators}.

list_authenticator(ConfKeyPath, AuthenticatorID) ->
    AuthenticatorsConfig = get_raw_config_with_defaults(ConfKeyPath),
    case find_config(AuthenticatorID, AuthenticatorsConfig) of
        {ok, AuthenticatorConfig} ->
            {200, maps:put(id, AuthenticatorID, convert_certs(AuthenticatorConfig))};
        {error, Reason} ->
            serialize_error(Reason)
    end.

update_authenticator(ConfKeyPath, ChainName, AuthenticatorID, Config) ->
    case update_config(ConfKeyPath, {update_authenticator, to_atom(ChainName), AuthenticatorID, Config}) of
        {ok, #{post_config_update := #{?AUTHN := #{id := ID}},
               raw_config := AuthenticatorsConfig}} ->
            {ok, AuthenticatorConfig} = find_config(ID, AuthenticatorsConfig),
            {200, maps:put(id, ID, convert_certs(fill_defaults(AuthenticatorConfig)))};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end.

delete_authenticator(ConfKeyPath, ChainName0, AuthenticatorID) ->
    ChainName = to_atom(ChainName0),
    case update_config(ConfKeyPath, {delete_authenticator, ChainName, AuthenticatorID}) of
        {ok, _} ->
            {204};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end.

move_authenitcator(ConfKeyPath, ChainName0, AuthenticatorID, Position) ->
    ChainName = to_atom(ChainName0),
    case parse_position(Position) of
        {ok, NPosition} ->
            case update_config(ConfKeyPath, {move_authenticator, ChainName, AuthenticatorID, NPosition}) of
                {ok, _} ->
                    {204};
                {error, {_, _, Reason}} ->
                    serialize_error(Reason)
            end;
        {error, Reason} ->
            serialize_error(Reason)
    end.

add_user(ChainName0, AuthenticatorID, #{<<"user_id">> := UserID, <<"password">> := Password} = UserInfo) ->
    ChainName = to_atom(ChainName0),
    IsSuperuser = maps:get(<<"is_superuser">>, UserInfo, false),
    case ?AUTHN:add_user(ChainName, AuthenticatorID, #{ user_id => UserID
                                                      , password => Password
                                                      , is_superuser => IsSuperuser}) of
        {ok, User} ->
            {201, User};
        {error, Reason} ->
            serialize_error(Reason)
    end;
add_user(_, _, #{<<"user_id">> := _}) ->
    serialize_error({missing_parameter, password});
add_user(_, _, _) ->
    serialize_error({missing_parameter, user_id}).

update_user(ChainName0, AuthenticatorID, UserID, UserInfo) ->
    ChainName = to_atom(ChainName0),
    case maps:with([<<"password">>, <<"is_superuser">>], UserInfo) =:= #{} of
        true ->
            serialize_error({missing_parameter, password});
        false ->
            case ?AUTHN:update_user(ChainName, AuthenticatorID, UserID, UserInfo) of
                {ok, User} ->
                    {200, User};
                {error, Reason} ->
                    serialize_error(Reason)
            end
    end.

find_user(ChainName0, AuthenticatorID, UserID) ->
    ChainName = to_atom(ChainName0),
    case ?AUTHN:lookup_user(ChainName, AuthenticatorID, UserID) of
        {ok, User} ->
            {200, User};
        {error, Reason} ->
            serialize_error(Reason)
    end.

delete_user(ChainName0, AuthenticatorID, UserID) ->
    ChainName = to_atom(ChainName0),
    case ?AUTHN:delete_user(ChainName, AuthenticatorID, UserID) of
        ok ->
            {204};
        {error, Reason} ->
            serialize_error(Reason)
    end.

list_users(ChainName0, AuthenticatorID) ->
    ChainName = to_atom(ChainName0),
    case ?AUTHN:list_users(ChainName, AuthenticatorID) of
        {ok, Users} ->
            {200, Users};
        {error, Reason} ->
            serialize_error(Reason)
    end.

update_config(Path, ConfigRequest) ->
    emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).

get_raw_config_with_defaults(ConfKeyPath) ->
    NConfKeyPath = [atom_to_binary(Key, utf8) || Key <- ConfKeyPath],
    RawConfig = emqx_map_lib:deep_get(NConfKeyPath, emqx_config:get_raw([]), []),
    to_list(fill_defaults(RawConfig)).

find_config(AuthenticatorID, AuthenticatorsConfig) ->
    case [AC || AC <- to_list(AuthenticatorsConfig), AuthenticatorID =:= ?AUTHN:generate_id(AC)] of
        [] -> {error, {not_found, {authenticator, AuthenticatorID}}};
        [AuthenticatorConfig] -> {ok, AuthenticatorConfig}
    end.

fill_defaults(Config) ->
    #{<<"authentication">> := CheckedConfig} =
        hocon_schema:check_plain(?AUTHN, #{<<"authentication">> => Config},
                                 #{only_fill_defaults => true}),
    CheckedConfig.

convert_certs(#{<<"ssl">> := SSLOpts} = Config) ->
    NSSLOpts = lists:foldl(fun(K, Acc) ->
                               case maps:get(K, Acc, undefined) of
                                   undefined -> Acc;
                                   Filename ->
                                       {ok, Bin} = file:read_file(Filename),
                                       Acc#{K => Bin}
                               end
                           end, SSLOpts, [<<"certfile">>, <<"keyfile">>, <<"cacertfile">>]),
    Config#{<<"ssl">> => NSSLOpts};
convert_certs(Config) ->
    Config.

serialize_error({not_found, {authenticator, ID}}) ->
    {404, #{code => <<"NOT_FOUND">>,
            message => list_to_binary(
                io_lib:format("Authenticator '~ts' does not exist", [ID])
            )}};

serialize_error({not_found, {listener, ID}}) ->
    {404, #{code => <<"NOT_FOUND">>,
            message => list_to_binary(
                io_lib:format("Listener '~ts' does not exist", [ID])
            )}};

serialize_error({not_found, {chain, ?GLOBAL}}) ->
    {500, #{code => <<"INTERNAL_SERVER_ERROR">>,
            message => <<"Authentication status is abnormal">>}};

serialize_error({not_found, {chain, Name}}) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => list_to_binary(
                io_lib:format("No authentication has been create for listener '~ts'", [Name])
            )}};

serialize_error({already_exists, {authenticator, ID}}) ->
    {409, #{code => <<"ALREADY_EXISTS">>,
            message => list_to_binary(
                io_lib:format("Authenticator '~ts' already exist", [ID])
            )}};

serialize_error(no_available_provider) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => <<"Unsupported authentication type">>}};

serialize_error(change_of_authentication_type_is_not_allowed) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => <<"Change of authentication type is not allowed">>}};

serialize_error(unsupported_operation) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => <<"Operation not supported in this authentication type">>}};

serialize_error({save_cert_to_file, invalid_certificate}) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => <<"Invalid certificate">>}};

serialize_error({save_cert_to_file, {_, Reason}}) ->
    {500, #{code => <<"INTERNAL_SERVER_ERROR">>,
            message => list_to_binary(
                io_lib:format("Cannot save certificate to file due to '~p'", [Reason])
            )}};

serialize_error({missing_parameter, Name}) ->
    {400, #{code => <<"MISSING_PARAMETER">>,
            message => list_to_binary(
                io_lib:format("The input parameter '~p' that is mandatory for processing this request is not supplied", [Name])
            )}};

serialize_error({invalid_parameter, Name}) ->
    {400, #{code => <<"INVALID_PARAMETER">>,
            message => list_to_binary(
                io_lib:format("The value of input parameter '~p' is invalid", [Name])
            )}};

serialize_error(Reason) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => list_to_binary(io_lib:format("~p", [Reason]))}}.

parse_position(<<"top">>) ->
    {ok, top};
parse_position(<<"bottom">>) ->
    {ok, bottom};
parse_position(<<"before:", Before/binary>>) ->
    {ok, {before, Before}};
parse_position(_) ->
    {error, {invalid_parameter, position}}.

to_list(M) when is_map(M) ->
    [M];
to_list(L) when is_list(L) ->
    L.

to_atom(B) when is_binary(B) ->
    binary_to_atom(B);
to_atom(A) when is_atom(A) ->
    A.


authenticator_array_example() ->
    [Config || #{value := Config} <- maps:values(authenticator_examples())].

authenticator_examples() ->
    #{
        'password-based:built-in-database' => #{
            summary => <<"Built-in password-based authentication">>,
            value => #{
                mechanism => <<"password-based">>,
                backend => <<"built-in-database">>,
                user_id_type => <<"username">>,
                password_hash_algorithm => #{
                    name => <<"sha256">>
                }
            }
        },
        'password-based:http-server' => #{
            summary => <<"Password-based authentication througth external HTTP API">>,
            value => #{
                mechanism => <<"password-based">>,
                backend => <<"http-server">>,
                method => <<"post">>,
                url => <<"http://localhost:80/login">>,
                headers => #{
                    <<"content-type">> => <<"application/json">>
                },
                body => #{
                    <<"username">> => <<"${mqtt-username}">>,
                    <<"password">> => <<"${mqtt-password}">>
                }
            }
        },
        'jwt' => #{
            summary => <<"JWT authentication">>,
            value => #{
                mechanism => <<"jwt">>,
                use_jwks => false,
                algorithm => <<"hmac-based">>,
                secret => <<"mysecret">>,
                secret_base64_encoded => false,
                verify_claims => #{
                    <<"username">> => <<"${mqtt-username}">>
                }
            }
        },
        'password-based:mongodb' => #{
            summary => <<"Password-based authentication with MongoDB backend">>,
            value => #{
                mechanism => <<"password-based">>,
                backend => <<"mongodb">>,
                server => <<"127.0.0.1:27017">>,
                database => example,
                collection => users,
                selector => #{
                    username => <<"${mqtt-username}">>
                },
                password_hash_field => <<"password_hash">>,
                salt_field => <<"salt">>,
                is_superuser_field => <<"is_superuser">>,
                password_hash_algorithm => <<"sha256">>,
                salt_position => <<"prefix">>
            }
        },
        'password-based:redis' => #{
            summary => <<"Password-based authentication with Redis backend">>,
            value => #{
                mechanism => <<"password-based">>,
                backend => <<"redis">>,
                server => <<"127.0.0.1:6379">>,
                database => 0,
                query => <<"HMGET ${mqtt-username} password_hash salt">>,
                password_hash_algorithm => <<"sha256">>,
                salt_position => <<"prefix">>
            }
        }
    }.
