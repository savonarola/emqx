%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_tool_format).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    to_openai_spec/1
]).

-type tool_spec() :: emqx_ai_completion_tool:tool_spec().
-type openai_spec() :: map().

-export_type([
    openai_spec/0
]).

%%--------------------------------------------------------------------
%% Internal constants
%%--------------------------------------------------------------------

-define(DEFAULT_FIELDS, [
    example,
    allowReserved,
    style,
    format,
    readOnly,
    explode,
    maxLength,
    allowEmptyValue,
    deprecated,
    minimum,
    maximum,
    %% is_template is a type property,
    %% but some exceptions are made for them to be field property
    %% for example, HTTP headers (which is a map type)
    is_template
]).
-define(NO_I18N, undefined).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec to_openai_spec(tool_spec()) -> openai_spec().
to_openai_spec(#{name := Name, description := Description, parameters := Parameters}) ->
    #{
        type => <<"function">>,
        function => #{
            name => Name,
            description => Description,
            parameters => hocon_to_swagger(Parameters)
        }
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
%% Hocon to Swagger

hocon_to_swagger(?R_REF(Module, StructName)) ->
    hocon_schema_to_spec(?R_REF(Module, StructName), Module).

struct_to_spec(Module, Field) ->
    Props = hocon_schema_fields(Module, Field),
    parse_object(Props, Module).

hocon_schema_fields(Module, StructName) ->
    case apply(Module, fields, [StructName]) of
        #{fields := Fields, desc := _} ->
            %% evil here, as it's match hocon_schema's internal representation

            %% TODO: make use of desc ?
            Fields;
        Other ->
            Other
    end.

hocon_schema_to_spec(?REF(StructName), LocalModule) ->
    hocon_schema_to_spec(?R_REF(LocalModule, StructName), LocalModule);
hocon_schema_to_spec(?R_REF(Module, StructName), _LocalModule) ->
    struct_to_spec(Module, StructName);
hocon_schema_to_spec(Type, LocalModule) when ?IS_TYPEREFL(Type) ->
    typename_to_spec(lists:flatten(typerefl:name(Type)), LocalModule);
hocon_schema_to_spec(?ARRAY(Item), LocalModule) ->
    Spec = hocon_schema_to_spec(Item, LocalModule),
    #{type => array, items => Spec};
hocon_schema_to_spec(?ENUM(Items), _LocalModule) ->
    #{type => string, enum => Items};
hocon_schema_to_spec(?MAP(Name, Type), LocalModule) ->
    Spec = hocon_schema_to_spec(Type, LocalModule),
    #{
        <<"type">> => object,
        <<"properties">> => #{<<"$", (to_bin(Name))/binary>> => Spec}
    };
hocon_schema_to_spec(?UNION(Types, _DisplayName), LocalModule) ->
    OneOf = lists:foldl(
        fun(Type, Acc) ->
            Spec = hocon_schema_to_spec(Type, LocalModule),
            [Spec | Acc]
        end,
        [],
        hoconsc:union_members(Types)
    ),
    #{<<"oneOf">> => OneOf};
hocon_schema_to_spec(Atom, _LocalModule) when is_atom(Atom) ->
    #{type => string, enum => [Atom]}.

typename_to_spec(TypeStr, Module) ->
    emqx_conf_schema_types:readable_swagger(Module, TypeStr).

to_bin(List) when is_list(List) ->
    case io_lib:printable_list(List) of
        true -> unicode:characters_to_binary(List);
        false -> List
    end;
to_bin(Boolean) when is_boolean(Boolean) -> Boolean;
to_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
to_bin({Type, Args}) ->
    unicode:characters_to_binary(io_lib:format("~ts-~p", [Type, Args]));
to_bin(X) ->
    X.

parse_object(PropList = [_ | _], Module) when is_list(PropList) ->
    {Props, Required} = parse_object_loop(PropList, Module),
    Object = #{<<"type">> => object, <<"properties">> => fix_empty_props(Props)},
    case Required of
        [] -> Object;
        _ -> maps:put(required, Required, Object)
    end;
parse_object(Other, Module) ->
    error(
        {invalid_object, #{
            msg => <<"Object only supports non-empty fields list">>,
            args => Other,
            module => Module
        }}
    ).

parse_object_loop(PropList0, Module) ->
    PropList = filter_hidden_key(PropList0, Module),
    parse_object_loop(PropList, Module, _Props = [], _Required = []).

filter_hidden_key(PropList0, Module) ->
    {PropList1, _} = lists:foldr(
        fun({Key, Hocon} = Prop, {PropAcc, KeyAcc}) ->
            NewKeyAcc = assert_no_duplicated_key(Key, KeyAcc, Module),
            case hoconsc:is_schema(Hocon) andalso is_hidden(Hocon) of
                true -> {PropAcc, NewKeyAcc};
                false -> {[Prop | PropAcc], NewKeyAcc}
            end
        end,
        {[], []},
        PropList0
    ),
    PropList1.

assert_no_duplicated_key(Key, Keys, Module) ->
    KeyBin = emqx_utils_conv:bin(Key),
    case lists:member(KeyBin, Keys) of
        true -> throw({duplicated_key, #{module => Module, key => KeyBin, keys => Keys}});
        false -> [KeyBin | Keys]
    end.

parse_object_loop([], _Module, Props, Required) ->
    {lists:reverse(Props), lists:usort(Required)};
parse_object_loop([{Name, Hocon} | Rest], Module, Props, Required) ->
    NameBin = to_bin(Name),
    case hoconsc:is_schema(Hocon) of
        true ->
            HoconType = hocon_schema:field_schema(Hocon, type),
            Init0 = init_prop([default | ?DEFAULT_FIELDS], #{}, Hocon),
            Init = maps:remove(
                summary,
                trans_description(Init0, Hocon, #{i18n_lang => en})
            ),
            Prop = hocon_schema_to_spec(HoconType, Module),
            NewRequiredAcc =
                case is_required(Hocon) of
                    true -> [NameBin | Required];
                    false -> Required
                end,
            parse_object_loop(
                Rest,
                Module,
                [{NameBin, maps:merge(Prop, Init)} | Props],
                NewRequiredAcc
            );
        false ->
            %% TODO: there is only a handful of such
            %% refactor the schema to unify the two cases
            SubObject = parse_object(Hocon, Module),
            parse_object_loop(
                Rest, Module, [{NameBin, SubObject} | Props], Required
            )
    end.

fix_empty_props([]) ->
    #{};
fix_empty_props(Props) ->
    maps:from_list(Props).

%% return true if the field has 'importance' set to 'hidden'
is_hidden(Hocon) ->
    hocon_schema:is_hidden(Hocon, #{include_importance_up_from => ?IMPORTANCE_NO_DOC}).

is_required(Hocon) ->
    hocon_schema:field_schema(Hocon, required) =:= true.

init_prop(Keys, Init, Type) ->
    lists:foldl(
        fun(Key, Acc) ->
            case hocon_schema:field_schema(Type, Key) of
                undefined -> Acc;
                Schema -> Acc#{Key => format_prop(Key, Schema)}
            end
        end,
        Init,
        Keys
    ).

format_prop(deprecated, Value) when is_boolean(Value) -> Value;
format_prop(deprecated, _) -> true;
format_prop(default, []) -> [];
format_prop(_, Schema) -> to_bin(Schema).

trans_description(Spec, Hocon, Options) ->
    Desc =
        case desc_struct(Hocon) of
            undefined -> undefined;
            ?DESC(_, _) = Struct -> get_i18n(<<"desc">>, Struct, undefined, Options);
            Text -> to_bin(Text)
        end,
    case Desc =:= undefined of
        true ->
            Spec;
        false ->
            Desc1 = binary:replace(Desc, [<<"\n">>], <<"<br/>">>, [global]),
            Spec#{description => Desc1}
    end.

get_i18n(Tag, ?DESC(Namespace, Id), Default, Options) ->
    Lang = get_lang(Options),
    case Lang of
        ?NO_I18N ->
            undefined;
        _ ->
            get_i18n_text(Lang, Namespace, Id, Tag, Default)
    end.

get_i18n_text(Lang, Namespace, Id, Tag, Default) ->
    case emqx_dashboard_desc_cache:lookup(Lang, Namespace, Id, Tag) of
        undefined ->
            Default;
        Text ->
            Text
    end.

%% So far i18n_lang in options is only used at build time.
%% At runtime, it's still the global config which controls the language.
get_lang(#{i18n_lang := Lang}) -> Lang;
get_lang(_) -> emqx:get_config([dashboard, i18n_lang]).

desc_struct(Hocon) ->
    R =
        case hocon_schema:field_schema(Hocon, desc) of
            undefined ->
                case hocon_schema:field_schema(Hocon, description) of
                    undefined -> get_ref_desc(Hocon);
                    Struct1 -> Struct1
                end;
            Struct ->
                Struct
        end,
    ensure_bin(R).

ensure_bin(undefined) -> undefined;
ensure_bin(?DESC(_Namespace, _Id) = Desc) -> Desc;
ensure_bin(Text) -> to_bin(Text).

get_ref_desc(?R_REF(Mod, Name)) ->
    case erlang:function_exported(Mod, desc, 1) of
        true -> Mod:desc(Name);
        false -> undefined
    end;
get_ref_desc(_) ->
    undefined.
