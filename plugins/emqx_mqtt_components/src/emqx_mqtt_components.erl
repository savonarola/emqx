%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_components).
-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-export([start_link/0, hook/0, unhook/0, cleanup_results/0]).
-export([on_subscribe/3, on_unsubscribed/3, on_publish/1, on_disconnected/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

cleanup_results() ->
    gen_server:call(?MODULE, cleanup_results).

hook() ->
    lists:foreach(
        fun({Hook, Callback}) ->
            Priority =
                case Hook of
                    'message.publish' -> ?HP_RETAINER + 1;
                    _ -> ?HP_LOWEST
                end,
            emqx_hooks:add(Hook, {?MODULE, Callback, []}, Priority)
        end,
        hooks()
    ).

unhook() ->
    lists:foreach(
        fun({Hook, Callback}) -> emqx_hooks:del(Hook, {?MODULE, Callback}) end,
        hooks()
    ).

on_subscribe(#{clientid := ClientId}, _Properties, Filters) ->
    %% TODO: Require declarations in the connection's first SUBSCRIBE packet.
    Allowed = gen_server:call(?MODULE, {subscribe, self(), ClientId, Filters}),
    {ok, Allowed}.

on_unsubscribed(_ClientInfo, Topic, _Options) ->
    gen_server:call(?MODULE, {unsubscribed, self(), Topic}).

on_publish(Msg = #message{topic = Topic}) ->
    case is_managed(Topic) of
        true ->
            ok = gen_server:call(?MODULE, {publish, self(), Msg}),
            {stop, emqx_message:set_header(allow_publish, false, Msg)};
        false ->
            {ok, Msg}
    end.

on_disconnected(_ClientInfo, _Reason, _ConnInfo) ->
    gen_server:call(?MODULE, {disconnect, self()}).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    {ok, #{components => #{}, effects => #{}, cleanup_results => []}}.

handle_call(cleanup_results, _From, State = #{cleanup_results := Results}) ->
    {reply, Results, State};
handle_call({subscribe, Pid, ClientId, Filters}, _From, State) ->
    {Allowed, Next} = subscribe_request(Pid, ClientId, Filters, State),
    {reply, Allowed, Next};
handle_call({unsubscribed, Pid, Topic}, _From, State) ->
    {reply, ok, unsubscribed(Pid, Topic, State)};
handle_call({publish, Pid, Msg}, _From, State) ->
    {reply, ok, publish(Pid, Msg, State)};
handle_call({disconnect, Pid}, _From, State) ->
    {reply, ok, disconnect(Pid, State)}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    {noreply, disconnect(Pid, State)};
handle_info(
    {check_subscriptions, Pid, Activation, Topics, Msg, Attempts},
    State = #{components := Components}
) ->
    Next =
        case Components of
            #{
                Pid := C = #{
                    state := starting, activation := Activation, subscriptions_pending := true
                }
            } ->
                check_subscriptions(Pid, C, Topics, Msg, Attempts, State);
            _ ->
                State
        end,
    {noreply, Next};
handle_info({check_unsubscribe, Id, Attempts}, State = #{effects := Effects}) ->
    Next =
        case Effects of
            #{
                Id := E = #{
                    type := state_subscription, owner := Pid, key := Key, cleanup := requested
                }
            } ->
                case
                    is_process_alive(Pid) andalso
                        lists:keymember(Key, 1, emqx_broker:subscriptions(Pid))
                of
                    false ->
                        settle_cleanup(State#{effects := Effects#{Id := E#{cleanup := retracted}}});
                    true when Attempts > 0 ->
                        erlang:send_after(10, self(), {check_unsubscribe, Id, Attempts - 1}),
                        State;
                    true ->
                        settle_cleanup(State#{effects := Effects#{Id := E#{cleanup := failed}}})
                end;
            _ ->
                State
        end,
    {noreply, Next};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

hooks() ->
    [
        {'client.subscribe', on_subscribe},
        {'session.unsubscribed', on_unsubscribed},
        {'message.publish', on_publish},
        {'client.disconnected', on_disconnected}
    ].

is_declaration(<<"$provide/", _/binary>>) -> true;
is_declaration(<<"$consume/", _/binary>>) -> true;
is_declaration(_) -> false.

is_managed(#share{topic = Topic}) -> is_managed(Topic);
is_managed(<<"$state/", _/binary>>) -> true;
is_managed(<<"$component-admin/", _/binary>>) -> true;
is_managed(<<"$component/", _/binary>>) -> true;
is_managed(<<"$service/", _/binary>>) -> true;
is_managed(Topic) -> is_declaration(Topic).

subscribe_request(Pid, ClientId, Filters, State) ->
    Declarations = [Topic || {Topic, _} <- Filters, is_declaration(Topic)],
    Declared =
        case Declarations of
            [] ->
                {ok, State};
            _ ->
                subscribe(Pid, [events_topic(ClientId)]),
                declare(Pid, ClientId, Declarations, State)
        end,
    case Declared of
        {error, Reason} ->
            event(Pid, ClientId, #{event => error, reason => Reason}),
            {[], State};
        {ok, Next0} ->
            Next = activate_waiting(Next0),
            #{components := Components} = Next,
            case
                lists:all(
                    fun({Topic, _}) -> subscription_allowed(Pid, ClientId, Topic, Components) end,
                    Filters
                )
            of
                true ->
                    {Filters,
                        lists:foldl(
                            fun({Topic, _}, Acc) -> record_subscription(Pid, Topic, Acc) end,
                            Next,
                            Filters
                        )};
                false ->
                    event(Pid, ClientId, #{event => error, reason => subscription_not_allowed}),
                    {[], Next}
            end
    end.

subscription_allowed(_Pid, _ClientId, Topic = #share{}, _Components) ->
    not is_managed(Topic);
subscription_allowed(Pid, ClientId, Topic, Components) ->
    case
        Topic =:= events_topic(ClientId) orelse not is_managed(Topic) orelse is_declaration(Topic)
    of
        true ->
            true;
        false ->
            case Components of
                #{Pid := #{state := Status, provides := Provides, bindings := Bindings}} when
                    Status =:= starting; Status =:= active
                ->
                    case Topic of
                        <<"$state/", _/binary>> ->
                            lists:member(Topic, Provides) orelse maps:is_key(Topic, Bindings);
                        <<"$service/", _/binary>> when Status =:= active ->
                            lists:member(
                                Topic, lists:append([provider_filters(Key) || Key <- Provides])
                            );
                        _ ->
                            false
                    end;
                _ ->
                    false
            end
    end.

record_subscription(Pid, <<"$state/", _/binary>> = Topic, State = #{components := Components}) ->
    case Components of
        #{Pid := C = #{bindings := #{Topic := Provider}}} ->
            {_Id, Next} = local_effect(Pid, C, Provider, state_subscription, Topic, State),
            Next;
        _ ->
            State
    end;
record_subscription(_Pid, _Topic, State) ->
    State.

unsubscribed(Pid, Topic, State = #{effects := Effects}) ->
    Next = maps:map(
        fun
            (
                _Id,
                E = #{type := state_subscription, owner := Owner, key := Key, cleanup := Cleanup}
            ) when
                Owner =:= Pid, Key =:= Topic, Cleanup =:= pending orelse Cleanup =:= requested
            ->
                E#{cleanup := retracted};
            (_Id, E) ->
                E
        end,
        Effects
    ),
    settle_cleanup(State#{effects := Next}).

declare(Pid, ClientId, Declarations, State = #{components := Components}) ->
    try
        check(not maps:is_key(Pid, Components), already_declared),
        Parsed = [parse_declaration(Topic) || Topic <- Declarations],
        Provides = [Key || {provide, Key} <- Parsed],
        Consumes = [Key || {consume, Key} <- Parsed],
        check(length(Parsed) =:= length(lists:usort(Parsed)), duplicate_declaration),
        check(not lists:any(fun(K) -> lists:member(K, Consumes) end, Provides), self_dependency),
        Existing = lists:append([
            Keys
         || #{provides := Keys} <- maps:values(Components)
        ]),
        check(not lists:any(fun(K) -> lists:member(K, Existing) end, Provides), provider_conflict),
        Component = #{
            clientid => ClientId,
            connected => true,
            activation_block => none,
            state => inactive,
            provides => Provides,
            consumes => Consumes,
            bindings => #{},
            effects => []
        },
        NewComponents = Components#{Pid => Component},
        check(not cyclic(Pid, NewComponents, []), dependency_cycle),
        Ref = monitor(process, Pid),
        {ok, State#{components := NewComponents#{Pid := Component#{monitor => Ref}}}}
    catch
        throw:Reason -> {error, Reason}
    end.

parse_declaration(<<"$provide/service/", Key/binary>>) ->
    {provide, service_key(Key)};
parse_declaration(<<"$consume/service/", Key/binary>>) ->
    {consume, service_key(Key)};
parse_declaration(<<"$provide/state/", Key/binary>>) ->
    {provide, state_key(Key)};
parse_declaration(<<"$consume/state/", Key/binary>>) ->
    {consume, state_key(Key)};
parse_declaration(_) ->
    throw(unsupported_declaration).

service_key(Key) ->
    check(Key =/= <<>> andalso binary:match(Key, [<<"+">>, <<"#">>]) =:= nomatch, invalid_service),
    <<"$service/", Key/binary>>.

state_key(Key) ->
    check(
        Key =/= <<>> andalso binary:match(Key, [<<"+">>, <<"#">>]) =:= nomatch, invalid_state_topic
    ),
    <<"$state/", Key/binary>>.

check(true, _Reason) -> ok;
check(false, Reason) -> throw(Reason).

cyclic(Pid, Components, Path) ->
    case lists:member(Pid, Path) of
        true ->
            true;
        false ->
            #{consumes := Keys} = maps:get(Pid, Components),
            lists:any(
                fun({Provider, #{provides := Provides}}) ->
                    lists:any(fun(K) -> lists:member(K, Provides) end, Keys) andalso
                        cyclic(Provider, Components, [Pid | Path])
                end,
                maps:to_list(Components)
            )
    end.

activate_waiting(State = #{components := Components}) ->
    Providers = maps:from_list([
        {Key, {Pid, Activation}}
     || {Pid, #{state := active, provides := Keys, activation := Activation}} <- maps:to_list(
            Components
        ),
        Key <- Keys
    ]),
    Next = maps:map(
        fun
            (
                Pid,
                C = #{
                    state := inactive,
                    activation_block := none,
                    consumes := Keys,
                    clientid := ClientId
                }
            ) ->
                case lists:all(fun(K) -> maps:is_key(K, Providers) end, Keys) of
                    true ->
                        event(Pid, ClientId, #{event => initialize}),
                        C#{
                            state := starting,
                            activation => erlang:unique_integer([positive, monotonic]),
                            bindings := maps:with(Keys, Providers)
                        };
                    false ->
                        C
                end;
            (_Pid, C) ->
                C
        end,
        Components
    ),
    State#{components := Next}.

publish(Pid, Msg = #message{topic = <<"$component-admin/", Action/binary>>}, State) ->
    admin(Pid, Action, Msg, State);
publish(Pid, Msg = #message{topic = <<"$component/reply/", Id/binary>>}, State) ->
    service_reply(Pid, Id, Msg, State);
publish(Pid, Msg = #message{topic = <<"$component/retracted/", Id/binary>>}, State) ->
    retract_reply(Pid, Id, Msg, State);
publish(Pid, Msg, State = #{components := Components}) ->
    case Components of
        #{Pid := C} ->
            operation(Pid, C, Msg, State);
        _ ->
            reply(Pid, Msg, #{event => error, reason => not_declared}),
            State
    end.

operation(
    Pid,
    C = #{state := starting},
    Msg = #message{topic = <<"$component/abort">>, flags = #{retain := false}},
    State
) ->
    Next = put_component(Pid, C#{activation_block := aborted, abort_request => Msg}, State),
    settle_cleanup(stop_component(Pid, Next));
operation(
    Pid,
    C = #{state := inactive, activation_block := Block},
    Msg = #message{topic = <<"$component/retry">>, flags = #{retain := false}},
    State
) ->
    Next =
        case Block of
            aborted -> put_component(Pid, C#{activation_block := none}, State);
            _ -> State
        end,
    reply(Pid, Msg, #{event => retry_accepted}),
    activate_waiting(Next);
operation(Pid, _C, Msg = #message{topic = Topic}, State) when
    Topic =:= <<"$component/abort">>; Topic =:= <<"$component/retry">>
->
    reply(Pid, Msg, #{event => error, reason => invalid_state_or_operation}),
    State;
operation(Pid, #{state := starting, subscriptions_pending := true}, Msg, State) ->
    reply(Pid, Msg, #{event => error, reason => initialization_pending}),
    State;
operation(
    Pid,
    C = #{state := starting, effects := Ids, provides := Keys},
    Msg = #message{topic = <<"$component/ready">>, flags = #{retain := false}},
    State = #{effects := Effects}
) ->
    case lists:all(fun(Id) -> maps:get(status, maps:get(Id, Effects)) =:= completed end, Ids) of
        true ->
            Topics = lists:append([provider_filters(Key) || Key <- Keys]),
            subscribe(Pid, Topics),
            check_subscriptions(Pid, C, Topics, Msg, 100, State);
        false ->
            reply(Pid, Msg, #{event => error, reason => initialization_pending}),
            State
    end;
operation(
    Pid,
    C = #{state := stopping, local_cleanup := requested},
    #message{topic = <<"$component/cleanup_complete">>, flags = #{retain := false}},
    State
) ->
    settle_cleanup(put_component(Pid, C#{local_cleanup := complete}, State));
operation(
    Pid,
    C = #{state := Status},
    Msg = #message{topic = <<"$component/release/", Id/binary>>, flags = #{retain := false}},
    State
) when
    Status =:= starting; Status =:= active
->
    release(Pid, C, Id, Msg, State);
operation(
    Pid,
    C = #{state := Status, provides := Keys},
    Msg = #message{topic = <<"$state/", _/binary>> = Key, flags = #{retain := true}},
    State
) when
    Status =:= starting; Status =:= active
->
    case lists:member(Key, Keys) of
        true ->
            write_state(Pid, C, Key, Msg, State);
        false ->
            reply(Pid, Msg, #{event => error, reason => undeclared_operation}),
            State
    end;
operation(
    Pid,
    C = #{state := Status, bindings := Bindings},
    Msg = #message{topic = <<"$service/", _/binary>> = Key, flags = #{retain := false}},
    State
) when
    Status =:= starting; Status =:= active
->
    case Bindings of
        #{Key := Provider} ->
            apply_service(Pid, Provider, Key, C, Msg, State);
        _ ->
            reply(Pid, Msg, #{event => error, reason => undeclared_operation}),
            State
    end;
operation(Pid, _C, Msg, State) ->
    reply(Pid, Msg, #{event => error, reason => invalid_state_or_operation}),
    State.

apply_service(
    Pid,
    {Provider, ProviderActivation},
    Key,
    C = #{effects := Ids, activation := Activation},
    Msg,
    State = #{effects := Effects}
) ->
    Props = emqx_message:get_header(properties, Msg, #{}),
    case Props of
        #{'Response-Topic' := ResponseTopic} ->
            Id = effect_id(),
            Effect = #{
                type => service,
                owner => Pid,
                owner_activation => Activation,
                provider => Provider,
                provider_activation => ProviderActivation,
                key => Key,
                response_topic => ResponseTopic,
                status => pending,
                cleanup => pending
            },
            Next = put_component(
                Pid,
                C#{effects := [Id | Ids]},
                State#{effects := Effects#{Id => Effect}}
            ),
            Forward = emqx_message:set_header(
                properties,
                Props#{'Response-Topic' := <<"$component/reply/", Id/binary>>},
                Msg#message{topic = <<Key/binary, "/apply/", Id/binary>>}
            ),
            deliver(Provider, <<Key/binary, "/apply/+">>, Forward),
            Next;
        _ ->
            reply(Pid, Msg, #{event => error, reason => response_topic_required}),
            State
    end.

effect_id() -> integer_to_binary(erlang:unique_integer([positive, monotonic])).

effect_response(Id, Msg) ->
    Props = emqx_message:get_header(properties, Msg, #{}),
    UserProps = [
        {K, V}
     || {K, V} <- maps:get('User-Property', Props, []), K =/= <<"component-effect-id">>
    ],
    emqx_message:set_header(
        properties, Props#{'User-Property' => [{<<"component-effect-id">>, Id} | UserProps]}, Msg
    ).

local_effect(
    Pid,
    C = #{effects := Ids, activation := Activation},
    {Provider, ProviderActivation},
    Type,
    Key,
    State = #{effects := Effects}
) ->
    Existing = [
        Id
     || Id <- Ids,
        #{type := T, key := K, cleanup := pending} <- [maps:get(Id, Effects)],
        T =:= Type,
        K =:= Key
    ],
    case Existing of
        [Id] ->
            {Id, State};
        [] ->
            Id = effect_id(),
            E = #{
                type => Type,
                owner => Pid,
                owner_activation => Activation,
                provider => Provider,
                provider_activation => ProviderActivation,
                key => Key,
                status => completed,
                cleanup => pending
            },
            {Id,
                put_component(Pid, C#{effects := [Id | Ids]}, State#{effects := Effects#{Id => E}})}
    end.

write_state(Pid, C = #{activation := Activation}, Key, Msg = #message{payload = Payload}, State) ->
    Result =
        case Payload of
            <<>> -> emqx_retainer:delete(Key);
            _ -> emqx_retainer:store_retained(emqx_message:set_header(retained, true, Msg))
        end,
    case Result of
        ok ->
            {Id, Next} = local_effect(Pid, C, {Pid, Activation}, state_write, Key, State),
            _ = emqx_broker:publish(Msg, #{bypass_hook => true}),
            case emqx_message:get_header(properties, Msg, #{}) of
                #{'Response-Topic' := _} ->
                    reply(Pid, Msg, #{event => state_written, effect_id => Id});
                _ ->
                    ok
            end,
            Next;
        {error, _} ->
            reply(Pid, Msg, #{event => error, reason => state_storage_unavailable}),
            State
    end.

release(Pid, #{activation := Activation}, Id, Msg, State = #{effects := Effects}) ->
    case Effects of
        #{Id := E = #{type := service, owner := Pid, owner_activation := Activation}} ->
            case E of
                #{release_request := _} ->
                    reply(Pid, Msg, #{event => error, reason => release_pending}),
                    State;
                _ ->
                    progress_release(Id, State#{
                        effects := Effects#{Id := E#{release_request => Msg}}
                    })
            end;
        _ ->
            reply(Pid, Msg, #{event => error, reason => effect_not_owned}),
            State
    end.

progress_release(Id, State = #{effects := Effects, components := Components}) ->
    case Effects of
        #{Id := #{release_request := _, status := completed, owner := Owner}} ->
            case Components of
                #{Owner := #{state := stopping}} -> State;
                _ -> finish_release(Id, retract_effect(Id, State))
            end;
        _ ->
            State
    end.

finish_release(Id, State = #{effects := Effects}) ->
    case Effects of
        #{Id := E = #{release_request := Msg, owner := Owner, cleanup := Result}} ->
            case terminal(Result) of
                true ->
                    reply(Owner, Msg, #{event => released, effect_id => Id, status => Result}),
                    State#{effects := Effects#{Id := maps:remove(release_request, E)}};
                false ->
                    State
            end;
        _ ->
            State
    end.

retract_effect(Id, State = #{effects := Effects}) ->
    E = #{cleanup := Cleanup, key := Key} = maps:get(Id, Effects),
    case {Cleanup, E} of
        {pending, #{type := service, provider := Provider}} ->
            Msg = message(<<Key/binary, "/retract/", Id/binary>>, <<>>, #{
                'Response-Topic' => <<"$component/retracted/", Id/binary>>
            }),
            deliver(Provider, <<Key/binary, "/retract/+">>, Msg),
            State#{effects := Effects#{Id := E#{cleanup := requested}}};
        {pending, #{type := state_write}} ->
            ok = emqx_retainer:delete(Key),
            State#{effects := Effects#{Id := E#{cleanup := retracted}}};
        {pending, #{type := state_subscription, owner := Owner}} ->
            Owner ! {unsubscribe, [{Key, #{}}]},
            erlang:send_after(10, self(), {check_unsubscribe, Id, 100}),
            State#{effects := Effects#{Id := E#{cleanup := requested}}};
        _ ->
            State
    end.

admin(
    Pid,
    Action,
    Msg = #message{payload = Payload, flags = #{retain := false}},
    State = #{components := Components}
) when
    Action =:= <<"enable">>; Action =:= <<"disable">>
->
    case emqx_utils_json:safe_decode(Payload) of
        {ok, #{<<"clientid">> := ClientId}} when is_binary(ClientId) ->
            case
                [
                    {Target, C}
                 || {Target, C = #{clientid := Id, connected := true}} <- maps:to_list(Components),
                    Id =:= ClientId
                ]
            of
                [{Target, C}] ->
                    Enabled = Action =:= <<"enable">>,
                    Block =
                        case Enabled of
                            true -> none;
                            false -> disabled
                        end,
                    Next = put_component(Target, C#{activation_block := Block}, State),
                    Updated =
                        case Enabled of
                            true -> activate_waiting(Next);
                            false -> settle_cleanup(stop_component(Target, Next))
                        end,
                    Event =
                        case Enabled of
                            true -> enabled;
                            false -> disabled
                        end,
                    reply(Pid, Msg, #{event => Event, clientid => ClientId}),
                    Updated;
                [] ->
                    reply(Pid, Msg, #{event => error, reason => component_not_found}),
                    State
            end;
        _ ->
            reply(Pid, Msg, #{event => error, reason => invalid_admin_request}),
            State
    end;
admin(Pid, _Action, Msg, State) ->
    reply(Pid, Msg, #{event => error, reason => invalid_admin_request}),
    State.

service_reply(Pid, Id, Msg, State = #{effects := Effects, components := Components}) ->
    case Effects of
        #{Id := E = #{provider := Pid, owner := Owner, response_topic := Topic, status := pending}} ->
            case Components of
                #{Owner := #{state := Status}} when Status =:= starting; Status =:= active ->
                    Response = effect_response(Id, Msg),
                    deliver(Owner, Topic, Response#message{topic = Topic});
                _ ->
                    ok
            end,
            Next = State#{effects := Effects#{Id := E#{status := completed}}},
            settle_cleanup(progress_release(Id, Next));
        _ ->
            State
    end.

retract_reply(Pid, Id, #message{payload = Payload}, State = #{effects := Effects}) ->
    case Effects of
        #{Id := E = #{provider := Pid, cleanup := requested}} ->
            case emqx_utils_json:safe_decode(Payload) of
                {ok, #{<<"status">> := Status}} when
                    Status =:= <<"retracted">>; Status =:= <<"failed">>; Status =:= <<"unknown">>
                ->
                    Result = #{
                        <<"retracted">> => retracted,
                        <<"failed">> => failed,
                        <<"unknown">> => unknown
                    },
                    Next = State#{
                        effects := Effects#{Id := E#{cleanup := maps:get(Status, Result)}}
                    },
                    settle_cleanup(finish_release(Id, Next));
                _ ->
                    State
            end;
        _ ->
            State
    end.

disconnect(Pid, State = #{components := Components}) ->
    case Components of
        #{Pid := #{monitor := Ref, state := inactive}} ->
            demonitor(Ref, [flush]),
            State#{components := maps:remove(Pid, Components)};
        #{Pid := #{monitor := Ref}} ->
            demonitor(Ref, [flush]),
            Next = stop_component(Pid, State),
            #{components := Stopped} = Next,
            Stopping = maps:get(Pid, Stopped),
            settle_cleanup(
                put_component(
                    Pid,
                    disconnected(Stopping),
                    Next
                )
            );
        _ ->
            State
    end.

disconnected(C = #{local_cleanup := complete}) ->
    C#{connected := false};
disconnected(C) ->
    C#{connected := false, local_cleanup := unknown}.

stop_component(Pid, State = #{components := Components}) ->
    C = #{state := Status, clientid := ClientId} = maps:get(Pid, Components),
    case Status =:= starting orelse Status =:= active of
        false ->
            State;
        true ->
            event(Pid, ClientId, #{event => deactivated}),
            Stopped = maps:remove(subscriptions_pending, C),
            Next = put_component(
                Pid,
                Stopped#{
                    state := stopping,
                    local_cleanup => pending
                },
                State
            ),
            lists:foldl(fun stop_component/2, Next, dependents(Pid, Components))
    end.

dependents(Pid, Components) ->
    [
        Dep
     || {Dep, #{bindings := Bindings}} <- maps:to_list(Components),
        lists:any(fun({Provider, _Activation}) -> Provider =:= Pid end, maps:values(Bindings))
    ].

settle_cleanup(State = #{components := Components}) ->
    Next = lists:foldl(fun maybe_cleanup/2, State, maps:keys(Components)),
    case Next =:= State of
        true -> activate_waiting(Next);
        false -> settle_cleanup(Next)
    end.

maybe_cleanup(Pid, State = #{components := Components}) ->
    case Components of
        #{Pid := C = #{state := stopping}} ->
            case dependents(Pid, Components) of
                [] -> cleanup(Pid, C, State);
                _ -> State
            end;
        _ ->
            State
    end.

cleanup(Pid, C = #{effects := Ids}, State = #{effects := Effects, components := Components}) ->
    NextEffects = lists:foldl(
        fun(Id, Acc) ->
            E =
                #{provider := Provider, provider_activation := Activation, cleanup := Cleanup} = maps:get(
                    Id, Acc
                ),
            case E of
                #{type := service} ->
                    case {terminal(Cleanup), Components} of
                        {true, _} ->
                            Acc;
                        {false, #{Provider := #{connected := true, activation := Activation}}} ->
                            Acc;
                        {false, _} ->
                            Acc#{Id := E#{cleanup := unknown}}
                    end;
                _ ->
                    Acc
            end
        end,
        Effects,
        Ids
    ),
    Remaining = [Id || Id <- Ids, not terminal(maps:get(cleanup, maps:get(Id, NextEffects)))],
    Next = lists:foldl(fun finish_release/2, State#{effects := NextEffects}, Ids),
    case
        lists:any(fun(Id) -> maps:get(status, maps:get(Id, NextEffects)) =:= pending end, Remaining)
    of
        true -> Next;
        false -> cleanup_local(Pid, C, Remaining, Next)
    end.

cleanup_local(Pid, C = #{local_cleanup := pending, clientid := ClientId}, _Remaining, State) ->
    event(Pid, ClientId, #{event => cleanup_requested}),
    put_component(Pid, C#{local_cleanup := requested}, State);
cleanup_local(_Pid, #{local_cleanup := requested}, _Remaining, State) ->
    State;
cleanup_local(Pid, C, Remaining, State) ->
    cleanup_effects(Pid, C, Remaining, State).

terminal(retracted) -> true;
terminal(failed) -> true;
terminal(unknown) -> true;
terminal(_) -> false.

cleanup_effects(_Pid, _C, [Id | _], State) ->
    finish_release(Id, retract_effect(Id, State));
cleanup_effects(Pid, C, [], State) ->
    finish_cleanup(Pid, C, State).

finish_cleanup(
    Pid,
    C = #{provides := Provides, effects := Ids, clientid := ClientId},
    State = #{components := Components, effects := Effects, cleanup_results := Results}
) ->
    Report0 = maps:merge(
        maps:with([clientid, activation, local_cleanup], C), #{
            effects => [
                maps:merge(
                    maps:with([type, key, provider_activation, cleanup], maps:get(Id, Effects)), #{
                        id => Id
                    }
                )
             || Id <- Ids
            ]
        }
    ),
    Report =
        case C of
            #{abort_request := #message{payload = Reason}} ->
                Report0#{initialization_failure => Reason};
            _ ->
                Report0
        end,
    Next = State#{effects := maps:without(Ids, Effects), cleanup_results := [Report | Results]},
    Topics = lists:append([provider_filters(K) || K <- Provides]),
    Pid ! {unsubscribe, [{Topic, #{}} || Topic <- Topics]},
    case C of
        #{connected := true} ->
            case C of
                #{abort_request := Msg} -> reply(Pid, Msg, #{event => aborted});
                _ -> event(Pid, ClientId, #{event => stopped})
            end,
            put_component(
                Pid,
                maps:without([activation, abort_request], C#{
                    state := inactive, bindings := #{}, effects := []
                }),
                Next
            );
        #{connected := false} ->
            Next#{components := maps:remove(Pid, Components)}
    end.

put_component(Pid, C, State = #{components := Components}) ->
    State#{components := Components#{Pid := C}}.

provider_filters(<<"$state/", _/binary>> = Key) -> [Key];
provider_filters(Key) -> [Key, <<Key/binary, "/apply/+">>, <<Key/binary, "/retract/+">>].

subscribe(Pid, Topics) ->
    Pid ! {force_subscribe, [{Topic, #{qos => 1}} || Topic <- Topics]},
    ok.

%% TODO: Replace fixed-interval subscription polling with completion notifications.
check_subscriptions(Pid, C = #{activation := Activation}, Topics, Msg, Attempts, State) ->
    Installed = maps:from_list(emqx_broker:subscriptions(Pid)),
    case lists:all(fun(Topic) -> maps:is_key(Topic, Installed) end, Topics) of
        true ->
            Active = maps:remove(subscriptions_pending, C),
            Next = put_component(Pid, Active#{state := active}, State),
            reply(Pid, Msg, #{event => activated}),
            activate_waiting(Next);
        false when Attempts > 0 ->
            erlang:send_after(
                10, self(), {check_subscriptions, Pid, Activation, Topics, Msg, Attempts - 1}
            ),
            put_component(Pid, C#{subscriptions_pending => true}, State);
        false ->
            Pid ! {unsubscribe, [{Topic, #{}} || Topic <- Topics]},
            reply(Pid, Msg, #{event => error, reason => subscription_installation_failed}),
            put_component(Pid, maps:remove(subscriptions_pending, C), State)
    end.

events_topic(ClientId) ->
    <<"$component/", ClientId/binary, "/events">>.

event(Pid, ClientId, Data) ->
    Topic = events_topic(ClientId),
    deliver(Pid, Topic, message(Topic, emqx_utils_json:encode(Data), #{})).

reply(Pid, Msg = #message{from = ClientId}, Data) ->
    Props = emqx_message:get_header(properties, Msg, #{}),
    Topic = maps:get('Response-Topic', Props, events_topic(ClientId)),
    ResponseProps = maps:with(['Correlation-Data'], Props),
    deliver(Pid, Topic, message(Topic, emqx_utils_json:encode(Data), ResponseProps)).

message(Topic, Payload, Props) ->
    Msg = emqx_message:make(?MODULE, 1, Topic, Payload),
    emqx_message:set_header(properties, Props, Msg).

deliver(Pid, Filter, Msg) ->
    Pid ! {deliver, Filter, Msg},
    ok.
