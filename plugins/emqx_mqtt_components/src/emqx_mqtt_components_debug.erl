%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_components_debug).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-export([component/3, effects/2, message/3, subscription/3]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

component(_Pid, Same, Same) ->
    ok;
component(Pid, Previous, Current) ->
    emit(#{
        event => component_changed,
        component_id => Pid,
        previous => component(Previous),
        current => component(Current)
    }).

effects(Previous, Current) ->
    lists:foreach(
        fun(Id) ->
            case {maps:find(Id, Previous), maps:find(Id, Current)} of
                {Same, Same} ->
                    ok;
                {Before, After} ->
                    emit(#{
                        event => effect_changed,
                        effect_id => Id,
                        previous => effect(Before),
                        current => effect(After)
                    })
            end
        end,
        lists:usort(maps:keys(Previous) ++ maps:keys(Current))
    ).

message(_Direction, _Pid, #message{topic = <<"$component/debug">>}) ->
    ok;
message(Direction, Pid, Msg = #message{topic = Topic, payload = Payload, qos = QoS}) ->
    emit(#{
        event => message,
        direction => Direction,
        component_id => Pid,
        topic => Topic,
        payload => Payload,
        qos => QoS,
        retain => emqx_message:get_flag(retain, Msg),
        properties => properties(emqx_message:get_header(properties, Msg, #{}))
    }).

subscription(Action, Pid, Topics) ->
    Managed = [
        Topic
     || Topic <- Topics,
        Topic =/= <<"$component/debug">>,
        managed(Topic)
    ],
    case Managed of
        [] ->
            ok;
        _ ->
            emit(#{event => subscription, action => Action, component_id => Pid, topics => Managed})
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

component(error) ->
    null;
component({ok, C}) ->
    Bindings = [
        #{key => Key, provider => Pid, activation => Activation}
     || {Key, {Pid, Activation}} <- maps:to_list(maps:get(bindings, C))
    ],
    maps:merge(maps:without([monitor, abort_request, bindings], C), #{bindings => Bindings}).

effect(error) ->
    null;
effect({ok, E}) ->
    maps:merge(maps:remove(release_request, E), #{
        release_pending => maps:is_key(release_request, E)
    }).

properties(Props) ->
    maps:map(
        fun
            ('Correlation-Data', Data) -> base64_value(Data);
            ('User-Property', Pairs) -> [#{key => K, value => V} || {K, V} <- Pairs];
            (_, Value) -> Value
        end,
        Props
    ).

managed(#share{topic = Topic}) -> managed(Topic);
managed(<<"$state/", _/binary>>) -> true;
managed(<<"$service/", _/binary>>) -> true;
managed(<<"$component/", _/binary>>) -> true;
managed(<<"$provide/", _/binary>>) -> true;
managed(<<"$consume/", _/binary>>) -> true;
managed(_) -> false.

emit(Data) ->
    Event = Data#{
        sequence => erlang:unique_integer([positive, monotonic]),
        timestamp => erlang:system_time(millisecond)
    },
    Msg = emqx_message:make(
        ?MODULE, 0, <<"$component/debug">>, emqx_utils_json:encode(json(Event))
    ),
    _ = emqx_broker:publish(Msg, #{bypass_hook => true}),
    ok.

json(Value) when is_pid(Value) -> list_to_binary(pid_to_list(Value));
json(Value) when is_binary(Value) ->
    case unicode:characters_to_binary(Value) of
        Value -> Value;
        _ -> base64_value(Value)
    end;
json(Value) when is_map(Value) -> maps:map(fun(_, V) -> json(V) end, Value);
json(Value) when is_list(Value) -> [json(V) || V <- Value];
json(Value = #share{}) ->
    emqx_topic:maybe_format_share(Value);
json(Value) ->
    Value.

base64_value(Value) -> #{encoding => base64, data => base64:encode(Value)}.
