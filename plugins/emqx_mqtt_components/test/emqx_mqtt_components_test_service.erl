%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_components_test_service).
-behaviour(gen_server).

-export([start_link/2, mqtt/1, effects/1, complete/3, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Id, Declarations) ->
    gen_server:start_link(?MODULE, {self(), Id, Declarations}, []).

mqtt(Pid) -> gen_server:call(Pid, mqtt).
effects(Pid) -> gen_server:call(Pid, effects).
complete(Pid, Msg, Outcome) -> gen_server:call(Pid, {complete, Msg, Outcome}).
stop(Pid) -> gen_server:stop(Pid).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init({Owner, Id, Declarations}) ->
    {ok, MQTT} = emqtt:start_link([{clientid, Id}, {proto_ver, v5}, {clean_start, true}]),
    {ok, _} = emqtt:connect(MQTT),
    Topics = [<<"test/", Id/binary, "/reply">> | Declarations],
    {ok, _, _} = emqtt:subscribe(MQTT, [{Topic, 1} || Topic <- Topics]),
    {ok, #{owner => Owner, mqtt => MQTT, effects => #{}, pending => #{}}}.

handle_call(mqtt, _From, State = #{mqtt := MQTT}) ->
    {reply, MQTT, State};
handle_call(effects, _From, State = #{effects := Effects}) ->
    {reply, Effects, State};
handle_call(
    {complete, #{topic := Topic}, Outcome},
    _From,
    State = #{mqtt := MQTT, effects := Effects, pending := Pending}
) ->
    {#{payload := Payload, properties := Props}, Rest} = maps:take(Topic, Pending),
    [Id, Operation | _] = lists:reverse(binary:split(Topic, <<"/">>, [global])),
    NextEffects =
        case {Operation, Outcome} of
            {<<"apply">>, applied} -> Effects#{Id => Payload};
            {<<"retract">>, retracted} -> maps:remove(Id, Effects);
            {<<"retract">>, failed} -> Effects;
            {<<"retract">>, unknown} -> Effects
        end,
    Response =
        case Outcome of
            applied -> <<"accepted">>;
            _ -> emqx_utils_json:encode(#{status => Outcome})
        end,
    #{'Response-Topic' := Reply} = Props,
    {ok, _} = emqtt:publish(
        MQTT, Reply, maps:with(['Correlation-Data'], Props), Response, [{qos, 1}]
    ),
    {reply, ok, State#{effects := NextEffects, pending := Rest}}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(
    {publish, Msg = #{topic := Topic}},
    State = #{owner := Owner, mqtt := MQTT, pending := Pending}
) ->
    Owner ! {mqtt, MQTT, Msg},
    {noreply, State#{pending := Pending#{Topic => Msg}}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{mqtt := MQTT}) ->
    case is_process_alive(MQTT) of
        true -> emqtt:stop(MQTT);
        false -> ok
    end.
