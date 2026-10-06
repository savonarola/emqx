%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_components_demo).
-behaviour(gen_server).

-export([start_link/2, disconnect/1, reconnect/1, port/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([init/2]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Id, Role) ->
    gen_server:start_link(?MODULE, {self(), Id, Role}, []).

disconnect(Pid) -> gen_server:call(Pid, disconnect).
reconnect(Pid) -> gen_server:call(Pid, reconnect).
port(Pid) -> gen_server:call(Pid, port).
stop(Pid) -> gen_server:stop(Pid).

%%------------------------------------------------------------------------------
%% HTTP handler
%%------------------------------------------------------------------------------

init(Req0, Router) ->
    {Body, Req1} = read_body(Req0, <<>>),
    Request = #{verb => cowboy_req:method(Req1), path => cowboy_req:path(Req1), body => Body},
    {Status, Response} = gen_server:call(Router, {http, Request}),
    Req = cowboy_req:reply(Status, #{<<"content-type">> => <<"application/json">>}, Response, Req1),
    {ok, Req, Router}.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init({Owner, Id, Role}) ->
    State0 = #{
        owner => Owner,
        id => Id,
        role => Role,
        active => false,
        routes => #{},
        pending => #{},
        reply_topic => <<"demo/", Id/binary, "/reply">>
    },
    State = connect(State0),
    case Role of
        router ->
            Listener = {?MODULE, self()},
            Dispatch = cowboy_router:compile([{'_', [{"/[...]", ?MODULE, self()}]}]),
            {ok, _} = cowboy:start_clear(
                Listener,
                #{socket_opts => [{port, 0}]},
                #{env => #{dispatch => Dispatch}}
            ),
            {ok, State#{listener => Listener}};
        {handler, _Prefix} ->
            {ok, State}
    end.

handle_call(port, _From, State = #{listener := Listener}) ->
    {reply, ranch:get_port(Listener), State};
handle_call(disconnect, _From, State = #{mqtt := MQTT}) ->
    ok = emqtt:disconnect(MQTT),
    {reply, ok, State#{mqtt := undefined, active := false, routes := #{}}};
handle_call(reconnect, _From, State = #{mqtt := undefined}) ->
    {reply, ok, connect(State)};
handle_call({http, _Request}, _From, State = #{active := false}) ->
    {reply, {503, <<>>}, State};
handle_call(
    {http, Request = #{path := Path}},
    From,
    State = #{routes := Routes, mqtt := MQTT, reply_topic := Reply, pending := Pending}
) ->
    Matches = lists:reverse(
        lists:sort([
            {byte_size(Prefix), Topic}
         || #{<<"path_prefix">> := Prefix, <<"handle_topic">> := Topic} <- maps:values(Routes),
            string:prefix(Path, Prefix) =/= nomatch
        ])
    ),
    case Matches of
        [] ->
            {reply, {404, <<>>}, State};
        [{_, Topic} | _] ->
            Id = integer_to_binary(erlang:unique_integer([positive])),
            {ok, _} = emqtt:publish(
                MQTT,
                Topic,
                #{'Response-Topic' => Reply, 'Correlation-Data' => Id},
                emqx_utils_json:encode(Request),
                [{qos, 1}]
            ),
            Timer = erlang:send_after(2000, self(), {request_timeout, Id}),
            {noreply, State#{pending := Pending#{Id => {From, Timer}}}}
    end.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({publish, Msg = #{topic := Topic, payload := Payload}}, State = #{id := Id}) ->
    Events = <<"$component/", Id/binary, "/events">>,
    Next =
        case Topic of
            Events ->
                Data =
                    case response_status(Msg) of
                        undefined -> emqx_utils_json:decode(Payload);
                        Status -> #{<<"event">> => Status}
                    end,
                lifecycle(Data, State);
            _ ->
                mqtt_message(Msg, State)
        end,
    {noreply, Next};
handle_info({request_timeout, Id}, State = #{pending := Pending}) ->
    case maps:take(Id, Pending) of
        {{From, _Timer}, Rest} ->
            gen_server:reply(From, {504, <<>>}),
            {noreply, State#{pending := Rest}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State of
        #{listener := Listener} -> ok = cowboy:stop_listener(Listener);
        _ -> ok
    end,
    case State of
        #{mqtt := MQTT} when is_pid(MQTT) -> emqtt:stop(MQTT);
        _ -> ok
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

connect(State = #{id := Id, role := Role, reply_topic := Reply}) ->
    {ok, MQTT} = emqtt:start_link([{clientid, Id}, {proto_ver, v5}, {clean_start, true}]),
    {ok, _} = emqtt:connect(MQTT),
    Declarations =
        case Role of
            router -> [<<"$provide/service/reg-route">>];
            {handler, _} -> [<<"$consume/service/reg-route">>, handle_topic(Id)]
        end,
    {ok, _, _} = emqtt:subscribe(MQTT, [{T, 1} || T <- [Reply | Declarations]]),
    State#{mqtt => MQTT}.

lifecycle(#{<<"event">> := <<"initialize">>}, State = #{role := router}) ->
    ready(State),
    State;
lifecycle(
    #{<<"event">> := <<"initialize">>},
    State = #{role := {handler, Prefix}, id := Id, mqtt := MQTT, reply_topic := Reply}
) ->
    notify(initializing, State),
    Payload = emqx_utils_json:encode(#{handle_topic => handle_topic(Id), path_prefix => Prefix}),
    {ok, _} = emqtt:publish(
        MQTT,
        <<"$service/reg-route">>,
        #{'Response-Topic' => Reply, 'Correlation-Data' => <<"register">>},
        Payload,
        [{qos, 1}]
    ),
    State;
lifecycle(#{<<"event">> := <<"activated">>}, State) ->
    notify(active, State),
    State#{active := true};
lifecycle(#{<<"event">> := <<"deactivated">>}, State) ->
    notify(inactive, State),
    State#{active := false};
lifecycle(#{<<"event">> := <<"cleanup_requested">>}, State = #{mqtt := MQTT, routes := Routes}) ->
    0 = map_size(Routes),
    {ok, _} = emqtt:publish(MQTT, <<"$component/cleanup_complete">>, <<>>, [{qos, 1}]),
    State;
lifecycle(#{<<"event">> := <<"stopped">>}, State) ->
    notify(stopped, State),
    State;
lifecycle(Event, State) ->
    notify(Event, State),
    State.

mqtt_message(
    Msg = #{topic := <<"$service/reg-route/apply/", Effect/binary>>, payload := Payload},
    State = #{role := router, routes := Routes}
) ->
    Route = emqx_utils_json:decode(Payload),
    respond(Msg, <<"ok">>, <<>>, State),
    notify({registered, Effect}, State),
    State#{routes := Routes#{Effect => Route}};
mqtt_message(
    Msg = #{topic := <<"$service/reg-route/retract/", Effect/binary>>},
    State = #{role := router, routes := Routes}
) ->
    Next = State#{routes := maps:remove(Effect, Routes)},
    respond(Msg, <<"retracted">>, <<>>, Next),
    notify({retracted, Effect}, Next),
    Next;
mqtt_message(
    Msg = #{topic := Topic, properties := #{'Correlation-Data' := Id}, payload := Body},
    State = #{role := router, reply_topic := Topic, pending := Pending}
) ->
    case maps:take(Id, Pending) of
        {{From, Timer}, Rest} ->
            erlang:cancel_timer(Timer),
            Status = binary_to_integer(response_status(Msg)),
            gen_server:reply(From, {Status, Body}),
            State#{pending := Rest};
        error ->
            State
    end;
mqtt_message(
    Msg = #{topic := Topic, properties := #{'Correlation-Data' := <<"register">>}},
    State = #{role := {handler, _}, reply_topic := Topic}
) ->
    <<"ok">> = response_status(Msg),
    ready(State),
    State;
mqtt_message(
    Msg = #{payload := Payload}, State = #{role := {handler, _}, active := true, id := Id}
) ->
    Request = emqx_utils_json:decode(Payload),
    Body = emqx_utils_json:encode(Request#{<<"handler">> => Id}),
    respond(Msg, <<"200">>, Body, State),
    State;
mqtt_message(_Msg, State) ->
    State.

ready(#{mqtt := MQTT}) ->
    {ok, _} = emqtt:publish(MQTT, <<"$component/ready">>, <<>>, [{qos, 1}]),
    ok.

respond(#{properties := Props = #{'Response-Topic' := Topic}}, Status, Body, #{mqtt := MQTT}) ->
    ResponseProps = maps:with(['Correlation-Data'], Props),
    {ok, _} = emqtt:publish(
        MQTT,
        Topic,
        ResponseProps#{'User-Property' => [{<<"component-status">>, Status}]},
        Body,
        [{qos, 1}]
    ),
    ok.

response_status(#{properties := Props}) ->
    proplists:get_value(<<"component-status">>, maps:get('User-Property', Props, [])).

notify(Event, #{owner := Owner}) ->
    Owner ! {component, self(), Event},
    ok.

handle_topic(Id) -> <<"demo/", Id/binary, "/handle">>.

read_body(Req0, Acc) ->
    case cowboy_req:read_body(Req0) of
        {ok, Data, Req} -> {<<Acc/binary, Data/binary>>, Req};
        {more, Data, Req} -> read_body(Req, <<Acc/binary, Data/binary>>)
    end.
