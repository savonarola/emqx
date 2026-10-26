%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_tokio_representative).
-behaviour(gen_server).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-spec start_link(non_neg_integer(), pos_integer()) -> gen_server:start_ret().
start_link(Slot, BatchSize) -> gen_server:start_link(?MODULE, [Slot, BatchSize], []).

init([Slot, BatchSize]) ->
    Runtime = emqx_tokio_runtime:resource(),
    ok = emqx_tokio_nif:register(Runtime, Slot, self()),
    {ok, #{runtime => Runtime, slot => Slot, batch_size => BatchSize}}.

handle_call(_Request, _From, State) -> {reply, {error, not_supported}, State}.
handle_cast(_Request, State) -> {noreply, State}.

handle_info(native_ready, #{runtime := Runtime, slot := Slot, batch_size := Max} = State) ->
    {Batch, Status} = emqx_tokio_nif:take(Runtime, Slot, Max),
    lists:foreach(fun process_event/1, Batch),
    ?tp(native_publish_batch, #{
        slot => Slot, publishes => length([ok || {publish, _, _, _, _, _} <- Batch])
    }),
    case Status of
        more -> self() ! native_ready;
        empty -> ok
    end,
    {noreply, State};
handle_info(#deliver{} = Delivery, #{runtime := Runtime, slot := Slot, batch_size := Max} = State) ->
    Batch = lists:filtermap(fun delivery/1, collect_deliveries(Max - 1, [Delivery])),
    ok = emqx_tokio_nif:deliver(Runtime, Slot, Batch),
    ?tp(native_delivery_batch, #{slot => Slot, count => length(Batch)}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

process_event({subscribe, Filter}) ->
    try
        ok = emqx_broker:subscribe(Filter, undefined, #{native => true, qos => 1})
    catch
        Class:Reason ->
            ?SLOG(error, #{
                msg => "native_subscribe_failed",
                filter => Filter,
                exception => Class,
                reason => Reason
            })
    end;
process_event({unsubscribe, Filter}) ->
    ok = emqx_broker:unsubscribe(Filter);
process_event({publish, ClientId, Topic, Payload, QoS, Properties}) ->
    Msg0 = emqx_message:make(ClientId, QoS, Topic, Payload),
    Msg1 = emqx_message:set_header(native_origin, node(), Msg0),
    Msg =
        case Properties of
            undefined -> Msg1;
            _ -> emqx_message:set_header(properties, Properties, Msg1)
        end,
    _ = emqx_broker:publish(Msg),
    ok.

collect_deliveries(0, Acc) ->
    lists:reverse(Acc);
collect_deliveries(Left, Acc) ->
    receive
        #deliver{} = Delivery -> collect_deliveries(Left - 1, [Delivery | Acc])
    after 0 -> lists:reverse(Acc)
    end.

%% Drop expired messages, as regular sessions do, and lower the expiry of the others.
delivery(#deliver{topic = Filter, message = Msg0}) ->
    case emqx_message:is_expired(Msg0, default) of
        true ->
            false;
        false ->
            Msg = emqx_message:update_expiry(Msg0),
            #message{topic = Topic, payload = Payload, qos = QoS} = Msg,
            Properties = emqx_message:get_header(properties, Msg, #{}),
            {true, {Filter, Topic, iolist_to_binary(Payload), QoS, Properties}}
    end.
