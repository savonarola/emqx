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
        slot => Slot, publishes => length([ok || {publish, _, _, _, _} <- Batch])
    }),
    case Status of
        more -> self() ! native_ready;
        empty -> ok
    end,
    {noreply, State};
handle_info(#deliver{} = Delivery, #{runtime := Runtime, slot := Slot, batch_size := Max} = State) ->
    Batch = collect_deliveries(Max - 1, [delivery(Delivery)]),
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
        ok = emqx_broker:subscribe(Filter, undefined, #{native => true, qos => 0})
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
process_event({publish, ClientId, Topic, Payload, QoS}) ->
    Msg0 = emqx_message:make(ClientId, QoS, Topic, Payload),
    Msg = emqx_message:set_header(native_origin, node(), Msg0),
    _ = emqx_broker:publish(Msg),
    ok.

collect_deliveries(0, Acc) ->
    lists:reverse(Acc);
collect_deliveries(Left, Acc) ->
    receive
        #deliver{} = Delivery -> collect_deliveries(Left - 1, [delivery(Delivery) | Acc])
    after 0 -> lists:reverse(Acc)
    end.

delivery(#deliver{
    topic = Filter, message = #message{topic = Topic, payload = Payload, flags = Flags}
}) ->
    {Filter, Topic, iolist_to_binary(Payload), maps:get(retain, Flags, false)}.
