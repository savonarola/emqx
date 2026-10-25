%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_tokio_nif).

-export([
    runtime_new/0,
    native_new/1,
    runtime_stop/2,
    register/3,
    take/3,
    deliver/3,
    forwarding/2,
    listener_apply/4,
    listener_status/2,
    listeners_stop/2,
    redis_connect/2,
    redis_get/4,
    redis_set/5
]).

-export([init/0]).
-on_load(init/0).

init() ->
    NifName = "libemqx_tokio_nif",
    Niflib = filename:join(priv_dir(), NifName),
    erlang:load_nif(Niflib, none).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec runtime_new() -> {ok, reference()} | {error, string()}.
runtime_new() ->
    not_loaded(?LINE).

-spec native_new(map()) -> {ok, reference()} | {error, string()}.
native_new(_Limits) -> not_loaded(?LINE).

-spec runtime_stop(reference(), pid()) -> ok.
runtime_stop(_Runtime, _ReplyTo) -> not_loaded(?LINE).

-spec register(reference(), non_neg_integer(), pid()) -> ok.
register(_Runtime, _Slot, _Pid) -> not_loaded(?LINE).

-spec take(reference(), non_neg_integer(), pos_integer()) -> {list(), more | empty}.
take(_Runtime, _Slot, _Max) -> not_loaded(?LINE).

-spec deliver(reference(), non_neg_integer(), list()) -> ok.
deliver(_Runtime, _Slot, _Batch) -> not_loaded(?LINE).

-spec forwarding(reference(), boolean()) -> ok.
forwarding(_Runtime, _Enabled) -> not_loaded(?LINE).

-spec listener_apply(reference(), map(), boolean(), pid()) -> reference().
listener_apply(_Runtime, _Config, _Restart, _ReplyTo) -> not_loaded(?LINE).

-spec listener_status(reference(), pid()) -> reference().
listener_status(_Runtime, _ReplyTo) -> not_loaded(?LINE).

-spec listeners_stop(reference(), pid()) -> reference().
listeners_stop(_Runtime, _ReplyTo) -> not_loaded(?LINE).

-spec redis_connect(reference(), binary()) ->
    {ok, reference()} | {error, string()}.
redis_connect(_Runtime, _Url) ->
    not_loaded(?LINE).

-spec redis_get(reference(), reference(), binary(), pid()) -> ok.
redis_get(_Runtime, _Conn, _Key, _ReceiverPid) ->
    not_loaded(?LINE).

-spec redis_set(reference(), reference(), binary(), binary(), pid()) -> ok.
redis_set(_Runtime, _Conn, _Key, _Value, _ReceiverPid) ->
    not_loaded(?LINE).

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end.
