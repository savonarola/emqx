%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_tokio_nif).

-export([
    runtime_new/0,
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
