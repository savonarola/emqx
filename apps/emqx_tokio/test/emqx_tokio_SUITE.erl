%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_tokio_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_tokio],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

t_nif_runtime(_Config) ->
    {ok, Runtime} = emqx_tokio_nif:runtime_new(),
    ?assert(is_reference(Runtime)).

t_nif_redis_set_get(_Config) ->
    {ok, Runtime} = emqx_tokio_nif:runtime_new(),
    {ok, Conn} = emqx_tokio_nif:redis_connect(Runtime, <<"redis://127.0.0.1:6379">>),
    RandomValue = emqx_guid:gen(),
    {ok, Ref0} = emqx_tokio_nif:redis_set(Runtime, Conn, <<"emqx_test_key">>, RandomValue, self()),
    receive
        {ok, Ref0} -> ok;
        {error, Ref0, Reason0} -> ct:fail({redis_set_failed, Reason0})
    after 5000 ->
        ct:fail(no_reply_from_redis_set)
    end,

    {ok, Ref1} = emqx_tokio_nif:redis_get(Runtime, Conn, <<"emqx_test_key">>, self()),

    receive
        {ok, Ref1, RandomValue} -> ?assertEqual(RandomValue, RandomValue);
        {ok, Ref1, OtherValue} -> ct:fail({redis_get_invalid_value, OtherValue});
        {error, Ref1, Reason1} -> ct:fail({redis_get_failed, Reason1})
    after 5000 ->
        ct:fail(no_reply_from_redis_get)
    end.
