%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_subscribe_odd_behaviour_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_storage,
                {emqx, emqx_streams_test_utils:cth_config(emqx)},
                {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
                {emqx_streams, emqx_streams_test_utils:cth_config(emqx_streams)}
            ],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_CaseName, Config) ->
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = emqx_streams_test_utils:reset_config().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% The scenario is:
%% * Insert a message into the DS.
%% * Find its exact timestamp=TS
%% * Insert a second message with significantly later timestamp
%% * Subscribe to its stream passing start_time=TS-1, TS, TS+1 to make_iterator
%% * Expect to receive the second message in all cases

%% This one succeeds
t_subscribe_start_time_minus_one(_Config) ->
    test_subscribe(-1).

%% This one fails, catchup beamformer falls into a dead loop(?) and
%% subscription is stuck
t_subscribe_start_time_exact(_Config) ->
    test_subscribe(0).

%% This one also succeeds
t_subscribe_start_time_plus_one(_Config) ->
    test_subscribe(1).

test_subscribe(StartTimeDelta) ->
    Topic = [<<"topic">>, <<"1">>, <<"1">>, <<"key">>, <<"1">>],
    SubTopic = [<<"topic">>, <<"1">>, <<"1">>, <<"key">>, '#'],

    %% Insert two message into the database, save the timestamp of the first one into StartTime
    DirtyOpts = #{
        db => streams_message_regular,
        shard => <<"0">>,
        reply => true
    },
    _ = emqx_ds:dirty_append(DirtyOpts, [{Topic, ?ds_tx_ts_monotonic, <<"hello1">>}]),
    receive
        {'DOWN', _, _, _, _} ->
            ok
    after 2000 ->
        ct:fail("dirty_append failed")
    end,
    [{_, StartTime, _}] = emqx_ds:dirty_read(streams_message_regular, SubTopic),
    ct:sleep(2000),
    _ = emqx_ds:dirty_append(DirtyOpts, [{Topic, ?ds_tx_ts_monotonic, <<"hello2">>}]),
    receive
        {'DOWN', _, _, _, _} ->
            ok
    after 2000 ->
        ct:fail("dirty_append failed")
    end,

    %% Find stream
    [{_Slab, Stream}] = emqx_ds:get_streams(streams_message_regular, SubTopic, 0),

    %% Make iterator adjusting start_time with delta
    {ok, Iterator} = emqx_ds:make_iterator(
        streams_message_regular, Stream, SubTopic, StartTime + StartTimeDelta
    ),

    %% Subscribe and receive messages
    {ok, SubHandle, _} = emqx_ds:subscribe(streams_message_regular, Iterator, #{max_unacked => 1}),
    Messages0 = recv_message(SubHandle),

    %% Check if we received the second message
    Messages = [Msg || {_, _, <<"hello2">>} = Msg <- Messages0],
    ?assertEqual(1, length(Messages)).

recv_message(SubHandle) ->
    receive
        #ds_sub_reply{seqno = SeqNo, payload = {ok, _It, Batch}, size = _Size} ->
            emqx_ds:suback(streams_message_regular, SubHandle, SeqNo),
            Batch ++ recv_message(SubHandle)
    after 2000 ->
        []
    end.
