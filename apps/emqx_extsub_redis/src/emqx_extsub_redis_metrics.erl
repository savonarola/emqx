%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis_metrics).

-export([
    child_spec/0,
    inc/1,
    inc/2,
    observe_hist/2
]).

-export([
    get_rates/0,
    get_counters/0
]).

-export([
    print_hists/0
]).

-define(METRICS_WORKER, ext_sub_redis_metrics).
-define(METRICS_ID, redis).

-define(LATENCY_BUCKETS, [
    2,
    5,
    10,
    20,
    50,
    100,
    250,
    500,
    750,
    1000,
    2000,
    5000
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

child_spec() ->
    emqx_metrics_worker:child_spec(?METRICS_WORKER, ?METRICS_WORKER, [
        {?METRICS_ID, [
            {hist, fetch_latency_ms, ?LATENCY_BUCKETS},
            {hist, flush_latency_ms, ?LATENCY_BUCKETS},
            {counter, fetch},
            {counter, via_pubsub_behind},
            {counter, via_pubsub_ahead},
            {counter, via_pubsub_ok}
        ]}
    ]).

inc(Metric) ->
    inc(Metric, 1).

inc(Metric, Val) ->
    emqx_metrics_worker:inc(?METRICS_WORKER, ?METRICS_ID, Metric, Val).

get_rates() ->
    #{rate := Rates} = emqx_metrics_worker:get_metrics(?METRICS_WORKER, ?METRICS_ID),
    Rates.

get_counters() ->
    #{counters := Counters} = emqx_metrics_worker:get_metrics(?METRICS_WORKER, ?METRICS_ID),
    Counters.

observe_hist(Metric, Val) ->
    emqx_metrics_worker:observe_hist(?METRICS_WORKER, ?METRICS_ID, Metric, Val).

print_hists() ->
    emqx_utils_metrics:print_hists(?METRICS_WORKER, ?METRICS_ID).
