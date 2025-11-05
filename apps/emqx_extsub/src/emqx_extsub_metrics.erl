%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_metrics).

-export([
    child_spec/0,
    inc/2,
    inc/3,
    observe_hist/3
]).

-export([
    get_rates/1,
    get_counters/1
]).

-export([
    print_hist/2
]).

-define(EXT_SUB_METRICS_WORKER, ext_sub_metrics).

% -define(LATENCY_BUCKETS, [
%     2,
%     5,
%     10,
%     20,
%     50,
%     100,
%     250,
%     500,
%     750,
%     1000,
%     2000,
%     5000
% ]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

child_spec() ->
    emqx_metrics_worker:child_spec(?EXT_SUB_METRICS_WORKER, ?EXT_SUB_METRICS_WORKER, []).

inc(Id, Metric) ->
    inc(Id, Metric, 1).

inc(Id, Metric, Val) ->
    emqx_metrics_worker:inc(?EXT_SUB_METRICS_WORKER, Id, Metric, Val).

get_rates(Id) ->
    #{rate := Rates} = emqx_metrics_worker:get_metrics(?EXT_SUB_METRICS_WORKER, Id),
    Rates.

get_counters(Id) ->
    #{counters := Counters} = emqx_metrics_worker:get_metrics(?EXT_SUB_METRICS_WORKER, Id),
    Counters.

observe_hist(Id, Metric, Val) ->
    emqx_metrics_worker:observe_hist(?EXT_SUB_METRICS_WORKER, Id, Metric, Val).

print_hist(Id, Metric) ->
    emqx_utils_metrics:print_hists(?EXT_SUB_METRICS_WORKER, Id, Metric).
