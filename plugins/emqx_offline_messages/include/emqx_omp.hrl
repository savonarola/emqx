%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_OMP_HRL).
-define(EMQX_OMP_HRL, true).

-record(config_change, {
    id :: emqx_omp:backend_id(),
    module :: module(),
    old :: map(),
    new :: map()
}).

%% `{{BackendId, Namespace}, TopicId, Offset}` of a message returned to ExtSub.
-define(OMP_POSITION, omp_position).

%% Committed messages with consecutive offsets, sent with `emqx_broker:dispatch_info/2`.
-record(omp_live, {
    namespace :: {emqx_omp:backend_id(), emqx_omp_backend:namespace()},
    topic_id :: emqx_omp_backend:topic_id(),
    offset :: emqx_omp_backend:offset(),
    messages :: [emqx_types:message()]
}).

-endif.
