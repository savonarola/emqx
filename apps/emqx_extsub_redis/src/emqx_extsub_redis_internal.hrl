%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MQ_INTERNAL_HRL).
-define(EMQX_MQ_INTERNAL_HRL, true).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(tp_debug(KIND, EVENT), ?tp(debug, KIND, EVENT)).

-endif.