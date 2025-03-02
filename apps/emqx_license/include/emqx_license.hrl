%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQX License Management CLI.
%%--------------------------------------------------------------------

-ifndef(_EMQX_LICENSE_).
-define(_EMQX_LICENSE_, true).

-define(SINGLE_NODE_LICENSE_LOG,
    "\n"
    "==============================================================================\n"
    "Using a single-node development license.\n"
    "This license is not permitted for production use.\n"
    "Visit https://www.emqx.com/apply-licenses/emqx?version=5 to apply license for:\n"
    " - Production use\n"
    " - Education and Non-profit use (clustered deployment, free of charge)\n"
    "==============================================================================\n"
).

%% The new license key since 5.9 is not a trial license, but a single-node license.
%% In case someone uses the old license key, we will print this log.
-define(EVALUATION_LOG,
    "\n"
    "==================================================================================\n"
    "Using an evaluation license limited to ~p concurrent sessions.\n"
    "This license is for evaluation purposes only and not permitted for production use.\n"
    "Visit https://emqx.com/apply-licenses/emqx?version=5 to apply a license.\n"
    "==================================================================================\n"
).

-define(EXPIRY_LOG,
    "\n"
    "============================================================================\n"
    "License has been expired for ~p days.\n"
    "Visit https://emqx.com/apply-licenses/emqx?version=5 to apply a new license.\n"
    "Or contact EMQ customer services via email contact@emqx.io\n"
    "============================================================================\n"
).

-define(TRIAL, 0).
-define(OFFICIAL, 1).
-define(SINGLE_NODE, 2).

-define(SMALL_CUSTOMER, 0).
-define(MEDIUM_CUSTOMER, 1).
-define(LARGE_CUSTOMER, 2).
-define(BUSINESS_CRITICAL_CUSTOMER, 3).
-define(BYOC_CUSTOMER, 4).
-define(EDUCATION_NONPROFIT_CUSTOMER, 5).
-define(EVALUATION_CUSTOMER, 10).
-define(DEVELOPMENT_CUSTOMER, 11).

-define(EXPIRED_DAY, -90).

-define(ERR_EXPIRED, expired).
-define(ERR_MAX_UPTIME, max_uptime_reached).

%% The default max_sessions limit for 'single-node' license type.
-define(DEFAULT_MAX_SESSIONS_LTYPE2, 10_000_000).

%% The default max_sessions limit for business-critical customer
%% before dynamic_max_connections set.
-define(DEFAULT_MAX_SESSIONS_CTYPE3, 25).

-endif.
