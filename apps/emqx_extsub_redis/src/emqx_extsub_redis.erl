%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_redis).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-export([
    register_hooks/0,
    unregister_hooks/0
]).
-export([on_message_publish/1]).

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER + 1).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

%%--------------------------------------------------------------------
%% Hooks callbacks
%%--------------------------------------------------------------------

on_message_publish(#message{topic = <<"$redis/", _/binary>> = Topic} = Message) ->
    ok = emqx_extsub_redis_persist:insert(#{topic => Topic}, Message),
    {ok, Message};
on_message_publish(Message) ->
    {ok, Message}.