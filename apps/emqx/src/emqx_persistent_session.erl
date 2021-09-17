%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_persistent_session).

-export([ discard/1
        , discard/2
        , lookup/1
        , persist/3
        , persist_message/1
        , pending/1
        , pending/2
        , resume/3
        ]).

-export([ add_subscription/3
        , remove_subscription/3
        ]).

-export([ mark_as_delivered/2
        , mark_resume_begin/1
        ]).

-export([ pending_messages_in_db/2
        ]).

-export([ mnesia/1
        ]).

-include("emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").


-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(MAX_EXPIRY_INTERVAL, 4294967295000). %% 16#FFFFFFFF * 1000

%% NOTE: Order is significant because of traversal order of the table.
-define(MARKER, 3).
-define(ABANDONED, 2).
-define(DELIVERED, 1).
-define(UNDELIVERED, 0).
-type pending_tag() :: ?DELIVERED | ?UNDELIVERED | ?ABANDONED | ?MARKER.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

-define(SESSION_STORE, emqx_session_store).
-define(SESS_MSG_TAB, emqx_session_msg).
-define(MSG_TAB, emqx_persistent_msg).

-record(session_store, { client_id        :: binary()
                       , expiry_interval  :: non_neg_integer()
                       , ts               :: non_neg_integer()
                       , session          :: emqx_session:session()}).

-record(session_msg, {key      :: {binary(), binary(), emqx_guid:guid(), pending_tag()},
                      val = [] :: []}).

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?SESSION_STORE, [
                {type, set},
                {rlog_shard, ?PERSISTENT_SESSION_SHARD},
                {ram_copies, [node()]},
                {record_name, session_store},
                {attributes, record_info(fields, session_store)},
                {storage_properties, [{ets, [{read_concurrency, true}]}]}]),

    ok = ekka_mnesia:create_table(?SESS_MSG_TAB, [
                {type, ordered_set},
                {rlog_shard, ?PERSISTENT_SESSION_SHARD},
                {ram_copies, [node()]},
                {record_name, session_msg},
                {attributes, record_info(fields, session_msg)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]),

    ok = ekka_mnesia:create_table(?MSG_TAB, [
                {type, set},
                {rlog_shard, ?PERSISTENT_SESSION_SHARD},
                {ram_copies, [node()]},
                {record_name, message},
                {attributes, record_info(fields, message)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?SESSION_STORE, ram_copies),
    ok = ekka_mnesia:copy_table(?SESS_MSG_TAB, ram_copies),
    ok = ekka_mnesia:copy_table(?MSG_TAB, ram_copies).


%%--------------------------------------------------------------------
%% Session API
%%--------------------------------------------------------------------

%% The timestamp (TS) is the last time a client interacted with the session,
%% or when the client disconnected.
-spec persist(emqx_types:clientinfo(),
              emqx_types:conninfo(),
              emqx_session:session()) -> emqx_session:session().

persist(#{ clientid := ClientID }, ConnInfo, Session) ->
    case ClientID == undefined orelse not emqx_session:info(is_persistent, Session) of
        true -> ok;
        false ->
            SS = #session_store{ client_id       = ClientID
                               , expiry_interval = maps:get(expiry_interval, ConnInfo)
                               , ts              = timestamp_from_conninfo(ConnInfo)
                               , session         = Session},
            case persistent_session_status(SS) of
                not_persistent -> Session;
                expired        -> discard(ClientID, Session);
                persistent     -> ekka_mnesia:dirty_write(?SESSION_STORE, SS),
                                  Session
            end
    end.

timestamp_from_conninfo(ConnInfo) ->
    case maps:get(disconnected_at, ConnInfo, undefined) of
        undefined  -> erlang:system_time(millisecond);
        Disconnect -> Disconnect
    end.

lookup(ClientID) when is_binary(ClientID) ->
    case mnesia:dirty_read(?SESSION_STORE, ClientID) of
        [] -> [];
        [#session_store{session = S} = SS] ->
            case persistent_session_status(SS) of
                not_persistent -> []; %% For completeness. Should not happen
                expired        -> [];
                persistent     -> [S]
            end
    end.

-spec discard(binary()) -> 'ok'.
discard(ClientID) ->
    case lookup(ClientID) of
        [] -> ok;
        [Session] ->
            discard(ClientID, Session),
            ok
    end.

-spec discard(binary(), emgx_session:session()) -> emgx_session:session().
discard(ClientID, Session) ->
    ekka_mnesia:dirty_delete(?SESSION_STORE, ClientID),
    SessionID = emqx_session:info(id, Session),
    Key = {SessionID, <<>>, <<>>, ?ABANDONED},
    ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = Key }),
    Subscriptions = emqx_session:info(subscriptions, Session),
    emqx_session_router:delete_routes(SessionID, Subscriptions),
    emqx_session:set_field(is_persistent, false, Session).

-spec mark_resume_begin(emqx_session:sessionID()) -> emqx_guid:guid().
mark_resume_begin(SessionID) ->
    MarkerID = emqx_guid:gen(),
    DBKey = {SessionID, MarkerID, <<>>, ?MARKER},
    ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = DBKey }),
    MarkerID.

add_subscription(TopicFilter, SessionID, true = _IsPersistent) ->
    emqx_session_router:do_add_route(TopicFilter, SessionID);
add_subscription(_TopicFilter, _SessionID, false = _IsPersistent) ->
    ok.

remove_subscription(TopicFilter, SessionID, true = _IsPersistent) ->
    session_router:do_delete_route(TopicFilter, SessionID);
remove_subscription(_TopicFilter, _SessionID, false = _IsPersistent) ->
    ok.

%%--------------------------------------------------------------------
%% Resuming from DB state
%%--------------------------------------------------------------------

%% Must be called inside a emqx_cm_locker transaction.
-spec resume(emqx_types:clientinfo(), emqx_types:conninfo(), emqx_session:session()
            ) -> {emqx_session:session(), [emqx_types:deliver()]}.
resume(ClientInfo = #{clientid := ClientID}, ConnInfo, Session) ->
    SessionID = emqx_session:info(id, Session),
    ?tp(ps_resuming, #{from => db, sid => SessionID}),

    %% NOTE: Order is important!

    %% 1. Get pending messages from DB.
    ?tp(ps_initial_pendings, #{sid => SessionID}),
    Pendings1 = pending(SessionID),
    Pendings2 = emqx_session:ignore_local(Pendings1, ClientID, Session),
    ?tp(ps_got_initial_pendings, #{ sid => SessionID
                                  , msgs => Pendings1}),

    %% 2. Enqueue messages to mimic that the process was alive
    %%    when the messages were delivered.
    ?tp(ps_persist_pendings, #{sid => SessionID}),
    Session1 = emqx_session:enqueue(Pendings2, Session),
    persist(ClientInfo, ConnInfo, Session1),
    mark_as_delivered(SessionID, Pendings2),
    ?tp(ps_persist_pendings_msgs, #{ msgs => Pendings2
                                   , sid => SessionID}),

    %% 3. Notify writers that we are resuming.
    %%    They will buffer new messages.
    ?tp(ps_notify_writers, #{sid => SessionID}),
    Nodes = ekka_mnesia:running_nodes(),
    NodeMarkers = resume_begin(Nodes, SessionID),

    %% 4. Subscribe to topics.
    ?tp(ps_resume_session, #{sid => SessionID}),
    ok = emqx_session:resume(ClientInfo, Session1),

    %% 5. Get pending messages from DB until we find all markers.
    ?tp(ps_marker_pendings, #{sid => SessionID}),
    MarkerIDs = [Marker || {_, Marker} <- NodeMarkers],
    Pendings3 = pending(SessionID, MarkerIDs),
    Pendings4 = emqx_session:ignore_local(Pendings3, ClientID, Session),
    ?tp(ps_marker_pendings_msgs, #{ sid => SessionID
                                  , pendings => Pendings4}),

    %% 6. Get pending messages from writers.
    ?tp(ps_resume_end, #{sid => SessionID}),
    WriterPendings = resume_end(Nodes, SessionID),
    ?tp(ps_writer_pendings, #{ msgs => WriterPendings
                             , sid => SessionID}),

    %% 7. Drain the inbox and usort the messages
    %%    with the pending messages. (Should be done by caller.)
    {Session1, Pendings4 ++ WriterPendings}.

resume_begin(Nodes, SessionID) ->
    Res = erpc:multicall(Nodes, emqx_session_router, resume_begin, [self(), SessionID]),
    [{Node, Marker} || {{ok, {ok, Marker}}, Node} <- lists:zip(Nodes, Res)].

resume_end(Nodes, SessionID) ->
    Res = erpc:multicall(Nodes, emqx_session_router, resume_end, [self(), SessionID]),
    ?tp(ps_erpc_multical_result, #{ res => Res, sid => SessionID }),
    %% TODO: Should handle the errors
    [ {deliver, STopic, M}
      || {ok, {ok, Messages}} <- Res,
         {{M, STopic}} <- Messages
    ].


%%--------------------------------------------------------------------
%% Messages API
%%--------------------------------------------------------------------

persist_message(Msg) ->
    case emqx_message:get_flag(dup, Msg) orelse emqx_message:is_sys(Msg) of
        true  -> ok;
        false ->
            case emqx_session_router:match_routes(emqx_message:topic(Msg)) of
                [] -> ok;
                Routes ->
                    %% TODO: This should store in external backend
                    ekka_mnesia:dirty_write(?MSG_TAB, Msg),
                    MsgId = emqx_message:id(Msg),
                    persist_message_routes(Routes, MsgId, Msg)
            end
    end.

persist_message_routes([#route{dest = SessionID, topic = STopic}|Left], MsgId, Msg) ->
    Key = {SessionID, MsgId, STopic, ?UNDELIVERED},
    ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = Key }),
    emqx_session_router:buffer(SessionID, STopic, Msg),
    persist_message_routes(Left, MsgId, Msg);
persist_message_routes([], _MsgId, _Msg) ->
    ok.

mark_as_delivered(SessionID, [{deliver, STopic, Msg}|Left]) ->
    MsgID = emqx_message:id(Msg),
    Key = {SessionID, MsgID, STopic, ?DELIVERED},
    ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = Key }),
    mark_as_delivered(SessionID, Left);
mark_as_delivered(_SessionID, []) ->
    ok.

-spec pending(emqx_session:sessionID()) ->
          [{emqx_types:message(), STopic :: binary()}].
pending(SessionID) ->
    pending(SessionID, []).

-spec pending(emqx_session:sessionID(), MarkerIDs :: [emqx_guid:guid()]) ->
          [{emqx_types:message(), STopic :: binary()}].
pending(SessionID, MarkerIds) ->
    %% TODO: Handle lost MarkerIDs
    case emqx_session_router:pending(SessionID, MarkerIds) of
        incomplete ->
            timer:sleep(10),
            pending(SessionID, MarkerIds);
        Delivers ->
            Delivers
    end.

%%--------------------------------------------------------------------
%% Session internal functions
%%--------------------------------------------------------------------

%% @private [MQTT-3.1.2-23]
persistent_session_status(#session_store{expiry_interval = 0}) ->
    not_persistent;
persistent_session_status(#session_store{expiry_interval = ?MAX_EXPIRY_INTERVAL}) ->
    persistent;
persistent_session_status(#session_store{expiry_interval = E, ts = TS}) ->
    case E + TS > erlang:system_time(millisecond) of
        true  -> persistent;
        false -> expired
    end.

%%--------------------------------------------------------------------
%% Pending messages internal functions
%%--------------------------------------------------------------------

pending_messages_in_db(SessionID, MarkerIds) ->
    {atomic, Res} = ekka_mnesia:ro_transaction(
                      ?PERSISTENT_SESSION_SHARD,
                      pending_messages_fun(SessionID, MarkerIds)),
    Res.

pending_messages_fun(SessionID, MarkerIds) ->
    fun() ->
        case pending_messages({SessionID, <<>>, <<>>, ?DELIVERED}, [], MarkerIds) of
            {Pending, []} -> read_pending_msgs(Pending, []);
            {_Pending, [_|_]} -> incomplete
        end
    end.

read_pending_msgs([{MsgId, STopic}|Left], Acc) ->
    [Msg] = mnesia:read(?MSG_TAB, MsgId),
    read_pending_msgs(Left, [{deliver, STopic, Msg}|Acc]);
read_pending_msgs([], Acc) ->
    lists:reverse(Acc).


%% The keys are ordered by
%%     {sessionID(), <<>>, <<>>, ?ABANDONED} For abandoned sessions (clean started or expired).
%%     {sessionID(), emqx_guid:guid(), STopic :: binary(), ?DELIVERED | ?UNDELIVERED | ?MARKER}
%%  where
%%     <<>> < emqx_guid:guid()
%%     emqx_guid:guid() is ordered in ts() and by node()
%%     ?UNDELIVERED < ?DELIVERED < ?MARKER
%%
%% We traverse the table until we reach another session.
%% TODO: Garbage collect the delivered messages.
pending_messages({SessionID, PrevMsgId, PrevSTopic, PrevTag} = PrevKey, Acc, MarkerIds) ->
    case mnesia:dirty_next(?SESS_MSG_TAB, PrevKey) of
        {S, <<>>, <<>>, ?ABANDONED} when S =:= SessionID ->
            {[], []};
        {S, MsgId, <<>>, ?MARKER} = Key when S =:= SessionID ->
            MarkerIds1 = MarkerIds -- [MsgId],
            case PrevTag =:= ?UNDELIVERED of
                false -> pending_messages(Key, Acc, MarkerIds1);
                true  -> pending_messages(Key, [{PrevMsgId, PrevSTopic}|Acc], MarkerIds1)
            end;
        {S, MsgId, STopic, ?DELIVERED} = Key when S =:= SessionID,
                                                  MsgId =:= PrevMsgId,
                                                  STopic =:= PrevSTopic ->
            pending_messages(Key, Acc, MarkerIds);
        {S, _MsgId, _STopic, _Tag} = Key when S =:= SessionID ->
            case PrevTag =:= ?UNDELIVERED of
                false -> pending_messages(Key, Acc, MarkerIds);
                true  -> pending_messages(Key, [{PrevMsgId, PrevSTopic}|Acc], MarkerIds)
            end;
        _What -> %% Next sessionID or '$end_of_table'
            case PrevTag =:= ?UNDELIVERED of
                false -> {lists:reverse(Acc), MarkerIds};
                true  -> {lists:reverse([{PrevMsgId, PrevSTopic}|Acc]), MarkerIds}
            end
    end.
