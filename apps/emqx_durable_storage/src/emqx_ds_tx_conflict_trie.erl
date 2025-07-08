%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements a data structure for storing
%% information about recent updates. It is used to reject transactions
%% that may potentially read dirty data.
%%
%% For simplicity, it doesn't support topic matching with '+'.
-module(emqx_ds_tx_conflict_trie).

%% API:
-export([
    topic_filter_to_conflict_domain/1,
    new/2,
    push_topic/3,
    push_stream_range/3,
    rotate/1,
    is_dirty_topic/3,
    is_dirty_stream_range/3,
    print/1
]).

-export_type([t/0, conflict_domain/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

%%================================================================================
%% Type declarations
%%================================================================================

%% Note: currently conflict checking for stream time ranges is very
%% simplistic. We don't use a fancy interval tree or anything...
%% Currently the only operation that dirties a time range in the
%% stream is `tx_delete_time_range'. It's intended for cleaning up old
%% data. As such, we assume that it is:
%%
%% 1. This operation is used on large ranges of keys spanning {0, T}
%% 2. It deletes old data that nobody _has_ to read.
%%
%% Because of that we simply store the biggest interval containing all
%% dirty intervals and it has the largest serial of all
%% `delete_time_range' operations.
-type dirty_streams() :: #{
    emqx_ds:stream() => {emqx_ds:time(), emqx_ds:time(), emqx_ds_optimistic_tx:serial()}
}.

-record(conflict_tree, {
    rotate_every :: pos_integer() | infinity,
    min_serial,
    old_max_serial,
    max_serial,
    %% Dirty topics:
    old_trie = gb_trees:empty(),
    trie = gb_trees:empty(),
    %% Dirty streams:
    old_dirty_streams = #{} :: dirty_streams(),
    dirty_streams = #{} :: dirty_streams()
}).

-opaque t() :: #conflict_tree{}.

-type conflict_domain() :: [binary() | '#'].

%%================================================================================
%% API functions
%%================================================================================

-spec print(t()) -> map().
print(#conflict_tree{min_serial = Min, max_serial = Max, old_trie = Old, trie = Current}) ->
    #{
        range => {Min, Max},
        old => gb_trees:to_list(Old),
        current => gb_trees:to_list(Current)
    }.

%% @doc Translate topic filter to conflict domain. This function
%% replaces all topic levels following a wildcard (+ or #) with #
-spec topic_filter_to_conflict_domain(emqx_ds:topic_filter()) -> conflict_domain().
topic_filter_to_conflict_domain(TF) ->
    tf2cd(TF, []).

%% @doc Create a new conflict tracking trie.
%%
%% @param MinSerial Transaction serial at the time of creation.
%%
%% @param RotateEvery Automatically rotate the trie when the tracked
%% serial interval reaches this value. Atom `infinity' disables
%% automatic rotation.
-spec new(emqx_ds_optimistic_tx:serial(), pos_integer() | infinity) -> t().
new(MinSerial, RotateEvery) when
    RotateEvery > 0;
    RotateEvery =:= infinity
->
    #conflict_tree{
        rotate_every = RotateEvery,
        min_serial = MinSerial,
        max_serial = MinSerial,
        old_max_serial = MinSerial
    }.

%% @doc Add a new conflict domain to the trie.
-spec push_topic(conflict_domain(), emqx_ds_optimistic_tx:serial(), t()) -> t().
push_topic(
    CD,
    Serial,
    S = #conflict_tree{min_serial = MinS, max_serial = MaxS, trie = Trie}
) when Serial > MinS, Serial >= MaxS ->
    case need_rotation(Serial, S) of
        true ->
            push_topic(CD, Serial, rotate(S));
        false ->
            WildcardPrefix = static_prefix(CD),
            S#conflict_tree{
                max_serial = Serial,
                trie = do_push(WildcardPrefix, CD, Serial, Trie)
            }
    end.

%% @doc Add a new conflict over stream time range.
-spec push_stream_range(emqx_ds:stream_range(), emqx_ds_optimistic_tx:serial(), t()) -> t().
push_stream_range(
    SR = {Stream, From, To},
    Serial,
    S = #conflict_tree{min_serial = MinS, max_serial = MaxS, dirty_streams = DS0}
) when Serial > MinS, Serial >= MaxS ->
    case need_rotation(Serial, S) of
        true ->
            push_stream_range(SR, Serial, rotate(S));
        false ->
            DS = maps:update_with(
                Stream,
                fun({OldFrom, OldTo, _}) ->
                    {min(From, OldFrom), max(To, OldTo), Serial}
                end,
                {From, To, Serial},
                DS0
            ),
            S#conflict_tree{
                max_serial = Serial,
                dirty_streams = DS
            }
    end.

%% @doc Return `false' if there are no entries with serial **greater**
%% than `Serial' in the tracked conflict range. Otherwise, return
%% `true'.
-spec is_dirty_topic(conflict_domain(), emqx_ds_optimistic_tx:serial(), t()) -> boolean().
is_dirty_topic(
    _CD,
    Serial,
    #conflict_tree{min_serial = MinS}
) when Serial < MinS ->
    %% Serial is out of the tracked conflict range:
    true;
is_dirty_topic(
    CD,
    Serial,
    #conflict_tree{trie = Trie, old_trie = OldTrie}
) ->
    check_dirty(CD, Serial, Trie) orelse
        check_dirty(CD, Serial, OldTrie).

-spec is_dirty_stream_range(emqx_ds:stream_range(), emqx_ds_optimistic_tx:serial(), t()) ->
    boolean().
is_dirty_stream_range(
    _CR,
    Serial,
    #conflict_tree{min_serial = MinS}
) when Serial < MinS ->
    %% Serial is out of the tracked conflict range:
    true;
is_dirty_stream_range(
    CR = {_Stream, From, To},
    Serial,
    #conflict_tree{dirty_streams = DS, old_dirty_streams = ODS}
) when is_integer(From), is_integer(To), From =< To ->
    check_dirty_stream_range(CR, Serial, DS) orelse
        check_dirty_stream_range(CR, Serial, ODS).

%% @doc This function is used to reduce size of the trie by removing old
%% conflicts.
%%
%% It moves `current' trie to `old', empties the current trie, and
%% shrinks the tracked interval.
%%
%% From practical standpoint it means that any transaction that
%% started earlier than the beginning of the conflict window is
%% unconditionally considered conflicting and is rejected.
-spec rotate(t()) -> t().
rotate(
    S = #conflict_tree{
        old_max_serial = OldMax, max_serial = Max, trie = Trie, dirty_streams = Streams
    }
) ->
    S#conflict_tree{
        min_serial = OldMax,
        old_max_serial = Max,
        max_serial = Max,
        old_trie = Trie,
        trie = gb_trees:empty(),
        old_dirty_streams = Streams,
        dirty_streams = #{}
    }.

%%================================================================================
%% Internal functions
%%================================================================================

check_dirty_stream_range({Stream, From, To}, Serial, Dirty) ->
    case Dirty of
        #{Stream := {DirtyFrom, DirtyTo, DirtySerial}} ->
            ranges_intersect(From, To, DirtyFrom, DirtyTo) andalso
                DirtySerial >= Serial;
        #{} ->
            false
    end.

ranges_intersect(Ab, Ae, Bb, Be) ->
    not (Ab > Be orelse Ae < Bb).

tf2cd([], Acc) ->
    lists:reverse(Acc);
tf2cd(['#' | _], Acc) ->
    lists:reverse(['#' | Acc]);
tf2cd(['+' | _], Acc) ->
    lists:reverse(['#' | Acc]);
tf2cd([Const | Rest], Acc) ->
    tf2cd(Rest, [Const | Acc]).

do_push(false, CD, Serial, Trie) ->
    gb_trees:enter(CD, Serial, Trie);
do_push(WildcardPrefix, CD, Serial, Trie) ->
    gb_trees:enter(CD, Serial, remove_keys_with_prefix(WildcardPrefix, Trie)).

check_dirty(CD, Serial, Trie) ->
    WCPrefixes = wc_prefixes(CD),
    maybe
        %% 1. Verify the exact domain:
        false ?= compare_domain_serial(CD, Serial, Trie),
        %% 2. Verify its wildcard prefixes:
        false ?=
            lists:search(
                fun(WCPrefix) ->
                    compare_domain_serial(WCPrefix, Serial, Trie)
                end,
                WCPrefixes
            ),
        %% 3. If CD is itself a wildcard we need to check all
        %% subdomains in the trie:
        case static_prefix(CD) of
            false ->
                false;
            Prefix ->
                Fun = fun(_Key, KeySerial, Acc) ->
                    Acc orelse (KeySerial > Serial)
                end,
                fold_keys_with_prefix(Fun, Prefix, Trie, false)
        end
    else
        _ -> true
    end.

compare_domain_serial(CD, Serial, Trie) ->
    case gb_trees:lookup(CD, Trie) of
        {value, Val} when Val > Serial ->
            true;
        _ ->
            false
    end.

remove_keys_with_prefix(Prefix, Trie) ->
    Fun = fun(Key, _Val, Acc) ->
        gb_trees:delete(Key, Acc)
    end,
    fold_keys_with_prefix(Fun, Prefix, Trie, Trie).

fold_keys_with_prefix(Fun, Prefix, Trie, Acc) ->
    It = gb_trees:iterator_from(Prefix, Trie),
    do_fold_keys_with_prefix(Fun, Prefix, It, Acc).

do_fold_keys_with_prefix(Fun, Prefix, It0, Acc0) ->
    case gb_trees:next(It0) of
        none ->
            Acc0;
        {Key, Val, It} ->
            case lists:prefix(Prefix, Key) of
                true ->
                    Acc = Fun(Key, Val, Acc0),
                    do_fold_keys_with_prefix(Fun, Prefix, It, Acc);
                false ->
                    Acc0
            end
    end.

%% Return "static" part of the wildcard conflict domain or `false' for
%% non-wildcard domains.
-spec static_prefix(conflict_domain()) -> [binary()] | false.
static_prefix(L) ->
    static_prefix(L, []).

static_prefix([], _Acc) ->
    false;
static_prefix(['#'], Acc) ->
    lists:reverse(Acc);
static_prefix([A | L], Acc) ->
    static_prefix(L, [A | Acc]).

%% Return all wildcard conflict domains that match the given conflict
%% domain.
-spec wc_prefixes(conflict_domain()) -> [conflict_domain()].
wc_prefixes(CD) ->
    wc_prefixes(CD, [], []).

wc_prefixes(Suffix, PrefixAcc, Acc) ->
    Prefix = lists:reverse(['#' | PrefixAcc]),
    case Suffix of
        ['#'] ->
            Acc;
        [] ->
            [Prefix | Acc];
        [Token | Rest] ->
            wc_prefixes(Rest, [Token | PrefixAcc], [Prefix | Acc])
    end.

need_rotation(Serial, #conflict_tree{rotate_every = RotateEvery, old_max_serial = OldMax}) ->
    (Serial - OldMax) >= RotateEvery.

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

-define(dirty(CD, SERIAL, TRIE),
    ?assertMatch(
        true,
        is_dirty_topic(CD, SERIAL, TRIE)
    )
).

-define(clean(CD, SERIAL, TRIE),
    ?assertMatch(
        false,
        is_dirty_topic(CD, SERIAL, TRIE)
    )
).

tf2cd_test() ->
    ?assertEqual([], topic_filter_to_conflict_domain([])),
    ?assertEqual(
        [<<1>>, <<2>>],
        topic_filter_to_conflict_domain([<<1>>, <<2>>])
    ),
    ?assertEqual(
        [<<1>>, <<2>>, '#'],
        topic_filter_to_conflict_domain([<<1>>, <<2>>, '+', <<3>>])
    ),
    ?assertEqual(
        [<<1>>, <<2>>, '#'],
        topic_filter_to_conflict_domain([<<1>>, <<2>>, '#'])
    ).

%% Test is_dirty on an empty trie:
is_dirty0_test() ->
    Trie = mk_test_trie(0, []),
    ?clean([], 1, Trie),
    ?clean([], 1, rotate(Trie)),

    ?clean(['#'], 1, Trie),
    ?clean(['#'], 1, rotate(Trie)),

    ?clean([<<1>>, <<2>>, '#'], 1, Trie),
    ?clean([<<1>>, <<2>>, '#'], 1, rotate(Trie)).

%% Test is_dirty on a trie without wildcards:
is_dirty1_test() ->
    D1 = {[<<1>>], 2},
    D2 = {[<<1>>, <<2>>], 3},
    D3 = {[<<2>>], 4},
    D4 = {[], 5},
    Trie = mk_test_trie(0, [D1, D2, D3, D4]),
    %% Non-existent domains:
    ?clean([<<3>>], 1, Trie),
    ?clean([<<3>>], 1, rotate(Trie)),
    %% Existing domains:
    ?clean([<<1>>], 2, Trie),
    ?dirty([<<1>>], 1, Trie),
    ?dirty([<<1>>], 1, rotate(Trie)),

    ?dirty([], 1, Trie),
    ?dirty([], 4, rotate(Trie)),
    ?clean([], 5, Trie),
    ?clean([], 5, rotate(Trie)),
    %% Existing wildcard domains:
    ?dirty([<<1>>, '#'], 1, Trie),
    ?dirty([<<1>>, '#'], 1, rotate(Trie)),
    ?dirty([<<1>>, '#'], 2, Trie),
    ?dirty([<<1>>, '#'], 2, rotate(Trie)),

    ?clean([<<1>>, '#'], 3, Trie),
    ?clean([<<1>>, '#'], 3, rotate(Trie)),
    %% All domains:
    ?dirty(['#'], 1, Trie),
    ?dirty(['#'], 1, rotate(Trie)),
    ?dirty(['#'], 4, Trie),
    ?dirty(['#'], 4, rotate(Trie)),
    ?clean(['#'], 5, Trie),
    ?clean(['#'], 5, rotate(Trie)).

%% Test is_dirty on a trie with wildcards:
is_dirty2_test() ->
    D1 = {[<<1>>], 2},
    D2 = {[<<1>>, <<2>>, '#'], 3},
    D3 = {[<<2>>, '#'], 4},
    Trie = mk_test_trie(0, [D1, D2, D3]),
    %% Non-existent domains:
    ?clean([<<3>>], 1, Trie),
    ?clean([<<3>>], 1, rotate(Trie)),
    %% Existing domains:
    ?clean([<<1>>], 2, Trie),
    ?clean([<<1>>], 2, rotate(Trie)),
    ?dirty([<<1>>, <<2>>], 2, Trie),
    ?dirty([<<1>>, <<2>>], 2, rotate(Trie)),

    ?clean([<<1>>, <<2>>], 3, Trie),
    ?dirty([<<1>>, <<2>>, <<3>>], 2, Trie),
    ?clean([<<1>>, <<2>>, <<3>>], 3, Trie),
    %% Existing wildcard domains:
    ?dirty([<<1>>, <<2>>, '#'], 1, Trie),
    ?dirty([<<1>>, <<2>>, '#'], 1, rotate(Trie)),

    ?dirty([<<1>>, <<2>>, '#'], 2, Trie),
    ?clean([<<1>>, <<2>>, '#'], 3, Trie),
    ?clean([<<1>>, <<2>>, '#'], 3, rotate(Trie)),

    ?dirty([<<1>>, <<2>>, <<3>>, '#'], 2, Trie),
    ?clean([<<1>>, <<2>>, <<3>>, '#'], 3, Trie),

    %% All domains:
    ?dirty(['#'], 1, Trie),
    ?dirty(['#'], 1, rotate(Trie)),
    ?dirty(['#'], 3, Trie),
    ?dirty(['#'], 3, rotate(Trie)),
    ?clean(['#'], 4, Trie),
    ?clean(['#'], 4, rotate(Trie)).

wc_prefixes_test() ->
    ?assertEqual(
        [['#']],
        wc_prefixes([])
    ),
    ?assertEqual(
        [],
        wc_prefixes(['#'])
    ),
    ?assertEqual(
        [
            [<<>>, '#'],
            ['#']
        ],
        wc_prefixes([<<>>])
    ),
    ?assertEqual(
        [
            ['#']
        ],
        wc_prefixes([<<>>, '#'])
    ),
    ?assertEqual(
        [
            [<<1>>, <<2>>, <<3>>, '#'],
            [<<1>>, <<2>>, '#'],
            [<<1>>, '#'],
            ['#']
        ],
        wc_prefixes([<<1>>, <<2>>, <<3>>])
    ),
    ?assertEqual(
        [
            [<<1>>, <<2>>, '#'],
            [<<1>>, '#'],
            ['#']
        ],
        wc_prefixes([<<1>>, <<2>>, <<3>>, '#'])
    ).

push_test() ->
    T0 = new(0, infinity),
    T1 = push_topic([<<1>>], 1, T0),
    T2 = push_topic([<<1>>, <<1>>], 1, T1),
    T3 = push_topic([<<1>>, <<2>>], 1, T2),
    T4 = push_topic([<<1>>, <<3>>, '#'], 1, T3),
    T5 = push_topic([<<2>>], 2, T4),
    T6 = push_topic([<<2>>, <<1>>], 2, T5),
    T7 = push_topic([<<2>>, <<2>>], 2, T6),
    T8 = push_topic([<<2>>, <<3>>], 2, T7),
    ?assertEqual(
        [
            {[<<1>>], 1},
            {[<<1>>, <<1>>], 1},
            {[<<1>>, <<2>>], 1},
            {[<<1>>, <<3>>, '#'], 1},
            {[<<2>>], 2},
            {[<<2>>, <<1>>], 2},
            {[<<2>>, <<2>>], 2},
            {[<<2>>, <<3>>], 2}
        ],
        gb_trees:to_list(T8#conflict_tree.trie)
    ),
    %% Overwrite some key:
    T9 = push_topic([<<1>>, <<3>>, '#'], 3, T8),
    T10 = push_topic([<<1>>, <<2>>], 3, T9),
    ?assertEqual(
        [
            {[<<1>>], 1},
            {[<<1>>, <<1>>], 1},
            {[<<1>>, <<2>>], 3},
            {[<<1>>, <<3>>, '#'], 3},
            {[<<2>>], 2},
            {[<<2>>, <<1>>], 2},
            {[<<2>>, <<2>>], 2},
            {[<<2>>, <<3>>], 2}
        ],
        gb_trees:to_list(T10#conflict_tree.trie)
    ),
    %% Insert wildcard:
    T11 = push_topic([<<1>>, '#'], 4, T10),
    ?assertEqual(
        [
            {[<<1>>, '#'], 4},
            {[<<2>>], 2},
            {[<<2>>, <<1>>], 2},
            {[<<2>>, <<2>>], 2},
            {[<<2>>, <<3>>], 2}
        ],
        gb_trees:to_list(T11#conflict_tree.trie)
    ),
    ok.

rotate_test() ->
    T0 = new(0, 3),
    T1 = push_topic([<<1>>], 1, T0),
    T2 = push_topic([<<2>>], 2, T1),
    ?assertEqual(
        [],
        gb_trees:to_list(T2#conflict_tree.old_trie)
    ),
    %% Push an item with serial that should rotate the tree:
    T3 = push_topic([<<3>>], 3, T2),
    %% Old items were moved to the old trie:
    ?assertEqual(
        [
            {[<<1>>], 1},
            {[<<2>>], 2}
        ],
        gb_trees:to_list(T3#conflict_tree.old_trie)
    ),
    %% New item has been added to the current trie:
    ?assertEqual(
        [
            {[<<3>>], 3}
        ],
        gb_trees:to_list(T3#conflict_tree.trie)
    ),
    ?assertEqual(0, T3#conflict_tree.min_serial),
    ?assertEqual(2, T3#conflict_tree.old_max_serial),
    ?assertEqual(3, T3#conflict_tree.max_serial),
    %% Push an item to the same "generation":
    T4 = push_topic([<<4>>], 4, T3),
    %% Push an item that will rotate the tree again:
    T5 = push_topic([<<5>>], 6, T4),
    ?assertEqual(
        [
            {[<<3>>], 3},
            {[<<4>>], 4}
        ],
        gb_trees:to_list(T5#conflict_tree.old_trie)
    ),
    ?assertEqual(
        [
            {[<<5>>], 6}
        ],
        gb_trees:to_list(T5#conflict_tree.trie)
    ),
    ?assertEqual(2, T5#conflict_tree.min_serial),
    ?assertEqual(4, T5#conflict_tree.old_max_serial),
    ?assertEqual(6, T5#conflict_tree.max_serial),
    ok.

mk_test_trie(Min, L) ->
    lists:foldl(
        fun({Dom, Serial}, Acc) ->
            push_topic(Dom, Serial, Acc)
        end,
        new(Min, infinity),
        L
    ).

is_dirty_stream_range_test() ->
    Dirty = push_stream_range({foo, 10, 15}, 42, new(0, infinity)),
    ?assert(is_dirty_stream_range({foo, 1, 10}, 0, Dirty)),
    ?assert(is_dirty_stream_range({foo, 1, 10}, 0, rotate(Dirty))),
    ?assert(is_dirty_stream_range({foo, 15, 20}, 42, Dirty)),
    ?assert(is_dirty_stream_range({foo, 15, 20}, 42, rotate(Dirty))),
    %% Newer serial:
    ?assertNot(is_dirty_stream_range({foo, 15, 20}, 43, Dirty)),
    ?assertNot(is_dirty_stream_range({foo, 15, 20}, 43, rotate(Dirty))),
    ?assertNot(is_dirty_stream_range({foo, 0, 100}, 43, Dirty)),
    ?assertNot(is_dirty_stream_range({foo, 0, 100}, 43, rotate(Dirty))),
    %% Not overlapping:
    ?assertNot(is_dirty_stream_range({foo, 0, 9}, 0, Dirty)),
    ?assertNot(is_dirty_stream_range({foo, 16, 18}, 0, Dirty)).

ranges_intersect_test() ->
    ?assert(ranges_intersect(0, 0, 0, 0)),
    ?assert(ranges_intersect(0, 10, 0, 10)),
    ?assert(ranges_intersect(5, 5, 0, 10)),
    ?assert(ranges_intersect(5, 15, 0, 10)),
    ?assert(ranges_intersect(0, 5, 5, 10)),
    ?assert(ranges_intersect(0, 6, 5, 10)),
    ?assertNot(ranges_intersect(0, 0, 1, 1)),
    ?assertNot(ranges_intersect(1, 1, 0, 0)).

-endif.
