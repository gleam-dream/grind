%% A bench-owned, per-node ETS counter keyed by `bench_index`, tracking how
%% many times each bench job's handler has actually run (`delivery_count` in
%% `bench_effects`) -- mirroring `consumer/test/consumer_counter.erl`'s own
%% pattern, since Gleam itself has no mutable closure state.
%%
%% `next(-1)` (a bench_index no real job ever uses) is repurposed by
%% `grind_bench/worker.record_effect` as a plain, monotonically increasing
%% counter of ledger-write failures -- read back by
%% `grind_bench/audit`'s partial I6 check via `value(-1)`.
-module(grind_bench_counter_ffi).
-export([reset/1, next/1, value/1, reset_all/0]).

-define(TABLE, grind_bench_counters).

reset(Key) ->
    ensure_table(),
    ets:insert(?TABLE, {Key, 0}),
    nil.

reset_all() ->
    ensure_table(),
    ets:delete_all_objects(?TABLE),
    nil.

next(Key) ->
    ensure_table(),
    ets:update_counter(?TABLE, Key, {2, 1}, {Key, 0}).

value(Key) ->
    ensure_table(),
    case ets:lookup(?TABLE, Key) of
        [{Key, N}] -> N;
        [] -> 0
    end.

ensure_table() ->
    case ets:whereis(?TABLE) of
        undefined -> _ = ets:new(?TABLE, [named_table, public, set]);
        _ -> ok
    end.
