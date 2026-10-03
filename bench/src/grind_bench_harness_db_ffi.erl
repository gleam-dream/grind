-module(grind_bench_harness_db_ffi).
-export([restart_retries/0, bump_restart_retries/0, is_noproc/1]).

restart_retries() ->
    persistent_counter_value().

bump_restart_retries() ->
    counters:add(counter(), 1, 1),
    nil.

%% pgo exits `{noproc, {pgo_pool, checkout, [Pool, Options]}}` when its pool
%% process is not registered.
is_noproc({noproc, _}) -> true;
is_noproc(noproc) -> true;
is_noproc(_) -> false.

counter() ->
    case persistent_term:get(grind_bench_harness_db_restarts, undefined) of
        undefined ->
            Counter = counters:new(1, [atomics]),
            persistent_term:put(grind_bench_harness_db_restarts, Counter),
            Counter;
        Counter -> Counter
    end.

persistent_counter_value() ->
    counters:get(counter(), 1).
