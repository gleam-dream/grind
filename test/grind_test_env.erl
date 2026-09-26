-module(grind_test_env).
-export([database_url/0, queue_database_url/0, owner_a_url/0, owner_b_url/0, schema_bad_url/0, schema_fresh_url/0, schema_markers_url/0, schema_missing_jobs_url/0, schema_missing_migrations_url/0, schema_missing_resolutions_url/0, schema_missing_acknowledgements_url/0, schema_missing_attempt_sequence_url/0, schema_missing_unique_submissions_url/0, schema_atomic_url/0, schema_concurrent_url/0, schema_partial_url/0, schema_upgrade_url/0, schema_upgrade_fresh_url/0, schema_future_foreign_url/0, schema_shape_url/0, schema_mixed_case_url/0, resolution_route_a_url/0, resolution_route_b_url/0, repeatable_read_url/0, quarantine_url/0, fault_proxy_url/0, migration_deadline_url/0, mark_database_test_executed/1, monotonic_ms/0, unique_test_run_id/0, pool_connection_atom/1]).

database_url() -> env("GRIND_TEST_DATABASE_URL").
queue_database_url() -> env("GRIND_TEST_QUEUE_DATABASE_URL").
owner_a_url() -> env("GRIND_TEST_OWNER_A_URL").
owner_b_url() -> env("GRIND_TEST_OWNER_B_URL").
schema_bad_url() -> env("GRIND_TEST_SCHEMA_BAD_URL").
schema_fresh_url() -> env("GRIND_TEST_SCHEMA_FRESH_URL").
schema_markers_url() -> env("GRIND_TEST_SCHEMA_MARKERS_URL").
schema_missing_jobs_url() -> env("GRIND_TEST_SCHEMA_MISSING_JOBS_URL").
schema_missing_migrations_url() -> env("GRIND_TEST_SCHEMA_MISSING_MIGRATIONS_URL").
schema_missing_resolutions_url() -> env("GRIND_TEST_SCHEMA_MISSING_RESOLUTIONS_URL").
schema_missing_acknowledgements_url() -> env("GRIND_TEST_SCHEMA_MISSING_ACK_URL").
schema_missing_attempt_sequence_url() -> env("GRIND_TEST_SCHEMA_MISSING_ATTEMPT_SEQUENCE_URL").
schema_missing_unique_submissions_url() -> env("GRIND_TEST_SCHEMA_MISSING_UNIQUE_SUBMISSIONS_URL").
schema_atomic_url() -> env("GRIND_TEST_SCHEMA_ATOMIC_URL").
schema_concurrent_url() -> env("GRIND_TEST_SCHEMA_CONCURRENT_URL").
schema_partial_url() -> env("GRIND_TEST_SCHEMA_PARTIAL_URL").
schema_upgrade_url() -> env("GRIND_TEST_SCHEMA_UPGRADE_URL").
schema_upgrade_fresh_url() -> env("GRIND_TEST_SCHEMA_UPGRADE_FRESH_URL").
schema_future_foreign_url() -> env("GRIND_TEST_SCHEMA_FUTURE_FOREIGN_URL").
schema_shape_url() -> env("GRIND_TEST_SCHEMA_SHAPE_URL").
schema_mixed_case_url() -> env("GRIND_TEST_SCHEMA_MIXED_CASE_URL").
resolution_route_a_url() -> env("GRIND_TEST_RESOLUTION_ROUTE_A_URL").
resolution_route_b_url() -> env("GRIND_TEST_RESOLUTION_ROUTE_B_URL").
repeatable_read_url() -> env("GRIND_TEST_REPEATABLE_READ_URL").
quarantine_url() -> env("GRIND_TEST_QUARANTINE_URL").
fault_proxy_url() -> env("GRIND_TEST_FAULT_PROXY_URL").
migration_deadline_url() -> env("GRIND_TEST_MIGRATION_DEADLINE_URL").

env(Name) ->
    case os:getenv(Name) of
        false -> {error, nil};
        Url -> {ok, unicode:characters_to_binary(Url)}
    end.

mark_database_test_executed(Name) ->
    case os:getenv("GRIND_TEST_MARKER") of
        false -> erlang:error(missing_grind_test_marker);
        Path ->
            ok = file:write_file(Path, <<Name/binary, "\n">>, [append]),
            nil
    end.

%% Monotonic wall-clock milliseconds, for measuring elapsed duration in a
%% timing-sensitive test. Never used to derive a shared point in time across
%% processes or as a substitute for a database-time boundary.
monotonic_ms() -> erlang:monotonic_time(millisecond).

%% A positive integer unique across both calls within one `gleam test` run
%% AND separate runs (each of which is its own fresh Erlang VM, where
%% `erlang:unique_integer/1` alone restarts from a small value every time —
%% confirmed by observation: reusing it alone made a second, immediate
%% `gleam test` run against the same un-recreated database collide with the
%% first run's leftover receipt and fail). Combines wall-clock microseconds
%% (differs across separate VM invocations) with the per-VM monotonic
%% counter (differs across calls within one run). For suffixing fixed test
%% literals (worker ids, submission ids, queue names) so uniqueness tests
%% can be re-run against a persistent development database without
%% colliding with rows/receipts a previous run left behind. Never used as a
%% substitute for the disposable-cluster isolation the CI gate already
%% provides.
unique_test_run_id() ->
    erlang:system_time(microsecond) * 1000000 +
        erlang:unique_integer([positive, monotonic]) rem 1000000.

%% Canary for `grind_postgres_ffi`'s own dependency on pog's private
%% `pog.Connection` shape (`{pool, Name} | {single_connection, Conn}`):
%% `pog_connection_pool_shape_test` (test/grind_test.gleam) asserts a freshly
%% named connection is still the `{pool, Name}` tuple this matches. A pog
%% upgrade that changes that internal representation would fail this probe
%% loudly instead of letting `grind_postgres_ffi:with_deadline/3` silently
%% stop matching and fall through to some other, wrong behavior.
pool_connection_atom({pool, Name}) -> {ok, Name};
pool_connection_atom(_Other) -> {error, nil}.
