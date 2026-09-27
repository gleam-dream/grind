%% Mirrors consumer/test/consumer_test_env.erl's own pattern: environment
%% variable lookups plus a marker file the gate script can grep, so a
%% silently-skipped (missing env var) contract test cannot be mistaken for a
%% passing one.
-module(bench_test_env).
-export([database_url/0, schema_drift_url/0, mark/1]).

database_url() -> env("GRIND_BENCH_TEST_DATABASE_URL").
schema_drift_url() -> env("GRIND_BENCH_TEST_SCHEMA_DRIFT_URL").

env(Name) ->
    case os:getenv(Name) of
        false -> {error, nil};
        Url -> {ok, unicode:characters_to_binary(Url)}
    end.

mark(Name) ->
    case os:getenv("GRIND_BENCH_TEST_MARKER") of
        false -> erlang:error(missing_grind_bench_test_marker);
        Path ->
            ok = file:write_file(Path, <<Name/binary, "\n">>, [append]),
            nil
    end.
