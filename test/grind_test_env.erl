-module(grind_test_env).
-export([database_url/0, queue_database_url/0, owner_a_url/0, owner_b_url/0, schema_bad_url/0, schema_fresh_url/0, schema_markers_url/0, schema_missing_jobs_url/0, schema_missing_migrations_url/0, schema_missing_resolutions_url/0, schema_missing_acknowledgements_url/0, schema_missing_attempt_sequence_url/0, schema_atomic_url/0, resolution_route_a_url/0, resolution_route_b_url/0, mark_database_test_executed/1]).

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
schema_atomic_url() -> env("GRIND_TEST_SCHEMA_ATOMIC_URL").
resolution_route_a_url() -> env("GRIND_TEST_RESOLUTION_ROUTE_A_URL").
resolution_route_b_url() -> env("GRIND_TEST_RESOLUTION_ROUTE_B_URL").

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
