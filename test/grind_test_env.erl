-module(grind_test_env).
-export([database_url/0, queue_database_url/0, owner_a_url/0, owner_b_url/0, schema_bad_url/0, schema_v1_url/0, schema_v2_url/0, schema_v3_url/0, schema_v4_url/0, schema_v5_url/0, schema_v4_missing_receipt_url/0, resolution_route_a_url/0, resolution_route_b_url/0, mark_database_test_executed/1]).

database_url() -> env("GRIND_TEST_DATABASE_URL").
queue_database_url() -> env("GRIND_TEST_QUEUE_DATABASE_URL").
owner_a_url() -> env("GRIND_TEST_OWNER_A_URL").
owner_b_url() -> env("GRIND_TEST_OWNER_B_URL").
schema_bad_url() -> env("GRIND_TEST_SCHEMA_BAD_URL").
schema_v1_url() -> env("GRIND_TEST_SCHEMA_V1_URL").
schema_v2_url() -> env("GRIND_TEST_SCHEMA_V2_URL").
schema_v3_url() -> env("GRIND_TEST_SCHEMA_V3_URL").
schema_v4_url() -> env("GRIND_TEST_SCHEMA_V4_URL").
schema_v5_url() -> env("GRIND_TEST_SCHEMA_V5_URL").
schema_v4_missing_receipt_url() -> env("GRIND_TEST_SCHEMA_V4_MISSING_RECEIPT_URL").
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
