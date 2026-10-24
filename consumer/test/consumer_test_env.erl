-module(consumer_test_env).
-export([database_url/0, storage_failure_url/0, mark/1]).

database_url() -> env("GRIND_CONSUMER_DATABASE_URL").
storage_failure_url() -> env("GRIND_CONSUMER_STORAGE_FAILURE_URL").

env(Name) ->
    case os:getenv(Name) of
        false -> {error, nil};
        Url -> {ok, unicode:characters_to_binary(Url)}
    end.

mark(Name) ->
    case os:getenv("GRIND_CONSUMER_TEST_MARKER") of
        false -> erlang:error(missing_grind_consumer_test_marker);
        Path ->
            ok = file:write_file(Path, <<Name/binary, "\n">>, [append]),
            nil
    end.
