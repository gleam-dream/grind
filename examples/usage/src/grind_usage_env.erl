-module(grind_usage_env).
-export([database_url/0, cigogne_url/0, mark/1, stop_supervisor/1]).

%% Environment and process plumbing for the usage examples. Application code
%% needs none of it.

database_url() -> env("GRIND_USAGE_DATABASE_URL").
cigogne_url() -> env("GRIND_USAGE_CIGOGNE_DATABASE_URL").

env(Name) ->
    case os:getenv(Name) of
        false -> {error, nil};
        "" -> {error, nil};
        Url -> {ok, unicode:characters_to_binary(Url)}
    end.

%% Records that a topic ran to its last assertion. The gate reads the file to
%% prove that every topic executed. Without a marker file this does nothing.
mark(Name) ->
    case os:getenv("GRIND_USAGE_TEST_MARKER") of
        false -> nil;
        "" -> nil;
        Path ->
            ok = file:write_file(Path, <<Name/binary, "\n">>, [append]),
            nil
    end.

%% Stops a supervision tree started by an example and waits for it to exit.
stop_supervisor(Pid) ->
    Ref = erlang:monitor(process, Pid),
    unlink(Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', Ref, process, Pid, _} -> nil
    after 15000 ->
        erlang:demonitor(Ref, [flush]),
        nil
    end.
