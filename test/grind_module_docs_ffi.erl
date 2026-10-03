-module(grind_module_docs_ffi).
-export([public_sources/0]).

%% Every public module's source path, with its text. Modules under
%% src/grind/internal are excluded, matching Gleam's internal_modules default.
public_sources() ->
    Paths = ["src/grind.gleam" | filelib:wildcard("src/grind/*.gleam")],
    [{unicode:characters_to_binary(Path), read(Path)}
     || Path <- lists:sort(Paths), not lists:prefix("src/grind/internal/", Path)].

read(Path) ->
    {ok, Text} = file:read_file(Path),
    Text.
