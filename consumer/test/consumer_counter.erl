-module(consumer_counter).
-export([reset/1, next/1, value/1]).

%% A tiny named-counter table the test application owns, independent of
%% Grind. Used to count handler invocations per job/key without any mutable
%% state captured in a Gleam closure (Gleam has none).
-define(TABLE, grind_consumer_counters).

reset(Key) ->
    ensure_table(),
    ets:insert(?TABLE, {Key, 0}),
    nil.

%% Increments the named counter (creating it at 0 first if absent) and
%% returns the new value.
next(Key) ->
    ensure_table(),
    ets:update_counter(?TABLE, Key, {2, 1}, {Key, 0}).

%% Reads the current value without incrementing it; 0 if never touched.
value(Key) ->
    ensure_table(),
    case ets:lookup(?TABLE, Key) of
        [{Key, N}] -> N;
        [] -> 0
    end.

ensure_table() ->
    case ets:whereis(?TABLE) of
        undefined -> _ = ets:new(?TABLE, [named_table, set, public]);
        _ -> ok
    end.
