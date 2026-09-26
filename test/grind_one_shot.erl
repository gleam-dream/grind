-module(grind_one_shot).
-export([new/0, arm/1, take/1]).

%% A one-shot counter a test can hand to a `Hooks` closure it builds
%% (`grind/internal/consumer_hooks`), so the closure fires its fault exactly
%% once — the first `take/1` call after the counter is armed — and behaves
%% normally every other time. Not process-bound: the closure runs inside
%% the coordinator process under test, a different process from the one
%% that arms the counter, so a plain Gleam variable or the calling
%% process's own dictionary cannot carry this state across that boundary.
%% `counters` is Erlang/OTP's own mutable counter primitive, freshly zeroed
%% by `counters:new/2`; only sequential access from one coordinator's own
%% message loop is ever expected here, so the get-then-subtract in `take/1`
%% needs no atomic compare-and-swap.
new() ->
    counters:new(1, []).

arm(Ref) ->
    counters:put(Ref, 1, 1),
    nil.

take(Ref) ->
    case counters:get(Ref, 1) of
        N when N > 0 ->
            counters:sub(Ref, 1, 1),
            true;
        _ ->
            false
    end.
