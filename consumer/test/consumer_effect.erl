-module(consumer_effect).
-export([reset/0, apply/2, count/1, receipt/1, arm_crash_after_effect/1, take_fault/1]).

-define(TABLE, grind_consumer_effects).
-define(FAULT_TABLE, grind_consumer_effect_faults).

reset() ->
    reset_table(?TABLE),
    reset_table(?FAULT_TABLE),
    nil.

reset_table(Table) ->
    case ets:whereis(Table) of
        undefined -> _ = ets:new(Table, [named_table, set, public]);
        Existing -> ets:delete_all_objects(Existing)
    end.

%% Applies the caller's synthetic effect exactly once per idempotency key: a
%% first call for a key inserts a freshly minted receipt (carrying a unique
%% token, so it cannot be predicted or reproduced by a pure function of the
%% key alone) and returns call count 1; every later call for the same key
%% returns that same retained receipt and call count unchanged, regardless of
%% the amount given. This is the app's own dedup/idempotency table, not a
%% Grind guarantee: an assertion that a job's committed outcome equals the
%% receipt read back from this table (receipt/1) is evidence the value
%% actually came from this table, not merely a value a test could have
%% computed independently from the key.
%%
%% If a one-shot crash was armed for this key (see arm_crash_after_effect/1),
%% the effect is still applied and retained first, and only then does this
%% call raise, simulating a worker crashing after performing its effect but
%% before Grind's acknowledgement commits.
apply(Key, _Amount) ->
    Token = erlang:unique_integer([positive]),
    Receipt =
        <<"synthetic-receipt/", Key/binary, "/",
            (integer_to_binary(Token))/binary>>,
    Result =
        case ets:insert_new(?TABLE, {Key, Receipt, 1}) of
            true -> {Receipt, 1};
            false ->
                case ets:lookup(?TABLE, Key) of
                    [{Key, ExistingReceipt, Calls}] -> {ExistingReceipt, Calls};
                    [] -> erlang:error(effect_disappeared)
                end
        end,
    case take_fault(Key) of
        true -> erlang:error({simulated_crash_after_effect, Key});
        false -> Result
    end.

count(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, _Receipt, Calls}] -> Calls;
        [] -> 0
    end.

receipt(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, Receipt, _Calls}] -> {ok, Receipt};
        [] -> {error, nil}
    end.

%% Arms a one-shot crash for the given key: the next apply/2 call for this
%% exact key applies (and retains) its effect as usual, then raises. The
%% flag is consumed by that one call; every later call for the same key
%% (e.g. an authorized replay's rerun) applies normally with no crash.
arm_crash_after_effect(Key) ->
    ensure_table(?FAULT_TABLE),
    ets:insert(?FAULT_TABLE, {Key, armed}),
    nil.

%% Atomically consumes an armed crash flag for Key, if any, returning
%% whether one was present. One-shot: a second call for the same key
%% returns false.
take_fault(Key) ->
    ensure_table(?FAULT_TABLE),
    case ets:take(?FAULT_TABLE, Key) of
        [{Key, armed}] -> true;
        [] -> false
    end.

ensure_table(Table) ->
    case ets:whereis(Table) of
        undefined -> _ = ets:new(Table, [named_table, set, public]);
        _ -> ok
    end.
