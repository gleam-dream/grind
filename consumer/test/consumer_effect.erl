-module(consumer_effect).
-export([reset/0, apply/2, count/1]).

-define(TABLE, grind_consumer_effects).

reset() ->
    case ets:whereis(?TABLE) of
        undefined -> _ = ets:new(?TABLE, [named_table, set, public]);
        Table -> ets:delete_all_objects(Table)
    end,
    nil.

apply(Key, _Amount) ->
    Receipt = <<"synthetic-receipt/", Key/binary>>,
    case ets:insert_new(?TABLE, {Key, Receipt, 1}) of
        true -> {Receipt, 1};
        false ->
            case ets:lookup(?TABLE, Key) of
                [{Key, ExistingReceipt, Calls}] -> {ExistingReceipt, Calls};
                [] -> erlang:error(effect_disappeared)
            end
    end.

count(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, _Receipt, Calls}] -> Calls;
        [] -> 0
    end.
