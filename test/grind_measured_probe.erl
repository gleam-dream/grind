-module(grind_measured_probe).
-export([counter/0, increment/1, count/1, with_contention/3,
         exceptions_preserved/1]).

counter() -> counters:new(1, []).
increment(Counter) -> counters:add(Counter, 1, 1), nil.
count(Counter) -> counters:get(Counter, 1).

%% Own the only real pool connection, then witness the tested caller queued
%% before starting the hold interval. This is a fault barrier against pinned
%% pgo state, not a synthetic replacement for its checkout implementation.
with_contention({pool, Name}, HoldMs, Run) ->
    {ok, Ref, Conn} = pgo:checkout(Name, [{timeout, 5000}]),
    Parent = self(),
    {Caller, Monitor} = spawn_monitor(fun() ->
        Reply = try Run() of Value -> {ok, Value}
                catch Class:Reason:Stack -> {raised, Class, Reason, Stack} end,
        Parent ! {measured_result, self(), Reply}
    end),
    try
        await_queued(Name, Caller, 200),
        HeldFrom = erlang:monotonic_time(microsecond),
        timer:sleep(HoldMs),
        HeldUs = erlang:monotonic_time(microsecond) - HeldFrom,
        ok = pgo:checkin(Ref, Conn),
        receive
            {measured_result, Caller, {ok, Value}} -> {Value, HeldUs};
            {measured_result, Caller, {raised, Class, Reason, Stack}} ->
                erlang:raise(Class, Reason, Stack);
            {'DOWN', Monitor, process, Caller, Reason} ->
                error({measured_caller_died, Reason})
        after 4000 -> error(measured_caller_did_not_return) end
    after
        %% Give back the test holder if a setup assertion failed before its
        %% ordinary checkin. Never return another process's holder twice.
        case ets:info(element(4, Ref), owner) of
            Parent -> catch pgo:checkin(Ref, Conn);
            _ -> ok
        end,
        exit(Caller, kill),
        demonitor(Monitor, [flush])
    end.

await_queued(_Name, _Caller, 0) -> error(measured_caller_never_queued);
await_queued(Name, Caller, Tries) ->
    {_, Queue, _} = sys:get_state(Name),
    Queued = lists:any(fun
        ({{_, _, {Pid, _}}}) -> Pid =:= Caller;
        (_) -> false
    end, ets:tab2list(Queue)),
    case Queued of
        true -> ok;
        false -> timer:sleep(5), await_queued(Name, Caller, Tries - 1)
    end.

%% A supplied stack makes preservation exact and keeps this test independent
%% of helper names or compiler tail-call choices. Exercise all BEAM classes,
%% including function_clause (which the production FFI narrowly guards).
exceptions_preserved(Connection) ->
    Stack = [{?MODULE, intentional_callback_failure, 0, [{line, 1}]}],
    lists:foreach(fun({Class, Reason}) ->
        Calls = counter(),
        Raise = fun(_CheckedOut) ->
            increment(Calls),
            erlang:raise(Class, Reason, Stack)
        end,
        lists:foreach(fun(Run) ->
            Before = count(Calls),
            try Run(Raise) of
                Value -> error({callback_exception_swallowed, Value})
            catch
                GotClass:GotReason:GotStack ->
                    {Class, Reason, Stack} = {GotClass, GotReason, GotStack}
            end,
            1 = count(Calls) - Before,
            %% A following real query needs the returned holder; the enclosing
            %% fixture also closes the managed pool after these exceptions.
            {ok, _} = grind_postgres_ffi:execute_safely(
                pog:query(<<"SELECT 917 AS after_callback_exception">>), Connection)
        end, [fun(F) -> grind_postgres_ffi:call_safely(Connection, F) end,
              fun(F) -> grind_postgres_ffi:call_measured(Connection, F) end,
              fun(F) -> grind_postgres_ffi:transaction_safely(Connection, F) end,
              fun(F) -> grind_postgres_ffi:transaction_measured(Connection, F) end])
    end, [{error, function_clause}, {throw, measured_throw}, {exit, measured_exit}]),
    true.
