-module(grind_reconnect_probe).
-export([stale_holder_queries/3, multiple_stale_single_call/1, stale_holder_deadline/1]).

%% Fault injection uses actual idle pgo holders and real connection processes.
%% Neither the holder nor the queue is erased or replaced by this probe.
stale_holder_queries({pool, Name}, KillOwner, Query) ->
    Pool = whereis(Name),
    {ok, Ref, Conn} = pgo:checkout(Name, [{timeout, 1500}]),
    Owner = element(2, Conn),
    Socket = element(3, Conn),
    Holder = element(4, Ref),
    ok = pgo:checkin(Ref, Conn),
    %% Same-sender ordering places the checkin ahead of this barrier.
    {ready, Queue, _} = sys:get_state(Pool),
    Pool = ets:info(Holder, owner),
    case KillOwner of
        true ->
            Monitor = monitor(process, Owner),
            exit(Owner, kill),
            receive {'DOWN', Monitor, process, Owner, killed} -> ok
            after 2000 -> error(connection_did_not_die) end;
        false ->
            %% pgo's real reconnect path closes the current TCP socket and
            %% starts another connection in the SAME connection process.
            ok = pgo:break(Conn)
    end,
    Replacement = wait_replacement(Queue, Owner, Socket, KillOwner, 300),
    Pool = whereis(Name),
    Pool = ets:info(Holder, owner),
    false = socket_open(Socket),
    case KillOwner of
        true -> false = is_process_alive(Owner);
        false -> true = is_process_alive(Owner)
    end,
    Results = [Query() || _ <- lists:seq(1, 6)],
    io:format("STALE_HOLDER ~p~n", [#{killed_owner => KillOwner,
        old_owner => Owner, old_socket => Socket, old_holder => Holder,
        replacement => Replacement, results => Results}]),
    Pool = whereis(Name),
    true = is_process_alive(Pool),
    Results.

wait_replacement(_Queue, _Owner, _Socket, _Killed, 0) ->
    error(replacement_connection_not_ready);
wait_replacement(Queue, Owner, Socket, Killed, Remaining) ->
    Holders = [Holder || {{_, Holder}} <- ets:tab2list(Queue)],
    Ready = lists:filtermap(fun(Holder) ->
        case ets:lookup(Holder, '__info__') of
            [{_, Candidate, _, _, Conn}] ->
                CandidateSocket = element(3, Conn),
                CorrectOwner = case Killed of
                    true -> Candidate =/= Owner;
                    false -> Candidate =:= Owner
                end,
                case CorrectOwner andalso CandidateSocket =/= Socket
                     andalso is_process_alive(Candidate)
                     andalso socket_open(CandidateSocket) of
                    true -> {true, {Candidate, CandidateSocket, Holder}};
                    false -> false
                end;
            _ -> false
        end
    end, Holders),
    case Ready of
        [Replacement | _] -> Replacement;
        [] -> timer:sleep(10),
              wait_replacement(Queue, Owner, Socket, Killed, Remaining - 1)
    end.

socket_open(Socket) ->
    case inet:peername(Socket) of
        {ok, _} -> true;
        _ -> false
    end.

multiple_stale_single_call(Connection = {pool, Name}) ->
    {Pool, Stale} = poison_three_holders(Name),
    Calls = counters:new(1, []),
    Result = grind_postgres_ffi:call_safely(Connection, fun(CheckedOut) ->
        counters:add(Calls, 1, 1),
        {ok, _} = pog:execute(pog:query(<<"SELECT 99432 AS single_callback_probe">>), CheckedOut),
        %% A result returned AFTER actual SQL must stay unchanged. Holder
        %% admission may retry, but the callback must never be invoked again.
        {error, query_timeout}
    end),
    {error, query_timeout} = Result,
    1 = counters:get(Calls, 1),
    _ = sys:get_state(Pool),
    true = lists:all(fun(Holder) -> ets:info(Holder) =:= undefined end, Stale),
    {ok, _} = grind_postgres_ffi:execute_safely(
        pog:query(<<"SELECT 99433 AS following_callback_probe">>), Connection),
    Pool = whereis(Name),
    io:format("MULTIPLE_STALE_HOLDERS ~p~n", [#{retired => length(Stale),
        callbacks => counters:get(Calls, 1), result => Result}]),
    true.

stale_holder_deadline(Connection = {pool, Name}) ->
    {Pool, _Stale} = poison_three_holders(Name),
    DebuggerBefore = whereis(dbg_iserver),
    {module, pgo_pool} = int:i(pgo_pool),
    %% Pinned checkout_info/2 starts only after the actual holder transfer and
    %% deadline timer. Pause the first stale candidate, not a fake clock.
    ok = int:break(pgo_pool, 350),
    Parent = self(),
    Calls = counters:new(1, []),
    Caller = spawn(fun() ->
        Started = erlang:monotonic_time(millisecond),
        Result = grind_postgres_ffi:call_safely(Connection, fun(CheckedOut) ->
            counters:add(Calls, 1, 1),
            pog:execute(pog:query(<<"SELECT pg_sleep(1.5)">>), CheckedOut)
        end),
        Parent ! {deadline_result, self(), Result, erlang:monotonic_time(millisecond) - Started}
    end),
    try
        wait_break(Caller, 200),
        timer:sleep(1200),
        int:no_break(pgo_pool),
        ok = int:continue(Caller),
        receive
            {deadline_result, Caller, Result, Elapsed} ->
                io:format("STALE_HOLDER_DEADLINE ~p~n", [#{result => Result,
                    elapsed_ms => Elapsed, callbacks => counters:get(Calls, 1)}]),
                %% D=2000; resetting D after stale retirement would let the
                %% 1500ms SQL succeed after the 1200ms admission pause.
                {error, query_timeout} = Result,
                1 = counters:get(Calls, 1),
                true = Elapsed >= 1800,
                true = Elapsed < 2600,
                Pool = whereis(Name),
                true
        after 4000 -> error(stale_retirement_extended_deadline) end
    after
        catch exit(Caller, kill),
        int:no_break(pgo_pool),
        int:n(pgo_pool),
        false = lists:member(pgo_pool, int:interpreted()),
        case DebuggerBefore of
            undefined -> gen_server:stop(dbg_iserver, normal, infinity);
            _ -> ok
        end
    end.

poison_three_holders(Name) ->
    Pool = whereis(Name),
    {ok, Ref, Conn} = pgo:checkout(Name, [{timeout, 1500}]),
    ok = pgo:checkin(Ref, Conn),
    {ready, Queue, _} = sys:get_state(Pool),
    Stale = poison_holders(Queue, Conn, element(4, Ref), 3),
    4 = length(ets:tab2list(Queue)),
    true = lists:all(fun(Holder) -> ets:info(Holder, owner) =:= Pool end, Stale),
    {Pool, Stale}.

poison_holders(_Queue, _Conn, _Holder, 0) -> [];
poison_holders(Queue, Conn, Holder, Count) ->
    Owner = element(2, Conn),
    Socket = element(3, Conn),
    ok = pgo:break(Conn),
    {Owner, _, Replacement} = wait_replacement(Queue, Owner, Socket, false, 300),
    false = socket_open(Socket),
    [{_, Owner, _, _, NextConn}] = ets:lookup(Replacement, '__info__'),
    [Holder | poison_holders(Queue, NextConn, Replacement, Count - 1)].

wait_break(_, 0) -> error({no_checkout_debug_break, int:snapshot()});
wait_break(Pid, Remaining) ->
    case [ok || {Candidate, _, break, _} <- int:snapshot(), Candidate =:= Pid] of
        [_] -> ok;
        [] -> timer:sleep(10), wait_break(Pid, Remaining - 1)
    end.
