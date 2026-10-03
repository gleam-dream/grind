-module(grind_postgres_ffi).
-include_lib("pgo/src/pgo_internal.hrl").
-export([
    call_safely/2,
    execute_safely/2,
    call_measured/2,
    execute_measured/2,
    stop_consumer_supervisor/1,
    stop_supervisor/1,
    transaction_safely/2,
    transaction_measured/2,
    transaction_or_checkout_failure/2,
    migration_transaction_safely/3,
    is_single_connection/1
]).

%% Grind's own checkout deadline (`postgres.Settings.statement_deadline_ms`;
%% docs/RELEASE-READINESS.md, "Acknowledgement deadline"). Vanilla pog never
%% lets a caller configure the connection-hold deadline for a transaction or
%% a squirrel-generated call: `pog_ffi:checkout/1` always calls
%% `pgo:checkout/1` with pgo's own hardcoded 5000 ms (`pgo_pool.erl`'s
%% `?TIMEOUT`), and a `pog.Query`'s own `.timeout` field is silently ignored
%% once a connection is already checked out (`pog_ffi:query/4`'s
%% `{single_connection, _}` branch passes no timeout at all to
%% `pgo_handler:extended_query/4`). Every Grind storage call already funnels
%% through `execute_safely/2`, `call_safely/2`, `transaction_safely/2` or
%% `transaction_or_checkout_failure/2` below, so this module does its own
%% bounded `pgo:checkout/2` up front — passing an explicit `deadline` option
%% arms `pgo_pool`'s own absolute deadline timer (`pgo_pool.erl`,
%% `abs_timeout/2` + `start_deadline/5`), which force-closes the checked-out
%% socket if it is *still held* when the deadline elapses, regardless of what
%% statement is in flight. That is what actually bounds a stuck
%% `BEGIN`/`COMMIT`/renewal `UPDATE` on a half-open socket — proven
%% empirically against a real TCP fault proxy; see
%% docs/RECOVERY-EVIDENCE.md, "Acknowledgement deadline". Then it runs the
%% caller's work against the pog `Connection` shape `{single_connection,
%% Conn}` (`pog.gleam`'s compiled representation — confirmed against
%% build/packages/pog/src/pog.erl and pog_ffi.erl), so `pog:execute/2` and
%% `pog:transaction/2` never re-checkout with pog's own unconfigurable
%% default. This couples Grind directly to pog's private `Connection` shape
%% and to `pgo`'s private connection record and checkout/return APIs — accepted
%% deliberately
%% (user decision: no fork), guarded by pinning both dependencies to a tight
%% version range in gleam.toml and by `pog_connection_pool_shape_test`
%% (test/grind_test.gleam), which fails loudly the moment a pog/pgo upgrade
%% changes either shape instead of this module silently mismatching it. The
%% deadline is attached to a pool by its atom name (an
%% `erlang:process.Name` — itself just an atom; `gleam_erlang_ffi:new_name/1`)
%% via `persistent_term`, owned by the supervised deadline process in
%% `grind_pool_ffi`. The owner starts before the pool and stops after it,
%% including failed startup. It is not threaded through storage call sites.
%% Runs `Fun` (which receives both the `Connection` shape to actually issue
%% the call against, and the raw pgo `Conn` term underneath it, needed to
%% `pgo:break/1` it on a post-checkout protocol crash — see
%% `guarded_query`/`guarded_transaction`) under a Grind-owned checkout
%% deadline when `Connection` is a `Pool` (`{pool, PoolName}`); a
%% `SingleConnection` (`{single_connection, Conn}`, already checked out by an
%% enclosing call — always the case for the nested squirrel/inline calls
%% issued from inside a `transaction_safely` callback) runs `Fun` directly,
%% since nothing further to check out. `OnCheckoutFailure` produces this
%% caller's own "checkout itself failed" value — the callers below need
%% different shapes here (a bare `pog.QueryError` vs. a
%% `pog.TransactionError`), so it is not baked in here.
with_deadline(Connection, Fun, OnCheckoutFailure) ->
    {Value, _Timing} = with_deadline_timing(Connection, Fun, OnCheckoutFailure),
    Value.

with_deadline_timing({pool, PoolName}, Fun, OnCheckoutFailure) ->
    with_deadline_ms_timing(PoolName, configured, Fun, OnCheckoutFailure);
with_deadline_timing(SingleConnection = {single_connection, Conn}, Fun, _OnCheckoutFailure) ->
    {Fun(SingleConnection, Conn), no_checkout}.

%% pgo writes its query cache in the application caller, even after the pool
%% and socket owner have stopped. Register before checkout and release only
%% after checkin: the lifecycle owner cannot purge or reopen the pool while
%% a managed caller can still write old cache entries. A missing or closing
%% owner rejects the call before any database work is sent.
with_deadline_ms(PoolName, Deadline, Fun, OnCheckoutFailure) ->
    {Value, _Timing} = with_deadline_ms_timing(PoolName, Deadline, Fun, OnCheckoutFailure),
    Value.

with_deadline_ms_timing(PoolName, Deadline, Fun, OnCheckoutFailure) ->
    case grind_pool_ffi:acquire(PoolName) of
        {ok, Owner, Token, ConfiguredDeadlineMs} ->
            DeadlineMs = case Deadline of
                configured -> ConfiguredDeadlineMs;
                ExplicitDeadlineMs -> ExplicitDeadlineMs
            end,
            ExpiresAt = erlang:monotonic_time(millisecond) + DeadlineMs,
            try checkout_before(PoolName, ExpiresAt, Fun, OnCheckoutFailure, 0, 0)
            after grind_pool_ffi:release(Owner, Token)
            end;
        {error, closed} -> checkout_unavailable(OnCheckoutFailure, 0, 0)
    end.

%% A crashed or reconnected pgo connection can leave its old holder queued.
%% Checking it back in recycles that dead socket forever. Retire unusable
%% candidates before sending any SQL, sharing one absolute deadline across
%% all candidates. This retries admission only: Fun is never retried here.
checkout_before(PoolName, ExpiresAt, Fun, OnCheckoutFailure, WaitUs, Candidates) ->
    case erlang:monotonic_time(millisecond) < ExpiresAt of
        false -> checkout_unavailable(OnCheckoutFailure, WaitUs, Candidates);
        true -> checkout_candidate(PoolName, ExpiresAt, Fun, OnCheckoutFailure, WaitUs, Candidates)
    end.

checkout_candidate(PoolName, ExpiresAt, Fun, OnCheckoutFailure, WaitUs, Candidates) ->
    %% Time only the real checkout call. Probes, stale-holder retirement,
    %% callback execution and cleanup do not masquerade as pool waiting.
    %% Pinned pgo has no receive timeout while queued: this interval may exceed
    %% D, and measuring it must neither clamp nor replace the absolute deadline.
    StartedUs = erlang:monotonic_time(microsecond),
    try pgo:checkout(PoolName, [{timeout, infinity}, {deadline, ExpiresAt}]) of
        {ok, Ref, Conn} ->
            CheckedOutWaitUs = WaitUs + erlang:monotonic_time(microsecond) - StartedUs,
            case connection_usable(Conn, ExpiresAt) andalso
                 erlang:monotonic_time(millisecond) < ExpiresAt of
                true ->
                    Value = try Fun({single_connection, Conn}, Conn)
                    after
                        %% Cleanup cannot change a result or prove whether an
                        %% earlier command committed. Ambiguity stays ambiguous.
                        catch return_connection(Ref, Conn, ExpiresAt)
                    end,
                    {Value, {checkout_timing, CheckedOutWaitUs, Candidates + 1, checked_out}};
                false ->
                    retire_connection(Ref, Conn),
                    checkout_before(PoolName, ExpiresAt, Fun, OnCheckoutFailure,
                                    CheckedOutWaitUs, Candidates + 1)
            end;
        {error, _Reason} ->
            %% Covers every checkout-time rejection pgo can return here,
            %% including the codepath with no `pog_ffi:convert_error/1`
            %% clause (a plain string, "connection not available because
            %% deadline reached while in queue" — `pgo_pool.erl`,
            %% `checkout_info/2`) that can otherwise crash the caller with
            %% `error:function_clause` instead of a typed error (DEFECT 2).
            %% Never reached through `pog_ffi:checkout/1` or `pgo:query/3`
            %% any more, because this module always checks out itself first.
            %% Correctly `connection_unavailable`, not `query_timeout`: the
            %% checkout itself failed, so nothing was ever sent — this is
            %% the "knowably never ran" case, unlike the post-checkout crash
            %% `guarded_query`/`guarded_transaction` handle below.
            checkout_unavailable(OnCheckoutFailure,
                                 WaitUs + erlang:monotonic_time(microsecond) - StartedUs,
                                 Candidates + 1)
    catch
        exit:{_Reason, {pgo_pool, checkout, _Details}} ->
            checkout_unavailable(OnCheckoutFailure,
                                 WaitUs + erlang:monotonic_time(microsecond) - StartedUs,
                                 Candidates + 1)
    end.

checkout_unavailable(OnCheckoutFailure, WaitUs, Candidates) ->
    {OnCheckoutFailure(), {checkout_timing, WaitUs, Candidates, checkout_unavailable}}.

%% A completed report is built only after the enclosing helper's after clauses
%% have returned the connection and released its lifecycle token. Unexpected
%% exceptions propagate with their original class/reason/stack; no report is
%% returned for them. This boundary invokes no diagnostic callback or subscriber.
measured(Run) ->
    StartedUs = erlang:monotonic_time(microsecond),
    {Value, Timing} = Run(),
    {measured, Value, erlang:monotonic_time(microsecond) - StartedUs, Timing}.

return_connection(Ref, Conn, ExpiresAt) ->
    case connection_usable(Conn, ExpiresAt) of
        true -> pgo:checkin(Ref, Conn);
        false -> retire_connection(Ref, Conn)
    end.

retire_connection(Ref, Conn) ->
    %% break/1 only casts to the connection owner; it cannot remove a holder
    %% whose owner is dead or has already advanced to a replacement socket.
    catch pgo_pool:disconnect(Ref, {error, closed}, Conn, []),
    ok.

connection_usable(#conn{owner = Owner, socket = Socket,
                        socket_module = Module}, ExpiresAt) ->
    Remaining = ExpiresAt - erlang:monotonic_time(millisecond),
    is_process_alive(Owner) andalso Remaining > 0 andalso
        socket_usable(Module, Socket, ExpiresAt).

socket_usable(gen_tcp, Socket, _ExpiresAt) when is_port(Socket) ->
    socket_options_available(fun() -> inet:getopts(Socket, [active]) end);
socket_usable(gen_tcp, Socket, ExpiresAt) ->
    bounded_socket_probe(fun() -> inet:getopts(Socket, [active]) end, ExpiresAt);
socket_usable(ssl, Socket, ExpiresAt) ->
    bounded_socket_probe(fun() -> ssl:getopts(Socket, [active]) end, ExpiresAt).

bounded_socket_probe(GetOptions, ExpiresAt) ->
    %% TLS and gen_tcp's socket backend use infinite synchronous calls for
    %% getopts. A separate watchdog bounds the probe and notices caller death;
    %% its linked worker dies with it even if getopts never returns.
    Caller = self(),
    {Probe, Monitor} = spawn_monitor(fun() ->
        CallerMonitor = monitor(process, Caller),
        Watchdog = self(),
        Worker = spawn_link(fun() ->
            Watchdog ! {self(), socket_options_available(GetOptions)}
        end),
        Result = receive
            {Worker, Available} -> Available;
            {'DOWN', CallerMonitor, process, Caller, _Reason} -> false
        after max(0, ExpiresAt - erlang:monotonic_time(millisecond)) -> false
        end,
        exit({socket_probe, Result})
    end),
    receive
        {'DOWN', Monitor, process, Probe, {socket_probe, Result}} -> Result;
        {'DOWN', Monitor, process, Probe, _Reason} -> false
    after max(0, ExpiresAt - erlang:monotonic_time(millisecond)) ->
        exit(Probe, kill),
        demonitor(Monitor, [flush]),
        false
    end.

socket_options_available(GetOptions) ->
    try GetOptions() of
        {ok, _Options} -> true;
        {error, _Reason} -> false
    catch _:_ -> false
    end.

%% Defends the *post-checkout* half of DEFECT 2: `pgo_handler:extended_query/4`
%% can still return an error shape `pog_ffi:convert_error/1` has no clause
%% for (for example `econnreset`/`etimedout` on a genuinely lost socket,
%% distinct from the `closed` shape it does handle), raising
%% `error:function_clause` from *inside* `pog_ffi:convert_error/1` itself
%% rather than returning a typed `pog.QueryError`. Narrowed to a crash whose
%% top stack frame is genuinely `pog_ffi:convert_error` (`is_convert_error_crash/1`)
%% so an unrelated `function_clause` bug elsewhere still crashes its caller
%% instead of being silently absorbed into an endless `QueueAckUnknown` retry.
%%
%% This happens *after* the request was already sent — an `econnreset`/
%% `etimedout` on `recv` gives no proof the server never received or applied
%% it, unlike a checkout failure (nothing was ever sent). Reported as
%% `query_timeout` (uncertain — the same outcome a genuine query timeout
%% already produces, feeding the existing `QueueAckUnknown`/retry path), not
%% `connection_unavailable` (which the rest of this module reserves for
%% "checkout itself failed, knowably never ran"). `Conn`'s own state is
%% unknown after a crash mid-protocol decode, so it is `pgo:break/1`'d
%% (disconnected and replaced) before it is checked back in, rather than
%% risking a corrupted connection being handed to the next caller.
guarded_query(Conn, Fun) ->
    try Fun()
    catch
        exit:{_Reason, {pgo_pool, checkout, _Details}} ->
            {error, connection_unavailable};
        error:function_clause:Stacktrace ->
            case is_convert_error_crash(Stacktrace) of
                true ->
                    catch pgo:break(Conn),
                    {error, query_timeout};
                false ->
                    erlang:raise(error, function_clause, Stacktrace)
            end
    end.

guarded_transaction(Conn, Fun) ->
    try Fun()
    catch
        exit:{_Reason, {pgo_pool, checkout, _Details}} ->
            {error, {transaction_query_error, connection_unavailable}};
        error:function_clause:Stacktrace ->
            case is_convert_error_crash(Stacktrace) of
                true ->
                    catch pgo:break(Conn),
                    {error, {transaction_query_error, query_timeout}};
                false ->
                    erlang:raise(error, function_clause, Stacktrace)
            end
    end.

is_convert_error_crash([{pog_ffi, convert_error, _Arity, _Location} | _Rest]) ->
    true;
is_convert_error_crash(_Stacktrace) ->
    false.

%% Runs a single `pog.execute` call under the deadline. Kept as its own
%% exported function (rather than always going through `call_safely/2`)
%% because it is `execute_safely`'s own historical shape every Grind call
%% site already uses.
execute_safely(Query, Connection) ->
    call_safely(Connection, fun(Conn) -> pog:execute(Query, Conn) end).

execute_measured(Query, Connection) ->
    call_measured(Connection, fun(Conn) -> pog:execute(Query, Conn) end).

%% Generic form of `execute_safely/2`, for a Squirrel-generated query
%% function (`grind/internal/sql`) that calls `pog.execute` itself rather
%% than going through `execute_safely/2` — and for any other one-off call
%% that must run against *this* deadline-checked-out connection. `Fun`
%% receives the `Connection` to actually call `pog.execute`/a
%% Squirrel-generated function against — never the original `Connection`
%% passed in here, which may still be a `Pool`.
call_safely(Connection, Fun) ->
    with_deadline(
        Connection,
        fun(WrappedConn, RawConn) -> guarded_query(RawConn, fun() -> Fun(WrappedConn) end) end,
        fun() -> {error, connection_unavailable} end
    ).

call_measured(Connection, Fun) ->
    measured(fun() ->
        with_deadline_timing(
            Connection,
            fun(WrappedConn, RawConn) -> guarded_query(RawConn, fun() -> Fun(WrappedConn) end) end,
            fun() -> {error, connection_unavailable} end
        )
    end).

%% Runs `pog.transaction` under the deadline: `BEGIN`, the callback's own
%% statements, and `COMMIT`/`ROLLBACK` all run against the one connection
%% this module itself checked out, bounded by the same deadline throughout —
%% never re-checked-out mid-transaction, and never subject to pog's own
%% unconfigurable default.
transaction_safely(Connection, Callback) ->
    with_deadline(
        Connection,
        fun(WrappedConn, RawConn) ->
            guarded_transaction(RawConn, fun() -> pog:transaction(WrappedConn, Callback) end)
        end,
        fun() -> {error, {transaction_query_error, connection_unavailable}} end
    ).

transaction_measured(Connection, Callback) ->
    measured(fun() ->
        with_deadline_timing(
            Connection,
            fun(WrappedConn, RawConn) ->
                guarded_transaction(RawConn, fun() -> pog:transaction(WrappedConn, Callback) end)
            end,
            fun() -> {error, {transaction_query_error, connection_unavailable}} end
        )
    end).

%% Unlike `transaction_safely/2`, this does not disguise a checkout failure
%% (the pool could not hand out a connection at all, before `BEGIN` ever
%% runs — definitely not committed) as the same `transaction_query_error`
%% shape a genuinely uncertain mid-transaction connection loss produces
%% (checked out fine, then lost the connection during the callback or its
%% own `COMMIT`; might have committed). The outer `{error, nil}` means
%% "checkout itself failed"; `{ok, Result}` means the transaction actually
%% ran to completion (successfully, rolled back, or with its own
%% `transaction_query_error`), and `Result` is exactly what it returned.
%% `grind/internal/unique_admission`'s own `run` is the only caller that
%% needs this distinction today; other `transaction_safely/2` callers
%% (acknowledgement, audited resolution) conservatively still report their
%% own "unknown" outcome for a checkout failure too
%% (`docs/IMPLEMENTATION-SCOPE.md` backlog) — safe, only less precise.
transaction_or_checkout_failure(Connection, Callback) ->
    with_deadline(
        Connection,
        fun(WrappedConn, RawConn) ->
            {ok, guarded_transaction(RawConn, fun() -> pog:transaction(WrappedConn, Callback) end)}
        end,
        fun() -> {error, nil} end
    ).

%% `postgres.migrate`'s own transaction, under `Settings.migration_deadline_ms`
%% instead of the shared per-pool deadline — a schema migration's DDL step
%% can legitimately need longer than an ordinary job-lifecycle statement.
%% `DeadlineMs` is always the caller-supplied migration deadline, never the
%% deadline owner's shared value for the pool.
migration_transaction_safely(Connection, DeadlineMs, Callback) ->
    case Connection of
        {pool, PoolName} ->
            with_deadline_ms(
                PoolName,
                DeadlineMs,
                fun(WrappedConn, RawConn) ->
                    guarded_transaction(RawConn, fun() -> pog:transaction(WrappedConn, Callback) end)
                end,
                fun() -> {error, {transaction_query_error, connection_unavailable}} end
            );
        SingleConnection = {single_connection, Conn} ->
            guarded_transaction(Conn, fun() -> pog:transaction(SingleConnection, Callback) end)
    end.

stop_consumer_supervisor(Pid) ->
    case erlang:is_process_alive(Pid) of
        false -> consumer_stopped(Pid);
        true ->
            try gen_server:stop(Pid, normal, 6000) of
                _ -> consumer_stopped(Pid)
            catch
                exit:{noproc, _} -> consumer_stopped(Pid);
                exit:noproc -> consumer_stopped(Pid);
                exit:timeout -> {error, nil}
            end
    end.

%% Keep the ownership link until shutdown is confirmed: a timed-out stop
%% must still let owner death tear down its consumer. Once stopped, unlink
%% and consume only this supervisor's normal exit notification, which a
%% trapping caller would otherwise retain on every start/stop cycle.
consumer_stopped(Pid) ->
    unlink(Pid),
    receive {'EXIT', Pid, normal} -> ok after 0 -> ok end,
    {ok, nil}.

%% Reports whether this call itself stopped a still-live process
%% (`{ok, true}`) or found it already gone (`{ok, false}`). The deadline
%% owner child handles cleanup as part of the tree's shutdown. `exit:timeout`
%% (the 6000ms bound elapsed without `gen_server:stop` confirming shutdown)
%% is reported as `{error, nil}` rather than crashing the caller. A deadline
%% owner is never cleared independently of the pool it bounds.
stop_supervisor(Pid) ->
    unlink(Pid),
    case erlang:is_process_alive(Pid) of
        false -> {ok, false};
        true ->
            try gen_server:stop(Pid, shutdown, 6000) of
                _ -> {ok, true}
            catch
                exit:{noproc, _} -> {ok, false};
                exit:noproc -> {ok, false};
                exit:timeout -> {error, nil}
            end
    end.

%% Whether a pog `Connection` is one checked-out connection (the `tx` a
%% `pog.transaction` callback receives) rather than a pool.
is_single_connection({single_connection, _}) -> true;
is_single_connection(_) -> false.
