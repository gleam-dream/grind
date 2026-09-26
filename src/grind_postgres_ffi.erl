-module(grind_postgres_ffi).
-export([
    call_safely/2,
    execute_safely/2,
    stop_consumer_supervisor/1,
    stop_supervisor/1,
    transaction_safely/2,
    transaction_or_checkout_failure/2,
    migration_transaction_safely/3,
    set_deadline/2,
    clear_deadline/1
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
%% bounded `pgo:checkout/2` up front — passing an explicit `timeout` option
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
%% and to `pgo`'s own checkout/checkin/break API — accepted deliberately
%% (user decision: no fork), guarded by pinning both dependencies to a tight
%% version range in gleam.toml and by `pog_connection_pool_shape_test`
%% (test/grind_test.gleam), which fails loudly the moment a pog/pgo upgrade
%% changes either shape instead of this module silently mismatching it. The
%% deadline is attached to a pool by its atom name (an
%% `erlang:process.Name` — itself just an atom; `gleam_erlang_ffi:new_name/1`)
%% via `persistent_term`, set once in `postgres.start` and cleared in
%% `postgres.close`, rather than threaded through every one of Grind's
%% storage call sites.
-define(DEFAULT_DEADLINE_MS, 5000).

%% Both take the pog `Connection` a freshly started `Database` always holds
%% (always the `Pool` shape at this point, never a `SingleConnection`) rather
%% than a bare pool name, so `postgres.start`/`postgres.close` can pass the
%% same connection value they already have in scope.
set_deadline({pool, PoolName}, DeadlineMs) ->
    persistent_term:put({grind_pg_deadline, PoolName}, DeadlineMs),
    nil.

clear_deadline({pool, PoolName}) ->
    catch persistent_term:erase({grind_pg_deadline, PoolName}),
    nil.

deadline_for(PoolName) ->
    persistent_term:get({grind_pg_deadline, PoolName}, ?DEFAULT_DEADLINE_MS).

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
with_deadline({pool, PoolName}, Fun, OnCheckoutFailure) ->
    with_deadline_ms(PoolName, deadline_for(PoolName), Fun, OnCheckoutFailure);
with_deadline(SingleConnection = {single_connection, Conn}, Fun, _OnCheckoutFailure) ->
    Fun(SingleConnection, Conn).

with_deadline_ms(PoolName, DeadlineMs, Fun, OnCheckoutFailure) ->
    try pgo:checkout(PoolName, [{timeout, DeadlineMs}]) of
        {ok, Ref, Conn} ->
            try
                Fun({single_connection, Conn}, Conn)
            after
                catch pgo:checkin(Ref, Conn)
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
            OnCheckoutFailure()
    catch
        exit:{_Reason, {pgo_pool, checkout, _Details}} ->
            OnCheckoutFailure()
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
%% one `set_deadline/2` attached to the pool.
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
        false -> {ok, nil};
        true ->
            try gen_server:stop(Pid, normal, 6000) of
                _ -> {ok, nil}
            catch
                exit:{noproc, _} -> {ok, nil};
                exit:noproc -> {ok, nil};
                exit:timeout -> {error, nil}
            end
    end.

%% Reports whether this call itself stopped a still-live process
%% (`{ok, true}`) or found it already gone (`{ok, false}`) — `postgres.close`
%% uses this to decide whether erasing this pool name's `persistent_term`
%% deadline entry is actually safe (see `set_deadline`/`clear_deadline`
%% above and `postgres.close`'s own doc comment): a stale `Database` handle
%% whose supervisor already stopped must not erase a *different*, currently
%% live pool that has since reused the same registered name. `exit:timeout`
%% (the 6000ms bound elapsed without `gen_server:stop` confirming shutdown)
%% is reported as `{error, nil}` rather than crashing the caller and leaving
%% the name's own deadline entry cleared out from under a pool that may
%% still be alive.
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
