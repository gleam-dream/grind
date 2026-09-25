-module(grind_postgres_ffi).
-export([
    call_safely/1,
    execute_safely/2,
    stop_consumer_supervisor/1,
    stop_supervisor/1,
    transaction_safely/2,
    transaction_or_checkout_failure/2
]).

%% Generic form of execute_safely/2: runs any zero-arity fun and catches the
%% same pgo_pool checkout exit, reporting it as the same `connection_unavailable`
%% error `pog.QueryError` carries. Squirrel-generated query functions
%% (`grind/internal/sql`) call `pog:execute/2` directly rather than going
%% through `execute_safely/2`, so callers wrap those calls in `call_safely`
%% to keep Grind's no-crash-on-checkout-failure behavior without Grind having
%% to hand-write (and keep in sync with) a query-shaped wrapper per generated
%% function.
call_safely(Fun) ->
    try Fun()
    catch
        exit:{_Reason, {pgo_pool, checkout, _Details}} ->
            {error, connection_unavailable}
    end.

execute_safely(Query, Connection) ->
    try pog:execute(Query, Connection)
    catch
        exit:{_Reason, {pgo_pool, checkout, _Details}} ->
            {error, connection_unavailable}
    end.

transaction_safely(Connection, Callback) ->
    try pog:transaction(Connection, Callback)
    catch
        exit:{_Reason, {pgo_pool, checkout, _Details}} ->
            {error, {transaction_query_error, connection_unavailable}}
    end.

%% Unlike transaction_safely/2, this does not disguise a checkout failure
%% (the pool could not hand out a connection at all, before `BEGIN` ever
%% runs — definitely not committed) as the same `transaction_query_error`
%% shape a genuinely uncertain mid-transaction connection loss produces
%% (checked out fine, then lost the connection during the callback or its
%% own COMMIT — might have committed). The outer `{error, nil}` means
%% "checkout itself failed"; `{ok, Result}` means pog:transaction/2 actually
%% ran to completion (successfully, rolled back, or with its own
%% `transaction_query_error`), and `Result` is exactly what it returned.
%% `grind/internal/unique_admission`'s `run` is the only caller that needs
%% this distinction today; other `transaction_safely/2` callers
%% (acknowledgement, audited resolution) conservatively still report their
%% own "unknown" outcome for a checkout failure too (`docs/IMPLEMENTATION-SCOPE.md`
%% backlog) — safe, only less precise.
transaction_or_checkout_failure(Connection, Callback) ->
    try
        {ok, pog:transaction(Connection, Callback)}
    catch
        exit:{_Reason, {pgo_pool, checkout, _Details}} ->
            {error, nil}
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

stop_supervisor(Pid) ->
    unlink(Pid),
    case erlang:is_process_alive(Pid) of
        false -> nil;
        true ->
            try gen_server:stop(Pid, shutdown, 6000) of
                _ -> nil
            catch
                exit:{noproc, _} -> nil;
                exit:noproc -> nil
            end
    end.
