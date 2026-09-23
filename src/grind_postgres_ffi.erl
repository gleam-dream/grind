-module(grind_postgres_ffi).
-export([
    execute_safely/2,
    stop_consumer_supervisor/1,
    stop_supervisor/1,
    transaction_safely/2
]).

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
