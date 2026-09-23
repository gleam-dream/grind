-module(grind_postgres_ffi).
-export([stop_supervisor/1]).

stop_supervisor(Pid) ->
    unlink(Pid),
    case erlang:is_process_alive(Pid) of
        false -> nil;
        true ->
            try sys:terminate(Pid, shutdown) of
                _ -> nil
            catch
                exit:{noproc, _} -> nil;
                exit:noproc -> nil
            end
    end.
