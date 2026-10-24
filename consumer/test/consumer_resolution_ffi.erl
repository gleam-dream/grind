-module(consumer_resolution_ffi).
-export([kill_wait/1]).
kill_wait(Pid) ->
    Ref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> nil
    after 5000 -> erlang:error(process_did_not_stop)
    end.
