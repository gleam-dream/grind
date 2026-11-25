-module(grind_restart_probe).
-export([only_child/1, kill/1]).

%% The pid of the one child of a supervisor, or an error while it restarts.
only_child(Supervisor) ->
    case supervisor:which_children(Supervisor) of
        [{_, Pid, _, _}] when is_pid(Pid) -> {ok, Pid};
        _ -> {error, nil}
    end.

kill(Pid) ->
    exit(Pid, kill),
    nil.
