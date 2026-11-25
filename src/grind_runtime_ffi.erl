-module(grind_runtime_ffi).
-export([parent_pid/0, put_pool/2, get_pool/1, exit_shutdown/0, no_pool/0,
         record_incarnation/2, await_previous_incarnation/2]).

%% The calling process's supervisor: the first entry of `$ancestors`, which
%% proc_lib sets for every process a supervisor starts.
parent_pid() ->
    case get('$ancestors') of
        [Parent | _] when is_pid(Parent) -> Parent;
        [Name | _] when is_atom(Name) -> whereis(Name)
    end.

%% The pool name a Grind runtime name was configured with, recorded once per
%% `grind.start`/`grind.supervised` call so `grind.connection` resolves it
%% from the runtime name alone.
put_pool(Name, Pool) ->
    persistent_term:put({grind_runtime_pool, Name}, Pool),
    nil.

get_pool(Name) ->
    try persistent_term:get({grind_runtime_pool, Name}) of
        Pool -> {ok, Pool}
    catch
        error:badarg -> {error, nil}
    end.

exit_shutdown() ->
    erlang:exit(shutdown).

%% The pool name of a synthetic handler context (`grind/testing`): a fixed
%% name no pool is ever started under, so a query on it fails at once.
no_pool() ->
    grind_testing_no_pool.

%% The root supervisor of the runtime last started under a Grind runtime name.
%% A root that is killed or crashes dies at once, but its descendants stop on
%% their own schedule: a consumer drains for its grace and the pool owner
%% waits for its callers, each still holding its registered name. A new
%% incarnation therefore starts only after every process of the previous one
%% has exited, so it never finds one of its names taken by a remnant.
record_incarnation(Name, Root) ->
    persistent_term:put({grind_incarnation, Name}, Root),
    nil.

%% `{ok, nil}` once no process of the previous incarnation is alive;
%% `{error, still_running}` while its root is alive (a live runtime owns the
%% name); `{error, did_not_stop}` when its processes outlive `WithinMs`.
await_previous_incarnation(Name, WithinMs) ->
    case persistent_term:get({grind_incarnation, Name}, undefined) of
        undefined -> {ok, nil};
        Root ->
            case is_process_alive(Root) of
                true -> {error, still_running};
                false ->
                    Remnants = [Pid || Pid <- processes(), Pid =/= self(),
                                       descends_from(Pid, Root)],
                    Deadline = erlang:monotonic_time(millisecond) + WithinMs,
                    await_exits([monitor(process, Pid) || Pid <- Remnants], Deadline)
            end
    end.

%% proc_lib records each process's supervisors, nearest first, in
%% `$ancestors`; a registered parent appears by name, an unnamed one (every
%% Grind supervisor) by pid.
descends_from(Pid, Root) ->
    case process_info(Pid, dictionary) of
        {dictionary, Dictionary} ->
            case lists:keyfind('$ancestors', 1, Dictionary) of
                {_, Ancestors} -> lists:member(Root, Ancestors);
                false -> false
            end;
        undefined -> false
    end.

await_exits([], _Deadline) -> {ok, nil};
await_exits([Ref | Rest] = Refs, Deadline) ->
    Remaining = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {'DOWN', Ref, process, _, _} -> await_exits(Rest, Deadline)
    after Remaining ->
        [demonitor(Other, [flush]) || Other <- Refs],
        {error, did_not_stop}
    end.
