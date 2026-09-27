%% Raw BEAM introspection with no Gleam-side equivalent, for
%% `grind_bench/sampler_beam`'s 1s JSONL snapshots.
-module(grind_bench_sampler_ffi).
-export([snapshot/0, message_queue_len/1, current_function_and_reductions/1]).

%% A fixed-shape tuple (never a map -- Gleam decodes an Erlang tuple as a
%% `#(...)` directly, with no decoder needed) of:
%% {process_count, port_count, atom_count, run_queue, reductions,
%%  memory_total, memory_processes, memory_ets, memory_atom}
snapshot() ->
    Mem = erlang:memory(),
    Total = proplists:get_value(total, Mem, 0),
    Processes = proplists:get_value(processes, Mem, 0),
    Ets = proplists:get_value(ets, Mem, 0),
    AtomMem = proplists:get_value(atom, Mem, 0),
    RunQueue = erlang:statistics(run_queue),
    %% erlang:statistics(reductions) returns {TotalReductions,
    %% ReductionsSinceLastCall} -- a plain pair, not a {reductions, N}
    %% tagged tuple (found via a real crash report the first time this ran:
    %% "no match of right hand side value {N, N}"). This module wants the
    %% running total, so it takes the first element.
    {Reductions, _ReductionsSinceLastCall} = erlang:statistics(reductions),
    ProcCount = erlang:system_info(process_count),
    PortCount = erlang:system_info(port_count),
    AtomCount = erlang:system_info(atom_count),
    {ProcCount, PortCount, AtomCount, RunQueue, Reductions, Total, Processes,
     Ets, AtomMem}.

%% -1 if the pid is already dead by the time this reads it -- a benign race
%% under a coordinator that has just finished shutting down, not an error the
%% sampler should crash over.
message_queue_len(Pid) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, N} -> N;
        undefined -> -1
    end.

%% Item 11: a statistical coordinator profiler samples this at high
%% frequency instead of running a full `eprof`/`fprof` session (both need
%% the `tools` application, which is not guaranteed to be on the code path
%% of every OTP install this runs under) -- `{module, function, arity}` as
%% strings/ints Gleam can decode plainly, plus the process's own *total*
%% reduction count so a caller can derive a reduction-rate delta across two
%% samples. `error` if the pid is already dead by the time this reads it.
current_function_and_reductions(Pid) ->
    case erlang:process_info(Pid, [current_function, reductions]) of
        undefined ->
            {error, nil};
        [{current_function, {Mod, Fun, Arity}}, {reductions, Reductions}] ->
            {ok, {atom_to_binary(Mod, utf8), atom_to_binary(Fun, utf8), Arity, Reductions}}
    end.
