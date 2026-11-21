-module(grind_copy_size).
-export([flat_bytes/1, heap_bytes_after_gc/1, received_bytes/1]).

%% The bytes a term occupies when it is copied to another process: a message,
%% a spawn closure or a start argument. Module constants count here although
%% the runtime does not copy them, so a value should refer to large constants
%% from inside its functions instead of capturing them.
flat_bytes(Term) ->
    erts_debug:flat_size(Term) * erlang:system_info(wordsize).

%% The bytes a process's heap holds after a full collection: what it retains,
%% not what it has allocated since its last collection.
heap_bytes_after_gc(Pid) ->
    true = erlang:garbage_collect(Pid),
    {total_heap_size, Words} = erlang:process_info(Pid, total_heap_size),
    Words * erlang:system_info(wordsize).

%% Runs `Fun` in a fresh process and returns its result with the flat bytes
%% of every message that process received while it ran: what a call copies
%% into its caller. The process is traced from before it starts the call
%% until it finishes, and the trace is drained before the sum is taken.
received_bytes(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() ->
        receive {Ref, go} -> ok end,
        Result = Fun(),
        Parent ! {Ref, done, Result}
    end),
    1 = erlang:trace(Pid, true, ['receive']),
    Pid ! {Ref, go},
    Result = receive {Ref, done, R} -> R end,
    Delivered = erlang:trace_delivered(Pid),
    receive {trace_delivered, Pid, Delivered} -> ok end,
    {Result, collect(Pid, Ref, 0)}.

collect(Pid, Ref, Total) ->
    receive
        {trace, Pid, 'receive', {Ref, go}} -> collect(Pid, Ref, Total);
        {trace, Pid, 'receive', Message} ->
            collect(Pid, Ref, Total + flat_bytes(Message))
    after 0 -> Total
    end.
