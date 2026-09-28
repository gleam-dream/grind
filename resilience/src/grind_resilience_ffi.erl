-module(grind_resilience_ffi).
-export([env/1, read_line/0, init/0, effect/1, finished/1, await_release/1,
         kill_worker/1, runtime/0, next_attempt/1, decision/2, effect_synced/1, trace/2]).

env(Key) -> unicode:characters_to_binary(os:getenv(binary_to_list(Key))).
read_line() ->
    case io:get_line("") of
        eof -> {error, nil};
        Line -> {ok, unicode:characters_to_binary(Line)}
    end.

init() ->
    ets:new(grind_resilience_workers, [named_table, public]),
    nil.

%% One append per effect, flushed to disk before reaching the release barrier.
%% Each independent VM has its own effect file. Killing a VM cannot erase an
%% effect already witnessed by the controller.
effect(Key) ->
    ets:insert(grind_resilience_workers, {Key, self()}),
    record(<<"effect">>, Key),
    ets:insert(grind_resilience_workers, {{synced, Key}, true}),
    nil.

effect_synced(Key) -> ets:member(grind_resilience_workers, {synced, Key}).

next_attempt(Key) ->
    ets:update_counter(grind_resilience_workers, {attempt, Key}, 1, {{attempt, Key}, 0}).

decision(Key, Kind) -> record(Kind, Key).

finished(Key) ->
    record(<<"handler_finished">>, Key),
    ets:delete(grind_resilience_workers, Key),
    ets:delete(grind_resilience_workers, {attempt, Key}),
    ets:delete(grind_resilience_workers, {synced, Key}),
    nil.

record(Kind, Key) ->
    Event = #{event => Kind, key => Key, node => env(<<"RESILIENCE_NODE">>),
              beam_node => atom_to_binary(node()),
              os_pid => list_to_integer(os:getpid()),
              worker_pid => list_to_binary(pid_to_list(self())),
              at_ms => erlang:system_time(millisecond)},
    {ok, File} = file:open(os:getenv("RESILIENCE_EFFECTS"), [append, raw, binary]),
    try
        ok = file:write(File, [json:encode(Event), <<"\n">>]),
        ok = file:sync(File)
    after file:close(File) end,
    nil.

await_release(<<>>) -> nil;
await_release(Path) ->
    case filelib:is_regular(binary_to_list(Path)) of
        true -> nil;
        false -> timer:sleep(10), await_release(Path)
    end.

kill_worker(Key) ->
    case ets:lookup(grind_resilience_workers, Key) of
        [{_, Pid}] -> exit(Pid, kill), true;
        [] -> false
    end.

runtime() ->
    Info = [process_info(Pid, message_queue_len) || Pid <- processes()],
    Queued = lists:sum([N || {message_queue_len, N} <- Info]),
    Deadlines = length([ok || {{grind_pg_deadline, _}, _} <- persistent_term:get()]),
    {trap_exit, TrapsExits} = process_info(self(), trap_exit),
    {messages, Messages} = process_info(self(), messages),
    {list_to_integer(os:getpid()), erlang:system_info(process_count),
     erlang:system_info(atom_count), erlang:memory(total), Queued, Deadlines,
     ets:info(pg_types_table, size), ets:info(pgo_query_cache, size),
     ets:info(grind_resilience_workers, size), TrapsExits,
     [iolist_to_binary(io_lib:format("~p", [Message])) || Message <- Messages]}.

%% Read-only fault diagnostics run in a separate process so blocked public stop
%% can be observed without modifying the consumer or draining any mailbox.
trace(Path, Duration) ->
    spawn(fun() -> trace_loop(Path, erlang:monotonic_time(millisecond) + Duration) end),
    nil.

trace_loop(Path, Until) ->
    Rows = [#{pid => format(Pid), info => format(process_info(Pid,
                [registered_name, status, current_function, current_stacktrace, message_queue_len]))}
            || Pid <- processes()],
    Event = #{at_ms => erlang:system_time(millisecond), processes => Rows},
    ok = file:write_file(Path, [json:encode(Event), <<"\n">>], [append]),
    case erlang:monotonic_time(millisecond) < Until of
        true -> timer:sleep(500), trace_loop(Path, Until);
        false -> ok
    end.

format(Value) -> iolist_to_binary(io_lib:format("~p", [Value])).
