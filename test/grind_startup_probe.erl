-module(grind_startup_probe).
-export([resources/0, pool_cache_counts/1, late_type_writer_drain/2, late_query_writer_drain/3]).

pool_cache_counts({pool, Name}) ->
    {length(ets:match_object(pg_types_table, {{Name, '_'}, '_'})),
     length(ets:match_object(pgo_query_cache, {{Name, '_'}, '_'}))}.

%% Lifecycle acceptance probes: named pools/forwarders and persistent checkout
%% bounds are resources for which an unsuccessful start returns no close handle.
resources() ->
    Names = lists:sort([Name || Name <- registered(),
        lists:prefix("grind_postgres_", atom_to_list(Name))]),
    Deadlines = lists:sort([{Name, Value} ||
        {{grind_pg_deadline, Name}, Value} <- persistent_term:get()]),
    {Names, Deadlines, cache_keys(pg_types_table), cache_keys(pgo_query_cache)}.

cache_keys(Table) ->
    case ets:info(Table) of
        undefined -> [];
        _ -> lists:sort([Key || {Key = {Name, _}, _} <- ets:tab2list(Table),
                         lists:prefix("grind_postgres_", atom_to_list(Name))
                         orelse lists:prefix("grind_queue_renewals", atom_to_list(Name))])
    end.

%% These probes stop real pgo cache writers at the two independently reproduced
%% teardown races. No pool cache is erased by the probes themselves.
late_type_writer_drain({pool, Name}, Close) ->
    {Pool, Tree, Type, _} = owned_tree(Name),
    true = erlang:suspend_process(Tree),
    Parent = self(),
    Closer = spawn(fun() -> Parent ! {closed, self(), Close()} end),
    try
        wait_dead(Pool),
        assert_close_pending(Closer),
        {error, closed} = grind_pool_ffi:acquire(Name),
        {Owner, _} = persistent_term:get({grind_pg_deadline, Name}),
        true = is_process_alive(Owner),
        true = is_process_alive(Type),
        ok = pgo_type_server:reload(Type),
        {Types, _} = pool_cache_counts({pool, Name}),
        true = Types > 0,
        assert_close_pending(Closer),
        true = erlang:resume_process(Tree),
        await_closed(Closer),
        wait_dead(Tree), wait_dead(Type),
        assert_clean(Name),
        true
    after
        catch erlang:resume_process(Tree)
    end.

late_query_writer_drain({pool, Name}, Close, KillCaller) ->
    {Pool, Tree, _, Children} = owned_tree(Name),
    DebuggerBefore = whereis(dbg_iserver),
    {module, pgo_query_cache} = int:i(pgo_query_cache),
    %% Pinned pgo_query_cache:insert/3 enters ets:insert on this source line.
    ok = int:break(pgo_query_cache, 28),
    Parent = self(),
    Caller = spawn(fun() ->
        Result = grind_postgres_ffi:execute_safely(
            pog:'query'(<<"SELECT 99173 AS owned_late_query_cache_probe">>), {pool, Name}),
        Parent ! {query_result, self(), Result}
    end),
    try
        wait_break(Caller, 200),
        Closer = spawn(fun() -> Parent ! {closed, self(), Close()} end),
        lists:foreach(fun wait_dead/1, [Pool, Tree | Children]),
        assert_close_pending(Closer),
        {error, closed} = grind_pool_ffi:acquire(Name),
        {Owner, _} = persistent_term:get({grind_pg_deadline, Name}),
        true = is_process_alive(Owner),
        case KillCaller of
            true -> exit(Caller, kill), wait_dead(Caller);
            false ->
                %% Public close remains bounded, while the owner must outlive
                %% the old default five-second child shutdown timeout.
                receive {closed, Closer, {error, stop_timed_out}} -> ok;
                        {closed, Closer, Unexpected} -> error({expected_close_timeout, Unexpected})
                after 7000 -> error(public_close_did_not_time_out) end,
                true = is_process_alive(Owner),
                {Owner, _} = persistent_term:get({grind_pg_deadline, Name}),
                {error, closed} = grind_pool_ffi:acquire(Name),
                ok = int:continue(Caller),
                receive {query_result, Caller, _Result} -> ok
                after 3000 -> error(query_result_timeout) end
        end,
        case KillCaller of
            true -> await_closed(Closer);
            false -> {ok, nil} = Close()
        end,
        assert_clean(Name),
        true
    after
        catch exit(Caller, kill),
        int:no_break(pgo_query_cache),
        int:n(pgo_query_cache),
        false = lists:member(pgo_query_cache, int:interpreted()),
        case DebuggerBefore of
            undefined -> gen_server:stop(dbg_iserver, normal, infinity);
            _ -> ok
        end
    end.

owned_tree(Name) ->
    Pool = whereis(Name),
    {links, Links} = process_info(Pool, links),
    [Tree] = [Pid || Pid <- Links, is_pid(Pid),
                    proc_lib:translate_initial_call(Pid) =:= {supervisor, pgo_pool_sup, 1}],
    {_, Type, _, _} = lists:keyfind(type_server, 1, supervisor:which_children(Tree)),
    {Pool, Tree, Type, subtree(Tree)}.

subtree(Sup) ->
    lists:flatmap(fun({_, Pid, Type, _}) when is_pid(Pid) ->
        case Type of supervisor -> [Pid | subtree(Pid)]; worker -> [Pid] end;
        (_) -> []
    end, supervisor:which_children(Sup)).

assert_close_pending(Closer) ->
    receive {closed, Closer, Result} -> error({close_returned_before_writer_drain, Result})
    after 50 -> ok end.

await_closed(Closer) ->
    receive {closed, Closer, {ok, nil}} -> ok;
            {closed, Closer, Error} -> error({close_failed, Error})
    after 3000 -> error(close_did_not_finish_after_drain) end.

assert_clean(Name) ->
    {0, 0} = pool_cache_counts({pool, Name}),
    undefined = whereis(Name),
    undefined = persistent_term:get({grind_pg_deadline, Name}, undefined),
    ok.

wait_dead(Pid) ->
    Ref = monitor(process, Pid),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 3000 -> error({still_alive, Pid}) end.

wait_break(_, 0) -> error({no_debug_break, int:snapshot()});
wait_break(Pid, N) ->
    case [S || S={P, _, break, _} <- int:snapshot(), P =:= Pid] of
        [_] -> ok;
        [] -> timer:sleep(10), wait_break(Pid, N-1)
    end.
