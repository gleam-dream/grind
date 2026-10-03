-module(grind_pool_ffi).
-behaviour(gen_server).

-export([start_deadline_owner/2, start_deadline_owner/3, managed_start/2, acquire/1, release/2,
         start_unlinked/1, abort_start/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(owner, {name, deadline, search_path = none, pool = undefined, pool_monitor = undefined, pool_stopped = false,
                subtree = unattached, calls = #{}}).

%% One stable registration per pool protects the live incarnation's deadline.
start_deadline_owner(PoolName, DeadlineMs) ->
    start_deadline_owner(PoolName, DeadlineMs, none).

%% `SearchPath` is `none` or `{some, Value}`: the `search_path` every managed
%% checkout from this pool runs under, restored before the connection
%% returns to the pool (`grind_postgres_ffi`, `scoped/3`).
start_deadline_owner(PoolName, DeadlineMs, SearchPath) ->
    OwnerName = list_to_atom(atom_to_list(PoolName) ++ "_deadline_owner"),
    gen_server:start_link({local, OwnerName}, ?MODULE, {PoolName, DeadlineMs, SearchPath}, []).

init({PoolName, DeadlineMs, SearchPath}) ->
    process_flag(trap_exit, true),
    persistent_term:put({grind_pg_deadline, PoolName}, {self(), DeadlineMs}),
    {ok, #owner{name = PoolName, deadline = DeadlineMs, search_path = SearchPath}}.

%% Register the exact pgo subtree before a managed pool start can succeed. The
%% pinned pgo_pool starts an internal supervisor, but its own death does not
%% wait for that supervisor's descendants to finish writing global caches.
managed_start(PoolName, Start) ->
    case Start() of
        {ok, {started, PoolPid, _}} = Started ->
            Children = owned_direct_children(PoolPid, PoolName),
            Trees = [Pid || Pid <- Children,
                     proc_lib:translate_initial_call(Pid) =:= {supervisor, pgo_pool_sup, 1}],
            case Trees of
                [Tree] ->
                    case owner_call(PoolName, {attach, PoolPid, Tree}) of
                        ok -> Started;
                        {error, Reason} ->
                            unwind_pool(PoolPid, Children),
                            start_error({pool_owner_attach_failed, Reason})
                    end;
                _ ->
                    unwind_pool(PoolPid, Children),
                    start_error({unsupported_pgo_pool_topology, Trees})
            end;
        Error -> Error
    end.

%% Registered pgo parents appear by NAME in '$ancestors', so that dictionary
%% cannot prove an incarnation. Inspect live links first; after a parent dies,
%% scan only pgo_pool_sup processes and match the exact PoolPid retained in the
%% pinned connection_sup childspec. Never adopt a same-name foreign pool.
owned_direct_children(Parent, PoolName) ->
    Candidates = case process_info(Parent, links) of
        {links, Links} -> Links;
        undefined -> [Pid || Pid <- processes(), possible_parent(Pid, Parent, PoolName)]
    end,
    [Pid || Pid <- Candidates, is_pid(Pid), owned_pgo_tree(Pid, Parent)].

%% This narrows the exceptional scan before any synchronous supervisor call;
%% ancestry by name is never accepted as ownership proof.
possible_parent(Pid, Parent, PoolName) ->
    case process_info(Pid, dictionary) of
        {dictionary, Dictionary} ->
            case proplists:get_value('$ancestors', Dictionary) of
                [Head | _] when Head =:= Parent; Head =:= PoolName -> true;
                _ -> false
            end;
        undefined -> false
    end.

owned_pgo_tree(Pid, Parent) ->
    case proc_lib:translate_initial_call(Pid) of
        {supervisor, pgo_pool_sup, 1} ->
            try supervisor:get_childspec(Pid, connection_sup) of
                {ok, #{start := {pgo_connection_sup, start_link, [_, Parent, _, _]}}} -> true;
                _ -> false
            catch exit:_ -> false end;
        _ -> false
    end.

unwind_pool(PoolPid, Children) ->
    stop_process(PoolPid),
    lists:foreach(fun stop_process/1, Children).

start_error(Reason) -> {error, {init_exited, {abnormal, Reason}}}.

owner_call(PoolName, Request) ->
    case persistent_term:get({grind_pg_deadline, PoolName}, undefined) of
        {Owner, _} ->
            try gen_server:call(Owner, Request, infinity)
            catch exit:_ -> {error, closed} end;
        _ -> {error, closed}
    end.

%% Infinite admission avoids a timed-out caller leaving a token that is granted
%% later. Admission and deadline selection are one owner-serialized operation.
acquire(PoolName) -> owner_call(PoolName, {acquire, self()}).

release(Owner, Token) ->
    gen_server:cast(Owner, {release, Token}),
    ok.

handle_call({attach, PoolPid, Tree}, _From,
            State = #owner{name = Name, subtree = unattached}) ->
    case whereis(Name) =:= PoolPid andalso is_process_alive(Tree) of
        true ->
            Ref = monitor(process, Tree),
            PoolRef = monitor(process, PoolPid),
            {reply, ok, State#owner{pool = PoolPid, pool_monitor = PoolRef, subtree = {Tree, Ref}}};
        false -> {reply, {error, closed}, State}
    end;
handle_call({acquire, Caller}, _From,
            State = #owner{name = Name, pool = Pool, subtree = {_, _},
                           deadline = Deadline, search_path = SearchPath,
                           calls = Calls}) ->
    case whereis(Name) =:= Pool andalso is_process_alive(Pool) of
        true ->
            Token = monitor(process, Caller),
            {reply, {ok, self(), Token, Deadline, SearchPath},
             State#owner{calls = Calls#{Token => Caller}}};
        false -> {reply, {error, closed}, State}
    end;
handle_call({acquire, _}, _From, State) -> {reply, {error, closed}, State};
handle_call(_Request, _From, State) -> {reply, {error, unsupported}, State}.

handle_cast({release, Token}, State) -> {noreply, release_token(Token, State)};
handle_cast(_Request, State) -> {noreply, State}.

handle_info({'DOWN', Ref, process, _, _}, State) -> {noreply, down(Ref, State)};
handle_info(_Message, State) -> {noreply, State}.

release_token(Token, State = #owner{calls = Calls}) ->
    case maps:is_key(Token, Calls) of
        true ->
            demonitor(Token, [flush]),
            State#owner{calls = maps:remove(Token, Calls)};
        false -> State
    end.

down(Ref, State = #owner{pool_monitor = Ref}) -> State#owner{pool_stopped = true};
down(Ref, State = #owner{subtree = {_, Ref}}) -> State#owner{subtree = stopped};
down(Ref, State = #owner{calls = Calls}) -> State#owner{calls = maps:remove(Ref, Calls)}.

terminate(_Reason, State = #owner{name = Name, pool = Pool}) ->
    %% Standalone cooperative owner stop must stop its live sibling itself; the
    %% parent cannot react to owner DOWN until terminate returns. Use a helper so
    %% the owner remains responsive to rejected admissions while a stop waits.
    case is_pid(Pool) andalso is_process_alive(Pool) of
        true -> spawn(fun() -> stop_process(Pool) end);
        false -> ok
    end,
    drain(State),
    %% The owner remains registered through termination; only its exact token may
    %% erase metadata. Raw pog callers and brutal owner kill are outside this
    %% cooperative lifetime barrier (managed callers are admitted above).
    case persistent_term:get({grind_pg_deadline, Name}, undefined) of
        {Owner, _} when Owner =:= self() ->
            case whereis(Name) of
                undefined ->
                    %% Pinned pg_types/pgo private table and key layouts.
                    catch ets:match_delete(pg_types_table, {{Name, '_'}, '_'}),
                    catch ets:match_delete(pgo_query_cache, {{Name, '_'}, '_'});
                _ -> ok
            end,
            persistent_term:erase({grind_pg_deadline, Name});
        _ -> ok
    end,
    ok.

drain(State = #owner{pool = Pool, pool_stopped = PoolStopped,
                     subtree = Tree, calls = Calls}) ->
    case (Pool =:= undefined orelse PoolStopped) andalso
         (Tree =:= unattached orelse Tree =:= stopped) andalso map_size(Calls) =:= 0 of
        true -> ok;
        false -> drain_wait(State)
    end.

drain_wait(State) ->
    receive
        {'$gen_call', From, _Request} ->
            gen_server:reply(From, {error, closed}),
            drain(State);
        {'$gen_cast', {release, Token}} -> drain(release_token(Token, State));
        {'DOWN', Ref, process, _, _} -> drain(down(Ref, State));
        _Other -> drain(State)
    end.

stop_process(Pid) ->
    Ref = monitor(process, Pid),
    try gen_server:stop(Pid, shutdown, infinity)
    catch exit:_ -> ok end,
    receive {'DOWN', Ref, process, Pid, _} -> ok end.

start_unlinked(Start) ->
    Caller = self(),
    {Starter, Monitor} = spawn_monitor(fun() ->
        process_flag(trap_exit, true),
        Result = Start(),
        case Result of
            {ok, {started, Pid, _}} -> unlink(Pid);
            {error, _} -> ok
        end,
        Caller ! {self(), Result}
    end),
    receive
        {Starter, Result} ->
            demonitor(Monitor, [flush]),
            Result;
        {'DOWN', Monitor, process, Starter, Reason} ->
            {error, {init_exited, {abnormal, Reason}}}
    end.

abort_start(Pid) ->
    unlink(Pid),
    stop_process(Pid),
    nil.
