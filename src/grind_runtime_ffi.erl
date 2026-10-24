-module(grind_runtime_ffi).
-export([parent_pid/0, put_pool/2, get_pool/1, exit_shutdown/0, no_pool/0]).

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
