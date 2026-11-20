-module(grind_usage_copy_size).
-export([flat_bytes/1]).

%% The bytes a term occupies when it is copied to another process. A test
%% can bound a worker value with it, because Grind copies the worker's
%% handler closure into each job's processes.
flat_bytes(Term) ->
    erts_debug:flat_size(Term) * erlang:system_info(wordsize).
