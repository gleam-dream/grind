-module(grind_copy_size).
-export([flat_bytes/1]).

%% The bytes a term occupies when it is copied to another process: a message,
%% a spawn closure or a start argument. Module constants count here although
%% the runtime does not copy them, so a value should refer to large constants
%% from inside its functions instead of capturing them.
flat_bytes(Term) ->
    erts_debug:flat_size(Term) * erlang:system_info(wordsize).
