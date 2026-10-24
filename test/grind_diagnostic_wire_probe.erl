-module(grind_diagnostic_wire_probe).
-export([attach/3, detach/1, emit/3, native_map/1, put/3, remove/2]).

attach(Id, Name, Callback) ->
    case telemetry:attach({?MODULE, Id, self()}, Name, fun handle/4, Callback) of
        ok -> {ok, nil};
        {error, _} -> {error, nil}
    end.

handle(_Name, Measurements, Metadata, Callback) ->
    Callback(Measurements, Metadata).

detach(Id) ->
    telemetry:detach({?MODULE, Id, self()}),
    nil.

emit(Name, Measurements, Metadata) ->
    telemetry:execute(Name, Measurements, Metadata),
    nil.

%% Only fixed test field names enter this probe; no new atoms are created.
native_map(Entries) ->
    maps:from_list([{binary_to_existing_atom(Key, utf8), Value} || {Key, Value} <- Entries]).

put(Map, Key, Value) ->
    maps:put(binary_to_existing_atom(Key, utf8), Value, Map).

remove(Map, Key) ->
    maps:remove(binary_to_existing_atom(Key, utf8), Map).
