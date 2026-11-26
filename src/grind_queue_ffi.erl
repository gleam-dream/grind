-module(grind_queue_ffi).
-export([monotonic_ms/0, monotonic_us/0, node_name/0, describe_exit/1]).
monotonic_ms() -> erlang:monotonic_time(millisecond).
monotonic_us() -> erlang:monotonic_time(microsecond).
node_name() -> atom_to_binary(node(), utf8).

%% A bounded description of a handler process's exit reason that is safe to
%% store on its job: the kind of failure and where it happened, never a
%% value the failure carried. A Gleam panic, `let assert`, `assert` or `todo`
%% gives its kind, module, function, line and message. Any other reason
%% gives only its atom, or the tag atom of its tuple (`badmatch`, not the
%% value that did not match).
describe_exit({Reason, Stack}) when is_list(Stack) ->
    describe_exit(Reason);
describe_exit(#{gleam_error := Kind, module := Module, function := Function,
                line := Line, message := Message})
  when is_atom(Kind), is_binary(Module), is_binary(Function),
       is_integer(Line), is_binary(Message) ->
    bounded(<<(atom_to_binary(Kind, utf8))/binary, " in ", Module/binary, ".",
              Function/binary, ", line ", (integer_to_binary(Line))/binary,
              ": ", Message/binary>>);
describe_exit(Reason) when is_atom(Reason) ->
    bounded(atom_to_binary(Reason, utf8));
describe_exit(Reason) when is_tuple(Reason), tuple_size(Reason) > 0,
                           is_atom(element(1, Reason)) ->
    bounded(atom_to_binary(element(1, Reason), utf8));
describe_exit(_) ->
    <<"abnormal exit">>.

%% At most 500 characters and 2000 bytes of valid UTF-8, without a NUL,
%% which PostgreSQL text cannot hold. The byte limit matters because one
%% character (grapheme) can carry any number of combining code points.
bounded(Text) ->
    case unicode:characters_to_binary(Text, utf8, utf8) of
        Valid when is_binary(Valid) ->
            Clean = binary:replace(Valid, <<0>>, <<>>, [global]),
            Sliced = unicode:characters_to_binary(string:slice(Clean, 0, 500)),
            within_bytes(Sliced, 2000, <<>>);
        _ ->
            <<"abnormal exit">>
    end.

%% The longest prefix of whole code points that fits in `Budget` bytes.
within_bytes(<<>>, _Budget, Kept) ->
    Kept;
within_bytes(<<Point/utf8, Rest/binary>>, Budget, Kept) ->
    Size = byte_size(<<Point/utf8>>),
    case Size =< Budget of
        true -> within_bytes(Rest, Budget - Size, <<Kept/binary, Point/utf8>>);
        false -> Kept
    end.
