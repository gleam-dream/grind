-module(grind_queue_ffi).
-export([monotonic_ms/0, monotonic_us/0, node_name/0]).
monotonic_ms() -> erlang:monotonic_time(millisecond).
monotonic_us() -> erlang:monotonic_time(microsecond).
node_name() -> atom_to_binary(node(), utf8).
