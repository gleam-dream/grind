%% Small, bench-owned Erlang helpers with no Gleam-side equivalent: this
%% node's own name (for `bench_effects.node`) and the plain arguments passed
%% after `--` to `gleam run -m grind_bench/load -- <scenario> <args>` (no
%% `argv`-shaped Hex dependency needed for a harness this small).
-module(grind_bench_ffi).
-export([node_name/0, plain_arguments/0, getenv/1, halt/1, monotonic_ms/0,
         fresh_run_id/0, cpu_times_ms/1, wall_clock_unix_ms/0]).

node_name() ->
    atom_to_binary(erlang:node(), utf8).

%% A schema-name-safe, per-process-unique suffix (item 1: fresh Grind schema
%% per bench run, `bench_jobs_<run_id>`) -- combines wall-clock microseconds
%% with `erlang:unique_integer/1` so two runs started in the same
%% microsecond (unlikely, but `gleam run` is fast to start) still never
%% collide. Digits only, so `"bench_jobs_" <> fresh_run_id()` is always a
%% valid unquoted-looking identifier well under the 63-byte limit.
fresh_run_id() ->
    Micros = erlang:system_time(microsecond),
    Unique = erlang:unique_integer([positive]),
    iolist_to_binary(io_lib:format("~p_~p", [Micros, Unique])).

%% Item 9: postmaster + every child backend's own cumulative CPU time, in
%% milliseconds, read via `ps -o time= -p <pid>` for the postmaster and every
%% pid under it (`ps --ppid`/pgrep is not portable to macOS, so this walks
%% `ps -eo pid,ppid` once and follows the tree in Erlang instead). Returns
%% `{ok, Ms}` or `{error, nil}` if the postmaster pid file cannot be read or
%% `ps` itself fails -- callers treat this as best-effort evidence, never a
%% hard gate.
cpu_times_ms(PgDataDir) ->
    case file:read_file(filename:join(PgDataDir, "postmaster.pid")) of
        {error, _} ->
            {error, nil};
        {ok, Contents} ->
            [FirstLine | _] = binary:split(Contents, <<"\n">>),
            case catch binary_to_integer(FirstLine) of
                {'EXIT', _} ->
                    {error, nil};
                Postmaster ->
                    case ps_tree_seconds() of
                        {error, nil} -> {error, nil};
                        {ok, Tree} ->
                            Pids = descendants(Postmaster, Tree, [Postmaster]),
                            Total = lists:sum([cpu_seconds_of(P, Tree) || P <- Pids]),
                            {ok, round(Total * 1000)}
                    end
            end
    end.

%% `[{Pid, PPid, CpuSeconds}]` for every process `ps` reports right now.
ps_tree_seconds() ->
    case os:cmd("ps -axo pid,ppid,time") of
        [] ->
            {error, nil};
        Output ->
            Lines = tl(string:split(Output, "\n", all)),
            {ok, lists:filtermap(fun parse_ps_line/1, Lines)}
    end.

parse_ps_line(Line) ->
    case string:tokens(Line, " \t") of
        [PidStr, PPidStr, TimeStr] ->
            case {catch list_to_integer(PidStr), catch list_to_integer(PPidStr)} of
                {Pid, PPid} when is_integer(Pid), is_integer(PPid) ->
                    {true, {Pid, PPid, parse_ps_time(TimeStr)}};
                _ ->
                    false
            end;
        _ ->
            false
    end.

%% `ps time` is `[[dd-]hh:]mm:ss[.ff]` -- macOS's own `ps` reports
%% `MM:SS.ff` (hundredths of a second after a literal `.`, e.g.
%% `"84:04.37"`), which this crashed on the first time it ran for real
%% (`list_to_integer("04.37")` raises `badarg`, and multiplying the
%% resulting `{'EXIT', _}` tuple by an integer is `badarith`). Only the
%% final (seconds) segment can carry a fractional part; every other segment
%% (minutes, hours, an optional leading `D-` days prefix) is a plain
%% integer.
parse_ps_time(TimeStr) ->
    {DaysStr, Rest} = case string:split(TimeStr, "-") of
        [D, R] -> {D, R};
        [R] -> {"0", R}
    end,
    Segments = string:split(Rest, ":", all),
    SecondsPart = lists:last(Segments),
    OtherSegments = lists:droplast(Segments),
    OtherVals = [safe_int(S) || S <- OtherSegments],
    {Hours, Minutes} = case OtherVals of
        [] -> {0, 0};
        [Mm] -> {0, Mm};
        [Hh, Mm | _] -> {Hh, Mm}
    end,
    safe_int(DaysStr) * 86400.0
        + Hours * 3600.0
        + Minutes * 60.0
        + safe_seconds(SecondsPart).

safe_int(S) ->
    case catch list_to_integer(S) of
        {'EXIT', _} -> 0;
        N -> N
    end.

%% The seconds segment alone may be `"SS.ff"` (a float) or plain `"SS"` (an
%% integer) -- tries both rather than assuming either.
safe_seconds(S) ->
    case catch list_to_float(S) of
        {'EXIT', _} -> safe_int(S) * 1.0;
        F -> F
    end.

descendants(Pid, Tree, Acc) ->
    Children = [P || {P, PPid, _} <- Tree, PPid =:= Pid, not lists:member(P, Acc)],
    lists:foldl(fun(Child, Acc0) -> descendants(Child, Tree, [Child | Acc0]) end, Acc, Children).

cpu_seconds_of(Pid, Tree) ->
    case lists:keyfind(Pid, 1, Tree) of
        {Pid, _, Seconds} -> Seconds;
        false -> 0.0
    end.

plain_arguments() ->
    [unicode:characters_to_binary(A) || A <- init:get_plain_arguments()].

getenv(Name) ->
    case os:getenv(unicode:characters_to_list(Name)) of
        false -> {error, nil};
        Value -> {ok, unicode:characters_to_binary(Value)}
    end.

%% Sets the harness's own exit code -- `scripts/bench-postgres.sh`'s smoke
%% step relies on this to fail the gate when the audit itself fails, since a
%% Gleam program run via `gleam run` otherwise always exits 0.
halt(Code) ->
    erlang:halt(Code).

monotonic_ms() ->
    erlang:monotonic_time(millisecond).

%% Item 13 provenance: a wall-clock (not monotonic) unix-ms timestamp for
%% each CSV row.
wall_clock_unix_ms() ->
    os:system_time(millisecond).
