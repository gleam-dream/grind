//// A 1-second BEAM introspection sampler: appends one JSON line per tick to
//// a caller-chosen path (raw evidence -- gitignored under
//// `bench/results/`, never committed; see `grind_bench/summarize` for
//// the generated percentile/CSV rollup this feeds).
////
//// Runs synchronously on whatever process calls `run` -- a load scenario
//// spawns it on its own unlinked process (`process.spawn_unlinked`) and
//// stops it with `process.kill` once the scenario's own run finishes
//// (a `gleam_erlang` `Subject` can only be received from by the process
//// that created it, so a "stop" message from the spawning process cannot
//// be received here -- see `docs/RECOVERY-EVIDENCE.md`-style note in
//// `grind_bench/load`'s own doc comment on this exact trap). There is no
//// supervision here: a sampler dying mid-run should not affect the load
//// run itself, and the run is always bounded (a fixed job count or
//// wall-clock budget), never a long-lived service. `max_ticks` is this
//// module's own hard backstop against a caller that forgets to kill the
//// sampler process at all.

import gleam/erlang/process
import gleam/json
import gleam/list
import gleam/string
import gleam/time/timestamp
import simplifile

fn now_unix_ms() -> Int {
  let #(seconds, nanoseconds) =
    timestamp.system_time() |> timestamp.to_unix_seconds_and_nanoseconds
  seconds * 1000 + nanoseconds / 1_000_000
}

@external(erlang, "grind_bench_sampler_ffi", "snapshot")
fn raw_snapshot() -> #(Int, Int, Int, Int, Int, Int, Int, Int, Int)

fn snapshot_line() -> String {
  let #(
    process_count,
    port_count,
    atom_count,
    run_queue,
    reductions,
    memory_total,
    memory_processes,
    memory_ets,
    memory_atom,
  ) = raw_snapshot()
  json.object([
    #("unix_ms", json.int(now_unix_ms())),
    #("process_count", json.int(process_count)),
    #("port_count", json.int(port_count)),
    #("atom_count", json.int(atom_count)),
    #("run_queue", json.int(run_queue)),
    #("reductions", json.int(reductions)),
    #("memory_total_bytes", json.int(memory_total)),
    #("memory_processes_bytes", json.int(memory_processes)),
    #("memory_ets_bytes", json.int(memory_ets)),
    #("memory_atom_bytes", json.int(memory_atom)),
  ])
  |> json.to_string
}

/// Appends one snapshot per `interval_ms` to `path` until `max_ticks`
/// elapse or this process is killed (`process.kill`) from outside, whichever
/// comes first.
pub fn run(path: String, interval_ms: Int, max_ticks: Int) -> Nil {
  let _ = simplifile.create_directory_all(parent_dir(path))
  let _ = simplifile.write(to: path, contents: "")
  loop(path, interval_ms, max_ticks)
}

fn loop(path: String, interval_ms: Int, ticks_remaining: Int) -> Nil {
  case ticks_remaining <= 0 {
    True -> Nil
    False -> {
      process.sleep(interval_ms)
      let _ = simplifile.append(to: path, contents: snapshot_line() <> "\n")
      loop(path, interval_ms, ticks_remaining - 1)
    }
  }
}

/// The directory portion of a `/`-joined path, `"."` if `path` has no `/` at
/// all. Plain `gleam/string`/`gleam/list` -- no FFI needed.
fn parent_dir(path: String) -> String {
  case string.split(path, "/") {
    [] | [_] -> "."
    parts ->
      case list.take(parts, list.length(parts) - 1) {
        [] -> "."
        directories -> string.join(directories, "/")
      }
  }
}
