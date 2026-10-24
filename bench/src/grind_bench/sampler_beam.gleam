//// Appends BEAM snapshots at the requested interval to a caller-chosen path.
//// Runs synchronously in its caller; load scenarios use an unlinked process
//// and kill it when the run ends. A Subject belongs to the process that
//// creates it, so a parent-created Subject cannot receive a stop here.
//// Sampler failure does not stop the workload; required sample coverage is
//// checked separately. max_ticks bounds a sampler the caller forgets to kill.

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
