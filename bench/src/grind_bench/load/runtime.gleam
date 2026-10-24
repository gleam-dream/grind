import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import grind/internal/worker
import grind_bench/worker as bench_worker

pub type BenchWorker =
  worker.Worker(bench_worker.BenchJob, Int, Nil)

@external(erlang, "grind_bench_ffi", "plain_arguments")
pub fn plain_arguments() -> List(String)

@external(erlang, "grind_bench_ffi", "getenv")
pub fn getenv(name: String) -> Result(String, Nil)

@external(erlang, "grind_bench_ffi", "halt")
pub fn halt(code: Int) -> Nil

@external(erlang, "grind_bench_ffi", "monotonic_ms")
pub fn monotonic_ms() -> Int

@external(erlang, "grind_bench_ffi", "wall_clock_unix_ms")
fn wall_clock_unix_ms() -> Int

@external(erlang, "grind_bench_ffi", "cpu_times_ms")
pub fn cpu_times_ms_ffi(pg_data_dir: String) -> Result(Int, Nil)

@external(erlang, "grind_bench_counter_ffi", "value")
pub fn counter_value(key: Int) -> Int

@external(erlang, "grind_bench_counter_ffi", "reset_all")
pub fn reset_all_counters() -> Nil

@external(erlang, "grind_bench_counter_ffi", "next")
pub fn bump(key: Int) -> Int

@external(erlang, "grind_bench_sampler_ffi", "message_queue_len")
pub fn message_queue_len(pid: process.Pid) -> Int

@external(erlang, "grind_bench_sampler_ffi", "current_function_and_reductions")
pub fn current_function_and_reductions(
  pid: process.Pid,
) -> Result(#(String, String, Int, Int), Nil)

/// Ledger `bench_index`es -1/-2/-3 are never real bench jobs (every real
/// index is `>= 0`) -- reused as bench-run-scoped counters via the same ETS
/// table `grind_bench/worker`'s own delivery counters already live in.
/// Mutation tests exercise these through the real
/// `attach_audit_observers` wiring from `bench/test/`, a different module in
/// this same package.
pub const ledger_error_counter = -1

pub const quarantine_counter = -2

pub const forwarder_drop_counter = -3

/// L4's own `SubmissionId`-uniqueness counter, same reused-ETS-table
/// scheme as the three above.
pub const l4_submission_counter = -4

/// Any completion-observer query failure invalidates the run.
pub const completion_observer_error_counter = -5

pub fn results_dir() -> String {
  result.unwrap(getenv("GRIND_BENCH_RESULTS_DIR"), "results/adhoc")
}

/// Only L7 and diagnostic profiles use this budget. It is a drain wait bound,
/// not the throughput denominator or the statement/lease timeout.
pub fn drain_timeout_ms() -> Int {
  case parse_drain_timeout_ms(getenv("GRIND_BENCH_DRAIN_TIMEOUT_MS")) {
    Ok(value) -> value
    Error(Nil) ->
      panic as "GRIND_BENCH_DRAIN_TIMEOUT_MS must be a positive decimal integer"
  }
}

/// Unset means the historical 60-second budget; explicit empty/invalid input
/// must fail rather than quietly selecting a different experiment.
pub fn parse_drain_timeout_ms(raw: Result(String, Nil)) -> Result(Int, Nil) {
  case raw {
    Error(Nil) -> Ok(60_000)
    Ok(text) -> {
      let digits = string.to_graphemes(text)
      let decimal =
        text != ""
        && list.all(digits, fn(char) {
          list.contains(
            ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
            char,
          )
        })
      case decimal, int.parse(text) {
        True, Ok(value) if value > 0 -> Ok(value)
        _, _ -> Error(Nil)
      }
    }
  }
}

pub fn database_url() -> String {
  case getenv("GRIND_BENCH_DATABASE_URL") {
    Ok(url) -> url
    Error(Nil) ->
      panic as "GRIND_BENCH_DATABASE_URL must be set to run grind_bench/load"
  }
}

/// The harness's ledger, drain-poll and sampler role. Defaults to
/// `database_url()` (same role as Grind) when the disposable cluster was
/// started without a separate `grind_ctl` role.
pub fn ctl_database_url() -> String {
  result.unwrap(getenv("GRIND_BENCH_CTL_DATABASE_URL"), database_url())
}

pub fn bench_keep() -> Bool {
  getenv("BENCH_KEEP") == Ok("1")
}

fn provenance_commit() -> String {
  result.unwrap(getenv("GRIND_BENCH_COMMIT"), "unknown")
}

fn provenance_dirty() -> String {
  result.unwrap(getenv("GRIND_BENCH_DIRTY"), "unknown")
}

pub fn provenance_prefix() -> String {
  provenance_commit()
  <> ","
  <> provenance_dirty()
  <> ","
  <> int.to_string(wall_clock_unix_ms())
  <> ","
  <> result.unwrap(getenv("GRIND_BENCH_SOURCE_SHA256"), "unknown")
  <> ","
  <> result.unwrap(getenv("GRIND_BENCH_NETWORK_DELAY_MS"), "0")
}

pub fn provenance_header_prefix() -> String {
  "commit,dirty,timestamp_unix_ms,source_sha256,network_delay_ms"
}

/// `0, 1, .. count - 1` as a list -- `gleam/list` has no `range` in the
/// stdlib version this project resolves; `gleam/int.range` is a fold, not a
/// list builder, so this wraps it once here.
pub fn int_range(count: Int) -> List(Int) {
  int.range(from: 0, to: count, with: [], run: fn(acc, i) { [i, ..acc] })
  |> list.reverse
}

pub fn list_at_or_panic(values: List(a), index: Int) -> a {
  case values, index {
    [head, ..], 0 -> head
    [_, ..rest], n if n > 0 -> list_at_or_panic(rest, n - 1)
    _, _ -> panic as "list_at_or_panic: index out of range"
  }
}

pub fn parent_dir(path: String) -> String {
  case string.split(path, "/") {
    [] | [_] -> "."
    parts ->
      case list.take(parts, list.length(parts) - 1) {
        [] -> "."
        directories -> string.join(directories, "/")
      }
  }
}
