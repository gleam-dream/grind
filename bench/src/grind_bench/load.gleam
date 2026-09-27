//// `gleam run -m grind_bench/load -- <scenario> <args>` -- the bench CLI
//// entry point. Scenarios:
////
////   smoke [job_count]
////     Preload `job_count` jobs (default 1000), drain them, run the full
////     I1-I7 audit, and exit non-zero if it fails. This is
////     `scripts/bench-postgres.sh`'s own gate step.
////
////   l1 <job_count> <consumers> <concurrency> <queues> <cost_ms> [repeat]
////     One point of the drain-throughput matrix: `consumers` independent
////     `Consumer`s (round-robin over `queues` distinct queue names, each
////     with `queue.with_maximum_concurrency(concurrency)`) draining
////     `job_count` preloaded jobs whose handler sleeps `cost_ms` before
////     completing. Appends one row to `<results_dir>/l1.csv` (and one to
////     `<results_dir>/statements.csv` -- item 9's `pg_stat_statements`
////     bucket split). `repeat` (default `0`) is a caller-supplied repeat
////     index, carried through to the CSV row only -- see
////     `scripts/bench-matrix.sh`, item 8's "≥3 repeats, discard warm-up".
////
////   l7 <job_count> <consumers> <concurrency> [repeat]
////     One point of the coordinator-bottleneck matrix: like `l1` but always
////     one queue and `cost_ms` fixed at 1 (the plan's "0 ms jobs, delay 1
////     ms" -- this harness has one lever for simulated handler work, so
////     both L1's "job cost" and L7's "delay" map onto the same `cost_ms`;
////     see this task's own final report for that simplification). Each
////     consumer's own coordinator `Pid` (`@internal
////     queue.coordinator_pid`) is sampled for `message_queue_len` every
////     20ms for the run's own duration. Appends one row to
////     `<results_dir>/l7.csv`.
////
////   profile <job_count> <consumers> <concurrency>
////     Item 11: a statistical profiler for the coordinator-bottleneck
////     question -- samples every consumer's own coordinator `current_function`
////     and total `reductions` every 2ms for the run's own duration (a
////     `tools`-application-free stand-in for `eprof`/`fprof`; see
////     `grind_bench_sampler_ffi:current_function_and_reductions/1`'s own
////     doc comment), then reports the sampled function distribution --
////     "where the serial time per job goes" -- to `<results_dir>/profile.csv`
////     and stdout. Same one-queue, `cost_ms = 1` shape as `l7`.
////
//// Every scenario reads `GRIND_BENCH_DATABASE_URL` (required, Grind's own
//// pool -- item 10's `grind_a` role) and `GRIND_BENCH_RESULTS_DIR` (default
//// `"results/adhoc"`, relative to the bench project's own working
//// directory -- this module is always run via `cd bench && gleam run -m
//// grind_bench/load -- ...`, matching how `consumer/`'s own test suite is
//// always run from `consumer/`). `GRIND_BENCH_CTL_DATABASE_URL` (item 10's
//// `grind_ctl` role -- ledger, drain polling, samplers) defaults to
//// `GRIND_BENCH_DATABASE_URL` when unset. `GRIND_BENCH_POSTGRES_LOG` (item
//// 6/I6, the disposable cluster's own log file) and
//// `GRIND_BENCH_PG_DATA_DIR` (item 9, DB CPU via `ps`) are both optional --
//// their checks/measurements are skipped, not failed, when unset.
//// `GRIND_BENCH_COMMIT`/`GRIND_BENCH_DIRTY` (item 13 provenance) default to
//// `"unknown"`/`"0"`.

import gleam/dict
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleam/result
import gleam/string
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/worker
import grind_bench
import grind_bench/audit
import grind_bench/preload
import grind_bench/sampler_beam
import grind_bench/sampler_db
import grind_bench/statement_split
import grind_bench/summarize
import grind_bench/worker as bench_worker
import pog
import simplifile
import sinal
import sinal/forwarder

type BenchWorker =
  worker.Worker(bench_worker.BenchJob, Int, Nil)

@external(erlang, "grind_bench_ffi", "plain_arguments")
fn plain_arguments() -> List(String)

@external(erlang, "grind_bench_ffi", "getenv")
fn getenv(name: String) -> Result(String, Nil)

@external(erlang, "grind_bench_ffi", "halt")
fn halt(code: Int) -> Nil

@external(erlang, "grind_bench_ffi", "monotonic_ms")
fn monotonic_ms() -> Int

@external(erlang, "grind_bench_ffi", "wall_clock_unix_ms")
fn wall_clock_unix_ms() -> Int

@external(erlang, "grind_bench_ffi", "cpu_times_ms")
fn cpu_times_ms_ffi(pg_data_dir: String) -> Result(Int, Nil)

@external(erlang, "grind_bench_counter_ffi", "value")
pub fn counter_value(key: Int) -> Int

@external(erlang, "grind_bench_counter_ffi", "reset_all")
fn reset_all_counters() -> Nil

@external(erlang, "grind_bench_counter_ffi", "next")
fn bump(key: Int) -> Int

@external(erlang, "grind_bench_sampler_ffi", "message_queue_len")
fn message_queue_len(pid: process.Pid) -> Int

@external(erlang, "grind_bench_sampler_ffi", "current_function_and_reductions")
fn current_function_and_reductions(
  pid: process.Pid,
) -> Result(#(String, String, Int, Int), Nil)

/// Ledger `bench_index`es -1/-2/-3 are never real bench jobs (every real
/// index is `>= 0`) -- reused as bench-run-scoped counters via the same ETS
/// table `grind_bench/worker`'s own delivery counters already live in.
/// `pub`: item 4's mutation tests exercise these through the real
/// `attach_audit_observers` wiring from `bench/test/`, a different module in
/// this same package.
pub const ledger_error_counter = -1

pub const quarantine_counter = -2

pub const forwarder_drop_counter = -3

fn results_dir() -> String {
  result.unwrap(getenv("GRIND_BENCH_RESULTS_DIR"), "results/adhoc")
}

fn database_url() -> String {
  case getenv("GRIND_BENCH_DATABASE_URL") {
    Ok(url) -> url
    Error(Nil) ->
      panic as "GRIND_BENCH_DATABASE_URL must be set to run grind_bench/load"
  }
}

/// Item 10: the harness's own ledger/drain-poll/samplers role. Defaults to
/// `database_url()` (same role as Grind) when the disposable cluster was
/// started without a separate `grind_ctl` role.
fn ctl_database_url() -> String {
  result.unwrap(getenv("GRIND_BENCH_CTL_DATABASE_URL"), database_url())
}

fn bench_keep() -> Bool {
  getenv("BENCH_KEEP") == Ok("1")
}

fn provenance_commit() -> String {
  result.unwrap(getenv("GRIND_BENCH_COMMIT"), "unknown")
}

fn provenance_dirty() -> String {
  result.unwrap(getenv("GRIND_BENCH_DIRTY"), "0")
}

fn provenance_prefix() -> String {
  provenance_commit()
  <> ","
  <> provenance_dirty()
  <> ","
  <> int.to_string(wall_clock_unix_ms())
}

fn provenance_header_prefix() -> String {
  "commit,dirty,timestamp_unix_ms"
}

pub fn main() -> Nil {
  case plain_arguments() {
    ["smoke"] -> run_smoke(1000)
    ["smoke", job_count] -> run_smoke(parse_or_panic(job_count))
    ["l1", job_count, consumers, concurrency, queues, cost_ms] ->
      run_l1(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
        parse_or_panic(queues),
        parse_or_panic(cost_ms),
        0,
      )
    ["l1", job_count, consumers, concurrency, queues, cost_ms, repeat] ->
      run_l1(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
        parse_or_panic(queues),
        parse_or_panic(cost_ms),
        parse_or_panic(repeat),
      )
    ["l7", job_count, consumers, concurrency] ->
      run_l7(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
        0,
      )
    ["l7", job_count, consumers, concurrency, repeat] ->
      run_l7(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
        parse_or_panic(repeat),
      )
    ["profile", job_count, consumers, concurrency] ->
      run_profile(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
      )
    other -> {
      io.println(
        "unknown grind_bench/load invocation: " <> string.inspect(other),
      )
      io.println(
        "usage: gleam run -m grind_bench/load -- smoke [job_count] | l1 <job_count> <consumers> <concurrency> <queues> <cost_ms> [repeat] | l7 <job_count> <consumers> <concurrency> [repeat] | profile <job_count> <consumers> <concurrency>",
      )
      halt(2)
    }
  }
}

fn parse_or_panic(value: String) -> Int {
  case int.parse(value) {
    Ok(n) -> n
    Error(Nil) -> panic as { "not an integer: " <> value }
  }
}

/// `0, 1, .. count - 1` as a list -- `gleam/list` has no `range` in the
/// stdlib version this project resolves; `gleam/int.range` is a fold, not a
/// list builder, so this wraps it once here.
fn int_range(count: Int) -> List(Int) {
  int.range(from: 0, to: count, with: [], run: fn(acc, i) { [i, ..acc] })
  |> list.reverse
}

fn list_at_or_panic(values: List(a), index: Int) -> a {
  case values, index {
    [head, ..], 0 -> head
    [_, ..rest], n if n > 0 -> list_at_or_panic(rest, n - 1)
    _, _ -> panic as "list_at_or_panic: index out of range"
  }
}

/// Shared setup every scenario needs: a fresh, migrated Grind installation
/// (item 1: its own run-scoped schema) plus a reset ledger and a dedicated
/// drain-polling connection (item 6).
type Harness {
  Harness(
    database: postgres.Database,
    ledger: pog.Connection,
    drain: pog.Connection,
  )
}

fn setup(pool_size: Int, ledger_pool_size: Int) -> Harness {
  let config =
    grind_bench.default_config(database_url())
    |> grind_bench.with_ctl_database_url(ctl_database_url())
    |> grind_bench.with_grind_pool_size(pool_size)
    |> grind_bench.with_ledger_pool_size(ledger_pool_size)
  let assert Ok(ledger) = grind_bench.start_ledger_pool(config)
  // Item 1: drop any schema orphaned by a run that never reached its own
  // end-of-run cleanup (a crash, or an interrupted BENCH_KEEP run), before
  // this run creates its own fresh one.
  let assert Ok(Nil) = grind_bench.drop_stale_bench_schemas(ledger)
  let assert Ok(settings) =
    grind_bench.grind_settings(config) |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  let assert Ok(Nil) = postgres.migrate(database)
  grind_bench.assert_queue_empty(database)
  let assert Ok(Nil) = grind_bench.reset_ledger(ledger)
  let assert Ok(drain) = grind_bench.start_drain_connection(config)
  reset_all_counters()
  case preload.check_schema_matches(database) {
    Ok(Nil) -> Nil
    Error(drift) -> {
      io.println(
        "grind_bench/preload: schema drift detected, refusing to preload: "
        <> string.inspect(drift),
      )
      halt(1)
    }
  }
  Harness(database:, ledger:, drain:)
}

fn schema_of(database: postgres.Database) -> String {
  job.installation_schema(postgres.installation(database))
}

/// Item 1: drops this run's own fresh schema unless `BENCH_KEEP=1`, so
/// schemas never accumulate across ordinary runs (see `setup`'s own
/// `drop_stale_bench_schemas` for the backstop on a run that skips this).
fn cleanup_schema(ledger: pog.Connection, grind_schema: String) -> Nil {
  case bench_keep() {
    True ->
      io.println(
        "BENCH_KEEP=1: leaving schema \"" <> grind_schema <> "\" in place",
      )
    False -> {
      let assert Ok(Nil) = grind_bench.drop_schema(ledger, grind_schema)
      Nil
    }
  }
}

/// Attaches the two observers every scenario's audit needs:
/// `[grind, job, quarantined]` (I3) and `[sinal, forwarder, dropped]` (I7).
/// `pub`: item 4's mutation tests attach through this exact function (the
/// real wiring a load scenario uses), not a copy -- `id_suffix` lets each
/// test call this with a distinct `sinal.handler_id` so repeated calls
/// within one `gleam test` process (each test its own call) never collide
/// on a duplicate handler id. Both observers live for the caller's own
/// process lifetime -- never detached.
pub fn attach_audit_observers(id_suffix: String) -> Nil {
  let assert Ok(quarantine_id) =
    sinal.handler_id("grind-bench-quarantine-" <> id_suffix)
  let assert Ok(_) =
    sinal.observe(quarantine_id, observation.quarantined(), fn(_m, _d) {
      let _ = bump(quarantine_counter)
      Nil
    })
  let assert Ok(dropped_id) =
    sinal.handler_id("grind-bench-forwarder-dropped-" <> id_suffix)
  let assert Ok(_) =
    sinal.observe(dropped_id, forwarder.dropped_event(), fn(_m, _d) {
      let _ = bump(forwarder_drop_counter)
      Nil
    })
  Nil
}

fn worker_versions(worker_def: BenchWorker) -> worker.Metadata {
  worker.metadata(worker_def)
}

/// Preloads `job_count` jobs, round-robin distributed across `queues`, and
/// records `bench_submissions` for every preloaded row -- item 2, bullet 1:
/// any `record_submissions` insert failure now fails the whole run (via
/// `let assert`) rather than being swallowed.
fn preload_and_track(
  database: postgres.Database,
  ledger: pog.Connection,
  worker_def: BenchWorker,
  queues: List(String),
  job_count: Int,
  cost_ms: Int,
) -> Nil {
  let meta = worker_versions(worker_def)
  let encode_input = fn(job: preload.PreloadJob) -> String {
    worker.encode_input(
      worker_def,
      bench_worker.BenchJob(bench_index: job.bench_index, cost_ms: job.cost_ms),
    )
  }
  let queues_count = list.length(queues)
  int_range(queues_count)
  |> list.each(fn(queue_index) {
    let queue_name = list_at_or_panic(queues, queue_index)
    let indices =
      int_range(job_count)
      |> list.filter(fn(index) { index % queues_count == queue_index })
    case indices {
      [] -> Nil
      _ -> {
        let jobs = list.map(indices, preload.immediate(_, cost_ms))
        let assert Ok(pairs) =
          preload.preload(database, queue_name, meta, encode_input, jobs, 500)
        let assert Ok(Nil) = record_submissions(ledger, pairs, queue_name)
        Nil
      }
    }
  })
}

/// Item 2, bullet 1: propagates the first insert failure instead of
/// swallowing it (`list.try_each`, not `list.each` with a discarded
/// result).
fn record_submissions(
  ledger: pog.Connection,
  pairs: List(#(Int, Int)),
  queue_name: String,
) -> Result(Nil, pog.QueryError) {
  list.try_each(pairs, fn(pair) {
    let #(bench_index, job_id) = pair
    let query =
      pog.query(
        "INSERT INTO bench_submissions (bench_index, job_id, queue) VALUES ($1, $2, $3)",
      )
      |> pog.parameter(pog.int(bench_index))
      |> pog.parameter(pog.int(job_id))
      |> pog.parameter(pog.text(queue_name))
    pog.execute(query, ledger) |> result.map(fn(_) { Nil })
  })
}

/// Item 2, bullet 2: fails the run immediately (before any consumer ever
/// starts) if the ledger's own submission count does not equal `job_count`.
fn assert_submission_count(ledger: pog.Connection, job_count: Int) -> Nil {
  case audit.check_submission_count(ledger, job_count) {
    Ok(Ok(Nil)) -> Nil
    other -> {
      io.println(
        "grind_bench/load: submission count mismatch after preload: "
        <> string.inspect(other),
      )
      halt(1)
    }
  }
}

/// Item 7: `ANALYZE` (fresh planner stats) and `CHECKPOINT` (flush preload's
/// own dirty buffers) after preload, before `t0` -- so neither shows up as
/// noise inside the measured window. Runs against Grind's own connection
/// (`ANALYZE`, schema-scoped) and the ctl/ledger connection (`CHECKPOINT`,
/// cluster-wide, no schema needed). Both are best-effort: a role without
/// `CHECKPOINT` privilege (a production-shaped role, not this disposable
/// cluster's superuser) would fail this and should skip it, so failures are
/// logged, not fatal.
fn analyze_and_checkpoint(
  database: postgres.Database,
  ledger: pog.Connection,
) -> Nil {
  let connection = postgres.connection(database)
  case pog.execute(pog.query("ANALYZE grind_jobs"), connection) {
    Ok(_) -> Nil
    Error(err) ->
      io.println("grind_bench/load: ANALYZE failed: " <> string.inspect(err))
  }
  case pog.execute(pog.query("CHECKPOINT"), ledger) {
    Ok(_) -> Nil
    Error(err) ->
      io.println("grind_bench/load: CHECKPOINT failed: " <> string.inspect(err))
  }
}

fn wait_for_drain(
  drain: pog.Connection,
  grind_schema: String,
  timeout_ms: Int,
) -> Result(Nil, Nil) {
  poll_drain(drain, grind_schema, monotonic_ms() + timeout_ms)
}

fn poll_drain(
  drain: pog.Connection,
  grind_schema: String,
  deadline: Int,
) -> Result(Nil, Nil) {
  case audit.check_all_succeeded_with_expected_output(drain, grind_schema) {
    Ok(Ok(Nil)) -> Ok(Nil)
    // Still violations (some job not yet succeeded) -- keep polling, unless
    // the deadline has passed.
    Ok(Error(_)) ->
      case monotonic_ms() >= deadline {
        True -> Error(Nil)
        False -> {
          process.sleep(5)
          poll_drain(drain, grind_schema, deadline)
        }
      }
    // A real query failure (item 3, bullet 4's own "fail loud" rule applies
    // here too): silently retrying this the way an ordinary "not yet
    // drained" result is retried would misreport a genuine harness/DB
    // problem as an ordinary timeout -- exactly the bug a missing
    // `search_path` on the drain connection produced the first time this
    // ran for real (500 jobs finished in under a second; the poll spun
    // silently for the full 30s timeout because every query it ran failed).
    Error(query_error) ->
      panic as {
        "grind_bench/load: drain poll query failed: "
        <> string.inspect(query_error)
      }
  }
}

/// Item 7: `t0` is the first bench job's own handler dispatch
/// (`bench_effects.started_at`, the closest DB timestamp to "first claim"
/// the public/ledger schema offers -- see this module's own top-level doc
/// comment on why this, not consumer-startup wall-clock, is `t0`), and
/// `t_end` is the last job's own `grind_jobs.finished_at`. Both come from
/// the database, not this process's own wall clock, so N consumers' own
/// staggered startup calls (`list.map` over `queue.start`) can never give
/// an early one a head start inside the measured window, and the drain
/// poll's own interval never inflates the tail.
fn timestamps_ms(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(#(Int, Int), Nil) {
  use t0 <- result.try(unix_ms_query(
    "SELECT (extract(epoch FROM min(started_at)) * 1000)::bigint FROM bench_effects",
    ledger,
  ))
  let sql =
    "SELECT (extract(epoch FROM max(j.finished_at)) * 1000)::bigint FROM bench_submissions bs JOIN \""
    <> grind_schema
    <> "\".\"grind_jobs\" j ON j.id = bs.job_id"
  use t_end <- result.try(unix_ms_query(sql, ledger))
  Ok(#(t0, t_end))
}

fn unix_ms_query(sql: String, ledger: pog.Connection) -> Result(Int, Nil) {
  let query =
    pog.query(sql)
    |> pog.returning({
      use value <- decode.field(0, decode.optional(decode.int))
      decode.success(value)
    })
  case pog.execute(query, ledger) {
    Ok(pog.Returned(rows: [Some(value)], ..)) -> Ok(value)
    _ -> Error(Nil)
  }
}

/// Item 6/I6: `GRIND_BENCH_POSTGRES_LOG`'s own line count right now --
/// captured before a run starts so `postgres_log_window` can slice off
/// exactly the lines written during the run, with no `log_line_prefix`
/// timestamp parsing required. `0` if the env var is unset or the file does
/// not exist yet (a fresh disposable cluster's log starts empty).
fn postgres_log_lines_before() -> Int {
  case getenv("GRIND_BENCH_POSTGRES_LOG") {
    Error(Nil) -> 0
    Ok(path) ->
      case simplifile.read(path) {
        Error(_) -> 0
        Ok(content) -> list.length(string.split(content, "\n"))
      }
  }
}

/// `Error(Nil)`: `GRIND_BENCH_POSTGRES_LOG` is unset (I6's log scan is
/// skipped entirely, not failed -- see `audit.run`'s own `postgres_log_window`
/// parameter). `Ok(lines)`: every log line written since `lines_before`.
fn postgres_log_window(lines_before: Int) -> Result(List(String), Nil) {
  case getenv("GRIND_BENCH_POSTGRES_LOG") {
    Error(Nil) -> Error(Nil)
    Ok(path) ->
      case simplifile.read(path) {
        Error(_) -> Error(Nil)
        Ok(content) -> Ok(list.drop(string.split(content, "\n"), lines_before))
      }
  }
}

/// Item 9: postmaster + descendants' own cumulative CPU time (`ps` cputime),
/// `None` if `GRIND_BENCH_PG_DATA_DIR` is unset or unreadable -- best-effort,
/// never fatal.
fn cpu_ms_now() -> Result(Int, Nil) {
  case getenv("GRIND_BENCH_PG_DATA_DIR") {
    Error(Nil) -> Error(Nil)
    Ok(dir) -> cpu_times_ms_ffi(dir)
  }
}

/// Item 9: `#(cpu_ms_delta, cpu_percent_of_one_core)` from a before/after
/// `ps` cputime pair and the run's own wall-clock `elapsed_ms`. `None` if
/// either sample was unavailable.
fn cpu_delta(
  before: Result(Int, Nil),
  after: Result(Int, Nil),
  elapsed_ms: Int,
) -> Result(#(Int, Float), Nil) {
  case before, after {
    Ok(b), Ok(a) -> {
      let delta_ms = a - b
      let percent = case elapsed_ms > 0 {
        True -> int.to_float(delta_ms) /. int.to_float(elapsed_ms) *. 100.0
        False -> 0.0
      }
      Ok(#(delta_ms, percent))
    }
    _, _ -> Error(Nil)
  }
}

fn run_audit_and_report(
  ledger: pog.Connection,
  database: postgres.Database,
  label: String,
  expected_job_count: Int,
  log_lines_before: Int,
) -> Nil {
  let grind_schema = schema_of(database)
  let log_window = postgres_log_window(log_lines_before)
  let assert Ok(report) =
    audit.run(
      ledger,
      grind_schema,
      expected_job_count,
      counter_value(ledger_error_counter),
      counter_value(forwarder_drop_counter),
      counter_value(quarantine_counter),
      log_window,
    )
  let _ = postgres.close(database)
  cleanup_schema(ledger, grind_schema)
  case audit.passed(report) {
    True -> io.println(label <> ": audit PASSED (I1-I7)")
    False -> {
      io.println(label <> ": audit FAILED")
      list.each(report.violations, fn(violation) {
        io.println("  - " <> string.inspect(violation))
      })
      halt(1)
    }
  }
}

fn write_row(path: String, header: String, row: String) -> Nil {
  let _ = simplifile.create_directory_all(parent_dir(path))
  let exists = case simplifile.read(path) {
    Ok(_) -> True
    Error(_) -> False
  }
  let content = case exists {
    True -> row <> "\n"
    False -> header <> "\n" <> row <> "\n"
  }
  let _ = simplifile.append(to: path, contents: content)
  Nil
}

fn write_rows(path: String, header: String, rows: List(String)) -> Nil {
  list.each(rows, fn(row) { write_row(path, header, row) })
}

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

fn run_smoke(job_count: Int) -> Nil {
  io.println("grind_bench smoke: " <> int.to_string(job_count) <> " jobs")
  let harness = setup(10, grind_bench.ledger_pool_size_for_concurrency(10))
  let Harness(database:, ledger:, drain:) = harness
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.smoke.echo")
  let assert Ok(workers) = registry.new("bench-smoke")
  let assert Ok(workers) = registry.register(workers, worker_def)
  attach_audit_observers("smoke")
  let log_lines_before = postgres_log_lines_before()

  preload_and_track(database, ledger, worker_def, ["bench-smoke"], job_count, 0)
  assert_submission_count(ledger, job_count)
  analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_concurrency(10)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)

  let grind_schema = schema_of(database)
  let drained = wait_for_drain(drain, grind_schema, 30_000)
  let _ = queue.stop(consumer)

  case drained {
    Error(Nil) -> {
      io.println("smoke: timed out waiting for drain")
      let _ = postgres.close(database)
      halt(1)
    }
    Ok(Nil) -> {
      let elapsed_ms = case timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      io.println(
        "smoke: drained "
        <> int.to_string(job_count)
        <> " jobs in "
        <> int.to_string(elapsed_ms)
        <> "ms (DB-timestamp-derived)",
      )
      run_audit_and_report(
        ledger,
        database,
        "smoke",
        job_count,
        log_lines_before,
      )
    }
  }
}

fn run_l1(
  job_count: Int,
  consumers: Int,
  concurrency: Int,
  queues_n: Int,
  cost_ms: Int,
  repeat: Int,
) -> Nil {
  let total_concurrency = consumers * concurrency
  let harness =
    setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let Harness(database:, ledger:, drain:) = harness
  let queue_names =
    int_range(queues_n)
    |> list.map(fn(i) { "l1-q" <> int.to_string(i) })
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l1.echo")
  let registries =
    list.map(queue_names, fn(name) {
      let assert Ok(r) = registry.new(name)
      let assert Ok(r) = registry.register(r, worker_def)
      r
    })
  attach_audit_observers("l1")
  let log_lines_before = postgres_log_lines_before()
  preload_and_track(
    database,
    ledger,
    worker_def,
    queue_names,
    job_count,
    cost_ms,
  )
  assert_submission_count(ledger, job_count)
  analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(10)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy

  let label =
    int.to_string(consumers)
    <> "x"
    <> int.to_string(concurrency)
    <> "x"
    <> int.to_string(queues_n)
    <> "xc"
    <> int.to_string(cost_ms)
  let beam_raw_path = results_dir() <> "/raw/l1-" <> label <> "-beam.jsonl"
  let db_raw_path = results_dir() <> "/raw/l1-" <> label <> "-db.jsonl"

  let cpu_before = cpu_ms_now()
  let statements_before = statement_split.snapshot(drain)

  let consumers_list =
    int_range(consumers)
    |> list.map(fn(i) {
      let r = list_at_or_panic(registries, i % queues_n)
      let assert Ok(c) = queue.start(database, r, policy)
      c
    })
  let beam_sampler_pid =
    process.spawn_unlinked(fn() { sampler_beam.run(beam_raw_path, 1000, 300) })
  let db_sampler_pid =
    process.spawn_unlinked(fn() {
      sampler_db.run(postgres.connection(database), db_raw_path, 1000, 300)
    })

  let grind_schema = schema_of(database)
  let drained = wait_for_drain(drain, grind_schema, 60_000)
  process.kill(beam_sampler_pid)
  process.kill(db_sampler_pid)
  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  case drained {
    Error(Nil) -> {
      io.println("l1: timed out waiting for drain")
      halt(1)
    }
    Ok(Nil) -> {
      let elapsed_ms = case timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      let cpu_after = cpu_ms_now()
      let #(cpu_ms_delta, cpu_pct) =
        result.unwrap(cpu_delta(cpu_before, cpu_after, elapsed_ms), #(-1, -1.0))
      let jobs_per_sec = case elapsed_ms > 0 {
        True ->
          int.to_float(job_count) /. { int.to_float(elapsed_ms) /. 1000.0 }
        False -> 0.0
      }
      let row =
        string.join(
          [
            provenance_prefix(),
            int.to_string(repeat),
            int.to_string(consumers),
            int.to_string(concurrency),
            int.to_string(queues_n),
            int.to_string(cost_ms),
            int.to_string(job_count),
            int.to_string(elapsed_ms),
            float.to_string(jobs_per_sec),
            int.to_string(total_concurrency),
            int.to_string(cpu_ms_delta),
            float.to_string(cpu_pct),
          ],
          ",",
        )
      write_row(
        results_dir() <> "/l1.csv",
        provenance_header_prefix()
          <> ",repeat,consumers,concurrency,queues,cost_ms,job_count,elapsed_ms,jobs_per_sec,pool_size,db_cpu_ms,db_cpu_pct_of_core",
        row,
      )
      io.println("l1 " <> row)
      write_statement_split_rows(
        "l1-" <> label,
        statements_before,
        statement_split.snapshot(drain),
      )
      summarize_field_to_csv(
        beam_raw_path,
        "process_count",
        "l1-" <> label <> "-beam",
      )
      summarize_field_to_csv(
        beam_raw_path,
        "memory_total_bytes",
        "l1-" <> label <> "-beam",
      )
      summarize_field_to_csv(
        beam_raw_path,
        "reductions",
        "l1-" <> label <> "-beam",
      )
      summarize_field_to_csv(
        db_raw_path,
        "waiting_locks",
        "l1-" <> label <> "-db",
      )
      run_audit_and_report(ledger, database, "l1", job_count, log_lines_before)
    }
  }
}

fn write_statement_split_rows(
  label: String,
  before: Result(statement_split.Totals, Nil),
  after: Result(statement_split.Totals, Nil),
) -> Nil {
  let deltas = statement_split.diff(before, after)
  case deltas {
    [] -> Nil
    _ ->
      write_rows(
        results_dir() <> "/statements.csv",
        provenance_header_prefix()
          <> ",label,bucket,calls_delta,total_exec_time_ms_delta",
        list.map(deltas, fn(entry) {
          let #(bucket, calls, total_ms) = entry
          string.join(
            [
              provenance_prefix(),
              label,
              statement_split.bucket_name(bucket),
              int.to_string(calls),
              float.to_string(total_ms),
            ],
            ",",
          )
        }),
      )
  }
}

/// Reads `field` out of every line of `path` (a `grind_bench/sampler_beam`
/// or `grind_bench/sampler_db` JSONL file), summarizes it, and appends one
/// row to `<results_dir>/samplers.csv`. Silently does nothing if the file is
/// empty or `field` was never present (a run short enough to collect zero
/// samples, or a `pg_stat_statements`-only field on a cluster without the
/// extension) -- the samplers themselves are best-effort evidence, not a
/// gated contract.
fn summarize_field_to_csv(path: String, field: String, label: String) -> Nil {
  case summarize.read_field_values(path, field) {
    Error(_) -> Nil
    Ok(values) ->
      case summarize.percentiles(values) {
        Error(Nil) -> Nil
        Ok(stats) -> {
          let _ =
            summarize.write_csv(results_dir() <> "/samplers.csv", [
              summarize.csv_row(label, field, stats),
            ])
          Nil
        }
      }
  }
}

fn run_l7(
  job_count: Int,
  consumers: Int,
  concurrency: Int,
  repeat: Int,
) -> Nil {
  let total_concurrency = consumers * concurrency
  let harness =
    setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let Harness(database:, ledger:, drain:) = harness
  let queue_name = "l7-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l7.echo")
  let assert Ok(r) = registry.new(queue_name)
  let assert Ok(r) = registry.register(r, worker_def)
  attach_audit_observers("l7")
  let log_lines_before = postgres_log_lines_before()
  preload_and_track(database, ledger, worker_def, [queue_name], job_count, 1)
  assert_submission_count(ledger, job_count)
  analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(5)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy

  let cpu_before = cpu_ms_now()

  let consumers_list =
    int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, r, policy)
      c
    })
  let coordinator_pids = list.filter_map(consumers_list, queue.coordinator_pid)

  let raw_path =
    results_dir()
    <> "/raw/l7-"
    <> int.to_string(consumers)
    <> "x"
    <> int.to_string(concurrency)
    <> ".jsonl"
  // `process.spawn_unlinked` + `process.kill` (not a `Subject`-based "stop"
  // message): a `gleam_erlang` `Subject` can only be received from by the
  // process that created it, so a subject created here could never be
  // received on inside the spawned sampler process -- see
  // `grind_bench/sampler_beam.run`'s own doc comment for the same trap
  // found and fixed there first.
  let sampler_pid =
    process.spawn_unlinked(fn() {
      sample_coordinators(coordinator_pids, raw_path, 20)
    })

  let grind_schema = schema_of(database)
  let drained = wait_for_drain(drain, grind_schema, 60_000)
  process.kill(sampler_pid)
  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  case drained {
    Error(Nil) -> {
      io.println("l7: timed out waiting for drain")
      halt(1)
    }
    Ok(Nil) -> {
      let elapsed_ms = case timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      let cpu_after = cpu_ms_now()
      let #(cpu_ms_delta, cpu_pct) =
        result.unwrap(cpu_delta(cpu_before, cpu_after, elapsed_ms), #(-1, -1.0))
      let jobs_per_sec = case elapsed_ms > 0 {
        True ->
          int.to_float(job_count) /. { int.to_float(elapsed_ms) /. 1000.0 }
        False -> 0.0
      }
      let mqlen_stats =
        summarize.read_field_values(raw_path, "message_queue_len_max")
        |> result.unwrap([])
        |> summarize.percentiles
      let #(mqlen_p50, mqlen_p99) = case mqlen_stats {
        Ok(summarize.Percentiles(p50:, p99:, ..)) -> #(p50, p99)
        Error(Nil) -> #(0.0, 0.0)
      }
      let row =
        string.join(
          [
            provenance_prefix(),
            int.to_string(repeat),
            int.to_string(consumers),
            int.to_string(concurrency),
            int.to_string(job_count),
            int.to_string(elapsed_ms),
            float.to_string(jobs_per_sec),
            float.to_string(mqlen_p50),
            float.to_string(mqlen_p99),
            int.to_string(cpu_ms_delta),
            float.to_string(cpu_pct),
          ],
          ",",
        )
      write_row(
        results_dir() <> "/l7.csv",
        provenance_header_prefix()
          <> ",repeat,consumers,concurrency,job_count,elapsed_ms,jobs_per_sec,coordinator_mqlen_p50,coordinator_mqlen_p99,db_cpu_ms,db_cpu_pct_of_core",
        row,
      )
      io.println("l7 " <> row)
      run_audit_and_report(ledger, database, "l7", job_count, log_lines_before)
    }
  }
}

fn sample_coordinators(
  pids: List(process.Pid),
  path: String,
  interval_ms: Int,
) -> Nil {
  let _ = simplifile.create_directory_all(parent_dir(path))
  let _ = simplifile.write(to: path, contents: "")
  sample_loop(pids, path, interval_ms, 100_000)
}

fn sample_loop(
  pids: List(process.Pid),
  path: String,
  interval_ms: Int,
  ticks_remaining: Int,
) -> Nil {
  case ticks_remaining <= 0 {
    True -> Nil
    False -> {
      process.sleep(interval_ms)
      let max_len = list.fold(list.map(pids, message_queue_len), 0, int.max)
      let line =
        json.object([
          #("unix_ms", json.int(monotonic_ms())),
          #("message_queue_len_max", json.int(max_len)),
        ])
        |> json.to_string
      let _ = simplifile.append(to: path, contents: line <> "\n")
      sample_loop(pids, path, interval_ms, ticks_remaining - 1)
    }
  }
}

// -- profile: item 11, statistical coordinator profiling -------------------

type FunctionSample {
  FunctionSample(module: String, function: String, arity: Int)
}

fn function_sample_decoder() -> decode.Decoder(FunctionSample) {
  use module <- decode.field("module", decode.string)
  use function <- decode.field("function", decode.string)
  use arity <- decode.field("arity", decode.int)
  decode.success(FunctionSample(module:, function:, arity:))
}

type ProfileTick {
  ProfileTick(total_reductions: Int, functions: List(FunctionSample))
}

fn profile_tick_decoder() -> decode.Decoder(ProfileTick) {
  use total_reductions <- decode.field("total_reductions", decode.int)
  use functions <- decode.field(
    "functions",
    decode.list(function_sample_decoder()),
  )
  decode.success(ProfileTick(total_reductions:, functions:))
}

fn run_profile(job_count: Int, consumers: Int, concurrency: Int) -> Nil {
  let total_concurrency = consumers * concurrency
  let harness =
    setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let Harness(database:, ledger:, drain:) = harness
  let queue_name = "profile-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.profile.echo")
  let assert Ok(r) = registry.new(queue_name)
  let assert Ok(r) = registry.register(r, worker_def)
  attach_audit_observers("profile")
  let log_lines_before = postgres_log_lines_before()
  preload_and_track(database, ledger, worker_def, [queue_name], job_count, 1)
  assert_submission_count(ledger, job_count)
  analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(5)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let consumers_list =
    int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, r, policy)
      c
    })
  let coordinator_pids = list.filter_map(consumers_list, queue.coordinator_pid)

  let label = int.to_string(consumers) <> "x" <> int.to_string(concurrency)
  let raw_path = results_dir() <> "/raw/profile-" <> label <> ".jsonl"
  let sampler_pid =
    process.spawn_unlinked(fn() {
      profile_sample_loop(coordinator_pids, raw_path, 2, 100_000)
    })

  let grind_schema = schema_of(database)
  let drained = wait_for_drain(drain, grind_schema, 60_000)
  process.kill(sampler_pid)
  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  case drained {
    Error(Nil) -> {
      io.println("profile: timed out waiting for drain")
      halt(1)
    }
    Ok(Nil) -> {
      let elapsed_ms = case timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      report_profile(raw_path, "profile-" <> label, elapsed_ms)
      run_audit_and_report(
        ledger,
        database,
        "profile",
        job_count,
        log_lines_before,
      )
    }
  }
}

fn profile_sample_loop(
  pids: List(process.Pid),
  path: String,
  interval_ms: Int,
  ticks_remaining: Int,
) -> Nil {
  case ticks_remaining <= 0 {
    True -> Nil
    False -> {
      let _ = simplifile.create_directory_all(parent_dir(path))
      profile_loop(pids, path, interval_ms, ticks_remaining, True)
    }
  }
}

fn profile_loop(
  pids: List(process.Pid),
  path: String,
  interval_ms: Int,
  ticks_remaining: Int,
  first_tick: Bool,
) -> Nil {
  case ticks_remaining <= 0 {
    True -> Nil
    False -> {
      case first_tick {
        True -> {
          let _ = simplifile.write(to: path, contents: "")
          Nil
        }
        False -> Nil
      }
      process.sleep(interval_ms)
      let samples = list.filter_map(pids, current_function_and_reductions)
      let total_reductions =
        list.fold(samples, 0, fn(acc, sample) {
          let #(_, _, _, reductions) = sample
          acc + reductions
        })
      let line =
        json.object([
          #("unix_ms", json.int(monotonic_ms())),
          #("total_reductions", json.int(total_reductions)),
          #(
            "functions",
            json.array(samples, fn(sample) {
              let #(module, function, arity, _) = sample
              json.object([
                #("module", json.string(module)),
                #("function", json.string(function)),
                #("arity", json.int(arity)),
              ])
            }),
          ),
        ])
        |> json.to_string
      let _ = simplifile.append(to: path, contents: line <> "\n")
      profile_loop(pids, path, interval_ms, ticks_remaining - 1, False)
    }
  }
}

fn report_profile(path: String, label: String, elapsed_ms: Int) -> Nil {
  case simplifile.read(path) {
    Error(_) -> io.println("profile: no samples captured at " <> path)
    Ok(content) -> {
      let ticks =
        content
        |> string.split("\n")
        |> list.map(string.trim)
        |> list.filter(fn(line) { line != "" })
        |> list.filter_map(fn(line) {
          json.parse(line, profile_tick_decoder()) |> result.replace_error(Nil)
        })
      case ticks {
        [] -> io.println("profile: no decodable samples at " <> path)
        _ -> {
          let all_samples = list.flat_map(ticks, fn(tick) { tick.functions })
          let total_samples = list.length(all_samples)
          let tally =
            list.fold(all_samples, dict.new(), fn(acc, sample) {
              let key =
                sample.module
                <> ":"
                <> sample.function
                <> "/"
                <> int.to_string(sample.arity)
              dict.upsert(acc, key, fn(existing) {
                case existing {
                  option.Some(n) -> n + 1
                  option.None -> 1
                }
              })
            })
          let reduction_delta = case ticks {
            [first, ..] ->
              case list.last(ticks) {
                Ok(last) -> last.total_reductions - first.total_reductions
                Error(Nil) -> 0
              }
            [] -> 0
          }
          let reduction_rate_per_sec = case elapsed_ms > 0 {
            True ->
              int.to_float(reduction_delta)
              /. { int.to_float(elapsed_ms) /. 1000.0 }
            False -> 0.0
          }
          let rows =
            dict.to_list(tally)
            |> list.sort(fn(a, b) { int.compare(b.1, a.1) })
            |> list.map(fn(entry) {
              let #(key, count) = entry
              let percent = case total_samples > 0 {
                True ->
                  int.to_float(count) /. int.to_float(total_samples) *. 100.0
                False -> 0.0
              }
              string.join(
                [
                  provenance_prefix(),
                  label,
                  key,
                  int.to_string(count),
                  float.to_string(percent),
                  float.to_string(reduction_rate_per_sec),
                ],
                ",",
              )
            })
          write_rows(
            results_dir() <> "/profile.csv",
            provenance_header_prefix()
              <> ",label,function,sample_count,percent,reduction_rate_per_sec",
            rows,
          )
          io.println(
            "profile "
            <> label
            <> ": "
            <> int.to_string(total_samples)
            <> " samples, "
            <> int.to_string(reduction_delta)
            <> " reductions ("
            <> float.to_string(reduction_rate_per_sec)
            <> "/s), top functions:",
          )
          rows
          |> list.take(12)
          |> list.each(fn(row) { io.println("  " <> row) })
        }
      }
    }
  }
}
