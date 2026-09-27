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
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import grind/job
import grind/observation
import grind/postgres
import grind/pruner
import grind/queue
import grind/registry
import grind/submission
import grind/unique
import grind/worker
import grind_bench
import grind_bench/audit
import grind_bench/instrumentation
import grind_bench/latency
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

/// L4's own `SubmissionId`-uniqueness counter, same reused-ETS-table
/// scheme as the three above.
pub const l4_submission_counter = -4

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
    ["l2", consumers, interval_ms, filler_rows, duration_ms] ->
      run_l2(
        parse_or_panic(consumers),
        parse_or_panic(interval_ms),
        parse_or_panic(filler_rows),
        parse_or_panic(duration_ms),
        0,
      )
    ["l2", consumers, interval_ms, filler_rows, duration_ms, repeat] ->
      run_l2(
        parse_or_panic(consumers),
        parse_or_panic(interval_ms),
        parse_or_panic(filler_rows),
        parse_or_panic(duration_ms),
        parse_or_panic(repeat),
      )
    ["l3", arrival_per_sec, duration_ms] ->
      run_l3(parse_or_panic(arrival_per_sec), parse_or_panic(duration_ms), 0)
    ["l3", arrival_per_sec, duration_ms, repeat] ->
      run_l3(
        parse_or_panic(arrival_per_sec),
        parse_or_panic(duration_ms),
        parse_or_panic(repeat),
      )
    ["l4", submitters, mode, total_submissions] ->
      run_l4(
        parse_or_panic(submitters),
        mode,
        parse_or_panic(total_submissions),
        0,
      )
    ["l4", submitters, mode, total_submissions, repeat] ->
      run_l4(
        parse_or_panic(submitters),
        mode,
        parse_or_panic(total_submissions),
        parse_or_panic(repeat),
      )
    ["l5", pruner_on, duration_ms] ->
      run_l5(parse_or_panic(pruner_on), parse_or_panic(duration_ms), 0)
    ["l5", pruner_on, duration_ms, repeat] ->
      run_l5(
        parse_or_panic(pruner_on),
        parse_or_panic(duration_ms),
        parse_or_panic(repeat),
      )
    ["l6t1", concurrency, job_count, cost_ms] ->
      run_l6t1(
        parse_or_panic(concurrency),
        parse_or_panic(job_count),
        parse_or_panic(cost_ms),
        0,
      )
    ["l6t1", concurrency, job_count, cost_ms, repeat] ->
      run_l6t1(
        parse_or_panic(concurrency),
        parse_or_panic(job_count),
        parse_or_panic(cost_ms),
        parse_or_panic(repeat),
      )
    ["l6t2", k_slow_acks, d_ms] ->
      run_l6t2(parse_or_panic(k_slow_acks), parse_or_panic(d_ms), 0)
    ["l6t2", k_slow_acks, d_ms, repeat] ->
      run_l6t2(
        parse_or_panic(k_slow_acks),
        parse_or_panic(d_ms),
        parse_or_panic(repeat),
      )
    other -> {
      io.println(
        "unknown grind_bench/load invocation: " <> string.inspect(other),
      )
      io.println(
        "usage: gleam run -m grind_bench/load -- smoke [job_count] | l1 <job_count> <consumers> <concurrency> <queues> <cost_ms> [repeat] | l7 <job_count> <consumers> <concurrency> [repeat] | profile <job_count> <consumers> <concurrency> | l2 <consumers> <interval_ms> <filler_rows> <duration_ms> [repeat] | l3 <arrival_per_sec> <duration_ms> [repeat] | l4 <submitters> <hot|cold> <total_submissions> [repeat] | l5 <0|1 pruner_on> <duration_ms> [repeat] | l6t1 <concurrency> <job_count> <cost_ms> [repeat] | l6t2 <k_slow_acks> <d_ms> [repeat]",
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
  setup_with_settings(pool_size, ledger_pool_size, fn(settings) { settings })
}

/// Like `setup`, but overrides `postgres.Settings.statement_deadline_ms`
/// (`D`) before validating -- L6's T2 scenario scales `D` down from the
/// real 4000ms default for wall-clock feasibility while preserving the
/// exact `L = 6D` / `cost = 3L` relationships (see `run_l6t2`'s own doc
/// comment).
fn setup_with_deadline(
  pool_size: Int,
  ledger_pool_size: Int,
  deadline_ms: Int,
) -> Harness {
  // `postgres.validate` requires `unique_lock_wait_ms + 1000 <
  // statement_deadline_ms` (`docs/RISKS.md`-documented margin) -- the
  // default `unique_lock_wait_ms` (2000) only clears the real default
  // deadline (4000). A scaled-down `deadline_ms` (L6T2's own reduced `D`)
  // needs a proportionally scaled-down lock wait too, or `setup` itself
  // fails closed before this scenario ever starts.
  let lock_wait_ms = int.max(1, deadline_ms / 4)
  setup_with_settings(pool_size, ledger_pool_size, fn(settings) {
    settings
    |> postgres.with_statement_deadline(deadline_ms)
    |> postgres.with_unique_lock_wait(lock_wait_ms)
  })
}

fn setup_with_settings(
  pool_size: Int,
  ledger_pool_size: Int,
  adjust: fn(postgres.Settings) -> postgres.Settings,
) -> Harness {
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
    grind_bench.grind_settings(config) |> adjust |> postgres.validate
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

// -- Shared helpers for L2-L6 -----------------------------------------------

fn bool_str(value: Bool) -> String {
  case value {
    True -> "true"
    False -> "false"
  }
}

fn bucket_totals(
  deltas: List(#(statement_split.Bucket, Int, Float)),
  bucket: statement_split.Bucket,
) -> #(Int, Float) {
  case list.find(deltas, fn(entry) { entry.0 == bucket }) {
    Ok(#(_, calls, ms)) -> #(calls, ms)
    Error(Nil) -> #(0, 0.0)
  }
}

fn percentile_field(
  stats: Result(summarize.Percentiles, Nil),
  which: String,
) -> String {
  case stats {
    Error(Nil) -> "-1"
    Ok(summarize.Percentiles(p50:, p99:, max:, ..)) ->
      case which {
        "p50" -> float.to_string(p50)
        "p99" -> float.to_string(p99)
        "max" -> float.to_string(max)
        _ -> "-1"
      }
  }
}

fn count_jobs_in_queue(connection: pog.Connection, queue_name: String) -> Int {
  let query =
    pog.query("SELECT count(*) FROM grind_jobs WHERE queue = $1")
    |> pog.parameter(pog.text(queue_name))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  let assert Ok(pog.Returned(rows: [count], ..)) =
    pog.execute(query, connection)
  count
}

fn dead_tuple_count(connection: pog.Connection, grind_schema: String) -> Int {
  let query =
    pog.query(
      "SELECT coalesce(n_dead_tup, 0) FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'grind_jobs'",
    )
    |> pog.parameter(pog.text(grind_schema))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case pog.execute(query, connection) {
    Ok(pog.Returned(rows: [count], ..)) -> count
    _ -> -1
  }
}

fn submitted_job_ids_ordered(
  ledger: pog.Connection,
) -> Result(List(Int), pog.QueryError) {
  let query =
    pog.query("SELECT job_id FROM bench_submissions ORDER BY job_id ASC")
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  pog.execute(query, ledger) |> result.map(fn(returned) { returned.rows })
}

/// A bulk, direct-SQL insert of `count` already-`succeeded` filler rows
/// (never through `postgres.submit`, never tracked in `bench_submissions`)
/// -- L2's own stand-in for "a large table of already-finished jobs"
/// without paying real preload/drain cost for rows nothing ever claims (a
/// terminal-state row is never selected by `attempt.claim_one`). One
/// statement, `generate_series`-driven, rather than a chunked loop.
fn insert_filler_succeeded(connection: pog.Connection, count: Int) -> Nil {
  let query =
    pog.query(
      "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, output, max_attempts, state, available_at, finished_at) "
      <> "SELECT 'l2-filler', 'l2-filler-worker', 'v1', 'v1', '{}'::jsonb, 'v1', '0'::jsonb, 20, 'succeeded', clock_timestamp(), clock_timestamp() "
      <> "FROM generate_series(1, $1)",
    )
    |> pog.parameter(pog.int(count))
  let assert Ok(_) = pog.execute(query, connection)
  Nil
}

/// A reduced audit for a scenario that submits no bench-tracked jobs at all
/// (L2's idle-polling probe) or that legitimately prunes rows mid-run
/// (L5 with the pruner on): I1 (missing/extra job rows), I2 (effect
/// counts), and I5 (acknowledgement correlation) all assume nothing is
/// deleted and every ledger row still has a matching `grind_jobs` row --
/// see `grind_bench/audit`'s own doc comment, "pruning is expected to be
/// off for every scenario this checker runs against." This checks only
/// I3's `[grind, job, quarantined]` counter would already have measured
/// deliberately (skipped here -- the caller reports it, since L5's own
/// pruner-on run and L2's idle probe both expect zero quarantines and
/// L6 does not use this helper at all), I4 (no job left `executing`), I6
/// (ledger-write errors and PostgreSQL log anomalies), and I7 (forwarder
/// drops).
fn reduced_audit_and_report(
  ledger: pog.Connection,
  database: postgres.Database,
  label: String,
  log_lines_before: Int,
) -> Nil {
  let grind_schema = schema_of(database)
  let quarantine_count = counter_value(quarantine_counter)
  let forwarder_drop_count = counter_value(forwarder_drop_counter)
  let ledger_error_count = counter_value(ledger_error_counter)
  let assert Ok(executing) =
    audit.check_no_executing_after_drain(ledger, grind_schema)
  let log_window = postgres_log_window(log_lines_before)
  let log_result = case log_window {
    Ok(window) -> audit.check_postgres_log(window)
    Error(Nil) -> Ok(Nil)
  }
  let _ = postgres.close(database)
  cleanup_schema(ledger, grind_schema)
  let ok =
    quarantine_count == 0
    && forwarder_drop_count == 0
    && ledger_error_count == 0
    && executing == Ok(Nil)
    && log_result == Ok(Nil)
  case ok {
    True ->
      io.println(
        label <> ": reduced audit PASSED (I4/I6/I7; I1/I2/I3/I5 relaxed)",
      )
    False -> {
      io.println(
        label
        <> ": reduced audit FAILED quarantine="
        <> int.to_string(quarantine_count)
        <> " forwarder_drops="
        <> int.to_string(forwarder_drop_count)
        <> " ledger_errors="
        <> int.to_string(ledger_error_count)
        <> " executing_check="
        <> string.inspect(executing)
        <> " log="
        <> string.inspect(log_result),
      )
      halt(1)
    }
  }
}

fn wait_for_n(subject: process.Subject(a), n: Int) -> Nil {
  case n <= 0 {
    True -> Nil
    False -> {
      let assert Ok(_) = process.receive(subject, within: 120_000)
      wait_for_n(subject, n - 1)
    }
  }
}

/// Open-loop submission (L3, L5): `parallelism` independent, unlinked
/// processes each submit their own equal share of `arrival_per_sec *
/// duration_ms / 1000` jobs, paced at roughly `arrival_per_sec /
/// parallelism` jobs/sec each, through the real `postgres.submit` path
/// (never `grind_bench/preload`'s bulk insert -- an open-loop arrival
/// process is exactly the one-row-at-a-time real submission path a real
/// application would use). `parallelism` scales with the target rate (one
/// pacer per ~50/s) so no single pacer process's own submit-call latency
/// caps the achievable rate. Returns the total job count actually
/// submitted (`per_worker * parallelism`, which can be a few jobs short of
/// the nominal `arrival_per_sec * duration_ms / 1000` from integer
/// division -- always used as the caller's own "how many did I actually
/// submit" ground truth, never the nominal target).
fn run_open_loop(
  database: postgres.Database,
  ledger: pog.Connection,
  worker_def: BenchWorker,
  queue_name: String,
  arrival_per_sec: Int,
  duration_ms: Int,
) -> Int {
  let parallelism = int.max(1, arrival_per_sec / 50)
  let total_jobs = arrival_per_sec * duration_ms / 1000
  let per_worker = int.max(1, total_jobs / parallelism)
  let interval_ms = int.max(1, duration_ms / per_worker)
  let done = process.new_subject()
  int_range(parallelism)
  |> list.each(fn(worker_index) {
    let start_index = worker_index * per_worker
    let _ =
      process.spawn_unlinked(fn() {
        open_loop_submit_loop(
          database,
          ledger,
          worker_def,
          queue_name,
          start_index,
          per_worker,
          interval_ms,
        )
        process.send(done, Nil)
      })
    Nil
  })
  wait_for_n(done, parallelism)
  per_worker * parallelism
}

fn open_loop_submit_loop(
  database: postgres.Database,
  ledger: pog.Connection,
  worker_def: BenchWorker,
  queue_name: String,
  index: Int,
  remaining: Int,
  interval_ms: Int,
) -> Nil {
  case remaining <= 0 {
    True -> Nil
    False -> {
      let assert Ok(handle) =
        postgres.submit(
          database,
          queue_name,
          worker_def,
          bench_worker.BenchJob(bench_index: index, cost_ms: 0),
        )
      let job_id = job.id_value(handle)
      let assert Ok(Nil) =
        record_submissions(ledger, [#(index, job_id)], queue_name)
      process.sleep(interval_ms)
      open_loop_submit_loop(
        database,
        ledger,
        worker_def,
        queue_name,
        index + 1,
        remaining - 1,
        interval_ms,
      )
    }
  }
}

// -- L2: polling cost --------------------------------------------------------

/// Idle consumers polling an empty queue (never fed) for a fixed
/// observation window, with `filler_rows` already-`succeeded` rows bulk
/// inserted first (a stand-in for "a large table of finished jobs" -- see
/// `insert_filler_succeeded`) -- the plan's "empty queue then 1M finished
/// rows" reduced to a representative point count and filler-row count for
/// wall-clock feasibility (documented in `docs/PERFORMANCE-EVIDENCE.md`).
/// Relaxes I1/I2/I3(quarantine only, still checks I3's forwarder-adjacent
/// zero-quarantine expectation via the driver counter)/I5: no bench-tracked
/// job is ever submitted, so those checks (which all assume a submitted,
/// ledger-tracked job) are vacuous rather than meaningful here -- see
/// `reduced_audit_and_report`.
fn run_l2(
  consumers: Int,
  interval_ms: Int,
  filler_rows: Int,
  duration_ms: Int,
  repeat: Int,
) -> Nil {
  let harness =
    setup(
      int.max(consumers, 10),
      grind_bench.ledger_pool_size_for_concurrency(consumers),
    )
  let Harness(database:, ledger:, drain:) = harness
  let connection = postgres.connection(database)
  case filler_rows > 0 {
    True -> insert_filler_succeeded(connection, filler_rows)
    False -> Nil
  }
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l2.echo")
  let assert Ok(registry_) = registry.new("l2-probe")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  attach_audit_observers("l2")
  let log_lines_before = postgres_log_lines_before()

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(interval_ms)
    |> queue.with_maximum_concurrency(1)
    |> queue.validate_policy

  let cpu_before = cpu_ms_now()
  let statements_before = statement_split.snapshot(drain)

  let consumers_list =
    int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, registry_, policy)
      c
    })

  process.sleep(duration_ms)

  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  let cpu_after = cpu_ms_now()
  let #(cpu_ms_delta, cpu_pct) =
    result.unwrap(cpu_delta(cpu_before, cpu_after, duration_ms), #(-1, -1.0))
  let deltas =
    statement_split.diff(statements_before, statement_split.snapshot(drain))
  let #(claim_calls, claim_ms) =
    bucket_totals(deltas, statement_split.ClaimOrOtherGrind)
  let #(quarantine_calls, quarantine_ms) =
    bucket_totals(deltas, statement_split.Quarantine)
  let duration_sec = int.to_float(duration_ms) /. 1000.0
  let claims_per_sec = int.to_float(claim_calls) /. duration_sec
  let quarantine_per_sec = int.to_float(quarantine_calls) /. duration_sec

  let row =
    string.join(
      [
        provenance_prefix(),
        int.to_string(repeat),
        int.to_string(consumers),
        int.to_string(interval_ms),
        int.to_string(filler_rows),
        int.to_string(duration_ms),
        int.to_string(claim_calls),
        float.to_string(claim_ms),
        int.to_string(quarantine_calls),
        float.to_string(quarantine_ms),
        float.to_string(claims_per_sec),
        float.to_string(quarantine_per_sec),
        int.to_string(cpu_ms_delta),
        float.to_string(cpu_pct),
      ],
      ",",
    )
  write_row(
    results_dir() <> "/l2.csv",
    provenance_header_prefix()
      <> ",repeat,consumers,interval_ms,filler_rows,duration_ms,claim_calls,claim_total_ms,quarantine_calls,quarantine_total_ms,claims_per_sec,quarantine_per_sec,db_cpu_ms,db_cpu_pct_of_core",
    row,
  )
  io.println("l2 " <> row)
  reduced_audit_and_report(ledger, database, "l2", log_lines_before)
}

// -- L3: open-loop latency ---------------------------------------------------

/// Open-loop arrival at `arrival_per_sec` for `duration_ms`, against a
/// fixed 4-consumer x C10 shape (the plan's own fixed point for this
/// scenario), then a full drain and the standard I1-I7 audit (no
/// relaxation -- this is an ordinary healthy run, just paced by arrival
/// rather than preloaded). Reports per-job insert->finish and
/// start->ack percentiles from `grind_bench/latency`.
fn run_l3(arrival_per_sec: Int, duration_ms: Int, repeat: Int) -> Nil {
  let consumers = 4
  let concurrency = 10
  let total_concurrency = consumers * concurrency
  let harness =
    setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let Harness(database:, ledger:, drain:) = harness
  let queue_name = "l3-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l3.echo")
  let assert Ok(registry_) = registry.new(queue_name)
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  attach_audit_observers("l3")
  let log_lines_before = postgres_log_lines_before()

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(10)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let consumers_list =
    int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, registry_, policy)
      c
    })

  let cpu_before = cpu_ms_now()
  let job_count =
    run_open_loop(
      database,
      ledger,
      worker_def,
      queue_name,
      arrival_per_sec,
      duration_ms,
    )
  assert_submission_count(ledger, job_count)

  let grind_schema = schema_of(database)
  let drained = wait_for_drain(drain, grind_schema, 60_000)
  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  case drained {
    Error(Nil) -> {
      io.println("l3: timed out waiting for drain")
      halt(1)
    }
    Ok(Nil) -> {
      let cpu_after = cpu_ms_now()
      let elapsed_ms = case timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      let #(cpu_ms_delta, cpu_pct) =
        result.unwrap(cpu_delta(cpu_before, cpu_after, elapsed_ms), #(-1, -1.0))
      let assert Ok(insert_finish) =
        latency.insert_to_finish_ms(ledger, grind_schema)
      let assert Ok(start_ack) = latency.start_to_ack_ms(ledger, grind_schema)
      let insert_finish_stats = summarize.percentiles(insert_finish)
      let start_ack_stats = summarize.percentiles(start_ack)
      let row =
        string.join(
          [
            provenance_prefix(),
            int.to_string(repeat),
            int.to_string(arrival_per_sec),
            int.to_string(consumers),
            int.to_string(concurrency),
            int.to_string(duration_ms),
            int.to_string(job_count),
            int.to_string(elapsed_ms),
            percentile_field(insert_finish_stats, "p50"),
            percentile_field(insert_finish_stats, "p99"),
            percentile_field(insert_finish_stats, "max"),
            percentile_field(start_ack_stats, "p50"),
            percentile_field(start_ack_stats, "p99"),
            percentile_field(start_ack_stats, "max"),
            int.to_string(cpu_ms_delta),
            float.to_string(cpu_pct),
          ],
          ",",
        )
      write_row(
        results_dir() <> "/l3.csv",
        provenance_header_prefix()
          <> ",repeat,arrival_per_sec,consumers,concurrency,duration_ms,job_count,elapsed_ms,insert_to_finish_p50,insert_to_finish_p99,insert_to_finish_max,start_to_ack_p50,start_to_ack_p99,start_to_ack_max,db_cpu_ms,db_cpu_pct_of_core",
        row,
      )
      io.println("l3 " <> row)
      run_audit_and_report(ledger, database, "l3", job_count, log_lines_before)
    }
  }
}

// -- L4: unique contention ----------------------------------------------------

type L4Acc {
  L4Acc(
    inserted: Int,
    existing: Int,
    contended: Int,
    other_errors: Int,
    latencies_ms: List(Float),
  )
}

fn merge_l4(a: L4Acc, b: L4Acc) -> L4Acc {
  L4Acc(
    inserted: a.inserted + b.inserted,
    existing: a.existing + b.existing,
    contended: a.contended + b.contended,
    other_errors: a.other_errors + b.other_errors,
    latencies_ms: list.append(a.latencies_ms, b.latencies_ms),
  )
}

fn collect_l4(subject: process.Subject(L4Acc), n: Int, acc: L4Acc) -> L4Acc {
  case n <= 0 {
    True -> acc
    False -> {
      let assert Ok(next) = process.receive(subject, within: 120_000)
      collect_l4(subject, n - 1, merge_l4(acc, next))
    }
  }
}

fn l4_worker(id: String) -> worker.Worker(Int, Int, Nil) {
  let assert Ok(input) = worker.codec(id <> "-input-v1", json.int, decode.int)
  let assert Ok(output) = worker.codec(id <> "-output-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define(id, "v1", input, output, fn(value) { Ok(value) })
  definition
}

fn classify_l4(
  acc: L4Acc,
  outcome: Result(
    submission.Admission(Int, Int, Nil),
    submission.SubmitError(Int, Int, Nil),
  ),
  elapsed_ms: Float,
) -> L4Acc {
  let L4Acc(inserted:, existing:, contended:, other_errors:, latencies_ms:) =
    acc
  let latencies_ms2 = [elapsed_ms, ..latencies_ms]
  case outcome {
    Ok(submission.Inserted(_)) ->
      L4Acc(
        inserted: inserted + 1,
        existing:,
        contended:,
        other_errors:,
        latencies_ms: latencies_ms2,
      )
    Ok(submission.Existing(_)) | Ok(submission.Rescheduled(_)) ->
      L4Acc(
        inserted:,
        existing: existing + 1,
        contended:,
        other_errors:,
        latencies_ms: latencies_ms2,
      )
    Error(submission.AdmissionContended) ->
      L4Acc(
        inserted:,
        existing:,
        contended: contended + 1,
        other_errors:,
        latencies_ms: latencies_ms2,
      )
    Error(_) ->
      L4Acc(
        inserted:,
        existing:,
        contended:,
        other_errors: other_errors + 1,
        latencies_ms: latencies_ms2,
      )
  }
}

fn l4_submit_loop(
  database: postgres.Database,
  worker_def: worker.Worker(Int, Int, Nil),
  queue_name: String,
  policy: unique.Policy(Int),
  key_fn: fn(Int) -> Int,
  start_index: Int,
  remaining: Int,
  acc: L4Acc,
) -> L4Acc {
  case remaining <= 0 {
    True -> acc
    False -> {
      let key = key_fn(start_index)
      let assert Ok(submission_id) =
        submission.submission_id(
          "l4-" <> int.to_string(bump(l4_submission_counter)),
        )
      let before = monotonic_ms()
      let outcome =
        postgres.submit_unique(
          database,
          queue_name,
          submission_id,
          worker_def,
          key,
          submission.Immediately,
          policy,
          unique.KeepExisting,
        )
      let elapsed = int.to_float(monotonic_ms() - before)
      let next_acc = classify_l4(acc, outcome, elapsed)
      l4_submit_loop(
        database,
        worker_def,
        queue_name,
        policy,
        key_fn,
        start_index + 1,
        remaining - 1,
        next_acc,
      )
    }
  }
}

/// Unique-admission contention: `submitters` parallel processes each
/// hammering `submit_unique` (`KeepExisting`, `WhileRetained`,
/// `IncompleteOrSucceeded`) against either 10 "hot" keys (`mode = "hot"`,
/// deterministic `index % 10` -- heavy, sustained contention on a handful
/// of keys) or a pool of up to 10,000 deterministically distinct "cold"
/// keys (`mode = "cold"`, `index` used directly, never repeated within one
/// run -- near-zero contention by construction, the baseline). No consumer
/// runs at all: this scenario measures the admission SQL layer alone, not
/// job execution. "I2 + one row per key": hot mode asserts exactly 10
/// `grind_jobs` rows exist under its own queue no matter how many
/// submitters or submissions ran (`KeepExisting` always keeps the first);
/// cold mode asserts the row count equals the `Inserted` count (no
/// duplicate/lost row for a distinct key).
fn run_l4(
  submitters: Int,
  mode: String,
  total_submissions: Int,
  repeat: Int,
) -> Nil {
  let harness =
    setup(
      int.max(submitters, 10),
      grind_bench.ledger_pool_size_for_concurrency(submitters),
    )
  let Harness(database:, ledger: _ledger, drain:) = harness
  let queue_name = "l4-" <> mode
  let worker_def = l4_worker("bench.l4." <> mode)
  attach_audit_observers("l4-" <> mode)
  let log_lines_before = postgres_log_lines_before()

  let assert Ok(key) =
    unique.selected("l4-key", fn(x) { x }, {
      let assert Ok(codec) = worker.codec("l4-key-v1", json.int, decode.int)
      codec
    })
  let policy =
    unique.policy(
      key,
      unique.WithinQueue,
      unique.while_retained(),
      unique.IncompleteOrSucceeded,
    )
  let key_fn = case mode {
    "hot" -> fn(index: Int) { index % 10 }
    _ -> fn(index: Int) { index }
  }
  let per_submitter = int.max(1, total_submissions / submitters)
  let actual_total = per_submitter * submitters

  let db_raw_path =
    results_dir()
    <> "/raw/l4-"
    <> int.to_string(submitters)
    <> "-"
    <> mode
    <> ".jsonl"
  let db_sampler_pid =
    process.spawn_unlinked(fn() {
      sampler_db.run(postgres.connection(database), db_raw_path, 50, 600)
    })

  let before_ms = monotonic_ms()
  let done = process.new_subject()
  int_range(submitters)
  |> list.each(fn(i) {
    let start_index = i * per_submitter
    let _ =
      process.spawn_unlinked(fn() {
        let acc =
          l4_submit_loop(
            database,
            worker_def,
            queue_name,
            policy,
            key_fn,
            start_index,
            per_submitter,
            L4Acc(0, 0, 0, 0, []),
          )
        process.send(done, acc)
      })
    Nil
  })
  let total_acc = collect_l4(done, submitters, L4Acc(0, 0, 0, 0, []))
  let elapsed_ms = monotonic_ms() - before_ms
  process.kill(db_sampler_pid)

  let L4Acc(inserted:, existing:, contended:, other_errors:, latencies_ms:) =
    total_acc
  let stats = summarize.percentiles(latencies_ms)
  let elapsed_sec = int.to_float(int.max(elapsed_ms, 1)) /. 1000.0
  let admissions_per_sec = int.to_float(actual_total) /. elapsed_sec
  let contended_rate =
    int.to_float(contended) /. int.to_float(int.max(actual_total, 1))
  let row_count = count_jobs_in_queue(postgres.connection(database), queue_name)

  let row =
    string.join(
      [
        provenance_prefix(),
        int.to_string(repeat),
        int.to_string(submitters),
        mode,
        int.to_string(actual_total),
        int.to_string(elapsed_ms),
        int.to_string(inserted),
        int.to_string(existing),
        int.to_string(contended),
        int.to_string(other_errors),
        float.to_string(admissions_per_sec),
        float.to_string(contended_rate),
        percentile_field(stats, "p50"),
        percentile_field(stats, "p99"),
        percentile_field(stats, "max"),
        int.to_string(row_count),
      ],
      ",",
    )
  write_row(
    results_dir() <> "/l4.csv",
    provenance_header_prefix()
      <> ",repeat,submitters,mode,total_submissions,elapsed_ms,inserted,existing,contended,other_errors,admissions_per_sec,contended_rate,latency_p50_ms,latency_p99_ms,latency_max_ms,row_count",
    row,
  )
  io.println("l4 " <> row)
  summarize_field_to_csv(
    db_raw_path,
    "waiting_locks",
    "l4-" <> int.to_string(submitters) <> "-" <> mode,
  )

  let one_row_per_key_ok = case mode {
    "hot" -> row_count == 10
    _ -> row_count == inserted
  }
  let quarantine_count = counter_value(quarantine_counter)
  let forwarder_drop_count = counter_value(forwarder_drop_counter)
  let log_window = postgres_log_window(log_lines_before)
  let log_result = case log_window {
    Ok(window) -> audit.check_postgres_log(window)
    Error(Nil) -> Ok(Nil)
  }
  let grind_schema = schema_of(database)
  let _ = postgres.close(database)
  cleanup_schema(drain, grind_schema)
  case
    one_row_per_key_ok
    && quarantine_count == 0
    && forwarder_drop_count == 0
    && log_result == Ok(Nil)
  {
    True ->
      io.println(
        "l4: reduced audit PASSED (one row per key; I3/I6/I7; no consumer ran, I1/I2/I4/I5 not applicable)",
      )
    False -> {
      io.println(
        "l4: reduced audit FAILED one_row_per_key="
        <> bool_str(one_row_per_key_ok)
        <> " row_count="
        <> int.to_string(row_count)
        <> " quarantine="
        <> int.to_string(quarantine_count)
        <> " forwarder_drops="
        <> int.to_string(forwarder_drop_count)
        <> " log="
        <> string.inspect(log_result),
      )
      halt(1)
    }
  }
}

// -- L5: pruner concurrent -----------------------------------------------------

/// Open-loop load (200/s, the same `run_open_loop` L3 uses, 4xC10) with the
/// supervised pruner either off (`pruner_on = 0`, the control) or on
/// (`pruner_on = 1`: interval 1000ms, limit 10000, `max_age_ms` reduced to
/// 3000ms from the plan's 5s for wall-clock feasibility within this run's
/// own `duration_ms`). After the open-loop submission phase and drain, an
/// extra `max_age_ms + 2000` sleep lets the pruner catch up before dead
/// tuples are read and one direct, timed `postgres.prune_finished` call
/// reports a concrete prune-duration sample. **`pruner_on = 1` relaxes
/// I1/I2/I5** (`reduced_audit_and_report` -- pruning deletes rows the
/// ledger still references, exactly the case `grind_bench/audit`'s own doc
/// comment says this checker assumes never happens); `pruner_on = 0` runs
/// the full, unrelaxed I1-I7 audit as the control.
fn run_l5(pruner_on: Int, duration_ms: Int, repeat: Int) -> Nil {
  let consumers = 4
  let concurrency = 10
  let arrival_per_sec = 200
  let max_age_ms = 3000
  let total_concurrency = consumers * concurrency
  let harness =
    setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let Harness(database:, ledger:, drain:) = harness
  let queue_name = "l5-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l5.echo")
  let assert Ok(registry_) = registry.new(queue_name)
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  attach_audit_observers("l5")
  let log_lines_before = postgres_log_lines_before()

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(10)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let consumers_list =
    int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, registry_, policy)
      c
    })

  let maybe_pruner = case pruner_on == 1 {
    True -> {
      let assert Ok(pruner_policy) =
        pruner.default_policy()
        |> pruner.with_interval(1000)
        |> pruner.with_limit(10_000)
        |> pruner.with_max_age(max_age_ms)
        |> pruner.validate_policy
      let assert Ok(started) = pruner.start(database, pruner_policy)
      Some(started)
    }
    False -> None
  }

  let job_count =
    run_open_loop(
      database,
      ledger,
      worker_def,
      queue_name,
      arrival_per_sec,
      duration_ms,
    )
  assert_submission_count(ledger, job_count)

  let grind_schema = schema_of(database)
  let drained = wait_for_drain(drain, grind_schema, 60_000)
  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  case drained {
    Error(Nil) -> {
      io.println("l5: timed out waiting for drain")
      halt(1)
    }
    Ok(Nil) -> {
      // Latency is read immediately after drain, before pruning (on the
      // `pruner_on = 1` side) gets any chance to delete the very rows these
      // queries join against -- this is the number the on/off comparison
      // is about, so it must be measured on the same footing both ways.
      let assert Ok(insert_finish) =
        latency.insert_to_finish_ms(ledger, grind_schema)
      let assert Ok(start_ack) = latency.start_to_ack_ms(ledger, grind_schema)
      let insert_finish_stats = summarize.percentiles(insert_finish)
      let start_ack_stats = summarize.percentiles(start_ack)

      // Only now let the pruner (if on) catch up, and only take a direct,
      // timed `prune_finished` sample when the pruner is on: doing this on
      // the `pruner_on = 0` control would delete rows the control's own
      // full I1-I7 audit (below) requires to still exist.
      process.sleep(max_age_ms + 2000)
      let #(prune_duration_ms, pruned_now) = case pruner_on == 1 {
        True -> {
          let prune_before = monotonic_ms()
          let prune_report =
            postgres.prune_finished(
              database,
              older_than_ms: max_age_ms,
              limit: 10_000,
            )
          let duration = monotonic_ms() - prune_before
          let jobs = case prune_report {
            Ok(postgres.PruneReport(jobs:)) -> jobs
            Error(_) -> -1
          }
          #(duration, jobs)
        }
        False -> #(-1, 0)
      }
      let dead_tuples =
        dead_tuple_count(postgres.connection(database), grind_schema)
      case maybe_pruner {
        Some(started) -> {
          let _ = pruner.stop(started)
          Nil
        }
        None -> Nil
      }

      let row =
        string.join(
          [
            provenance_prefix(),
            int.to_string(repeat),
            int.to_string(pruner_on),
            int.to_string(duration_ms),
            int.to_string(job_count),
            percentile_field(insert_finish_stats, "p99"),
            percentile_field(start_ack_stats, "p99"),
            int.to_string(dead_tuples),
            int.to_string(prune_duration_ms),
            int.to_string(pruned_now),
          ],
          ",",
        )
      write_row(
        results_dir() <> "/l5.csv",
        provenance_header_prefix()
          <> ",repeat,pruner_on,duration_ms,job_count,insert_to_finish_p99,start_to_ack_p99,dead_tuples,prune_call_duration_ms,pruned_now",
        row,
      )
      io.println("l5 " <> row)
      case pruner_on == 1 {
        True ->
          reduced_audit_and_report(ledger, database, "l5", log_lines_before)
        False ->
          run_audit_and_report(
            ledger,
            database,
            "l5",
            job_count,
            log_lines_before,
          )
      }
    }
  }
}

// -- L6: lease-log/slow-ack instrumentation, T1 and T2 -----------------------

/// T1 (healthy renewal lag under real defaults): one consumer at
/// `concurrency`, `job_count` jobs each costing `cost_ms` (long enough to
/// span at least one `L / 3` renewal interval at the real, unmodified
/// default `L = 30000`/`D = 4000`), the lease-log trigger installed, no
/// slow acks (`K = 0`, healthy). Reports the worst (minimum) observed
/// per-renewal headroom and derives `2L/3 - h` (the renewal-lag proxy the
/// threshold's own derivation uses -- see `src/grind/queue.gleam`'s own
/// `LeaseTooShortForDeadline` doc comment) against the `L / 6` bound. Runs
/// the full, unrelaxed I1-I7 audit (K=0 is a healthy run; nothing should be
/// uncertain or quarantined).
fn run_l6t1(
  concurrency: Int,
  job_count: Int,
  cost_ms: Int,
  repeat: Int,
) -> Nil {
  let l = 30_000
  let harness =
    setup(
      int.max(concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(concurrency),
    )
  let Harness(database:, ledger:, drain:) = harness
  let grind_schema = schema_of(database)
  let connection = postgres.connection(database)
  let assert Ok(Nil) =
    instrumentation.install_lease_log(connection, grind_schema)
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l6t1.echo")
  let assert Ok(registry_) = registry.new("l6t1-q0")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  attach_audit_observers("l6t1")
  let log_lines_before = postgres_log_lines_before()

  preload_and_track(
    database,
    ledger,
    worker_def,
    ["l6t1-q0"],
    job_count,
    cost_ms,
  )
  assert_submission_count(ledger, job_count)
  analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, registry_, policy)

  let waves = job_count / int.max(concurrency, 1) + 2
  let timeout_ms = int.max(120_000, cost_ms * waves)
  let drained = wait_for_drain(drain, grind_schema, timeout_ms)
  let _ = queue.stop(consumer)

  case drained {
    Error(Nil) -> {
      io.println("l6t1: timed out waiting for drain")
      halt(1)
    }
    Ok(Nil) -> {
      let assert Ok(job_ids) = submitted_job_ids_ordered(ledger)
      let assert Ok(headroom_ms) =
        instrumentation.renewal_headroom_ms(ledger, job_ids)
      let stats = summarize.percentiles(headroom_ms)
      let worst_headroom = case stats {
        Ok(summarize.Percentiles(min:, ..)) -> min
        Error(Nil) -> -1.0
      }
      let p99_headroom = case stats {
        Ok(summarize.Percentiles(p99:, ..)) -> p99
        Error(Nil) -> -1.0
      }
      let two_thirds_l = 2.0 *. int.to_float(l) /. 3.0
      let worst_lag = two_thirds_l -. worst_headroom
      let l_over_6 = int.to_float(l) /. 6.0
      let t1_triggered = worst_lag >. l_over_6
      let row =
        string.join(
          [
            provenance_prefix(),
            int.to_string(repeat),
            int.to_string(concurrency),
            int.to_string(job_count),
            int.to_string(cost_ms),
            int.to_string(list.length(headroom_ms)),
            float.to_string(worst_headroom),
            float.to_string(p99_headroom),
            float.to_string(worst_lag),
            float.to_string(l_over_6),
            bool_str(t1_triggered),
          ],
          ",",
        )
      write_row(
        results_dir() <> "/l6_t1.csv",
        provenance_header_prefix()
          <> ",repeat,concurrency,job_count,cost_ms,renewal_samples,worst_headroom_ms,p99_headroom_ms,worst_lag_ms,l_over_6_ms,t1_triggered",
        row,
      )
      io.println("l6t1 " <> row)
      run_audit_and_report(
        ledger,
        database,
        "l6t1",
        job_count,
        log_lines_before,
      )
    }
  }
}

/// T2 (sibling starvation under `K` slow acks): `C = 10` fixed, `L = 6 *
/// d_ms` and handler cost `3 * L`, exactly the plan's own relationships,
/// with `d_ms` scaled down from the real 4000ms default (documented in
/// `docs/PERFORMANCE-EVIDENCE.md`) so the whole scenario finishes in tens
/// of seconds rather than minutes -- the geometry (`L = 6D`, `cost = 3L`,
/// slow-ack delay `0.8D`) is exact, only the absolute unit shrinks. The
/// first `k_slow_acks` submitted job ids (lowest id, i.e. first claimed
/// under FIFO-ish claim ordering) are marked as slow-ack targets, delaying
/// each of their own acknowledgement inserts by `0.8 * d_ms`.
///
/// **This scenario deliberately relaxes I3** (uncertain/quarantine): a slow
/// ack legitimately starves a sibling's renewal and quarantines it -- that
/// is exactly what T2 measures, not a defect (`reduced_l6t2_audit_and_report`,
/// below). It also does not use `wait_for_drain`'s own "every job
/// succeeded" poll: a quarantined job may never reach `succeeded` at all
/// (it stays `uncertain` until an audited replay this scenario never
/// issues), so a fixed, generously bounded sleep is used instead --
/// `cost_ms + k_slow_acks * 0.8d_ms + L + 5000` covers the handler run,
/// every slow ack serialized on the coordinator's own loop, and the next
/// poll tick's quarantine sweep after the longest lease this run uses would
/// have expired.
fn run_l6t2(k_slow_acks: Int, d_ms: Int, repeat: Int) -> Nil {
  let concurrency = 10
  let l = 6 * d_ms
  let cost_ms = 3 * l
  let delay_ms = d_ms * 8 / 10
  let harness =
    setup_with_deadline(
      int.max(concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(concurrency),
      d_ms,
    )
  let Harness(database:, ledger:, drain: _drain) = harness
  let grind_schema = schema_of(database)
  let connection = postgres.connection(database)
  let assert Ok(Nil) =
    instrumentation.install_lease_log(connection, grind_schema)
  let assert Ok(Nil) =
    instrumentation.install_slow_ack(connection, grind_schema, delay_ms)

  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l6t2.echo")
  let assert Ok(registry_) = registry.new("l6t2-q0")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  attach_audit_observers("l6t2")
  let log_lines_before = postgres_log_lines_before()

  preload_and_track(
    database,
    ledger,
    worker_def,
    ["l6t2-q0"],
    concurrency,
    cost_ms,
  )
  assert_submission_count(ledger, concurrency)
  analyze_and_checkpoint(database, ledger)

  let assert Ok(all_job_ids) = submitted_job_ids_ordered(ledger)
  let slow_ids = list.take(all_job_ids, k_slow_acks)
  let non_stalled_ids = list.drop(all_job_ids, k_slow_acks)
  let assert Ok(Nil) = instrumentation.mark_slow_ack_targets(ledger, slow_ids)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(l)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, registry_, policy)

  let wait_ms = cost_ms + k_slow_acks * delay_ms + l + 5000
  process.sleep(wait_ms)
  let _ = queue.stop(consumer)

  let assert Ok(non_stalled_headroom) =
    instrumentation.renewal_headroom_ms(ledger, non_stalled_ids)
  let stats = summarize.percentiles(non_stalled_headroom)
  let min_headroom = case stats {
    Ok(summarize.Percentiles(min:, ..)) -> min
    Error(Nil) -> -1.0
  }
  let l_over_10 = int.to_float(l) /. 10.0
  let assert Ok(non_stalled_quarantined) =
    instrumentation.quarantine_transition_count(ledger, non_stalled_ids)
  let assert Ok(any_quarantined) =
    instrumentation.quarantine_transition_count(ledger, all_job_ids)
  let headroom_breach = min_headroom <. l_over_10
  let t2_triggered = headroom_breach || any_quarantined > 0

  let row =
    string.join(
      [
        provenance_prefix(),
        int.to_string(repeat),
        int.to_string(concurrency),
        int.to_string(k_slow_acks),
        int.to_string(d_ms),
        int.to_string(l),
        int.to_string(cost_ms),
        int.to_string(delay_ms),
        int.to_string(list.length(non_stalled_headroom)),
        float.to_string(min_headroom),
        float.to_string(l_over_10),
        int.to_string(non_stalled_quarantined),
        int.to_string(any_quarantined),
        bool_str(t2_triggered),
      ],
      ",",
    )
  write_row(
    results_dir() <> "/l6_t2.csv",
    provenance_header_prefix()
      <> ",repeat,concurrency,k_slow_acks,d_ms,l_ms,cost_ms,delay_ms,non_stalled_renewal_samples,non_stalled_min_headroom_ms,l_over_10_ms,non_stalled_quarantined,any_quarantined,t2_triggered",
    row,
  )
  io.println("l6t2 " <> row)

  let _ = instrumentation.drop_slow_ack(connection, grind_schema)
  let _ = instrumentation.drop_lease_log(connection, grind_schema)
  reduced_l6t2_audit_and_report(ledger, database, concurrency, log_lines_before)
}

/// L6T2's own audit: relaxes I3 in full (a slow ack may legitimately
/// quarantine any sibling, stalled or not -- that is the scenario, not a
/// defect) and I1b/I5's "every job succeeded" shape (a quarantined job
/// never reaches `succeeded`), but still checks I2 (no lost/duplicated
/// effect), I6 (ledger-write errors, PostgreSQL log anomalies), and I7
/// (forwarder drops) -- none of which a slow ack should affect.
fn reduced_l6t2_audit_and_report(
  ledger: pog.Connection,
  database: postgres.Database,
  expected_job_count: Int,
  log_lines_before: Int,
) -> Nil {
  let grind_schema = schema_of(database)
  let assert Ok(submission_count) =
    audit.check_submission_count(ledger, expected_job_count)
  let assert Ok(effects) = audit.check_effect_counts(ledger, grind_schema)
  let ledger_error_count = counter_value(ledger_error_counter)
  let forwarder_drop_count = counter_value(forwarder_drop_counter)
  let log_window = postgres_log_window(log_lines_before)
  let log_result = case log_window {
    Ok(window) -> audit.check_postgres_log(window)
    Error(Nil) -> Ok(Nil)
  }
  let _ = postgres.close(database)
  cleanup_schema(ledger, grind_schema)
  let ok =
    submission_count == Ok(Nil)
    && effects == Ok(Nil)
    && ledger_error_count == 0
    && forwarder_drop_count == 0
    && log_result == Ok(Nil)
  case ok {
    True ->
      io.println(
        "l6t2: reduced audit PASSED (I2/I6/I7; I1b/I3/I5 relaxed -- slow acks may legitimately produce uncertain/quarantined jobs)",
      )
    False -> {
      io.println(
        "l6t2: reduced audit FAILED submission_count="
        <> string.inspect(submission_count)
        <> " effects="
        <> string.inspect(effects)
        <> " ledger_errors="
        <> int.to_string(ledger_error_count)
        <> " forwarder_drops="
        <> int.to_string(forwarder_drop_count)
        <> " log="
        <> string.inspect(log_result),
      )
      halt(1)
    }
  }
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
