import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/result
import gleam/string
import grind/postgres
import grind/worker
import grind_bench/audit
import grind_bench/load/runtime
import grind_bench/preload
import grind_bench/worker as bench_worker
import pog

fn worker_versions(worker_def: runtime.BenchWorker) -> worker.Metadata {
  worker.metadata(worker_def)
}

/// Preloads `job_count` jobs, round-robin distributed across `queues`, and
/// records `bench_submissions` for every preloaded row -- item 2, bullet 1:
/// any `record_submissions` insert failure now fails the whole run (via
/// `let assert`) rather than being swallowed.
pub fn preload_and_track(
  database: postgres.Database,
  ledger: pog.Connection,
  worker_def: runtime.BenchWorker,
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
  runtime.int_range(queues_count)
  |> list.each(fn(queue_index) {
    let queue_name = runtime.list_at_or_panic(queues, queue_index)
    let indices =
      runtime.int_range(job_count)
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
pub fn record_submissions(
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
pub fn assert_submission_count(ledger: pog.Connection, job_count: Int) -> Nil {
  case audit.check_submission_count(ledger, job_count) {
    Ok(Ok(Nil)) -> Nil
    other -> {
      io.println(
        "grind_bench/load: submission count mismatch after preload: "
        <> string.inspect(other),
      )
      runtime.halt(1)
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
pub fn analyze_and_checkpoint(
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

pub fn wait_for_drain(
  drain: pog.Connection,
  grind_schema: String,
  timeout_ms: Int,
) -> Result(Nil, Nil) {
  poll_drain(drain, grind_schema, runtime.monotonic_ms() + timeout_ms)
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
      case runtime.monotonic_ms() >= deadline {
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
