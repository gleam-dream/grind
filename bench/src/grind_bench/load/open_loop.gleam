import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import gleam/string
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind_bench
import grind_bench/latency
import grind_bench/load/context
import grind_bench/load/observers
import grind_bench/load/report
import grind_bench/load/runtime
import grind_bench/load/workload
import grind_bench/statement_split
import grind_bench/summarize
import grind_bench/worker as bench_worker
import pog

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
pub fn run_open_loop(
  database: postgres.Database,
  ledger: pog.Connection,
  worker_def: runtime.BenchWorker,
  queue_name: String,
  arrival_per_sec: Int,
  duration_ms: Int,
) -> Int {
  let parallelism = int.max(1, arrival_per_sec / 50)
  let total_jobs = arrival_per_sec * duration_ms / 1000
  let per_worker = int.max(1, total_jobs / parallelism)
  let interval_ms = int.max(1, duration_ms / per_worker)
  let done = process.new_subject()
  runtime.int_range(parallelism)
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
  worker_def: runtime.BenchWorker,
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
        workload.record_submissions(ledger, [#(index, job_id)], queue_name)
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
pub fn run_l2(
  consumers: Int,
  interval_ms: Int,
  filler_rows: Int,
  duration_ms: Int,
  repeat: Int,
) -> Nil {
  let harness =
    context.setup(
      int.max(consumers, 10),
      grind_bench.ledger_pool_size_for_concurrency(consumers),
    )
  let context.Harness(database:, ledger:, drain:) = harness
  let connection = postgres.connection(database)
  case filler_rows > 0 {
    True -> report.insert_filler_succeeded(connection, filler_rows)
    False -> Nil
  }
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l2.echo")
  let assert Ok(registry_) = registry.new("l2-probe")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers("l2")
  let log_lines_before = report.postgres_log_lines_before()

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(interval_ms)
    |> queue.with_maximum_concurrency(1)
    |> queue.validate_policy

  let cpu_before = report.cpu_ms_now()
  let statements_before = statement_split.snapshot(drain)

  let consumers_list =
    runtime.int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, registry_, policy)
      c
    })

  process.sleep(duration_ms)

  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  let cpu_after = report.cpu_ms_now()
  let #(cpu_ms_delta, cpu_pct) =
    result.unwrap(report.cpu_delta(cpu_before, cpu_after, duration_ms), #(
      -1,
      -1.0,
    ))
  let deltas =
    statement_split.diff(statements_before, statement_split.snapshot(drain))
  let #(claim_calls, claim_ms) =
    report.bucket_totals(deltas, statement_split.ClaimOrOtherGrind)
  let #(quarantine_calls, quarantine_ms) =
    report.bucket_totals(deltas, statement_split.Quarantine)
  let duration_sec = int.to_float(duration_ms) /. 1000.0
  let claims_per_sec = int.to_float(claim_calls) /. duration_sec
  let quarantine_per_sec = int.to_float(quarantine_calls) /. duration_sec

  let row =
    string.join(
      [
        runtime.provenance_prefix(),
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
  report.write_row(
    runtime.results_dir() <> "/l2.csv",
    runtime.provenance_header_prefix()
      <> ",repeat,consumers,interval_ms,filler_rows,duration_ms,claim_calls,claim_total_ms,quarantine_calls,quarantine_total_ms,claims_per_sec,quarantine_per_sec,db_cpu_ms,db_cpu_pct_of_core",
    row,
  )
  io.println("l2 " <> row)
  report.reduced_audit_and_report(ledger, database, "l2", log_lines_before)
}

// -- L3: open-loop latency ---------------------------------------------------

/// Open-loop arrival at `arrival_per_sec` for `duration_ms`, against a
/// fixed 4-consumer x C10 shape (the plan's own fixed point for this
/// scenario), then a full drain and the standard I1-I7 audit (no
/// relaxation -- this is an ordinary healthy run, just paced by arrival
/// rather than preloaded). Reports per-job insert->finish and
/// start->ack percentiles from `grind_bench/latency`.
pub fn run_l3(arrival_per_sec: Int, duration_ms: Int, repeat: Int) -> Nil {
  let consumers = 4
  let concurrency = 10
  let total_concurrency = consumers * concurrency
  let harness =
    context.setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let context.Harness(database:, ledger:, drain:) = harness
  let queue_name = "l3-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l3.echo")
  let assert Ok(registry_) = registry.new(queue_name)
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers("l3")
  let log_lines_before = report.postgres_log_lines_before()

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(10)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let consumers_list =
    runtime.int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, registry_, policy)
      c
    })

  let cpu_before = report.cpu_ms_now()
  let job_count =
    run_open_loop(
      database,
      ledger,
      worker_def,
      queue_name,
      arrival_per_sec,
      duration_ms,
    )
  workload.assert_submission_count(ledger, job_count)

  let grind_schema = context.schema_of(database)
  let drained = workload.wait_for_drain(drain, grind_schema, 60_000)
  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  case drained {
    Error(Nil) -> {
      io.println("l3: timed out waiting for drain")
      runtime.halt(1)
    }
    Ok(Nil) -> {
      let cpu_after = report.cpu_ms_now()
      let elapsed_ms = case report.timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      let #(cpu_ms_delta, cpu_pct) =
        result.unwrap(report.cpu_delta(cpu_before, cpu_after, elapsed_ms), #(
          -1,
          -1.0,
        ))
      let assert Ok(insert_finish) =
        latency.insert_to_finish_ms(ledger, grind_schema)
      let assert Ok(start_ack) = latency.start_to_ack_ms(ledger, grind_schema)
      let insert_finish_stats = summarize.percentiles(insert_finish)
      let start_ack_stats = summarize.percentiles(start_ack)
      let row =
        string.join(
          [
            runtime.provenance_prefix(),
            int.to_string(repeat),
            int.to_string(arrival_per_sec),
            int.to_string(consumers),
            int.to_string(concurrency),
            int.to_string(duration_ms),
            int.to_string(job_count),
            int.to_string(elapsed_ms),
            report.percentile_field(insert_finish_stats, "p50"),
            report.percentile_field(insert_finish_stats, "p99"),
            report.percentile_field(insert_finish_stats, "max"),
            report.percentile_field(start_ack_stats, "p50"),
            report.percentile_field(start_ack_stats, "p99"),
            report.percentile_field(start_ack_stats, "max"),
            int.to_string(cpu_ms_delta),
            float.to_string(cpu_pct),
          ],
          ",",
        )
      report.write_row(
        runtime.results_dir() <> "/l3.csv",
        runtime.provenance_header_prefix()
          <> ",repeat,arrival_per_sec,consumers,concurrency,duration_ms,job_count,elapsed_ms,insert_to_finish_p50,insert_to_finish_p99,insert_to_finish_max,start_to_ack_p50,start_to_ack_p99,start_to_ack_max,db_cpu_ms,db_cpu_pct_of_core",
        row,
      )
      io.println("l3 " <> row)
      report.run_audit_and_report(
        ledger,
        database,
        "l3",
        job_count,
        log_lines_before,
      )
    }
  }
}
