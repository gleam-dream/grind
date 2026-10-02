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
import grind_bench/instrumentation
import grind_bench/latency
import grind_bench/load/arrivals
import grind_bench/load/context
import grind_bench/load/observers
import grind_bench/load/plan_evidence
import grind_bench/load/report
import grind_bench/load/runtime
import grind_bench/load/workload
import grind_bench/statement_split
import grind_bench/summarize
import grind_bench/worker as bench_worker
import pog

/// Absolute arrival slots, bounded submitters, and complete accounting. An
/// overloaded generator writes its denominators before failing the run.
pub fn run_open_loop(
  database: postgres.Database,
  ledger: pog.Connection,
  worker_def: runtime.BenchWorker,
  queue_name: String,
  arrival_per_sec: Int,
  duration_ms: Int,
) -> Int {
  let max_inflight = case runtime.getenv("GRIND_BENCH_MAX_INFLIGHT") {
    Ok(value) -> {
      let assert Ok(limit) = int.parse(value)
      limit
    }
    Error(Nil) -> 256
  }
  let run =
    arrivals.generate(arrival_per_sec, duration_ms, max_inflight, fn(index) {
      case
        postgres.submit(
          database,
          queue_name,
          worker_def,
          bench_worker.BenchJob(index, 0),
        )
      {
        Ok(handle) ->
          workload.record_submissions(
            ledger,
            [#(index, job.id_value(handle))],
            queue_name,
          )
          |> result.map_error(fn(_) { Nil })
        Error(_) -> Error(Nil)
      }
    })
  let stats = summarize.percentiles(run.lags_ms)
  let max_lag = case stats {
    Ok(summarize.Percentiles(max:, ..)) -> max
    Error(Nil) -> 0.0
  }
  let lag_valid = max_lag <=. int.to_float(int.max(20, duration_ms / 50))
  let generator_valid = run.capacity_limited == 0 && lag_valid
  let complete =
    run.scheduled == run.dispatched + run.capacity_limited
    && run.dispatched == run.admitted + run.failed + run.unfinished
  let status = case generator_valid, run.failed == 0 && run.unfinished == 0 {
    False, _ -> "generator_limited"
    True, False -> "admission_failed"
    True, True -> "valid"
  }
  report.write_row(
    runtime.results_dir() <> "/arrivals.csv",
    runtime.provenance_header_prefix()
      <> ",queue,offered_per_sec,duration_ms,scheduled,dispatched,admitted,failed_or_unrecorded,unfinished,capacity_limited,max_inflight,max_outstanding,offered_elapsed_ms,generator_lag_p99_ms,generator_lag_max_ms,generator_valid,status",
    string.join(
      [
        runtime.provenance_prefix(),
        queue_name,
        int.to_string(arrival_per_sec),
        int.to_string(duration_ms),
        int.to_string(run.scheduled),
        int.to_string(run.dispatched),
        int.to_string(run.admitted),
        int.to_string(run.failed),
        int.to_string(run.unfinished),
        int.to_string(run.capacity_limited),
        int.to_string(max_inflight),
        int.to_string(run.max_outstanding),
        int.to_string(run.offered_elapsed_ms),
        report.percentile_field(stats, "p99"),
        float.to_string(max_lag),
        report.bool_str(generator_valid),
        status,
      ],
      ",",
    ),
  )
  io.println(
    "arrivals "
    <> status
    <> " scheduled="
    <> int.to_string(run.scheduled)
    <> " dispatched="
    <> int.to_string(run.dispatched)
    <> " admitted="
    <> int.to_string(run.admitted)
    <> " capacity_limited="
    <> int.to_string(run.capacity_limited),
  )
  let assert True =
    complete && generator_valid && run.failed == 0 && run.unfinished == 0
  run.admitted
}

// -- L2: polling cost --------------------------------------------------------

/// Idle pollers against zero, 100k or a million retained rows. Report call counts
/// and execution time per call separately; constant counts do not establish
/// constant scan cost. This scenario submits no bench-tracked jobs.
pub fn run_l2(
  consumers: Int,
  interval_ms: Int,
  filler_rows: Int,
  duration_ms: Int,
  repeat: Int,
) -> Nil {
  let harness =
    context.setup_without_completion_observer(
      int.max(consumers, 10),
      grind_bench.ledger_pool_size_for_concurrency(consumers),
    )
  let context.Harness(database:, ledger:, drain:, ..) = harness
  let connection = postgres.connection(database)
  case filler_rows > 0 {
    True -> report.insert_filler_succeeded(connection, filler_rows)
    False -> Nil
  }
  workload.analyze_and_checkpoint(database, ledger)
  let actual_rows = plan_evidence.count_rows(connection)
  let assert True =
    actual_rows.total == filler_rows
    && actual_rows.retained_succeeded == filler_rows
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l2.echo")
  let assert Ok(registry_) = registry.new("l2-probe")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers()
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
  // Timing/statistics snapshots have ended. Probe the same table with
  // PostgreSQL's runtime EXPLAIN instrumentation only after measurement.
  let plan_path =
    plan_evidence.capture(database, registry_, filler_rows, actual_rows, repeat)

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
        float.to_string(claim_ms /. int.to_float(int.max(claim_calls, 1))),
        float.to_string(
          quarantine_ms /. int.to_float(int.max(quarantine_calls, 1)),
        ),
        float.to_string(claims_per_sec),
        float.to_string(quarantine_per_sec),
        int.to_string(cpu_ms_delta),
        float.to_string(cpu_pct),
        int.to_string(actual_rows.total),
        int.to_string(actual_rows.retained_succeeded),
        int.to_string(int.max(consumers, 10)),
        int.to_string(consumers),
        int.to_string(int.max(consumers, 10) + consumers),
        plan_path,
      ],
      ",",
    )
  report.write_row(
    runtime.results_dir() <> "/l2.csv",
    runtime.provenance_header_prefix()
      <> ",repeat,consumers,interval_ms,filler_rows,duration_ms,claim_calls,claim_total_ms,quarantine_calls,quarantine_total_ms,claim_mean_ms,quarantine_mean_ms,claims_per_sec,quarantine_per_sec,db_cpu_ms,db_cpu_pct_of_core,actual_total_rows,actual_retained_succeeded_rows,main_pool_size,reserved_renewal_connections,total_grind_connections,untimed_plan_path",
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
/// start->ack percentiles plus handler duration, claim-to-start latency and
/// independently observed durable-ACK latency. Observation adds up to the
/// sampling/checkout lag; SQL timestamps alone are not COMMIT timestamps.
pub fn run_l3(arrival_per_sec: Int, duration_ms: Int, repeat: Int) -> Nil {
  let consumers = 4
  let concurrency = 10
  let total_concurrency = consumers * concurrency
  let harness =
    context.setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let context.Harness(database:, ledger:, drain:, ..) = harness
  let assert Ok(Nil) =
    instrumentation.install_lease_log(
      postgres.connection(database),
      context.schema_of(database),
    )
  let queue_name = "l3-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l3.echo")
  let assert Ok(registry_) = registry.new(queue_name)
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()

  let assert Ok(policy) =
    queue.default_policy()
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
  context.stop_observer(harness)
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
      let assert Ok(claim_start) = latency.claim_to_start_ms(ledger)
      let assert Ok(handler_duration) = latency.handler_duration_ms(ledger)
      let assert Ok(observed_ack) =
        latency.insert_to_observed_ack_ms(ledger, grind_schema)
      let assert True =
        list.length(claim_start) == job_count
        && list.length(handler_duration) == job_count
        && list.length(observed_ack) == job_count
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
            report.percentile_field(summarize.percentiles(claim_start), "p99"),
            report.percentile_field(
              summarize.percentiles(handler_duration),
              "p99",
            ),
            report.percentile_field(summarize.percentiles(observed_ack), "p99"),
            int.to_string(cpu_ms_delta),
            float.to_string(cpu_pct),
            int.to_string(list.length(observed_ack)),
            int.to_string(int.max(total_concurrency, 10)),
            int.to_string(consumers),
            int.to_string(int.max(total_concurrency, 10) + consumers),
          ],
          ",",
        )
      report.write_row(
        runtime.results_dir() <> "/l3.csv",
        runtime.provenance_header_prefix()
          <> ",repeat,arrival_per_sec,consumers,concurrency,duration_ms,job_count,elapsed_ms,insert_to_sql_finished_at_p50_ms,insert_to_sql_finished_at_p99_ms,insert_to_sql_finished_at_max_ms,handler_start_to_sql_receipt_timestamp_p50_ms,handler_start_to_sql_receipt_timestamp_p99_ms,handler_start_to_sql_receipt_timestamp_max_ms,claim_to_start_p99,handler_duration_p99,insert_to_observed_ack_p99,db_cpu_ms,db_cpu_pct_of_core,durable_completion_count,main_pool_size,reserved_renewal_connections,total_grind_connections",
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
