import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/result
import gleam/string
import grind/postgres
import grind/queue
import grind/registry
import grind_bench
import grind_bench/load/context
import grind_bench/load/drain as drain_measurement
import grind_bench/load/observers
import grind_bench/load/report
import grind_bench/load/runtime
import grind_bench/load/workload
import grind_bench/sampler_beam
import grind_bench/sampler_db
import grind_bench/statement_split
import grind_bench/summarize
import grind_bench/worker as bench_worker

pub fn run_smoke(job_count: Int) -> Nil {
  io.println("grind_bench smoke: " <> int.to_string(job_count) <> " jobs")
  let harness =
    context.setup(10, grind_bench.ledger_pool_size_for_concurrency(10))
  let context.Harness(database:, ledger:, drain:, ..) = harness
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.smoke.echo")
  let assert Ok(workers) = registry.new("bench-smoke")
  let assert Ok(workers) = registry.register(workers, worker_def)
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()

  workload.preload_and_track(
    database,
    ledger,
    worker_def,
    ["bench-smoke"],
    job_count,
    0,
  )
  workload.assert_submission_count(ledger, job_count)
  workload.analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_concurrency(10)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)

  let grind_schema = context.schema_of(database)
  let drained = workload.wait_for_drain(drain, grind_schema, 30_000)
  context.stop_observer(harness)
  let _ = queue.stop(consumer)

  case drained {
    Error(Nil) -> {
      io.println("smoke: timed out waiting for drain")
      let _ = postgres.close(database)
      runtime.halt(1)
    }
    Ok(Nil) -> {
      let elapsed_ms = case report.timestamps_ms(ledger, grind_schema) {
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
      report.run_audit_and_report(
        ledger,
        database,
        "smoke",
        job_count,
        log_lines_before,
      )
    }
  }
}

pub fn run_l1(
  job_count: Int,
  consumers: Int,
  concurrency: Int,
  queues_n: Int,
  cost_ms: Int,
  repeat: Int,
) -> Nil {
  let total_concurrency = consumers * concurrency
  let harness =
    context.setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let context.Harness(database:, ledger:, drain:, ..) = harness
  let queue_names =
    runtime.int_range(queues_n)
    |> list.map(fn(i) { "l1-q" <> int.to_string(i) })
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l1.echo")
  let registries =
    list.map(queue_names, fn(name) {
      let assert Ok(r) = registry.new(name)
      let assert Ok(r) = registry.register(r, worker_def)
      r
    })
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()
  workload.preload_and_track(
    database,
    ledger,
    worker_def,
    queue_names,
    job_count,
    cost_ms,
  )
  workload.assert_submission_count(ledger, job_count)
  workload.analyze_and_checkpoint(database, ledger)

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
    <> "-r"
    <> int.to_string(repeat)
  let beam_raw_path =
    runtime.results_dir() <> "/raw/l1-" <> label <> "-beam.jsonl"
  let db_raw_path = runtime.results_dir() <> "/raw/l1-" <> label <> "-db.jsonl"

  let cpu_before = report.cpu_ms_now()
  let statements_before = statement_split.snapshot(drain)

  let consumers_list =
    runtime.int_range(consumers)
    |> list.map(fn(i) {
      let r = runtime.list_at_or_panic(registries, i % queues_n)
      let assert Ok(c) = queue.start(database, r, policy)
      c
    })
  let beam_sampler_pid =
    process.spawn_unlinked(fn() { sampler_beam.run(beam_raw_path, 1000, 300) })
  let db_sampler_pid =
    process.spawn_unlinked(fn() {
      sampler_db.run(drain, db_raw_path, 1000, 300)
    })

  let grind_schema = context.schema_of(database)
  let drained = workload.wait_for_drain(drain, grind_schema, 60_000)
  context.stop_observer(harness)
  process.kill(beam_sampler_pid)
  process.kill(db_sampler_pid)
  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  case drained {
    Error(Nil) -> {
      io.println("l1: timed out waiting for drain")
      runtime.halt(1)
    }
    Ok(Nil) -> {
      let elapsed_ms = case report.timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      let cpu_after = report.cpu_ms_now()
      let #(cpu_ms_delta, cpu_pct) =
        result.unwrap(report.cpu_delta(cpu_before, cpu_after, elapsed_ms), #(
          -1,
          -1.0,
        ))
      let jobs_per_sec = case elapsed_ms > 0 {
        True ->
          int.to_float(job_count) /. { int.to_float(elapsed_ms) /. 1000.0 }
        False -> 0.0
      }
      let row =
        string.join(
          [
            runtime.provenance_prefix(),
            int.to_string(repeat),
            int.to_string(consumers),
            int.to_string(concurrency),
            int.to_string(queues_n),
            int.to_string(cost_ms),
            int.to_string(job_count),
            int.to_string(elapsed_ms),
            float.to_string(jobs_per_sec),
            int.to_string(int.max(total_concurrency, 10)),
            int.to_string(cpu_ms_delta),
            float.to_string(cpu_pct),
            int.to_string(int.max(total_concurrency, 10)),
            int.to_string(consumers),
            int.to_string(int.max(total_concurrency, 10) + consumers),
          ],
          ",",
        )
      report.write_row(
        runtime.results_dir() <> "/l1.csv",
        runtime.provenance_header_prefix()
          <> ",repeat,consumers,concurrency,queues,cost_ms,job_count,elapsed_ms,jobs_per_sec,pool_size,db_cpu_ms,db_cpu_pct_of_core,main_pool_size,reserved_renewal_connections,total_grind_connections",
        row,
      )
      io.println("l1 " <> row)
      report.write_statement_split_rows(
        "l1-" <> label,
        statements_before,
        statement_split.snapshot(drain),
      )
      report.summarize_field_to_csv(
        beam_raw_path,
        "process_count",
        "l1-" <> label <> "-beam",
      )
      report.summarize_field_to_csv(
        beam_raw_path,
        "memory_total_bytes",
        "l1-" <> label <> "-beam",
      )
      report.summarize_field_to_csv(
        beam_raw_path,
        "reductions",
        "l1-" <> label <> "-beam",
      )
      report.summarize_field_to_csv(
        db_raw_path,
        "waiting_locks",
        "l1-" <> label <> "-db",
      )
      report.run_audit_and_report(
        ledger,
        database,
        "l1",
        job_count,
        log_lines_before,
      )
    }
  }
}

pub fn run_l7(
  job_count: Int,
  consumers: Int,
  concurrency: Int,
  repeat: Int,
) -> Nil {
  let drain_timeout_ms = runtime.drain_timeout_ms()
  let total_concurrency = consumers * concurrency
  let harness =
    context.setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let context.Harness(database:, ledger:, ..) = harness
  let queue_name = "l7-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l7.echo")
  let assert Ok(r) = registry.new(queue_name)
  let assert Ok(r) = registry.register(r, worker_def)
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()
  workload.preload_and_track(
    database,
    ledger,
    worker_def,
    [queue_name],
    job_count,
    1,
  )
  workload.assert_submission_count(ledger, job_count)
  workload.analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(5)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy

  let cpu_before = report.cpu_ms_now()

  let startup_started_ms = runtime.monotonic_ms()
  let consumers_list =
    runtime.int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, r, policy)
      c
    })
  let coordinator_pids = list.filter_map(consumers_list, queue.coordinator_pid)

  let raw_path =
    runtime.results_dir()
    <> "/raw/l7-"
    <> int.to_string(consumers)
    <> "x"
    <> int.to_string(concurrency)
    <> "-r"
    <> int.to_string(repeat)
    <> ".jsonl"
  let grind_schema = context.schema_of(database)
  let drained =
    drain_measurement.run(
      harness,
      consumers_list,
      drain_measurement.Config(
        raw_path:,
        interval_ms: 20,
        timeout_ms: drain_timeout_ms,
        expected_jobs: job_count,
        startup_started_ms:,
        sample: fn() {
          [
            #(
              "message_queue_len_max",
              json.int(list.fold(
                list.map(coordinator_pids, runtime.message_queue_len),
                0,
                int.max,
              )),
            ),
          ]
        },
      ),
    )

  case drained {
    Error(Nil) -> {
      io.println("l7: drain failed; see " <> raw_path <> ".drain.json")
      runtime.halt(1)
    }
    Ok(Nil) -> {
      let elapsed_ms = case report.timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      let cpu_after = report.cpu_ms_now()
      let #(cpu_ms_delta, cpu_pct) =
        result.unwrap(report.cpu_delta(cpu_before, cpu_after, elapsed_ms), #(
          -1,
          -1.0,
        ))
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
            runtime.provenance_prefix(),
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
            int.to_string(int.max(total_concurrency, 10)),
            int.to_string(consumers),
            int.to_string(int.max(total_concurrency, 10) + consumers),
          ],
          ",",
        )
      report.write_row(
        runtime.results_dir() <> "/l7.csv",
        runtime.provenance_header_prefix()
          <> ",repeat,consumers,concurrency,job_count,elapsed_ms,jobs_per_sec,coordinator_mqlen_p50,coordinator_mqlen_p99,db_cpu_ms,db_cpu_pct_of_core,main_pool_size,reserved_renewal_connections,total_grind_connections",
        row,
      )
      io.println("l7 " <> row)
      report.run_audit_and_report(
        ledger,
        database,
        "l7",
        job_count,
        log_lines_before,
      )
    }
  }
}
