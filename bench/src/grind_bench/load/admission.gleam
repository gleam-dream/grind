import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/string
import grind/postgres
import grind/queue
import grind/registry
import grind/submission
import grind/unique
import grind/worker
import grind_bench/audit
import grind_bench/load/context
import grind_bench/load/observers
import grind_bench/load/report
import grind_bench/load/runtime
import grind_bench/sampler_db
import grind_bench/summarize

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
  let assert Ok(input) =
    worker.codec(id <> "-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output) =
    worker.codec(id <> "-output-v1", worker.infallible(json.int), decode.int)
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
          "l4-" <> int.to_string(runtime.bump(runtime.l4_submission_counter)),
        )
      let before = runtime.monotonic_ms()
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
      let elapsed = int.to_float(runtime.monotonic_ms() - before)
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

/// Hot/cold admission comparison at a fixed pool size of 80 and a live C10
/// consumer. The observer uses its own connection and requires at least 30
/// lock samples. Latency, CPU and actual lock waits are separate evidence;
/// increasing latency alone is never labelled advisory-lock contention.
pub fn run_l4(
  submitters: Int,
  mode: String,
  total_submissions: Int,
  repeat: Int,
) -> Nil {
  let harness = context.setup_without_completion_observer(80, 8)
  let context.Harness(database:, ledger: _ledger, drain:, ..) = harness
  let queue_name = "l4-" <> mode
  let worker_def = l4_worker("bench.l4." <> mode)
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, worker_def)
  let assert Ok(consumer_policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(10)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, consumer_policy)

  let assert Ok(key) =
    unique.selected("l4-key", fn(x) { x }, {
      let assert Ok(codec) =
        worker.codec("l4-key-v1", worker.infallible(json.int), decode.int)
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
    runtime.results_dir()
    <> "/raw/l4-"
    <> int.to_string(submitters)
    <> "-"
    <> mode
    <> "-r"
    <> int.to_string(repeat)
    <> ".jsonl"
  let db_sampler_pid =
    process.spawn_unlinked(fn() {
      sampler_db.run(drain, db_raw_path, 10, 60_000)
    })

  let cpu_before = report.cpu_ms_now()
  let before_ms = runtime.monotonic_ms()
  let done = process.new_subject()
  runtime.int_range(submitters)
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
  let elapsed_ms = runtime.monotonic_ms() - before_ms
  process.kill(db_sampler_pid)
  let _ = queue.stop(consumer)
  let assert Ok(lock_samples) =
    summarize.read_field_values(db_raw_path, "waiting_locks")
  let assert True =
    list.length(lock_samples) >= 30
    && list.all(lock_samples, fn(value) { value >=. 0.0 })
  let cpu_after = report.cpu_ms_now()
  let #(cpu_ms, cpu_pct) = case
    report.cpu_delta(cpu_before, cpu_after, elapsed_ms)
  {
    Ok(value) -> value
    Error(Nil) -> #(-1, -1.0)
  }

  let L4Acc(inserted:, existing:, contended:, other_errors:, latencies_ms:) =
    total_acc
  let stats = summarize.percentiles(latencies_ms)
  let elapsed_sec = int.to_float(int.max(elapsed_ms, 1)) /. 1000.0
  let admissions_per_sec = int.to_float(actual_total) /. elapsed_sec
  let contended_rate =
    int.to_float(contended) /. int.to_float(int.max(actual_total, 1))
  let row_count =
    report.count_jobs_in_queue(postgres.connection(database), queue_name)

  let row =
    string.join(
      [
        runtime.provenance_prefix(),
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
        report.percentile_field(stats, "p50"),
        report.percentile_field(stats, "p99"),
        report.percentile_field(stats, "max"),
        int.to_string(row_count),
        "80",
        "10",
        int.to_string(list.length(lock_samples)),
        int.to_string(cpu_ms),
        float.to_string(cpu_pct),
        "80",
        "1",
        "81",
      ],
      ",",
    )
  report.write_row(
    runtime.results_dir() <> "/l4.csv",
    runtime.provenance_header_prefix()
      <> ",repeat,submitters,mode,total_submissions,elapsed_ms,inserted,existing,contended,other_errors,admissions_per_sec,contended_rate,latency_p50_ms,latency_p99_ms,latency_max_ms,row_count,pool_size,consumer_concurrency,lock_samples,db_cpu_ms,db_cpu_pct,main_pool_size,reserved_renewal_connections,total_grind_connections",
    row,
  )
  io.println("l4 " <> row)
  report.summarize_field_to_csv(
    db_raw_path,
    "waiting_locks",
    "l4-"
      <> int.to_string(submitters)
      <> "-"
      <> mode
      <> "-r"
      <> int.to_string(repeat),
  )

  let one_row_per_key_ok = case mode {
    "hot" -> row_count == 10
    _ -> row_count == inserted
  }
  let quarantine_count = runtime.counter_value(runtime.quarantine_counter)
  let forwarder_drop_count =
    runtime.counter_value(runtime.forwarder_drop_counter)
  let log_window = report.postgres_log_window(log_lines_before)
  let log_result = case log_window {
    Ok(window) -> audit.check_postgres_log(window)
    Error(Nil) -> Ok(Nil)
  }
  let grind_schema = context.schema_of(database)
  let _ = postgres.close(database)
  context.cleanup_schema(drain, grind_schema)
  case
    one_row_per_key_ok
    && other_errors == 0
    && quarantine_count == 0
    && forwarder_drop_count == 0
    && log_result == Ok(Nil)
  {
    True ->
      io.println(
        "l4: reduced audit PASSED (one row per key; I3/I6/I7; consumer active; admission identity audit, I1/I2/I4/I5 not asserted)",
      )
    False -> {
      io.println(
        "l4: reduced audit FAILED one_row_per_key="
        <> report.bool_str(one_row_per_key_ok)
        <> " row_count="
        <> int.to_string(row_count)
        <> " quarantine="
        <> int.to_string(quarantine_count)
        <> " forwarder_drops="
        <> int.to_string(forwarder_drop_count)
        <> " log="
        <> string.inspect(log_result),
      )
      runtime.halt(1)
    }
  }
}
// -- L5: pruner concurrent -----------------------------------------------------
