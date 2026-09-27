import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/string
import grind/postgres
import grind/submission
import grind/unique
import grind/worker
import grind_bench
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
pub fn run_l4(
  submitters: Int,
  mode: String,
  total_submissions: Int,
  repeat: Int,
) -> Nil {
  let harness =
    context.setup(
      int.max(submitters, 10),
      grind_bench.ledger_pool_size_for_concurrency(submitters),
    )
  let context.Harness(database:, ledger: _ledger, drain:) = harness
  let queue_name = "l4-" <> mode
  let worker_def = l4_worker("bench.l4." <> mode)
  observers.attach_audit_observers("l4-" <> mode)
  let log_lines_before = report.postgres_log_lines_before()

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
    runtime.results_dir()
    <> "/raw/l4-"
    <> int.to_string(submitters)
    <> "-"
    <> mode
    <> ".jsonl"
  let db_sampler_pid =
    process.spawn_unlinked(fn() {
      sampler_db.run(postgres.connection(database), db_raw_path, 50, 600)
    })

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
      ],
      ",",
    )
  report.write_row(
    runtime.results_dir() <> "/l4.csv",
    runtime.provenance_header_prefix()
      <> ",repeat,submitters,mode,total_submissions,elapsed_ms,inserted,existing,contended,other_errors,admissions_per_sec,contended_rate,latency_p50_ms,latency_p99_ms,latency_max_ms,row_count",
    row,
  )
  io.println("l4 " <> row)
  report.summarize_field_to_csv(
    db_raw_path,
    "waiting_locks",
    "l4-" <> int.to_string(submitters) <> "-" <> mode,
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
