import gleam/dynamic/decode
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/result
import gleam/string
import grind/internal/postgres
import grind_bench/audit
import grind_bench/harness_db
import grind_bench/load/context
import grind_bench/load/runtime
import grind_bench/statement_split
import grind_bench/summarize
import pog
import simplifile

pub fn timestamps_ms(
  ledger: pog.Connection,
  _grind_schema: String,
) -> Result(#(Int, Int), Nil) {
  use t0 <- result.try(unix_ms_query(
    "SELECT (extract(epoch FROM min(started_at)) * 1000)::bigint FROM bench_effects",
    ledger,
  ))
  let sql =
    "SELECT (extract(epoch FROM max(d.observed_at)) * 1000)::bigint FROM grind_bench.bench_durable_completions d JOIN bench_submissions s ON s.job_id=d.job_id"
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
  case harness_db.execute(query, ledger) {
    Ok(pog.Returned(rows: [Some(value)], ..)) -> Ok(value)
    _ -> Error(Nil)
  }
}

/// Item 6/I6: `GRIND_BENCH_POSTGRES_LOG`'s own line count right now --
/// captured before a run starts so `postgres_log_window` can slice off
/// exactly the lines written during the run, with no `log_line_prefix`
/// timestamp parsing required. `0` if the env var is unset. An explicitly
/// configured unreadable log invalidates the run instead of skipping I6.
pub fn postgres_log_lines_before() -> Int {
  case runtime.getenv("GRIND_BENCH_POSTGRES_LOG") {
    Error(Nil) -> 0
    Ok(path) ->
      case simplifile.read(path) {
        Error(_) ->
          panic as "configured PostgreSQL log is unreadable; run evidence is invalid"
        Ok(content) ->
          case string.ends_with(content, "\n") || content == "" {
            True -> list.length(string.split(content, "\n")) - 1
            False -> list.length(string.split(content, "\n"))
          }
      }
  }
}

/// `Error(Nil)`: `GRIND_BENCH_POSTGRES_LOG` is unset (I6's log scan is
/// skipped entirely, not failed -- see `audit.run`'s own `postgres_log_window`
/// parameter). An explicitly configured unreadable log invalidates the run.
/// `Ok(lines)`: every log line written since `lines_before`.
pub fn postgres_log_window(lines_before: Int) -> Result(List(String), Nil) {
  case runtime.getenv("GRIND_BENCH_POSTGRES_LOG") {
    Error(Nil) -> Error(Nil)
    Ok(path) ->
      case simplifile.read(path) {
        Error(_) ->
          panic as "configured PostgreSQL log is unreadable; run evidence is invalid"
        Ok(content) -> Ok(list.drop(string.split(content, "\n"), lines_before))
      }
  }
}

/// Item 9: postmaster + descendants' own cumulative CPU time (`ps` cputime),
/// `None` if `GRIND_BENCH_PG_DATA_DIR` is unset or unreadable -- best-effort,
/// never fatal.
pub fn cpu_ms_now() -> Result(Int, Nil) {
  case runtime.getenv("GRIND_BENCH_PG_DATA_DIR") {
    Error(Nil) -> Error(Nil)
    Ok(dir) -> runtime.cpu_times_ms_ffi(dir)
  }
}

/// Item 9: `#(cpu_ms_delta, cpu_percent_of_one_core)` from a before/after
/// `ps` cputime pair and the run's own wall-clock `elapsed_ms`. `None` if
/// either sample was unavailable.
pub fn cpu_delta(
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

pub fn run_audit_and_report(
  ledger: pog.Connection,
  database: postgres.Database,
  label: String,
  expected_job_count: Int,
  log_lines_before: Int,
) -> Nil {
  let grind_schema = context.schema_of(database)
  let log_window = postgres_log_window(log_lines_before)
  let assert Ok(report) =
    audit.run(
      ledger,
      grind_schema,
      expected_job_count,
      runtime.counter_value(runtime.ledger_error_counter),
      runtime.counter_value(runtime.forwarder_drop_counter),
      runtime.counter_value(runtime.quarantine_counter),
      log_window,
    )
  let _ = postgres.close(database)
  context.cleanup_schema(ledger, grind_schema)
  case audit.passed(report) {
    True -> io.println(label <> ": audit PASSED (I1-I7)")
    False -> {
      io.println(label <> ": audit FAILED")
      list.each(report.violations, fn(violation) {
        io.println("  - " <> string.inspect(violation))
      })
      runtime.halt(1)
    }
  }
}

pub fn write_row(path: String, header: String, row: String) -> Nil {
  let _ = simplifile.create_directory_all(runtime.parent_dir(path))
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

pub fn write_rows(path: String, header: String, rows: List(String)) -> Nil {
  list.each(rows, fn(row) { write_row(path, header, row) })
}

// -- Shared helpers for L2-L6 -----------------------------------------------

pub fn bool_str(value: Bool) -> String {
  case value {
    True -> "true"
    False -> "false"
  }
}

pub fn bucket_totals(
  deltas: List(#(statement_split.Bucket, Int, Float)),
  bucket: statement_split.Bucket,
) -> #(Int, Float) {
  case list.find(deltas, fn(entry) { entry.0 == bucket }) {
    Ok(#(_, calls, ms)) -> #(calls, ms)
    Error(Nil) -> #(0, 0.0)
  }
}

pub fn percentile_field(
  stats: Result(summarize.Percentiles, Nil),
  which: String,
) -> String {
  case stats {
    Error(Nil) -> "-1"
    Ok(summarize.Percentiles(p50:, p95:, p99:, max:, ..)) ->
      case which {
        "p50" -> float.to_string(p50)
        "p95" -> float.to_string(p95)
        "p99" -> float.to_string(p99)
        "max" -> float.to_string(max)
        _ -> "-1"
      }
  }
}

pub fn count_jobs_in_queue(
  connection: pog.Connection,
  queue_name: String,
) -> Int {
  let query =
    pog.query("SELECT count(*) FROM grind_jobs WHERE queue = $1")
    |> pog.parameter(pog.text(queue_name))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  let assert Ok(pog.Returned(rows: [count], ..)) =
    harness_db.execute(query, connection)
  count
}

pub fn dead_tuple_count(
  connection: pog.Connection,
  grind_schema: String,
) -> Int {
  let query =
    pog.query(
      "SELECT coalesce(n_dead_tup, 0) FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'grind_jobs'",
    )
    |> pog.parameter(pog.text(grind_schema))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case harness_db.execute(query, connection) {
    Ok(pog.Returned(rows: [count], ..)) -> count
    _ -> -1
  }
}

pub fn submitted_job_ids_ordered(
  ledger: pog.Connection,
) -> Result(List(Int), pog.QueryError) {
  let query =
    pog.query("SELECT job_id FROM bench_submissions ORDER BY job_id ASC")
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  harness_db.execute(query, ledger)
  |> result.map(fn(returned) { returned.rows })
}

/// A bulk, direct-SQL insert of `count` already-`succeeded` filler rows
/// (never through `postgres.submit`, never tracked in `bench_submissions`)
/// -- L2's own stand-in for "a large table of already-finished jobs"
/// without paying real preload/drain cost for rows nothing ever claims (a
/// terminal-state row is never selected by `attempt.claim_one`). One
/// bounded batch at a time, so a million-row preload cannot hit the
/// driver deadline for a single enormous insert.
pub fn insert_filler_succeeded(connection: pog.Connection, count: Int) -> Nil {
  case count <= 0 {
    True -> Nil
    False -> {
      let batch = int.min(count, 10_000)
      insert_filler_batch(connection, batch)
      insert_filler_succeeded(connection, count - batch)
    }
  }
}

fn insert_filler_batch(connection: pog.Connection, count: Int) -> Nil {
  let query =
    pog.query(
      "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, output, max_attempts, state, available_at, finished_at) "
      <> "SELECT 'l2-filler', 'l2-filler-worker', 'v1', 'v1', '{}'::jsonb, 'v1', '0'::jsonb, 20, 'succeeded', clock_timestamp(), clock_timestamp() "
      <> "FROM generate_series(1, $1)",
    )
    |> pog.parameter(pog.int(count))
  let assert Ok(_) = harness_db.execute(query, connection)
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
pub fn reduced_audit_and_report(
  ledger: pog.Connection,
  database: postgres.Database,
  label: String,
  log_lines_before: Int,
) -> Nil {
  let grind_schema = context.schema_of(database)
  let quarantine_count = runtime.counter_value(runtime.quarantine_counter)
  let forwarder_drop_count =
    runtime.counter_value(runtime.forwarder_drop_counter)
  let ledger_error_count = runtime.counter_value(runtime.ledger_error_counter)
  let assert Ok(executing) =
    audit.check_no_executing_after_drain(ledger, grind_schema)
  let log_window = postgres_log_window(log_lines_before)
  let log_result = case log_window {
    Ok(window) -> audit.check_postgres_log(window)
    Error(Nil) -> Ok(Nil)
  }
  let _ = postgres.close(database)
  context.cleanup_schema(ledger, grind_schema)
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
      runtime.halt(1)
    }
  }
}

pub fn write_statement_split_rows(
  label: String,
  before: Result(statement_split.Totals, Nil),
  after: Result(statement_split.Totals, Nil),
) -> Nil {
  let deltas = statement_split.diff(before, after)
  case deltas {
    [] -> Nil
    _ ->
      write_rows(
        runtime.results_dir() <> "/statements.csv",
        runtime.provenance_header_prefix()
          <> ",label,bucket,calls_delta,total_exec_time_ms_delta",
        list.map(deltas, fn(entry) {
          let #(bucket, calls, total_ms) = entry
          string.join(
            [
              runtime.provenance_prefix(),
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
pub fn summarize_field_to_csv(
  path: String,
  field: String,
  label: String,
) -> Nil {
  case summarize.read_field_values(path, field) {
    Error(_) -> Nil
    Ok(values) ->
      case summarize.percentiles(values) {
        Error(Nil) -> Nil
        Ok(stats) -> {
          let _ =
            summarize.write_csv(runtime.results_dir() <> "/samplers.csv", [
              summarize.csv_row(label, field, stats),
            ])
          Nil
        }
      }
  }
}
