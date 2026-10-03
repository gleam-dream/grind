//// Bulk job preload: batched multi-row `INSERT ... VALUES`, built from
//// exactly the same column list and per-row expressions as
//// `grind/postgres.submit_with_availability`'s own single-row `INSERT`
//// (`src/grind/postgres.gleam`, the plain `submit`/`submit_at` path -- not
//// `grind/internal/unique_admission`'s richer insert, which also writes a
//// `grind_unique_submissions` receipt and is not what a plain preloaded job
//// needs). Grind itself has no bulk-submit API (see `docs/RISKS.md`,
//// finding 5 in the bench planning notes), so a load scenario that needs
//// many thousands of pre-existing rows goes around `postgres.submit`
//// entirely -- through this bench-owned statement instead, batched for
//// throughput, one call per batch rather than one call per row.
////
//// **Column drift guard.** `check_schema_matches` (exercised by
//// `bench/test/grind_bench_preload_test.gleam`) queries
//// `information_schema.columns` for the live `grind_jobs` table and fails
//// if either (a) a column this preload's own `INSERT` references no longer
//// exists, or (b) some *other* column is `NOT NULL` with no default (so any
//// `INSERT` must supply it) and is not in this preload's own column list --
//// case (b) is exactly what would happen if a future Grind migration added
//// a new required column this preload was never updated for.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import grind/internal/postgres
import grind/internal/worker
import grind_bench/harness_db
import pog

/// One job to preload. `available_at_ms: None` means immediately available
/// (`queued`), matching `postgres.submit`'s own default; `Some(ms)` mirrors
/// `postgres.submit_at`.
pub type PreloadJob {
  PreloadJob(bench_index: Int, cost_ms: Int, available_at_ms: Option(Int))
}

pub fn immediate(bench_index: Int, cost_ms: Int) -> PreloadJob {
  PreloadJob(bench_index:, cost_ms:, available_at_ms: None)
}

/// The exact column list `postgres.submit_with_availability`'s own `INSERT`
/// supplies -- see that function's own SQL text
/// (`src/grind/postgres.gleam`). Kept as a literal, not read from source, so
/// `check_schema_matches` has a concrete list to check against the live
/// database rather than parsing Gleam source.
pub fn mirrored_columns() -> List(String) {
  [
    "queue", "worker_id", "worker_version", "input_version", "input",
    "output_version", "error_version", "max_attempts", "state", "available_at",
  ]
}

pub type SchemaDriftError {
  ColumnMissing(column: String)
  QueryFailed(pog.QueryError)
  /// A `grind_jobs` column is `NOT NULL` with no default (so every `INSERT`
  /// must supply it) but is not one of `mirrored_columns()` -- this
  /// preload's own `INSERT` would fail with a `not-null constraint`
  /// violation the moment such a column is added upstream.
  UnmirroredRequiredColumn(column: String)
}

/// Checks `mirrored_columns()` against the live `grind_jobs` table in
/// `database`'s own configured schema. See this module's own doc comment,
/// "Column drift guard".
pub fn check_schema_matches(
  database: postgres.Database,
) -> Result(Nil, SchemaDriftError) {
  let connection = postgres.connection(database)
  use existing <- result.try(
    existing_columns(connection) |> result.map_error(QueryFailed),
  )
  use _ <- result.try(
    list.try_each(mirrored_columns(), fn(column) {
      case list.contains(existing, column) {
        True -> Ok(Nil)
        False -> Error(ColumnMissing(column))
      }
    }),
  )
  use required <- result.try(
    required_columns_without_default(connection)
    |> result.map_error(QueryFailed),
  )
  list.try_each(required, fn(column) {
    case list.contains(mirrored_columns(), column) {
      True -> Ok(Nil)
      False -> Error(UnmirroredRequiredColumn(column))
    }
  })
}

fn existing_columns(
  connection: pog.Connection,
) -> Result(List(String), pog.QueryError) {
  let query =
    pog.query(
      "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs'",
    )
    |> pog.returning({
      use column_name <- decode.field(0, decode.string)
      decode.success(column_name)
    })
  harness_db.execute(query, connection)
  |> result.map(fn(returned) { returned.rows })
}

fn required_columns_without_default(
  connection: pog.Connection,
) -> Result(List(String), pog.QueryError) {
  let query =
    pog.query(
      "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs' AND is_nullable = 'NO' AND column_default IS NULL",
    )
    |> pog.returning({
      use column_name <- decode.field(0, decode.string)
      decode.success(column_name)
    })
  harness_db.execute(query, connection)
  |> result.map(fn(returned) { returned.rows })
}

/// Preloads `jobs` under `queue` and `meta` (a real `worker.Metadata`,
/// `worker.metadata(worker_def)` -- item 12: `meta.max_attempts` replaces a
/// hardcoded `20`, so this preload never silently drifts from whatever
/// `with_max_attempts` the caller's own worker definition actually set),
/// batched `batch_size` rows per statement. `encode_input` builds each row's
/// `input` JSON text through the worker's own public codec (item 12:
/// `worker.encode_input(worker_def, ...)` -- never a hand-rolled JSON
/// string), so a preloaded row's `input` column is byte-for-byte what
/// `postgres.submit` would have written for the same value. Returns
/// `#(bench_index, job_id)` pairs for every preloaded row, correlated via
/// `input->>'bench_index'` in the `RETURNING` clause rather than row order
/// (a multi-row `INSERT ... RETURNING`'s row order is not part of any
/// documented PostgreSQL guarantee).
pub fn preload(
  database: postgres.Database,
  queue: String,
  meta: worker.Metadata,
  encode_input: fn(PreloadJob) -> String,
  jobs: List(PreloadJob),
  batch_size: Int,
) -> Result(List(#(Int, Int)), pog.QueryError) {
  let connection = postgres.connection(database)
  jobs
  |> list.sized_chunk(batch_size)
  |> list.try_fold([], fn(acc, batch) {
    use inserted <- result.try(preload_batch(
      connection,
      queue,
      meta,
      encode_input,
      batch,
    ))
    Ok(list.append(acc, inserted))
  })
}

fn preload_batch(
  connection: pog.Connection,
  queue: String,
  meta: worker.Metadata,
  encode_input: fn(PreloadJob) -> String,
  batch: List(PreloadJob),
) -> Result(List(#(Int, Int)), pog.QueryError) {
  let indexed = list.index_map(batch, fn(job, index) { #(index, job) })
  let value_tuples =
    list.map(indexed, fn(entry) {
      let #(index, _job) = entry
      let base = index * 9
      "($"
      <> itoa(base + 1)
      <> ", $"
      <> itoa(base + 2)
      <> ", $"
      <> itoa(base + 3)
      <> ", $"
      <> itoa(base + 4)
      <> ", $"
      <> itoa(base + 5)
      <> "::jsonb, $"
      <> itoa(base + 6)
      <> ", $"
      <> itoa(base + 7)
      <> ", $"
      <> itoa(base + 8)
      <> ", CASE WHEN $"
      <> itoa(base + 9)
      <> "::bigint IS NULL OR $"
      <> itoa(base + 9)
      <> "::bigint <= (extract(epoch FROM clock_timestamp()) * 1000)::bigint THEN 'queued' ELSE 'scheduled' END, "
      <> "CASE WHEN $"
      <> itoa(base + 9)
      <> "::bigint IS NULL THEN clock_timestamp() ELSE to_timestamp($"
      <> itoa(base + 9)
      <> "::double precision / 1000.0) END)"
    })
    |> string.join(", ")
  let sql =
    "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, error_version, max_attempts, state, available_at) VALUES "
    <> value_tuples
    <> " RETURNING id, (input->>'bench_index')::bigint"
  let query =
    list.fold(indexed, pog.query(sql), fn(query, entry) {
      let #(_, job) = entry
      let PreloadJob(available_at_ms:, ..) = job
      let input_json = encode_input(job)
      let availability_parameter = case available_at_ms {
        Some(ms) -> pog.int(ms)
        None -> pog.null()
      }
      query
      |> pog.parameter(pog.text(queue))
      |> pog.parameter(pog.text(meta.id))
      |> pog.parameter(pog.text(meta.worker_version))
      |> pog.parameter(pog.text(meta.input_version))
      |> pog.parameter(pog.text(input_json))
      |> pog.parameter(pog.text(meta.output_version))
      |> pog.parameter(pog.null())
      |> pog.parameter(pog.int(meta.max_attempts))
      |> pog.parameter(availability_parameter)
    })
    |> pog.returning({
      use job_id <- decode.field(0, decode.int)
      use bench_index <- decode.field(1, decode.int)
      decode.success(#(bench_index, job_id))
    })
  harness_db.execute(query, connection)
  |> result.map(fn(returned) { returned.rows })
}

fn itoa(value: Int) -> String {
  int.to_string(value)
}
