//// Preloads benchmark jobs with batched INSERTs. This bypasses ordinary
//// admission and creates no submission receipts, so it measures processing
//// of existing work rather than submission throughput.
////
//// Rows use the supplied worker metadata and input encoder. The schema guard
//// checks every mirrored column and rejects any required column without a
//// default that the preload does not supply.

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

/// The preload's literal INSERT column list. The schema guard compares this
/// list with live columns rather than parsing production Gleam source.
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

/// Preloads jobs using the supplied worker metadata and input encoder, with
/// batch_size rows per statement. Configured attempt limits and input encoding
/// therefore match the worker definition rather than benchmark constants.
/// Returns #(bench_index, job_id) pairs, correlated by the stored bench_index;
/// INSERT RETURNING row order is not a PostgreSQL guarantee.
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
