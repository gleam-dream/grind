//// Proves `grind_bench/preload`'s column-drift guard actually catches
//// drift (red, then green again), and that a preloaded row is
//// behaviorally identical to one `postgres.submit` would have produced
//// for the same input (item 12).
////
//// The drift-mutation tests run against their own dedicated database
//// (`GRIND_BENCH_TEST_SCHEMA_DRIFT_URL`), never the shared
//// `GRIND_BENCH_TEST_DATABASE_URL` other bench tests use, because they
//// `ALTER TABLE grind_jobs` directly -- a real schema mutation, not safe to
//// interleave with other tests reading that same table concurrently.

import exception
import gleam/dynamic/decode
import gleeunit/should
import grind/job
import grind/postgres
import grind/worker
import grind_bench
import grind_bench/preload
import grind_bench/worker as bench_worker
import pog

@external(erlang, "bench_test_env", "database_url")
fn database_url() -> Result(String, Nil)

@external(erlang, "bench_test_env", "schema_drift_url")
fn schema_drift_url() -> Result(String, Nil)

@external(erlang, "bench_test_env", "mark")
fn mark(name: String) -> Nil

/// Every call site closes `database` via `exception.defer` so pools do not
/// accumulate across this module's own tests and exhaust the disposable
/// cluster's `max_connections`, and drops its own fresh schema (item 1) so
/// repeated `gleam test` runs never accumulate `bench_jobs_*` schemas.
fn setup_database(url: String) -> postgres.Database {
  let config =
    grind_bench.default_config(url) |> grind_bench.with_grind_pool_size(2)
  let assert Ok(settings) =
    grind_bench.grind_settings(config) |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  let assert Ok(Nil) = postgres.migrate(database)
  database
}

fn teardown(database: postgres.Database, url: String) -> Nil {
  let schema = job.installation_schema(postgres.installation(database))
  let _ = postgres.close(database)
  let config = grind_bench.default_config(url)
  let assert Ok(ledger) = grind_bench.start_ledger_pool(config)
  let assert Ok(Nil) = grind_bench.drop_schema(ledger, schema)
  Nil
}

fn raw_exec(database: postgres.Database, sql: String) -> Nil {
  let connection = postgres.connection(database)
  let assert Ok(_) = pog.execute(pog.query(sql), connection)
  Nil
}

pub fn schema_matches_freshly_migrated_schema_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let database = setup_database(url)
      use <- exception.defer(fn() { teardown(database, url) })
      preload.check_schema_matches(database) |> should.equal(Ok(Nil))
      mark("bench-preload-schema-matches-clean-passed")
    }
  }
}

/// Red: renaming a column this preload's `INSERT` references makes the
/// guard report `ColumnMissing`. Green: renaming it back makes the guard
/// pass again.
pub fn column_missing_red_then_green_test() {
  case schema_drift_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_column_missing(url)
  }
}

fn run_column_missing(url: String) -> Nil {
  let database = setup_database(url)
  use <- exception.defer(fn() { teardown(database, url) })
  raw_exec(
    database,
    "ALTER TABLE grind_jobs RENAME COLUMN output_version TO output_version_renamed",
  )
  preload.check_schema_matches(database)
  |> should.equal(Error(preload.ColumnMissing("output_version")))

  raw_exec(
    database,
    "ALTER TABLE grind_jobs RENAME COLUMN output_version_renamed TO output_version",
  )
  preload.check_schema_matches(database) |> should.equal(Ok(Nil))

  mark("bench-preload-column-missing-red-then-green-passed")
}

/// Red: adding a `NOT NULL`, no-default column to `grind_jobs` -- exactly
/// what a future Grind migration adding a genuinely required column would
/// do -- makes the guard report `UnmirroredRequiredColumn`, since this
/// preload's own `INSERT` was never updated to supply it. Green: dropping
/// the column again makes the guard pass.
pub fn unmirrored_required_column_red_then_green_test() {
  case schema_drift_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_unmirrored_required_column(url)
  }
}

fn run_unmirrored_required_column(url: String) -> Nil {
  let database = setup_database(url)
  use <- exception.defer(fn() { teardown(database, url) })
  raw_exec(
    database,
    "ALTER TABLE grind_jobs ADD COLUMN bench_guard_required text NOT NULL DEFAULT 'x'",
  )
  raw_exec(
    database,
    "ALTER TABLE grind_jobs ALTER COLUMN bench_guard_required DROP DEFAULT",
  )
  preload.check_schema_matches(database)
  |> should.equal(
    Error(preload.UnmirroredRequiredColumn("bench_guard_required")),
  )

  raw_exec(database, "ALTER TABLE grind_jobs DROP COLUMN bench_guard_required")
  preload.check_schema_matches(database) |> should.equal(Ok(Nil))

  mark("bench-preload-unmirrored-required-column-red-then-green-passed")
}

/// The real bench worker (`grind_bench/worker.build`, the exact one
/// `grind_bench/load` submits under) -- not an ad hoc echo worker -- because
/// `preload`'s own row correlation (`RETURNING id, (input->>'bench_index')`,
/// see that module's own doc comment) round-trips `bench_index` through the
/// row's actual stored `input` JSON, which only holds if the worker's own
/// encoder actually produces a `{"bench_index": ...}` shape the way
/// `BenchJob`'s codec does. A plain-`Int`-input worker's `encode_input`
/// would produce a bare JSON number instead, breaking that correlation --
/// exactly the failure this test caught the first time it used one.
fn preload_encode_input(
  worker_def: worker.Worker(bench_worker.BenchJob, Int, Nil),
) -> fn(preload.PreloadJob) -> String {
  fn(preload_job: preload.PreloadJob) -> String {
    worker.encode_input(
      worker_def,
      bench_worker.BenchJob(
        bench_index: preload_job.bench_index,
        cost_ms: preload_job.cost_ms,
      ),
    )
  }
}

/// A preloaded row is behaviorally identical to a `postgres.submit`-admitted
/// one for the same worker/queue/input shape: `state` is `queued` for both,
/// `max_attempts` comes from the worker's own metadata (`7`, not a
/// hardcoded `20`), and the `input` JSON text is built through
/// `worker.encode_input` -- the same public codec `postgres.submit` itself
/// uses -- rather than a hand-rolled string.
pub fn preloaded_row_matches_submit_shape_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_preloaded_row_matches_submit_shape(url)
  }
}

fn run_preloaded_row_matches_submit_shape(url: String) -> Nil {
  let database = setup_database(url)
  use <- exception.defer(fn() { teardown(database, url) })
  let config = grind_bench.default_config(url)
  let assert Ok(ledger) = grind_bench.start_ledger_pool(config)
  let assert Ok(worker_def) =
    bench_worker.build(ledger, "bench-preload-shape-echo")
  let assert Ok(worker_def) = worker.with_max_attempts(worker_def, 7)
  let meta = worker.metadata(worker_def)

  let assert Ok(pairs) =
    preload.preload(
      database,
      "bench-preload-shape",
      meta,
      preload_encode_input(worker_def),
      [preload.immediate(4321, 0)],
      1,
    )
  let assert [#(4321, preloaded_job_id)] = pairs

  let assert Ok(row) = fetch_row(database, preloaded_job_id)
  let #(state, _input_text, max_attempts) = row
  state |> should.equal("queued")
  max_attempts |> should.equal(7)

  mark("bench-preload-matches-submit-shape-passed")
}

fn fetch_row(
  database: postgres.Database,
  job_id: Int,
) -> Result(#(String, String, Int), Nil) {
  let connection = postgres.connection(database)
  let query =
    pog.query(
      "SELECT state, input::text, max_attempts FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use input_text <- decode.field(1, decode.string)
      use max_attempts <- decode.field(2, decode.int)
      decode.success(#(state, input_text, max_attempts))
    })
  case pog.execute(query, connection) {
    Ok(returned) ->
      case returned.rows {
        [row] -> Ok(row)
        _ -> Error(Nil)
      }
    Error(_) -> Error(Nil)
  }
}

/// Item 12's own explicit ask: "a test comparing a preloaded row against a
/// real `postgres.submit` row column by column, excluding ids and
/// timestamps." Uses `to_jsonb(t) - '<col>' - ...` in SQL, not a hardcoded
/// Gleam-side column list, so it stays correct across a future migration
/// that adds or renames columns neither this preload nor `postgres.submit`
/// itself needs to change for.
pub fn preloaded_row_equals_submitted_row_column_by_column_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_column_by_column_comparison(url)
  }
}

fn run_column_by_column_comparison(url: String) -> Nil {
  let database = setup_database(url)
  use <- exception.defer(fn() { teardown(database, url) })
  let config = grind_bench.default_config(url)
  let assert Ok(ledger) = grind_bench.start_ledger_pool(config)
  let assert Ok(worker_def) =
    bench_worker.build(ledger, "bench-preload-column-compare")
  let meta = worker.metadata(worker_def)
  let queue_name = "bench-preload-column-compare"

  let assert Ok(submitted_handle) =
    postgres.submit(
      database,
      queue_name,
      worker_def,
      bench_worker.BenchJob(bench_index: 555, cost_ms: 0),
    )
  let submitted_job_id = job.id_value(submitted_handle)

  let assert Ok(pairs) =
    preload.preload(
      database,
      queue_name,
      meta,
      preload_encode_input(worker_def),
      [preload.immediate(555, 0)],
      1,
    )
  let assert [#(555, preloaded_job_id)] = pairs

  let assert Ok(submitted_json) =
    fetch_comparable_row(database, submitted_job_id)
  let assert Ok(preloaded_json) =
    fetch_comparable_row(database, preloaded_job_id)
  submitted_json |> should.equal(preloaded_json)

  mark("bench-preload-column-by-column-passed")
}

fn fetch_comparable_row(
  database: postgres.Database,
  job_id: Int,
) -> Result(String, Nil) {
  let connection = postgres.connection(database)
  let query =
    pog.query(
      "SELECT (to_jsonb(t) - 'id' - 'inserted_at' - 'available_at' - 'lease_expires_at' - 'finished_at' - 'cancel_requested_at' - 'uncertain_at')::text FROM grind_jobs t WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.returning({
      use text <- decode.field(0, decode.string)
      decode.success(text)
    })
  case pog.execute(query, connection) {
    Ok(returned) ->
      case returned.rows {
        [row] -> Ok(row)
        _ -> Error(Nil)
      }
    Error(_) -> Error(Nil)
  }
}
