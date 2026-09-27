//// Per-job latency queries shared by the open-loop (L3) and pruner-impact
//// (L5) scenarios: millisecond gaps read directly from database
//// timestamps (`grind_jobs.inserted_at`/`finished_at`,
//// `bench_effects.started_at`, `grind_job_acknowledgements.committed_at`),
//// never the bench process's own wall clock -- the same "read timing from
//// the database, not the driver" discipline `grind_bench/load`'s own
//// `timestamps_ms` doc comment explains for L1/L7's aggregate elapsed time.
////
//// The bench worker (`grind_bench/worker`) never fails, so every finished
//// job has exactly one effect and one `succeeded` acknowledgement --
//// these queries do not need to dedupe retries the way a general-purpose
//// tool would.

import gleam/dynamic/decode
import gleam/result
import pog

fn qualify(schema: String, table: String) -> String {
  "\"" <> schema <> "\".\"" <> table <> "\""
}

/// Milliseconds from `grind_jobs.inserted_at` to `grind_jobs.finished_at`,
/// one row per finished bench-tracked job.
pub fn insert_to_finish_ms(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(List(Float), pog.QueryError) {
  let jobs = qualify(grind_schema, "grind_jobs")
  let sql =
    "SELECT extract(epoch FROM (j.finished_at - j.inserted_at)) * 1000.0 "
    <> "FROM bench_submissions bs JOIN "
    <> jobs
    <> " j ON j.id = bs.job_id WHERE j.finished_at IS NOT NULL"
  run_float_query(ledger, sql)
}

/// Milliseconds from `bench_effects.started_at` (the handler's own
/// dispatch, the closest DB timestamp to "claimed and started" the
/// public/ledger schema offers) to the job's own committed `succeeded`
/// acknowledgement.
pub fn start_to_ack_ms(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(List(Float), pog.QueryError) {
  let acks = qualify(grind_schema, "grind_job_acknowledgements")
  let sql =
    "SELECT extract(epoch FROM (a.committed_at - be.started_at)) * 1000.0 "
    <> "FROM bench_submissions bs "
    <> "JOIN bench_effects be ON be.bench_index = bs.bench_index "
    <> "JOIN "
    <> acks
    <> " a ON a.job_id = bs.job_id "
    <> "WHERE a.committed_state = 'succeeded'"
  run_float_query(ledger, sql)
}

fn run_float_query(
  ledger: pog.Connection,
  sql: String,
) -> Result(List(Float), pog.QueryError) {
  let query =
    pog.query(sql)
    |> pog.returning({
      use value <- decode.field(0, decode.float)
      decode.success(value)
    })
  pog.execute(query, ledger) |> result.map(fn(returned) { returned.rows })
}
