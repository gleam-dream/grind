import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option}
import grind/internal/terminal
import grind/job
import grind/postgres
import grind/submission
import grind/unique
import grind/worker
import pog

pub type RawInput {
  RawInput(json.Json)
}

pub fn encode_raw_input(input: RawInput) -> json.Json {
  let RawInput(value) = input
  value
}

pub fn raw_input_decoder() -> decode.Decoder(RawInput) {
  decode.success(RawInput(json.null()))
}

// -- Uniqueness increments 4-7 (queue scope, state eligibility, period
// boundaries at database time, receipts/idempotency) -----------------------
//
// Shared helpers for forcing a persisted column via raw SQL and for counting
// rows, used throughout the uniqueness tests.

pub fn force_job_timestamp(
  connection: pog.Connection,
  job_id: Int,
  column: String,
  sql_expression: String,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET "
      <> column
      <> " = "
      <> sql_expression
      <> " WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  Nil
}

pub fn force_job_state(
  connection: pog.Connection,
  job_id: Int,
  state: String,
) -> Nil {
  // `grind_jobs_finished_at_check` (grind_v12) requires `finished_at` to be
  // set iff `state` is one of the six terminal states, so a raw state flip
  // must set it consistently too, not just leave whatever the row already
  // had.
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = $1, finished_at = CASE WHEN $1 IN ("
      <> terminal.states_sql()
      <> ") THEN clock_timestamp() ELSE NULL END WHERE id = $2",
    )
    |> pog.parameter(pog.text(state))
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  Nil
}

pub fn job_available_at_ms(connection: pog.Connection, job_id: Int) -> Int {
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM available_at) * 1000)::bigint FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.returning({
      use ms <- decode.field(0, decode.int)
      decode.success(ms)
    })
    |> pog.execute(on: connection)
  let assert [ms] = returned.rows
  ms
}

/// Forces a row's `available_at` to a due (past) database time directly,
/// leaving `state` untouched — used by the Increment 10 reschedule/claim race
/// test to make a genuinely `scheduled` row immediately claimable without
/// waiting on wall-clock time, so the only real synchronization point in that
/// test is the barrier-forced lock overlap itself.
pub fn force_available_at_due(connection: pog.Connection, job_id: Int) -> Nil {
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() - interval '2 seconds' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  Nil
}

/// Reads a receipt's `rescheduled_from`/`rescheduled_to` columns (both
/// `NULL` for a non-reschedule decision), in milliseconds, for the Increment
/// 10 reschedule tests.
pub fn unique_receipt_reschedule_fields(
  connection: pog.Connection,
  submission_id_text: String,
) -> #(Option(Int), Option(Int)) {
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM rescheduled_from) * 1000)::bigint, (extract(epoch FROM rescheduled_to) * 1000)::bigint FROM grind_unique_submissions WHERE submission_id = $1",
    )
    |> pog.parameter(pog.text(submission_id_text))
    |> pog.returning({
      use from_ms <- decode.field(0, decode.optional(decode.int))
      use to_ms <- decode.field(1, decode.optional(decode.int))
      decode.success(#(from_ms, to_ms))
    })
    |> pog.execute(on: connection)
  let assert [row] = returned.rows
  row
}

/// `submit_unique` under `Immediately`/`RescheduleScheduledTo(target)`, the
/// counterpart to `submit_keep_existing` in `grind/support/submissions`
/// for the reschedule-action tests.
pub fn submit_reschedule(
  database: postgres.Database,
  queue: String,
  id_text: String,
  worker_def: worker.Worker(input, output, error),
  input: input,
  policy: unique.Policy(input),
  target: job.AvailableAt,
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let assert Ok(submission) = submission.submission_id(id_text)
  postgres.submit_unique(
    database,
    queue,
    submission,
    worker_def,
    input,
    submission.Immediately,
    policy,
    unique.RescheduleScheduledTo(target),
  )
}

/// A future database-time `AvailableAt`, `offset_ms` ahead of the test
/// cluster's own `clock_timestamp()` — never the calling BEAM node's clock —
/// read through `connection`.
pub fn future_available_at(
  connection: pog.Connection,
  offset_ms: Int,
) -> job.AvailableAt {
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint + $1",
    )
    |> pog.parameter(pog.int(offset_ms))
    |> pog.returning({
      use ms <- decode.field(0, decode.int)
      decode.success(ms)
    })
    |> pog.execute(on: connection)
  let assert [future_ms] = returned.rows
  let assert Ok(available_at) = job.available_at(future_ms)
  available_at
}
