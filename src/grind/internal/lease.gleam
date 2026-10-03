//// The lease-fencing predicates and the quarantine scan shared by
//// `grind/postgres` (the public, cross-queue `quarantine_expired` sweep)
//// and `grind/internal/attempt` (the per-poll scan `attempt.claim_one`
//// runs before claiming). See each function's own doc comment for the
//// invariant it enforces.

import gleam/dynamic/decode
import gleam/list
import gleam/option.{type Option, None, Some}
import grind/internal/events
import grind/internal/store
import grind/telemetry
import pog
import sinal/forwarder.{type Forwarder}

/// Renders the fenced "the lease is still live" predicate, comparing
/// `lease_expires_at` against the given SQL time expression (in production,
/// always `clock_timestamp()`). Renewal, contract-mismatch release, every
/// acknowledgement disposition, and the ack-rejection reader all gate on
/// this exact fragment; a test can render the identical production text
/// against a fixed instant instead of duplicating the comparison by hand.
/// `now_expression` is spliced into the query text verbatim, not bound as a
/// parameter — it must be trusted SQL supplied by this module or a test
/// (a literal function call or column reference such as `clock_timestamp()`
/// or `instant`), never untrusted or caller-supplied input.
pub fn live_lease_predicate(now_expression: String) -> String {
  "lease_expires_at > " <> now_expression
}

/// The complement of `live_lease_predicate`: true once the lease has
/// already expired at the given time expression. Used by the quarantine
/// scan that looks for abandoned attempts. Kept as its own mirrored
/// fragment (rather than `NOT (` <> live_lease_predicate <> `)`) so the
/// production SQL text is unchanged by this extraction. Same trust
/// requirement as `live_lease_predicate`: `now_expression` is spliced in
/// verbatim and must be trusted SQL, never user input.
pub fn expired_lease_predicate(now_expression: String) -> String {
  "lease_expires_at <= " <> now_expression
}

/// The `UPDATE ... SET state = 'uncertain', ...` fragment shared by every
/// quarantine scan (the per-queue scan a consumer runs on each poll, and the
/// public cross-queue `postgres.quarantine_expired`). `candidate_select` is
/// the trusted SQL text (never caller input) for the `candidate` CTE that
/// picks which rows to flip; only its shape differs between the two
/// callers. This never decodes or runs any worker code — it only ever flips
/// an already-expired `executing` row to `uncertain` under
/// `expired_lease_predicate`, the single-sourced fragment both callers
/// splice in.
///
/// `candidate_select` must select `id, attempt_id, attempt_count`: the
/// `RETURNING` list reports the expired attempt from the candidate row as it
/// was before the update, because a replay clears `attempt_id` and refunds
/// `attempt_count` (a replay is a redelivery, not a business attempt, like a
/// snooze), and every `RETURNING` expression on `job` reads the updated row.
pub fn quarantine_update_sql(candidate_select: String) -> String {
  let replay = replay_condition("job")
  "WITH candidate AS ("
  <> candidate_select
  <> ") UPDATE grind_jobs AS job SET state = CASE WHEN "
  <> replay
  <> " THEN 'queued' ELSE 'uncertain' END, replay_count = CASE WHEN "
  <> replay
  <> " THEN job.replay_count + 1 ELSE job.replay_count END, available_at = CASE WHEN "
  <> replay
  <> " THEN clock_timestamp() ELSE job.available_at END, attempt_id = CASE WHEN "
  <> replay
  <> " THEN NULL ELSE job.attempt_id END, attempt_count = CASE WHEN "
  <> replay
  <> " THEN GREATEST(job.attempt_count - 1, 0) ELSE job.attempt_count END, attempt_owner = CASE WHEN "
  <> replay
  <> " THEN NULL ELSE job.attempt_owner END, lease_expires_at = CASE WHEN "
  <> replay
  <> " THEN NULL ELSE job.lease_expires_at END, failure_description = CASE WHEN "
  <> replay
  <> " THEN 'expired attempt replayed (' || (job.replay_count + 1)::text || ' of ' || job.max_replays::text || ')' WHEN job.cancel_requested_at IS NOT NULL THEN 'expired after cancellation request; prior effect unknown' WHEN job.failure_description IS NULL THEN 'expired attempt requires outcome reconciliation' ELSE job.failure_description || '; expired attempt requires outcome reconciliation' END, uncertain_at = CASE WHEN "
  <> replay
  <> " THEN NULL ELSE clock_timestamp() END FROM candidate WHERE job.id = candidate.id RETURNING job.id, job.queue, job.worker_id, job.worker_version, candidate.attempt_id, job.attempt_epoch, candidate.attempt_count, (job.cancel_requested_at IS NOT NULL), job.state = 'queued', job.correlation"
}

/// Whether an expired attempt of the row `alias` is replayed instead of held
/// `uncertain`: its worker opted into `ReplayAfterLeaseExpiry`, it has
/// replays left, and no cancellation is pending. Evaluated against the row
/// before the update, as every `SET` expression is.
fn replay_condition(alias: String) -> String {
  "("
  <> alias
  <> ".max_replays IS NOT NULL AND "
  <> alias
  <> ".replay_count < "
  <> alias
  <> ".max_replays AND "
  <> alias
  <> ".cancel_requested_at IS NULL)"
}

/// One row a quarantine scan moved: id, queue, worker id and version, the
/// expired attempt's id, epoch and number, whether a cancellation was
/// pending, whether the row was replayed rather than held, and its stored
/// correlation.
pub type QuarantinedRow {
  QuarantinedRow(
    id: Int,
    queue: String,
    worker_id: String,
    worker_version: String,
    attempt_id: Option(Int),
    attempt_epoch: Int,
    attempt_count: Int,
    cancellation_was_requested: Bool,
    replayed: Bool,
    correlation: Option(String),
  )
}

pub fn quarantine_row_decoder() -> decode.Decoder(QuarantinedRow) {
  use id <- decode.field(0, decode.int)
  use queue <- decode.field(1, decode.string)
  use worker_id <- decode.field(2, decode.string)
  use worker_version <- decode.field(3, decode.string)
  use attempt_id <- decode.field(4, decode.optional(decode.int))
  use attempt_epoch <- decode.field(5, decode.int)
  use attempt_count <- decode.field(6, decode.int)
  use cancellation_was_requested <- decode.field(7, decode.bool)
  use replayed <- decode.field(8, decode.bool)
  use correlation <- decode.field(9, decode.optional(decode.string))
  decode.success(QuarantinedRow(
    id:,
    queue:,
    worker_id:,
    worker_version:,
    attempt_id:,
    attempt_epoch:,
    attempt_count:,
    cancellation_was_requested:,
    replayed:,
    correlation:,
  ))
}

/// Quarantines at most one expired `executing` row in `queue`, run once at
/// the start of every `attempt.claim_one` (one poll's worth of work).
/// Scoped only by queue — **not** by any registered
/// worker identity or version: an expired lease belonging to a worker
/// version this consumer no longer registers (after a worker-version bump,
/// the old version's still-executing row) is quarantined exactly the same
/// as one this consumer could itself claim. Quarantining never decodes or
/// runs code — it only flips `executing` to `uncertain` under
/// `expired_lease_predicate` — so there is no codec or registration reason
/// to restrict it to registered identities; the identity filter this
/// function had before only ever hid an abandoned old-version attempt from
/// every consumer that outlived it. A consumer that never polls a given
/// queue at all still leaves that queue's expired rows to the public
/// `postgres.quarantine_expired`.
///
/// Takes `connection`/`forwarder` already extracted from a `Database` (via
/// `postgres.connection`/`postgres.forwarder`) rather than the opaque
/// `Database` itself, and returns the bare `pog.QueryError` rather than a
/// `postgres.QueueRunError` — this module does not depend on
/// `grind/postgres`, so its caller (`attempt.claim_one`) wraps the error
/// into `QueueClaimFailed` itself.
pub fn quarantine_expired_in_queue(
  connection: pog.Connection,
  forwarder: Forwarder,
  queue: String,
) -> Result(Nil, pog.QueryError) {
  quarantine_expired_in_queue_measured(connection, forwarder, queue).value
}

pub fn quarantine_expired_in_queue_measured(
  connection: pog.Connection,
  forwarder: Forwarder,
  queue: String,
) -> store.Measured(Result(Nil, pog.QueryError)) {
  // `FOR NO KEY UPDATE`: `quarantine_update_sql`'s own `UPDATE` never
  // touches `id`, so this candidate lock does not need to conflict with a
  // concurrent `unique_admission.candidate_sql`'s `FOR KEY SHARE` on the
  // same row — see `attempt.claim_registered_job`'s identical reasoning.
  let sql =
    quarantine_update_sql(
      "SELECT id, attempt_id, attempt_count FROM grind_jobs WHERE queue = $1 AND state = 'executing' AND "
      <> expired_lease_predicate("clock_timestamp()")
      <> " ORDER BY id FOR NO KEY UPDATE SKIP LOCKED LIMIT 1",
    )
  let query =
    pog.query(sql)
    |> pog.parameter(pog.text(queue))
    |> pog.returning(quarantine_row_decoder())
  let measured = store.execute_measured(query, on: connection)
  let value = case measured.value {
    Error(error) -> Error(error)
    Ok(returned) -> {
      list.each(returned.rows, fn(row) { emit_quarantined(forwarder, row) })
      Ok(Nil)
    }
  }
  store.Measured(
    value:,
    call_duration_us: measured.call_duration_us,
    checkout: measured.checkout,
  )
}

/// Builds and forwards `[grind, job, quarantined]` for one row a quarantine
/// scan's own `RETURNING` reports as moved to `uncertain`. Called only after
/// that autocommitted `UPDATE` already returned the row. `attempt_id` is
/// only absent for a row with no attempt on record at all — unreachable for
/// a row this scan can find (it only ever matches `state = 'executing'`,
/// which always has one), but decoded as optional defensively rather than
/// asserted, and skipped (fail-closed, like every other stored-state mapping
/// in this module) rather than guessed at.
pub fn emit_quarantined(fwd: Forwarder, row: QuarantinedRow) -> Nil {
  let QuarantinedRow(
    id: job_id,
    queue:,
    worker_id:,
    worker_version:,
    attempt_id:,
    attempt_epoch: epoch,
    attempt_count: attempt,
    cancellation_was_requested:,
    replayed:,
    correlation:,
  ) = row
  case attempt_id {
    None -> Nil
    Some(attempt_id) -> {
      let _ =
        forwarder.emit(
          fwd,
          telemetry.quarantined(),
          events.job_measurements(),
          telemetry.QuarantinedMetadata(
            ref: telemetry.JobRef(
              job_id:,
              queue:,
              worker_id:,
              worker_version:,
              correlation: events.correlation(job_id, correlation),
            ),
            attempt: telemetry.AttemptRef(attempt_id:, epoch:, attempt:),
            cancellation_was_requested:,
            replayed:,
          ),
        )
      Nil
    }
  }
}
