//// The lease-fencing predicates and the quarantine scan shared by
//// `grind/postgres` (the public, cross-queue `quarantine_expired` sweep)
//// and `grind/internal/attempt` (the per-poll scan `attempt.claim_one`
//// runs before claiming). See each function's own doc comment for the
//// invariant it enforces.

import gleam/dynamic/decode
import gleam/list
import gleam/option.{type Option, None, Some}
import grind/internal/store
import grind/observation
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
pub fn quarantine_update_sql(candidate_select: String) -> String {
  "WITH candidate AS ("
  <> candidate_select
  <> ") UPDATE grind_jobs AS job SET state = 'uncertain', failure_description = CASE WHEN job.cancel_requested_at IS NOT NULL THEN 'expired after cancellation request; prior effect unknown' WHEN job.failure_description IS NULL THEN 'expired attempt requires outcome reconciliation' ELSE job.failure_description || '; expired attempt requires outcome reconciliation' END, uncertain_at = clock_timestamp() FROM candidate WHERE job.id = candidate.id RETURNING job.id, job.queue, job.worker_id, job.worker_version, job.attempt_id, job.attempt_epoch, job.attempt_count, (job.cancel_requested_at IS NOT NULL)"
}

pub fn quarantine_row_decoder() -> decode.Decoder(
  #(Int, String, String, String, Option(Int), Int, Int, Bool),
) {
  use id <- decode.field(0, decode.int)
  use queue <- decode.field(1, decode.string)
  use worker_id <- decode.field(2, decode.string)
  use worker_version <- decode.field(3, decode.string)
  use attempt_id <- decode.field(4, decode.optional(decode.int))
  use attempt_epoch <- decode.field(5, decode.int)
  use attempt_count <- decode.field(6, decode.int)
  use cancellation_was_requested <- decode.field(7, decode.bool)
  decode.success(#(
    id,
    queue,
    worker_id,
    worker_version,
    attempt_id,
    attempt_epoch,
    attempt_count,
    cancellation_was_requested,
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
      "SELECT id FROM grind_jobs WHERE queue = $1 AND state = 'executing' AND "
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
pub fn emit_quarantined(
  fwd: Forwarder,
  row: #(Int, String, String, String, Option(Int), Int, Int, Bool),
) -> Nil {
  let #(
    job_id,
    queue,
    worker_id,
    worker_version,
    attempt_id,
    epoch,
    attempt,
    cancellation_was_requested,
  ) = row
  case attempt_id {
    None -> Nil
    Some(attempt_id) -> {
      let _ =
        forwarder.emit(
          fwd,
          observation.quarantined(),
          observation.QuarantinedMeasurements(count: 1),
          observation.QuarantinedMetadata(
            ref: observation.JobRef(
              job_id:,
              queue:,
              worker_id:,
              worker_version:,
            ),
            attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
            cancellation_was_requested:,
          ),
        )
      Nil
    }
  }
}
