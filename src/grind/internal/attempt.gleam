//// The claim/renew/acknowledge protocol driving one attempt's lifecycle
//// against `grind_jobs`: `claim_one` claims a due row, `execute_claim` runs
//// its typed handler, `renew` extends the lease while it runs, and
//// `acknowledge` commits the proposed outcome through the same attempt
//// fence `claim_one` established. The acknowledgement transaction and
//// receipt matching live in `grind/internal/attempt/acknowledgement`.
//// `grind/postgres` keeps the shared
//// `QueueRunError`/`AckRejection` result types this module's public
//// functions return, plus the `connection`/`forwarder`
//// accessors this module reads a `Database` through, since `Database` is
//// opaque outside `grind/postgres` (which defines it) and this module
//// cannot pattern-match its fields directly.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import grind/diagnostic
import grind/internal/attempt/acknowledgement.{type Claim, AckProposal, Claim} as attempt_acknowledgement
import grind/internal/diagnostics
import grind/internal/lease
import grind/internal/sql
import grind/internal/store
import grind/job.{type State}
import grind/observation
import grind/postgres.{type Database}
import grind/registry.{type Registry}
import grind/worker
import pog
import sinal/forwarder.{type Forwarder}

/// A claimed row bound to its registered typed execution closure.
pub opaque type ClaimedJob {
  ClaimedJob(claim: Claim, run: fn() -> worker.Execution)
}

/// Atomically claims one due row without running its handler in the caller.
/// The attempt process owns acknowledgement; its consumer's independent
/// renewer extends the live lease after this boundary.
pub fn claim_one(
  database: Database,
  queue: String,
  workers: Registry,
  attempt_owner: String,
  lease_duration_ms: Int,
) -> Result(Option(ClaimedJob), postgres.QueueRunError) {
  let fwd = postgres.forwarder(database)
  let reference = diagnostics.queue_ref(queue, attempt_owner)
  let measured =
    lease.quarantine_expired_in_queue_measured(
      postgres.connection(database),
      fwd,
      queue,
    )
  case
    diagnostics.checkout(
      fwd,
      reference,
      diagnostic.QuarantineScan,
      diagnostic.MainPool,
      measured,
    )
  {
    Error(error) -> {
      diagnostics.claim_failed(
        fwd,
        reference,
        diagnostic.QuarantineScan,
        error,
        measured.call_duration_us,
      )
      Error(postgres.QueueClaimFailed(error))
    }
    Ok(Nil) ->
      claim_registered_job(
        database,
        queue,
        workers,
        attempt_owner,
        registry.identities(workers),
        lease_duration_ms,
      )
  }
}

/// Executes only the typed closure captured when a matching job was claimed.
pub fn execute_claim(claimed: ClaimedJob) -> worker.Execution {
  let ClaimedJob(run:, ..) = claimed
  run()
}

/// Returns stable fencing fields for the queue actor's private active entry.
pub fn claim_identity(claimed: ClaimedJob) -> #(Int, Int, Int) {
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  #(id, attempt_id, epoch)
}

pub fn diagnostic_context(
  claimed: ClaimedJob,
  queue: String,
  owner: String,
) -> diagnostic.AttemptContext {
  let ClaimedJob(claim:, ..) = claimed
  diagnostic_context_from_claim(claim, queue, owner)
}

fn diagnostic_context_from_claim(
  claim: Claim,
  queue: String,
  owner: String,
) -> diagnostic.AttemptContext {
  let Claim(
    id:,
    attempt_id:,
    epoch:,
    current_attempt:,
    worker_id:,
    worker_version:,
    ..,
  ) = claim
  diagnostic.AttemptContext(
    ref: observation.JobRef(job_id: id, queue:, worker_id:, worker_version:),
    attempt: observation.AttemptRef(
      attempt_id:,
      epoch:,
      attempt: current_attempt,
    ),
    consumer: diagnostics.consumer_ref(owner),
  )
}

fn claim_previous_state(claim: Claim) -> String {
  let Claim(previous_state:, ..) = claim
  previous_state
}

/// The result of one lease-renewal attempt: `Renewed` means the fenced
/// `UPDATE` matched this exact attempt's still-`executing` row and extended
/// its lease; `LeaseLost` means it matched nothing (the row is no longer
/// this attempt's — expired, reassigned, or already acknowledged), which is
/// not itself an error.
pub type Renewal {
  Renewed
  LeaseLost
}

/// A skipped row is still live but locked, typically by its own ACK.
/// Skipping it preserves progress for the rest of the renewal batch.
pub type BatchRenewal {
  BatchRenewed
  BatchLocked
  BatchLeaseLost
}

/// Renews one consumer's leases in one bounded checkout on its reserved
/// connection. The candidate lock never waits for an acknowledgement's row.
/// Both candidate selection and update retain the live database-time fence.
pub fn renew_many(
  connection: pog.Connection,
  queue: String,
  owner: String,
  claims: List(ClaimedJob),
  lease_duration_ms: Int,
) -> Result(List(#(Int, Int, BatchRenewal)), pog.QueryError) {
  case
    renew_many_observed(connection, queue, owner, claims, lease_duration_ms).value
  {
    Error(error) -> Error(error)
    Ok(rows) ->
      Ok(list.map(rows, fn(row) { #(row.attempt_id, row.epoch, row.status) }))
  }
}

pub type ObservedRenewal {
  ObservedRenewal(
    attempt_id: Int,
    epoch: Int,
    status: BatchRenewal,
    remaining_lease_ms: Option(Int),
  )
}

pub fn renew_many_observed(
  connection: pog.Connection,
  queue: String,
  owner: String,
  claims: List(ClaimedJob),
  lease_duration_ms: Int,
) -> store.Measured(Result(List(ObservedRenewal), pog.QueryError)) {
  let identities = list.map(claims, claim_identity)
  let live = lease.live_lease_predicate("clock_timestamp()")
  let query =
    pog.query(
      "WITH fences AS (SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) AS f(id, attempt_id, epoch)), locked AS MATERIALIZED (SELECT j.id FROM grind_jobs j JOIN fences f ON j.id = f.id AND j.attempt_id = f.attempt_id AND j.attempt_epoch = f.epoch WHERE j.queue = $4 AND j.attempt_owner = $5 AND j.state = 'executing' AND "
      <> live
      <> " FOR NO KEY UPDATE OF j SKIP LOCKED), renewed AS (UPDATE grind_jobs j SET lease_expires_at = clock_timestamp() + ($6::double precision * interval '1 millisecond') FROM locked l WHERE j.id = l.id AND "
      <> live
      <> " RETURNING j.id, j.lease_expires_at AS new_lease_expires_at) SELECT f.attempt_id, f.epoch, CASE WHEN r.id IS NOT NULL THEN 1 WHEN j.state = 'executing' AND j.queue = $4 AND j.attempt_owner = $5 AND j.attempt_id = f.attempt_id AND j.attempt_epoch = f.epoch AND j."
      <> live
      <> " THEN 2 ELSE 0 END, CASE WHEN r.id IS NOT NULL THEN floor(extract(epoch FROM (r.new_lease_expires_at - clock_timestamp())) * 1000)::bigint WHEN j.queue = $4 AND j.attempt_owner = $5 AND j.attempt_id = f.attempt_id AND j.attempt_epoch = f.epoch THEN floor(extract(epoch FROM (j.lease_expires_at - clock_timestamp())) * 1000)::bigint ELSE NULL::bigint END FROM fences f LEFT JOIN renewed r ON r.id = f.id LEFT JOIN grind_jobs j ON j.id = f.id",
    )
    |> pog.parameter(pog.array(pog.int, list.map(identities, fn(x) { x.0 })))
    |> pog.parameter(pog.array(pog.int, list.map(identities, fn(x) { x.1 })))
    |> pog.parameter(pog.array(pog.int, list.map(identities, fn(x) { x.2 })))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(owner))
    |> pog.parameter(pog.int(lease_duration_ms))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use epoch <- decode.field(1, decode.int)
      use status <- decode.field(2, decode.int)
      let status = case status {
        1 -> BatchRenewed
        2 -> BatchLocked
        _ -> BatchLeaseLost
      }
      use remaining_lease_ms <- decode.field(3, decode.optional(decode.int))
      decode.success(ObservedRenewal(
        attempt_id:,
        epoch:,
        status:,
        remaining_lease_ms:,
      ))
    })
  let measured = store.execute_measured(query, on: connection)
  let value = case measured.value {
    Error(error) -> Error(error)
    Ok(returned) -> Ok(returned.rows)
  }
  store.Measured(
    value:,
    call_duration_us: measured.call_duration_us,
    checkout: measured.checkout,
  )
}

/// Extends the current lease using PostgreSQL's clock and current row values.
/// An expired or fenced claim cannot be renewed.
pub fn renew(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
  lease_duration_ms: Int,
) -> Result(Renewal, pog.QueryError) {
  let connection = postgres.connection(database)
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() + ($6::double precision * interval '1 millisecond') WHERE id = $1 AND queue = $2 AND state = 'executing' AND attempt_id = $3 AND attempt_epoch = $4 AND attempt_owner = $5 AND "
      <> lease.live_lease_predicate("clock_timestamp()")
      <> " RETURNING id",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.int(lease_duration_ms))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(error)
    Ok(returned) ->
      case returned.rows {
        [_] -> Ok(Renewed)
        [] -> Ok(LeaseLost)
        _ ->
          panic as "renew: fenced UPDATE by primary key id matched more than one row"
      }
  }
}

/// Releases a claim only when its exact attempt still owns the executing row.
/// This is used when the temporary worker child failed to start before
/// receiving `StartAttempt`, so it refunds the uninvoked attempt.
pub fn release_unstarted(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
) -> Result(Bool, postgres.QueueRunError) {
  let connection = postgres.connection(database)
  let forwarder = postgres.forwarder(database)
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  case
    store.call_safely(connection, fn(connection) {
      sql.release_unstarted_claim(
        connection,
        id,
        queue,
        attempt_id,
        epoch,
        attempt_owner,
        claim_previous_state(claim),
      )
    })
  {
    Error(error) -> Error(postgres.QueueClaimReleaseFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> {
          case job.state_of_stored(claim_previous_state(claim)) {
            Error(Nil) -> Nil
            Ok(restored_state) ->
              emit_released(
                forwarder,
                queue,
                id,
                worker_id,
                worker_version,
                attempt_id,
                epoch,
                claim.current_attempt,
                restored_state,
              )
          }
          Ok(True)
        }
        [] -> Ok(False)
        _ -> Error(postgres.QueueClaimReleaseRejected)
      }
  }
}

/// Builds and forwards `[grind, job, released]` for a claim refunded before
/// its worker ever ran. Called only after the release's own fenced
/// `UPDATE ... RETURNING` already returned that row. `epoch` is the
/// *released* attempt's own epoch (the value this claim was made under) —
/// the row's own `attempt_epoch` column is incremented again by the next
/// claim, so it no longer matches this event's `epoch` by the time a handler
/// might read it back.
fn emit_released(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  attempt_id: Int,
  epoch: Int,
  attempt: Int,
  restored_state: State,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.released(),
      observation.ReleasedMeasurements(count: 1),
      observation.ReleasedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
        restored_state:,
      ),
    )
  Nil
}

/// Commits the proposed worker outcome through the same attempt fence.
pub fn acknowledge(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
  execution: worker.Execution,
) -> Result(Bool, postgres.QueueRunError) {
  let ClaimedJob(claim:, ..) = claimed
  let Claim(output_version:, error_version:, ..) = claim
  case execution {
    worker.ExecutedSuccess(actual_version, _) ->
      case actual_version == output_version {
        True ->
          run_acknowledgement(
            database,
            queue,
            attempt_owner,
            claim,
            execution,
            output_version,
          )
        False ->
          Error(postgres.QueueAckProposalCodecMismatch(
            worker.OutputCodec,
            output_version,
            actual_version,
          ))
      }
    worker.ExecutedBusinessFailure(actual_version, _, _, _) ->
      case actual_version == error_version {
        True ->
          run_acknowledgement(
            database,
            queue,
            attempt_owner,
            claim,
            execution,
            output_version,
          )
        False ->
          Error(postgres.QueueAckProposalCodecMismatch(
            worker.ErrorCodec,
            option_version(error_version),
            option_version(actual_version),
          ))
      }
    worker.ExecutedRetryable(actual_version, _, _, _) ->
      case actual_version == error_version {
        True ->
          run_acknowledgement(
            database,
            queue,
            attempt_owner,
            claim,
            execution,
            output_version,
          )
        False ->
          Error(postgres.QueueAckProposalCodecMismatch(
            worker.ErrorCodec,
            option_version(error_version),
            option_version(actual_version),
          ))
      }
    worker.ExecutedInvalidInput(_) | worker.ExecutedUnencodable(_, _) ->
      run_acknowledgement(
        database,
        queue,
        attempt_owner,
        claim,
        execution,
        output_version,
      )
    worker.ExecutedSnoozed(_, _)
    | worker.ExecutedDiscarded(_)
    | worker.ExecutedCancelled(_)
    | worker.ExecutedUncertain(_) ->
      run_acknowledgement(
        database,
        queue,
        attempt_owner,
        claim,
        execution,
        output_version,
      )
  }
}

fn claim_registered_job(
  database: Database,
  queue: String,
  workers: Registry,
  attempt_owner: String,
  identities: List(#(String, String)),
  lease_duration_ms: Int,
) -> Result(Option(ClaimedJob), postgres.QueueRunError) {
  let connection = postgres.connection(database)
  let forwarder = postgres.forwarder(database)
  let eligibility =
    list.index_map(identities, fn(_, index) {
      let id_parameter = 4 + index * 2
      let version_parameter = id_parameter + 1
      "(worker_id = $"
      <> int.to_string(id_parameter)
      <> " AND worker_version = $"
      <> int.to_string(version_parameter)
      <> ")"
    })
    |> string.join(" OR ")
  // `statement_timestamp()` (stable for this statement's whole duration,
  // unlike the per-row-volatile `clock_timestamp()`) so the planner can
  // treat this as a real index condition against `grind_jobs_claim_idx`
  // instead of a post-scan filter — measured 0.92ms -> 0.009ms against a
  // large `grind_jobs`. Never used for a lease/fencing comparison (those
  // stay on `clock_timestamp()`, see `lease.live_lease_predicate`'s own doc
  // comment): this is a plain "is this job due yet" eligibility check, not
  // a fence against another process's concurrent claim.
  let eligible_state =
    "state IN ('queued', 'scheduled', 'retryable') AND available_at <= statement_timestamp()"
  // `FOR NO KEY UPDATE`, not `FOR UPDATE`: this candidate lock's own
  // `UPDATE` never touches `id` (the only column any unique index on
  // `grind_jobs` covers), so the weaker mode is exactly as safe and does
  // not conflict with `unique_admission/query.candidate_sql`'s own `FOR KEY
  // SHARE` on a `KeepExisting` uniqueness candidate — a plain `FOR UPDATE`
  // here would otherwise make a claim spuriously contend
  // (`AdmissionContended`) with an unrelated admission reading the exact
  // same row for a reason that was never actually incompatible with this
  // claim's own write. See `docs/UNIQUENESS-CONTRACT.md`, "Admission
  // transaction" step 6, for the full contention picture across claim,
  // cancel, and quarantine.
  let sql =
    "WITH candidate AS (SELECT id, state AS previous_state FROM grind_jobs WHERE queue = $1 AND "
    <> eligible_state
    <> " AND cancel_requested_at IS NULL AND ("
    <> eligibility
    <> ") ORDER BY available_at, id FOR NO KEY UPDATE SKIP LOCKED LIMIT 1) UPDATE grind_jobs AS job SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = job.attempt_epoch + 1, attempt_owner = $2, lease_expires_at = clock_timestamp() + ($3::double precision * interval '1 millisecond'), attempt_count = job.attempt_count + 1, delivery_count = job.delivery_count + 1 FROM candidate WHERE job.id = candidate.id RETURNING job.id, job.attempt_id, job.attempt_epoch, job.input_version, job.input::text, job.worker_id, job.worker_version, job.output_version, job.error_version, job.attempt_count, job.max_attempts, job.snooze_count, job.delivery_count, candidate.previous_state"
  let parameters =
    list.append(
      [pog.text(queue), pog.text(attempt_owner), pog.int(lease_duration_ms)],
      list.flat_map(identities, fn(identity) {
        let #(worker_id, worker_version) = identity
        [pog.text(worker_id), pog.text(worker_version)]
      }),
    )
  let query =
    list.fold(parameters, pog.query(sql), fn(query, parameter) {
      pog.parameter(query, parameter)
    })
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      use attempt_id <- decode.field(1, decode.int)
      use epoch <- decode.field(2, decode.int)
      use input_version <- decode.field(3, decode.string)
      use encoded_input <- decode.field(4, decode.string)
      use worker_id <- decode.field(5, decode.string)
      use worker_version <- decode.field(6, decode.string)
      use output_version <- decode.field(7, decode.string)
      use error_version <- decode.field(8, decode.optional(decode.string))
      use current_attempt <- decode.field(9, decode.int)
      use max_attempts <- decode.field(10, decode.int)
      use snooze_count <- decode.field(11, decode.int)
      use delivery_count <- decode.field(12, decode.int)
      use previous_state <- decode.field(13, decode.string)
      decode.success(Claim(
        id:,
        attempt_id:,
        epoch:,
        input_version:,
        encoded_input:,
        worker_id:,
        worker_version:,
        output_version:,
        error_version:,
        current_attempt:,
        max_attempts:,
        snooze_count:,
        delivery_count:,
        previous_state:,
      ))
    })
  let reference = diagnostics.queue_ref(queue, attempt_owner)
  let measured = store.execute_measured(query, on: connection)
  case
    diagnostics.checkout(
      forwarder,
      reference,
      diagnostic.ClaimCandidate,
      diagnostic.MainPool,
      measured,
    )
  {
    Error(error) -> {
      diagnostics.claim_failed(
        forwarder,
        reference,
        diagnostic.ClaimCandidate,
        error,
        measured.call_duration_us,
      )
      Error(postgres.QueueClaimFailed(error))
    }
    Ok(returned) ->
      case returned.rows {
        [] -> Ok(None)
        [claim] -> {
          let Claim(
            id: claimed_id,
            attempt_id:,
            epoch:,
            input_version:,
            encoded_input:,
            worker_id:,
            worker_version:,
            output_version:,
            error_version:,
            current_attempt:,
            max_attempts:,
            snooze_count:,
            previous_state:,
            ..,
          ) = claim
          case job.state_of_stored(previous_state) {
            Error(Nil) -> Nil
            Ok(previous_state) ->
              emit_claimed(
                forwarder,
                queue,
                claimed_id,
                worker_id,
                worker_version,
                attempt_id,
                epoch,
                current_attempt,
                previous_state,
              )
          }
          case registry.select(workers, queue, worker_id, worker_version) {
            Error(_) -> Error(postgres.QueueStorageInvariantViolated)
            Ok(#(registered_input, registered_output, registered_error, run)) ->
              case
                codec_contract_mismatch(
                  input_version,
                  registered_input,
                  output_version,
                  registered_output,
                  error_version,
                  registered_error,
                )
              {
                Some(#(kind, expected, actual)) -> {
                  case
                    mark_contract_mismatch(
                      database,
                      queue,
                      attempt_owner,
                      claim,
                      kind,
                      expected,
                      actual,
                    )
                  {
                    Error(error) -> Error(error)
                    Ok(True) ->
                      case codec_kind_of_stored(kind) {
                        Ok(kind) ->
                          Error(postgres.QueueCodecMismatch(
                            kind:,
                            expected:,
                            actual:,
                          ))
                        Error(Nil) ->
                          Error(postgres.QueueStorageInvariantViolated)
                      }
                    Ok(False) -> Error(postgres.QueueStorageInvariantViolated)
                  }
                }
                None ->
                  Ok(
                    Some(
                      ClaimedJob(claim:, run: fn() {
                        run(
                          input_version,
                          encoded_input,
                          worker.RetryContext(
                            current_attempt:,
                            max_attempts:,
                            snooze_count:,
                          ),
                        )
                      }),
                    ),
                  )
              }
          }
        }
        _ -> Error(postgres.QueueStorageInvariantViolated)
      }
  }
}

/// Builds and forwards `[grind, job, claimed]` for one freshly claimed row.
/// Called only after the claim's own fenced `UPDATE ... RETURNING` already
/// returned that row — an autocommitted statement, so a returned row is
/// already the proof of commit. Never blocks the caller: see
/// `grind/observation`'s module documentation for the forwarder's delivery
/// semantics.
fn emit_claimed(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  attempt_id: Int,
  epoch: Int,
  attempt: Int,
  previous_state: State,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.claimed(),
      observation.ClaimedMeasurements(count: 1),
      observation.ClaimedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
        previous_state:,
      ),
    )
  Nil
}

fn codec_contract_mismatch(
  stored_input: String,
  registered_input: String,
  stored_output: String,
  registered_output: String,
  stored_error: Option(String),
  registered_error: Option(String),
) -> Option(#(String, String, String)) {
  case stored_input == registered_input {
    False -> Some(#("input", stored_input, registered_input))
    True ->
      case stored_output == registered_output {
        False -> Some(#("output", stored_output, registered_output))
        True ->
          case stored_error == registered_error {
            False ->
              Some(#(
                "error",
                option_version(stored_error),
                option_version(registered_error),
              ))
            True -> None
          }
      }
  }
}

fn option_version(version: Option(String)) -> String {
  case version {
    Some(version) -> version
    None -> "none"
  }
}

fn mark_contract_mismatch(
  database: Database,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  kind: String,
  expected: String,
  actual: String,
) -> Result(Bool, postgres.QueueRunError) {
  let connection = postgres.connection(database)
  let forwarder = postgres.forwarder(database)
  let Claim(
    id:,
    attempt_id:,
    epoch:,
    worker_id:,
    worker_version:,
    current_attempt:,
    ..,
  ) = claim
  let query =
    pog.query(
      "UPDATE grind_jobs SET state = 'contract_mismatch', failure_description = $6, attempt_id = NULL, attempt_owner = NULL, lease_expires_at = NULL, attempt_count = GREATEST(attempt_count - 1, 0), finished_at = clock_timestamp() WHERE id = $1 AND queue = $2 AND state = 'executing' AND attempt_id = $3 AND attempt_epoch = $4 AND attempt_owner = $5 AND "
      <> lease.live_lease_predicate("clock_timestamp()")
      <> " RETURNING id",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(
      "stored "
      <> kind
      <> " codec version "
      <> expected
      <> " does not match registered version "
      <> actual,
    ))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(postgres.QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> {
          case codec_kind_of_stored(kind) {
            Error(Nil) -> Nil
            Ok(kind) ->
              emit_contract_mismatch(
                forwarder,
                queue,
                id,
                worker_id,
                worker_version,
                attempt_id,
                epoch,
                current_attempt,
                kind,
                expected,
                actual,
              )
          }
          Ok(True)
        }
        _ -> Error(postgres.QueueStorageInvariantViolated)
      }
  }
}

/// Builds and forwards `[grind, job, contract_mismatch_recorded]` for a claim released
/// because a registered worker's codec contract no longer matches what was
/// persisted at admission. Called only after this release's own fenced
/// `UPDATE ... RETURNING` already returned that row.
fn emit_contract_mismatch(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  attempt_id: Int,
  epoch: Int,
  attempt: Int,
  kind: worker.CodecKind,
  expected_version: String,
  actual_version: String,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.contract_mismatch_recorded(),
      observation.ContractMismatchMeasurements(count: 1),
      observation.ContractMismatchMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
        kind:,
        expected_version:,
        actual_version:,
      ),
    )
  Nil
}

fn codec_kind_of_stored(kind: String) -> Result(worker.CodecKind, Nil) {
  case kind {
    "input" -> Ok(worker.InputCodec)
    "output" -> Ok(worker.OutputCodec)
    "error" -> Ok(worker.ErrorCodec)
    _ -> Error(Nil)
  }
}

fn run_acknowledgement(
  database: Database,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  execution: worker.Execution,
  expected_output_version: String,
) -> Result(Bool, postgres.QueueRunError) {
  let connection = postgres.connection(database)
  let forwarder = postgres.forwarder(database)
  let Claim(
    id:,
    attempt_id:,
    epoch:,
    worker_id:,
    worker_version:,
    current_attempt:,
    ..,
  ) = claim
  let proposal = case execution {
    worker.ExecutedSuccess(version, encoded) ->
      AckProposal(
        proposed_state: "succeeded",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: Some(version),
        output: Some(encoded),
        error_version: None,
        error: None,
        failure_description: None,
      )
    worker.ExecutedBusinessFailure(version, encoded, description, cause) ->
      AckProposal(
        proposed_state: "business_failed",
        failure_cause: Some(worker.business_failure_cause_to_string(cause)),
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: version,
        error: encoded,
        failure_description: Some(description),
      )
    worker.ExecutedRetryable(version, encoded, description, delay_ms) ->
      AckProposal(
        proposed_state: "retryable",
        failure_cause: None,
        requested_delay_ms: Some(delay_ms),
        output_version: None,
        output: None,
        error_version: version,
        error: encoded,
        failure_description: Some(description),
      )
    worker.ExecutedInvalidInput(description) ->
      AckProposal(
        proposed_state: "runtime_failed",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(description),
      )
    worker.ExecutedUnencodable(codec, reason) ->
      AckProposal(
        proposed_state: "runtime_failed",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(worker.unencodable_description(codec, reason)),
      )
    worker.ExecutedSnoozed(delay_ms, reason) ->
      AckProposal(
        proposed_state: "snoozed",
        failure_cause: None,
        requested_delay_ms: Some(delay_ms),
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(reason),
      )
    worker.ExecutedDiscarded(reason) ->
      AckProposal(
        proposed_state: "discarded",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(reason),
      )
    worker.ExecutedCancelled(reason) ->
      AckProposal(
        proposed_state: "cancelled",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(reason),
      )
    worker.ExecutedUncertain(reason) ->
      AckProposal(
        proposed_state: "uncertain",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(reason),
      )
  }
  let command_id =
    attempt_acknowledgement.acknowledgement_command_id(id, attempt_id, epoch)
  let started_at = diagnostics.monotonic_us()
  let measured =
    store.transaction_measured(connection, fn(transaction) {
      attempt_acknowledgement.acknowledge_transaction(
        transaction,
        queue,
        attempt_owner,
        claim,
        command_id,
        proposal,
        execution,
        expected_output_version,
      )
    })
  let reference = diagnostics.queue_ref(queue, attempt_owner)
  let transaction_result =
    diagnostics.checkout(
      forwarder,
      reference,
      diagnostic.Acknowledge,
      diagnostic.MainPool,
      measured,
    )
  let resolved =
    attempt_acknowledgement.resolve_ack_transaction_result(
      connection,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
      execution,
      transaction_result,
      forwarder,
      reference,
    )
  let acknowledged = case resolved {
    Error(error) -> Error(error)
    Ok(commit) -> {
      case job.state_of_stored(commit.committed_state) {
        Error(Nil) -> Nil
        Ok(committed_state) -> {
          let confirmation = case commit.via_receipt_match {
            True -> observation.Reconciled
            False -> observation.Replied
          }
          emit_acknowledged(
            forwarder,
            queue,
            id,
            worker_id,
            worker_version,
            attempt_id,
            epoch,
            current_attempt,
            proposed_of_execution(execution),
            committed_state,
            observation_failure_cause(commit.failure_cause),
            commit.available_at_unix_ms,
            confirmation,
            command_id,
          )
        }
      }
      Ok(True)
    }
  }
  let outcome = case resolved {
    Ok(commit) ->
      case commit.via_receipt_match {
        True -> diagnostic.AckReconciled
        False -> diagnostic.AckReplied
      }
    Error(postgres.QueueAckUnknown(..)) -> diagnostic.AckUnknown
    Error(postgres.QueueAckStale(..)) -> diagnostic.AckFenceRejected
    Error(postgres.QueueAckCommandConflict) -> diagnostic.AckCommandConflict
    Error(_) ->
      case transaction_result {
        Error(pog.TransactionRolledBack(_)) -> diagnostic.AckRolledBack
        _ -> diagnostic.AckFailed
      }
  }
  let _ =
    forwarder.emit(
      forwarder,
      diagnostic.acknowledgement(),
      diagnostic.AcknowledgementMeasurements(
        count: 1,
        duration_us: diagnostics.monotonic_us() - started_at,
      ),
      diagnostic.AcknowledgementMetadata(
        context: diagnostic_context_from_claim(claim, queue, attempt_owner),
        command_id:,
        outcome:,
      ),
    )
  acknowledged
}

/// Builds and forwards `[grind, job, acknowledged]` from a proven-committed
/// `AckCommit`. Called only from `run_acknowledgement`, strictly after
/// `transaction_safely` (and, on a lost reply, `reconcile_unknown_ack`) has
/// already returned — never from inside a transaction callback. The
/// forwarder hand-off result is ignored: an observation is a diagnostic
/// side channel, never a policy decision, and its own failure or drop must
/// never affect a job's committed outcome.
fn emit_acknowledged(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  attempt_id: Int,
  epoch: Int,
  attempt: Int,
  proposed: observation.Proposed,
  committed_state: State,
  failure_cause: Option(worker.BusinessFailureCause),
  available_at_unix_ms: Option(Int),
  confirmation: observation.Confirmation,
  command_id: String,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.acknowledged(),
      observation.AcknowledgedMeasurements(count: 1),
      observation.AcknowledgedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
        proposed:,
        committed_state:,
        failure_cause:,
        available_at_unix_ms:,
        confirmation:,
        command_id:,
      ),
    )
  Nil
}

fn proposed_of_execution(execution: worker.Execution) -> observation.Proposed {
  case execution {
    worker.ExecutedSuccess(_, _) -> observation.ProposedSuccess
    worker.ExecutedBusinessFailure(_, _, _, _) ->
      observation.ProposedBusinessFailure
    worker.ExecutedRetryable(_, _, _, _) -> observation.ProposedRetryable
    worker.ExecutedInvalidInput(_) -> observation.ProposedRuntimeFailed
    worker.ExecutedUnencodable(_, _) -> observation.ProposedRuntimeFailed
    worker.ExecutedSnoozed(_, _) -> observation.ProposedSnoozed
    worker.ExecutedDiscarded(_) -> observation.ProposedDiscarded
    worker.ExecutedCancelled(_) -> observation.ProposedCancelled
    worker.ExecutedUncertain(_) -> observation.ProposedUncertain
  }
}

fn observation_failure_cause(
  raw: Option(String),
) -> Option(worker.BusinessFailureCause) {
  case raw {
    Some(raw) ->
      option.from_result(worker.business_failure_cause_from_string(raw))
    None -> None
  }
}

/// The stable acknowledgement command ID for one attempt fence. Exposed only
/// so tests can construct the same ID an internal reconciliation path would.
pub fn acknowledgement_command_id(
  job_id: Int,
  attempt_id: Int,
  epoch: Int,
) -> String {
  attempt_acknowledgement.acknowledgement_command_id(job_id, attempt_id, epoch)
}
