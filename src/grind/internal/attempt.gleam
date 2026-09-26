//// The claim/renew/acknowledge protocol driving one attempt's lifecycle
//// against `grind_jobs`: `claim_one` claims a due row, `execute_claim` runs
//// its typed handler, `renew` extends the lease while it runs, and
//// `acknowledge` commits the proposed outcome through the same attempt
//// fence `claim_one` established. `grind/postgres` keeps the shared
//// `QueueRunError`/`AckRejection` result types this module's public
//// functions return, plus the `connection`/`storage_owner`/`forwarder`
//// accessors this module reads a `Database` through, since `Database` is
//// opaque outside `grind/postgres` (which defines it) and this module
//// cannot pattern-match its fields directly.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
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

type Claim {
  Claim(
    id: Int,
    attempt_id: Int,
    epoch: Int,
    input_version: String,
    encoded_input: String,
    worker_id: String,
    worker_version: String,
    output_version: String,
    error_version: Option(String),
    current_attempt: Int,
    max_attempts: Int,
    snooze_count: Int,
    delivery_count: Int,
    previous_state: String,
  )
}

type AckProposal {
  AckProposal(
    proposed_state: String,
    failure_cause: Option(String),
    requested_delay_ms: Option(Int),
    output_version: Option(String),
    output: Option(String),
    error_version: Option(String),
    error: Option(String),
    failure_description: Option(String),
  )
}

/// The proven-committed disposition of one acknowledgement command, carried
/// from wherever it was proven (a fresh write, or a durable receipt read
/// back matching this exact command) up to `acknowledge`, which is the only
/// place that emits `[grind, job, acknowledged]` — never from inside a
/// transaction callback. `via_receipt_match: True` means this call's own
/// transaction did not write anything new; the exact same command was
/// already durably applied, so the observation's `confirmation` is
/// `Reconciled` rather than `Replied`. `available_at_unix_ms` is only ever
/// known for a fresh write (read from that write's own `RETURNING`); a
/// receipt match cannot recover it, since `grind_job_acknowledgements` does
/// not retain `available_at`.
type AckCommit {
  AckCommit(
    committed_state: String,
    failure_cause: Option(String),
    available_at_unix_ms: Option(Int),
    via_receipt_match: Bool,
  )
}

/// A claimed row bound to its registered typed execution closure.
pub opaque type ClaimedJob {
  ClaimedJob(claim: Claim, run: fn() -> worker.Execution)
}

/// Atomically claims one due row without running its handler in the caller.
/// The queue actor owns renewal and acknowledgement after this boundary.
pub fn claim_one(
  database: Database,
  queue: String,
  workers: Registry,
  attempt_owner: String,
  lease_duration_ms: Int,
) -> Result(Option(ClaimedJob), postgres.QueueRunError) {
  case
    lease.quarantine_expired_in_queue(
      postgres.connection(database),
      postgres.storage_owner(database),
      postgres.forwarder(database),
      queue,
    )
  {
    Error(error) -> Error(postgres.QueueClaimFailed(error))
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
  let storage_owner = postgres.storage_owner(database)
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() + ($7::double precision * interval '1 millisecond') WHERE id = $1 AND storage_owner = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND "
      <> lease.live_lease_predicate("clock_timestamp()")
      <> " RETURNING id",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(storage_owner))
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
  let storage_owner = postgres.storage_owner(database)
  let forwarder = postgres.forwarder(database)
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  case
    store.call_safely(connection, fn(connection) {
      sql.release_unstarted_claim(
        connection,
        id,
        storage_owner,
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
    worker.ExecutedInvalidInput(_) ->
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
  let storage_owner = postgres.storage_owner(database)
  let forwarder = postgres.forwarder(database)
  let eligibility =
    list.index_map(identities, fn(_, index) {
      let id_parameter = 5 + index * 2
      let version_parameter = id_parameter + 1
      "(worker_id = $"
      <> int.to_string(id_parameter)
      <> " AND worker_version = $"
      <> int.to_string(version_parameter)
      <> ")"
    })
    |> string.join(" OR ")
  let eligible_state =
    "state IN ('queued', 'scheduled', 'retryable') AND available_at <= clock_timestamp()"
  let sql =
    "WITH candidate AS (SELECT id, state AS previous_state FROM grind_jobs WHERE storage_owner = $1 AND queue = $2 AND "
    <> eligible_state
    <> " AND cancel_requested_at IS NULL AND ("
    <> eligibility
    <> ") ORDER BY available_at, id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE grind_jobs AS job SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = job.attempt_epoch + 1, attempt_owner = $3, lease_expires_at = clock_timestamp() + ($4::double precision * interval '1 millisecond'), attempt_count = job.attempt_count + 1, delivery_count = job.delivery_count + 1 FROM candidate WHERE job.id = candidate.id RETURNING job.id, job.attempt_id, job.attempt_epoch, job.input_version, job.input::text, job.worker_id, job.worker_version, job.output_version, job.error_version, job.attempt_count, job.max_attempts, job.snooze_count, job.delivery_count, candidate.previous_state"
  let parameters =
    list.append(
      [
        pog.text(storage_owner),
        pog.text(queue),
        pog.text(attempt_owner),
        pog.int(lease_duration_ms),
      ],
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
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(postgres.QueueClaimFailed(error))
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
  let storage_owner = postgres.storage_owner(database)
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
      "UPDATE grind_jobs SET state = 'contract_mismatch', failure_description = $7, attempt_id = NULL, attempt_owner = NULL, lease_expires_at = NULL, attempt_count = GREATEST(attempt_count - 1, 0) WHERE id = $1 AND storage_owner = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND "
      <> lease.live_lease_predicate("clock_timestamp()")
      <> " RETURNING id",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(storage_owner))
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
  let storage_owner = postgres.storage_owner(database)
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
  let command_id = acknowledgement_command_id(id, attempt_id, epoch)
  let transaction_result =
    store.transaction_safely(connection, fn(transaction) {
      acknowledge_transaction(
        transaction,
        storage_owner,
        queue,
        attempt_owner,
        claim,
        command_id,
        proposal,
        execution,
        expected_output_version,
      )
    })
  case
    resolve_ack_transaction_result(
      connection,
      storage_owner,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
      execution,
      transaction_result,
    )
  {
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

fn resolve_ack_transaction_result(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
  transaction_result: Result(
    AckCommit,
    pog.TransactionError(postgres.QueueRunError),
  ),
) -> Result(AckCommit, postgres.QueueRunError) {
  case transaction_result {
    Ok(commit) -> Ok(commit)
    Error(pog.TransactionRolledBack(error)) -> Error(error)
    Error(pog.TransactionQueryError(_)) ->
      reconcile_unknown_ack(
        connection,
        storage_owner,
        queue,
        attempt_owner,
        claim,
        command_id,
        proposal,
        execution,
      )
  }
}

fn reconcile_unknown_ack(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
) -> Result(AckCommit, postgres.QueueRunError) {
  case
    matching_acknowledgement(
      connection,
      storage_owner,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
    )
  {
    Ok(Some(#(True, committed_state, failure_cause))) ->
      Ok(AckCommit(
        committed_state:,
        failure_cause:,
        available_at_unix_ms: None,
        via_receipt_match: True,
      ))
    Ok(Some(#(False, _, _))) -> Error(postgres.QueueAckCommandConflict)
    Ok(None) | Error(_) ->
      Error(postgres.QueueAckUnknown(command_id, execution))
  }
}

fn acknowledge_transaction(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
  expected_output_version: String,
) -> Result(AckCommit, postgres.QueueRunError) {
  case
    matching_acknowledgement(
      connection,
      storage_owner,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
    )
  {
    Error(error) -> Error(error)
    Ok(Some(#(True, committed_state, failure_cause))) ->
      Ok(AckCommit(
        committed_state:,
        failure_cause:,
        available_at_unix_ms: None,
        via_receipt_match: True,
      ))
    Ok(Some(#(False, _, _))) -> Error(postgres.QueueAckCommandConflict)
    Ok(None) -> {
      let Claim(id:, attempt_id:, epoch:, error_version:, ..) = claim
      let AckProposal(
        proposed_state:,
        failure_cause:,
        output_version: _,
        requested_delay_ms:,
        output:,
        error_version: proposed_error_version,
        error:,
        failure_description:,
      ) = proposal
      let #(sql, parameters) = case proposed_state {
        "succeeded" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'succeeded' END, output = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $1::jsonb END, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE NULL END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND output_version = $8 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, output),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.text(expected_output_version),
          ],
        )
        "business_failed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'business_failed' END, output = NULL, error = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $1::jsonb END, error_version = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $2 END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $3 END, failure_cause = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $4 END, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $5 AND storage_owner = $6 AND queue = $7 AND state = 'executing' AND attempt_id = $8 AND attempt_epoch = $9 AND attempt_owner = $10 AND error_version IS NOT DISTINCT FROM $11 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " AND (cancel_requested_at IS NOT NULL OR (($4 = 'budget_exhausted' AND attempt_count >= max_attempts) OR ($4 = 'retry_declined' AND attempt_count < max_attempts))) RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, error),
            pog.nullable(pog.text, proposed_error_version),
            pog.nullable(pog.text, failure_description),
            pog.nullable(pog.text, failure_cause),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.nullable(pog.text, error_version),
          ],
        )
        "retryable" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'retryable' END, available_at = CASE WHEN cancel_requested_at IS NOT NULL THEN available_at ELSE clock_timestamp() + ($1::double precision * interval '1 millisecond') END, output = NULL, error = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $2::jsonb END, error_version = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $3 END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $4 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $5 AND storage_owner = $6 AND queue = $7 AND state = 'executing' AND attempt_id = $8 AND attempt_epoch = $9 AND attempt_owner = $10 AND error_version IS NOT DISTINCT FROM $11 AND (attempt_count < max_attempts OR cancel_requested_at IS NOT NULL) AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.int, requested_delay_ms),
            pog.nullable(pog.text, error),
            pog.nullable(pog.text, proposed_error_version),
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.nullable(pog.text, error_version),
          ],
        )
        "snoozed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'scheduled' END, available_at = CASE WHEN cancel_requested_at IS NOT NULL THEN available_at ELSE clock_timestamp() + ($1::double precision * interval '1 millisecond') END, snooze_count = CASE WHEN cancel_requested_at IS NOT NULL THEN snooze_count ELSE snooze_count + 1 END, attempt_count = CASE WHEN cancel_requested_at IS NOT NULL THEN attempt_count ELSE GREATEST(attempt_count - 1, 0) END, output = NULL, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $2 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $3 AND storage_owner = $4 AND queue = $5 AND state = 'executing' AND attempt_id = $6 AND attempt_epoch = $7 AND attempt_owner = $8 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.int, requested_delay_ms),
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "discarded" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'discarded' END, output = NULL, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "cancelled" -> #(
          "UPDATE grind_jobs SET state = 'cancelled', output = NULL, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "uncertain" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'uncertain' END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, uncertain_at = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE clock_timestamp() END, attempt_owner = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE attempt_owner END, lease_expires_at = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE lease_expires_at END, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "runtime_failed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'runtime_failed' END, output = NULL, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        _ -> #("SELECT 1 WHERE FALSE", [])
      }
      let query =
        list.fold(parameters, pog.query(sql), fn(query, parameter) {
          pog.parameter(query, parameter)
        })
        |> pog.returning({
          use updated_id <- decode.field(0, decode.int)
          use committed_state <- decode.field(1, decode.string)
          use committed_description <- decode.field(
            2,
            decode.optional(decode.string),
          )
          use committed_available_at_ms <- decode.field(3, decode.int)
          decode.success(#(
            updated_id,
            committed_state,
            committed_description,
            committed_available_at_ms,
          ))
        })
      case store.execute_safely(query, on: connection) {
        Error(error) -> Error(postgres.QueueAckFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] ->
              // A duplicate ACK can wait behind the first writer's row lock.
              // Re-read its receipt after the UPDATE observes the committed
              // state instead of misreporting that exact retry as stale.
              case
                matching_acknowledgement(
                  connection,
                  storage_owner,
                  queue,
                  attempt_owner,
                  claim,
                  command_id,
                  proposal,
                )
              {
                Ok(Some(#(True, committed_state, failure_cause))) ->
                  Ok(AckCommit(
                    committed_state:,
                    failure_cause:,
                    available_at_unix_ms: None,
                    via_receipt_match: True,
                  ))
                Ok(Some(#(False, _, _))) ->
                  Error(postgres.QueueAckCommandConflict)
                Ok(None) ->
                  current_ack_rejection(
                    connection,
                    storage_owner,
                    queue,
                    attempt_owner,
                    claim,
                    execution,
                  )
                Error(error) -> Error(error)
              }
            [#(_, committed_state, _committed_description, available_at_ms)] ->
              insert_acknowledgement(
                connection,
                storage_owner,
                queue,
                attempt_owner,
                claim,
                command_id,
                proposal,
                committed_state,
                available_at_for_observation(committed_state, available_at_ms),
              )
            _ -> Error(postgres.QueueStorageInvariantViolated)
          }
      }
    }
  }
}

/// `available_at` is only a meaningful "next eligibility" signal for a
/// *committed* `retryable`/`scheduled` outcome — read from the acknowledge
/// UPDATE's own `RETURNING`, never from the proposal. A proposed
/// retry/snooze whose `available_at` write was itself overridden (a
/// concurrent cancellation commits `cancelled` instead, leaving
/// `available_at` at its unrelated pre-ack value) must not report that
/// stale value as if it were a real next-eligibility time; gating on the
/// committed state, not the proposed one, is what keeps this correct. For
/// every other outcome the column still holds a value (it is `NOT NULL`),
/// but that value is not what an observer means by "when does this job
/// become eligible again", so it is not surfaced there.
fn available_at_for_observation(
  committed_state: String,
  available_at_ms: Int,
) -> Option(Int) {
  case committed_state {
    "retryable" | "scheduled" -> Some(available_at_ms)
    _ -> None
  }
}

fn current_ack_rejection(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  execution: worker.Execution,
) -> Result(AckCommit, postgres.QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "SELECT state, attempt_id, attempt_epoch, attempt_owner, "
      <> lease.live_lease_predicate("clock_timestamp()")
      <> " FROM grind_jobs WHERE id = $1 AND storage_owner = $2 AND queue = $3",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use current_attempt <- decode.field(1, decode.optional(decode.int))
      use current_epoch <- decode.field(2, decode.optional(decode.int))
      use current_owner <- decode.field(3, decode.optional(decode.string))
      use lease_live <- decode.field(4, decode.bool)
      decode.success(#(
        state,
        current_attempt,
        current_epoch,
        current_owner,
        lease_live,
      ))
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(postgres.QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] ->
          Error(postgres.QueueAckStale(execution, postgres.AckRecordMissing))
        [#(state, current_attempt, current_epoch, current_owner, lease_live)] ->
          case state {
            "executing" ->
              case
                current_attempt == Some(attempt_id)
                && current_epoch == Some(epoch)
                && current_owner == Some(attempt_owner)
              {
                False ->
                  Error(postgres.QueueAckStale(
                    execution,
                    postgres.AckOwnershipChanged(
                      attempt_id: current_attempt,
                      epoch: current_epoch,
                      owner: current_owner,
                    ),
                  ))
                True ->
                  case lease_live {
                    False ->
                      Error(postgres.QueueAckStale(
                        execution,
                        postgres.AckLeaseExpired(
                          attempt_id,
                          epoch,
                          attempt_owner,
                        ),
                      ))
                    True ->
                      Error(postgres.QueueAckStale(
                        execution,
                        postgres.AckRecordChanged,
                      ))
                  }
              }
            _ ->
              case job.state_of_stored(state) {
                Ok(decoded_state) ->
                  Error(postgres.QueueAckStale(
                    execution,
                    postgres.AckStateChanged(decoded_state),
                  ))
                Error(Nil) -> Error(postgres.QueueStorageInvariantViolated)
              }
          }
        _ -> Error(postgres.QueueStorageInvariantViolated)
      }
  }
}

/// The stable acknowledgement command ID for one attempt fence. Exposed only
/// so tests can construct the same ID an internal reconciliation path would,
/// without re-encoding this format themselves.
pub fn acknowledgement_command_id(
  job_id: Int,
  attempt_id: Int,
  epoch: Int,
) -> String {
  "grind-ack:"
  <> int.to_string(job_id)
  <> ":"
  <> int.to_string(attempt_id)
  <> ":"
  <> int.to_string(epoch)
}

/// The fixed-order envelope deliberately hashes PostgreSQL's JSONB rendering,
/// not a cross-runtime canonical JSON representation. Presence flags keep SQL
/// NULL distinct from JSON null. Large numeric textual variants may conflict.
fn acknowledgement_fingerprint_sql(first_parameter: Int) -> String {
  let proposed_state = sql_parameter(first_parameter, "text")
  let output_version = sql_parameter(first_parameter + 1, "text")
  let output = sql_parameter(first_parameter + 2, "text")
  let error_version = sql_parameter(first_parameter + 3, "text")
  let error = sql_parameter(first_parameter + 4, "text")
  let reason = sql_parameter(first_parameter + 5, "text")
  let delay = sql_parameter(first_parameter + 6, "bigint")
  let cause = sql_parameter(first_parameter + 7, "text")
  "sha256(convert_to(jsonb_build_array('grind-ack-proposal-v1', "
  <> proposed_state
  <> ", "
  <> output_version
  <> " IS NOT NULL, "
  <> output_version
  <> ", "
  <> output
  <> " IS NOT NULL, CASE WHEN "
  <> output
  <> " IS NULL THEN NULL::jsonb ELSE "
  <> output
  <> "::jsonb END, "
  <> error_version
  <> " IS NOT NULL, "
  <> error_version
  <> ", "
  <> error
  <> " IS NOT NULL, CASE WHEN "
  <> error
  <> " IS NULL THEN NULL::jsonb ELSE "
  <> error
  <> "::jsonb END, "
  <> reason
  <> " IS NOT NULL, "
  <> reason
  <> ", "
  <> delay
  <> " IS NOT NULL, "
  <> delay
  <> ", "
  <> cause
  <> " IS NOT NULL, "
  <> cause
  <> ")::text, 'UTF8'))"
}

fn sql_parameter(index: Int, cast: String) -> String {
  "$" <> int.to_string(index) <> "::" <> cast
}

/// Looks up the exact acknowledgement row for `command_id`, if one exists,
/// and reports both whether it matches this exact proposal and the
/// disposition it actually committed — so a caller that finds a match can
/// build the `Reconciled` observation from a real receipt read rather than
/// re-deriving it from this call's own (possibly stale) proposal.
fn matching_acknowledgement(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
) -> Result(Option(#(Bool, String, Option(String))), postgres.QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  let AckProposal(
    proposed_state:,
    failure_cause:,
    requested_delay_ms:,
    output_version:,
    output:,
    error_version:,
    error:,
    failure_description:,
  ) = proposal
  let query =
    pog.query(
      "SELECT storage_owner = $1 AND command_id = $2 AND queue = $3 AND job_id = $4 AND worker_id = $5 AND worker_version = $6 AND attempt_id = $7 AND attempt_epoch = $8 AND attempt_owner = $9 AND proposal_sha256 = "
      <> acknowledgement_fingerprint_sql(10)
      <> ", committed_state, failure_cause FROM grind_job_acknowledgements WHERE storage_owner = $1 AND command_id = $2",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(command_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(proposed_state))
    |> pog.parameter(pog.nullable(pog.text, output_version))
    |> pog.parameter(pog.nullable(pog.text, output))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.nullable(pog.int, requested_delay_ms))
    |> pog.parameter(pog.nullable(pog.text, failure_cause))
    |> pog.returning({
      use matches <- decode.field(0, decode.bool)
      use committed_state <- decode.field(1, decode.string)
      use committed_failure_cause <- decode.field(
        2,
        decode.optional(decode.string),
      )
      decode.success(#(matches, committed_state, committed_failure_cause))
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(postgres.QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Ok(None)
        [row] -> Ok(Some(row))
        _ -> Error(postgres.QueueStorageInvariantViolated)
      }
  }
}

fn insert_acknowledgement(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  actual_committed_state: String,
  available_at_unix_ms: Option(Int),
) -> Result(AckCommit, postgres.QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  let AckProposal(
    proposed_state:,
    failure_cause:,
    requested_delay_ms:,
    output_version:,
    output:,
    error_version:,
    error:,
    failure_description:,
  ) = proposal
  let committed_failure_cause = case actual_committed_state {
    "cancelled" -> None
    _ -> failure_cause
  }
  let query =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, failure_cause, proposal_sha256) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $18, $19, "
      <> acknowledgement_fingerprint_sql(10)
      <> ") RETURNING command_id",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(command_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(proposed_state))
    |> pog.parameter(pog.nullable(pog.text, output_version))
    |> pog.parameter(pog.nullable(pog.text, output))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.nullable(pog.int, requested_delay_ms))
    |> pog.parameter(pog.nullable(pog.text, failure_cause))
    |> pog.parameter(pog.text(actual_committed_state))
    |> pog.parameter(pog.nullable(pog.text, committed_failure_cause))
    |> pog.returning({
      use inserted_command_id <- decode.field(0, decode.string)
      decode.success(inserted_command_id)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(postgres.QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] ->
          Ok(AckCommit(
            committed_state: actual_committed_state,
            failure_cause: committed_failure_cause,
            available_at_unix_ms:,
            via_receipt_match: False,
          ))
        _ -> Error(postgres.QueueStorageInvariantViolated)
      }
  }
}
