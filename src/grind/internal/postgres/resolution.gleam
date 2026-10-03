//// Audited resolution of uncertain jobs. The receipt check, row lock,
//// fenced write, and commit result stay in one transaction protocol.

import gleam/option.{type Option, None, Some}
import gleam/result
import grind/internal/convert
import grind/internal/events
import grind/internal/job.{type JobHandle, type State, Queued, Scheduled}
import grind/internal/postgres/resolution_queries.{
  type ResolutionCommand, ResolutionCommand,
} as postgres_resolution_queries
import grind/internal/store
import grind/telemetry
import pog
import sinal/correlation.{type Correlation}
import sinal/forwarder.{type Forwarder}

pub type Decision(output, error) {
  ConfirmSuccess(output)
  ConfirmBusinessFailure(error)
  AuthorizeReplay
}

pub type Request(output, error) {
  Request(
    resolution_id: String,
    resolved_by: String,
    details: String,
    decision: Decision(output, error),
  )
}

pub type ResolutionResult {
  ResolutionApplied(State)
  ResolutionAlreadyApplied(State)
}

pub type ResolutionError {
  ReconciliationQueryFailed(pog.QueryError)
  ReconciliationNotRequired
  ResolutionCommandConflict
  ResolutionRouteMismatch
  ResolutionWorkerContractMismatch
  ResolutionCodecMismatch
  ResolutionRequiresErrorCodec
  ResolutionInvalidValue(reason: String)
  ResolutionCancellationPending
  ResolutionAttemptMetadataMissing
  ResolutionWriteRejected
  ResolutionCommitUnknown(resolution_id: String)
}

pub fn resolve_uncertain(
  connection: pog.Connection,
  fwd: Forwarder,
  handle: JobHandle(input, output, error),
  request: Request(output, error),
) -> Result(ResolutionResult, ResolutionError) {
  let Request(resolution_id:, resolved_by:, details:, decision:) = request
  {
    let #(_, _, _, _, _, bound_output_version, _) =
      job.reconciliation_fields(handle)
    use
      #(
        decision,
        state,
        output_version,
        encoded_output,
        error_version,
        encoded_error,
        failure_description,
      )
    <- result.try(case decision {
      ConfirmSuccess(value) -> {
        use #(version, encoded) <- result.map(
          job.encode_reconciled_success(handle, value)
          |> result.map_error(ResolutionInvalidValue),
        )
        #(
          "confirm_success",
          "succeeded",
          version,
          Some(encoded),
          None,
          None,
          None,
        )
      }
      ConfirmBusinessFailure(value) ->
        case job.encode_reconciled_error(handle, value) {
          None -> Error(ResolutionRequiresErrorCodec)
          Some(Error(reason)) -> Error(ResolutionInvalidValue(reason))
          Some(Ok(#(version, encoded))) ->
            Ok(#(
              "confirm_business_failure",
              "business_failed",
              bound_output_version,
              None,
              Some(version),
              Some(encoded),
              Some(details),
            ))
        }
      AuthorizeReplay ->
        Ok(#(
          "authorize_replay",
          "queued",
          bound_output_version,
          None,
          None,
          None,
          None,
        ))
    })
    let #(
      id,
      _,
      queue,
      worker_id,
      worker_version,
      expected_output_version,
      expected_error_version,
    ) = job.reconciliation_fields(handle)
    let command =
      ResolutionCommand(
        id:,
        queue:,
        worker_id:,
        worker_version:,
        expected_output_version:,
        expected_error_version:,
        resolution_id:,
        resolved_by:,
        details:,
        decision:,
        target_state: state,
        output_version:,
        encoded_output:,
        error_version:,
        encoded_error:,
        failure_description:,
      )
    case
      store.transaction_safely(connection, fn(transaction) {
        reconcile_transaction(transaction, command)
      })
    {
      Ok(result) -> {
        case resolution_decision_of_stored(decision) {
          Error(Nil) -> Nil
          Ok(decision) -> {
            let #(committed_state, confirmation) = case result {
              ResolutionApplied(state) -> #(state, telemetry.Replied)
              ResolutionAlreadyApplied(state) -> #(state, telemetry.Reconciled)
            }
            emit_resolved(
              fwd,
              events.read_correlation(connection, id),
              queue,
              id,
              worker_id,
              worker_version,
              decision,
              committed_state,
              resolution_id,
              resolved_by,
              confirmation,
            )
          }
        }
        Ok(result)
      }
      Error(pog.TransactionQueryError(_)) ->
        Error(ResolutionCommitUnknown(resolution_id))
      Error(pog.TransactionRolledBack(error)) -> Error(error)
    }
  }
}

/// Builds and forwards `[grind, job, resolved]` from a proven-committed
/// `ResolutionResult`. Called only from `resolve_uncertain`, strictly after
/// `transaction_safely` has already returned — never from inside a
/// transaction callback. `ResolutionApplied` is this call's own fresh commit
/// (`Replied`); `ResolutionAlreadyApplied` is a prior commit of this exact
/// `resolution_id` proven by a receipt read (`Reconciled`) — see
/// `resolution_receipt_outcome`.
fn emit_resolved(
  fwd: Forwarder,
  correlation: Correlation,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  decision: telemetry.ResolutionDecision,
  committed_state: State,
  resolution_id: String,
  resolved_by: String,
  confirmation: telemetry.Confirmation,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      telemetry.resolved(),
      events.job_measurements(),
      telemetry.ResolvedMetadata(
        ref: telemetry.JobRef(
          job_id:,
          queue:,
          worker_id:,
          worker_version:,
          correlation:,
        ),
        decision:,
        committed_state: convert.state(committed_state),
        resolution_id:,
        resolved_by:,
        confirmation:,
      ),
    )
  Nil
}

fn resolution_decision_of_stored(
  decision: String,
) -> Result(telemetry.ResolutionDecision, Nil) {
  case decision {
    "confirm_success" -> Ok(telemetry.DecisionConfirmSuccess)
    "confirm_business_failure" -> Ok(telemetry.DecisionConfirmBusinessFailure)
    "authorize_replay" -> Ok(telemetry.DecisionAuthorizeReplay)
    _ -> Error(Nil)
  }
}

fn reconcile_transaction(
  connection: pog.Connection,
  command: ResolutionCommand,
) -> Result(ResolutionResult, ResolutionError) {
  case resolution_receipt_outcome(connection, command) {
    Error(error) -> Error(error)
    Ok(Some(result)) -> Ok(result)
    Ok(None) -> apply_uncertain_resolution(connection, command)
  }
}

/// Looks up an existing resolution receipt for `command`'s `resolution_id`
/// and, if one exists, checks it matches this exact command. `Ok(None)`
/// means no receipt exists yet — the caller decides what to do (apply a
/// fresh resolution, or — the second, post-lock call site in
/// `apply_uncertain_resolution` below — report that reconciliation is
/// genuinely not required). Shared by two call sites deliberately: this is
/// the exact "re-read the receipt instead of misreporting a concurrent
/// retry as stale" pattern the acknowledgement path already uses
/// (`acknowledge_transaction`'s re-read of `matching_acknowledgement` after
/// a 0-row fenced `UPDATE`), applied here to `resolve_uncertain`'s
/// analogous race — see `docs/RECOVERY-EVIDENCE.md`, "Concurrent audited
/// resolution".
fn resolution_receipt_outcome(
  connection: pog.Connection,
  command: ResolutionCommand,
) -> Result(Option(ResolutionResult), ResolutionError) {
  let ResolutionCommand(
    id:,
    queue:,
    worker_id:,
    worker_version:,
    resolution_id:,
    resolved_by:,
    details:,
    decision:,
    target_state:,
    output_version:,
    encoded_output:,
    error_version:,
    encoded_error:,
    ..,
  ) = command
  let #(payload_version, payload) = case decision {
    "confirm_success" -> #(Some(output_version), encoded_output)
    "confirm_business_failure" -> #(error_version, encoded_error)
    _ -> #(None, None)
  }
  case find_resolution(connection, resolution_id, payload) {
    Error(error) -> Error(error)
    Ok(None) -> Ok(None)
    Ok(Some(#(
      job_id,
      old_queue,
      stored_worker_id,
      stored_worker_version,
      old_decision,
      old_resolver,
      old_details,
      old_target_state,
      old_payload_version,
      payload_match,
    ))) ->
      case
        job_id == id
        && old_queue == queue
        && stored_worker_id == Some(worker_id)
        && stored_worker_version == Some(worker_version)
        && old_decision == decision
        && old_resolver == resolved_by
        && old_details == details
        && old_target_state == target_state
        && old_payload_version == payload_version
        && payload_match == "same"
      {
        False -> Error(ResolutionCommandConflict)
        True ->
          resolution_state(old_target_state)
          |> result.map(fn(state) { Some(ResolutionAlreadyApplied(state)) })
      }
  }
}

fn find_resolution(
  connection: pog.Connection,
  resolution_id: String,
  payload: Option(String),
) -> Result(
  Option(
    #(
      Int,
      String,
      Option(String),
      Option(String),
      String,
      String,
      String,
      String,
      Option(String),
      String,
    ),
  ),
  ResolutionError,
) {
  let query =
    postgres_resolution_queries.find_resolution_query(resolution_id, payload)
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(ReconciliationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Ok(None)
        [resolution] -> Ok(Some(resolution))
        _ -> Error(ResolutionCommandConflict)
      }
  }
}

fn resolution_state(state: String) -> Result(State, ResolutionError) {
  case state {
    "queued" -> Ok(Queued)
    "scheduled" -> Ok(Scheduled)
    "executing" -> Ok(job.Executing)
    "succeeded" -> Ok(job.Succeeded)
    "business_failed" -> Ok(job.BusinessFailed)
    "runtime_failed" -> Ok(job.RuntimeFailed)
    "contract_mismatch" -> Ok(job.ContractMismatch)
    "uncertain" -> Ok(job.Uncertain)
    "discarded" -> Ok(job.Discarded)
    "cancelled" -> Ok(job.Cancelled)
    _ -> Error(ReconciliationNotRequired)
  }
}

fn apply_uncertain_resolution(
  connection: pog.Connection,
  command: ResolutionCommand,
) -> Result(ResolutionResult, ResolutionError) {
  let ResolutionCommand(
    id:,
    queue:,
    worker_id:,
    worker_version:,
    expected_output_version:,
    expected_error_version:,
    decision:,
    ..,
  ) = command
  // `FOR NO KEY UPDATE`, not `FOR UPDATE`: this row lock's own later
  // `UPDATE grind_jobs` (in `write_resolution`, below) never touches a key
  // column (`id`, `worker_id`, `worker_version`, `unique_key_contract`,
  // `unique_key_sha256` — the columns any unique index on `grind_jobs`
  // covers, all of which stay in that `UPDATE`'s `WHERE`, never its `SET`),
  // so the weaker mode is exactly as safe and does not conflict with
  // `unique_admission_query.candidate_sql`'s own `FOR KEY SHARE` on a
  // `KeepExisting` uniqueness candidate — a plain `FOR UPDATE` here would
  // otherwise make an audited resolution spuriously contend
  // (`AdmissionContended`) with an unrelated admission reading the exact
  // same row for a reason that was never actually incompatible with this
  // resolution's own write. See `docs/UNIQUENESS-CONTRACT.md`, "Admission
  // transaction" step 6, for the full contention picture across claim,
  // cancel, quarantine, and now resolution.
  let select = postgres_resolution_queries.lock_uncertain_query(id)
  use stored <- result.try(case store.execute_safely(select, on: connection) {
    Error(error) -> Error(ReconciliationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [stored] -> Ok(stored)
        _ -> Error(ReconciliationNotRequired)
      }
  })
  let #(
    stored_queue,
    stored_worker,
    stored_worker_version,
    stored_state,
    attempt_id,
    attempt_epoch,
    attempt_owner,
    lease_expires_at,
    stored_output_version,
    stored_error_version,
    cancel_requested,
  ) = stored
  let codec_matches = case decision {
    "authorize_replay" -> True
    "confirm_success" -> stored_output_version == expected_output_version
    "confirm_business_failure" ->
      stored_output_version == expected_output_version
      && stored_error_version == expected_error_version
    _ -> False
  }
  case stored_queue == queue {
    False -> Error(ResolutionRouteMismatch)
    True ->
      case
        stored_worker == worker_id && stored_worker_version == worker_version
      {
        False -> Error(ResolutionWorkerContractMismatch)
        True ->
          case stored_state == "uncertain" {
            // The row is no longer `uncertain` — either genuinely no
            // reconciliation is needed, or (the race this re-check exists
            // for) a concurrent call for this exact command won and already
            // committed while this call waited on the row lock just above.
            // Re-reading the receipt here, rather than assuming the former,
            // is the same "re-read instead of misreporting a concurrent
            // retry as stale" pattern `acknowledge_transaction` already
            // uses.
            False ->
              case resolution_receipt_outcome(connection, command) {
                Error(error) -> Error(error)
                Ok(Some(result)) -> Ok(result)
                Ok(None) -> Error(ReconciliationNotRequired)
              }
            True ->
              case codec_matches {
                False -> Error(ResolutionCodecMismatch)
                True ->
                  case decision == "authorize_replay" && cancel_requested {
                    True -> Error(ResolutionCancellationPending)
                    False ->
                      case attempt_id, attempt_owner, lease_expires_at {
                        Some(attempt_id), Some(attempt_owner), Some(_) ->
                          write_resolution(
                            connection,
                            command,
                            attempt_id,
                            attempt_epoch,
                            attempt_owner,
                          )
                        _, _, _ -> Error(ResolutionAttemptMetadataMissing)
                      }
                  }
              }
          }
      }
  }
}

fn write_resolution(
  connection: pog.Connection,
  command: ResolutionCommand,
  attempt_id: Int,
  attempt_epoch: Int,
  attempt_owner: String,
) -> Result(ResolutionResult, ResolutionError) {
  let insert =
    postgres_resolution_queries.insert_resolution_query(
      command,
      attempt_id,
      attempt_epoch,
      attempt_owner,
    )
  use _ <- result.try(case store.execute_safely(insert, on: connection) {
    Error(error) -> Error(ReconciliationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> Ok(Nil)
        _ -> Error(ResolutionWriteRejected)
      }
  })
  let update =
    postgres_resolution_queries.update_resolution_query(
      command,
      attempt_id,
      attempt_epoch,
      attempt_owner,
    )
  use state <- result.try(case store.execute_safely(update, on: connection) {
    Error(error) -> Error(ReconciliationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [state] -> Ok(state)
        _ -> Error(ResolutionWriteRejected)
      }
  })
  case resolution_state(state) {
    Error(error) -> Error(error)
    Ok(state) -> Ok(ResolutionApplied(state))
  }
}
