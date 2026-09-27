//// Reads and decodes jobs and acknowledgement receipts.
//// Every handle-based read checks its installation before querying storage.

import gleam/option.{type Option, None, Some, unwrap}
import gleam/result
import grind/internal/sql
import grind/internal/store
import grind/job.{type JobHandle, type State, Queued, Scheduled}
import grind/worker.{type Worker}
import pog

/// Internal mirror; `grind/postgres` owns the documented public errors.
pub type JobReadError {
  JobReadQueryFailed(pog.QueryError)
  JobNotFound
  QueueRouteMismatch(expected: String, actual: String)
  WorkerContractMismatch(
    expected_id: String,
    expected_version: String,
    actual_id: String,
    actual_version: String,
  )
  CodecContractMismatch(
    kind: worker.CodecKind,
    expected: String,
    actual: String,
  )
  CodecFailed(worker.StoredCodecError)
  InvalidStoredState(String)
  SucceededOutputMissing
  ReceiptNotFound
  ReceiptJobMismatch(expected: Int, actual: Int)
  HandleFromAnotherInstallation
}

pub type AcknowledgementReceipt {
  AcknowledgementReceipt(
    command_id: String,
    attempt_id: Int,
    attempt_epoch: Int,
    committed_state: job.State,
    business_failure_cause: Option(worker.BusinessFailureCause),
    committed_at_unix_ms: Int,
  )
}

pub fn bind_handle(
  connection: pog.Connection,
  installation: job.Installation,
  worker: Worker(input, output, error),
  id: Int,
) -> Result(JobHandle(input, output, error), JobReadError) {
  let worker.Metadata(
    id: expected_worker_id,
    worker_version: expected_worker_version,
    input_version: expected_input_version,
    output_version: expected_output_version,
    error_version: expected_error_version,
    ..,
  ) = worker.metadata(worker)
  case
    store.call_safely(connection, fn(connection) {
      sql.bind_handle(connection, id)
    })
  {
    Error(error) -> Error(JobReadQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(JobNotFound)
        [
          sql.BindHandleRow(
            queue:,
            worker_id: stored_worker_id,
            worker_version: stored_worker_version,
            input_version: stored_input_version,
            output_version: stored_output_version,
            error_version: stored_error_version,
          ),
        ] ->
          case
            stored_worker_id == expected_worker_id
            && stored_worker_version == expected_worker_version
          {
            False ->
              Error(WorkerContractMismatch(
                expected_id: expected_worker_id,
                expected_version: expected_worker_version,
                actual_id: stored_worker_id,
                actual_version: stored_worker_version,
              ))
            True ->
              case stored_input_version == expected_input_version {
                False ->
                  Error(CodecContractMismatch(
                    kind: worker.InputCodec,
                    expected: expected_input_version,
                    actual: stored_input_version,
                  ))
                True ->
                  case stored_output_version == expected_output_version {
                    False ->
                      Error(CodecContractMismatch(
                        kind: worker.OutputCodec,
                        expected: expected_output_version,
                        actual: stored_output_version,
                      ))
                    True ->
                      case stored_error_version == expected_error_version {
                        False ->
                          Error(CodecContractMismatch(
                            kind: worker.ErrorCodec,
                            expected: unwrap(expected_error_version, "none"),
                            actual: unwrap(stored_error_version, "none"),
                          ))
                        True ->
                          Ok(job.new_handle(id, installation, queue, worker))
                      }
                  }
              }
          }
        _ -> Error(JobNotFound)
      }
  }
}

pub fn arguments(
  connection: pog.Connection,
  database_installation: job.Installation,
  handle: JobHandle(input, output, error),
) -> Result(input, JobReadError) {
  let #(
    id,
    handle_installation,
    handle_queue,
    worker_id,
    worker_version,
    input_codec,
  ) = job.storage_fields(handle)
  case job.same_installation(handle_installation, database_installation) {
    False -> Error(HandleFromAnotherInstallation)
    True ->
      case
        store.call_safely(connection, fn(connection) {
          sql.arguments(connection, id)
        })
      {
        Error(error) -> Error(JobReadQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] -> Error(JobNotFound)
            [
              sql.ArgumentsRow(
                input: encoded,
                input_version: codec_version,
                queue: stored_queue,
                worker_id: stored_worker,
                worker_version: stored_worker_version,
              ),
            ] ->
              case stored_queue == handle_queue {
                False ->
                  Error(QueueRouteMismatch(
                    expected: handle_queue,
                    actual: stored_queue,
                  ))
                True ->
                  case
                    stored_worker == worker_id
                    && stored_worker_version == worker_version
                  {
                    False ->
                      Error(WorkerContractMismatch(
                        expected_id: worker_id,
                        expected_version: worker_version,
                        actual_id: stored_worker,
                        actual_version: stored_worker_version,
                      ))
                    True ->
                      worker.decode_codec(input_codec, codec_version, encoded)
                      |> result.map_error(CodecFailed)
                  }
              }
            _ -> Error(JobNotFound)
          }
      }
  }
}

pub fn state(
  connection: pog.Connection,
  database_installation: job.Installation,
  handle: JobHandle(input, output, error),
) -> Result(State, JobReadError) {
  let #(id, handle_installation, handle_queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  case job.same_installation(handle_installation, database_installation) {
    False -> Error(HandleFromAnotherInstallation)
    True ->
      case
        store.call_safely(connection, fn(connection) {
          sql.state(connection, id)
        })
      {
        Error(error) -> Error(JobReadQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] -> Error(JobNotFound)
            [
              sql.StateRow(
                queue: stored_queue,
                worker_id: stored_worker,
                worker_version: stored_worker_version,
                state:,
              ),
            ] ->
              case stored_queue == handle_queue {
                False ->
                  Error(QueueRouteMismatch(
                    expected: handle_queue,
                    actual: stored_queue,
                  ))
                True ->
                  case
                    stored_worker == worker_id
                    && stored_worker_version == worker_version
                  {
                    False ->
                      Error(WorkerContractMismatch(
                        expected_id: worker_id,
                        expected_version: worker_version,
                        actual_id: stored_worker,
                        actual_version: stored_worker_version,
                      ))
                    True ->
                      job.state_of_stored(state)
                      |> result.replace_error(InvalidStoredState(state))
                  }
              }
            _ -> Error(JobNotFound)
          }
      }
  }
}

pub fn outcome(
  connection: pog.Connection,
  database_installation: job.Installation,
  handle: JobHandle(input, output, error),
) -> Result(job.Outcome(output, error), JobReadError) {
  let #(
    id,
    handle_installation,
    handle_queue,
    worker_id,
    worker_version,
    output_codec,
    error_codec,
  ) = job.result_fields(handle)
  case job.same_installation(handle_installation, database_installation) {
    False -> Error(HandleFromAnotherInstallation)
    True ->
      case
        store.call_safely(connection, fn(connection) {
          sql.outcome(connection, id)
        })
      {
        Error(error) -> Error(JobReadQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] -> Error(JobNotFound)
            [
              sql.OutcomeRow(
                queue: stored_queue,
                worker_id: stored_worker,
                worker_version: stored_worker_version,
                state:,
                output: encoded_output,
                output_version:,
                error: encoded_error,
                error_version:,
                failure_description:,
                failure_cause:,
              ),
            ] ->
              outcome_from_row(
                handle_queue,
                worker_id,
                worker_version,
                output_codec,
                error_codec,
                #(
                  stored_queue,
                  stored_worker,
                  stored_worker_version,
                  state,
                  encoded_output,
                  output_version,
                  encoded_error,
                  error_version,
                  failure_description,
                  failure_cause,
                ),
              )
            _ -> Error(JobNotFound)
          }
      }
  }
}

pub fn reconcile_acknowledgement(
  connection: pog.Connection,
  database_installation: job.Installation,
  handle: JobHandle(input, output, error),
  command_id: String,
) -> Result(AcknowledgementReceipt, JobReadError) {
  let #(id, handle_installation, queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  case job.same_installation(handle_installation, database_installation) {
    False -> Error(HandleFromAnotherInstallation)
    True ->
      case
        store.call_safely(connection, fn(connection) {
          sql.reconcile_acknowledgement(connection, command_id)
        })
      {
        Error(error) -> Error(JobReadQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] -> Error(ReceiptNotFound)
            [receipt] -> {
              let sql.ReconcileAcknowledgementRow(
                queue: stored_queue,
                job_id: stored_id,
                worker_id: stored_worker,
                worker_version: stored_worker_version,
                attempt_id:,
                attempt_epoch:,
                committed_state:,
                failure_cause:,
                committed_at_unix_ms:,
              ) = receipt
              case stored_id == id {
                False ->
                  Error(ReceiptJobMismatch(expected: id, actual: stored_id))
                True ->
                  case stored_queue == queue {
                    False ->
                      Error(QueueRouteMismatch(
                        expected: queue,
                        actual: stored_queue,
                      ))
                    True ->
                      case
                        stored_worker == worker_id
                        && stored_worker_version == worker_version
                      {
                        False ->
                          Error(WorkerContractMismatch(
                            expected_id: worker_id,
                            expected_version: worker_version,
                            actual_id: stored_worker,
                            actual_version: stored_worker_version,
                          ))
                        True -> {
                          use committed_state <- result.try(
                            acknowledgement_state(committed_state),
                          )
                          use failure_cause <- result.try(
                            acknowledgement_failure_cause(failure_cause),
                          )
                          Ok(AcknowledgementReceipt(
                            command_id:,
                            attempt_id:,
                            attempt_epoch:,
                            committed_state:,
                            business_failure_cause: failure_cause,
                            committed_at_unix_ms:,
                          ))
                        }
                      }
                  }
              }
            }
            _ -> Error(ReceiptNotFound)
          }
      }
  }
}

fn acknowledgement_state(state: String) -> Result(job.State, JobReadError) {
  case state {
    "queued" -> Ok(job.Queued)
    "scheduled" -> Ok(job.Scheduled)
    "retryable" -> Ok(job.Retryable)
    "executing" -> Ok(job.Executing)
    "succeeded" -> Ok(job.Succeeded)
    "business_failed" -> Ok(job.BusinessFailed)
    "runtime_failed" -> Ok(job.RuntimeFailed)
    "contract_mismatch" -> Ok(job.ContractMismatch)
    "uncertain" -> Ok(job.Uncertain)
    "discarded" -> Ok(job.Discarded)
    "cancelled" -> Ok(job.Cancelled)
    other -> Error(InvalidStoredState(other))
  }
}

fn acknowledgement_failure_cause(
  cause: Option(String),
) -> Result(Option(worker.BusinessFailureCause), JobReadError) {
  case cause {
    None -> Ok(None)
    Some(raw) ->
      worker.business_failure_cause_from_string(raw)
      |> result.map(Some)
      |> result.replace_error(InvalidStoredState(raw))
  }
}

fn outcome_from_row(
  handle_queue: String,
  handle_worker: String,
  handle_worker_version: String,
  output_codec: worker.Codec(output),
  error_codec: Option(worker.Codec(error)),
  stored: #(
    String,
    String,
    String,
    String,
    Option(String),
    String,
    Option(String),
    Option(String),
    Option(String),
    Option(String),
  ),
) -> Result(job.Outcome(output, error), JobReadError) {
  let #(
    stored_queue,
    stored_worker,
    stored_worker_version,
    state,
    encoded_output,
    output_version,
    encoded_error,
    error_version,
    failure_description,
    failure_cause,
  ) = stored
  case stored_queue == handle_queue {
    False ->
      Error(QueueRouteMismatch(expected: handle_queue, actual: stored_queue))
    True ->
      case
        stored_worker == handle_worker
        && stored_worker_version == handle_worker_version
      {
        False ->
          Error(WorkerContractMismatch(
            expected_id: handle_worker,
            expected_version: handle_worker_version,
            actual_id: stored_worker,
            actual_version: stored_worker_version,
          ))
        True ->
          outcome_value(
            state,
            output_codec,
            error_codec,
            encoded_output,
            output_version,
            encoded_error,
            error_version,
            failure_description,
            failure_cause,
          )
      }
  }
}

fn outcome_value(
  state: String,
  output_codec: worker.Codec(output),
  error_codec: Option(worker.Codec(error)),
  encoded_output: Option(String),
  output_version: String,
  encoded_error: Option(String),
  error_version: Option(String),
  failure_description: Option(String),
  failure_cause: Option(String),
) -> Result(job.Outcome(output, error), JobReadError) {
  case state {
    "queued" -> Ok(job.Pending(Queued))
    "scheduled" -> Ok(job.Pending(Scheduled))
    "retryable" -> Ok(job.Pending(job.Retryable))
    "executing" -> Ok(job.Pending(job.Executing))
    "succeeded" ->
      case encoded_output {
        Some(encoded) ->
          worker.decode_codec(output_codec, output_version, encoded)
          |> result.map(job.SucceededWith)
          |> result.map_error(CodecFailed)
        None -> Error(SucceededOutputMissing)
      }
    "business_failed" -> {
      let cause = case failure_cause {
        Some(raw) ->
          option.from_result(worker.business_failure_cause_from_string(raw))
        None -> None
      }
      case error_codec, encoded_error, error_version {
        Some(codec), Some(encoded), Some(version) ->
          worker.decode_codec(codec, version, encoded)
          |> result.map(fn(error) {
            case cause {
              Some(terminal_cause) ->
                job.BusinessFailedWithCause(error, terminal_cause)
              None -> job.BusinessFailedWith(error)
            }
          })
          |> result.map_error(CodecFailed)
        _, _, _ ->
          case cause {
            Some(terminal_cause) ->
              Ok(job.FailedOperationallyWithCause(
                failure_description
                  |> unwrap("worker returned an application error"),
                terminal_cause,
              ))
            None ->
              Ok(job.FailedOperationally(
                failure_description
                |> unwrap("worker returned an application error"),
              ))
          }
      }
    }
    "runtime_failed" ->
      Ok(job.FailedOperationally(
        failure_description |> unwrap("worker runtime failed"),
      ))
    "contract_mismatch" ->
      Ok(job.FailedOperationally(
        failure_description |> unwrap("worker codec contract mismatch"),
      ))
    "uncertain" ->
      Ok(job.ReconciliationRequired(
        failure_description
        |> unwrap("attempt outcome requires reconciliation"),
      ))
    "discarded" ->
      Ok(job.DiscardedWithReason(failure_description |> unwrap("job discarded")))
    "cancelled" ->
      Ok(job.CancelledWithReason(failure_description |> unwrap("job cancelled")))
    other -> Error(InvalidStoredState(other))
  }
}
