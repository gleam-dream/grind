import gleam/option.{type Option, None, Some}
import grind/worker.{type BusinessFailureCause, type Codec, type Worker}

/// A typed reference to a persisted job. Codecs are retained from its definition.
pub opaque type JobHandle(input, output, error) {
  JobHandle(
    id: Int,
    storage_owner: String,
    queue: String,
    worker_id: String,
    worker_version: String,
    input: Codec(input),
    output: Codec(output),
    error: Option(Codec(error)),
  )
}

pub opaque type JobId {
  JobId(Int)
}

/// A checked absolute Unix-millisecond time at which a job may run.
pub opaque type AvailableAt {
  AvailableAt(Int)
}

pub type AvailableAtError {
  NegativeUnixMilliseconds
}

pub fn available_at(
  unix_milliseconds: Int,
) -> Result(AvailableAt, AvailableAtError) {
  case unix_milliseconds < 0 {
    True -> Error(NegativeUnixMilliseconds)
    False -> Ok(AvailableAt(unix_milliseconds))
  }
}

@internal
pub fn available_at_unix_milliseconds(available_at: AvailableAt) -> Int {
  let AvailableAt(unix_milliseconds) = available_at
  unix_milliseconds
}

pub type State {
  Queued
  Scheduled
  Retryable
  Executing
  Succeeded
  BusinessFailed
  RuntimeFailed
  ContractMismatch
  Uncertain
  Discarded
  Cancelled
}

/// Maps a persisted state column's text (`grind_jobs.state`,
/// `grind_unique_submissions.observed_state`) to its typed `State`. Shared
/// by `postgres.state` and the uniqueness admission path so the mapping is
/// defined once.
@internal
pub fn state_of_stored(text: String) -> Result(State, Nil) {
  case text {
    "queued" -> Ok(Queued)
    "scheduled" -> Ok(Scheduled)
    "retryable" -> Ok(Retryable)
    "executing" -> Ok(Executing)
    "succeeded" -> Ok(Succeeded)
    "business_failed" -> Ok(BusinessFailed)
    "runtime_failed" -> Ok(RuntimeFailed)
    "contract_mismatch" -> Ok(ContractMismatch)
    "uncertain" -> Ok(Uncertain)
    "discarded" -> Ok(Discarded)
    "cancelled" -> Ok(Cancelled)
    _ -> Error(Nil)
  }
}

/// The inverse of `state_of_stored`: a typed `State`'s persisted column
/// text. Total (every `State` variant has a stored representation), unlike
/// `state_of_stored`'s partial direction (stored text can be tampered with
/// or, for a closed enum read back from an `Option`/receipt column, absent).
/// Shared the same way, so `grind/observation` and `grind/postgres` encode a
/// `State` through this one definition rather than each keeping its own
/// copy.
@internal
pub fn state_to_stored(state: State) -> String {
  case state {
    Queued -> "queued"
    Scheduled -> "scheduled"
    Retryable -> "retryable"
    Executing -> "executing"
    Succeeded -> "succeeded"
    BusinessFailed -> "business_failed"
    RuntimeFailed -> "runtime_failed"
    ContractMismatch -> "contract_mismatch"
    Uncertain -> "uncertain"
    Discarded -> "discarded"
    Cancelled -> "cancelled"
  }
}

pub type Outcome(output, error) {
  Pending(State)
  SucceededWith(output)
  BusinessFailedWith(error)
  BusinessFailedWithCause(error, BusinessFailureCause)
  DiscardedWithReason(String)
  CancelledWithReason(String)
  FailedOperationally(String)
  FailedOperationallyWithCause(String, BusinessFailureCause)
  /// A worker declared or recovery detected an uncertain outcome needing an
  /// explicit audited resolution.
  ReconciliationRequired(String)
}

pub fn id(handle: JobHandle(input, output, error)) -> JobId {
  let JobHandle(id:, ..) = handle
  JobId(id)
}

/// Returns the durable PostgreSQL integer so applications can persist and rebind it.
pub fn id_value(handle: JobHandle(input, output, error)) -> Int {
  let JobHandle(id:, ..) = handle
  id
}

pub fn queue(handle: JobHandle(input, output, error)) -> String {
  let JobHandle(queue:, ..) = handle
  queue
}

/// Internal: bind the submitting worker's typed codecs to a returned row.
@internal
pub fn new_handle(
  id: Int,
  storage_owner: String,
  queue: String,
  worker: Worker(input, output, error),
) -> JobHandle(input, output, error) {
  let #(metadata, input, output, error) = worker.handle_data(worker)
  let worker.Metadata(id: worker_id, worker_version: worker_version, ..) =
    metadata
  JobHandle(
    id:,
    storage_owner:,
    queue:,
    worker_id:,
    worker_version:,
    input:,
    output:,
    error:,
  )
}

/// Internal persistence fields. These do not form a caller-managed workflow.
@internal
pub fn storage_fields(
  handle: JobHandle(input, output, error),
) -> #(Int, String, String, String, String, Codec(input)) {
  let JobHandle(
    id:,
    storage_owner:,
    queue:,
    worker_id:,
    worker_version:,
    input:,
    ..,
  ) = handle
  #(id, storage_owner, queue, worker_id, worker_version, input)
}

@internal
pub fn result_fields(
  handle: JobHandle(input, output, error),
) -> #(Int, String, String, String, String, Codec(output), Option(Codec(error))) {
  let JobHandle(
    id:,
    storage_owner:,
    queue:,
    worker_id:,
    worker_version:,
    output:,
    error:,
    ..,
  ) = handle
  #(id, storage_owner, queue, worker_id, worker_version, output, error)
}

/// Internal identity and persisted codec contract used to resolve an
/// uncertain attempt without repeating worker metadata at the call site.
@internal
pub fn reconciliation_fields(
  handle: JobHandle(input, output, error),
) -> #(Int, String, String, String, String, String, Option(String)) {
  let JobHandle(
    id:,
    storage_owner:,
    queue:,
    worker_id:,
    worker_version:,
    output:,
    error:,
    ..,
  ) = handle
  let error_version = case error {
    Some(codec) -> Some(worker.codec_version(codec))
    None -> None
  }
  #(
    id,
    storage_owner,
    queue,
    worker_id,
    worker_version,
    worker.codec_version(output),
    error_version,
  )
}

/// Encodes a caller-confirmed output using the admitted job's bound codec.
@internal
pub fn encode_reconciled_success(
  handle: JobHandle(input, output, error),
  value: output,
) -> #(String, String) {
  let JobHandle(output:, ..) = handle
  worker.encode_value(output, value)
}

/// Encodes a caller-confirmed business error when the worker retained a codec.
@internal
pub fn encode_reconciled_error(
  handle: JobHandle(input, output, error),
  value: error,
) -> Option(#(String, String)) {
  let JobHandle(error:, ..) = handle
  case error {
    Some(codec) -> Some(worker.encode_value(codec, value))
    None -> None
  }
}
