import gleam/option.{type Option, None, Some}
import grind/worker.{type Codec, type Worker}

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
  Executing
  Succeeded
  BusinessFailed
  RuntimeFailed
  ContractMismatch
  Uncertain
  Discarded
  Cancelled
}

pub type Outcome(output, error) {
  Pending(State)
  SucceededWith(output)
  BusinessFailedWith(error)
  FailedOperationally(String)
  /// An expired execution needs an explicit audited outcome resolution.
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
