import gleam/option.{type Option}
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
  Discarded
  Cancelled
}

pub type Outcome(output, error) {
  Pending(State)
  SucceededWith(output)
  BusinessFailedWith(error)
  FailedOperationally(String)
}

pub fn id(handle: JobHandle(input, output, error)) -> JobId {
  let JobHandle(id:, ..) = handle
  JobId(id)
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
