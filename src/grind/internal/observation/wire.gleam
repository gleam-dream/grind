//// Shared Sinal wire primitives for Grind observations.
//// Public event records and their domain projections remain in observation.

import grind/job
import sinal/fields

/// A single-field `Fields(Int)` for the `count` measurement every
/// `[grind, job, *]` event in this module carries (always `1`).
pub fn count_fields() -> fields.Fields(Int) {
  fields.int("count")
}

/// Every `job.State`, in declaration order, for `job_state_field`.
const job_states = [
  job.Queued,
  job.Scheduled,
  job.Retryable,
  job.Executing,
  job.Succeeded,
  job.BusinessFailed,
  job.RuntimeFailed,
  job.ContractMismatch,
  job.Uncertain,
  job.Discarded,
  job.Cancelled,
]

/// A `job.State` written as its stored column text (`job.state_to_stored`).
pub fn job_state_field(key: String) -> fields.Fields(job.State) {
  fields.enum(key, job_states, job.state_to_stored)
}

/// The shared `JobRef` codec: which job, queue, and worker contract an event
/// is about. Built once here and embedded into every event's metadata,
/// rather than re-declaring the same four fields per event.
pub fn job_ref_fields(
  make: fn(Int, String, String, String) -> reference,
  job_id: fn(reference) -> Int,
  queue: fn(reference) -> String,
  worker_id: fn(reference) -> String,
  worker_version: fn(reference) -> String,
) -> fields.Fields(reference) {
  use job_id <- fields.include(fields.int("job_id"), get: job_id)
  use queue <- fields.include(fields.string("queue"), get: queue)
  use worker_id <- fields.include(fields.string("worker_id"), get: worker_id)
  use worker_version <- fields.include(
    fields.string("worker_version"),
    get: worker_version,
  )
  fields.success(make(job_id, queue, worker_id, worker_version))
}

/// The shared `AttemptRef` codec: `attempt_id`, `epoch`, and the attempt
/// number, embedded into every event about one claimed attempt.
pub fn attempt_ref_fields(
  make: fn(Int, Int, Int) -> reference,
  attempt_id: fn(reference) -> Int,
  epoch: fn(reference) -> Int,
  attempt: fn(reference) -> Int,
) -> fields.Fields(reference) {
  use attempt_id <- fields.include(fields.int("attempt_id"), get: attempt_id)
  use epoch <- fields.include(fields.int("epoch"), get: epoch)
  use attempt <- fields.include(fields.int("attempt"), get: attempt)
  fields.success(make(attempt_id, epoch, attempt))
}

/// Builds a `[grind, job, <part>]` name from its final component (the shared
/// `[grind, job, ...]` prefix is fixed for every event this module exposes).
pub fn job_event_name(part: String) -> List(String) {
  ["grind", "job", part]
}
