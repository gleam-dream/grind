//// Builds the jobs `grind.submit` admits, and names the states a stored job
//// moves through.
////
//// ```gleam
//// import gleam/time/duration
//// import grind
//// import grind/job
////
//// let request =
////   job.new(receipt_worker(), order.id)
////   |> job.with_id("receipt:" <> order.id)
////   |> job.after(duration.minutes(5))
//// grind.submit(jobs, request)
//// ```
////
//// A job built with `new` runs now, in its worker's queue, with its
//// worker's attempt limit and a correlation Grind generates. Every submit
//// records a receipt under the job's id (`with_id`) or under an id Grind
//// generates, so a submit whose reply was lost is reconcilable. Give a job
//// your own id when the submit may be retried: resubmitting the same job
//// under the same id returns the original admission instead of a second
//// job.
////
//// `State` may gain variants; branch on `is_finished` or `state_name`
//// where a new state should not break your code.

import gleam/option.{None, Some}
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import grind/internal/admission
import grind/internal/job as internal
import grind/unique.{type Policy}
import grind/worker.{type Worker}
import sinal/correlation.{type Correlation}

/// A job to submit. Build it with `new` and the `with_*`, `at`, `after` and
/// `unique` setters.
pub type Job(input, output, error) =
  admission.Job(input, output, error)

/// A typed reference to a stored job. It keeps the worker's codecs, so
/// `grind.state`, `grind.outcome` and `grind.await` return typed values.
/// Store `id(handle)` to find the job again with `grind.bind`.
pub type JobHandle(input, output, error) =
  internal.JobHandle(input, output, error)

/// The stored lifecycle states.
pub type State {
  /// Waiting to run.
  Queued
  /// Waiting for its scheduled time, or snoozed.
  Scheduled
  /// Failed and waiting to retry.
  Retryable
  /// Claimed by a consumer and running.
  Executing
  Succeeded
  /// Failed terminally: attempts ran out, the retry policy declined, or the
  /// snooze limit was reached.
  BusinessFailed
  /// The input could not be decoded, or the handler's output or error was
  /// rejected by its codec or exceeded the payload limit.
  RuntimeFailed
  /// The stored codec versions no longer match the registered worker.
  ContractMismatch
  /// The attempt's effect is unknown; waits for an audited resolution.
  Uncertain
  Discarded
  Cancelled
}

/// Why a failure is terminal.
pub type TerminalCause {
  /// The last attempt failed.
  BudgetExhausted
  /// The worker's retry policy declined to retry.
  RetryDeclined
  /// The job snoozed more often than its worker allows.
  SnoozeLimitReached
}

/// Which of a worker's codecs a contract check concerns.
pub type CodecKind {
  InputCodec
  OutputCodec
  ErrorCodec
}

/// A job for `worker` with `input`, to run now in the worker's queue.
pub fn new(
  worker: Worker(input, output, error),
  input: input,
) -> Job(input, output, error) {
  admission.Job(
    worker:,
    input:,
    id: None,
    when: admission.Now,
    unique: None,
    correlation: None,
    queue: None,
    max_attempts: None,
  )
}

/// The job's idempotency key. A second submit with the same id and the same
/// job returns the first admission; with a different job it fails with
/// `grind.IdConflict`. An empty id fails at submit with `grind.EmptyJobId`.
/// The key lasts as long as the job is stored.
pub fn with_id(
  job: Job(input, output, error),
  id: String,
) -> Job(input, output, error) {
  admission.Job(..job, id: Some(id))
}

/// Runs the job no earlier than `at`. A time in the past runs it now.
pub fn at(
  job: Job(input, output, error),
  at: Timestamp,
) -> Job(input, output, error) {
  let #(seconds, nanoseconds) = timestamp.to_unix_seconds_and_nanoseconds(at)
  admission.Job(
    ..job,
    when: admission.AtUnixMs(seconds * 1000 + nanoseconds / 1_000_000),
  )
}

/// Runs the job `delay` after it is admitted, measured on the database's
/// clock. A resubmit under the same id converges on the first admission.
pub fn after(
  job: Job(input, output, error),
  delay: Duration,
) -> Job(input, output, error) {
  admission.Job(..job, when: admission.AfterMs(duration.to_milliseconds(delay)))
}

/// Admits the job only if no job occupies its uniqueness key.
pub fn unique(
  job: Job(input, output, error),
  policy: Policy(input),
) -> Job(input, output, error) {
  admission.Job(..job, unique: Some(policy))
}

/// The correlation the job carries into its worker's context and every
/// event Grind emits about it. Without one, Grind generates one at submit.
pub fn with_correlation(
  job: Job(input, output, error),
  correlation: Correlation,
) -> Job(input, output, error) {
  admission.Job(..job, correlation: Some(correlation))
}

/// Submits this one job to `queue` instead of its worker's queue. A consumer
/// must run that queue with this worker registered. Panics on an empty
/// queue.
pub fn with_queue(
  job: Job(input, output, error),
  queue: String,
) -> Job(input, output, error) {
  case queue {
    "" -> panic as "grind/job: a job queue must not be empty"
    _ -> admission.Job(..job, queue: Some(queue))
  }
}

/// This one job's attempt limit instead of its worker's. Panics below 1.
pub fn with_max_attempts(
  job: Job(input, output, error),
  max_attempts: Int,
) -> Job(input, output, error) {
  case max_attempts >= 1 {
    True -> admission.Job(..job, max_attempts: Some(max_attempts))
    False -> panic as "grind/job: a job needs at least one attempt"
  }
}

/// The stored job's durable id.
pub fn id(handle: JobHandle(input, output, error)) -> Int {
  handle.id
}

/// The queue the stored job runs in.
pub fn queue(handle: JobHandle(input, output, error)) -> String {
  handle.queue
}

/// The state's stored name, such as `"business_failed"`, for logs, metrics
/// and records.
pub fn state_name(state: State) -> String {
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

/// The state with this stored name.
pub fn state_from_name(name: String) -> Result(State, Nil) {
  case name {
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

/// Whether the state is terminal. `Uncertain` is not: it waits for an
/// audited resolution, which may replay the job.
pub fn is_finished(state: State) -> Bool {
  case state {
    Succeeded
    | BusinessFailed
    | RuntimeFailed
    | ContractMismatch
    | Discarded
    | Cancelled -> True
    Queued | Scheduled | Retryable | Executing | Uncertain -> False
  }
}

/// The cause's stored name, such as `"snooze_limit_reached"`.
pub fn terminal_cause_name(cause: TerminalCause) -> String {
  case cause {
    BudgetExhausted -> "budget_exhausted"
    RetryDeclined -> "retry_declined"
    SnoozeLimitReached -> "snooze_limit_reached"
  }
}
