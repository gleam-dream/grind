//// Defines typed workers: what a job's input, output and error are, how
//// they persist as JSON, which queue runs them, and how failures, snoozes,
//// timeouts and abandoned attempts are handled.
////
//// ```gleam
//// import gleam/dynamic/decode
//// import gleam/json
//// import grind/worker
////
//// pub fn mailer() -> worker.Worker(Email, String, Nil) {
////   worker.new(
////     "mailer.send",
////     input: worker.codec(worker.infallible(encode_email), email_decoder()),
////     output: worker.codec(worker.infallible(json.string), decode.string),
////     perform: fn(email) { Ok("msg:" <> email.to) },
////   )
////   |> worker.with_queue("mailers")
//// }
//// ```
////
//// A worker is a definition written in source code, so its constructors
//// and setters are total: an empty id, version or queue, or an
//// out-of-range limit, is a programming error and panics with the
//// worker's id. The worker id and version, and each codec version, are
//// stored with every job, so a consumer runs a job only with the exact
//// definition it was submitted under. Versions default to `"1"`; change one
//// with `with_version` or `with_codec_version` when a stored shape changes.
////
//// A handler built with `new` returns `Result(output, error)`. One built
//// with `responding` receives the job's `Context` (job id, attempt, snooze
//// count, correlation, cancellation and deadline) and returns a `Response`
//// that may also snooze, discard, cancel or report an uncertain effect.
////
//// | Policy             | Default                                   | Setter                      |
//// | ------------------ | ----------------------------------------- | --------------------------- |
//// | queue              | `"default"`                               | `with_queue`                |
//// | version            | `"1"`                                     | `with_version`              |
//// | attempts           | 20                                        | `with_max_attempts`         |
//// | retry delay        | 15 s doubling to 1 day, plus 0–10% jitter | `with_retry_policy`         |
//// | handler timeout    | 15 minutes                                | `with_timeout(Infinity)`    |
//// | snoozes per job    | 100, then `SnoozeLimitReached`            | `with_max_snoozes`          |
//// | abandoned attempts | `HoldUncertain`                           | `with_abandonment`          |

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import grind/internal/worker as definition
import sinal/correlation.{type Correlation}

/// A typed worker definition. Build it with `new` or `responding`.
pub type Worker(input, output, error) =
  definition.Worker(input, output, error)

/// A versioned JSON boundary for one value type. Build it with `codec`.
pub type Codec(value) =
  definition.Codec(value)

/// What a running handler knows about its job. Read it with `job_id`,
/// `attempt`, `max_attempts`, `snooze_count`, `queue`, `correlation`,
/// `cancellation` and `deadline`. `grind/testing.context` builds one for a
/// test.
pub type Context =
  definition.Context

/// What a `responding` handler proposes. Grind commits it through the
/// attempt's fenced acknowledgement; the job's committed outcome is read
/// back with `grind.outcome` or `grind.await`.
pub type Response(output, error) {
  /// The job succeeded with this output.
  Succeeded(output)
  /// The job failed with this error. Grind retries it while attempts and
  /// the retry policy allow, then ends it `Failed`.
  Failed(error)
  /// Run the job again after `after`, without using an attempt. The 101st
  /// snooze (by default) ends the job with cause `SnoozeLimitReached`.
  Snoozed(after: Duration, reason: String)
  /// End the job as discarded, without retrying.
  Discarded(reason: String)
  /// End the job as cancelled.
  Cancelled(reason: String)
  /// The handler cannot tell whether its effect happened. The job is held
  /// `uncertain` until an operator resolves it with `grind/admin`.
  Uncertain(evidence: String)
}

/// A retry policy's decision for one failed attempt.
pub type RetryDecision {
  RetryAfter(Duration)
  DoNotRetry
}

/// How long a handler may run before Grind stops it.
pub type Timeout {
  After(Duration)
  /// No limit: the attempt's lease is renewed for as long as it runs.
  Infinity
}

/// What happens to an attempt whose node died, whose handler exceeded its
/// timeout, or whose lease otherwise expired before an acknowledgement.
pub type Abandonment {
  /// Hold the job `uncertain` until an operator resolves it. The default:
  /// a handler that may have acted is never run twice without a decision.
  HoldUncertain
  /// Requeue the job after its lease expires, at most `max_replays` times,
  /// then hold it `uncertain`. Choose it for handlers that are idempotent
  /// by construction.
  ReplayAfterLeaseExpiry(max_replays: Int)
}

/// A codec with version `"1"`. `encode` may reject a value with a reason:
/// `grind.submit` then returns `InvalidInput` and writes nothing, and a
/// rejected output or error ends the job as a runtime failure. Wrap an
/// encoder that cannot fail with `infallible`.
///
/// ```gleam
/// worker.codec(worker.infallible(json.string), decode.string)
/// ```
pub fn codec(
  encode: fn(value) -> Result(json.Json, String),
  decoder: decode.Decoder(value),
) -> Codec(value) {
  definition.Codec(version: "1", encode:, decoder:)
}

/// Adapts an encoder that cannot fail, such as `json.string`.
pub fn infallible(
  encode: fn(value) -> json.Json,
) -> fn(value) -> Result(json.Json, String) {
  definition.infallible(encode)
}

/// Sets the codec's stored version. Change it when the encoded shape
/// changes, so jobs stored under the old shape are not decoded with the new
/// decoder. Panics on an empty version.
pub fn with_codec_version(
  codec: Codec(value),
  version: String,
) -> Codec(value) {
  case version {
    "" -> panic as "grind/worker: a codec version must not be empty"
    _ -> definition.Codec(..codec, version:)
  }
}

/// A worker whose handler returns `Result(output, error)`. An `Error` is
/// retried with the default backoff until the attempts run out. Panics on an
/// empty id.
pub fn new(
  id: String,
  input input: Codec(input),
  output output: Codec(output),
  perform perform: fn(input) -> Result(output, error),
) -> Worker(input, output, error) {
  check_id(id)
  definition.new(id, "1", input, output, None, fn(_context, input) {
    definition.from_result(perform(input))
  })
}

/// A worker whose handler receives the job's `Context` and returns a
/// `Response`. Panics on an empty id.
pub fn responding(
  id: String,
  input input: Codec(input),
  output output: Codec(output),
  handle handle: fn(Context, input) -> Response(output, error),
) -> Worker(input, output, error) {
  check_id(id)
  definition.new(id, "1", input, output, None, fn(context, input) {
    to_definition(handle(context, input))
  })
}

fn check_id(id: String) -> Nil {
  case id {
    "" -> panic as "grind/worker: a worker id must not be empty"
    _ -> Nil
  }
}

fn to_definition(
  response: Response(output, error),
) -> definition.WorkerResponse(output, error) {
  case response {
    Succeeded(output) -> definition.WorkerSucceeded(output)
    Failed(error) -> definition.WorkerFailed(error)
    Snoozed(after:, reason:) ->
      definition.WorkerSnoozed(
        definition.clamped_retry_delay(duration.to_milliseconds(after)),
        reason,
      )
    Discarded(reason:) -> definition.WorkerDiscarded(reason)
    Cancelled(reason:) -> definition.WorkerCancelled(reason)
    Uncertain(evidence:) -> definition.WorkerUncertain(evidence)
  }
}

/// Converts a `perform`-style result to a `Response`.
pub fn from_result(result: Result(output, error)) -> Response(output, error) {
  case result {
    Ok(output) -> Succeeded(output)
    Error(error) -> Failed(error)
  }
}

/// The queue this worker's jobs are submitted to and claimed from. The
/// default is `"default"`. `grind.with_worker` derives the queues a node
/// runs from its workers. Panics on an empty queue.
pub fn with_queue(
  worker: Worker(input, output, error),
  queue: String,
) -> Worker(input, output, error) {
  case queue {
    "" ->
      panic as { "grind/worker: worker " <> worker.id <> " has an empty queue" }
    _ -> definition.Worker(..worker, queue:)
  }
}

/// The worker's stored version, `"1"` by default. A consumer runs only jobs
/// submitted under the exact id and version it registers. Panics on an
/// empty version.
pub fn with_version(
  worker: Worker(input, output, error),
  version: String,
) -> Worker(input, output, error) {
  case version {
    "" ->
      panic as {
        "grind/worker: worker " <> worker.id <> " has an empty version"
      }
    _ -> definition.Worker(..worker, version:)
  }
}

/// Stores the handler's typed error, so `grind.outcome` returns
/// `Failed(Business(error), ..)`. Without it, a failure is stored as its
/// description only (`BusinessUnrecorded`).
pub fn with_error_codec(
  worker: Worker(input, output, error),
  codec: Codec(error),
) -> Worker(input, output, error) {
  definition.Worker(..worker, error: Some(codec))
}

/// The number of business attempts, 20 by default. A snooze or a replayed
/// lease does not use one. Panics below 1.
pub fn with_max_attempts(
  worker: Worker(input, output, error),
  max_attempts: Int,
) -> Worker(input, output, error) {
  case definition.with_max_attempts(worker, max_attempts) {
    Ok(worker) -> worker
    Error(_) ->
      panic as {
        "grind/worker: worker "
        <> worker.id
        <> " needs at least one attempt, got "
        <> int.to_string(max_attempts)
      }
  }
}

/// Chooses whether and when a failed attempt is retried. The callback gets
/// the typed error and the attempt that just failed (from 1). It is not
/// consulted after the last attempt. The default retries after
/// `default_backoff(attempt)` plus up to 10% jitter.
pub fn with_retry_policy(
  worker: Worker(input, output, error),
  policy: fn(error, Int) -> RetryDecision,
) -> Worker(input, output, error) {
  definition.with_retry_policy(
    worker,
    definition.retry_policy(fn(failure, context) {
      let definition.BusinessFailure(error) = failure
      case policy(error, context.current_attempt) {
        RetryAfter(delay) ->
          definition.RetryAfter(
            definition.clamped_retry_delay(duration.to_milliseconds(delay)),
          )
        DoNotRetry -> definition.DoNotRetry
      }
    }),
  )
}

/// The default delay before retrying after `attempt` failed, before jitter:
/// 15 seconds after the first attempt, doubling, capped at one day.
pub fn default_backoff(attempt: Int) -> Duration {
  duration.milliseconds(definition.default_retry_delay_milliseconds(attempt))
}

/// How long the handler may run, 15 minutes by default. When it expires the
/// handler is stopped and `with_abandonment` decides what happens next.
/// `Infinity` lifts the limit. Panics on a duration that is not positive.
pub fn with_timeout(
  worker: Worker(input, output, error),
  timeout: Timeout,
) -> Worker(input, output, error) {
  case timeout {
    Infinity -> definition.Worker(..worker, timeout_ms: None)
    After(limit) -> {
      let ms = duration.to_milliseconds(limit)
      case ms > 0 {
        True -> definition.Worker(..worker, timeout_ms: Some(ms))
        False ->
          panic as {
            "grind/worker: worker " <> worker.id <> " needs a positive timeout"
          }
      }
    }
  }
}

/// How many times one job may snooze, 100 by default. The next snooze ends
/// the job as failed with cause `SnoozeLimitReached`. Panics below 0.
pub fn with_max_snoozes(
  worker: Worker(input, output, error),
  max_snoozes: Int,
) -> Worker(input, output, error) {
  case max_snoozes >= 0 {
    True -> definition.Worker(..worker, max_snoozes:)
    False ->
      panic as {
        "grind/worker: worker " <> worker.id <> " has a negative snooze limit"
      }
  }
}

/// What happens to an abandoned attempt; `HoldUncertain` by default. Panics
/// on `ReplayAfterLeaseExpiry` with fewer than 1 replay.
pub fn with_abandonment(
  worker: Worker(input, output, error),
  abandonment: Abandonment,
) -> Worker(input, output, error) {
  case abandonment {
    HoldUncertain ->
      definition.Worker(..worker, abandonment: definition.HoldUncertain)
    ReplayAfterLeaseExpiry(max_replays:) ->
      case max_replays >= 1 {
        True ->
          definition.Worker(
            ..worker,
            abandonment: definition.ReplayAfterLeaseExpiry(max_replays:),
          )
        False ->
          panic as {
            "grind/worker: worker " <> worker.id <> " needs at least one replay"
          }
      }
  }
}

/// The worker's id.
pub fn id(worker: Worker(input, output, error)) -> String {
  worker.id
}

/// The worker's version.
pub fn version(worker: Worker(input, output, error)) -> String {
  worker.version
}

/// The job's id, the durable integer `job.id` returns at submit.
pub fn job_id(context: Context) -> Int {
  context.job_id
}

/// The current business attempt, from 1.
pub fn attempt(context: Context) -> Int {
  context.attempt
}

/// The job's attempt limit.
pub fn max_attempts(context: Context) -> Int {
  context.max_attempts
}

/// How many times this job has snoozed so far.
pub fn snooze_count(context: Context) -> Int {
  context.snooze_count
}

/// The queue the job runs in.
pub fn queue(context: Context) -> String {
  context.queue
}

/// The job's correlation: the one given with `job.with_correlation`, or one
/// Grind generated at submit. Pass it on to the packages the handler calls.
pub fn correlation(context: Context) -> Correlation {
  context.correlation
}

/// A selector that yields once a caller's `grind.cancel` of this job has
/// committed. Grind sees the cancellation at the attempt's next lease
/// renewal, within a third of the queue's lease. A handler that stops early
/// returns `Cancelled`.
pub fn cancellation(context: Context) -> process.Selector(Nil) {
  context.cancellation
}

/// When the handler will be stopped, if the worker has a timeout.
pub fn deadline(context: Context) -> Option(Timestamp) {
  context.deadline
}
