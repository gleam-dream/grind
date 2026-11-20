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
//// Each job carries a closure over the whole worker definition: the handler,
//// the codecs, the retry policy and the settlement, with everything they
//// capture. Grind copies it three times per job: into the attempt
//// supervisor, the attempt actor and the handler process. Only the handler keeps it while the job runs. A copy
//// loses internal sharing, so each costs the closure's flat size. Capture
//// small references, such as an id or a process name, and keep static data
//// in module constants, which Gleam inlines and the runtime does not copy.
//// Bound the worker value in a test with
//// `erts_debug:flat_size(Worker) * erlang:system_info(wordsize)`. The README
//// section "What each job copies" has an example; for codecs built with
//// json_blueprint, see its
//// [codec guide](https://github.com/lostbean/json_blueprint/blob/main/docs/CODEC-PRACTICE.md).
////
//// A handler built with `new` returns `Result(output, error)`. One built
//// with `responding` receives the job's `Context` (job id, attempt, snooze
//// count, correlation, cancellation, deadline and the runtime's database
//// connection) and returns a `Response`
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
//// | abandoned, held    | waits for an operator                     | `with_settlement`           |

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import grind/abandoned
import grind/internal/worker as definition
import pog
import sinal/correlation.{type Correlation}

/// A typed worker definition. Build it with `new` or `responding`.
pub type Worker(input, output, error) =
  definition.Worker(input, output, error)

/// A versioned JSON boundary for one value type. Build it with `codec`.
pub type Codec(value) =
  definition.Codec(value)

/// What a running handler knows about its job. Read it with `job_id`,
/// `attempt`, `max_attempts`, `snooze_count`, `queue`, `correlation`,
/// `cancellation`, `deadline` and `connection`. `grind/testing.context`
/// builds one for a test.
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
  /// by construction. A replay redelivers the same business attempt: it
  /// does not count against `with_max_attempts`, and `attempt` reads the
  /// same number again. `[grind, job, quarantined]` reports it with
  /// `replayed: True`.
  ReplayAfterLeaseExpiry(max_replays: Int)
}

/// What a worker's settlement decides about an abandoned job.
pub type Settlement(output, error) {
  /// The abandoned attempt succeeded with this output.
  ConfirmSuccess(output)
  /// The abandoned attempt failed with this error. The error is stored when
  /// the worker has an error codec; without one the job ends as a failure
  /// with no stored error, as the handler's own `Failed` does.
  ConfirmFailure(error)
  /// Run the job again.
  AuthorizeReplay
  /// Keep the job `uncertain` for an operator; `reason` joins its
  /// description. The job is not handed over again unless a later attempt
  /// is abandoned.
  LeaveUncertain(reason: String)
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

/// Declares how a job that an abandoned attempt left `uncertain` is
/// settled, so that it reaches the application as a decision instead of
/// waiting for an operator.
///
/// `settle` runs inside a transaction and is for database writes only.
/// What it writes on `transaction` commits together with its decision, or
/// not at all. An effect that must happen only after the commit (a
/// notification, an outside call, a message) is a follow-up job: submit it
/// on `transaction` with `grind.submit_in`. It commits with the decision,
/// survives a lost commit reply and a dead node, and runs afterwards with a
/// job's usual at-least-once delivery, so its handler must be safe to
/// repeat. Workers exist before the runtime, so `settle` reaches it through
/// `grind.named` with the name you start the runtime under.
///
/// An attempt is abandoned when its lease expires without an
/// acknowledgement and `with_abandonment` allows no further replay, or when
/// its handler exceeds its timeout under `HoldUncertain`. A handler's own
/// `Uncertain` response is not handed over: it already asked for an
/// operator.
///
/// The queue's consumer calls `settle` with an open transaction on the
/// runtime's pool and the abandoned job (`grind/abandoned`). It runs under
/// the application's `search_path` (on a runtime built from a database URL,
/// Grind's own schema, so qualify your tables), with the job's row locked,
/// within the storage deadline:
///
/// - Make only short database writes on `transaction`. Do not commit, roll
///   back or change its session.
/// - `settle` may run more than once for one abandonment, even after it
///   returned a decision: a try that fails after it returns and before the
///   commit keeps none of its writes, follow-up jobs included, and is tried
///   again. For a slow outside read, submit a checker job, return
///   `LeaveUncertain`, and resolve the job from the checker with
///   `admin.resolve_uncertain`.
/// - Return `Error(reason)` to try again later. The next try waits a lease
///   of the queue, doubling up to one hour; `abandoned.try_number` says
///   which try this is. A crash, a lost connection or a decision Grind
///   refuses (a value the output or error codec rejects) is tried again the
///   same way.
/// - `ConfirmFailure` on a worker without an error codec ends the job as a
///   failure with no stored error, as the handler's own `Failed` does.
///   `AuthorizeReplay` while a cancellation is pending declines the job for
///   an operator and rolls back what `settle` wrote, follow-up jobs
///   included, so check `abandoned.cancellation_requested` first. Bound
///   replays with `abandoned.authorized_replays`.
/// - A follow-up does not cover a job someone resolves by hand with
///   `admin.resolve_uncertain`; an application's own tool resolves with
///   `admin.resolve_uncertain_in` and submits the follow-up with
///   `grind.submit_in` in the same transaction.
/// - The decision is recorded as a resolution with the id
///   `grind-settlement:<job id>:<attempt id>`; do not use that prefix for
///   your own resolution ids.
/// - A consumer that polls on its own settles due jobs once per poll
///   interval. Where consumers are polled by hand or not at all, call
///   `admin.settle_due`. Test the function itself with
///   `grind/testing.abandoned`.
///
/// ```gleam
/// let jobs = grind.named(runtime_name)
/// worker.new("agent.run", input:, output:, perform: run_agent)
/// |> worker.with_error_codec(error_codec)
/// |> worker.with_abandonment(worker.ReplayAfterLeaseExpiry(1))
/// |> worker.with_settlement(fn(transaction, job) {
///   let run_id = abandoned.job_id(job)
///   use Nil <- result.try(insert_notice(transaction, run_id))
///   // Tell open screens to reread once the decision has committed.
///   use _ <- result.try(
///     grind.submit_in(jobs, transaction, job.new(reread, run_id))
///     |> result.map_error(grind.describe_submit_error),
///   )
///   Ok(worker.ConfirmFailure("the agent run stopped mid-way"))
/// })
/// ```
pub fn with_settlement(
  worker: Worker(input, output, error),
  settle: fn(pog.Connection, abandoned.Abandoned(input)) ->
    Result(Settlement(output, error), String),
) -> Worker(input, output, error) {
  definition.Worker(
    ..worker,
    settlement: Some(fn(transaction, held) {
      settle(transaction, abandoned.from_definition(held))
      |> result.map(fn(settlement) {
        case settlement {
          ConfirmSuccess(output) -> definition.SettleSuccess(output)
          ConfirmFailure(error) -> definition.SettleFailure(error)
          AuthorizeReplay -> definition.SettleReplay
          LeaveUncertain(reason:) -> definition.SettleLeaveUncertain(reason:)
        }
      })
    }),
  )
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

/// The runtime's pool, the one `grind.connection` returns, for the
/// handler's own queries. A worker is defined before the runtime starts, so
/// this is how its handler reaches the shared pool. Queries on it run
/// under the application's `search_path`. It is not the attempt's
/// acknowledgement: a write commits on its own, so a handler that may run
/// twice writes idempotently.
pub fn connection(context: Context) -> pog.Connection {
  context.connection
}
