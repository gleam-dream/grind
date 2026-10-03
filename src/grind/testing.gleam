//// Test support: run a worker's handler without a database, and drain a
//// queue on demand instead of waiting for consumers to poll.
////
//// ```gleam
//// import grind/testing
//// import grind/worker
////
//// pub fn mailer_sends_test() {
////   let assert Ok(worker.Succeeded("msg:a@b.c")) =
////     testing.perform(mailer(), Email("a@b.c", "hi"))
//// }
//// ```
////
//// `perform` round-trips the input, output and error through the worker's
//// codecs, as a consumer does, so a codec defect shows up in the test.
//// `drain` claims and runs a queue's due jobs in the test's own node, with
//// a bound on how long it may take; configure the runtime
//// `without_consumers` so nothing else claims them.

import gleam/erlang/process
import gleam/list
import gleam/option.{None}
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import grind.{type Grind}
import grind/internal/consumer
import grind/internal/runtime
import grind/internal/worker as definition
import grind/worker.{type Context, type Response, type Worker}

/// Why `perform` could not run the handler, or could not encode its
/// result.
pub type Error {
  /// The input codec rejected the input.
  InputRejected(reason: String)
  /// The encoded input did not decode with the input codec.
  InputUndecodable(reason: String)
  /// The output codec rejected the handler's output, or it did not decode.
  OutputRejected(reason: String)
  /// The error codec rejected the handler's error, or it did not decode.
  ErrorRejected(reason: String)
}

/// A context for job 1, attempt 1 of 20, no snoozes, queue `"default"`,
/// whose cancellation never fires.
pub fn context(job_id job_id: Int, attempt attempt: Int) -> Context {
  definition.synthetic_context(
    job_id:,
    attempt:,
    max_attempts: 20,
    snooze_count: 0,
    queue: "default",
  )
}

pub fn with_max_attempts(context: Context, max_attempts: Int) -> Context {
  definition.Context(..context, max_attempts:)
}

pub fn with_snooze_count(context: Context, snooze_count: Int) -> Context {
  definition.Context(..context, snooze_count:)
}

pub fn with_queue(context: Context, queue: String) -> Context {
  definition.Context(..context, queue:)
}

/// A context whose cancellation has already fired, for the process that
/// calls this (where `perform` runs the handler).
pub fn cancelled(context: Context) -> Context {
  let subject = process.new_subject()
  process.send(subject, Nil)
  definition.Context(
    ..context,
    cancellation: process.new_selector() |> process.select(subject),
  )
}

/// Runs the worker's handler once, in the calling process, with
/// `context(job_id: 1, attempt: 1)`.
pub fn perform(
  worker: Worker(input, output, error),
  input: input,
) -> Result(Response(output, error), Error) {
  perform_with(worker, context(job_id: 1, attempt: 1), input)
}

/// Runs the worker's handler once, in the calling process, with `context`.
/// The input is encoded and decoded first, and the output or error after.
pub fn perform_with(
  worker: Worker(input, output, error),
  context: Context,
  input: input,
) -> Result(Response(output, error), Error) {
  use encoded <- result.try(
    definition.encode_input(worker, input) |> result.map_error(InputRejected),
  )
  use input <- result.try(
    definition.decode_codec(worker.input, worker.input.version, encoded)
    |> result.map_error(fn(error) { InputUndecodable(string.inspect(error)) }),
  )
  case definition.respond(worker, context, input) {
    definition.WorkerSucceeded(output) ->
      round_trip(worker.output, output)
      |> result.map(worker.Succeeded)
      |> result.map_error(OutputRejected)
    definition.WorkerFailed(error) ->
      case worker.error {
        None -> Ok(worker.Failed(error))
        option.Some(codec) ->
          round_trip(codec, error)
          |> result.map(worker.Failed)
          |> result.map_error(ErrorRejected)
      }
    definition.WorkerSnoozed(delay, reason) ->
      Ok(worker.Snoozed(
        after: duration.milliseconds(definition.retry_delay_milliseconds(delay)),
        reason:,
      ))
    definition.WorkerDiscarded(reason) -> Ok(worker.Discarded(reason))
    definition.WorkerCancelled(reason) -> Ok(worker.Cancelled(reason))
    definition.WorkerUncertain(evidence) -> Ok(worker.Uncertain(evidence))
  }
}

fn round_trip(
  codec: definition.Codec(value),
  value: value,
) -> Result(value, String) {
  use #(version, encoded) <- result.try(definition.encode_value(codec, value))
  definition.decode_codec(codec, version, encoded)
  |> result.map_error(string.inspect)
}

/// Why `drain` stopped early.
pub type DrainError {
  /// No registered worker uses this queue.
  UnknownQueue(queue: String)
  /// `within` elapsed after `processed` jobs. A job still running then
  /// keeps its claim until its lease expires.
  DrainTimedOut(processed: Int)
  /// Claiming or acknowledging failed after `processed` jobs.
  DrainFailed(processed: Int, reason: String)
  /// No runtime is running under this name on this node.
  DrainNotRunning
}

type DrainMessage {
  Processed
  Done(Result(Nil, String))
}

/// Claims and runs up to `limit` due jobs from `queue`, one at a time, and
/// returns how many ran. Stops early when no job is due. Fails with
/// `DrainTimedOut` when `within` elapses first.
pub fn drain(
  grind: Grind,
  queue queue: String,
  limit limit: Int,
  within within: Duration,
) -> Result(Int, DrainError) {
  use runtime <- result.try(
    runtime.lookup(grind) |> result.replace_error(DrainNotRunning),
  )
  use plan <- result.try(
    list.find(runtime.plans, fn(plan) { plan.name == queue })
    |> result.replace_error(UnknownQueue(queue)),
  )
  let reply = process.new_subject()
  let runner =
    process.spawn_unlinked(fn() {
      let outcome = case
        consumer.start(runtime.database, plan.workers, plan.manual)
      {
        Error(error) -> Error(string.inspect(error))
        Ok(started) -> {
          let outcome = drain_loop(started, reply, limit)
          let _ = consumer.stop(started)
          outcome
        }
      }
      process.send(reply, Done(outcome))
    })
  let deadline = duration.to_milliseconds(within)
  await_drain(runner, reply, monotonic_ms() + deadline, 0)
}

fn drain_loop(
  started: consumer.Consumer,
  reply: process.Subject(DrainMessage),
  remaining: Int,
) -> Result(Nil, String) {
  case remaining <= 0 {
    True -> Ok(Nil)
    False ->
      case consumer.process_one(started) {
        Ok(True) -> {
          process.send(reply, Processed)
          drain_loop(started, reply, remaining - 1)
        }
        Ok(False) -> Ok(Nil)
        Error(error) -> Error(string.inspect(error))
      }
  }
}

fn await_drain(
  runner: process.Pid,
  reply: process.Subject(DrainMessage),
  deadline_ms: Int,
  processed: Int,
) -> Result(Int, DrainError) {
  let remaining = deadline_ms - monotonic_ms()
  case remaining > 0 {
    False -> {
      process.kill(runner)
      Error(DrainTimedOut(processed))
    }
    True ->
      case process.receive(reply, within: remaining) {
        Ok(Processed) -> await_drain(runner, reply, deadline_ms, processed + 1)
        Ok(Done(Ok(Nil))) -> Ok(processed)
        Ok(Done(Error(reason))) -> Error(DrainFailed(processed, reason))
        Error(Nil) -> {
          process.kill(runner)
          Error(DrainTimedOut(processed))
        }
      }
  }
}

@external(erlang, "grind_queue_ffi", "monotonic_ms")
fn monotonic_ms() -> Int
