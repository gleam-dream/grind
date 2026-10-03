//// Worker definitions are total, default to version "1" and the documented
//// bounds, and run without a database through `grind/testing`.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/option.{None, Some}
import gleam/time/duration
import gleeunit/should
import grind/internal/worker as definition
import grind/job
import grind/testing
import grind/unique
import grind/worker
import pog

fn int_codec() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

fn checked_codec() -> worker.Codec(Int) {
  worker.codec(
    fn(n) {
      case n >= 0 {
        True -> Ok(json.int(n))
        False -> Error("must not be negative")
      }
    },
    decode.int,
  )
}

pub fn worker_defaults_follow_the_release_defaults_test() {
  let echo_worker =
    worker.new(
      "defaults.echo",
      input: int_codec(),
      output: int_codec(),
      perform: fn(n) { Ok(n) },
    )
  worker.version(echo_worker) |> should.equal("1")
  echo_worker.input.version |> should.equal("1")
  echo_worker.queue |> should.equal("default")
  echo_worker.max_attempts |> should.equal(20)
  echo_worker.timeout_ms |> should.equal(Some(900_000))
  echo_worker.max_snoozes |> should.equal(100)
  echo_worker.abandonment |> should.equal(definition.HoldUncertain)
  let tuned =
    echo_worker
    |> worker.with_version("2")
    |> worker.with_queue("mailers")
    |> worker.with_timeout(worker.Infinity)
    |> worker.with_max_snoozes(0)
    |> worker.with_abandonment(worker.ReplayAfterLeaseExpiry(max_replays: 3))
  worker.version(tuned) |> should.equal("2")
  tuned.queue |> should.equal("mailers")
  tuned.timeout_ms |> should.equal(None)
  tuned.max_snoozes |> should.equal(0)
  tuned.abandonment |> should.equal(definition.ReplayAfterLeaseExpiry(3))
  worker.with_codec_version(int_codec(), "2").version |> should.equal("2")
}

fn panics(body: fn() -> a) -> Nil {
  exception.rescue(body) |> should.be_error
  Nil
}

pub fn definition_bugs_panic_with_the_worker_id_test() {
  panics(fn() {
    worker.new("", input: int_codec(), output: int_codec(), perform: fn(n) {
      Ok(n)
    })
  })
  let echo_worker =
    worker.new(
      "panics.echo",
      input: int_codec(),
      output: int_codec(),
      perform: fn(n) { Ok(n) },
    )
  panics(fn() { worker.with_queue(echo_worker, "") })
  panics(fn() { worker.with_version(echo_worker, "") })
  panics(fn() { worker.with_max_attempts(echo_worker, 0) })
  panics(fn() { worker.with_max_snoozes(echo_worker, -1) })
  panics(fn() {
    worker.with_timeout(echo_worker, worker.After(duration.milliseconds(0)))
  })
  panics(fn() {
    worker.with_abandonment(echo_worker, worker.ReplayAfterLeaseExpiry(0))
  })
  panics(fn() { worker.with_codec_version(int_codec(), "") })
  panics(fn() { unique.selected("", fn(n) { n }, int_codec()) })
  panics(fn() {
    unique.within(duration.milliseconds(0), from: unique.FromInsertion)
  })
  panics(fn() { job.new(echo_worker, 1) |> job.with_max_attempts(0) })
  panics(fn() { job.new(echo_worker, 1) |> job.with_queue("") })
}

pub fn perform_round_trips_through_the_codecs_test() {
  let doubling =
    worker.new(
      "perform.double",
      input: checked_codec(),
      output: checked_codec(),
      perform: fn(n) { Ok(n * 2) },
    )
  testing.perform(doubling, 4) |> should.equal(Ok(worker.Succeeded(8)))
  testing.perform(doubling, -1)
  |> should.equal(Error(testing.InputRejected("must not be negative")))
  let negating =
    worker.new(
      "perform.negate",
      input: int_codec(),
      output: checked_codec(),
      perform: fn(n) { Ok(0 - n) },
    )
  testing.perform(negating, 3)
  |> should.equal(Error(testing.OutputRejected("must not be negative")))
}

pub fn perform_with_passes_the_context_test() {
  let probe =
    worker.responding(
      "perform.context",
      input: int_codec(),
      output: int_codec(),
      handle: fn(context, _n) {
        case process.selector_receive(worker.cancellation(context), within: 0) {
          Ok(Nil) -> worker.Cancelled("cancelled")
          Error(Nil) ->
            case worker.snooze_count(context) {
              0 -> worker.Snoozed(after: duration.seconds(1), reason: "later")
              _ -> worker.Succeeded(worker.attempt(context))
            }
        }
      },
    )
  testing.perform(probe, 0)
  |> should.equal(
    Ok(worker.Snoozed(after: duration.seconds(1), reason: "later")),
  )
  testing.perform_with(
    probe,
    testing.context(job_id: 7, attempt: 3) |> testing.with_snooze_count(1),
    0,
  )
  |> should.equal(Ok(worker.Succeeded(3)))
  testing.perform_with(
    probe,
    testing.context(job_id: 7, attempt: 1) |> testing.cancelled,
    0,
  )
  |> should.equal(Ok(worker.Cancelled("cancelled")))
  // A test chooses the connection a handler's queries use.
  let db = pog.named_connection(process.new_name("definition_test_pool"))
  testing.context(job_id: 7, attempt: 1)
  |> testing.with_connection(db)
  |> worker.connection
  |> should.equal(db)
}

pub fn state_names_and_terminal_states_test() {
  job.state_name(job.BusinessFailed) |> should.equal("business_failed")
  job.state_from_name("business_failed") |> should.equal(Ok(job.BusinessFailed))
  job.state_from_name("nope") |> should.equal(Error(Nil))
  job.is_finished(job.Succeeded) |> should.be_true
  job.is_finished(job.Uncertain) |> should.be_false
  job.terminal_cause_name(job.SnoozeLimitReached)
  |> should.equal("snooze_limit_reached")
}

pub fn default_backoff_matches_the_documented_schedule_test() {
  worker.default_backoff(1) |> should.equal(duration.seconds(15))
  worker.default_backoff(2) |> should.equal(duration.seconds(30))
  worker.default_backoff(40) |> should.equal(duration.hours(24))
}
