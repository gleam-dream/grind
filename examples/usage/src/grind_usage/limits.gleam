//// Defaults, limits and queue tuning. Every setting has a finite default;
//// this example sets each one explicitly and shows that `grind.check`
//// reports an unsafe combination before anything starts. No database needed.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/time/duration
import grind
import grind/queue
import grind/worker
import grind_usage/support
import pog

fn text() -> worker.Codec(String) {
  worker.codec(worker.infallible(json.string), decode.string)
}

/// A worker with each policy chosen explicitly. The values shown are the
/// defaults except where noted.
pub fn thumbnail() -> worker.Worker(String, String, Nil) {
  worker.new(
    "images.thumbnail",
    input: text(),
    output: text(),
    perform: fn(path) { Ok(path <> ".thumb") },
  )
  |> worker.with_queue("media")
  // Handler execution: 15 minutes by default; `worker.Infinity` lifts it.
  |> worker.with_timeout(worker.After(duration.minutes(2)))
  // Business attempts: 20 by default.
  |> worker.with_max_attempts(5)
  // Snoozes per job: 100 by default.
  |> worker.with_max_snoozes(10)
  // Retry delay: doubling from 15 seconds, capped at a day, plus jitter.
  |> worker.with_retry_policy(fn(_error, attempt) {
    worker.RetryAfter(worker.default_backoff(attempt))
  })
  // An abandoned attempt is held `uncertain` unless the handler is idempotent.
  |> worker.with_abandonment(worker.ReplayAfterLeaseExpiry(2))
  |> worker.with_version("2")
}

/// A runtime configuration with every limit named. Change a value, and the
/// compiler and `grind.check` keep this example honest.
pub fn tuned(pool: pog.Config) -> grind.Config {
  grind.new(pool)
  |> grind.with_worker(thumbnail())
  |> grind.with_queue(
    queue.new("media")
    // Concurrency per node: 10 by default.
    |> queue.with_concurrency(4)
    // Poll interval: 250 milliseconds by default.
    |> queue.with_poll_interval(duration.milliseconds(100))
    // Attempt lease: 30 seconds, at least four statement deadlines.
    |> queue.with_lease(duration.seconds(30))
    // Shutdown grace: 15 seconds by default.
    |> queue.with_shutdown_grace(duration.seconds(10)),
  )
  // Storage call: a 4-second absolute deadline.
  |> grind.with_statement_deadline(duration.seconds(4))
  // Uniqueness lock wait: 2 seconds, ending a second before the deadline.
  |> grind.with_unique_lock_wait(duration.seconds(2))
  // Migration step: 30 seconds.
  |> grind.with_migration_deadline(duration.seconds(30))
  // Initial connect at start: 15 seconds.
  |> grind.with_connect_timeout(duration.seconds(15))
  // Encoded input, output and error: 1 MiB.
  |> grind.with_max_payload_bytes(1_048_576)
  // Observation forwarder: 1,024 events in flight.
  |> grind.with_observation_capacity(1024)
  // Finished jobs are pruned after 7 days.
  |> grind.with_pruner(max_age: duration.hours(24 * 7))
}

pub fn run() -> Nil {
  let pool = pog.default_config(process.new_name("usage_limits_pool"))

  let assert Ok(Nil) = grind.check(tuned(pool))

  // A lease shorter than four statement deadlines is refused up front.
  let short_lease =
    grind.new(pool)
    |> grind.with_worker(thumbnail())
    |> grind.with_queue(
      queue.new("media") |> queue.with_lease(duration.seconds(1)),
    )
  let assert Error(grind.LeaseTooShort(queue: "media", ..)) =
    grind.check(short_lease)

  // A limit must be positive.
  let assert Error(error) =
    grind.check(tuned(pool) |> grind.with_max_payload_bytes(0))
  let assert grind.NotPositive(setting: "payload limit", value: 0) = error
  io.println("limits: " <> grind.describe_config_error(error))
  support.mark("usage-limits-passed")
}
