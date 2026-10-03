//// Tunes how a node runs one queue. A queue needs no configuration to run:
//// `grind.with_worker` derives the queues from the workers, with the
//// defaults below. Add a `Queue` with `grind.with_queue` only to change
//// them.
////
//// ```gleam
//// grind.new(pool)
//// |> grind.with_worker(mailer())
//// |> grind.with_queue(queue.new("mailers") |> queue.with_concurrency(20))
//// ```
////
//// | Setting           | Default | Setter                |
//// | ----------------- | ------- | --------------------- |
//// | concurrency       | 10      | `with_concurrency`    |
//// | poll interval     | 250 ms  | `with_poll_interval`  |
//// | attempt lease     | 30 s    | `with_lease`          |
//// | shutdown grace    | 15 s    | `with_shutdown_grace` |
////
//// Concurrency is per node: each node that runs the queue claims up to this
//// many jobs at once. A claimed attempt holds a lease in the database,
//// renewed every third of its length by a reserved connection; an attempt
//// whose lease expires is recovered by its worker's abandonment policy. The
//// lease must be at least four times the statement deadline. On shutdown a
//// queue stops claiming and waits up to its grace for running jobs.
//// `grind.start` and `grind.supervised` check these values and report a
//// `grind.InvalidQueue` error.
////
//// Every bound in Grind is a `gleam/time/duration.Duration`, not a bare
//// `Int`: the queue's settings, the worker's timeout and retry delays, the
//// facade's deadlines and `grind.await(within:)`. One type keeps the unit
//// in the call (`duration.milliseconds(250)`, `duration.seconds(30)`), so a
//// lease in seconds cannot be passed where milliseconds are read; the
//// lease rule above compares two of them. An application holding
//// milliseconds converts once with `duration.milliseconds(ms)`, which needs
//// `gleam_time` as a direct dependency.

import gleam/time/duration.{type Duration}
import grind/internal/queue_config

/// One queue's settings. Build it with `new`.
pub type Queue =
  queue_config.Queue

/// Default settings for the queue named `name`. Panics on an empty name.
pub fn new(name: String) -> Queue {
  case name {
    "" -> panic as "grind/queue: a queue name must not be empty"
    _ -> queue_config.new(name)
  }
}

/// How many jobs one node runs from this queue at once.
pub fn with_concurrency(queue: Queue, concurrency: Int) -> Queue {
  queue_config.Queue(..queue, concurrency:)
}

/// How often an idle node looks for due jobs.
pub fn with_poll_interval(queue: Queue, interval: Duration) -> Queue {
  queue_config.Queue(
    ..queue,
    poll_interval_ms: duration.to_milliseconds(interval),
  )
}

/// How long a claimed attempt's lease lasts between renewals.
pub fn with_lease(queue: Queue, lease: Duration) -> Queue {
  queue_config.Queue(..queue, lease_ms: duration.to_milliseconds(lease))
}

/// How long shutdown waits for running jobs. A job still running then
/// keeps its claim and is recovered when its lease expires.
pub fn with_shutdown_grace(queue: Queue, grace: Duration) -> Queue {
  queue_config.Queue(
    ..queue,
    shutdown_grace_ms: duration.to_milliseconds(grace),
  )
}

/// The queue's name.
pub fn name(queue: Queue) -> String {
  queue.name
}
