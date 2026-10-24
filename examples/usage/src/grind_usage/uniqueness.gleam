//// Uniqueness: a policy decides when a second submit is the same job. A
//// conflict returns the existing job, or reschedules it.

import gleam/dynamic/decode
import gleam/io
import gleam/json
import gleam/time/duration
import gleam/time/timestamp
import grind
import grind/job
import grind/unique
import grind/worker
import grind_usage/support

fn amount() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

fn copy(queue: String) -> worker.Worker(Int, Int, Nil) {
  worker.new(
    support.unique("unique.copy"),
    input: amount(),
    output: amount(),
    perform: fn(value) { Ok(value) },
  )
  |> worker.with_queue(queue)
}

pub fn run(url: String) -> Nil {
  existing(url)
  rescheduled(url)
  io.println("uniqueness: duplicates found and rescheduled")
  support.mark("usage-uniqueness-passed")
}

/// The same input inside the period finds the job already there, even when a
/// retry carries its own id.
fn existing(url: String) -> Nil {
  let work = copy(support.unique("unique-existing"))
  use jobs <- support.with_grind(url, fn(config) {
    config |> grind.with_worker(work) |> grind.without_consumers
  })
  let policy =
    unique.policy(
      unique.full_input(),
      unique.within(duration.hours(1), from: unique.FromInsertion),
    )
  let assert Ok(grind.Inserted(first)) =
    grind.submit(jobs, job.new(work, 1) |> job.unique(policy))
  let retry = support.unique("retry")
  let assert Ok(grind.Existing(conflict)) =
    grind.submit(
      jobs,
      job.new(work, 1) |> job.unique(policy) |> job.with_id(retry),
    )
  let assert True = conflict.job_id == job.id(first)
  let assert Ok(grind.Existing(_)) =
    grind.submit(
      jobs,
      job.new(work, 1) |> job.unique(policy) |> job.with_id(retry),
    )
  Nil
}

/// A policy can move a scheduled job's time instead, across queues.
fn rescheduled(url: String) -> Nil {
  let first_queue = support.unique("unique-first")
  let work = copy(first_queue)
  use jobs <- support.with_grind(url, fn(config) {
    config |> grind.with_worker(work) |> grind.without_consumers
  })
  let later = timestamp.add(timestamp.system_time(), duration.hours(2))
  let policy =
    unique.policy(unique.full_input(), unique.while_retained())
    |> unique.with_scope(unique.AcrossQueues)
    |> unique.with_states(unique.ScheduledOnly)
    |> unique.reschedule_to(later)
  let assert Ok(grind.Inserted(first)) =
    grind.submit(
      jobs,
      job.new(work, 2) |> job.after(duration.hours(1)) |> job.unique(policy),
    )
  let assert Ok(grind.Rescheduled(conflict)) =
    grind.submit(
      jobs,
      job.new(work, 2)
        |> job.with_queue(support.unique("unique-second"))
        |> job.unique(policy),
    )
  let assert True = conflict.job_id == job.id(first)
  let assert True = conflict.queue == first_queue
  Nil
}
