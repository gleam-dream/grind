//// Submitting jobs: a plain submit, an idempotent submit under a caller's
//// id, a scheduled job, and reading its state and outcome.

import gleam/dynamic/decode
import gleam/io
import gleam/json
import gleam/time/duration
import grind
import grind/job
import grind/worker
import grind_usage/support

fn amount() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

pub fn copy(queue: String) -> worker.Worker(Int, Int, Nil) {
  worker.new(
    support.unique("jobs.copy"),
    input: amount(),
    output: amount(),
    perform: fn(value) { Ok(value) },
  )
  |> worker.with_queue(queue)
}

pub fn run(url: String) -> Nil {
  let copy = copy(support.unique("jobs"))
  use jobs <- support.with_grind(url, grind.with_worker(_, copy))
  let within = duration.seconds(10)

  // A plain submit runs now, in the worker's queue.
  let assert Ok(admission) = grind.submit(jobs, job.new(copy, 1))
  let assert Ok(grind.Succeeded(1)) =
    grind.await(jobs, grind.handle(admission), within:)

  // `with_id` makes the submit idempotent: the same job under the same id
  // returns the first admission, and a different job under it is refused.
  let id = support.unique("invoice")
  let assert Ok(grind.Inserted(first)) =
    grind.submit(jobs, job.new(copy, 2) |> job.with_id(id))
  let assert Ok(grind.Inserted(again)) =
    grind.submit(jobs, job.new(copy, 2) |> job.with_id(id))
  let assert True = job.id(first) == job.id(again)
  let assert Error(grind.IdConflict) =
    grind.submit(jobs, job.new(copy, 3) |> job.with_id(id))

  // A scheduled job waits for its time, then runs.
  let assert Ok(grind.Inserted(later)) =
    grind.submit(
      jobs,
      job.new(copy, 4) |> job.after(duration.milliseconds(500)),
    )
  let assert Ok(job.Scheduled) = grind.state(jobs, later)
  let assert Ok(grind.Succeeded(4)) = grind.await(jobs, later, within:)
  let assert Ok(4) = grind.arguments(jobs, later)

  io.println("jobs: three jobs admitted and finished")
  support.mark("usage-jobs-passed")
}
