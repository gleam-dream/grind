//// Queue statistics and retention: a content-free snapshot of one queue,
//// and bounded pruning of finished jobs.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/list
import gleam/time/duration
import grind
import grind/admin
import grind/job
import grind/worker
import grind_usage/support

fn amount() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

pub fn run(url: String) -> Nil {
  let work =
    worker.new(
      support.unique("retention.copy"),
      input: amount(),
      output: amount(),
      perform: fn(value) { Ok(value) },
    )
    |> worker.with_queue(support.unique("retention"))
  // Retention belongs to a schema, so this example prunes only its own.
  use jobs <- support.with_grind(url, fn(config) {
    config
    |> grind.with_schema(support.identifier("usage_retention"))
    |> grind.with_worker(work)
    |> grind.without_pruner
  })
  let assert Ok(grind.Inserted(handle)) = grind.submit(jobs, job.new(work, 1))
  let assert Ok(grind.Succeeded(1)) =
    grind.await(jobs, handle, within: duration.seconds(10))

  // Every known state is present; empty groups carry a zero count.
  // snips-start: retention
  let assert Ok(statistics) = admin.statistics(jobs, queue: work.queue)
  let assert Ok(finished) =
    list.find(statistics.states, fn(group) { group.state == job.Succeeded })
  let assert 1 = finished.count

  // One call is one bounded batch of finished jobs older than the age.
  process.sleep(20)
  let assert Ok(pruned) =
    admin.prune_finished(
      jobs,
      older_than: duration.milliseconds(10),
      limit: 1000,
    )
  let assert True = pruned >= 1
  // snips-end: retention
  let assert Error(grind.JobNotFound) = grind.state(jobs, handle)
  let assert Error(admin.InvalidLimit(limit: 0, maximum: 10_000)) =
    admin.prune_finished(jobs, older_than: duration.seconds(1), limit: 0)

  io.println("retention: statistics read and finished jobs pruned")
  support.mark("usage-retention-passed")
}
