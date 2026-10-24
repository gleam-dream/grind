//// Retention from outside the package: finished jobs are pruned.

import gleam/erlang/process
import gleam/time/duration
import gleeunit/should
import grind
import grind/admin
import grind/job
import grind/worker
import grind_consumer
import grind_consumer/support/env

pub fn public_consumer_prune_finished_deletes_old_jobs_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_job =
        worker.new(
          env.unique("retention.echo"),
          input: grind_consumer.amount_codec(),
          output: grind_consumer.amount_codec(),
          perform: fn(amount) { Ok(amount) },
        )
        |> worker.with_queue(env.unique("retention"))
      use jobs <- env.with_grind(url, fn(config) {
        config |> grind.with_worker(echo_job) |> grind.without_pruner
      })
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(echo_job, 1))
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(1)))
      process.sleep(20)
      let assert Ok(pruned) =
        admin.prune_finished(
          jobs,
          older_than: duration.milliseconds(10),
          limit: 10_000,
        )
      { pruned >= 1 } |> should.be_true
      grind.state(jobs, handle) |> should.equal(Error(grind.JobNotFound))
      admin.prune_finished(jobs, older_than: duration.seconds(1), limit: 0)
      |> should.equal(Error(admin.InvalidLimit(limit: 0, maximum: 10_000)))
      env.mark("consumer-prune-finished-passed")
    }
  }
}
