//// Settling due jobs on demand, and reading a job's settlement mark, through
//// the public API (docs/design/design.typ, "Declared settlement of abandoned
//// jobs").

import gleam/erlang/process
import gleam/list
import gleam/option.{None, Some}
import gleam/time/duration
import gleeunit/should
import grind
import grind/admin
import grind/facade/support.{string_codec, unique, with_runtime}
import grind/job
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/testing
import grind/worker

pub fn postgres_settle_due_settles_on_a_node_without_consumers_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let queue = unique("settle-due")
      let agent =
        worker.new(
          "settle.due",
          input: string_codec(),
          output: string_codec(),
          perform: fn(prompt) {
            process.sleep(60_000)
            Ok(prompt)
          },
        )
        |> worker.with_queue(queue)
        |> worker.with_error_codec(string_codec())
        |> worker.with_timeout(worker.After(duration.milliseconds(50)))
        |> worker.with_settlement(fn(_transaction, _abandoned) {
          Ok(worker.ConfirmFailure("the agent run stopped mid-way"))
        })
      use jobs <- with_runtime(url, fn(config) {
        config |> grind.with_worker(agent) |> grind.without_consumers
      })
      let assert Ok(grind.Inserted(run)) =
        grind.submit(jobs, job.new(agent, "summarise"))
      // The timeout holds the job uncertain; nothing settles it on its own on
      // a node without consumers.
      let assert Ok(1) =
        testing.drain(jobs, queue:, limit: 1, within: duration.seconds(10))
      let assert Ok(grind.Uncertain(_)) = grind.outcome(jobs, run)
      let assert Ok(summary) = summary_of(jobs, queue, job.id(run))
      let assert Some(mark) = summary.settlement
      admin.mark_tries(mark) |> should.equal(0)
      let assert Some(_) = admin.mark_due_at(mark)

      admin.settle_due(jobs, queue:, limit: 10) |> should.equal(Ok(1))

      let assert Ok(grind.Failed(grind.Business(reason), _, _)) =
        grind.outcome(jobs, run)
      reason |> should.equal("the agent run stopped mid-way")
      let assert Ok(summary) = summary_of(jobs, queue, job.id(run))
      let assert Some(mark) = summary.settlement
      admin.mark_tries(mark) |> should.equal(1)
      admin.mark_due_at(mark) |> should.equal(None)
      admin.settle_due(jobs, queue:, limit: 10) |> should.equal(Ok(0))
      mark_database_test_executed("settlement-settle-due")
    }
  }
}

fn summary_of(
  jobs: grind.Grind,
  queue: String,
  id: Int,
) -> Result(admin.JobSummary, Nil) {
  let assert Ok(summaries) =
    admin.list(jobs, admin.query(limit: 10) |> admin.in_queue(queue))
  list.find(summaries, fn(summary) { summary.id == id })
}
