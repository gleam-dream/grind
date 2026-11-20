//// Settling due jobs on demand, and reading a job's settlement mark, through
//// the public API (docs/design/design.typ, "Declared settlement of abandoned
//// jobs").

import exception
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/time/duration
import gleeunit/should
import grind
import grind/abandoned as abandonment
import grind/admin
import grind/facade/support.{int_codec, string_codec, unique, with_runtime}
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

/// A settlement that needs a slow outside read cannot make it inside its
/// function. It submits a checker job on its own transaction and leaves the
/// job for it; the checker resolves the job once it has its evidence.
pub fn postgres_settlement_hands_a_slow_check_to_a_job_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let queue = unique("settle-checker")
      let name = process.new_name("settle_checker_grind")
      let jobs = grind.named(name)
      let agent =
        worker.new(
          "settle.checked",
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
      let checker =
        worker.new(
          "settle.checker",
          input: int_codec(),
          output: int_codec(),
          perform: fn(run_id) {
            // The slow outside read happens here, outside any lock.
            let assert Ok(run) = grind.bind(jobs, agent, run_id)
            let assert Ok(_) =
              admin.resolve_uncertain(
                jobs,
                run,
                admin.resolution(
                  admin.ConfirmFailure("the provider has no record of the run"),
                  id: "checked:" <> int.to_string(run_id),
                  by: "settle.checker",
                  details: "provider status read after abandonment",
                ),
              )
            Ok(run_id)
          },
        )
        |> worker.with_queue(queue)
      let agent =
        agent
        |> worker.with_settlement(fn(transaction, abandoned) {
          let run_id = abandonment.job_id(abandoned)
          case grind.submit_in(jobs, transaction, job.new(checker, run_id)) {
            Ok(_) ->
              Ok(worker.LeaveUncertain("a checker job reads the provider"))
            Error(error) -> Error(grind.describe_submit_error(error))
          }
        })
      let assert Ok(_) =
        grind.start(
          grind.new(support.pool_config(url))
            |> grind.with_worker(agent)
            |> grind.with_worker(checker)
            |> grind.without_consumers
            |> grind.with_startup_migration,
          name,
        )
      use <- exception.defer(fn() { grind.stop(jobs) })
      let assert Ok(grind.Inserted(run)) =
        grind.submit(jobs, job.new(agent, "summarise"))
      let assert Ok(1) =
        testing.drain(jobs, queue:, limit: 1, within: duration.seconds(10))

      admin.settle_due(jobs, queue:, limit: 10) |> should.equal(Ok(1))
      let assert Ok(grind.Uncertain(_)) = grind.outcome(jobs, run)

      // The checker job committed with the declined settlement; it runs.
      let assert Ok(1) =
        testing.drain(jobs, queue:, limit: 1, within: duration.seconds(10))
      let assert Ok(grind.Failed(grind.Business(reason), _, _)) =
        grind.outcome(jobs, run)
      reason |> should.equal("the provider has no record of the run")
      mark_database_test_executed("settlement-checker-job")
    }
  }
}

/// An effect that must happen only after the decision commits is a
/// follow-up job submitted in the settlement's transaction: it commits with
/// the decision and runs afterwards, for an applied decision and for an
/// authorized replay alike.
pub fn postgres_follow_up_job_commits_with_the_decision_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let queue = unique("settle-follow-up")
      let name = process.new_name("settle_follow_up_grind")
      let jobs = grind.named(name)
      let told = process.new_subject()
      let tell =
        worker.new(
          "settle.tell",
          input: int_codec(),
          output: int_codec(),
          perform: fn(run_id) {
            process.send(told, run_id)
            Ok(run_id)
          },
        )
        |> worker.with_queue(queue)
      let agent =
        worker.new(
          "settle.followed",
          input: string_codec(),
          output: string_codec(),
          perform: fn(prompt) {
            case prompt {
              "replayed" -> Ok(prompt)
              _ -> {
                process.sleep(60_000)
                Ok(prompt)
              }
            }
          },
        )
        |> worker.with_queue(queue)
        |> worker.with_error_codec(string_codec())
        |> worker.with_timeout(worker.After(duration.milliseconds(50)))
        |> worker.with_settlement(fn(transaction, abandoned) {
          let run_id = abandonment.job_id(abandoned)
          case grind.submit_in(jobs, transaction, job.new(tell, run_id)) {
            Error(error) -> Error(grind.describe_submit_error(error))
            Ok(_) ->
              case abandonment.input(abandoned) {
                "replay me" -> Ok(worker.AuthorizeReplay)
                _ -> Ok(worker.ConfirmFailure("the agent run stopped mid-way"))
              }
          }
        })
      let assert Ok(_) =
        grind.start(
          grind.new(support.pool_config(url))
            |> grind.with_worker(agent)
            |> grind.with_worker(tell)
            |> grind.without_consumers
            |> grind.with_startup_migration,
          name,
        )
      use <- exception.defer(fn() { grind.stop(jobs) })

      // An applied decision.
      let assert Ok(grind.Inserted(failed)) =
        grind.submit(jobs, job.new(agent, "summarise"))
      let assert Ok(1) =
        testing.drain(jobs, queue:, limit: 1, within: duration.seconds(10))
      process.receive(told, within: 0) |> should.equal(Error(Nil))
      admin.settle_due(jobs, queue:, limit: 10) |> should.equal(Ok(1))
      let assert Ok(grind.Failed(_, _, _)) = grind.outcome(jobs, failed)
      let assert Ok(1) =
        testing.drain(jobs, queue:, limit: 1, within: duration.seconds(10))
      process.receive(told, within: 1000) |> should.equal(Ok(job.id(failed)))

      // An authorized replay.
      let assert Ok(grind.Inserted(replayed)) =
        grind.submit(jobs, job.new(agent, "replay me"))
      let assert Ok(1) =
        testing.drain(jobs, queue:, limit: 1, within: duration.seconds(10))
      admin.settle_due(jobs, queue:, limit: 10) |> should.equal(Ok(1))
      let assert Ok(grind.Pending(job.Queued)) = grind.outcome(jobs, replayed)
      // The follow-up and the replayed run are both due now.
      let assert Ok(2) =
        testing.drain(jobs, queue:, limit: 2, within: duration.seconds(10))
      process.receive(told, within: 1000) |> should.equal(Ok(job.id(replayed)))
      mark_database_test_executed("settlement-follow-up-job")
    }
  }
}
