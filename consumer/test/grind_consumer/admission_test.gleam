//// Admission from outside the package: uniqueness, idempotent ids, and
//// enqueueing inside the application's own transaction.

import gleam/time/duration
import gleam/time/timestamp
import gleeunit/should
import grind
import grind/job
import grind/unique
import grind/worker
import grind_consumer
import grind_consumer/support/env
import pog

fn echo_worker(queue: String) -> worker.Worker(Int, Int, Nil) {
  worker.new(
    env.unique("admission.echo"),
    input: grind_consumer.amount_codec(),
    output: grind_consumer.amount_codec(),
    perform: fn(amount) { Ok(amount) },
  )
  |> worker.with_queue(queue)
}

pub fn a_duplicate_finds_the_existing_job_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_job = echo_worker(env.unique("admission-unique"))
      use jobs <- env.with_grind(url, fn(config) {
        config |> grind.with_worker(echo_job) |> grind.without_consumers
      })
      let policy =
        unique.policy(
          unique.full_input(),
          unique.within(duration.hours(1), from: unique.FromInsertion),
        )
      let assert Ok(grind.Inserted(first)) =
        grind.submit(jobs, job.new(echo_job, 1) |> job.unique(policy))
      // A retried duplicate, even under its own id, converges on the job.
      let retry = env.unique("retry")
      let assert Ok(grind.Existing(conflict)) =
        grind.submit(
          jobs,
          job.new(echo_job, 1) |> job.unique(policy) |> job.with_id(retry),
        )
      conflict.job_id |> should.equal(job.id(first))
      let assert Ok(grind.Existing(_)) =
        grind.submit(
          jobs,
          job.new(echo_job, 1) |> job.unique(policy) |> job.with_id(retry),
        )
      env.mark("consumer-unique-admission-existing-conflict-retry-passed")
    }
  }
}

pub fn a_duplicate_reschedules_across_queues_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let first_queue = env.unique("admission-first")
      let echo_job = echo_worker(first_queue)
      use jobs <- env.with_grind(url, fn(config) {
        config |> grind.with_worker(echo_job) |> grind.without_consumers
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
          job.new(echo_job, 2)
            |> job.after(duration.hours(1))
            |> job.unique(policy),
        )
      let assert Ok(grind.Rescheduled(conflict)) =
        grind.submit(
          jobs,
          job.new(echo_job, 2)
            |> job.with_queue(env.unique("admission-second"))
            |> job.unique(policy),
        )
      conflict.job_id |> should.equal(job.id(first))
      conflict.queue |> should.equal(first_queue)
      env.mark("consumer-unique-reschedule-across-queues-passed")
    }
  }
}

pub fn public_consumer_submit_with_id_retry_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_job = echo_worker(env.unique("admission-id"))
      use jobs <- env.with_grind(url, grind.with_worker(_, echo_job))
      let id = env.unique("invoice")
      let assert Ok(grind.Inserted(first)) =
        grind.submit(jobs, job.new(echo_job, 3) |> job.with_id(id))
      let assert Ok(grind.Inserted(again)) =
        grind.submit(jobs, job.new(echo_job, 3) |> job.with_id(id))
      job.id(again) |> should.equal(job.id(first))
      grind.submit(jobs, job.new(echo_job, 4) |> job.with_id(id))
      |> should.equal(Error(grind.IdConflict))
      grind.await(jobs, first, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(3)))
      env.mark("consumer-submit-with-id-retry-passed")
    }
  }
}

pub fn a_job_commits_with_the_application_transaction_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_job = echo_worker(env.unique("admission-tx"))
      use jobs <- env.with_grind(url, grind.with_worker(_, echo_job))
      let db = grind.connection(jobs)
      let assert Ok(_) =
        pog.query(
          "CREATE TABLE IF NOT EXISTS consumer_orders (id text PRIMARY KEY)",
        )
        |> pog.execute(db)
      let order = env.unique("order")
      let assert Ok(grind.Inserted(handle)) =
        pog.transaction(db, fn(tx) {
          let assert Ok(_) =
            pog.query("INSERT INTO consumer_orders (id) VALUES ($1)")
            |> pog.parameter(pog.text(order))
            |> pog.execute(tx)
          grind.submit_in(jobs, tx, job.new(echo_job, 9) |> job.with_id(order))
        })
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(9)))
      env.mark("consumer-submit-in-passed")
    }
  }
}
