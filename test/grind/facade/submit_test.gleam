//// The public path: one runtime, typed workers, one submit, `await`.

import gleam/erlang/process
import gleam/option.{Some}
import gleam/string
import gleam/time/duration
import gleeunit/should
import grind
import grind/admin
import grind/facade/support.{int_codec, string_codec, unique, with_runtime}
import grind/job
import grind/queue
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/unique
import grind/worker
import sinal/correlation

pub fn facade_quickstart_submit_and_await_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let greet =
        worker.new(
          unique("facade.greet"),
          input: string_codec(),
          output: string_codec(),
          perform: fn(name) { Ok("Hello, " <> name) },
        )
        |> worker.with_queue(unique("facade-greet"))
      use jobs <- with_runtime(url, grind.with_worker(_, greet))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(greet, "Ada"))
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded("Hello, Ada")))
      grind.state(jobs, handle) |> should.equal(Ok(job.Succeeded))
      grind.arguments(jobs, handle) |> should.equal(Ok("Ada"))
      let assert Ok(rebound) = grind.bind(jobs, greet, job.id(handle))
      grind.outcome(jobs, rebound)
      |> should.equal(Ok(grind.Succeeded("Hello, Ada")))
      mark_database_test_executed("facade-quickstart-passed")
    }
  }
}

pub fn facade_await_returns_pending_when_time_runs_out_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let later =
        worker.new(
          unique("facade.later"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n) },
        )
        |> worker.with_queue(unique("facade-later"))
      use jobs <- with_runtime(url, grind.with_worker(_, later))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(later, 1) |> job.after(duration.hours(1)))
      grind.await(jobs, handle, within: duration.milliseconds(200))
      |> should.equal(Ok(grind.Pending(job.Scheduled)))
      mark_database_test_executed("facade-await-pending-passed")
    }
  }
}

pub fn facade_job_id_is_an_idempotency_key_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_worker =
        worker.new(
          unique("facade.echo"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n) },
        )
        |> worker.with_queue(unique("facade-echo"))
      use jobs <- with_runtime(url, fn(config) {
        config |> grind.with_worker(echo_worker) |> grind.without_consumers
      })
      let id = unique("order")
      let assert Ok(grind.Inserted(first)) =
        grind.submit(jobs, job.new(echo_worker, 7) |> job.with_id(id))
      let assert Ok(grind.Inserted(second)) =
        grind.submit(jobs, job.new(echo_worker, 7) |> job.with_id(id))
      job.id(second) |> should.equal(job.id(first))
      grind.submit(jobs, job.new(echo_worker, 8) |> job.with_id(id))
      |> should.equal(Error(grind.IdConflict))
      grind.submit(jobs, job.new(echo_worker, 8) |> job.with_id(""))
      |> should.equal(Error(grind.EmptyJobId))
      mark_database_test_executed("facade-job-id-idempotent-passed")
    }
  }
}

pub fn facade_payload_limit_rejects_before_writing_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_worker =
        worker.new(
          unique("facade.payload"),
          input: string_codec(),
          output: string_codec(),
          perform: fn(text) { Ok(text) },
        )
        |> worker.with_queue(unique("facade-payload"))
      use jobs <- with_runtime(url, fn(config) {
        config
        |> grind.with_worker(echo_worker)
        |> grind.with_max_payload_bytes(16)
      })
      let large = string.repeat("x", 64)
      grind.submit(jobs, job.new(echo_worker, large))
      |> should.equal(Error(grind.PayloadTooLarge(bytes: 66, limit: 16)))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(echo_worker, "short"))
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded("short")))
      mark_database_test_executed("facade-payload-limit-passed")
    }
  }
}

pub fn facade_handler_context_carries_job_identity_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let seen = process.new_subject()
      let probe =
        worker.responding(
          unique("facade.context"),
          input: int_codec(),
          output: int_codec(),
          handle: fn(context, n) {
            process.send(seen, context)
            worker.Succeeded(n)
          },
        )
        |> worker.with_queue(unique("facade-context"))
        |> worker.with_max_attempts(3)
      use jobs <- with_runtime(url, grind.with_worker(_, probe))
      let assert Ok(order) = correlation.from_string(unique("order"))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(probe, 5) |> job.with_correlation(order))
      let assert Ok(context) = process.receive(seen, within: 10_000)
      worker.job_id(context) |> should.equal(job.id(handle))
      worker.attempt(context) |> should.equal(1)
      worker.max_attempts(context) |> should.equal(3)
      worker.snooze_count(context) |> should.equal(0)
      worker.queue(context) |> should.equal(job.queue(handle))
      worker.correlation(context) |> should.equal(order)
      worker.deadline(context) |> option.is_some |> should.be_true
      // The context carries the runtime's pool.
      worker.connection(context) |> should.equal(grind.connection(jobs))
      // The correlation is stored with the job.
      let assert Ok([summary]) =
        admin.list(
          jobs,
          admin.query(limit: 1)
            |> admin.in_queue(job.queue(handle))
            |> admin.after(job.id(handle) - 1),
        )
      summary.correlation |> should.equal(Some(order))
      mark_database_test_executed("facade-context-passed")
    }
  }
}

pub fn facade_generated_correlation_and_queue_override_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let seen = process.new_subject()
      let probe =
        worker.responding(
          unique("facade.override"),
          input: int_codec(),
          output: int_codec(),
          handle: fn(context, n) {
            process.send(seen, worker.correlation(context))
            worker.Succeeded(n)
          },
        )
        |> worker.with_queue(unique("facade-override"))
      use jobs <- with_runtime(url, fn(config) {
        config
        |> grind.with_worker(probe)
        |> grind.with_queue(
          queue.new(probe.queue)
          |> queue.with_concurrency(2)
          |> queue.with_poll_interval(duration.milliseconds(50)),
        )
      })
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(probe, 1) |> job.with_max_attempts(1))
      let assert Ok(generated) = process.receive(seen, within: 10_000)
      // A correlation Grind generates is 32 hexadecimal characters.
      string.length(correlation.to_string(generated)) |> should.equal(32)
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(1)))
      let assert Ok(grind.Inserted(elsewhere)) =
        grind.submit(
          jobs,
          job.new(probe, 2) |> job.with_queue(unique("facade-unpolled")),
        )
      grind.await(jobs, elsewhere, within: duration.milliseconds(300))
      |> should.equal(Ok(grind.Pending(job.Queued)))
      mark_database_test_executed("facade-correlation-generated-passed")
    }
  }
}

pub fn facade_retry_policy_and_error_codec_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let failing =
        worker.new(
          unique("facade.failing"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Error(n * 10) },
        )
        |> worker.with_queue(unique("facade-failing"))
        |> worker.with_error_codec(int_codec())
        |> worker.with_retry_policy(fn(_error, _attempt) { worker.DoNotRetry })
      use jobs <- with_runtime(url, grind.with_worker(_, failing))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(failing, 4))
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(
        Ok(grind.Failed(
          grind.Business(40),
          Some(job.RetryDeclined),
          "worker returned an application error",
        )),
      )
      mark_database_test_executed("facade-retry-policy-passed")
    }
  }
}

pub fn facade_unique_policy_returns_existing_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_worker =
        worker.new(
          unique("facade.unique"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n) },
        )
        |> worker.with_queue(unique("facade-unique"))
      use jobs <- with_runtime(url, fn(config) {
        config |> grind.with_worker(echo_worker) |> grind.without_consumers
      })
      let policy = unique_policy()
      let assert Ok(grind.Inserted(first)) =
        grind.submit(jobs, job.new(echo_worker, 3) |> job.unique(policy))
      let assert Ok(grind.Existing(conflict) as admission) =
        grind.submit(jobs, job.new(echo_worker, 3) |> job.unique(policy))
      conflict.job_id |> should.equal(job.id(first))
      conflict.state |> should.equal(job.Queued)
      // `handle` returns the occupying job, readable without `bind`.
      grind.handle(admission) |> should.equal(first)
      grind.state(jobs, conflict.handle) |> should.equal(Ok(job.Queued))
      grind.handle(grind.Inserted(first)) |> should.equal(first)
      mark_database_test_executed("facade-unique-existing-passed")
    }
  }
}

fn unique_policy() -> unique.Policy(Int) {
  unique.policy(
    unique.full_input(),
    unique.within(duration.hours(1), from: unique.FromInsertion),
  )
}
