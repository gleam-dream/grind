//// Failure handling from outside the package: retries, cancellation of a
//// running job, and audited resolution of an uncertain one.

import gleam/erlang/process
import gleam/time/duration
import gleeunit/should
import grind
import grind/admin
import grind/job
import grind/queue
import grind/worker
import grind_consumer
import grind_consumer/support/env

pub fn public_consumer_retry_and_running_cancellation_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let counter = env.unique("retry-counter")
      env.reset_counter(counter)
      let flaky =
        worker.new(
          env.unique("recovery.flaky"),
          input: grind_consumer.amount_codec(),
          output: grind_consumer.amount_codec(),
          perform: fn(amount) {
            case env.next_counter(counter) {
              1 -> Error(Nil)
              _ -> Ok(amount)
            }
          },
        )
        |> worker.with_queue(env.unique("recovery-flaky"))
        |> worker.with_retry_policy(fn(_error, _attempt) {
          worker.RetryAfter(duration.milliseconds(50))
        })
      let running = process.new_subject()
      let blocking =
        worker.responding(
          env.unique("recovery.blocking"),
          input: grind_consumer.amount_codec(),
          output: grind_consumer.amount_codec(),
          handle: fn(context, amount) {
            process.send(running, Nil)
            case
              process.selector_receive(
                worker.cancellation(context),
                within: 20_000,
              )
            {
              Ok(Nil) -> worker.Cancelled("caller cancelled")
              Error(Nil) -> worker.Succeeded(amount)
            }
          },
        )
        |> worker.with_queue(env.unique("recovery-blocking"))
      use jobs <- env.with_grind(url, fn(config) {
        config
        |> grind.with_statement_deadline(duration.milliseconds(1500))
        |> grind.with_unique_lock_wait(duration.milliseconds(400))
        |> grind.with_worker(flaky)
        |> grind.with_worker(blocking)
        |> grind.with_queue(
          queue.new(flaky.queue)
          |> queue.with_lease(duration.seconds(6))
          |> queue.with_poll_interval(duration.milliseconds(50)),
        )
        |> grind.with_queue(
          queue.new(blocking.queue) |> queue.with_lease(duration.seconds(6)),
        )
      })
      let assert Ok(grind.Inserted(retried)) =
        grind.submit(jobs, job.new(flaky, 3))
      grind.await(jobs, retried, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(3)))

      let assert Ok(grind.Inserted(cancelled)) =
        grind.submit(jobs, job.new(blocking, 1))
      let assert Ok(Nil) = process.receive(running, within: 10_000)
      grind.cancel(jobs, cancelled)
      |> should.equal(Ok(grind.CancellationRequested))
      let assert Ok(grind.Cancelled(_)) =
        grind.await(jobs, cancelled, within: duration.seconds(10))
      env.mark("consumer-retry-and-cancellation-passed")
    }
  }
}

/// A handler crashes after its effect and before its acknowledgement. The
/// lease expires, the default `HoldUncertain` policy holds the job, and an
/// operator confirms the effect the application's own record shows.
pub fn public_consumer_effect_crash_uncertainty_audited_recovery_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      env.reset_effects()
      let key = env.unique("charge")
      env.arm_crash_after_effect(key)
      let charge =
        worker.new(
          env.unique("recovery.charge"),
          input: grind_consumer.payment_request_codec(),
          output: grind_consumer.text_codec(),
          perform: fn(request) {
            let #(receipt, _) =
              env.apply_synthetic_effect(
                request.idempotency_key,
                request.amount,
              )
            Ok(receipt)
          },
        )
        |> worker.with_queue(env.unique("recovery-charge"))
      use jobs <- env.with_grind(url, fn(config) {
        config
        |> grind.with_statement_deadline(duration.milliseconds(1500))
        |> grind.with_unique_lock_wait(duration.milliseconds(400))
        |> grind.with_worker(charge)
        |> grind.with_queue(
          queue.new(charge.queue)
          |> queue.with_lease(duration.seconds(6))
          |> queue.with_poll_interval(duration.milliseconds(100)),
        )
      })
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(
          jobs,
          job.new(charge, grind_consumer.PaymentRequest(key, 5)),
        )
      let assert Ok(grind.Uncertain(_)) =
        grind.await(jobs, handle, within: duration.seconds(30))
      // The operator sweep finds it, checks the application's own record,
      // and confirms the success that happened.
      let assert Ok([summary]) =
        admin.list(
          jobs,
          admin.query(limit: 10)
            |> admin.in_queue(charge.queue)
            |> admin.in_state(job.Uncertain),
        )
      summary.id |> should.equal(job.id(handle))
      let assert Ok(receipt) = env.synthetic_effect_receipt(key)
      let resolution =
        admin.resolution(
          admin.ConfirmSuccess(receipt),
          id: env.unique("resolution"),
          by: "operator@example.com",
          details: "the gateway shows the charge",
        )
      admin.resolve_uncertain(jobs, handle, resolution)
      |> should.equal(Ok(admin.Applied(job.Succeeded)))
      admin.resolve_uncertain(jobs, handle, resolution)
      |> should.equal(Ok(admin.AlreadyApplied(job.Succeeded)))
      grind.outcome(jobs, handle) |> should.equal(Ok(grind.Succeeded(receipt)))
      env.synthetic_effect_count(key) |> should.equal(1)
      env.mark("consumer-uncertainty-audited-recovery-passed")
    }
  }
}
