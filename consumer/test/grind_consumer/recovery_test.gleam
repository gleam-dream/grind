//// Failure handling from outside the package: retries, cancellation of a
//// running job, and audited resolution of an uncertain one.

import gleam/erlang/process
import gleam/option.{None, Some}
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

/// Cancellation expresses intent to stop; explicit effect evidence remains
/// available until the application investigates and attributes its resolution.
pub fn public_consumer_cancellation_preserves_uncertainty_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      cancellation_and_uncertainty(url, True)
      cancellation_and_uncertainty(url, False)
      env.mark("consumer-cancellation-preserves-uncertainty-passed")
    }
  }
}

fn cancellation_and_uncertainty(url: String, cancel_first: Bool) {
  let started = process.new_subject()
  let evidence = "provider may have executed payment operation-42"
  let charge =
    worker.responding(
      env.unique("recovery.cancel-uncertain"),
      input: grind_consumer.amount_codec(),
      output: grind_consumer.amount_codec(),
      handle: fn(_context, _amount) {
        let release = process.new_subject()
        process.send(started, release)
        let assert Ok(Nil) = process.receive(release, within: 10_000)
        worker.Uncertain(evidence)
      },
    )
    |> worker.with_queue(env.unique("cancel-uncertain"))
    |> worker.with_error_codec(grind_consumer.amount_codec())
  use jobs <- env.with_grind(url, fn(config) {
    config |> grind.with_worker(charge) |> grind.without_pruner
  })
  let assert Ok(grind.Inserted(handle)) =
    grind.submit(jobs, job.new(charge, 42))
  let assert Ok(release) = process.receive(started, within: 10_000)
  case cancel_first {
    True ->
      grind.cancel(jobs, handle)
      |> should.equal(Ok(grind.CancellationRequested))
    False -> Nil
  }
  process.send(release, Nil)
  grind.await(jobs, handle, within: duration.seconds(10))
  |> should.equal(Ok(grind.Uncertain(evidence)))
  // Repeating intent, including after acknowledgement, preserves evidence.
  grind.cancel(jobs, handle) |> should.equal(Ok(grind.AlreadyUncertain))
  grind.cancel(jobs, handle) |> should.equal(Ok(grind.AlreadyUncertain))
  let assert Ok([summary]) =
    admin.list(
      jobs,
      admin.query(limit: 10)
        |> admin.in_queue(charge.queue)
        |> admin.in_state(job.Uncertain),
    )
  summary.id |> should.equal(job.id(handle))
  summary.description |> should.equal(Some(evidence))
  summary.finished_at |> should.equal(None)
  process.sleep(20)
  let assert Ok(_) =
    admin.prune_finished(
      jobs,
      older_than: duration.milliseconds(10),
      limit: 10_000,
    )
  grind.outcome(jobs, handle) |> should.equal(Ok(grind.Uncertain(evidence)))
  let replay =
    admin.resolution(
      admin.AuthorizeReplay,
      id: env.unique("forbidden-replay"),
      by: "operator@example.com",
      details: "cancellation intent forbids a new attempt",
    )
  admin.resolve_uncertain(jobs, handle, replay)
  |> should.equal(Error(admin.CancellationPending))
  let decision = case cancel_first {
    True -> admin.ConfirmSuccess(42)
    False -> admin.ConfirmFailure(42)
  }
  let final_state = case cancel_first {
    True -> job.Succeeded
    False -> job.BusinessFailed
  }
  let confirmed =
    admin.resolution(
      decision,
      id: env.unique("confirmed-payment"),
      by: "operator@example.com",
      details: "provider ledger confirms operation-42",
    )
  admin.resolve_uncertain(jobs, handle, confirmed)
  |> should.equal(Ok(admin.Applied(final_state)))
  admin.resolve_uncertain(jobs, handle, confirmed)
  |> should.equal(Ok(admin.AlreadyApplied(final_state)))
  case cancel_first {
    True -> grind.outcome(jobs, handle) |> should.equal(Ok(grind.Succeeded(42)))
    False -> {
      let assert Ok(grind.Failed(grind.Business(42), _, _)) =
        grind.outcome(jobs, handle)
      Nil
    }
  }
  grind.cancel(jobs, handle)
  |> should.equal(Ok(grind.AlreadyFinished(final_state)))
  process.receive(started, within: 0) |> should.equal(Error(Nil))
}
