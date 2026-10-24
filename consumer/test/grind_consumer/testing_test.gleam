//// `grind/testing` from outside the package: a handler without a
//// database, and a queue drained on demand.

import gleam/time/duration
import gleeunit/should
import grind
import grind/job
import grind/testing
import grind/worker
import grind_consumer.{PaymentRejected, PaymentRequest}
import grind_consumer/support/env

fn charge() -> worker.Worker(
  grind_consumer.PaymentRequest,
  String,
  grind_consumer.PaymentError,
) {
  worker.new(
    env.unique("testing.charge"),
    input: grind_consumer.payment_request_codec(),
    output: grind_consumer.text_codec(),
    perform: fn(request) {
      case request.amount > 0 {
        True -> Ok("paid " <> request.idempotency_key)
        False -> Error(PaymentRejected(request.idempotency_key))
      }
    },
  )
  |> worker.with_queue(env.unique("testing"))
  |> worker.with_error_codec(grind_consumer.payment_error_codec())
}

pub fn perform_runs_a_handler_without_a_database_test() {
  testing.perform(charge(), PaymentRequest("a", 1))
  |> should.equal(Ok(worker.Succeeded("paid a")))
  testing.perform(charge(), PaymentRequest("b", 0))
  |> should.equal(Ok(worker.Failed(PaymentRejected("b"))))
}

pub fn drain_runs_due_jobs_on_demand_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let charge = charge()
      use jobs <- env.with_grind(url, fn(config) {
        config |> grind.with_worker(charge) |> grind.without_consumers
      })
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(charge, PaymentRequest("c", 2)))
      testing.drain(
        jobs,
        queue: charge.queue,
        limit: 10,
        within: duration.seconds(10),
      )
      |> should.equal(Ok(1))
      grind.outcome(jobs, handle) |> should.equal(Ok(grind.Succeeded("paid c")))
      env.mark("consumer-testing-support-passed")
    }
  }
}
