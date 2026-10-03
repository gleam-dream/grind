//// The common task from outside the package: one runtime with two
//// differently typed workers, one submit each, `await`.

import gleam/erlang/process
import gleam/option
import gleam/time/duration
import gleeunit/should
import grind
import grind/job
import grind/queue
import grind/worker
import grind_consumer.{
  type PaymentError, type PaymentRequest, PaymentRejected, PaymentRequest,
}
import grind_consumer/support/env

fn charge_worker() -> worker.Worker(PaymentRequest, String, PaymentError) {
  worker.new(
    env.unique("payments.charge"),
    input: grind_consumer.payment_request_codec(),
    output: grind_consumer.text_codec(),
    perform: fn(request) {
      case request {
        PaymentRequest("missing/99", _) -> Error(PaymentRejected("missing/99"))
        PaymentRequest(key, amount) -> {
          let #(receipt, _) = env.apply_synthetic_effect(key, amount)
          Ok(receipt)
        }
      }
    },
  )
  |> worker.with_queue(env.unique("payments"))
  |> worker.with_error_codec(grind_consumer.payment_error_codec())
  |> worker.with_max_attempts(1)
}

fn report_worker() -> worker.Worker(Int, Int, Nil) {
  worker.new(
    env.unique("reports.double"),
    input: grind_consumer.amount_codec(),
    output: grind_consumer.amount_codec(),
    perform: fn(amount) { Ok(amount * 2) },
  )
  |> worker.with_queue(env.unique("reports"))
}

pub fn two_typed_workers_run_from_one_runtime_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      env.reset_effects()
      let charge = charge_worker()
      let report = report_worker()
      use jobs <- env.with_grind(url, fn(config) {
        config
        |> grind.with_worker(charge)
        |> grind.with_worker(report)
        |> grind.with_queue(
          queue.new(charge.queue)
          |> queue.with_concurrency(4)
          |> queue.with_poll_interval(duration.milliseconds(50)),
        )
      })
      let assert Ok(grind.Inserted(paid)) =
        grind.submit(jobs, job.new(charge, PaymentRequest("order/1", 12)))
      let assert Ok(grind.Inserted(rejected)) =
        grind.submit(jobs, job.new(charge, PaymentRequest("missing/99", 1)))
      let assert Ok(grind.Inserted(doubled)) =
        grind.submit(jobs, job.new(report, 21))
      let within = duration.seconds(10)
      let assert Ok(grind.Succeeded(_receipt)) =
        grind.await(jobs, paid, within:)
      grind.await(jobs, rejected, within:)
      |> should.equal(
        Ok(grind.Failed(
          grind.Business(PaymentRejected("missing/99")),
          option.Some(job.BudgetExhausted),
          "worker returned an application error",
        )),
      )
      grind.await(jobs, doubled, within:)
      |> should.equal(Ok(grind.Succeeded(42)))
      env.synthetic_effect_count("order/1") |> should.equal(1)
      env.mark("two-worker-consumer-passed")
    }
  }
}

pub fn a_handler_reads_its_job_context_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let seen = process.new_subject()
      let probe =
        worker.responding(
          env.unique("context.probe"),
          input: grind_consumer.amount_codec(),
          output: grind_consumer.amount_codec(),
          handle: fn(context, amount) {
            process.send(seen, #(
              worker.job_id(context),
              worker.attempt(context),
            ))
            worker.Succeeded(amount)
          },
        )
        |> worker.with_queue(env.unique("context"))
      use jobs <- env.with_grind(url, grind.with_worker(_, probe))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(probe, 1))
      process.receive(seen, within: 10_000)
      |> should.equal(Ok(#(job.id(handle), 1)))
      env.mark("consumer-observes-context-passed")
    }
  }
}
