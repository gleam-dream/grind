import exception
import gleam/erlang/process
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind_consumer.{PaymentRejected, PaymentRequest}
import grind_consumer/support/env
import grind_consumer/support/wait
import grind_consumer/support/workers

pub fn public_consumer_executes_typed_workers_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_public_consumer_test(url)
  }
}

fn run_public_consumer_test(url: String) -> Nil {
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(60_000)
    |> queue.validate_policy
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  env.reset_effects()

  let payment_probe = process.new_subject()
  let report_probe = process.new_subject()
  let payment_worker = workers.payment_worker(payment_probe)
  let report_worker = workers.report_worker(report_probe)
  let assert Ok(workers) = registry.new("external-consumer")
  let assert Ok(workers) = registry.register(workers, payment_worker)
  let assert Ok(workers) = registry.register(workers, report_worker)

  let assert Ok(payment_handle) =
    postgres.submit(
      database,
      "external-consumer",
      payment_worker,
      PaymentRequest("payment/42", 500),
    )
  // A second, independently admitted job reuses the same application
  // idempotency key as `payment_handle`. Nothing is preseeded: this is what
  // actually exercises the app's own dedup table, because the worker's own
  // effect application for this second job is the one that must observe the
  // key already taken by the first job's own execution.
  let assert Ok(dedup_handle) =
    postgres.submit(
      database,
      "external-consumer",
      payment_worker,
      PaymentRequest("payment/42", 500),
    )
  let assert Ok(report_handle) =
    postgres.submit(database, "external-consumer", report_worker, 8)
  let assert Ok(failure_handle) =
    postgres.submit(
      database,
      "external-consumer",
      payment_worker,
      PaymentRequest("missing/99", 0),
    )

  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  process.receive(payment_probe, within: 5000)
  |> should.equal(Ok(workers.ChargeInvoked))
  process.receive(payment_probe, within: 5000)
  |> should.equal(Ok(workers.ChargeInvoked))
  process.receive(report_probe, within: 5000)
  |> should.equal(Ok(workers.ReportInvoked))
  process.receive(payment_probe, within: 5000)
  |> should.equal(Ok(workers.ChargeInvoked))

  // Handler probes arrive before their database acknowledgements. Wait for the
  // committed job states themselves before reading typed outcomes.
  wait.await_state(database, payment_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait.await_state(database, dedup_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait.await_state(database, report_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait.await_state(database, failure_handle, job.BusinessFailed, 250)
  |> should.equal(True)
  // Both payment jobs called into the same synthetic effect for the same
  // key, yet the key was only ever actually applied once.
  env.synthetic_effect_count("payment/42") |> should.equal(1)
  // The receipt carries a unique token minted only inside the app's own
  // table (consumer_effect.erl), so reading it back here and asserting both
  // jobs' committed outcomes equal it proves the outcome's value actually
  // came from that table -- not merely a value this test could have
  // predicted as a pure function of the key.
  let assert Ok(payment_receipt) = env.synthetic_effect_receipt("payment/42")
  postgres.outcome(database, payment_handle)
  |> should.equal(Ok(job.SucceededWith(payment_receipt)))
  postgres.outcome(database, dedup_handle)
  |> should.equal(Ok(job.SucceededWith(payment_receipt)))
  postgres.outcome(database, report_handle)
  |> should.equal(Ok(job.SucceededWith(16)))
  postgres.outcome(database, failure_handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(
      PaymentRejected("missing/99"),
      worker.BudgetExhausted,
    )),
  )
  env.mark("two-worker-consumer-passed")
}
