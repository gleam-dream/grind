import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleeunit
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/worker
import grind_consumer.{
  type PaymentError, type PaymentRequest, PaymentRejected, PaymentRequest,
}

pub fn main() -> Nil {
  gleeunit.main()
}

type Probe {
  ChargeInvoked
  ReportInvoked
}

@external(erlang, "consumer_test_env", "database_url")
fn database_url() -> Result(String, Nil)

@external(erlang, "consumer_test_env", "storage_failure_url")
fn storage_failure_url() -> Result(String, Nil)

@external(erlang, "consumer_test_env", "mark")
fn mark(name: String) -> Nil

@external(erlang, "consumer_effect", "reset")
fn reset_effects() -> Nil

@external(erlang, "consumer_effect", "apply")
fn apply_synthetic_effect(key: String, amount: Int) -> #(String, Int)

@external(erlang, "consumer_effect", "count")
fn synthetic_effect_count(key: String) -> Int

pub fn queue_policy_is_checked_before_start_test() {
  queue.default_policy()
  |> queue.with_poll_interval(0)
  |> queue.validate_policy
  |> should.equal(Error(queue.PollIntervalMustBePositive))

  queue.default_policy()
  |> queue.with_maximum_jobs_per_poll(-1)
  |> queue.validate_policy
  |> should.equal(Error(queue.MaximumJobsPerPollMustBePositive))

  queue.default_policy()
  |> queue.with_shutdown_grace(-1)
  |> queue.validate_policy
  |> should.equal(Error(queue.ShutdownGraceMustBeNonNegative))
}

pub fn public_consumer_executes_typed_workers_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_public_consumer_test(url)
  }
}

fn run_public_consumer_test(url: String) -> Nil {
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(60_000)
    |> queue.with_maximum_jobs_per_poll(3)
    |> queue.validate_policy
  let assert Ok(settings) =
    postgres.settings(url, process.new_name("external_consumer_pool"))
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  reset_effects()

  let payment_probe = process.new_subject()
  let report_probe = process.new_subject()
  let payment_worker = payment_worker(payment_probe)
  let report_worker = report_worker(report_probe)
  let assert Ok(workers) = registry.new("external-consumer")
  let assert Ok(workers) = registry.register(workers, payment_worker)
  let assert Ok(workers) = registry.register(workers, report_worker)

  let #(seed_receipt, seed_count) = apply_synthetic_effect("payment/42", 500)
  seed_receipt |> should.equal("synthetic-receipt/payment/42")
  seed_count |> should.equal(1)

  let assert Ok(payment_handle) =
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

  let assert Ok(consumer) = queue.start_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  process.receive(payment_probe, within: 5000)
  |> should.equal(Ok(ChargeInvoked))
  process.receive(report_probe, within: 5000)
  |> should.equal(Ok(ReportInvoked))
  process.receive(payment_probe, within: 5000)
  |> should.equal(Ok(ChargeInvoked))

  // Handler probes arrive before their database acknowledgements. Wait for the
  // committed job states themselves before reading typed outcomes.
  await_state(database, payment_handle, job.Succeeded, 250)
  |> should.equal(True)
  await_state(database, report_handle, job.Succeeded, 250)
  |> should.equal(True)
  await_state(database, failure_handle, job.BusinessFailed, 250)
  |> should.equal(True)
  synthetic_effect_count("payment/42") |> should.equal(1)
  postgres.outcome(database, payment_handle)
  |> should.equal(Ok(job.SucceededWith("synthetic-receipt/payment/42")))
  postgres.outcome(database, report_handle)
  |> should.equal(Ok(job.SucceededWith(16)))
  postgres.outcome(database, failure_handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(
      PaymentRejected("missing/99"),
      job.BudgetExhausted,
    )),
  )
  mark("two-worker-consumer-passed")
}

fn await_state(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  expected: job.State,
  remaining_checks: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(state) ->
      case state == expected, remaining_checks > 0 {
        True, _ -> True
        False, True -> {
          process.sleep(20)
          await_state(database, handle, expected, remaining_checks - 1)
        }
        False, False -> False
      }
    Error(_) -> False
  }
}

fn payment_worker(
  probe: process.Subject(Probe),
) -> worker.Worker(PaymentRequest, String, PaymentError) {
  let assert Ok(input) =
    worker.codec(
      "payment-request-v1",
      encode_payment_request,
      decode_payment_request(),
    )
  let assert Ok(output) = worker.codec("receipt-v1", json.string, decode.string)
  let assert Ok(error) =
    worker.codec(
      "payment-error-v1",
      encode_payment_error,
      decode_payment_error(),
    )
  let assert Ok(definition) =
    worker.define_with_error_codec(
      "payments.charge",
      "v1",
      input,
      output,
      error,
      fn(request) {
        process.send(probe, ChargeInvoked)
        case request {
          PaymentRequest("missing/99", _) ->
            Error(PaymentRejected("missing/99"))
          PaymentRequest(key, amount) -> {
            let #(receipt, _) = apply_synthetic_effect(key, amount)
            Ok(receipt)
          }
        }
      },
    )
  let assert Ok(single_attempt) = worker.with_max_attempts(definition, 1)
  single_attempt
}

fn encode_payment_request(request: PaymentRequest) -> json.Json {
  case request {
    PaymentRequest(key, amount) ->
      json.object([
        #("idempotency_key", json.string(key)),
        #("amount", json.int(amount)),
      ])
  }
}

fn decode_payment_request() -> decode.Decoder(PaymentRequest) {
  use key <- decode.field("idempotency_key", decode.string)
  use amount <- decode.field("amount", decode.int)
  decode.success(PaymentRequest(key, amount))
}

fn report_worker(
  probe: process.Subject(Probe),
) -> worker.Worker(Int, Int, Nil) {
  let assert Ok(input) = worker.codec("report-count-v1", json.int, decode.int)
  let assert Ok(output) = worker.codec("report-total-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define("reports.total", "v3", input, output, fn(count) {
      process.send(probe, ReportInvoked)
      Ok(count * 2)
    })
  definition
}

fn encode_payment_error(error: PaymentError) -> json.Json {
  case error {
    PaymentRejected(key) ->
      json.object([
        #("kind", json.string("payment_rejected")),
        #("key", json.string(key)),
      ])
  }
}

fn decode_payment_error() -> decode.Decoder(PaymentError) {
  use kind <- decode.field("kind", decode.string)
  use key <- decode.field("key", decode.string)
  case kind {
    "payment_rejected" -> decode.success(PaymentRejected(key))
    _ -> decode.failure(PaymentRejected(key), "known payment error kind")
  }
}

pub fn storage_start_failure_is_reported_test() {
  case storage_failure_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(settings) =
        postgres.settings(url, process.new_name("consumer_storage_failure"))
        |> postgres.validate
      let failure_observed = case postgres.start(settings) {
        Error(_) -> True
        Ok(database) -> {
          use <- exception.defer(fn() { postgres.close(database) })
          case postgres.migrate(database) {
            Error(_) -> True
            Ok(Nil) -> False
          }
        }
      }
      failure_observed |> should.equal(True)
      mark("consumer-storage-failure-passed")
    }
  }
}
