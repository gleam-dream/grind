//// A validating codec, built only from public Grind modules. Its encoder
//// returns `Result`, as a json_blueprint codec with refinements does. A
//// rejected input fails `submit` with `submission.InvalidInput`, and a
//// rejected output ends the job as `job.RuntimeFailed` after one attempt.

import exception
import gleam/dynamic/decode
import gleam/json
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/submission
import grind/worker
import grind_consumer.{type PaymentRequest, PaymentRequest}
import grind_consumer/support/env
import grind_consumer/support/workers

pub fn public_consumer_validating_codec_rejections_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_validating_codec_rejections_test(url)
  }
}

/// The amount must be positive, like a Blueprint `integer_between(1, ..)`.
fn encode_checked_request(
  request: PaymentRequest,
) -> Result(json.Json, String) {
  case request {
    PaymentRequest(_, amount) if amount < 1 -> Error("amount must be positive")
    _ -> Ok(workers.encode_payment_request(request))
  }
}

fn encode_checked_receipt(receipt: String) -> Result(json.Json, String) {
  case receipt {
    "" -> Error("receipt must not be empty")
    _ -> Ok(json.string(receipt))
  }
}

fn validated_worker() -> worker.Worker(PaymentRequest, String, Nil) {
  let assert Ok(input) =
    worker.codec(
      "checked-payment-request-v1",
      encode_checked_request,
      workers.decode_payment_request(),
    )
  let assert Ok(output) =
    worker.codec("checked-receipt-v1", encode_checked_receipt, decode.string)
  let assert Ok(definition) =
    worker.define("payments.checked", "v1", input, output, fn(request) {
      let PaymentRequest(key, _) = request
      case key {
        "no-receipt" -> Ok("")
        _ -> Ok("receipt:" <> key)
      }
    })
  definition
}

fn run_validating_codec_rejections_test(url: String) -> Nil {
  let assert Ok(settings) = postgres.settings(url) |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let definition = validated_worker()
  let queue_name = "consumer-validated"
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, env.manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  case
    postgres.submit(database, queue_name, definition, PaymentRequest("a", 0))
  {
    Error(submission.InvalidInput(reason)) ->
      reason |> should.equal("amount must be positive")
    _ -> panic as "a rejected input must fail submit with InvalidInput"
  }
  // Nothing was queued, so nothing is claimed.
  queue.process_one(consumer) |> should.equal(Ok(False))

  let assert Ok(accepted) =
    postgres.submit(database, queue_name, definition, PaymentRequest("ok", 5))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.outcome(database, accepted)
  |> should.equal(Ok(job.SucceededWith("receipt:ok")))

  let assert Ok(unrecordable) =
    postgres.submit(
      database,
      queue_name,
      definition,
      PaymentRequest("no-receipt", 5),
    )
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, unrecordable) |> should.equal(Ok(job.RuntimeFailed))
  postgres.outcome(database, unrecordable)
  |> should.equal(
    Ok(job.FailedOperationally(
      "output codec rejected the handler's output: receipt must not be empty",
    )),
  )
  queue.process_one(consumer) |> should.equal(Ok(False))
  env.mark("consumer-validating-codec-passed")
}
