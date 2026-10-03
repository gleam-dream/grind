import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import grind/internal/worker
import grind_consumer.{
  type PaymentError, type PaymentRequest, PaymentRejected, PaymentRequest,
}
import grind_consumer/support/env

pub type Probe {
  ChargeInvoked
  ReportInvoked
}

/// A running handler's own barrier: it reports it has started (handing back
/// a release subject) and then blocks until the test releases it.
pub type Barrier {
  BarrierStarted(process.Subject(BarrierRelease))
}

pub type BarrierRelease {
  BarrierRelease
}

pub fn payment_worker(
  probe: process.Subject(Probe),
) -> worker.Worker(PaymentRequest, String, PaymentError) {
  let assert Ok(input) =
    worker.codec(
      "payment-request-v1",
      worker.infallible(encode_payment_request),
      decode_payment_request(),
    )
  let assert Ok(output) =
    worker.codec("receipt-v1", worker.infallible(json.string), decode.string)
  let assert Ok(error) =
    worker.codec(
      "payment-error-v1",
      worker.infallible(encode_payment_error),
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
            let #(receipt, _) = env.apply_synthetic_effect(key, amount)
            Ok(receipt)
          }
        }
      },
    )
  let assert Ok(single_attempt) = worker.with_max_attempts(definition, 1)
  single_attempt
}

pub fn encode_payment_request(request: PaymentRequest) -> json.Json {
  case request {
    PaymentRequest(key, amount) ->
      json.object([
        #("idempotency_key", json.string(key)),
        #("amount", json.int(amount)),
      ])
  }
}

pub fn decode_payment_request() -> decode.Decoder(PaymentRequest) {
  use key <- decode.field("idempotency_key", decode.string)
  use amount <- decode.field("amount", decode.int)
  decode.success(PaymentRequest(key, amount))
}

pub fn report_worker(
  probe: process.Subject(Probe),
) -> worker.Worker(Int, Int, Nil) {
  let assert Ok(input) =
    worker.codec("report-count-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output) =
    worker.codec("report-total-v1", worker.infallible(json.int), decode.int)
  let assert Ok(definition) =
    worker.define("reports.total", "v3", input, output, fn(count) {
      process.send(probe, ReportInvoked)
      Ok(count * 2)
    })
  definition
}

pub fn encode_payment_error(error: PaymentError) -> json.Json {
  case error {
    PaymentRejected(key) ->
      json.object([
        #("kind", json.string("payment_rejected")),
        #("key", json.string(key)),
      ])
  }
}

pub fn decode_payment_error() -> decode.Decoder(PaymentError) {
  use kind <- decode.field("kind", decode.string)
  use key <- decode.field("key", decode.string)
  case kind {
    "payment_rejected" -> decode.success(PaymentRejected(key))
    _ -> decode.failure(PaymentRejected(key), "known payment error kind")
  }
}

/// A worker whose bound retry policy retries exactly once (a short real
/// delay) and then succeeds. The bound retry policy callback receives
/// Grind's own `RetryContext` and reports its `current_attempt` on
/// `retry_context_probe`. The `perform` handler itself has no such access
/// (see docs/IMPLEMENTATION-SCOPE.md, "Job lifecycle and attempt history"),
/// so it tracks its own invocation count on `attempt_probe` instead, which
/// in this single-worker, no-concurrent-claims scenario advances in
/// lockstep with Grind's persisted attempt count.
pub fn retry_then_succeed_worker(
  attempt_probe: process.Subject(Int),
  retry_context_probe: process.Subject(Int),
) -> worker.Worker(Int, String, Nil) {
  let assert Ok(input) =
    worker.codec("retry-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output) =
    worker.codec(
      "retry-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define("consumer.retry_then_succeed", "v1", input, output, fn(value) {
      let attempt = env.next_counter("consumer-retry-attempt")
      process.send(attempt_probe, attempt)
      case attempt {
        1 -> Error(Nil)
        _ -> Ok("retry-succeeded-" <> int.to_string(value))
      }
    })
  let assert Ok(short_delay) = worker.retry_delay(50)
  let policy =
    worker.retry_policy(fn(_failure, context) {
      process.send(retry_context_probe, context.current_attempt)
      worker.RetryAfter(short_delay)
    })
  worker.with_retry_policy(definition, policy)
}

/// A worker that reports it has started, blocks on a barrier, and only then
/// performs its (synthetic, idempotent) effect. Used to hold a job in
/// `Executing` state long enough for the test to request cancellation.
pub fn cancel_while_running_worker(
  started: process.Subject(Barrier),
) -> worker.Worker(PaymentRequest, String, Nil) {
  let assert Ok(input) =
    worker.codec(
      "cancel-running-request-v1",
      worker.infallible(encode_payment_request),
      decode_payment_request(),
    )
  let assert Ok(output) =
    worker.codec(
      "cancel-running-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "consumer.cancel_while_running",
      "v1",
      input,
      output,
      fn(request) {
        let release = process.new_subject()
        process.send(started, BarrierStarted(release))
        let PaymentRequest(key, amount) = request
        case process.receive(release, within: 10_000) {
          Ok(BarrierRelease) -> {
            let #(receipt, _) = env.apply_synthetic_effect(key, amount)
            Ok(receipt)
          }
          Error(Nil) -> Ok("released-by-timeout")
        }
      },
    )
  definition
}

/// A worker whose effect application may have been armed to crash right
/// after applying (see `arm_crash_after_effect`). Tracks its own invocation
/// count per key so the test can distinguish the crashing attempt from a
/// later authorized replay.
pub fn fault_prone_payment_worker() -> worker.Worker(
  PaymentRequest,
  String,
  Nil,
) {
  let assert Ok(input) =
    worker.codec(
      "fault-prone-request-v1",
      worker.infallible(encode_payment_request),
      decode_payment_request(),
    )
  let assert Ok(output) =
    worker.codec(
      "fault-prone-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "consumer.fault_prone_payment",
      "v1",
      input,
      output,
      fn(request) {
        let PaymentRequest(key, amount) = request
        let _ = env.next_counter(key)
        let #(receipt, _) = env.apply_synthetic_effect(key, amount)
        Ok(receipt)
      },
    )
  definition
}

/// The `Int` input / `String` output worker used by admission and
/// retention tests.
pub fn unique_echo_worker(id: String) -> worker.Worker(Int, String, Nil) {
  let assert Ok(input) =
    worker.codec(id <> "-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output) =
    worker.codec(
      id <> "-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(id, "v1", input, output, fn(value) {
      Ok(int.to_string(value))
    })
  definition
}
