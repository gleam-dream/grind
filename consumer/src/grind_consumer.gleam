//// A caller-owned domain, as an application would write it: its own types,
//// their JSON codecs, and the Grind workers over them.

import gleam/dynamic/decode
import gleam/json
import grind/worker

/// A payment request. Its idempotency key belongs to the application.
pub type PaymentRequest {
  PaymentRequest(idempotency_key: String, amount: Int)
}

pub type PaymentError {
  PaymentRejected(key: String)
}

pub fn payment_request_codec() -> worker.Codec(PaymentRequest) {
  worker.codec(
    worker.infallible(fn(request: PaymentRequest) {
      json.object([
        #("idempotency_key", json.string(request.idempotency_key)),
        #("amount", json.int(request.amount)),
      ])
    }),
    {
      use key <- decode.field("idempotency_key", decode.string)
      use amount <- decode.field("amount", decode.int)
      decode.success(PaymentRequest(key, amount))
    },
  )
}

pub fn payment_error_codec() -> worker.Codec(PaymentError) {
  worker.codec(
    worker.infallible(fn(error: PaymentError) {
      json.object([#("rejected", json.string(error.key))])
    }),
    {
      use key <- decode.field("rejected", decode.string)
      decode.success(PaymentRejected(key))
    },
  )
}

/// A codec that refuses a negative amount, as a validating codec (a
/// json_blueprint codec with a refinement) would.
pub fn amount_codec() -> worker.Codec(Int) {
  worker.codec(
    fn(amount) {
      case amount >= 0 {
        True -> Ok(json.int(amount))
        False -> Error("amount must not be negative")
      }
    },
    decode.int,
  )
}

pub fn text_codec() -> worker.Codec(String) {
  worker.codec(worker.infallible(json.string), decode.string)
}
