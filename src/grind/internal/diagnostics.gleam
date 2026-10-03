//// Payload-free projections and bounded forwarding for queue diagnostics.

import grind/internal/diagnostic
import grind/internal/store
import pog
import sinal/forwarder.{type Forwarder}

@external(erlang, "grind_queue_ffi", "monotonic_us")
pub fn monotonic_us() -> Int

@external(erlang, "grind_queue_ffi", "node_name")
fn node_name() -> String

pub fn consumer_ref(owner: String) -> diagnostic.ConsumerRef {
  diagnostic.ConsumerRef(node: node_name(), consumer: owner)
}

pub fn queue_ref(queue: String, owner: String) -> diagnostic.QueueRef {
  diagnostic.QueueRef(queue:, consumer: consumer_ref(owner))
}

pub fn checkout(
  fwd: Forwarder,
  queue: diagnostic.QueueRef,
  operation: diagnostic.Operation,
  pool: diagnostic.PoolRole,
  measured: store.Measured(Result(a, b)),
) -> Result(a, b) {
  case measured.checkout {
    store.NoCheckout -> Nil
    store.CheckoutTiming(wait_us:, candidates:, outcome:) -> {
      let checkout = case outcome {
        store.CheckedOut -> diagnostic.CheckoutAcquired
        store.CheckoutUnavailable -> diagnostic.CheckoutUnavailable
      }
      let returned = case measured.value {
        Ok(_) -> diagnostic.CallSucceeded
        Error(_) -> diagnostic.CallFailed
      }
      let _ =
        forwarder.emit(
          fwd,
          diagnostic.checkout(),
          diagnostic.CheckoutMeasurements(
            count: 1,
            wait_us:,
            call_duration_us: measured.call_duration_us,
            candidates:,
          ),
          diagnostic.CheckoutMetadata(
            queue:,
            operation:,
            pool:,
            checkout:,
            returned:,
          ),
        )
      Nil
    }
  }
  measured.value
}

pub fn claim_failed(
  fwd: Forwarder,
  queue: diagnostic.QueueRef,
  stage: diagnostic.Operation,
  error: pog.QueryError,
  duration_us: Int,
) -> Nil {
  let failure = case error {
    pog.QueryTimeout -> diagnostic.TimedOut
    pog.ConnectionUnavailable -> diagnostic.ConnectionUnavailable
    pog.ConstraintViolated(..) | pog.PostgresqlError(..) -> diagnostic.Rejected
    pog.UnexpectedArgumentCount(..)
    | pog.UnexpectedArgumentType(..)
    | pog.UnexpectedResultType(..) -> diagnostic.UnexpectedResult
  }
  let _ =
    forwarder.emit(
      fwd,
      diagnostic.claim_failed(),
      diagnostic.ClaimFailedMeasurements(count: 1, duration_us:),
      diagnostic.ClaimFailedMetadata(queue:, stage:, failure:),
    )
  Nil
}
