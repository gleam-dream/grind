//// Payload-free projections and bounded forwarding for queue diagnostics.

import grind/internal/store
import grind/telemetry
import pog
import sinal/forwarder.{type Forwarder}

@external(erlang, "grind_queue_ffi", "monotonic_us")
pub fn monotonic_us() -> Int

@external(erlang, "grind_queue_ffi", "node_name")
fn node_name() -> String

pub fn consumer_ref(owner: String) -> telemetry.ConsumerRef {
  telemetry.ConsumerRef(node: node_name(), consumer: owner)
}

pub fn queue_ref(queue: String, owner: String) -> telemetry.QueueRef {
  telemetry.QueueRef(queue:, consumer: consumer_ref(owner))
}

pub fn checkout(
  fwd: Forwarder,
  queue: telemetry.QueueRef,
  operation: telemetry.Operation,
  pool: telemetry.PoolRole,
  measured: store.Measured(Result(a, b)),
) -> Result(a, b) {
  case measured.checkout {
    store.NoCheckout -> Nil
    store.CheckoutTiming(wait_us:, candidates:, outcome:) -> {
      let checkout = case outcome {
        store.CheckedOut -> telemetry.CheckoutAcquired
        store.CheckoutUnavailable -> telemetry.CheckoutUnavailable
      }
      let returned = case measured.value {
        Ok(_) -> telemetry.CallSucceeded
        Error(_) -> telemetry.CallFailed
      }
      let _ =
        forwarder.emit(
          fwd,
          telemetry.checkout(),
          telemetry.CheckoutMeasurements(
            count: 1,
            wait_us:,
            call_duration_us: measured.call_duration_us,
            candidates:,
          ),
          telemetry.CheckoutMetadata(
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
  queue: telemetry.QueueRef,
  stage: telemetry.Operation,
  error: pog.QueryError,
  duration_us: Int,
) -> Nil {
  let failure = case error {
    pog.QueryTimeout -> telemetry.TimedOut
    pog.ConnectionUnavailable -> telemetry.ConnectionUnavailable
    pog.ConstraintViolated(..) | pog.PostgresqlError(..) -> telemetry.Rejected
    pog.UnexpectedArgumentCount(..)
    | pog.UnexpectedArgumentType(..)
    | pog.UnexpectedResultType(..) -> telemetry.UnexpectedResult
  }
  let _ =
    forwarder.emit(
      fwd,
      telemetry.claim_failed(),
      telemetry.ClaimFailedMeasurements(count: 1, duration_us:),
      telemetry.ClaimFailedMetadata(queue:, stage:, failure:),
    )
  Nil
}
