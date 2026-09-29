import exception
import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process
import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import grind/diagnostic
import grind/observation
import sinal

@external(erlang, "grind_diagnostic_wire_probe", "attach")
fn native_attach(
  id: String,
  name: List(Atom),
  callback: fn(Dynamic, Dynamic) -> Nil,
) -> Result(Nil, Nil)

@external(erlang, "grind_diagnostic_wire_probe", "detach")
fn native_detach(id: String) -> Nil

@external(erlang, "grind_diagnostic_wire_probe", "emit")
fn native_emit(
  name: List(Atom),
  measurements: Dynamic,
  metadata: Dynamic,
) -> Nil

@external(erlang, "grind_diagnostic_wire_probe", "native_map")
fn native_map(entries: List(#(String, Dynamic))) -> Dynamic

@external(erlang, "grind_diagnostic_wire_probe", "put")
fn native_put(map: Dynamic, key: String, value: Dynamic) -> Dynamic

@external(erlang, "grind_diagnostic_wire_probe", "remove")
fn native_remove(map: Dynamic, key: String) -> Dynamic

fn context() -> diagnostic.AttemptContext {
  diagnostic.AttemptContext(
    ref: observation.JobRef(41, "wire.queue", "wire.worker", "v1"),
    attempt: observation.AttemptRef(72, 3, 2),
    consumer: diagnostic.ConsumerRef("node@host", "consumer-17"),
  )
}

fn queue() -> diagnostic.QueueRef {
  diagnostic.QueueRef("wire.queue", context().consumer)
}

fn queue_entries() -> List(#(String, Dynamic)) {
  [
    #("queue", dynamic.string("wire.queue")),
    #("node", dynamic.string("node@host")),
    #("consumer", dynamic.string("consumer-17")),
  ]
}

fn context_entries() -> List(#(String, Dynamic)) {
  list.append(queue_entries(), [
    #("job_id", dynamic.int(41)),
    #("worker_id", dynamic.string("wire.worker")),
    #("worker_version", dynamic.string("v1")),
    #("attempt_id", dynamic.int(72)),
    #("epoch", dynamic.int(3)),
    #("attempt", dynamic.int(2)),
  ])
}

// Native maps are captured independently of the typed decoder. Exact equality
// pins atom keys, integer/string values, units, flattening and absence of payload.
fn round_trip(
  event: sinal.Event(m, d),
  part: String,
  measurements: m,
  metadata: d,
  expected_measurements: Dynamic,
  expected_metadata: Dynamic,
) -> Nil {
  sinal.event_name(event) |> should.equal(["grind", "diagnostic", part])
  let raw = process.new_subject()
  let typed = process.new_subject()
  let assert Ok(Nil) =
    native_attach(part, sinal.event_native_name(event), fn(m, d) {
      process.send(raw, #(m, d))
    })
  use <- exception.defer(fn() { native_detach(part) })
  let assert Ok(id) = sinal.handler_id("diagnostics-wire-" <> part)
  let assert Ok(attached) =
    sinal.observe(id, event, fn(m, d) { process.send(typed, #(m, d)) })
  use <- exception.defer(fn() {
    let assert Ok(Nil) = sinal.detach(attached)
    Nil
  })
  sinal.emit(event, measurements, metadata) |> should.equal(Ok(Nil))
  process.receive(raw, 1000)
  |> should.equal(Ok(#(expected_measurements, expected_metadata)))
  process.receive(typed, 1000) |> should.equal(Ok(#(measurements, metadata)))
}

fn rejects(
  event: sinal.Event(m, d),
  measurements: Dynamic,
  metadata: Dynamic,
  measurements_are_bad: Bool,
) -> Nil {
  let delivered = process.new_subject()
  let failed = process.new_subject()
  let assert Ok(id) = sinal.handler_id("diagnostics-wire-rejected")
  let assert Ok(attachment) =
    sinal.attach(
      id,
      event,
      fn(_, _, _) {
        process.send(delivered, Nil)
        Ok(Nil)
      },
      fn(_, failure: sinal.HandlerFailure(Nil)) {
        process.send(failed, failure)
      },
    )
  use <- exception.defer(fn() {
    // Malformed native delivery auto-detaches its handler.
    let _ = sinal.detach(attachment)
    Nil
  })
  native_emit(sinal.event_native_name(event), measurements, metadata)
  let assert Ok(failure) = process.receive(failed, 1000)
  case failure {
    sinal.MalformedMeasurements(_) -> measurements_are_bad |> should.be_true()
    sinal.MalformedMetadata(_) -> measurements_are_bad |> should.be_false()
    sinal.HandlerReturned(_) -> should.fail()
  }
  // Native dispatch is synchronous; this is not a timing-based absence check.
  process.receive(delivered, 0) |> should.equal(Error(Nil))
}

fn rejects_enum(
  event: sinal.Event(m, d),
  measurements: Dynamic,
  metadata: Dynamic,
  key: String,
) -> Nil {
  rejects(
    event,
    measurements,
    native_put(metadata, key, dynamic.string("future_unknown")),
    False,
  )
  rejects(event, measurements, native_put(metadata, key, dynamic.int(1)), False)
}

pub fn renewal_native_contract_test() {
  let event = diagnostic.renewal()
  let measurements =
    native_map([
      #("count", dynamic.int(1)),
      #("duration_us", dynamic.int(1234)),
      #("remaining_lease_ms", dynamic.int(-3)),
    ])
  let base =
    native_map(
      list.append(context_entries(), [
        #("phase", dynamic.string("handler_running")),
        #("outcome", dynamic.string("renewed")),
      ]),
    )
  list.each(
    [
      #(diagnostic.HandlerRunning, "handler_running"),
      #(diagnostic.AcknowledgementPending, "acknowledgement_pending"),
    ],
    fn(phase) {
      list.each(
        [
          #(diagnostic.Renewed, "renewed"),
          #(diagnostic.SkippedLocked, "skipped_locked"),
          #(diagnostic.LiveFenceUnavailable, "live_fence_unavailable"),
          #(diagnostic.StorageFailed, "storage_failed"),
          #(diagnostic.CompletionBudgetExhausted, "completion_budget_exhausted"),
        ],
        fn(outcome) {
          round_trip(
            event,
            "renewal",
            diagnostic.RenewalMeasurements(1, 1234, Some(-3)),
            diagnostic.RenewalMetadata(context(), phase.0, outcome.0),
            measurements,
            base
              |> native_put("phase", dynamic.string(phase.1))
              |> native_put("outcome", dynamic.string(outcome.1)),
          )
        },
      )
    },
  )
  round_trip(
    event,
    "renewal",
    diagnostic.RenewalMeasurements(1, 1234, None),
    diagnostic.RenewalMetadata(
      context(),
      diagnostic.HandlerRunning,
      diagnostic.StorageFailed,
    ),
    native_remove(measurements, "remaining_lease_ms"),
    native_put(base, "outcome", dynamic.string("storage_failed")),
  )
  rejects_enum(event, measurements, base, "phase")
  rejects_enum(event, measurements, base, "outcome")
  rejects(
    event,
    native_put(measurements, "remaining_lease_ms", dynamic.string("unknown")),
    base,
    True,
  )
}

pub fn acknowledgement_native_contract_test() {
  let event = diagnostic.acknowledgement()
  let measurements =
    native_map([#("count", dynamic.int(1)), #("duration_us", dynamic.int(1234))])
  let base =
    native_map(
      list.append(context_entries(), [
        #("command_id", dynamic.string("ack-41-72-3")),
        #("outcome", dynamic.string("replied")),
      ]),
    )
  list.each(
    [
      #(diagnostic.AckReplied, "replied"),
      #(diagnostic.AckReconciled, "reconciled"),
      #(diagnostic.AckRolledBack, "rolled_back"),
      #(diagnostic.AckUnknown, "unknown"),
      #(diagnostic.AckFenceRejected, "fence_rejected"),
      #(diagnostic.AckCommandConflict, "command_conflict"),
      #(diagnostic.AckFailed, "failed"),
    ],
    fn(outcome) {
      round_trip(
        event,
        "acknowledgement",
        diagnostic.AcknowledgementMeasurements(1, 1234),
        diagnostic.AcknowledgementMetadata(context(), "ack-41-72-3", outcome.0),
        measurements,
        native_put(base, "outcome", dynamic.string(outcome.1)),
      )
    },
  )
  rejects_enum(event, measurements, base, "outcome")
  rejects(
    event,
    native_put(measurements, "duration_us", dynamic.string("1234")),
    base,
    True,
  )
}

pub fn acknowledgement_retry_native_contract_test() {
  let event = diagnostic.acknowledgement_retry()
  let measurements =
    native_map([
      #("count", dynamic.int(1)),
      #("retry_number", dynamic.int(2)),
      #("delay_ms", dynamic.int(100)),
      #("pending_duration_us", dynamic.int(3001)),
    ])
  let base =
    native_map(
      list.append(context_entries(), [
        #("command_id", dynamic.string("ack-41-72-3")),
        #("reason", dynamic.string("after_failure")),
      ]),
    )
  list.each(
    [
      #(diagnostic.RetryAfterFailure, "after_failure"),
      #(diagnostic.RetryAfterUnknown, "after_unknown"),
    ],
    fn(reason) {
      round_trip(
        event,
        "acknowledgement_retry",
        diagnostic.RetryMeasurements(1, 2, 100, 3001),
        diagnostic.RetryMetadata(context(), "ack-41-72-3", reason.0),
        measurements,
        native_put(base, "reason", dynamic.string(reason.1)),
      )
    },
  )
  rejects_enum(event, measurements, base, "reason")
}

fn operations() -> List(#(diagnostic.Operation, String)) {
  [
    #(diagnostic.QuarantineScan, "quarantine_scan"),
    #(diagnostic.ClaimCandidate, "claim_candidate"),
    #(diagnostic.LeaseRenewal, "lease_renewal"),
    #(diagnostic.Acknowledge, "acknowledge"),
    #(diagnostic.ReconcileAcknowledgement, "reconcile_acknowledgement"),
  ]
}

pub fn checkout_native_contract_test() {
  let event = diagnostic.checkout()
  let measurements =
    native_map([
      #("count", dynamic.int(1)),
      #("checkout_wait_us", dynamic.int(23)),
      #("call_duration_us", dynamic.int(45)),
      #("candidates", dynamic.int(2)),
    ])
  let base =
    native_map(
      list.append(queue_entries(), [
        #("operation", dynamic.string("quarantine_scan")),
        #("pool", dynamic.string("main")),
        #("checkout", dynamic.string("acquired")),
        #("returned", dynamic.string("succeeded")),
      ]),
    )
  list.each(operations(), fn(operation) {
    list.each(
      [#(diagnostic.MainPool, "main"), #(diagnostic.ReservedPool, "reserved")],
      fn(pool) {
        list.each(
          [
            #(diagnostic.CheckoutAcquired, "acquired"),
            #(diagnostic.CheckoutUnavailable, "unavailable"),
          ],
          fn(checkout) {
            list.each(
              [
                #(diagnostic.CallSucceeded, "succeeded"),
                #(diagnostic.CallFailed, "failed"),
              ],
              fn(returned) {
                round_trip(
                  event,
                  "checkout",
                  diagnostic.CheckoutMeasurements(1, 23, 45, 2),
                  diagnostic.CheckoutMetadata(
                    queue(),
                    operation.0,
                    pool.0,
                    checkout.0,
                    returned.0,
                  ),
                  measurements,
                  base
                    |> native_put("operation", dynamic.string(operation.1))
                    |> native_put("pool", dynamic.string(pool.1))
                    |> native_put("checkout", dynamic.string(checkout.1))
                    |> native_put("returned", dynamic.string(returned.1)),
                )
              },
            )
          },
        )
      },
    )
  })
  list.each(["operation", "pool", "checkout", "returned"], fn(key) {
    rejects_enum(event, measurements, base, key)
  })
  rejects(event, native_remove(measurements, "checkout_wait_us"), base, True)
}

pub fn claim_failed_native_contract_test() {
  let event = diagnostic.claim_failed()
  let measurements =
    native_map([#("count", dynamic.int(1)), #("duration_us", dynamic.int(1234))])
  let base =
    native_map(
      list.append(queue_entries(), [
        #("stage", dynamic.string("claim_candidate")),
        #("failure", dynamic.string("timed_out")),
      ]),
    )
  list.each(
    [
      #(diagnostic.TimedOut, "timed_out"),
      #(diagnostic.ConnectionUnavailable, "connection_unavailable"),
      #(diagnostic.Rejected, "rejected"),
      #(diagnostic.UnexpectedResult, "unexpected_result"),
    ],
    fn(failure) {
      round_trip(
        event,
        "claim_failed",
        diagnostic.ClaimFailedMeasurements(1, 1234),
        diagnostic.ClaimFailedMetadata(
          queue(),
          diagnostic.ClaimCandidate,
          failure.0,
        ),
        measurements,
        native_put(base, "failure", dynamic.string(failure.1)),
      )
    },
  )
  rejects_enum(event, measurements, base, "stage")
  rejects_enum(event, measurements, base, "failure")
}

pub fn capacity_native_contract_test() {
  let event = diagnostic.capacity()
  let measurements =
    native_map([
      #("maximum", dynamic.int(10)),
      #("active", dynamic.int(7)),
      #("running", dynamic.int(4)),
      #("ack_pending", dynamic.int(3)),
      #("available", dynamic.int(3)),
    ])
  let metadata =
    native_map(
      list.append(queue_entries(), [#("draining", dynamic.bool(True))]),
    )
  round_trip(
    event,
    "capacity",
    diagnostic.CapacityMeasurements(10, 7, 4, 3, 3),
    diagnostic.CapacityMetadata(queue(), True),
    measurements,
    metadata,
  )
  rejects(
    event,
    measurements,
    native_put(metadata, "draining", dynamic.string("true")),
    False,
  )
  rejects(
    event,
    native_put(measurements, "active", dynamic.string("7")),
    metadata,
    True,
  )
  rejects(event, measurements, native_remove(metadata, "consumer"), False)
}
