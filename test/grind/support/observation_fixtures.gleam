import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleeunit/should
import grind/internal/registry
import grind/internal/worker
import grind/support/worker_failure.{type LookupFailure}
import grind/telemetry
import sinal
import sinal/forwarder

// -- `[grind, job, acknowledged]` observation (grind/observation) ----------
//
// `grind/postgres` emits this event through its own `sinal/forwarder`, never
// through a plain `sinal.emit` — see `grind/observation`'s module
// documentation. These tests attach with plain `sinal.observe`, exactly as
// an application would; `postgres_acknowledged_observation_isolation_test` is what proves
// dispatch happens in the forwarder process rather than the coordinator.

pub type AcknowledgedSignal {
  AcknowledgedSignal(
    measurements: telemetry.JobMeasurements,
    metadata: telemetry.AcknowledgedMetadata,
  )
}

pub type DroppedSignal {
  DroppedSignal(
    measurements: forwarder.Dropped,
    metadata: forwarder.DroppedMetadata,
  )
}

pub type OverflowGateEntered {
  OverflowGateEntered(process.Subject(Nil))
}

pub fn attach_acknowledged_observer(
  run: fn(telemetry.JobMeasurements, telemetry.AcknowledgedMetadata) -> Nil,
) -> sinal.Attachment {
  sinal.observe(telemetry.acknowledged(), run)
}

pub fn attach_dropped_observer(
  run: fn(forwarder.Dropped, forwarder.DroppedMetadata) -> Nil,
) -> sinal.Attachment {
  sinal.observe(forwarder.dropped_event(), run)
}

/// Counts how many pending messages are already waiting on `subject`,
/// draining them. A short per-check timeout (rather than `within: 0`) tolerates
/// a message still in flight from a handler that just ran, without turning
/// this into a fixed wall-clock wait for a specific count.
pub fn drain_subject_count(subject: process.Subject(Nil), count: Int) -> Int {
  case process.receive(subject, within: 50) {
    Ok(Nil) -> drain_subject_count(subject, count + 1)
    Error(Nil) -> count
  }
}

/// Deterministic negative/"exactly N" check for a sentinel-observed
/// `signal`: rather than a fixed wall-clock wait for "nothing more arrives"
/// (fragile — either too short under load, or slow), this asserts the very
/// *next* event received is a distinct, known-good sentinel acknowledgement
/// run through the same consumer/producer afterward. `sinal/forwarder`
/// guarantees per-producer FIFO delivery, so if the code under test had
/// wrongly emitted an extra event for the original job, it would have been
/// enqueued ahead of the sentinel's and would arrive here instead —
/// deterministically, not racily.
pub fn assert_next_observation_is_sentinel(
  signal: process.Subject(AcknowledgedSignal),
  sentinel_job_id: Int,
) -> Nil {
  let assert Ok(AcknowledgedSignal(_, metadata)) =
    process.receive(signal, within: 5000)
  metadata.ref.job_id |> should.equal(sentinel_job_id)
}

/// Registers a trivial, instantly-completing worker onto an existing
/// registry (same queue), for the "run a sentinel job through the same
/// consumer/producer afterward" pattern `assert_next_observation_is_sentinel`
/// needs. Kept separate from whatever worker the test under negative-path
/// scrutiny uses (which may block waiting on a release gate), so driving the
/// sentinel to completion can never itself hang.
pub fn register_sentinel_worker(
  workers: registry.Registry,
  id_suffix: String,
) -> #(registry.Registry, worker.Worker(Int, String, LookupFailure)) {
  let assert Ok(input_codec) =
    worker.codec(
      "observation-sentinel-" <> id_suffix <> "-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-sentinel-" <> id_suffix <> "-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "telemetry.sentinel." <> id_suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("sentinel-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.register(workers, definition)
  #(workers, definition)
}
