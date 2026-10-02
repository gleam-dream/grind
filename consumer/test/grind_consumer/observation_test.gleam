import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{None}
import gleeunit/should
import grind/diagnostic
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/worker
import grind_consumer/support/env
import sinal

/// Attaches `sinal.observe` to `grind/observation.acknowledged()` using only
/// public imports (`grind/observation`, `sinal`), runs one typed job through
/// the public consumer API, and decodes the resulting record — proving the
/// descriptor Grind owns is usable end to end from outside the package,
/// exactly as an application would use it.
pub fn public_consumer_observes_acknowledged_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_public_consumer_observation_test(url)
  }
}

fn run_public_consumer_observation_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let assert Ok(input_codec) =
    worker.codec(
      "consumer-observation-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "consumer-observation-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(echo_worker) =
    worker.define(
      "consumer.observation.echo",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("external-consumer-observation")
  let assert Ok(workers) = registry.register(workers, echo_worker)
  let assert Ok(handle) =
    postgres.submit(database, "external-consumer-observation", echo_worker, 41)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.acknowledged(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() {
    let assert Ok(Nil) = sinal.detach(attachment)
    Nil
  })

  let assert Ok(consumer) = queue.start(database, workers, env.manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.AcknowledgedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.ref.queue |> should.equal("external-consumer-observation")
  metadata.ref.worker_id |> should.equal("consumer.observation.echo")
  metadata.committed_state |> should.equal(job.Succeeded)
  metadata.proposed |> should.equal(observation.ProposedSuccess)
  metadata.confirmation |> should.equal(observation.Replied)
  env.mark("consumer-observes-acknowledged-passed")
}

pub fn public_consumer_observes_claimed_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_public_consumer_claimed_observation_test(url)
  }
}

/// Round 2's `[grind, job, claimed]` descriptor, attached the same way an
/// application would from outside the package (public imports only).
fn run_public_consumer_claimed_observation_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let assert Ok(input_codec) =
    worker.codec(
      "consumer-claimed-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "consumer-claimed-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(echo_worker) =
    worker.define(
      "consumer.claimed.echo",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("external-consumer-claimed")
  let assert Ok(workers) = registry.register(workers, echo_worker)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.claimed(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() {
    let assert Ok(Nil) = sinal.detach(attachment)
    Nil
  })

  let capacity = process.new_subject()
  let capacity_attachment =
    sinal.observe(diagnostic.capacity(), fn(measurements, metadata) {
      case
        metadata.queue.queue == "external-consumer-claimed"
        && measurements.running == 1
      {
        True -> process.send(capacity, #(measurements, metadata))
        False -> Nil
      }
    })
  use <- exception.defer(fn() {
    let assert Ok(Nil) = sinal.detach(capacity_attachment)
    Nil
  })

  let assert Ok(handle) =
    postgres.submit(database, "external-consumer-claimed", echo_worker, 41)
  let assert Ok(consumer) = queue.start(database, workers, env.manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.ClaimedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.ref.queue |> should.equal("external-consumer-claimed")
  metadata.ref.worker_id |> should.equal("consumer.claimed.echo")
  metadata.previous_state |> should.equal(job.Queued)
  let assert Ok(#(occupied, local)) = process.receive(capacity, within: 5000)
  occupied |> should.equal(diagnostic.CapacityMeasurements(1, 1, 1, 0, 0))
  local.queue.queue |> should.equal("external-consumer-claimed")
  local.queue.consumer.node |> should.not_equal("")
  local.queue.consumer.consumer |> should.not_equal("")
  local.draining |> should.equal(False)
  env.mark("consumer-observes-claimed-passed")
  env.mark("consumer-observes-capacity-passed")
}

/// All descriptors and their public records are usable without internal imports.
/// Synthetic synchronous dispatch proves the external codec surface; the test
/// above separately proves a real runtime capacity event reaches an application.
pub fn public_diagnostic_descriptors_are_usable_test() {
  let consumer = diagnostic.ConsumerRef("public@host", "public-consumer")
  let local = diagnostic.QueueRef("public.queue", consumer)
  let context =
    diagnostic.AttemptContext(
      observation.JobRef(1, "public.queue", "public.worker", "v1"),
      observation.AttemptRef(2, 3, 1),
      consumer,
    )
  public_diagnostic(
    diagnostic.renewal(),
    "renewal",
    diagnostic.RenewalMeasurements(1, 20, None),
    diagnostic.RenewalMetadata(
      context,
      diagnostic.HandlerRunning,
      diagnostic.StorageFailed,
    ),
  )
  public_diagnostic(
    diagnostic.acknowledgement(),
    "acknowledgement",
    diagnostic.AcknowledgementMeasurements(1, 30),
    diagnostic.AcknowledgementMetadata(
      context,
      "public-command",
      diagnostic.AckReconciled,
    ),
  )
  public_diagnostic(
    diagnostic.acknowledgement_retry(),
    "acknowledgement_retry",
    diagnostic.RetryMeasurements(1, 1, 10, 40),
    diagnostic.RetryMetadata(
      context,
      "public-command",
      diagnostic.RetryAfterUnknown,
    ),
  )
  public_diagnostic(
    diagnostic.checkout(),
    "checkout",
    diagnostic.CheckoutMeasurements(1, 10, 20, 1),
    diagnostic.CheckoutMetadata(
      local,
      diagnostic.LeaseRenewal,
      diagnostic.ReservedPool,
      diagnostic.CheckoutAcquired,
      diagnostic.CallSucceeded,
    ),
  )
  public_diagnostic(
    diagnostic.claim_failed(),
    "claim_failed",
    diagnostic.ClaimFailedMeasurements(1, 20),
    diagnostic.ClaimFailedMetadata(
      local,
      diagnostic.ClaimCandidate,
      diagnostic.ConnectionUnavailable,
    ),
  )
  public_diagnostic(
    diagnostic.capacity(),
    "capacity",
    diagnostic.CapacityMeasurements(2, 1, 0, 1, 1),
    diagnostic.CapacityMetadata(local, False),
  )
}

fn public_diagnostic(
  event: sinal.Event(m, d),
  part: String,
  measurements: m,
  metadata: d,
) -> Nil {
  sinal.name(event) |> should.equal(["grind", "diagnostic", part])
  let signal = process.new_subject()
  let attachment =
    sinal.observe(event, fn(m, d) { process.send(signal, #(m, d)) })
  use <- exception.defer(fn() {
    let assert Ok(Nil) = sinal.detach(attachment)
    Nil
  })
  sinal.emit(event, measurements, metadata)
  process.receive(signal, 1000) |> should.equal(Ok(#(measurements, metadata)))
}
