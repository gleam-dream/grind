import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleeunit/should
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
    worker.codec("consumer-observation-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("consumer-observation-output-v1", json.string, decode.string)
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
  let assert Ok(id) = sinal.handler_id("consumer-observation-acknowledged")
  let assert Ok(attachment) =
    sinal.observe(id, observation.acknowledged(), fn(measurements, metadata) {
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
    worker.codec("consumer-claimed-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("consumer-claimed-output-v1", json.string, decode.string)
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
  let assert Ok(id) = sinal.handler_id("consumer-observation-claimed")
  let assert Ok(attachment) =
    sinal.observe(id, observation.claimed(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() {
    let assert Ok(Nil) = sinal.detach(attachment)
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
  env.mark("consumer-observes-claimed-passed")
}
