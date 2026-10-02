import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{Some}
import gleeunit/should
import grind/internal/attempt
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/observers.{detach}
import grind/worker
import pog
import sinal

pub fn postgres_released_observation_emission_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_released_observation_emission_test(database_url)
  }
}

/// `release_unstarted_claim` refunds a claim whose temporary worker child
/// never started (before `execute_claim`/`acknowledge_claim` ever run).
/// `restored_state` is the same state the row held before this claim.
fn run_released_observation_emission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("released-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("released-emission-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "released.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("released-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "released-emission", definition, 1)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.released(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "released-emission",
      workers,
      "released-emission-owner",
      30_000,
    )
  let #(claimed_id, attempt_id, epoch) = attempt.claim_identity(claimed)
  attempt.release_unstarted(
    database,
    "released-emission",
    "released-emission-owner",
    claimed,
  )
  |> should.equal(Ok(True))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.ReleasedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(claimed_id)
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.attempt.attempt_id |> should.equal(attempt_id)
  metadata.attempt.epoch |> should.equal(epoch)
  metadata.attempt.attempt |> should.equal(1)
  metadata.restored_state |> should.equal(job.Queued)
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  mark_database_test_executed("released-observation-emission-passed")
}

pub fn postgres_released_observation_absent_when_not_unstarted_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_released_observation_absent_test(database_url)
  }
}

/// `release_unstarted_claim` returning `Ok(False)` (the attempt fence no
/// longer matches — here, because the claim was already acknowledged) must
/// never emit — proven by a genuine release through the exact same producer
/// arriving as the very next `released` observation.
fn run_released_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("released-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("released-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("released.absent", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("released-absent")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(_) = postgres.submit(database, "released-absent", definition, 1)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.released(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "released-absent",
      workers,
      "released-absent-owner",
      30_000,
    )
  let execution = attempt.execute_claim(claimed)
  attempt.acknowledge(
    database,
    "released-absent",
    "released-absent-owner",
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  attempt.release_unstarted(
    database,
    "released-absent",
    "released-absent-owner",
    claimed,
  )
  |> should.equal(Ok(False))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  let assert Ok(sentinel_handle) =
    postgres.submit(database, "released-absent", definition, 2)
  let assert Ok(Some(sentinel_claimed)) =
    attempt.claim_one(
      database,
      "released-absent",
      workers,
      "released-absent-owner",
      30_000,
    )
  let #(sentinel_id, _, _) = attempt.claim_identity(sentinel_claimed)
  sentinel_id |> should.equal(job.id_value(sentinel_handle))
  attempt.release_unstarted(
    database,
    "released-absent",
    "released-absent-owner",
    sentinel_claimed,
  )
  |> should.equal(Ok(True))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(sentinel_id)
  mark_database_test_executed("released-observation-absent-passed")
}

pub fn postgres_contract_mismatch_observation_emission_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_contract_mismatch_observation_emission_test(database_url)
  }
}

/// A stored `output_version` that no longer matches the currently registered
/// worker's codec (a deploy changed the codec without a worker/version bump)
/// releases the claim as `contract_mismatch` — the same forced mismatch
/// `run_batch_partial_error_test` uses.
fn run_contract_mismatch_observation_emission_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("contract-mismatch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("contract-mismatch-output-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define(
      "contract.mismatch.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(workers) = registry.new("contract-mismatch-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "contract-mismatch-emission", definition, 1)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("contract-mismatch-output-v2"))
    |> pog.parameter(pog.text("contract.mismatch.emission"))
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(
      observation.contract_mismatch_recorded(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: worker.OutputCodec,
        expected: "contract-mismatch-output-v2",
        actual: "contract-mismatch-output-v1",
      )),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.ContractMismatch))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements
  |> should.equal(observation.ContractMismatchMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.ref.queue |> should.equal("contract-mismatch-emission")
  metadata.ref.worker_id |> should.equal("contract.mismatch.emission")
  metadata.attempt.attempt |> should.equal(1)
  metadata.kind |> should.equal(worker.OutputCodec)
  metadata.expected_version |> should.equal("contract-mismatch-output-v2")
  metadata.actual_version |> should.equal("contract-mismatch-output-v1")
  mark_database_test_executed("contract-mismatch-observation-emission-passed")
}

pub fn postgres_contract_mismatch_observation_absent_on_matching_codec_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_contract_mismatch_observation_absent_test(database_url)
  }
}

/// An ordinary claim whose stored codec versions match the registered worker
/// never releases as `contract_mismatch` — proven by a following genuine
/// mismatch through the exact same producer arriving as the very next
/// `contract_mismatch` observation.
fn run_contract_mismatch_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("contract-mismatch-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("contract-mismatch-absent-output-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define(
      "contract.mismatch.absent",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(workers) = registry.new("contract-mismatch-absent")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(matching_handle) =
    postgres.submit(database, "contract-mismatch-absent", definition, 1)
  let assert Ok(mismatched_handle) =
    postgres.submit(database, "contract-mismatch-absent", definition, 2)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE id = $2")
    |> pog.parameter(pog.text("contract-mismatch-absent-output-v2"))
    |> pog.parameter(pog.int(job.id_value(mismatched_handle)))
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(
      observation.contract_mismatch_recorded(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, matching_handle) |> should.equal(Ok(job.Succeeded))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: worker.OutputCodec,
        expected: "contract-mismatch-absent-output-v2",
        actual: "contract-mismatch-absent-output-v1",
      )),
    ),
  )
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(mismatched_handle))
  mark_database_test_executed("contract-mismatch-observation-absent-passed")
}
