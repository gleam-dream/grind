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
import grind/support/ack_queries.{wait_for_commit_trigger_backend}
import grind/support/concurrency.{LongHandlerStarted, ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  database_url, mark_database_test_executed, queue_database_url,
}
import grind/support/observers.{detach}
import grind/support/syncrep.{terminate_backend}
import grind/support/unique_fixture.{unique_test_suffix}
import grind/support/worker_failure.{AccountMissing}
import grind/worker
import pog
import sinal

pub fn postgres_cancellation_observation_before_run_and_requested_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancellation_observation_emission_test(database_url)
  }
}

/// `CancelledBeforeRun` (a queued job cancelled before any attempt) and
/// `CancellationRequested` (an executing job) are the only two genuine
/// writes; `CancellationRequested` can repeat verbatim for an idempotent
/// re-request (`cancel_executing`'s own `COALESCE` re-affirms rather than
/// rejects), proven here by cancelling the same executing job twice.
fn run_cancellation_observation_emission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancellation-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancellation-emission-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "cancellation.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok(int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("cancellation-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(queued_handle) =
    postgres.submit(database, "cancellation-emission", definition, 1)
  let assert Ok(executing_handle) =
    postgres.submit(database, "cancellation-emission", definition, 2)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let signal = process.new_subject()
  let attachment =
    sinal.observe(
      observation.cancellation_decided(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  postgres.cancel(database, queued_handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  let assert Ok(#(measurements, before_run_metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.CancellationMeasurements(count: 1))
  before_run_metadata.ref.job_id |> should.equal(job.id_value(queued_handle))
  before_run_metadata.ref.queue |> should.equal("cancellation-emission")
  before_run_metadata.previous_state |> should.equal(job.Queued)
  before_run_metadata.outcome
  |> should.equal(observation.CancellationDecidedBeforeRun)

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })

  postgres.cancel(database, executing_handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  let assert Ok(#(_, requested_metadata_1)) =
    process.receive(signal, within: 5000)
  requested_metadata_1.ref.job_id
  |> should.equal(job.id_value(executing_handle))
  requested_metadata_1.previous_state |> should.equal(job.Executing)
  requested_metadata_1.outcome
  |> should.equal(observation.CancellationDecidedWhileRunning)

  // Idempotent re-request: the same outcome, delivered again.
  postgres.cancel(database, executing_handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  let assert Ok(#(_, requested_metadata_2)) =
    process.receive(signal, within: 5000)
  requested_metadata_2.outcome
  |> should.equal(observation.CancellationDecidedWhileRunning)

  process.send(release, ReleaseAttempt)
  let assert Ok(_) = process.receive(reply, within: 5000)
  postgres.state(database, executing_handle) |> should.equal(Ok(job.Cancelled))
  mark_database_test_executed("cancellation-observation-emission-passed")
}

pub fn postgres_cancellation_observation_absent_on_already_cancelled_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancellation_observation_absent_test(database_url)
  }
}

/// The read-only outcomes (`AlreadyCancelled`, `AlreadyUncertain`,
/// `AlreadyFinished`) commit nothing and must never emit — proven here by
/// cancelling an already-cancelled job, then a genuine cancellation through
/// the exact same producer arriving as the very next `cancellation`
/// observation.
fn run_cancellation_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancellation-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancellation-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "cancellation.absent",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "cancellation-absent", definition, 1)
  let assert Ok(other_handle) =
    postgres.submit(database, "cancellation-absent", definition, 2)

  // Attach *before* the first (genuinely emitting) cancellation, not after:
  // `forwarder.emit` hands the event to a separate forwarder process that
  // dispatches it asynchronously, so a handler attached immediately after
  // an emitting call returns can still race that call's own not-yet-
  // delivered event and wrongly observe it as if it belonged to a later,
  // supposedly silent operation. Draining and asserting this first event
  // ourselves — rather than starting from a subject that might already
  // have it queued — removes that race entirely.
  let signal = process.new_subject()
  let attachment =
    sinal.observe(
      observation.cancellation_decided(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  let assert Ok(#(_, first_metadata)) = process.receive(signal, within: 5000)
  first_metadata.ref.job_id |> should.equal(job.id_value(handle))

  // `AlreadyCancelled` is read-only and commits nothing, so it must never
  // emit — proven the same deterministic way as every other `_absent_`
  // observation test in this file: a following genuine cancellation
  // through the exact same producer must be the very next event on
  // `signal`, not a bounded `receive(within: 0)` that cannot distinguish
  // "genuinely never emitted" from "emitted, but not yet delivered".
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.AlreadyCancelled))

  postgres.cancel(database, other_handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(other_handle))
  mark_database_test_executed("cancellation-observation-absent-passed")
}

pub fn postgres_cancellation_observation_absent_on_commit_unknown_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_cancellation_observation_commit_unknown_test(database_url)
  }
}

/// A genuinely aborted commit (a deferred trigger's `pg_sleep` fires during
/// `cancel`'s own transaction `COMMIT`; killing that backend aborts the
/// whole transaction, including its own `grind_jobs` update) reports
/// `CancellationCommitUnknown` and must never emit. Once the trigger is
/// dropped, cancelling the same still-`queued` job for real is the sentinel
/// through the same producer.
fn run_cancellation_observation_commit_unknown_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancellation-commit-unknown-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "cancellation-commit-unknown-output-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "cancellation.commit.unknown",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "cancellation-commit-unknown", definition, 1)
  let assert Ok(sentinel_handle) =
    postgres.submit(database, "cancellation-commit-unknown", definition, 2)
  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)

  let trigger_name = "grind_test_cancellation_commit_unknown_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT (NEW.id = "
      <> int.to_string(job_id)
      <> ") THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> trigger_name
      <> " AFTER UPDATE ON grind_jobs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection)
  let drop_trigger = fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
  use <- exception.defer(drop_trigger)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(
      observation.cancellation_decided(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, postgres.cancel(database, handle))
    })
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(postgres.CancellationCommitUnknown)) =
    process.receive(reply, within: 10_000)
  process.receive(signal, within: 0) |> should.equal(Error(Nil))
  drop_trigger()

  postgres.state(database, handle) |> should.equal(Ok(job.Queued))

  // Sentinel: a distinct job through the exact same producer, so a stray
  // event wrongly emitted for the commit-unknown job above (which would
  // carry *that* job's id) is caught as a mismatch here rather than
  // coincidentally matching (cancelling the same job again would emit the
  // same `CancelledBeforeRun` either way, which could not tell the
  // two apart).
  postgres.cancel(database, sentinel_handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  mark_database_test_executed(
    "cancellation-observation-absent-on-commit-unknown-passed",
  )
}
