import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/option.{None, Some}
import gleeunit/should
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/support/concurrency.{LongHandlerStarted, ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/observation_fixtures.{
  AcknowledgedSignal, attach_acknowledged_observer,
}
import grind/support/observers.{detach}
import grind/support/queue_timing.{database_time_milliseconds}
import grind/support/worker_failure.{AccountMissing}
import grind/worker

pub fn postgres_acknowledged_observation_available_at_for_committed_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_available_at_retry_test(database_url)
  }
}

/// `available_at_unix_ms` for a genuinely committed `retryable` outcome:
/// `Some` and within the default backoff's bounds, read from the commit
/// (`RETURNING`), not re-derived from the proposal.
fn run_acknowledged_observation_available_at_retry_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-available-at-retry-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-available-at-retry-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "observation.available-at.retry",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Error(AccountMissing(value)) },
    )
  let assert Ok(workers) = registry.new("observation-available-at-retry")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "observation-available-at-retry", definition, 1)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let connection = postgres.connection(database)
  let before_ack_ms = database_time_milliseconds(connection)
  queue.process_one(consumer) |> should.equal(Ok(True))
  let after_ack_ms = database_time_milliseconds(connection)
  postgres.state(database, handle) |> should.equal(Ok(job.Retryable))

  let assert Ok(AcknowledgedSignal(_measurements, metadata)) =
    process.receive(signal, within: 5000)
  metadata.proposed |> should.equal(observation.ProposedRetryable)
  metadata.committed_state |> should.equal(job.Retryable)
  let assert Some(available_at_ms) = metadata.available_at_unix_ms
  should.be_true(available_at_ms >= before_ack_ms + 15_000)
  should.be_true(available_at_ms <= after_ack_ms + 15_000)
  mark_database_test_executed(
    "acknowledged-observation-available-at-committed-retry-passed",
  )
}

pub fn postgres_acknowledged_observation_available_at_for_committed_snooze_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_available_at_snooze_test(database_url)
  }
}

/// `available_at_unix_ms` for a genuinely committed snooze (`scheduled`)
/// outcome: `Some` and within the requested delay's bounds.
fn run_acknowledged_observation_available_at_snooze_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-available-at-snooze-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-available-at-snooze-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(delay) = worker.retry_delay(60_000)
  let assert Ok(ordinary) =
    worker.define(
      "observation.available-at.snooze",
      "v1",
      input_codec,
      output_codec,
      fn(_) { Error(AccountMissing(1)) },
    )
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerSnoozed(delay, "awaiting external account")
    })
  let assert Ok(workers) = registry.new("observation-available-at-snooze")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) =
    postgres.submit(database, "observation-available-at-snooze", snoozing, 1)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let connection = postgres.connection(database)
  let before_ack_ms = database_time_milliseconds(connection)
  queue.process_one(consumer) |> should.equal(Ok(True))
  let after_ack_ms = database_time_milliseconds(connection)
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  let assert Ok(AcknowledgedSignal(_measurements, metadata)) =
    process.receive(signal, within: 5000)
  metadata.proposed |> should.equal(observation.ProposedSnoozed)
  metadata.committed_state |> should.equal(job.Scheduled)
  let assert Some(available_at_ms) = metadata.available_at_unix_ms
  should.be_true(available_at_ms >= before_ack_ms + 60_000)
  should.be_true(available_at_ms <= after_ack_ms + 60_000)
  mark_database_test_executed(
    "acknowledged-observation-available-at-committed-snooze-passed",
  )
}

pub fn postgres_acknowledged_observation_available_at_none_when_cancel_overrides_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_available_at_cancel_overrides_retry_test(
        database_url,
      )
  }
}

/// `available_at_unix_ms` must come from the *committed* state, not the
/// proposed one: a proposed retry (`ProposedRetryable`) overridden by a
/// concurrent cancellation commits `cancelled`, whose row's `available_at`
/// is left at its unrelated pre-ack value — this must never be surfaced as
/// `Some`. Named mutation: gating on `proposed_state` instead of
/// `committed_state` (the exact bug this test was written to catch) makes
/// this test fail — see `docs/RECOVERY-EVIDENCE.md`, "Acknowledged
/// observation".
fn run_acknowledged_observation_available_at_cancel_overrides_retry_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-available-at-cancel-retry-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-available-at-cancel-retry-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "observation.available-at.cancel-retry",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Error(AccountMissing(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-available-at-cancel-retry")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(
      database,
      "observation-available-at-cancel-retry",
      definition,
      9,
    )
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))

  let assert Ok(AcknowledgedSignal(_measurements, metadata)) =
    process.receive(signal, within: 5000)
  metadata.proposed |> should.equal(observation.ProposedRetryable)
  metadata.committed_state |> should.equal(job.Cancelled)
  metadata.available_at_unix_ms |> should.equal(None)
  mark_database_test_executed(
    "acknowledged-observation-available-at-none-cancel-overrides-retry-passed",
  )
}
