import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{None}
import gleeunit/should
import grind/internal/attempt
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/job as public_job
import grind/support/ack_queries.{
  stored_attempt_identity, wait_for_commit_trigger_backend,
}
import grind/support/concurrency.{LongHandlerStarted, ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/diagnostics
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/lease_queries.{await_renewal_status}
import grind/support/observation_fixtures.{
  AcknowledgedSignal, assert_next_observation_is_sentinel,
  attach_acknowledged_observer, register_sentinel_worker,
}
import grind/support/observers.{detach}
import grind/support/queue_signals.{
  CapacityWorkerStarted, FirstAttemptStarted, WorkerInvoked,
}
import grind/support/syncrep.{
  install_syncrep_reply_trigger, require_syncrep_cluster_configured,
  terminate_backend, wait_for_syncrep_trigger_backend,
}
import grind/support/worker_failure.{AccountMissing}
import grind/telemetry
import pog

/// Carries a release gate created *inside* a blocked handler (so it is owned
/// by the forwarder process, which is the one that will `process.receive`
/// it) back out to the test process, which only ever `process.send`s to it.
type IsolationGateEntered {
  IsolationGateEntered(process.Subject(Nil))
}

/// Pure: `InvalidObservationCapacity` is rejected before any process starts.
pub fn postgres_settings_reject_non_positive_observation_capacity_test() {
  postgres.settings("postgres://ignored/ignored")
  |> postgres.with_observation_capacity(0)
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidObservationCapacity))
  postgres.settings("postgres://ignored/ignored")
  |> postgres.with_observation_capacity(-1)
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidObservationCapacity))
}

pub fn postgres_acknowledged_observation_commit_ordering_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_acknowledged_commit_ordering_test(database_url)
  }
}

/// Commit ordering: reading `postgres.state` *from inside* the attached
/// handler (which runs in the forwarder process) already observes the
/// committed state — proof the observation is emitted strictly after the
/// commit, not before it. A handler that read `Executing` here would mean
/// the emit ran ahead of (or racing) the transaction, not after it.
fn run_acknowledged_commit_ordering_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-ordering-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-ordering-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "telemetry.ordering",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("ordering-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("observation-ordering")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "observation-ordering", definition, 5)
  let job_id = job.id_value(handle)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(fn(measurements, metadata) {
      let observed_state = postgres.state(database, handle)
      process.send(signal, #(
        observed_state,
        AcknowledgedSignal(measurements, metadata),
      ))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  let connection = postgres.connection(database)
  let assert Ok(#(attempt_id, epoch)) =
    stored_attempt_identity(connection, job_id)
  let expected_command_id =
    attempt.acknowledgement_command_id(job_id, attempt_id, epoch)

  let assert Ok(#(observed_state, AcknowledgedSignal(measurements, metadata))) =
    process.receive(signal, within: 5000)
  observed_state |> should.equal(Ok(job.Succeeded))
  measurements.count |> should.equal(1)
  metadata.ref.job_id |> should.equal(job_id)
  metadata.ref.queue |> should.equal("observation-ordering")
  metadata.ref.worker_id |> should.equal("telemetry.ordering")
  metadata.ref.worker_version |> should.equal("v1")
  metadata.attempt.attempt_id |> should.equal(attempt_id)
  metadata.attempt.epoch |> should.equal(epoch)
  metadata.attempt.attempt |> should.equal(1)
  metadata.proposed |> should.equal(telemetry.ProposedSuccess)
  metadata.committed_state |> should.equal(public_job.Succeeded)
  metadata.failure_cause |> should.equal(None)
  metadata.available_at_unix_ms |> should.equal(None)
  metadata.confirmation |> should.equal(telemetry.Replied)
  metadata.command_id |> should.equal(expected_command_id)
  process.receive(signal, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("acknowledged-observation-commit-ordering-passed")
}

pub fn postgres_acknowledged_observation_isolation_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_isolation_test(database_url)
  }
}

/// Isolation: job A's acknowledgement observation is gate-blocked in the
/// forwarder process while job B is still executing under the same
/// coordinator. B's lease keeps renewing and B completes normally while A's
/// gate stays shut — proof the coordinator was never blocked by A's
/// observation. Named mutation: replacing `grind/postgres`'s
/// `forwarder.emit` call with a direct `sinal.emit` call makes this test
/// hang (the coordinator itself would run the blocked handler, starving B's
/// renewal), which is exactly the regression this test exists to catch.
fn run_acknowledged_observation_isolation_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-isolation-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-isolation-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "telemetry.isolation",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, CapacityWorkerStarted(value, release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("isolated-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-isolation")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle_a) =
    postgres.submit(database, "observation-isolation", definition, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "observation-isolation", definition, 2)
  let job_a_id = job.id_value(handle_a)

  // The release gate must be created *inside* the handler (owned by the
  // forwarder process that will `process.receive` it) and handed back to
  // the test over `gate_entered`; a `process.Subject` created in the test
  // process cannot be received on from a different process.
  let gate_entered = process.new_subject()
  let attachment =
    attach_acknowledged_observer(fn(_measurements, metadata) {
      case metadata.ref.job_id == job_a_id {
        True -> {
          let gate = process.new_subject()
          process.send(gate_entered, IsolationGateEntered(gate))
          let assert Ok(Nil) = process.receive(gate, within: 10_000)
          Nil
        }
        False -> Nil
      }
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(2)
    |> queue.with_lease_duration(6100)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let reply_a = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply_a, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, release_a)) =
    process.receive(started, within: 5000)

  let reply_b = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply_b, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, release_b)) =
    process.receive(started, within: 5000)

  process.send(release_a, ReleaseAttempt)
  process.receive(reply_a, within: 5000) |> should.equal(Ok(Ok(True)))
  // A's own acknowledged observation is now gate-blocked in the forwarder.
  let assert Ok(IsolationGateEntered(gate)) =
    process.receive(gate_entered, within: 5000)

  // While it stays blocked, B's lease keeps renewing and B completes.
  await_renewal_status(consumer, queue.LeaseRenewalConfirmed, 200)
  |> should.equal(True)
  process.send(release_b, ReleaseAttempt)
  process.receive(reply_b, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle_b) |> should.equal(Ok(job.Succeeded))

  process.send(gate, Nil)
  mark_database_test_executed("acknowledged-observation-isolation-passed")
}

pub fn postgres_acknowledged_observation_absent_on_commit_unknown_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_absent_on_commit_unknown_test(database_url)
  }
}

/// Negative (abort/unknown, sentinel pattern): the ack's COMMIT is aborted by
/// killing its backend mid-trigger (the same technique as
/// `postgres_ack_commit_connection_loss_is_unknown_test`), so nothing is
/// durably committed and `acknowledge_claim` reports `QueueAckUnknown`. No
/// `[grind, job, acknowledged]` observation is emitted for either "abort" or
/// "unknown" here, because in this codebase they are the exact same code
/// path: a connection lost during `COMMIT` is unconditionally reported
/// `QueueAckUnknown`, whether or not the transaction actually reached
/// commit. Named mutation: emitting on this `Error(QueueAckUnknown(..))`
/// result (instead of only ever emitting from `resolve_ack_transaction_result`'s
/// proven-commit branches) makes this test fail.
fn run_acknowledged_observation_absent_on_commit_unknown_test(
  database_url: String,
) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-commit-unknown-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-commit-unknown-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "telemetry.commit.unknown",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("commit-unknown-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-commit-unknown")
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "commit-unknown")
  let assert Ok(handle) =
    postgres.submit(database, "observation-commit-unknown", definition, 21)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_kill_observation_ack_backend() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_observation_ack_backend AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_observation_ack_backend()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_observation_ack_backend ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query(
        "DROP FUNCTION IF EXISTS grind_test_kill_observation_ack_backend()",
      )
      |> pog.execute(on: connection)
    Nil
  })

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 100)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckUnknown(_, _)))) =
    process.receive(reply, within: 10_000)
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  // Deterministic absence check: a sentinel job's own commit/observation
  // through the same consumer must be the very next event on `signal`.
  let assert Ok(sentinel) =
    postgres.submit(database, "observation-commit-unknown", sentinel_worker, 22)
  queue.process_one(consumer) |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, job.id_value(sentinel))

  mark_database_test_executed(
    "acknowledged-observation-absent-on-commit-unknown-passed",
  )
}

pub fn postgres_acknowledged_observation_absent_on_stale_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_absent_on_stale_ack_test(database_url)
  }
}

/// Negative (rollback/stale, sentinel pattern): a forced lease expiry makes
/// the fenced `UPDATE` inside `acknowledge_transaction` affect zero rows,
/// which (finding no matching receipt either) is reported as
/// `QueueAckStale`. Any `Error(..)` returned from inside that transaction
/// callback rolls the whole ack transaction back — "stale" and "rollback"
/// are the same mechanism here, not two independent ones. No observation is
/// emitted. Named mutation: moving the emit call to run unconditionally
/// after `acknowledge_transaction` returns (instead of only after
/// `resolve_ack_transaction_result` reports a proven commit) makes this
/// test fail.
fn run_acknowledged_observation_absent_on_stale_ack_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-stale-ack-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-stale-ack-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "telemetry.stale.ack",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("stale-ack-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-stale-ack")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "stale-ack")
  let assert Ok(handle) =
    postgres.submit(database, "observation-stale-ack", slow_worker, 9)
  let job_id = job.id_value(handle)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(30_000)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let connection = postgres.connection(database)
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  process.send(release, ReleaseAttempt)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(_, _)))) =
    process.receive(reply, within: 5000)
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  let assert Ok(sentinel) =
    postgres.submit(database, "observation-stale-ack", sentinel_worker, 10)
  queue.process_one(consumer) |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, job.id_value(sentinel))

  mark_database_test_executed(
    "acknowledged-observation-absent-on-stale-ack-passed",
  )
}

pub fn postgres_acknowledged_observation_reconciled_after_lost_reply_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_reconciled_after_lost_reply_test(
        database_url,
      )
  }
}

/// Lost reply (SyncRep harness, same technique as
/// `postgres_ack_committed_reply_lost_reconciles_from_receipt_test`): the
/// ack genuinely commits, but this call's own connection is severed while
/// its `COMMIT` is parked in `SyncRep`, so `acknowledge` only learns the
/// outcome via `reconcile_unknown_ack` reading the receipt back. Exactly one
/// `[grind, job, acknowledged]` observation is emitted, and its
/// `confirmation` is `Reconciled`, never `Replied`. Named mutation:
/// hard-coding `Replied` for every `AckCommit` (ignoring
/// `via_receipt_match`) makes this test fail.
fn run_acknowledged_observation_reconciled_after_lost_reply_test(
  database_url: String,
) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-reply-lost-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-reply-lost-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "telemetry.reply.lost",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("reply-lost-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-reply-lost")
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "reply-lost")
  let assert Ok(handle) =
    postgres.submit(database, "observation-reply-lost", definition, 33)
  let #(ack_diagnostics, diagnostic_attachment) =
    diagnostics.capture(telemetry.acknowledgement(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(diagnostic_attachment) })
  let #(checkouts, checkout_attachment) =
    diagnostics.capture(telemetry.checkout(), fn(meta) {
      meta.queue.queue == "observation-reply-lost"
      && meta.operation == telemetry.ReconcileAcknowledgement
    })
  use <- exception.defer(fn() { detach(checkout_attachment) })
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let job_id = job.id_value(handle)
  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_syncrep_observation_reply_lost",
    "grind_job_acknowledgements",
    "NEW.job_id = " <> int.to_string(job_id),
  ))

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  process.receive(reply, within: 10_000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  let assert Ok(#(attempt_id, epoch)) =
    stored_attempt_identity(connection, job_id)
  let expected_command_id =
    attempt.acknowledgement_command_id(job_id, attempt_id, epoch)

  let assert Ok(AcknowledgedSignal(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements.count |> should.equal(1)
  metadata.ref.job_id |> should.equal(job_id)
  metadata.committed_state |> should.equal(public_job.Succeeded)
  metadata.confirmation |> should.equal(telemetry.Reconciled)
  metadata.command_id |> should.equal(expected_command_id)

  let assert Ok(#(ack_timing, ack_diagnostic)) =
    process.receive(ack_diagnostics, 5000)
  ack_diagnostic.outcome |> should.equal(telemetry.AckReconciled)
  ack_diagnostic.context.ref |> should.equal(metadata.ref)
  ack_diagnostic.context.attempt |> should.equal(metadata.attempt)
  ack_diagnostic.command_id |> should.equal(expected_command_id)
  { ack_timing.duration_us > 0 } |> should.be_true()
  let assert Ok(#(checkout_timing, checkout)) = process.receive(checkouts, 5000)
  checkout.pool |> should.equal(telemetry.MainPool)
  checkout.checkout |> should.equal(telemetry.CheckoutAcquired)
  checkout.returned |> should.equal(telemetry.CallSucceeded)
  { checkout_timing.candidates >= 1 } |> should.be_true()
  { checkout_timing.call_duration_us >= checkout_timing.wait_us }
  |> should.be_true()
  mark_database_test_executed("diagnostic-ack-reconciled-after-lost-reply")

  // Exactly one observation for this command — no duplicate `Replied` also
  // arrived from the same lost-reply commit. Checked deterministically: a
  // sentinel job's own observation, run through the same consumer, must be
  // the very next event.
  let assert Ok(sentinel) =
    postgres.submit(database, "observation-reply-lost", sentinel_worker, 34)
  queue.process_one(consumer) |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, job.id_value(sentinel))

  mark_database_test_executed(
    "acknowledged-observation-reconciled-after-lost-reply-passed",
  )
}

pub fn postgres_acknowledged_observation_committed_state_overrides_proposal_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_committed_state_overrides_proposal_test(
        database_url,
      )
  }
}

/// Proposed vs. committed (same cancel-while-running technique as
/// `postgres_cancel_running_worker_overrides_proposal_on_ack_test`): the
/// worker proposes success, but a concurrent cancellation overrides it, and
/// the durable commit is `Cancelled`. The observation's `proposed` and
/// `committed_state` fields diverge accordingly and `committed_state` comes
/// from the commit, never from the proposal. Named mutation: building
/// `committed_state` from the worker's proposed disposition instead of
/// `AckCommit`'s own committed value makes this test fail.
fn run_acknowledged_observation_committed_state_overrides_proposal_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-cancel-running-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-cancel-running-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "telemetry.cancel.running",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) ->
            Ok("completed-despite-cancel-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-cancel-running")
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "cancel-running")
  let assert Ok(handle) =
    postgres.submit(database, "observation-cancel-running", definition, 9)
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
  metadata.proposed |> should.equal(telemetry.ProposedSuccess)
  metadata.committed_state |> should.equal(public_job.Cancelled)
  metadata.confirmation |> should.equal(telemetry.Replied)

  let assert Ok(sentinel) =
    postgres.submit(database, "observation-cancel-running", sentinel_worker, 10)
  queue.process_one(consumer) |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, job.id_value(sentinel))

  mark_database_test_executed(
    "acknowledged-observation-committed-state-overrides-proposal-passed",
  )
}
