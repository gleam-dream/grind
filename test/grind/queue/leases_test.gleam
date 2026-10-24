import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{None, Some}
import gleam/result
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/concurrency.{ReleaseAttempt}
import grind/support/diagnostics
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/lease_queries.{
  await_later_lease_expiry, await_renewal_status, lease_expiration,
}
import grind/support/observers.{detach}
import grind/support/queue_signals.{FirstAttemptStarted}
import grind/support/worker_failure.{AccountMissing}
import grind/telemetry
import pog

pub fn postgres_queue_renews_running_attempt_before_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_lease_renewal_test(database_url)
  }
}

pub fn postgres_expired_renewal_keeps_worker_fenced_and_returns_proposal_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_lease_renewal_loss_test(database_url)
  }
}

pub fn postgres_renewal_storage_error_is_unknown_then_retried_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_renewal_storage_error_test(database_url)
  }
}

pub fn postgres_closed_pool_renewal_recovers_without_rerun_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_closed_pool_renewal_test(database_url)
  }
}

fn run_lease_renewal_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("renewal-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "renewal-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define("lease.renewal", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("finished-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("lease-renewal")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "lease-renewal", slow_worker, 7)
  let #(renewals, attachment) =
    diagnostics.capture(telemetry.renewal(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(4008)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  let connection = postgres.connection(database)
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let observed_renewal = case
    lease_expiration(connection, job.id_value(handle))
  {
    Ok(initial_expiry) ->
      await_later_lease_expiry(
        connection,
        job.id_value(handle),
        initial_expiry + 50,
        125,
      )
    Error(Nil) -> False
  }
  process.send(release, ReleaseAttempt)
  let assert Ok(Ok(True)) = process.receive(reply, within: 5000)
  observed_renewal |> should.equal(True)
  let assert Ok(#(measurement, metadata)) = process.receive(renewals, 5000)
  metadata.outcome |> should.equal(telemetry.Renewed)
  metadata.phase |> should.equal(telemetry.HandlerRunning)
  measurement.count |> should.equal(1)
  { measurement.duration_us > 0 } |> should.be_true()
  let assert Some(headroom) = measurement.remaining_lease_ms
  { headroom > 0 && headroom <= 4008 } |> should.be_true()
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("diagnostic-renewal-database-headroom")
  mark_database_test_executed("lease-renewal-before-ack-passed")
}

fn run_lease_renewal_loss_test(database_url: String) -> Nil {
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
      "renewal-loss-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "renewal-loss-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.renewal.loss",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("lost-lease-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("lease-renewal-loss")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "lease-renewal-loss", slow_worker, 7)
  let #(renewals, attachment) =
    diagnostics.capture(telemetry.renewal(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(4008)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  let connection = postgres.connection(database)
  force_lease_expired(connection, job.id_value(handle))
  |> should.equal(Ok(Nil))
  await_renewal_lost(consumer, 200) |> should.equal(True)
  let assert Ok(#(measurement, metadata)) =
    diagnostics.await(
      renewals,
      fn(sample) { sample.1.outcome == telemetry.LiveFenceUnavailable },
      5000,
    )
  metadata.phase |> should.equal(telemetry.HandlerRunning)
  let assert Some(headroom) = measurement.remaining_lease_ms
  { headroom < 0 } |> should.be_true()
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))

  process.send(release, ReleaseAttempt)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
    proposed,
    postgres.AckLeaseExpired(..),
  )))) = process.receive(reply, within: 5000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "renewal-loss-output-v1",
    "\"lost-lease-7\"",
  ))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_database_test_executed("lease-renewal-loss-fenced-passed")
}

fn run_renewal_storage_error_test(database_url: String) -> Nil {
  let settings =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "renewal-storage-error-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "renewal-storage-error-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.renewal.storage-error",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("reconnected-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("renewal-storage-error")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "renewal-storage-error", slow_worker, 18)
  let #(renewals, attachment) =
    diagnostics.capture(telemetry.renewal(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(4008)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  // A PostgreSQL trigger returns a real query error for lease renewal while
  // leaving the connection and coordinator alive. This exercises the storage
  // error result path without conflating it with process death.
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_reject_renewal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.state = 'executing' AND NEW.state = 'executing' AND NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at THEN RAISE EXCEPTION 'injected renewal query failure'; END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_reject_renewal BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION grind_test_reject_renewal()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_reject_renewal ON grind_jobs",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_reject_renewal()")
      |> pog.execute(on: connection)
    Nil
  })
  await_renewal_status(consumer, queue.LeaseRenewalUnknown, 300)
  |> should.equal(True)
  let assert Ok(#(failed, failure)) =
    diagnostics.await(
      renewals,
      fn(sample) { sample.1.outcome == telemetry.StorageFailed },
      5000,
    )
  failed.remaining_lease_ms |> should.equal(None)
  failure.phase |> should.equal(telemetry.HandlerRunning)
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  let assert Ok(_) =
    pog.query("DROP TRIGGER grind_test_reject_renewal ON grind_jobs")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP FUNCTION grind_test_reject_renewal()")
    |> pog.execute(on: connection)
  await_renewal_status(consumer, queue.LeaseRenewalConfirmed, 300)
  |> should.equal(True)
  let assert Ok(#(recovered, success)) =
    diagnostics.await(
      renewals,
      fn(sample) { sample.1.outcome == telemetry.Renewed },
      5000,
    )
  let assert Some(headroom) = recovered.remaining_lease_ms
  { headroom > 0 } |> should.be_true()
  success.context |> should.equal(failure.context)
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("renewal-storage-error-retried-passed")
  mark_database_test_executed("diagnostic-renewal-failure-recovery")
}

fn run_closed_pool_renewal_test(database_url: String) -> Nil {
  let settings =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "closed-pool-renewal-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "closed-pool-renewal-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.closed-pool.renewal",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("recovered-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("closed-pool-renewal")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "closed-pool-renewal", slow_worker, 19)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(4008)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  // Closing the ordinary pool must not stop the independently owned renewal
  // connection. Observe real lease progress while public storage is unavailable,
  // then reopen the same pool to acknowledge the original execution.
  let assert Ok(observer_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(observer) = postgres.start(observer_settings)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = postgres.connection(observer)
  let assert Ok(initial_expiry) =
    lease_expiration(observer_connection, job.id_value(handle))
  let _ = postgres.close(database)
  postgres.state(database, handle)
  |> should.equal(Error(postgres.JobReadQueryFailed(pog.ConnectionUnavailable)))
  await_later_lease_expiry(
    observer_connection,
    job.id_value(handle),
    initial_expiry + 50,
    125,
  )
  |> should.equal(True)
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  let assert Ok(reopened_database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened_database) })
  await_renewal_status(consumer, queue.LeaseRenewalConfirmed, 300)
  |> should.equal(True)
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(reopened_database, handle)
  |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("closed-pool-renewal-recovered-passed")
}

fn await_renewal_lost(consumer: queue.Consumer, checks_remaining: Int) -> Bool {
  case queue.renewal_status(consumer) {
    Ok(Some(queue.LeaseRenewalLost)) -> True
    _ ->
      case checks_remaining > 0 {
        False -> False
        True -> {
          process.sleep(10)
          await_renewal_lost(consumer, checks_remaining - 1)
        }
      }
  }
}

fn force_lease_expired(
  connection: pog.Connection,
  id: Int,
) -> Result(Nil, Nil) {
  pog.query(
    "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.map(fn(_) { Nil })
}
