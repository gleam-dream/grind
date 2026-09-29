import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import grind/diagnostic
import grind/internal/attempt
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/support/ack_queries.{count_acknowledgements_for_job}
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt, spawn_lock_holder,
}
import grind/support/diagnostics
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/job_state.{wait_for_succeeded}
import grind/support/observers.{detach}
import grind/worker
import pog

pub fn postgres_diagnostic_locked_renewal_recovers_after_row_unlock_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> locked_renewal(url)
  }
}

fn locked_renewal(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.with_pool_size(10)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  // The raw lock holder must outlive the consumer's first 2000 ms renewal tick.
  // Its default deadline gives it an 8000 ms idle-transaction timeout, without
  // changing the consumer's 1002 ms deadline or 6000 ms lease.
  let assert Ok(lock_settings) = postgres.settings(url) |> postgres.validate
  let assert Ok(lock_database) = postgres.start(lock_settings)
  use <- exception.defer(fn() { postgres.close(lock_database) })
  let assert Ok(codec) =
    worker.codec("diagnostic-lock-int", json.int, decode.int)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("diagnostic.lock", "v1", codec, codec, fn(value) {
      let release = process.new_subject()
      process.send(invoked, Nil)
      process.send(started, release)
      let assert Ok(Nil) = process.receive(release, 20_000)
      Ok(value)
    })
  let assert Ok(workers) = registry.new("diagnostic-lock")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "diagnostic-lock", definition, 9)
  let #(renewals, attachment) =
    diagnostics.capture("locked-renewal", diagnostic.renewal(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_manual_polling
    |> queue.with_lease_duration(6000)
    |> queue.with_shutdown_grace(1000)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let completed = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(completed, queue.process_one(consumer))
    })
  let assert Ok(release_handler) = process.receive(started, 5000)
  use <- exception.defer(fn() { process.send(release_handler, Nil) })
  process.receive(invoked, 1000) |> should.equal(Ok(Nil))

  // This raw transaction holds the exact executing row until a renewal has
  // positively reported skipping it. No sleep or renewal-tick phase is assumed.
  let acquire =
    pog.query("SELECT id FROM grind_jobs WHERE id = $1 FOR NO KEY UPDATE")
    |> pog.parameter(pog.int(job.id_value(handle)))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(postgres.connection(lock_database), acquire)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    // Consume the transaction's actual result exactly once, even when an
    // assertion above or below fails. A released lock alone is not a commit reply.
    process.receive(lock_finished, 5000)
    |> should.equal(Ok(ClaimGateReleased(True)))
  })
  let assert Ok(#(locked, lock_metadata)) =
    diagnostics.await(
      renewals,
      fn(sample) { sample.1.outcome == diagnostic.SkippedLocked },
      5000,
    )
  lock_metadata.phase |> should.equal(diagnostic.HandlerRunning)
  lock_metadata.context.attempt.attempt |> should.equal(1)
  locked.count |> should.equal(1)
  { locked.duration_us > 0 } |> should.be_true()
  let assert Some(headroom) = locked.remaining_lease_ms
  { headroom > 0 } |> should.be_true()
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  process.send(release_lock, ReleaseAttempt)
  let assert Ok(#(renewed, renewal_metadata)) =
    diagnostics.await(
      renewals,
      fn(sample) { sample.1.outcome == diagnostic.Renewed },
      5000,
    )
  renewal_metadata.context |> should.equal(lock_metadata.context)
  renewal_metadata.phase |> should.equal(diagnostic.HandlerRunning)
  let assert Some(renewed_headroom) = renewed.remaining_lease_ms
  { renewed_headroom > 0 && renewed_headroom <= 6000 } |> should.be_true()
  process.send(release_handler, Nil)
  process.receive(completed, 5000) |> should.equal(Ok(Ok(True)))
  postgres.outcome(database, handle) |> should.equal(Ok(job.SucceededWith(9)))
  process.receive(invoked, 0) |> should.equal(Error(Nil))
  mark_database_test_executed("diagnostic-renewal-skipped-lock")
}

pub fn postgres_diagnostic_claim_failures_identify_storage_stage_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> claim_failures(url)
  }
}

fn claim_failures(url: String) -> Nil {
  let assert Ok(settings) = postgres.settings(url) |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(codec) =
    worker.codec("diagnostic-claim-int", json.int, decode.int)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("diagnostic.claim", "v1", codec, codec, fn(value) {
      process.send(invoked, value)
      Ok(value)
    })
  let assert Ok(workers) = registry.new("diagnostic-claim")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "diagnostic-claim", definition, 7)
  let id = job.id_value(handle)
  let #(failed, failed_attachment) =
    diagnostics.capture("claim-stage", diagnostic.claim_failed(), fn(meta) {
      meta.queue.queue == "diagnostic-claim"
    })
  use <- exception.defer(fn() { detach(failed_attachment) })
  let #(checkouts, checkout_attachment) =
    diagnostics.capture("claim-stage-checkout", diagnostic.checkout(), fn(meta) {
      meta.queue.queue == "diagnostic-claim"
    })
  use <- exception.defer(fn() { detach(checkout_attachment) })
  let assert Ok(policy) =
    queue.default_policy() |> queue.with_manual_polling |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  execute(
    connection,
    "CREATE FUNCTION diagnostic_reject_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(id)
      <> " AND NEW.state = 'executing' THEN RAISE EXCEPTION 'diagnostic claim fixture' USING ERRCODE = 'P0001'; END IF; RETURN NEW; END $$",
  )
  execute(
    connection,
    "CREATE TRIGGER diagnostic_reject_claim BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION diagnostic_reject_claim()",
  )
  use <- exception.defer(fn() {
    execute(
      connection,
      "DROP TRIGGER IF EXISTS diagnostic_reject_claim ON grind_jobs",
    )
    execute(connection, "DROP FUNCTION IF EXISTS diagnostic_reject_claim()")
  })
  let assert Error(queue.QueueProcessFailed(postgres.QueueClaimFailed(_))) =
    queue.process_one(consumer)
  let assert Ok(#(claim_measurements, claim_metadata)) =
    process.receive(failed, 5000)
  claim_metadata.stage |> should.equal(diagnostic.ClaimCandidate)
  claim_metadata.failure |> should.equal(diagnostic.Rejected)
  claim_measurements.count |> should.equal(1)
  { claim_measurements.duration_us > 0 } |> should.be_true()
  claim_metadata.queue.consumer.node |> should.not_equal("")
  claim_metadata.queue.consumer.consumer |> should.not_equal("")
  let assert Ok(#(_, claim_checkout)) =
    diagnostics.await(
      checkouts,
      fn(sample) {
        sample.1.operation == diagnostic.ClaimCandidate
        && sample.1.returned == diagnostic.CallFailed
      },
      5000,
    )
  claim_checkout.queue |> should.equal(claim_metadata.queue)
  claim_checkout.pool |> should.equal(diagnostic.MainPool)
  claim_checkout.checkout |> should.equal(diagnostic.CheckoutAcquired)
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  process.receive(invoked, 0) |> should.equal(Error(Nil))
  execute(connection, "DROP TRIGGER diagnostic_reject_claim ON grind_jobs")
  execute(connection, "DROP FUNCTION diagnostic_reject_claim()")

  // Only fixture setup claims directly: it never executes the handler. The
  // public consumer below must report the failing quarantine stage first.
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "diagnostic-claim",
      workers,
      "diagnostic-fixture",
      30_000,
    )
  let #(claimed_id, _, _) = attempt.claim_identity(claimed)
  claimed_id |> should.equal(id)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  execute(
    connection,
    "CREATE FUNCTION diagnostic_reject_quarantine() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(id)
      <> " AND NEW.state = 'uncertain' THEN RAISE EXCEPTION 'diagnostic quarantine fixture' USING ERRCODE = 'P0001'; END IF; RETURN NEW; END $$",
  )
  execute(
    connection,
    "CREATE TRIGGER diagnostic_reject_quarantine BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION diagnostic_reject_quarantine()",
  )
  use <- exception.defer(fn() {
    execute(
      connection,
      "DROP TRIGGER IF EXISTS diagnostic_reject_quarantine ON grind_jobs",
    )
    execute(
      connection,
      "DROP FUNCTION IF EXISTS diagnostic_reject_quarantine()",
    )
  })
  let assert Error(queue.QueueProcessFailed(postgres.QueueClaimFailed(_))) =
    queue.process_one(consumer)
  let assert Ok(#(quarantine_measurements, quarantine_metadata)) =
    process.receive(failed, 5000)
  quarantine_metadata.stage |> should.equal(diagnostic.QuarantineScan)
  quarantine_metadata.failure |> should.equal(diagnostic.Rejected)
  quarantine_metadata.queue |> should.equal(claim_metadata.queue)
  quarantine_measurements.count |> should.equal(1)
  { quarantine_measurements.duration_us > 0 } |> should.be_true()
  let assert Ok(#(_, quarantine_checkout)) =
    diagnostics.await(
      checkouts,
      fn(sample) {
        sample.1.operation == diagnostic.QuarantineScan
        && sample.1.returned == diagnostic.CallFailed
      },
      5000,
    )
  quarantine_checkout.queue |> should.equal(quarantine_metadata.queue)
  quarantine_checkout.pool |> should.equal(diagnostic.MainPool)
  quarantine_checkout.checkout |> should.equal(diagnostic.CheckoutAcquired)
  // ClaimFailedMetadata contains a QueueRef, never an invented job/attempt.
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  process.receive(invoked, 0) |> should.equal(Error(Nil))
  execute(connection, "DROP TRIGGER diagnostic_reject_quarantine ON grind_jobs")
  execute(connection, "DROP FUNCTION diagnostic_reject_quarantine()")
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invoked, 0) |> should.equal(Error(Nil))
  mark_database_test_executed("diagnostic-claim-stage-failures")
}

pub fn postgres_diagnostic_completion_budget_reports_once_before_quarantine_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> completion_budget(url)
  }
}

fn completion_budget(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(codec) =
    worker.codec("diagnostic-budget-int", json.int, decode.int)
  let invoked = process.new_subject()
  let held = process.new_subject()
  let assert Ok(definition) =
    worker.define("diagnostic.budget", "v1", codec, codec, fn(value) {
      case value {
        1 -> {
          process.send(invoked, Nil)
          Ok(value)
        }
        _ -> {
          let release = process.new_subject()
          process.send(held, release)
          let assert Ok(Nil) = process.receive(release, 30_000)
          Ok(value)
        }
      }
    })
  let assert Ok(workers) = registry.new("diagnostic-budget")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first) =
    postgres.submit(database, "diagnostic-budget", definition, 1)
  let assert Ok(second) =
    postgres.submit(database, "diagnostic-budget", definition, 2)
  let first_id = job.id_value(first)
  let second_id = job.id_value(second)
  // Every ACK write rolls back, but renewal updates remain available. Unlike
  // a slow-commit fixture this cannot consume the whole lease inside one call.
  execute(
    connection,
    "CREATE FUNCTION diagnostic_rollback_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(first_id)
      <> " AND NEW.state = 'succeeded' THEN RAISE EXCEPTION 'diagnostic budget fixture' USING ERRCODE = '40001'; END IF; RETURN NEW; END $$",
  )
  execute(
    connection,
    "CREATE TRIGGER diagnostic_rollback_ack BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION diagnostic_rollback_ack()",
  )
  use <- exception.defer(fn() {
    execute(
      connection,
      "DROP TRIGGER IF EXISTS diagnostic_rollback_ack ON grind_jobs",
    )
    execute(connection, "DROP FUNCTION IF EXISTS diagnostic_rollback_ack()")
  })
  // One subject preserves the single renewer's producer order across the
  // exhausted attempt and the sibling that remains eligible for renewal.
  let #(renewals, renewal_attachment) =
    diagnostics.capture(
      "completion-budget-renewals",
      diagnostic.renewal(),
      fn(meta) { meta.context.ref.queue == "diagnostic-budget" },
    )
  use <- exception.defer(fn() { detach(renewal_attachment) })
  let #(quarantines, quarantine_attachment) =
    diagnostics.capture(
      "completion-budget-quarantine",
      observation.quarantined(),
      fn(meta) { meta.ref.job_id == first_id },
    )
  use <- exception.defer(fn() { detach(quarantine_attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(2)
    |> queue.with_poll_interval(100)
    |> queue.with_lease_duration(6000)
    |> queue.with_shutdown_grace(1000)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let assert Ok(release) = process.receive(held, 5000)
  use <- exception.defer(fn() { process.send(release, Nil) })
  process.receive(invoked, 5000) |> should.equal(Ok(Nil))
  let assert Ok(#(budget_measurements, budget_metadata)) =
    diagnostics.await(
      renewals,
      fn(sample) {
        sample.1.context.ref.job_id == first_id
        && sample.1.outcome == diagnostic.CompletionBudgetExhausted
      },
      12_000,
    )
  budget_metadata.phase |> should.equal(diagnostic.AcknowledgementPending)
  budget_measurements.count |> should.equal(1)
  budget_measurements.duration_us |> should.equal(0)
  budget_measurements.remaining_lease_ms |> should.equal(None)
  // Local budget exhaustion itself neither expires the lease nor quarantines
  // the row. Its last successful renewal is still live at this barrier.
  postgres.state(database, first) |> should.equal(Ok(job.Executing))
  count_acknowledgements_for_job(connection, first_id) |> should.equal(0)
  list.each([1, 2], fn(_) {
    let assert Ok(#(measurements, metadata)) =
      diagnostics.await(
        renewals,
        fn(sample) {
          {
            sample.1.context.ref.job_id == first_id
            && sample.1.outcome == diagnostic.CompletionBudgetExhausted
          }
          |> should.be_false()
          sample.1.context.ref.job_id == second_id
          && sample.1.outcome == diagnostic.Renewed
        },
        5000,
      )
    metadata.phase |> should.equal(diagnostic.HandlerRunning)
    metadata.context.consumer |> should.equal(budget_metadata.context.consumer)
    let assert Some(remaining) = measurements.remaining_lease_ms
    { remaining > 0 } |> should.be_true()
  })
  // Keep rejecting ACKs. The independent expiry/quarantine path must eventually
  // produce its own committed lifecycle event; there is no conditional success
  // allowance and no fixture write extending the first attempt's lease.
  let assert Ok(#(_, quarantined)) = process.receive(quarantines, 12_000)
  quarantined.attempt |> should.equal(budget_metadata.context.attempt)
  postgres.state(database, first) |> should.equal(Ok(job.Uncertain))
  count_acknowledgements_for_job(connection, first_id) |> should.equal(0)
  process.receive(invoked, 0) |> should.equal(Error(Nil))
  process.send(release, Nil)
  wait_for_succeeded(database, second, 200) |> should.be_true()
  postgres.outcome(database, second) |> should.equal(Ok(job.SucceededWith(2)))
  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  // Shutdown is now a causal end to this consumer's work, not a fixed wait
  // used to infer that the quarantined handler was never automatically replayed.
  process.receive(invoked, 0) |> should.equal(Error(Nil))
  mark_database_test_executed("diagnostic-completion-budget-once")
}

fn execute(connection: pog.Connection, sql: String) -> Nil {
  let assert Ok(_) = pog.query(sql) |> pog.execute(on: connection)
  Nil
}
