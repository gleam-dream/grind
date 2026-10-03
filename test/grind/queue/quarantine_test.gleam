import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleeunit/should
import grind/internal/attempt
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/lease
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/concurrency.{ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/job_state.{attempt_snapshot}
import grind/support/queue_signals.{
  FirstAttemptStarted, TakeoverAttemptStarted, WorkerInvoked,
}
import grind/support/queue_timing.{settle_attempt}
import grind/support/worker_failure.{AccountMissing}
import pog

pub fn postgres_expired_attempt_requires_audited_replay_and_stale_ack_is_fenced_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_takeover_fencing_test(database_url)
  }
}

fn run_takeover_fencing_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("takeover-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "takeover-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let signals = process.new_subject()
  let assert Ok(first_worker) =
    worker.define("takeover.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, FirstAttemptStarted(release))
      case process.receive(release, within: 15_000) {
        Ok(ReleaseAttempt) -> Ok("obsolete-" <> int.to_string(value))
        Error(Nil) -> Error(Nil)
      }
    })
  let assert Ok(replay_worker) =
    worker.define("takeover.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, TakeoverAttemptStarted(release))
      case process.receive(release, within: 15_000) {
        Ok(ReleaseAttempt) -> Ok("current-" <> int.to_string(value))
        Error(Nil) -> Error(Nil)
      }
    })
  let assert Ok(first_registry) = registry.new("takeover-fence")
  let assert Ok(first_registry) =
    registry.register(first_registry, first_worker)
  let assert Ok(replay_registry) = registry.new("takeover-fence")
  let assert Ok(replay_registry) =
    registry.register(replay_registry, replay_worker)
  let assert Ok(first_consumer) =
    queue.start(database, first_registry, manual_policy())
  use <- exception.defer(fn() { queue.stop(first_consumer) })
  let assert Ok(replay_consumer) =
    queue.start(database, replay_registry, manual_policy())
  use <- exception.defer(fn() { queue.stop(replay_consumer) })
  let assert Ok(handle) =
    postgres.submit(database, "takeover-fence", first_worker, 7)
  let first_reply = process.new_subject()
  let first_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(first_reply, queue.process_one(first_consumer))
    })
  let assert Ok(FirstAttemptStarted(first_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(first_finished, first_release, first_reply)
  })

  let connection = postgres.connection(database)
  let assert Ok(first_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use attempt_count <- decode.field(3, decode.int)
      use delivery_count <- decode.field(4, decode.int)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        attempt_count,
        delivery_count,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#(first_attempt_id, first_epoch, first_owner, 1, 1)] =
    first_claim.rows
  first_epoch |> should.equal(1)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)

  replay_consumer
  |> queue.process_one
  |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  process.receive(signals, within: 0) |> should.equal(Error(Nil))

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "takeover-fence-authorized-replay",
      "on-call",
      "inspect the external effect before authorizing a new delivery",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(authorized_accounting) =
    pog.query(
      "SELECT attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use delivery_count <- decode.field(1, decode.int)
      decode.success(#(attempt_count, delivery_count))
    })
    |> pog.execute(on: connection)
  let assert [#(1, 1)] = authorized_accounting.rows

  let replay_reply = process.new_subject()
  let replay_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(replay_reply, queue.process_one(replay_consumer))
    })
  let assert Ok(TakeoverAttemptStarted(replay_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(replay_finished, replay_release, replay_reply)
  })
  let assert Ok(replay_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use attempt_count <- decode.field(3, decode.int)
      use delivery_count <- decode.field(4, decode.int)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        attempt_count,
        delivery_count,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#(replay_attempt_id, replay_epoch, replay_owner, 2, 2)] =
    replay_claim.rows
  replay_attempt_id |> should.not_equal(first_attempt_id)
  replay_epoch |> should.equal(first_epoch + 1)
  replay_owner |> should.not_equal(first_owner)

  process.send(first_release, ReleaseAttempt)
  process.receive(first_reply, within: 5000)
  |> should.equal(
    Ok(
      Error(
        queue.QueueProcessFailed(postgres.QueueAckStale(
          worker.ExecutedSuccess("takeover-output-v1", "\"obsolete-7\""),
          postgres.AckOwnershipChanged(
            attempt_id: Some(replay_attempt_id),
            epoch: Some(replay_epoch),
            owner: Some(replay_owner),
          ),
        )),
      ),
    ),
  )
  process.send(first_finished, Nil)

  process.send(replay_release, ReleaseAttempt)
  process.receive(replay_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.send(replay_finished, Nil)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("current-7")))
  mark_database_test_executed("expired-attempt-audited-replay-passed")
}

pub fn postgres_expired_attempt_requires_reconciliation_by_default_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_expired_attempt_quarantine_test(database_url)
  }
}

pub fn postgres_expiry_quarantine_is_bounded_per_attempt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_bounded_quarantine_test(database_url)
  }
}

pub fn postgres_acknowledgement_rejects_exact_database_expiry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_exact_expiry_test(database_url)
  }
}

pub fn postgres_ack_after_database_expiry_is_stale_without_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_after_database_expiry_test(database_url)
  }
}

fn run_exact_expiry_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "exact-expiry-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "exact-expiry-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker) =
    worker.define(
      "exact-expiry.echo",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) = postgres.submit(database, "exact-expiry", worker, 5)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = postgres.connection(database)
  let assert Ok(boundary) =
    pog.query(
      "WITH database_time AS MATERIALIZED (SELECT clock_timestamp() AS instant), boundary AS MATERIALIZED (UPDATE grind_jobs AS job SET lease_expires_at = database_time.instant FROM database_time WHERE job.id = $1 RETURNING job.lease_expires_at, database_time.instant) SELECT lease_expires_at = instant, "
      <> lease.live_lease_predicate("instant")
      <> ", "
      <> lease.expired_lease_predicate("instant")
      <> " FROM boundary",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use exact <- decode.field(0, decode.bool)
      use acknowledgement_allowed <- decode.field(1, decode.bool)
      use quarantine_eligible <- decode.field(2, decode.bool)
      decode.success(#(exact, acknowledgement_allowed, quarantine_eligible))
    })
    |> pog.execute(on: connection)
  let assert [#(exact, acknowledgement_allowed, quarantine_eligible)] =
    boundary.rows
  exact |> should.equal(True)
  acknowledgement_allowed |> should.equal(False)
  // At the exact boundary, `expired_lease_predicate` must be the complement
  // of `live_lease_predicate` (a strict `<=` and a strict `>` on the same
  // pair can never both be true or both be false), proving the quarantine
  // scan's own fragment agrees with the acknowledgement fragment on exactly
  // this tie instead of merely happening not to disagree elsewhere.
  quarantine_eligible |> should.equal(!acknowledgement_allowed)
  quarantine_eligible |> should.equal(True)
  mark_database_test_executed("exact-expiry-rejected")
}

/// Proves the same fenced-lease predicate rejects acknowledgement once the
/// lease has already expired by database time, on the *production*
/// acknowledgement path (the test above only exercises the SQL predicate
/// directly). The lease is deliberately much longer than this test's whole
/// run so no automatic renewal tick can fire and confuse the result with a
/// renewal-detected loss instead of the forced write below. Honest wording:
/// this proves the "lease already expired" side of the boundary on the real
/// `acknowledge_claim` path, not exact-instant equality — real time elapses
/// between the forced write below and the ack transaction's own later
/// `clock_timestamp()` call, so by the time production code evaluates the
/// predicate the lease is already in the past, not tied to it. Exact
/// equality at a single instant is what the predicate-only test above
/// proves, against this same shared fragment.
fn run_ack_after_database_expiry_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "ack-after-expiry-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "ack-after-expiry-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "ack.after.expiry",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("after-expiry-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("ack-after-expiry")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "ack-after-expiry", slow_worker, 9)
  let job_id = job.id_value(handle)
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
  let assert Ok(#(attempt_id, epoch, _, Some(attempt_owner))) =
    attempt_snapshot(connection, job_id)

  // Tightest reachable forced expiry: the row's lease is set to the
  // database's own "now" rather than a value already further in the past.
  // The elapsed time between this UPDATE committing and the ack
  // transaction's own later clock_timestamp() call is what pushes the
  // lease into the past by the time production code evaluates it.
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  // No renewal tick has fired yet (lease_duration_ms / 3 is far outside this
  // test's whole run), so the coordinator's own renewal status is still
  // whatever the claim left it at. This confirms the ack rejection below
  // comes from the forced write, not from a renewal loss the coordinator
  // already detected on its own.
  queue.renewal_status(consumer)
  |> should.equal(Ok(Some(queue.LeaseRenewalConfirmed)))

  process.send(release, ReleaseAttempt)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
    proposed,
    postgres.AckLeaseExpired(stale_attempt_id, stale_epoch, stale_owner),
  )))) = process.receive(reply, within: 5000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "ack-after-expiry-output-v1",
    "\"after-expiry-9\"",
  ))
  stale_attempt_id |> should.equal(attempt_id)
  stale_epoch |> should.equal(epoch)
  stale_owner |> should.equal(attempt_owner)

  let command_id = attempt.acknowledgement_command_id(job_id, attempt_id, epoch)
  let assert Ok(receipt_rows) =
    pog.query(
      "SELECT count(*) FROM grind_job_acknowledgements WHERE command_id = $1",
    )
    |> pog.parameter(pog.text(command_id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  let assert [0] = receipt_rows.rows

  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(Error(postgres.ReceiptNotFound))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed(
    "ack-after-database-expiry-stale-no-receipt-passed",
  )
}

fn run_bounded_quarantine_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("bounded-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "bounded-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker) =
    worker.define("bounded.echo", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("bounded-quarantine")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(first) =
    postgres.submit(database, "bounded-quarantine", worker, 1)
  let assert Ok(second) =
    postgres.submit(database, "bounded-quarantine", worker, 2)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'expired-owner', lease_expires_at = clock_timestamp(), attempt_count = 1, delivery_count = 1, cancel_requested_at = CASE WHEN input = '1'::jsonb THEN clock_timestamp() ELSE NULL END WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("bounded.echo"))
    |> pog.parameter(pog.text("bounded-quarantine"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  let states = [
    postgres.state(database, first),
    postgres.state(database, second),
  ]
  let uncertain_count =
    list.count(states, fn(state) { state == Ok(job.Uncertain) })
  uncertain_count |> should.equal(1)
  let executing_count =
    list.count(states, fn(state) { state == Ok(job.Executing) })
  executing_count |> should.equal(1)
  postgres.state(database, first) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, first)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired after cancellation request; prior effect unknown",
    )),
  )

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, second) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, second)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  mark_database_test_executed("quarantine-bounded-passed")
}

fn run_expired_attempt_quarantine_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("quarantine-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "quarantine-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let probe = process.new_subject()
  let assert Ok(worker) =
    worker.define("quarantine.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("expired-quarantine")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(handle) =
    postgres.submit(database, "expired-quarantine", worker, 9)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'dead-consumer', lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("quarantine.echo"))
    |> pog.parameter(pog.text("expired-quarantine"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  postgres.arguments(database, handle) |> should.equal(Ok(9))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  let assert Ok(expired_attempt) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, lease_expires_at <= clock_timestamp(), failure_description, uncertain_at IS NOT NULL FROM grind_jobs WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("quarantine.echo"))
    |> pog.parameter(pog.text("expired-quarantine"))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use lease_expired <- decode.field(3, decode.bool)
      use reason <- decode.field(4, decode.string)
      use uncertainty_time_recorded <- decode.field(5, decode.bool)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        lease_expired,
        reason,
        uncertainty_time_recorded,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      attempt_id,
      attempt_epoch,
      attempt_owner,
      lease_expired,
      reason,
      uncertainty_time_recorded,
    ),
  ] = expired_attempt.rows
  attempt_id |> should.not_equal(0)
  attempt_epoch |> should.equal(1)
  attempt_owner |> should.equal("dead-consumer")
  lease_expired |> should.equal(True)
  reason |> should.equal("expired attempt requires outcome reconciliation")
  uncertainty_time_recorded |> should.equal(True)
  mark_database_test_executed("expired-attempt-quarantine-passed")
}
