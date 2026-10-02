import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleeunit/should
import grind/diagnostic
import grind/internal/attempt
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/ack_queries.{
  stored_attempt_identity, wait_for_commit_trigger_backend,
}
import grind/support/concurrency.{ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/diagnostics
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/job_state.{
  retry_transient_query, wait_for_job_state_tolerating_errors,
}
import grind/support/observers.{detach}
import grind/support/queue_signals.{FirstAttemptStarted, WorkerInvoked}
import grind/support/syncrep.{
  backend_pid_is_alive, install_syncrep_reply_trigger,
  require_syncrep_cluster_configured, terminate_backend,
  wait_for_syncrep_trigger_backend,
}
import grind/support/worker_failure.{AccountMissing}
import grind/worker
import pog

pub fn postgres_ack_commit_connection_loss_is_unknown_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_commit_connection_loss_test(database_url)
  }
}

fn run_ack_commit_connection_loss_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "ack-commit-loss-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "ack-commit-loss-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("ack.commit.loss", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      process.send(invoked, WorkerInvoked)
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("terminated-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("ack-commit-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-commit-loss", definition, 21)
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
      "CREATE FUNCTION grind_test_kill_ack_backend() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_ack_backend AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_ack_backend()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_ack_backend ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_kill_ack_backend()")
      |> pog.execute(on: connection)
    Nil
  })

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 100)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckUnknown(
    command_id,
    proposed,
  )))) = process.receive(reply, within: 10_000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "ack-commit-loss-output-v1",
    "\"terminated-21\"",
  ))
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(Error(postgres.ReceiptNotFound))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("ack-commit-connection-loss-unknown-passed")
}

/// Same aborted-commit fault as `run_ack_commit_connection_loss_test` above,
/// but against an AUTOMATIC consumer instead of a manually driven one: no
/// caller is waiting on `process_one`'s reply to retry anything, so recovery
/// here depends entirely on the coordinator's own handling of
/// `QueueAckUnknown`. Before the fix, the coordinator dropped the claim on
/// any `ProcessError` (including this one), so the job sat `Executing` until
/// its lease eventually expired and a claim-time scan quarantined it to
/// `Uncertain` — recoverable only by an operator's audited resolution, never
/// on its own. With the fix, the coordinator keeps retrying the exact same
/// `acknowledge_claim` on its renewal timer; once the trigger stops sleeping
/// (the transaction that aborted never left a receipt behind, so the retry
/// performs the acknowledgement fresh, exactly like the unique-admission
/// aborted-commit test's plain retry converges), the job reaches `succeeded`
/// on its own, without ever needing `resolve_uncertain`.
pub fn postgres_automatic_ack_commit_connection_loss_recovers_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_automatic_ack_commit_connection_loss_recovers_test(database_url)
  }
}

fn run_automatic_ack_commit_connection_loss_recovers_test(
  database_url: String,
) -> Nil {
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
      "auto-ack-commit-loss-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "auto-ack-commit-loss-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "auto.ack.commit.loss",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("auto-terminated-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("auto-ack-commit-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "auto-ack-commit-loss", definition, 41)
  let #(acks, ack_attachment) =
    diagnostics.capture(diagnostic.acknowledgement(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(ack_attachment) })
  let #(retries, retry_attachment) =
    diagnostics.capture(diagnostic.acknowledgement_retry(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(retry_attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(4008)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_kill_ack_backend_auto() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_ack_backend_auto AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_ack_backend_auto()",
    )
    |> pog.execute(on: connection)
  let drop_trigger = fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_ack_backend_auto ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_kill_ack_backend_auto()")
      |> pog.execute(on: connection)
    Nil
  }
  use <- exception.defer(drop_trigger)

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  // The aborted transaction left no receipt at all — same proof
  // `run_ack_commit_connection_loss_test` uses above — so this is genuinely
  // the "never committed" case, not a lost reply after a real commit.
  // `wait_for_job_state_tolerating_errors`, not the plain
  // `wait_for_job_state`: the pool just lost the connection this test itself
  // terminated, and a read against the same pool can transiently error while
  // it recovers — that must not be mistaken for "state observed and it
  // doesn't match yet".
  wait_for_job_state_tolerating_errors(database, handle, job.Executing, 250)
  |> should.equal(True)

  // Dropping the trigger before the coordinator's own retry mirrors the
  // unique-admission aborted-commit test's own ordering: the same
  // `command_id` would hang in another 30-second sleep otherwise, with
  // nobody left to terminate that backend.
  drop_trigger()

  // `pg_terminate_backend` only signals the backend; it can take several
  // seconds of real time for the coordinator's own blocked `acknowledge_claim`
  // call to observe the closed connection (the same reason
  // `run_ack_commit_connection_loss_test` above waits up to 10 seconds for
  // its reply) — this budget must cover that detection latency plus at
  // least one retry interval afterward.
  wait_for_job_state_tolerating_errors(database, handle, job.Succeeded, 750)
  |> should.equal(True)
  // Same transient-pool-recovery tolerance as the wait above: the row is
  // already confirmed committed at this point, but a plain read moments
  // after this test's own killed connection can still transiently time out
  // while the pool recovers.
  retry_transient_query(fn() { postgres.arguments(database, handle) }, 20)
  |> should.equal(Ok(41))
  retry_transient_query(fn() { postgres.outcome(database, handle) }, 20)
  |> should.equal(Ok(job.SucceededWith("auto-terminated-41")))
  let assert Ok(#(_, unknown)) = process.receive(acks, 5000)
  unknown.outcome |> should.equal(diagnostic.AckUnknown)
  let assert Ok(#(retried, retry)) = process.receive(retries, 5000)
  retry.reason |> should.equal(diagnostic.RetryAfterUnknown)
  retried.retry_number |> should.equal(1)
  retried.delay_ms |> should.equal(1336)
  retry.context |> should.equal(unknown.context)
  retry.command_id |> should.equal(unknown.command_id)
  let assert Ok(#(_, completed)) =
    diagnostics.await(
      acks,
      fn(sample) { sample.1.outcome == diagnostic.AckReplied },
      5000,
    )
  completed.context |> should.equal(unknown.context)
  completed.command_id |> should.equal(unknown.command_id)
  mark_database_test_executed("diagnostic-ack-unknown-retry")
  mark_database_test_executed(
    "automatic-ack-commit-connection-loss-recovers-passed",
  )
}

/// A *persistently* aborting commit — every acknowledgement attempt for
/// this job, first and every retry alike, hits the same deferred-trigger
/// abort (the trigger is never dropped mid-test) — must not retry forever.
/// Bounded renewal (`ConsumerState.pending_ack_retry_budget`, roughly one
/// lease duration's worth of ticks) means only the first few retries renew
/// the lease; once that budget is spent the lease is left to lapse, and
/// either the retry's own next attempt observes a known
/// `QueueAckStale(_, AckLeaseExpired(..))` or a poll's ordinary
/// claim-time quarantine scan gets there first — either way the job ends up
/// `uncertain`, not stuck `executing` forever. `maximum_concurrency: 2`
/// keeps this consumer polling (via the free-capacity fix) so its own
/// quarantine scan keeps running throughout. Driven entirely by
/// `pg_stat_activity` barriers (`wait_for_commit_trigger_backend`) and a
/// generous iteration cap, not a wall-clock sleep: if retrying were
/// unbounded, this loop would exhaust its cap with the job still
/// `executing`.
pub fn postgres_automatic_ack_retry_bounded_eventually_uncertain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_ack_retry_bounded_test(database_url)
  }
}

fn run_automatic_ack_retry_bounded_test(database_url: String) -> Nil {
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
      "auto-ack-bounded-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "auto-ack-bounded-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "auto.ack.bounded",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("bounded-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("auto-ack-bounded")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "auto-ack-bounded", definition, 51)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(6100)
    |> queue.with_poll_interval(50)
    |> queue.with_maximum_concurrency(2)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_kill_ack_backend_bounded() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_ack_backend_bounded AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_ack_backend_bounded()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_ack_backend_bounded ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_kill_ack_backend_bounded()")
      |> pog.execute(on: connection)
    Nil
  })

  process.send(release, ReleaseAttempt)

  // The retry budget itself is ~3 ticks regardless of lease length (see
  // `ConsumerState.pending_ack_retry_budget`), but each tick now fires every
  // `lease_duration_ms / 3` — a real ~2000ms with this test's now-6100ms
  // lease (bumped up from 300ms so `queue.start` clears
  // `LeaseTooShortForDeadline` against the default `statement_deadline_ms`)
  // rather than ~100ms, so this iteration cap must cover several times
  // longer in wall-clock terms than before.
  kill_ack_backends_until_uncertain(database, handle, connection, 800)
  |> should.equal(True)

  mark_database_test_executed(
    "automatic-ack-retry-bounded-eventually-uncertain-passed",
  )
}

/// Repeatedly finds and kills this job's own acknowledgement backend (the
/// deferred trigger installed by the caller sleeps every single attempt),
/// checking the job's own state between kills, until it observes `uncertain`
/// or exhausts `remaining_iterations`. Once the ack retry loop itself gives
/// up (a known `QueueAckStale` once the lease lapses), no further backend
/// ever sleeps for this job — `current_ack_rejection` reads the row instead
/// of ever reaching the trigger's own `INSERT` once the row is no longer
/// `executing` — so a "miss" here just means waiting for some poll's own
/// quarantine scan to catch up, not a sign anything is wrong.
fn kill_ack_backends_until_uncertain(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  connection: pog.Connection,
  remaining_iterations: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(job.Uncertain) -> True
    _ ->
      case remaining_iterations > 0 {
        False -> False
        True ->
          case wait_for_commit_trigger_backend(connection, 10) {
            Ok(backend_pid) -> {
              let _ = terminate_backend(connection, backend_pid)
              kill_ack_backends_until_uncertain(
                database,
                handle,
                connection,
                remaining_iterations - 1,
              )
            }
            Error(Nil) -> {
              process.sleep(20)
              kill_ack_backends_until_uncertain(
                database,
                handle,
                connection,
                remaining_iterations - 1,
              )
            }
          }
      }
  }
}

/// Increment 2: a genuinely successful ack whose reply is lost after
/// PostgreSQL has already committed locally. The disposable cluster is
/// started with `synchronous_standby_names=grind_never_standby` and
/// `synchronous_commit=local` (scripts/test-postgres.sh), so an ordinary
/// commit stays local, but a deferred constraint trigger scoped to this
/// job's acknowledgement row raises this one transaction's own
/// `synchronous_commit` to `on` (session-local, `set_config(..., true)`)
/// just before COMMIT. Because the configured standby name never connects,
/// that COMMIT parks in PostgreSQL's `SyncRep` wait *after* its WAL record is
/// already locally flushed — genuinely committed, reply not yet sent.
/// Terminating that backend at that exact moment (observed by polling
/// `pg_stat_activity` for `wait_event = 'SyncRep'`) reproduces "PostgreSQL
/// committed, but the client's connection closed before it saw the reply"
/// without a TCP proxy or any production test hook: the client observes a
/// closed connection during COMMIT, exactly like the existing aborted-commit
/// test, but this time a receipt genuinely exists to reconcile from.
pub fn postgres_ack_committed_reply_lost_reconciles_from_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_ack_committed_reply_lost_reconciles_test(database_url)
  }
}

fn run_ack_committed_reply_lost_reconciles_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)
  let assert Ok(input_codec) =
    worker.codec(
      "ack-reply-lost-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "ack-reply-lost-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("ack.reply.lost", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      process.send(invoked, WorkerInvoked)
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("reply-lost-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("ack-reply-lost")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-reply-lost", definition, 33)
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
    "grind_test_syncrep_reply_lost",
    "grind_job_acknowledgements",
    "NEW.job_id = " <> int.to_string(job_id),
  ))

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  process.receive(reply, within: 10_000) |> should.equal(Ok(Ok(True)))
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("reply-lost-33")))
  let assert Ok(#(attempt_id, epoch)) =
    stored_attempt_identity(connection, job_id)
  let command_id = attempt.acknowledgement_command_id(job_id, attempt_id, epoch)
  let assert Ok(postgres.AcknowledgementReceipt(committed_state:, ..)) =
    postgres.reconcile_acknowledgement(database, handle, command_id)
  committed_state |> should.equal(job.Succeeded)
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("ack-committed-reply-lost-reconciled-passed")
}

pub fn postgres_reconcile_acknowledgement_wrong_job_command_id_is_receipt_job_mismatch_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_reconcile_acknowledgement_wrong_job_test(database_url)
  }
}

/// A receipt read back under another job's own command ID is a genuine
/// caller mistake (the command ID was copied from the wrong handle), not a
/// missing job or a missing receipt: `ReceiptJobMismatch` names it
/// precisely instead of collapsing it into `JobNotFound` or
/// `QueueRouteMismatch`.
fn run_reconcile_acknowledgement_wrong_job_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "receipt-job-mismatch-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "receipt-job-mismatch-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "receipt.job.mismatch",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("receipt-job-mismatch")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle_a) =
    postgres.submit(database, "receipt-job-mismatch", definition, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "receipt-job-mismatch", definition, 2)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  queue.process_one(consumer) |> should.equal(Ok(True))
  queue.process_one(consumer) |> should.equal(Ok(True))
  let connection = postgres.connection(database)
  let job_id_a = job.id_value(handle_a)
  let job_id_b = job.id_value(handle_b)
  let assert Ok(#(attempt_id_b, epoch_b)) =
    stored_attempt_identity(connection, job_id_b)
  let command_id_b =
    attempt.acknowledgement_command_id(job_id_b, attempt_id_b, epoch_b)
  postgres.reconcile_acknowledgement(database, handle_a, command_id_b)
  |> should.equal(
    Error(postgres.ReceiptJobMismatch(expected: job_id_a, actual: job_id_b)),
  )
  mark_database_test_executed(
    "reconcile-acknowledgement-receipt-job-mismatch-passed",
  )
}
