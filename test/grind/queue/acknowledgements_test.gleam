import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{None, Some}
import gleeunit/should
import grind/internal/attempt
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/ack_queries.{count_acknowledgements_for_job}
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt, spawn_lock_holder,
  spawn_submit, unique_test_lock_key,
}
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, queue_database_url, repeatable_read_url,
}
import grind/support/lock_wait.{await_lock_wait_counts}
import grind/support/queue_signals.{FirstAttemptStarted, WorkerInvoked}
import grind/support/syncrep.{
  install_syncrep_reply_trigger, require_syncrep_cluster_configured,
  terminate_backend, wait_for_backend_gone, wait_for_syncrep_trigger_backend,
}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_databases}
import grind/support/worker_failure.{AccountMissing}
import pog

pub fn postgres_acknowledgement_persists_a_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_receipt_test(database_url)
  }
}

/// Same fault as above, but Grind's own pool is closed (not the PostgreSQL
/// backend) while the ack's COMMIT is still parked in `SyncRep`, so the
/// receipt lookup that would otherwise reconcile the lost reply cannot run
/// either. A separate observer pool (independent of Grind's pool) is used to
/// poll for the SyncRep wait, read the committed attempt identity, and later
/// terminate the stuck backend once the store-unavailable assertion has been
/// made, exactly as prescribed.
pub fn postgres_ack_committed_reply_lost_with_store_unavailable_is_unknown_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_ack_committed_reply_lost_with_store_unavailable_test(
        database_url,
        False,
      )
  }
}

pub fn postgres_cancelled_uncertain_ack_reply_lost_retains_evidence_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_ack_committed_reply_lost_with_store_unavailable_test(
        database_url,
        True,
      )
  }
}

fn run_ack_committed_reply_lost_with_store_unavailable_test(
  database_url: String,
  uncertain: Bool,
) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_settings =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = postgres.connection(observer)
  require_syncrep_cluster_configured(observer_connection)

  let assert Ok(input_codec) =
    worker.codec(
      "ack-reply-lost-unavailable-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "ack-reply-lost-unavailable-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "ack.reply.lost.unavailable",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("unavailable-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let definition = case uncertain {
    False -> definition
    True ->
      worker.with_queue_handler(definition, fn(_value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        let assert Ok(ReleaseAttempt) = process.receive(release, within: 10_000)
        worker.WorkerUncertain("provider may have executed")
      })
  }
  let assert Ok(workers) = registry.new("ack-reply-lost-unavailable")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-reply-lost-unavailable", definition, 34)
  let attempt_owner = "ack-reply-lost-unavailable-owner"
  // Claimed directly through the postgres-level API (not `queue`), so the
  // opaque `ClaimedJob`/`Execution` values stay in scope for the same-command
  // retry through `attempt.acknowledge` after the pool is reopened,
  // below. `claim_one` itself does not block; only the worker's own handler
  // (invoked by `execute_claim`, in the spawned process) does.
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "ack-reply-lost-unavailable",
      workers,
      attempt_owner,
      30_000,
    )
  let #(claimed_id, attempt_id, epoch) = attempt.claim_identity(claimed)
  let command_id =
    attempt.acknowledgement_command_id(claimed_id, attempt_id, epoch)
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let execution = attempt.execute_claim_inline(claimed)
      let ack_result =
        attempt.acknowledge(
          database,
          "ack-reply-lost-unavailable",
          attempt_owner,
          claimed,
          execution,
        )
      process.send(reply, #(execution, ack_result))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  case uncertain {
    True ->
      postgres.cancel(database, handle)
      |> should.equal(Ok(postgres.CancellationRequested))
    False -> Nil
  }
  let job_id = job.id_value(handle)
  use <- exception.defer(install_syncrep_reply_trigger(
    observer_connection,
    "grind_test_syncrep_reply_lost_unavailable",
    "grind_job_acknowledgements",
    "NEW.job_id = " <> int.to_string(job_id),
  ))

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) =
    wait_for_syncrep_trigger_backend(observer_connection, 300)

  let _ = postgres.close(database)

  let assert Ok(#(execution, ack_result)) =
    process.receive(reply, within: 10_000)
  let expected_execution = case uncertain {
    True -> worker.ExecutedUncertain("provider may have executed")
    False ->
      worker.ExecutedSuccess(
        "ack-reply-lost-unavailable-output-v1",
        "\"unavailable-34\"",
      )
  }
  execution |> should.equal(expected_execution)
  ack_result
  |> should.equal(Error(postgres.QueueAckUnknown(command_id, execution)))

  terminate_backend(observer_connection, backend_pid) |> should.equal(True)
  // `pg_terminate_backend` only signals the backend; it returns before the
  // target has actually finished `ProcArrayEndTransaction` and exited. Unlike
  // the reconciles-from-receipt test (where the coordinator's own blocked
  // read on that same backend already orders its lookup after that step),
  // here Grind's pool was closed client-side, so nothing else orders "reopen
  // and query" after "the backend actually finished committing." Wait for it
  // explicitly instead of assuming it.
  let assert Ok(Nil) =
    wait_for_backend_gone(observer_connection, backend_pid, 300)

  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  let assert Ok(postgres.AcknowledgementReceipt(committed_state:, ..)) =
    postgres.reconcile_acknowledgement(reopened, handle, command_id)
  committed_state
  |> should.equal(case uncertain {
    True -> job.Uncertain
    False -> job.Succeeded
  })
  postgres.outcome(reopened, handle)
  |> should.equal(
    Ok(case uncertain {
      True -> job.ReconciliationRequired("provider may have executed")
      False -> job.SucceededWith("unavailable-34")
    }),
  )

  // The same command, retried end to end through the reopened store: proves
  // idempotent replay, not just that the receipt can be read back.
  attempt.acknowledge(
    reopened,
    "ack-reply-lost-unavailable",
    attempt_owner,
    claimed,
    execution,
  )
  |> should.equal(Ok(True))

  let assert Ok(fresh_consumer) =
    queue.start(reopened, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(fresh_consumer)
    Nil
  })
  queue.process_one(fresh_consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  case uncertain {
    True -> {
      let assert Ok(intent) =
        pog.query(
          "SELECT cancel_requested_at IS NOT NULL FROM grind_jobs WHERE id = $1",
        )
        |> pog.parameter(pog.int(job.id_value(handle)))
        |> pog.returning({
          use requested <- decode.field(0, decode.bool)
          decode.success(requested)
        })
        |> pog.execute(on: postgres.connection(reopened))
      intent.rows |> should.equal([True])
      mark_database_test_executed("uncertain-cancel-ack-lost-reply-reconciled")
    }
    False ->
      mark_database_test_executed(
        "ack-committed-reply-lost-store-unavailable-unknown-passed",
      )
  }
}

fn run_ack_receipt_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ack-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-output-v1", worker.infallible(json.string), decode.string)
  let invocation = process.new_subject()
  let assert Ok(definition) =
    worker.define("ack.receipt", "v1", input_codec, output_codec, fn(value) {
      process.send(invocation, WorkerInvoked)
      Ok("result-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("ack-receipt")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-receipt", definition, 8)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "ack-receipt",
      workers,
      "ack-receipt-owner",
      30_000,
    )
  let execution = attempt.execute_claim_inline(claimed)
  process.receive(invocation, within: 1000) |> should.equal(Ok(WorkerInvoked))
  attempt.acknowledge(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    worker.ExecutedSuccess("ack-output-v2", "\"wrong-codec\""),
  )
  |> should.equal(
    Error(postgres.QueueAckProposalCodecMismatch(
      worker.OutputCodec,
      "ack-output-v1",
      "ack-output-v2",
    )),
  )
  let #(claimed_id, attempt_id, epoch) = attempt.claim_identity(claimed)
  let command_id =
    attempt.acknowledgement_command_id(claimed_id, attempt_id, epoch)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_test_reject_ack CHECK (command_id <> '"
      <> command_id
      <> "')",
    )
    |> pog.execute(on: connection)
  let assert Error(_) =
    attempt.acknowledge(
      database,
      "ack-receipt",
      "ack-receipt-owner",
      claimed,
      execution,
    )
  let assert Ok(after_failed_ack) =
    pog.query(
      "SELECT state, (SELECT count(*) FROM grind_job_acknowledgements WHERE command_id = $2)::bigint FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.parameter(pog.text(command_id))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use receipts <- decode.field(1, decode.int)
      decode.success(#(state, receipts))
    })
    |> pog.execute(on: connection)
  let assert [#(state_after_failed_ack, receipt_count_after_failed_ack)] =
    after_failed_ack.rows
  state_after_failed_ack |> should.equal("executing")
  receipt_count_after_failed_ack |> should.equal(0)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_test_reject_ack",
    )
    |> pog.execute(on: connection)
  attempt.acknowledge(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  // A retry with the same stable command and exact proposal is idempotent.
  attempt.acknowledge(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  attempt.acknowledge(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    worker.ExecutedSuccess("ack-output-v1", "\"tampered\""),
  )
  |> should.equal(Error(postgres.QueueAckCommandConflict))
  let assert Ok(receipts) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, command_id, attempt_owner, queue, worker_id, worker_version, committed_state, failure_cause, octet_length(proposal_sha256), (extract(epoch FROM committed_at) * 1000)::bigint FROM grind_job_acknowledgements WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use epoch <- decode.field(1, decode.int)
      use command_id <- decode.field(2, decode.string)
      use attempt_owner <- decode.field(3, decode.string)
      use queue <- decode.field(4, decode.string)
      use worker_id <- decode.field(5, decode.string)
      use worker_version <- decode.field(6, decode.string)
      use committed_state <- decode.field(7, decode.string)
      use failure_cause <- decode.field(8, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(9, decode.int)
      use committed_at <- decode.field(10, decode.int)
      decode.success(#(
        attempt_id,
        epoch,
        command_id,
        attempt_owner,
        queue,
        worker_id,
        worker_version,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        committed_at,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      attempt_id,
      epoch,
      command_id,
      attempt_owner,
      queue,
      worker_id,
      worker_version,
      committed,
      failure_cause,
      fingerprint_bytes,
      committed_at,
    ),
  ] = receipts.rows
  should.be_true(attempt_id > 0)
  epoch |> should.equal(1)
  command_id |> should.not_equal("")
  attempt_owner |> should.equal("ack-receipt-owner")
  queue |> should.equal("ack-receipt")
  worker_id |> should.equal("ack.receipt")
  worker_version |> should.equal("v1")
  committed |> should.equal("succeeded")
  failure_cause |> should.equal(None)
  fingerprint_bytes |> should.equal(32)
  should.be_true(committed_at > 0)
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: receipt_command,
    attempt_id: receipt_attempt,
    attempt_epoch: receipt_epoch,
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at_unix_ms: receipt_time,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_command |> should.equal(command_id)
  receipt_attempt |> should.equal(attempt_id)
  receipt_epoch |> should.equal(epoch)
  receipt_state |> should.equal(job.Succeeded)
  receipt_cause |> should.equal(None)
  receipt_time |> should.equal(committed_at)
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("result-8")))
  mark_database_test_executed("durable-ack-receipt-passed")
}

pub fn postgres_manual_batch_reports_acknowledged_prefix_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_batch_partial_error_test(database_url)
  }
}

fn run_batch_partial_error_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("partial-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec("partial-output-v1", worker.infallible(json.int), decode.int)
  let assert Ok(first_worker) =
    worker.define(
      "batch.partial.first",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(incompatible_worker) =
    worker.define(
      "batch.partial.incompatible",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(last_worker) =
    worker.define(
      "batch.partial.last",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(workers) = registry.new("batch-partial")
  let assert Ok(workers) = registry.register(workers, first_worker)
  let assert Ok(workers) = registry.register(workers, incompatible_worker)
  let assert Ok(workers) = registry.register(workers, last_worker)
  let assert Ok(first) =
    postgres.submit(database, "batch-partial", first_worker, 1)
  let assert Ok(incompatible) =
    postgres.submit(database, "batch-partial", incompatible_worker, 2)
  let assert Ok(last) =
    postgres.submit(database, "batch-partial", last_worker, 3)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2 AND queue = $3",
    )
    |> pog.parameter(pog.text("partial-output-v2"))
    |> pog.parameter(pog.text("batch.partial.incompatible"))
    |> pog.parameter(pog.text("batch-partial"))
    |> pog.execute(on: connection)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_batch_jobs(3)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_available(consumer)
  |> should.equal(queue.BatchStopped(
    acknowledged_before_error: 1,
    error: queue.QueueProcessFailed(postgres.QueueCodecMismatch(
      kind: worker.OutputCodec,
      expected: "partial-output-v2",
      actual: "partial-output-v1",
    )),
  ))
  postgres.state(database, first) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, incompatible)
  |> should.equal(Ok(job.ContractMismatch))
  postgres.state(database, last) |> should.equal(Ok(job.Queued))
  mark_database_test_executed("batch-partial-commit-count-passed")
}

/// A retried (duplicate) acknowledgement forced to genuinely overlap the
/// first one's commit: A's fenced `UPDATE` is blocked behind a test-only
/// `BEFORE UPDATE` barrier trigger scoped to this job (the same
/// held-then-released-on-cue shape used throughout); B — the identical
/// acknowledgement command, from a separate pool — starts while A is still
/// blocked, and B's own fenced `UPDATE` then genuinely waits on the row
/// lock A's in-flight `UPDATE` holds (a real PostgreSQL tuple-lock wait,
/// confirmed via `pg_stat_activity`'s `transactionid` wait event — not the
/// advisory wait A is parked on). Once A completes and commits, B's
/// `UPDATE` no longer matches (the row is no longer `executing`), so B
/// falls through to `acknowledge_transaction`'s own re-read of the
/// acknowledgement receipt and must return `Ok(True)`, exactly as A did —
/// not a query failure. See `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/RECOVERY-EVIDENCE.md`, "Isolation-level
/// pinning", for the genuine red this test produced before
/// `postgres.validate` pinned every pooled connection's own
/// `default_transaction_isolation` to `read committed`.
pub fn postgres_ack_duplicate_reports_ok_under_pinned_isolation_test() {
  case repeatable_read_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_duplicate_repeatable_read_test(database_url)
  }
}

fn run_ack_duplicate_repeatable_read_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use entries <- with_unique_databases(database_url, [
    "grind_ack_rr_a_" <> suffix,
    "grind_ack_rr_b_" <> suffix,
  ])
  let assert [#(database_a, connection_a), #(database_b, _)] = entries

  let assert Ok(input_codec) =
    worker.codec(
      "ack-rr-input-" <> suffix <> "-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "ack-rr-output-" <> suffix <> "-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "ack.rr-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("ack-rr-" <> suffix)
  let assert Ok(workers) = registry.register(workers, definition)
  let test_queue = "ack-rr-" <> suffix
  let attempt_owner = "ack-rr-owner-" <> suffix

  let assert Ok(_handle) =
    postgres.submit(database_a, test_queue, definition, 8)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(database_a, test_queue, workers, attempt_owner, 30_000)
  let execution = attempt.execute_claim_inline(claimed)
  let #(job_id, _, _) = attempt.claim_identity(claimed)

  let lock_key = unique_test_lock_key(4)
  let trigger_name = "grind_test_ack_overlap_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND OLD.state = 'executing' AND NEW.state <> 'executing' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection_a)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> trigger_name
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection_a)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection_a)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection_a)
    Nil
  })

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_ack_overlap_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection_a, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net (see the uniqueness barrier tests above for why this is
  // registered here, right after obtaining `release_lock`, rather than only
  // sending it explicitly further down).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    attempt.acknowledge(
      database_a,
      test_queue,
      attempt_owner,
      claimed,
      execution,
    )
  })

  // A must actually be blocked updating behind the barrier (an advisory
  // wait) before B starts.
  await_lock_wait_counts(connection_a, 1, 0, 500) |> should.equal(True)

  spawn_submit(result_b, fn() {
    attempt.acknowledge(
      database_b,
      test_queue,
      attempt_owner,
      claimed,
      execution,
    )
  })

  // B must be genuinely waiting on the row lock A's own `UPDATE` holds
  // (`transactionid`, a real tuple-lock wait — not the advisory wait A is
  // parked on) before we release the barrier, or this proves nothing about
  // the overlap.
  await_lock_wait_counts(connection_a, 1, 1, 500) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  outcome_a |> should.equal(Ok(True))
  outcome_b |> should.equal(Ok(True))
  count_acknowledgements_for_job(connection_a, job_id) |> should.equal(1)

  mark_database_test_executed("ack-duplicate-ok-under-pinned-isolation-passed")
}
