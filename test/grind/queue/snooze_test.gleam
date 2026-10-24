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
import grind/support/concurrency.{LongHandlerStarted, ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/queue_signals.{LaterWorkerInvoked, WorkerInvoked}
import grind/support/queue_timing.{database_time_milliseconds}
import grind/support/worker_failure.{AccountMissing}
import pog

pub fn postgres_worker_snooze_commits_scheduled_state_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_snooze_test(database_url)
  }
}

fn run_worker_snooze_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "snooze-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(delay) = worker.retry_delay(60_000)
  let ordinary_probe = process.new_subject()
  let assert Ok(ordinary) =
    worker.define("worker.snooze", "v1", input_codec, output_codec, fn(_) {
      process.send(ordinary_probe, WorkerInvoked)
      Error(AccountMissing(1))
    })
  let queue_probe = process.new_subject()
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      process.send(queue_probe, LaterWorkerInvoked)
      worker.WorkerSnoozed(delay, "awaiting external account")
    })
  let assert Ok(workers) = registry.new("snoozes")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) = postgres.submit(database, "snoozes", snoozing, 1)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let before_ack_ms = database_time_milliseconds(postgres.connection(database))
  queue.process_one(consumer) |> should.equal(Ok(True))
  let after_ack_ms = database_time_milliseconds(postgres.connection(database))
  process.receive(queue_probe, within: 0)
  |> should.equal(Ok(LaterWorkerInvoked))
  process.receive(queue_probe, within: 0) |> should.equal(Error(Nil))
  process.receive(ordinary_probe, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.Pending(job.Scheduled)))
  let assert Ok(snooze_evidence) =
    pog.query(
      "SELECT job.attempt_count, job.snooze_count, floor(extract(epoch FROM job.available_at) * 1000)::bigint, floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use snooze_count <- decode.field(1, decode.int)
      use available_at_ms <- decode.field(2, decode.int)
      use sampled_now_ms <- decode.field(3, decode.int)
      use committed_state <- decode.field(4, decode.string)
      use failure_cause <- decode.field(5, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(6, decode.int)
      decode.success(#(
        attempt_count,
        snooze_count,
        available_at_ms,
        sampled_now_ms,
        committed_state,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(
      attempt_count,
      snooze_count,
      available_at_ms,
      sampled_now_ms,
      "scheduled",
      None,
      32,
    ),
  ] = snooze_evidence.rows
  attempt_count |> should.equal(0)
  snooze_count |> should.equal(1)
  should.be_true(available_at_ms >= before_ack_ms + 60_000)
  should.be_true(available_at_ms <= after_ack_ms + 60_000)
  should.be_true(sampled_now_ms >= after_ack_ms)
  queue.process_one(consumer) |> should.equal(Ok(False))
  mark_database_test_executed("worker-snooze-scheduled-passed")
}

pub fn postgres_worker_snooze_receipt_write_failure_rolls_back_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_snooze_receipt_rollback_test(database_url)
  }
}

fn run_snooze_receipt_rollback_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let connection = postgres.connection(database)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_reject_snooze_receipt ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_reject_snooze_receipt()")
      |> pog.execute(on: connection)
    let _ = postgres.close(database)
  })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "snooze-rollback-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "snooze-rollback-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(delay) = worker.retry_delay(60_000)
  let assert Ok(ordinary) =
    worker.define(
      "worker.snooze.rollback",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerSnoozed(delay, "rollback receipt test")
    })
  let assert Ok(workers) = registry.new("snooze-rollback")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) =
    postgres.submit(database, "snooze-rollback", snoozing, 8)
  let attempt_owner = "snooze-rollback-owner"
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "snooze-rollback",
      workers,
      attempt_owner,
      30_000,
    )
  let proposed = attempt.execute_claim_inline(claimed)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_reject_snooze_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.committed_state = 'scheduled' THEN RAISE EXCEPTION 'injected snooze receipt failure'; END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_reject_snooze_receipt BEFORE INSERT ON grind_job_acknowledgements FOR EACH ROW EXECUTE FUNCTION grind_test_reject_snooze_receipt()",
    )
    |> pog.execute(on: connection)
  let acknowledgement_failed = case
    attempt.acknowledge(
      database,
      "snooze-rollback",
      attempt_owner,
      claimed,
      proposed,
    )
  {
    Error(_) -> True
    Ok(_) -> False
  }
  acknowledgement_failed |> should.equal(True)
  let assert Ok(state_after_rollback) =
    pog.query(
      "SELECT state, attempt_count, snooze_count, (SELECT count(*) = 0 FROM grind_job_acknowledgements WHERE job_id = $1) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_count <- decode.field(1, decode.int)
      use snooze_count <- decode.field(2, decode.int)
      use no_receipt <- decode.field(3, decode.bool)
      decode.success(#(state, attempt_count, snooze_count, no_receipt))
    })
    |> pog.execute(on: connection)
  let assert [#("executing", 1, 0, True)] = state_after_rollback.rows
  mark_database_test_executed("worker-snooze-receipt-rollback-passed")
}

pub fn postgres_worker_snooze_ack_receipt_binds_delay_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_snooze_delay_receipt_test(database_url)
  }
}

pub fn postgres_snooze_after_audited_replay_refunds_current_attempt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_snooze_after_replay_test(database_url)
  }
}

fn run_snooze_after_replay_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "snooze-replay-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "snooze-replay-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(delay) = worker.retry_delay(0)
  let ordinary_probe = process.new_subject()
  let queue_probe = process.new_subject()
  let assert Ok(ordinary) =
    worker.define(
      "worker.snooze.replay",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(ordinary_probe, value)
        Ok(int.to_string(value))
      },
    )
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      let release = process.new_subject()
      process.send(queue_probe, LongHandlerStarted(release))
      let _ = process.receive(release, within: 10_000)
      worker.WorkerSnoozed(delay, "audited replay snooze")
    })
  let assert Ok(workers) = registry.new("snooze-audited-replay")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) =
    postgres.submit(database, "snooze-audited-replay", snoozing, 8)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 9, attempt_owner = 'expired-snooze-owner', lease_expires_at = clock_timestamp(), attempt_count = 1, delivery_count = 1 WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "snooze-audited-replay",
      "on-call",
      "inspect the prior effect before authorizing a new delivery",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(before_claim) =
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
  before_claim.rows |> should.equal([#(1, 1)])

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(queue_probe, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })
  let assert Ok(during_attempt) =
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
  during_attempt.rows |> should.equal([#(2, 2)])

  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.receive(ordinary_probe, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.Pending(job.Scheduled)))
  let assert Ok(evidence) =
    pog.query(
      "SELECT attempt_count, max_attempts, delivery_count, snooze_count, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use max_attempts <- decode.field(1, decode.int)
      use delivery_count <- decode.field(2, decode.int)
      use snooze_count <- decode.field(3, decode.int)
      use committed <- decode.field(4, decode.string)
      use failure_cause <- decode.field(5, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(6, decode.int)
      decode.success(#(
        attempt_count,
        max_attempts,
        delivery_count,
        snooze_count,
        committed,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: connection)
  evidence.rows |> should.equal([#(1, 20, 2, 1, "scheduled", None, 32)])
  mark_database_test_executed(
    "worker-snooze-audited-replay-refunds-current-attempt",
  )
}

fn run_snooze_delay_receipt_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "snooze-delay-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "snooze-delay-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(delay) = worker.retry_delay(60_000)
  let assert Ok(ordinary) =
    worker.define(
      "worker.snooze.delay.receipt",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerSnoozed(delay, "receipt payload conflict")
    })
  let assert Ok(workers) = registry.new("snooze-delay-receipt")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) =
    postgres.submit(database, "snooze-delay-receipt", snoozing, 9)
  let attempt_owner = "snooze-delay-owner"
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "snooze-delay-receipt",
      workers,
      attempt_owner,
      30_000,
    )
  let proposal = worker.ExecutedSnoozed(60_000, "receipt payload conflict")
  attempt.acknowledge(
    database,
    "snooze-delay-receipt",
    attempt_owner,
    claimed,
    proposal,
  )
  |> should.equal(Ok(True))
  attempt.acknowledge(
    database,
    "snooze-delay-receipt",
    attempt_owner,
    claimed,
    worker.ExecutedSnoozed(70_000, "receipt payload conflict"),
  )
  |> should.equal(Error(postgres.QueueAckCommandConflict))
  attempt.acknowledge(
    database,
    "snooze-delay-receipt",
    attempt_owner,
    claimed,
    worker.ExecutedSnoozed(60_000, "changed proposal reason"),
  )
  |> should.equal(Error(postgres.QueueAckCommandConflict))
  let assert Ok(receipt) =
    pog.query(
      "SELECT state, attempt_count, snooze_count, receipt.committed_state, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_count <- decode.field(1, decode.int)
      use snooze_count <- decode.field(2, decode.int)
      use committed_state <- decode.field(3, decode.string)
      use fingerprint_bytes <- decode.field(4, decode.int)
      decode.success(#(
        state,
        attempt_count,
        snooze_count,
        committed_state,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  receipt.rows |> should.equal([#("scheduled", 0, 1, "scheduled", 32)])
  mark_database_test_executed("worker-snooze-delay-receipt-conflict-passed")
}
