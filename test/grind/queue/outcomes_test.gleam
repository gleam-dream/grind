import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{None, Some}
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/queue_signals.{RetryPolicyInvoked, WorkerInvoked}
import grind/support/worker_failure.{
  AccountMissing, decode_lookup_failure, encode_lookup_failure,
}
import grind/worker
import pog

pub fn postgres_queue_commits_typed_worker_success_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_postgres_queue_success_test(database_url)
  }
}

pub fn postgres_queue_rejects_output_codec_drift_before_invocation_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_output_codec_mismatch_test(database_url)
  }
}

pub fn postgres_queue_persists_typed_business_failure_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_business_failure_test(database_url)
  }
}

pub fn postgres_worker_discard_has_distinct_committed_outcome_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_discard_outcome_test(database_url)
  }
}

pub fn postgres_worker_cancel_has_distinct_committed_outcome_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_cancel_outcome_test(database_url)
  }
}

pub fn postgres_worker_uncertainty_is_reconcilable_without_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_uncertainty_test(database_url)
  }
}

fn run_business_failure_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("lookup-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("lookup-output-v1", json.string, decode.string)
  let assert Ok(error_codec) =
    worker.codec(
      "lookup-error-v1",
      encode_lookup_failure,
      decode_lookup_failure(),
    )
  let assert Ok(lookup) =
    worker.define_with_error_codec(
      "accounts.lookup.failure",
      "v1",
      input_codec,
      output_codec,
      error_codec,
      fn(account_id) { Error(AccountMissing(account_id)) },
    )
  let assert Ok(lookup) = worker.with_max_attempts(lookup, 1)
  let assert Ok(workers) = registry.new("business-failures")
  let assert Ok(workers) = registry.register(workers, lookup)
  let assert Ok(handle) =
    postgres.submit(database, "business-failures", lookup, 42)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(42), worker.BudgetExhausted)),
  )
  postgres.state(database, handle)
  |> should.equal(Ok(job.BusinessFailed))
  mark_database_test_executed("typed-business-failure-passed")
}

fn run_worker_discard_outcome_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("discard-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("discard-output-v1", json.string, decode.string)
  let assert Ok(ordinary) =
    worker.define("worker.discard", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let discarding =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerDiscarded("not needed")
    })
  let assert Ok(workers) = registry.new("worker-discard")
  let assert Ok(workers) = registry.register(workers, discarding)
  let assert Ok(handle) =
    postgres.submit(database, "worker-discard", discarding, 8)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Discarded))
  let assert Ok(receipt) =
    pog.query(
      "SELECT job.failure_description, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256), job.error IS NULL, job.error_version IS NULL FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use failure_description <- decode.field(0, decode.optional(decode.string))
      use committed_state <- decode.field(1, decode.string)
      use failure_cause <- decode.field(2, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(3, decode.int)
      use error_empty <- decode.field(4, decode.bool)
      use error_version_empty <- decode.field(5, decode.bool)
      decode.success(#(
        failure_description,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        error_empty,
        error_version_empty,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(Some("not needed"), "discarded", None, 32, True, True)] =
    receipt.rows
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.DiscardedWithReason("not needed")))
  mark_database_test_executed("worker-discard-distinct-outcome-passed")
}

fn run_worker_cancel_outcome_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("worker-cancel-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("worker-cancel-output-v1", json.string, decode.string)
  let assert Ok(ordinary) =
    worker.define("worker.cancel", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let cancelling =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerCancelled("worker declined the job")
    })
  let assert Ok(workers) = registry.new("worker-cancel")
  let assert Ok(workers) = registry.register(workers, cancelling)
  let assert Ok(handle) =
    postgres.submit(database, "worker-cancel", cancelling, 8)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))
  let assert Ok(receipt) =
    pog.query(
      "SELECT job.failure_description, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256), job.error IS NULL, job.error_version IS NULL FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use failure_description <- decode.field(0, decode.optional(decode.string))
      use committed_state <- decode.field(1, decode.string)
      use failure_cause <- decode.field(2, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(3, decode.int)
      use error_empty <- decode.field(4, decode.bool)
      use error_version_empty <- decode.field(5, decode.bool)
      decode.success(#(
        failure_description,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        error_empty,
        error_version_empty,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(Some("worker declined the job"), "cancelled", None, 32, True, True),
  ] = receipt.rows
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("worker declined the job")))
  mark_database_test_executed("worker-cancel-distinct-outcome-passed")
}

fn run_worker_uncertainty_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("uncertain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("uncertain-output-v1", json.string, decode.string)
  let effect_probe = process.new_subject()
  let policy_probe = process.new_subject()
  let assert Ok(ordinary) =
    worker.define(
      "worker.effect.uncertain",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(effect_probe, WorkerInvoked)
        Error(AccountMissing(value))
      },
    )
  let assert Ok(limited) = worker.with_max_attempts(ordinary, 2)
  let assert Ok(retry_delay) = worker.retry_delay(1000)
  let retry_policy =
    worker.retry_policy(fn(failure, context) {
      case failure {
        worker.BusinessFailure(_) -> {
          let worker.RetryContext(current_attempt:, ..) = context
          process.send(policy_probe, RetryPolicyInvoked(current_attempt, 9))
          worker.RetryAfter(retry_delay)
        }
      }
    })
  let with_policy = worker.with_retry_policy(limited, retry_policy)
  let uncertain =
    worker.with_queue_handler(with_policy, fn(_) {
      process.send(effect_probe, WorkerInvoked)
      worker.WorkerUncertain("external effect may have completed")
    })
  let assert Ok(workers) = registry.new("worker-uncertainty")
  let assert Ok(workers) = registry.register(workers, uncertain)
  let assert Ok(handle) =
    postgres.submit(database, "worker-uncertainty", uncertain, 17)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(effect_probe, within: 0) |> should.equal(Ok(WorkerInvoked))
  process.receive(policy_probe, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired("external effect may have completed")),
  )
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.AlreadyUncertain))
  let assert Ok(evidence) =
    pog.query(
      "SELECT job.attempt_count, job.max_attempts, job.delivery_count, job.attempt_id IS NOT NULL, job.attempt_owner IS NOT NULL, receipt.command_id, receipt.attempt_id, receipt.attempt_epoch, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256), job.error IS NULL, job.error_version IS NULL FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use max_attempts <- decode.field(1, decode.int)
      use delivery_count <- decode.field(2, decode.int)
      use has_attempt_id <- decode.field(3, decode.bool)
      use has_attempt_owner <- decode.field(4, decode.bool)
      use command_id <- decode.field(5, decode.string)
      use attempt_id <- decode.field(6, decode.int)
      use attempt_epoch <- decode.field(7, decode.int)
      use committed_state <- decode.field(8, decode.string)
      use failure_cause <- decode.field(9, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(10, decode.int)
      use no_error <- decode.field(11, decode.bool)
      use no_error_version <- decode.field(12, decode.bool)
      decode.success(#(
        attempt_count,
        max_attempts,
        delivery_count,
        has_attempt_id,
        has_attempt_owner,
        command_id,
        attempt_id,
        attempt_epoch,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        no_error,
        no_error_version,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(
      1,
      2,
      1,
      True,
      True,
      command_id,
      attempt_id,
      attempt_epoch,
      "uncertain",
      None,
      32,
      True,
      True,
    ),
  ] = evidence.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: receipt_command,
    attempt_id: receipt_attempt,
    attempt_epoch: receipt_epoch,
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at_unix_ms: _,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_command |> should.equal(command_id)
  receipt_attempt |> should.equal(attempt_id)
  receipt_epoch |> should.equal(attempt_epoch)
  receipt_state |> should.equal(job.Uncertain)
  receipt_cause |> should.equal(None)
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(effect_probe, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("worker-uncertainty-reconciliable-no-retry")
}

fn run_output_codec_mismatch_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("mismatch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("mismatch-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(effect) =
    worker.define("codec.drift", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("codec-drift")
  let assert Ok(workers) = registry.register(workers, effect)
  let assert Ok(handle) = postgres.submit(database, "codec-drift", effect, 9)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("mismatch-output-v2"))
    |> pog.parameter(pog.text("codec.drift"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: worker.OutputCodec,
        expected: "mismatch-output-v2",
        actual: "mismatch-output-v1",
      )),
    ),
  )
  postgres.state(database, handle)
  |> should.equal(Ok(job.ContractMismatch))
  process.receive(probe, within: 0)
  |> should.equal(Error(Nil))
  mark_database_test_executed("codec-contract-rejected")
}

fn run_postgres_queue_success_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("queue-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("queue-output-v1", json.string, decode.string)
  let assert Ok(increment) =
    worker.define("queue.increment", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value + 1))
    })
  let assert Ok(workers) = registry.new("default")
  let assert Ok(workers) = registry.register(workers, increment)
  let assert Ok(handle) = postgres.submit(database, "default", increment, 41)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.state(database, handle)
  |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("42")))
  mark_database_test_executed("committed-success-passed")
}
