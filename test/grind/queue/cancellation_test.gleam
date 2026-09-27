import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{None}
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/concurrency.{LongHandlerStarted, ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/queue_signals.{WorkerInvoked}
import grind/support/worker_failure.{AccountMissing}
import grind/worker
import pog

pub fn postgres_cancel_queued_job_before_execution_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancel_before_execution_test(database_url)
  }
}

pub fn postgres_cancel_after_completion_preserves_result_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancel_after_completion_test(database_url)
  }
}

pub fn postgres_cancel_running_worker_overrides_proposal_on_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancel_running_ack_test(database_url)
  }
}

pub fn postgres_cancelled_expired_attempt_is_quarantined_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancelled_expired_attempt_test(database_url)
  }
}

pub fn postgres_cancel_running_uncertain_proposal_is_preserved_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancel_running_uncertain_test(database_url)
  }
}

fn run_cancel_before_execution_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-before-run-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-before-run-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "worker.cancel.before.run",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(probe, WorkerInvoked)
        Ok(int.to_string(value))
      },
    )
  let assert Ok(workers) = registry.new("cancel-before-run")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cancel-before-run", definition, 5)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.AlreadyCancelled))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  let assert Ok(accounting) =
    pog.query(
      "SELECT attempt_count, delivery_count, cancel_requested_at IS NULL, (SELECT count(*) = 0 FROM grind_job_acknowledgements WHERE job_id = $1) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use delivery_count <- decode.field(1, decode.int)
      use no_cancel_request <- decode.field(2, decode.bool)
      use no_ack <- decode.field(3, decode.bool)
      decode.success(#(attempt_count, delivery_count, no_cancel_request, no_ack))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(0, 0, True, True)] = accounting.rows
  mark_database_test_executed("cancel-before-run-committed")
}

fn run_cancel_after_completion_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-complete-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-complete-output-v1", json.string, decode.string)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "worker.cancel.complete",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(invoked, WorkerInvoked)
        Ok("done-" <> int.to_string(value))
      },
    )
  let assert Ok(workers) = registry.new("cancel-after-completion")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cancel-after-completion", definition, 12)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.AlreadyFinished(job.Succeeded)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("done-12")))
  mark_database_test_executed("cancel-after-completion-preserved")
}

fn run_cancel_running_ack_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-running-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-running-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "worker.cancel.running",
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
  let assert Ok(workers) = registry.new("cancel-running")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cancel-running", definition, 9)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })

  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  let assert Ok(receipt) =
    pog.query(
      "SELECT command_id, attempt_id, attempt_epoch, committed_state, failure_cause, octet_length(proposal_sha256) FROM grind_job_acknowledgements WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use command_id <- decode.field(0, decode.string)
      use attempt_id <- decode.field(1, decode.int)
      use attempt_epoch <- decode.field(2, decode.int)
      use committed_state <- decode.field(3, decode.string)
      use failure_cause <- decode.field(4, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(5, decode.int)
      decode.success(#(
        command_id,
        attempt_id,
        attempt_epoch,
        committed_state,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(command_id, attempt_id, attempt_epoch, "cancelled", None, 32)] =
    receipt.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: receipt_command,
    attempt_id: receipt_attempt,
    attempt_epoch: receipt_epoch,
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at_unix_ms: committed_at,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_command |> should.equal(command_id)
  receipt_attempt |> should.equal(attempt_id)
  receipt_epoch |> should.equal(attempt_epoch)
  receipt_state |> should.equal(job.Cancelled)
  receipt_cause |> should.equal(None)
  should.be_true(committed_at > 0)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET failure_description = 'changed current job diagnostic' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: postgres.connection(database))
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(
    Ok(postgres.AcknowledgementReceipt(
      command_id: receipt_command,
      attempt_id: receipt_attempt,
      attempt_epoch: receipt_epoch,
      committed_state: receipt_state,
      business_failure_cause: receipt_cause,
      committed_at_unix_ms: committed_at,
    )),
  )
  mark_database_test_executed("cancel-running-ack-wins")
}

fn run_cancel_running_uncertain_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-uncertain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-uncertain-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(ordinary) =
    worker.define(
      "worker.cancel.uncertain",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("ordinary-" <> int.to_string(value)) },
    )
  let definition =
    worker.with_queue_handler(ordinary, fn(value) {
      let release = process.new_subject()
      process.send(started, LongHandlerStarted(release))
      let _ = process.receive(release, within: 10_000)
      case value {
        13 ->
          worker.WorkerUncertain("effect may have happened before cancellation")
        14 -> worker.WorkerCancelled("worker proposed its own cancellation")
        _ -> worker.WorkerUncertain("unexpected cancellation-test input")
      }
    })
  let assert Ok(workers) = registry.new("cancel-running-uncertain")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cancel-running-uncertain", definition, 13)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  let assert Ok(evidence) =
    pog.query(
      "SELECT receipt.command_id, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256), job.cancel_requested_at IS NULL, job.uncertain_at IS NULL FROM grind_job_acknowledgements AS receipt JOIN grind_jobs AS job ON job.id = receipt.job_id WHERE receipt.job_id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use command_id <- decode.field(0, decode.string)
      use committed_state <- decode.field(1, decode.string)
      use failure_cause <- decode.field(2, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(3, decode.int)
      use request_cleared <- decode.field(4, decode.bool)
      use uncertainty_cleared <- decode.field(5, decode.bool)
      decode.success(#(
        command_id,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        request_cleared,
        uncertainty_cleared,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(command_id, "cancelled", None, 32, True, True)] = evidence.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at_unix_ms: _,
    ..,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_state |> should.equal(job.Cancelled)
  receipt_cause |> should.equal(None)

  let assert Ok(worker_cancel_handle) =
    postgres.submit(database, "cancel-running-uncertain", definition, 14)
  let worker_cancel_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(worker_cancel_reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(worker_cancel_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(worker_cancel_release, ReleaseAttempt)
  })
  postgres.cancel(database, worker_cancel_handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(worker_cancel_release, ReleaseAttempt)
  process.receive(worker_cancel_reply, within: 5000)
  |> should.equal(Ok(Ok(True)))
  postgres.outcome(database, worker_cancel_handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  let assert Ok(worker_cancel_command) =
    pog.query(
      "SELECT command_id FROM grind_job_acknowledgements WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(worker_cancel_handle)))
    |> pog.returning({
      use command_id <- decode.field(0, decode.string)
      decode.success(command_id)
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [worker_cancel_command_id] = worker_cancel_command.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    committed_state: worker_cancel_state,
    business_failure_cause: worker_cancel_cause,
    ..,
  )) =
    postgres.reconcile_acknowledgement(
      database,
      worker_cancel_handle,
      worker_cancel_command_id,
    )
  worker_cancel_state |> should.equal(job.Cancelled)
  worker_cancel_cause |> should.equal(None)
  mark_database_test_executed("cancel-running-worker-cancel-compact-receipt")
  mark_database_test_executed("cancel-running-uncertain-compact-receipt")
}

fn run_cancelled_expired_attempt_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-expired-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-expired-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(ordinary) =
    worker.define(
      "worker.cancel.expired.replay",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let definition =
    worker.with_queue_handler(ordinary, fn(value) {
      let release = process.new_subject()
      process.send(started, LongHandlerStarted(release))
      let _ = process.receive(release, within: 10_000)
      worker.WorkerSucceeded(
        "effect-completed-after-cancel-" <> int.to_string(value),
      )
    })
  let queue_name = "cancel-expired"
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) = postgres.submit(database, queue_name, definition, 11)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing' AND cancel_requested_at IS NOT NULL",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: postgres.connection(database))

  process.send(release, ReleaseAttempt)
  let ack_result = process.receive(reply, within: 5000)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
    worker.ExecutedSuccess(_, encoded_output),
    postgres.AckLeaseExpired(_, _, _),
  )))) = ack_result
  encoded_output
  |> should.equal("\"effect-completed-after-cancel-11\"")
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired after cancellation request; prior effect unknown",
    )),
  )
  let assert Ok(row) =
    pog.query(
      "SELECT state, attempt_id, attempt_epoch, attempt_owner, attempt_count, delivery_count, cancel_requested_at IS NOT NULL, failure_description, (SELECT count(*) = 0 FROM grind_job_acknowledgements WHERE job_id = $1) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_id <- decode.field(1, decode.int)
      use attempt_epoch <- decode.field(2, decode.int)
      use attempt_owner <- decode.field(3, decode.string)
      use attempt_count <- decode.field(4, decode.int)
      use delivery_count <- decode.field(5, decode.int)
      use cancel_requested <- decode.field(6, decode.bool)
      use description <- decode.field(7, decode.string)
      use no_ack_receipt <- decode.field(8, decode.bool)
      decode.success(#(
        state,
        attempt_id,
        attempt_epoch,
        attempt_owner,
        attempt_count,
        delivery_count,
        cancel_requested,
        description,
        no_ack_receipt,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(
      "uncertain",
      attempt_id,
      attempt_epoch,
      attempt_owner,
      1,
      1,
      True,
      description,
      True,
    ),
  ] = row.rows
  attempt_id |> should.not_equal(0)
  attempt_epoch |> should.not_equal(0)
  attempt_owner |> should.not_equal("")
  description
  |> should.equal("expired after cancellation request; prior effect unknown")
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "cancel-pending-replay",
      "on-call",
      "the cancellation request blocks replay until effect evidence is reviewed",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Error(postgres.ResolutionCancellationPending))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  let assert Ok(no_audit) =
    pog.query(
      "SELECT count(*) FROM grind_job_resolutions WHERE job_id = $1 AND resolution_id = $2",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.parameter(pog.text("cancel-pending-replay"))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [0] = no_audit.rows
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "cancel-pending-confirmed",
      "on-call",
      "external effect evidence confirms the known result",
      postgres.ConfirmSuccess("confirmed-without-replay"),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("confirmed-without-replay")))
  mark_database_test_executed("cancel-pending-expiry-quarantined")
}
