//// A worker defined with an error codec registers an error-contract version
//// at admission. Every later write must keep that version on the job row:
//// the claim and `bind_handle` compare it with the registered worker, so a
//// write that clears it turns a snoozed or replayed job into a contract
//// mismatch and makes a finished job impossible to re-bind.

import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, Some}
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/worker_failure.{
  type LookupFailure, AccountMissing, decode_lookup_failure,
  encode_lookup_failure,
}
import grind/worker
import one_shot
import pog

const error_contract = "error-contract-error-v1"

pub fn postgres_snoozed_job_with_error_codec_runs_again_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_snooze_then_run_test(database_url)
  }
}

pub fn postgres_succeeded_job_with_error_codec_binds_handle_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_success_then_bind_test(database_url)
  }
}

pub fn postgres_failed_job_with_error_codec_binds_handle_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_failure_then_bind_test(database_url)
  }
}

pub fn postgres_discarded_and_cancelled_jobs_with_error_codec_bind_handle_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_discard_and_cancel_then_bind_test(database_url)
  }
}

pub fn postgres_replayed_job_with_error_codec_runs_again_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_replay_then_run_test(database_url)
  }
}

fn typed_worker(
  id: String,
  handler: fn(Int) -> worker.WorkerResponse(String, LookupFailure),
) -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input_codec) =
    worker.codec(
      "error-contract-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "error-contract-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(error_codec) =
    worker.codec(
      error_contract,
      worker.infallible(encode_lookup_failure),
      decode_lookup_failure(),
    )
  let assert Ok(definition) =
    worker.define_with_error_codec(
      id,
      "v1",
      input_codec,
      output_codec,
      error_codec,
      fn(_) { Error(AccountMissing(0)) },
    )
  worker.with_queue_handler(definition, handler)
}

fn with_consumer(
  database_url: String,
  queue_name: String,
  definitions: List(worker.Worker(Int, String, LookupFailure)),
  next: fn(postgres.Database, queue.Consumer) -> Nil,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(workers) = registry.new(queue_name)
  let workers =
    definitions
    |> list.fold(workers, fn(workers, definition) {
      let assert Ok(workers) = registry.register(workers, definition)
      workers
    })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  next(database, consumer)
}

fn stored_error_version(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
) -> Option(String) {
  let assert Ok(returned) =
    pog.query("SELECT error_version FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use version <- decode.field(0, decode.optional(decode.string))
      decode.success(version)
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [version] = returned.rows
  version
}

fn run_snooze_then_run_test(database_url: String) -> Nil {
  let assert Ok(no_delay) = worker.retry_delay(0)
  let snooze_once = one_shot.armed()
  let definition =
    typed_worker("error-contract.snooze", fn(value) {
      case one_shot.take(snooze_once) {
        True -> worker.WorkerSnoozed(no_delay, "waiting for upstream")
        False ->
          worker.WorkerSucceeded("ran after snooze " <> int.to_string(value))
      }
    })
  use database, consumer <- with_consumer(
    database_url,
    "error-contract-snooze",
    [definition],
  )
  let assert Ok(handle) =
    postgres.submit(database, "error-contract-snooze", definition, 1)

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  stored_error_version(database, handle) |> should.equal(Some(error_contract))

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("ran after snooze 1")))
  stored_error_version(database, handle) |> should.equal(Some(error_contract))
  let assert Ok(bound) =
    postgres.bind_handle(database, definition, job.id_value(handle))
  postgres.outcome(database, bound)
  |> should.equal(Ok(job.SucceededWith("ran after snooze 1")))
  mark_database_test_executed("error-contract-snooze-then-run-passed")
}

fn run_success_then_bind_test(database_url: String) -> Nil {
  let definition =
    typed_worker("error-contract.success", fn(value) {
      worker.WorkerSucceeded("done " <> int.to_string(value))
    })
  use database, consumer <- with_consumer(
    database_url,
    "error-contract-success",
    [definition],
  )
  let assert Ok(handle) =
    postgres.submit(database, "error-contract-success", definition, 2)

  queue.process_one(consumer) |> should.equal(Ok(True))
  stored_error_version(database, handle) |> should.equal(Some(error_contract))
  let assert Ok(bound) =
    postgres.bind_handle(database, definition, job.id_value(handle))
  postgres.outcome(database, bound)
  |> should.equal(Ok(job.SucceededWith("done 2")))
  mark_database_test_executed("error-contract-success-then-bind-passed")
}

fn run_failure_then_bind_test(database_url: String) -> Nil {
  let typed =
    typed_worker("error-contract.failure", fn(value) {
      worker.WorkerFailed(AccountMissing(value))
    })
  let assert Ok(definition) = worker.with_max_attempts(typed, 1)
  use database, consumer <- with_consumer(
    database_url,
    "error-contract-failure",
    [definition],
  )
  let assert Ok(handle) =
    postgres.submit(database, "error-contract-failure", definition, 3)

  queue.process_one(consumer) |> should.equal(Ok(True))
  stored_error_version(database, handle) |> should.equal(Some(error_contract))
  let assert Ok(bound) =
    postgres.bind_handle(database, definition, job.id_value(handle))
  postgres.outcome(database, bound)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(3), worker.BudgetExhausted)),
  )
  mark_database_test_executed("error-contract-failure-then-bind-passed")
}

fn run_discard_and_cancel_then_bind_test(database_url: String) -> Nil {
  let discarding =
    typed_worker("error-contract.discard", fn(_) {
      worker.WorkerDiscarded("not worth retrying")
    })
  use database, consumer <- with_consumer(
    database_url,
    "error-contract-terminal",
    [discarding],
  )
  let assert Ok(discarded) =
    postgres.submit(database, "error-contract-terminal", discarding, 4)
  queue.process_one(consumer) |> should.equal(Ok(True))
  stored_error_version(database, discarded)
  |> should.equal(Some(error_contract))
  let assert Ok(bound_discarded) =
    postgres.bind_handle(database, discarding, job.id_value(discarded))
  postgres.outcome(database, bound_discarded)
  |> should.equal(Ok(job.DiscardedWithReason("not worth retrying")))

  let assert Ok(cancelled) =
    postgres.submit(database, "error-contract-terminal", discarding, 5)
  postgres.cancel(database, cancelled)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  stored_error_version(database, cancelled)
  |> should.equal(Some(error_contract))
  let assert Ok(bound_cancelled) =
    postgres.bind_handle(database, discarding, job.id_value(cancelled))
  postgres.outcome(database, bound_cancelled)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  mark_database_test_executed("error-contract-discard-cancel-then-bind-passed")
}

fn run_replay_then_run_test(database_url: String) -> Nil {
  let uncertain_once = one_shot.armed()
  let definition =
    typed_worker("error-contract.replay", fn(value) {
      case one_shot.take(uncertain_once) {
        True -> worker.WorkerUncertain("upstream reply lost")
        False -> worker.WorkerSucceeded("replayed " <> int.to_string(value))
      }
    })
  use database, consumer <- with_consumer(
    database_url,
    "error-contract-replay",
    [definition],
  )
  let assert Ok(handle) =
    postgres.submit(database, "error-contract-replay", definition, 6)

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "error-contract-replay",
      "on-call",
      "upstream confirmed nothing was applied",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  stored_error_version(database, handle) |> should.equal(Some(error_contract))

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("replayed 6")))
  mark_database_test_executed("error-contract-replay-then-run-passed")
}
