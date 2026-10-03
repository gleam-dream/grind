//// A codec's encoder may reject a value (a json_blueprint codec with an
//// `integer_between` refinement, say). A rejected input fails every submit
//// path with `submission.InvalidInput` and writes nothing. A rejected
//// output or error, after the handler ran, ends the job as
//// `job.RuntimeFailed` on its first attempt, with no retry. A rejected
//// operator-confirmed value fails `resolve_uncertain` before any write.

import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/submission
import grind/internal/unique
import grind/internal/worker
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import grind/support/lease_queries
import grind/support/worker_failure.{
  type LookupFailure, AccountMissing, decode_lookup_failure,
  encode_lookup_failure,
}
import pog

pub fn postgres_rejected_input_writes_nothing_on_every_submit_path_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_rejected_input_test(database_url)
  }
}

pub fn postgres_rejected_output_ends_runtime_failed_without_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_rejected_output_test(database_url)
  }
}

pub fn postgres_rejected_error_ends_runtime_failed_without_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_rejected_error_test(database_url)
  }
}

pub fn postgres_rejected_resolution_value_writes_nothing_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_rejected_resolution_test(database_url)
  }
}

/// Accepts only non-negative integers.
fn non_negative(value: Int) -> Result(json.Json, String) {
  case value >= 0 {
    True -> Ok(json.int(value))
    False -> Error("must be at least 0")
  }
}

fn short_text(value: String) -> Result(json.Json, String) {
  case value {
    "too long" -> Error("longer than 4 bytes")
    _ -> Ok(json.string(value))
  }
}

fn checked_error(error: LookupFailure) -> Result(json.Json, String) {
  case error {
    AccountMissing(id) if id < 0 -> Error("account id must be at least 0")
    _ -> Ok(encode_lookup_failure(error))
  }
}

fn checked_worker(
  id: String,
  handler: fn(Int) -> worker.WorkerResponse(String, LookupFailure),
) -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input) =
    worker.codec("encoder-rejection-input-v1", non_negative, decode.int)
  let assert Ok(output) =
    worker.codec("encoder-rejection-output-v1", short_text, decode.string)
  let assert Ok(error) =
    worker.codec(
      "encoder-rejection-error-v1",
      checked_error,
      decode_lookup_failure(),
    )
  let assert Ok(definition) =
    worker.define_with_error_codec(id, "v1", input, output, error, fn(_) {
      Error(AccountMissing(0))
    })
  let assert Ok(definition) = worker.with_max_attempts(definition, 5)
  worker.with_queue_handler(definition, handler)
}

fn with_database(database_url: String, next: fn(postgres.Database) -> Nil) {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  next(database)
}

fn with_consumer(
  database: postgres.Database,
  queue_name: String,
  definition: worker.Worker(Int, String, LookupFailure),
  next: fn(queue.Consumer) -> Nil,
) -> Nil {
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  next(consumer)
}

fn count(database: postgres.Database, sql: String, value: String) -> Int {
  let assert Ok(returned) =
    pog.query(sql)
    |> pog.parameter(pog.text(value))
    |> pog.returning(decode.at([0], decode.int))
    |> pog.execute(on: postgres.connection(database))
  let assert [rows] = returned.rows
  rows
}

fn jobs_for(database: postgres.Database, worker_id: String) -> Int {
  count(
    database,
    "SELECT count(*)::bigint FROM grind_jobs WHERE worker_id = $1",
    worker_id,
  )
}

fn receipts_for(database: postgres.Database, worker_id: String) -> Int {
  count(
    database,
    "SELECT count(*)::bigint FROM grind_unique_submissions WHERE worker_id = $1",
    worker_id,
  )
}

fn rejected() -> Result(value, submission.SubmitError(input, output, error)) {
  Error(submission.InvalidInput("must be at least 0"))
}

fn run_rejected_input_test(database_url: String) -> Nil {
  use database <- with_database(database_url)
  let suffix = int.to_string(unique_test_run_id())
  let worker_id = "encoder-rejection.input." <> suffix
  let queue_name = "encoder-rejection-input-" <> suffix
  let definition =
    checked_worker(worker_id, fn(_) { worker.WorkerSucceeded("ok") })

  postgres.submit(database, queue_name, definition, -1)
  |> should.equal(rejected())

  let assert Ok(later) = job.available_at(4_102_444_800_000)
  postgres.submit_at(database, queue_name, definition, -1, later)
  |> should.equal(rejected())

  let assert Ok(id) = submission.submission_id("encoder-rejection-" <> suffix)
  postgres.submit_with_id(
    database,
    queue_name,
    id,
    definition,
    -1,
    submission.Immediately,
  )
  |> should.equal(rejected())

  let full_input =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      unique.while_retained(),
      unique.Incomplete,
    )
  postgres.submit_unique(
    database,
    queue_name,
    id,
    definition,
    -1,
    submission.Immediately,
    full_input,
    unique.KeepExisting,
  )
  |> should.equal(rejected())

  // The input itself encodes, but the selected key's codec rejects the
  // projection; the reason names the key.
  let assert Ok(key_codec) =
    worker.codec("encoder-rejection-key-v1", non_negative, decode.int)
  let assert Ok(key) =
    unique.selected("negated", fn(value) { 0 - value }, key_codec)
  let selected =
    unique.policy(
      key,
      unique.WithinQueue,
      unique.while_retained(),
      unique.Incomplete,
    )
  postgres.submit_unique(
    database,
    queue_name,
    id,
    definition,
    3,
    submission.Immediately,
    selected,
    unique.KeepExisting,
  )
  |> should.equal(
    Error(submission.InvalidInput("unique key negated: must be at least 0")),
  )

  jobs_for(database, worker_id) |> should.equal(0)
  receipts_for(database, worker_id) |> should.equal(0)

  // The same id is still free: nothing was recorded for it.
  let assert Ok(submission.Inserted(_)) =
    postgres.submit_with_id(
      database,
      queue_name,
      id,
      definition,
      0,
      submission.Immediately,
    )
  jobs_for(database, worker_id) |> should.equal(1)
  mark_database_test_executed("encoder-rejected-input-writes-nothing-passed")
}

fn run_rejected_output_test(database_url: String) -> Nil {
  use database <- with_database(database_url)
  let suffix = int.to_string(unique_test_run_id())
  let queue_name = "encoder-rejection-output-" <> suffix
  let definition =
    checked_worker("encoder-rejection.output." <> suffix, fn(_) {
      worker.WorkerSucceeded("too long")
    })
  use consumer <- with_consumer(database, queue_name, definition)
  let assert Ok(handle) = postgres.submit(database, queue_name, definition, 1)

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.RuntimeFailed))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.RuntimeFailedWith(
      "output codec rejected the handler's output: longer than 4 bytes",
    )),
  )
  lease_queries.attempt_count_for(
    postgres.connection(database),
    job.id_value(handle),
  )
  |> should.equal(Ok(1))
  queue.process_one(consumer) |> should.equal(Ok(False))
  mark_database_test_executed("encoder-rejected-output-runtime-failed-passed")
}

fn run_rejected_error_test(database_url: String) -> Nil {
  use database <- with_database(database_url)
  let suffix = int.to_string(unique_test_run_id())
  let queue_name = "encoder-rejection-error-" <> suffix
  let definition =
    checked_worker("encoder-rejection.error." <> suffix, fn(_) {
      worker.WorkerFailed(AccountMissing(-1))
    })
  use consumer <- with_consumer(database, queue_name, definition)
  let assert Ok(handle) = postgres.submit(database, queue_name, definition, 1)

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.RuntimeFailed))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.RuntimeFailedWith(
      "error codec rejected the handler's error: account id must be at least 0",
    )),
  )
  lease_queries.attempt_count_for(
    postgres.connection(database),
    job.id_value(handle),
  )
  |> should.equal(Ok(1))
  queue.process_one(consumer) |> should.equal(Ok(False))
  mark_database_test_executed("encoder-rejected-error-runtime-failed-passed")
}

fn run_rejected_resolution_test(database_url: String) -> Nil {
  use database <- with_database(database_url)
  let suffix = int.to_string(unique_test_run_id())
  let queue_name = "encoder-rejection-resolution-" <> suffix
  let definition =
    checked_worker("encoder-rejection.resolution." <> suffix, fn(_) {
      worker.WorkerUncertain("upstream reply lost")
    })
  use consumer <- with_consumer(database, queue_name, definition)
  let assert Ok(handle) = postgres.submit(database, queue_name, definition, 1)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "encoder-rejection-success-" <> suffix,
      "on-call",
      "upstream confirmed the write",
      postgres.ConfirmSuccess("too long"),
    ),
  )
  |> should.equal(Error(postgres.ResolutionInvalidValue("longer than 4 bytes")))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "encoder-rejection-failure-" <> suffix,
      "on-call",
      "upstream confirmed the failure",
      postgres.ConfirmBusinessFailure(AccountMissing(-1)),
    ),
  )
  |> should.equal(
    Error(postgres.ResolutionInvalidValue("account id must be at least 0")),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "encoder-rejection-success-" <> suffix,
      "on-call",
      "upstream confirmed the write",
      postgres.ConfirmSuccess("done"),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  mark_database_test_executed(
    "encoder-rejected-resolution-writes-nothing-passed",
  )
}
