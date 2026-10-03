import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/option.{None, Some}
import gleam/otp/actor
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/consumer_hooks
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/queue_signals.{RetryPolicyInvoked}
import grind/support/queue_timing.{database_time_milliseconds}
import grind/support/worker_failure.{
  AccountMissing, decode_lookup_failure, encode_lookup_failure,
}
import one_shot
import pog

pub fn postgres_business_failure_is_scheduled_before_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_business_retry_test(database_url)
  }
}

pub fn postgres_default_retry_backoff_is_persisted_at_database_time_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_default_retry_backoff_test(database_url)
  }
}

pub fn postgres_retry_delay_maximum_commits_without_precision_loss_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_retry_delay_maximum_test(database_url)
  }
}

fn run_default_retry_backoff_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "default-retry-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "default-retry-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "worker.default.retry",
      "v1",
      input_codec,
      output_codec,
      fn(_) { Error(AccountMissing(71)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "default-retry", definition, 1)
  let assert Ok(workers) = registry.new("default-retry")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let connection = postgres.connection(database)
  let before_ack_us = database_time_microseconds(connection)

  queue.process_one(consumer) |> should.equal(Ok(True))

  let after_ack_us = database_time_microseconds(connection)
  postgres.state(database, handle) |> should.equal(Ok(job.Retryable))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.Pending(job.Retryable)))
  let assert Ok(evidence) =
    pog.query(
      "SELECT state, attempt_count, max_attempts, delivery_count, floor(extract(epoch FROM available_at) * 1000000)::bigint, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_count <- decode.field(1, decode.int)
      use max_attempts <- decode.field(2, decode.int)
      use delivery_count <- decode.field(3, decode.int)
      use available_at_us <- decode.field(4, decode.int)
      use committed <- decode.field(5, decode.string)
      use failure_cause <- decode.field(6, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(7, decode.int)
      decode.success(#(
        state,
        attempt_count,
        max_attempts,
        delivery_count,
        available_at_us,
        committed,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#("retryable", 1, 20, 1, available_at_us, "retryable", None, 32)] =
    evidence.rows
  should.be_true(available_at_us >= before_ack_us + 15_000_000)
  should.be_true(available_at_us <= after_ack_us + 15_000_000)
  queue.process_one(consumer) |> should.equal(Ok(False))
  mark_database_test_executed("default-retry-backoff-database-time-passed")
}

fn run_retry_delay_maximum_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "maximum-delay-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "maximum-delay-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let maximum_delay_ms = worker.retry_delay_maximum_milliseconds()
  let assert Ok(delay) = worker.retry_delay(maximum_delay_ms)
  let assert Ok(ordinary) =
    worker.define(
      "worker.maximum.delay",
      "v1",
      input_codec,
      output_codec,
      fn(_) { Ok("ordinary path unused") },
    )
  let definition =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerSnoozed(delay, "maximum supported delay")
    })
  let assert Ok(handle) =
    postgres.submit(database, "maximum-delay", definition, 1)
  let assert Ok(workers) = registry.new("maximum-delay")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let connection = postgres.connection(database)
  let before_ack_us = database_time_microseconds(connection)

  queue.process_one(consumer) |> should.equal(Ok(True))

  let after_ack_us = database_time_microseconds(connection)
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let assert Ok(evidence) =
    pog.query(
      "SELECT floor(extract(epoch FROM job.available_at) * 1000000)::bigint, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use available_at_us <- decode.field(0, decode.int)
      use committed <- decode.field(1, decode.string)
      use failure_cause <- decode.field(2, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(3, decode.int)
      decode.success(#(
        available_at_us,
        committed,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#(available_at_us, "scheduled", None, 32)] = evidence.rows
  let delay_us = maximum_delay_ms * 1000
  should.be_true(available_at_us >= before_ack_us + delay_us)
  should.be_true(available_at_us <= after_ack_us + delay_us)
  mark_database_test_executed("retry-delay-maximum-postgres-ack-passed")
}

fn database_time_microseconds(connection: pog.Connection) -> Int {
  let assert Ok(sample) =
    pog.query(
      "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000000)::bigint",
    )
    |> pog.returning({
      use microseconds <- decode.field(0, decode.int)
      decode.success(microseconds)
    })
    |> pog.execute(on: connection)
  let assert [microseconds] = sample.rows
  microseconds
}

pub fn postgres_retry_policy_can_decline_without_an_error_codec_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_retry_declined_without_error_codec_test(database_url)
  }
}

fn run_retry_declined_without_error_codec_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "retry-declined-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "retry-declined-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "worker.retry.declined",
      "v1",
      input_codec,
      output_codec,
      fn(_) { Error(AccountMissing(91)) },
    )
  let policy_calls = process.new_subject()
  let policy =
    worker.retry_policy(fn(failure, _) {
      case failure {
        worker.BusinessFailure(AccountMissing(account_id)) ->
          process.send(policy_calls, account_id)
      }
      worker.DoNotRetry
    })
  let assert Ok(limited) = worker.with_max_attempts(definition, 2)
  let limited = worker.with_retry_policy(limited, policy)
  let assert Ok(error_codec) =
    worker.codec(
      "retry-declined-error-v1",
      worker.infallible(encode_lookup_failure),
      decode_lookup_failure(),
    )
  let assert Ok(typed_definition) =
    worker.define_with_error_codec(
      "worker.retry.declined.typed",
      "v1",
      input_codec,
      output_codec,
      error_codec,
      fn(_) { Error(AccountMissing(92)) },
    )
  let assert Ok(typed_limited) = worker.with_max_attempts(typed_definition, 2)
  let typed_limited = worker.with_retry_policy(typed_limited, policy)
  let assert Ok(workers) = registry.new("retry-declined")
  let assert Ok(workers) = registry.register(workers, limited)
  let assert Ok(workers) = registry.register(workers, typed_limited)
  let assert Ok(handle) =
    postgres.submit(database, "retry-declined", limited, 3)
  let assert Ok(typed_handle) =
    postgres.submit(database, "retry-declined", typed_limited, 4)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(policy_calls, within: 0) |> should.equal(Ok(91))
  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.FailedOperationallyWithCause(
      "worker returned an application error",
      worker.RetryDeclined,
    )),
  )
  process.receive(policy_calls, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(policy_calls, within: 0) |> should.equal(Ok(92))
  postgres.outcome(database, typed_handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(92), worker.RetryDeclined)),
  )
  process.receive(policy_calls, within: 0) |> should.equal(Error(Nil))
  let assert Ok(committed_failure) =
    pog.query(
      "SELECT attempt_count, max_attempts, delivery_count, failure_cause, error IS NULL, error_version IS NULL FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use max_attempts <- decode.field(1, decode.int)
      use delivery_count <- decode.field(2, decode.int)
      use cause <- decode.field(3, decode.optional(decode.string))
      use no_error <- decode.field(4, decode.bool)
      use no_error_version <- decode.field(5, decode.bool)
      decode.success(#(
        attempt_count,
        max_attempts,
        delivery_count,
        cause,
        no_error,
        no_error_version,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  committed_failure.rows
  |> should.equal([#(1, 2, 1, Some("retry_declined"), True, True)])
  let assert Ok(typed_failure) =
    pog.query(
      "SELECT failure_cause, error IS NOT NULL, error_version FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(typed_handle)))
    |> pog.returning({
      use cause <- decode.field(0, decode.optional(decode.string))
      use has_error <- decode.field(1, decode.bool)
      use error_version <- decode.field(2, decode.optional(decode.string))
      decode.success(#(cause, has_error, error_version))
    })
    |> pog.execute(on: postgres.connection(database))
  typed_failure.rows
  |> should.equal([
    #(Some("retry_declined"), True, Some("retry-declined-error-v1")),
  ])
  mark_database_test_executed("worker-retry-declined-without-error-codec")
}

fn run_business_retry_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "business-retry-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "business-retry-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(error_codec) =
    worker.codec(
      "business-retry-error-v1",
      worker.infallible(encode_lookup_failure),
      decode_lookup_failure(),
    )
  let assert Ok(delay) = worker.retry_delay(60_000)
  let assert Ok(definition) =
    worker.define_with_error_codec(
      "worker.business.retry",
      "v1",
      input_codec,
      output_codec,
      error_codec,
      fn(_) { Error(AccountMissing(42)) },
    )
  let retry_probe = process.new_subject()
  let policy =
    worker.retry_policy(fn(failure, context) {
      case failure {
        worker.BusinessFailure(AccountMissing(account_id)) -> {
          let worker.RetryContext(current_attempt:, ..) = context
          process.send(
            retry_probe,
            RetryPolicyInvoked(current_attempt, account_id),
          )
          worker.RetryAfter(delay)
        }
      }
    })
  let assert Ok(retrying) = worker.with_max_attempts(definition, 2)
  let retrying = worker.with_retry_policy(retrying, policy)
  let assert Ok(workers) = registry.new("business-retry")
  let assert Ok(workers) = registry.register(workers, retrying)
  let assert Ok(handle) =
    postgres.submit(database, "business-retry", retrying, 17)
  let fail_next_worker_start = one_shot.new()
  let hooks =
    consumer_hooks.Hooks(
      before_worker_start: fn() {
        case one_shot.take(fail_next_worker_start) {
          True -> Error("injected start failure")
          False -> Ok(Nil)
        }
      },
      after_worker_start: fn(_pid) { Nil },
    )
  let assert Ok(consumer) =
    queue.start_with_hooks(database, workers, manual_policy(), hooks)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Retryable))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.Pending(job.Retryable)))
  process.receive(retry_probe, within: 0)
  |> should.equal(Ok(RetryPolicyInvoked(1, 42)))
  let assert Ok(first_attempt) =
    pog.query(
      "SELECT state, attempt_count, max_attempts, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_count <- decode.field(1, decode.int)
      use max_attempts <- decode.field(2, decode.int)
      use delivery_count <- decode.field(3, decode.int)
      decode.success(#(state, attempt_count, max_attempts, delivery_count))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#("retryable", 1, 2, 1)] = first_attempt.rows
  let before_due = database_time_milliseconds(postgres.connection(database))
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(retry_probe, within: 0) |> should.equal(Error(Nil))

  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: postgres.connection(database))
  one_shot.arm(fail_next_worker_start)
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueWorkerStartFailed(actor.InitFailed("injected start failure")),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Retryable))
  let assert Ok(after_unstarted_retry) =
    pog.query(
      "SELECT attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use delivery_count <- decode.field(1, decode.int)
      decode.success(#(attempt_count, delivery_count))
    })
    |> pog.execute(on: postgres.connection(database))
  after_unstarted_retry.rows |> should.equal([#(1, 2)])
  process.receive(retry_probe, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(42), worker.BudgetExhausted)),
  )
  process.receive(retry_probe, within: 0) |> should.equal(Error(Nil))
  let assert Ok(attempt_receipts) =
    pog.query(
      "SELECT attempt_id, command_id, committed_state, failure_cause, octet_length(proposal_sha256) FROM grind_job_acknowledgements WHERE job_id = $1 ORDER BY attempt_id",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use command_id <- decode.field(1, decode.string)
      use committed <- decode.field(2, decode.string)
      use failure_cause <- decode.field(3, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(4, decode.int)
      decode.success(#(
        attempt_id,
        command_id,
        committed,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(first_attempt, first_command_id, "retryable", None, 32),
    #(second_attempt, _, "business_failed", Some("budget_exhausted"), 32),
  ] = attempt_receipts.rows
  should.be_true(first_attempt < second_attempt)
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: reconciled_command,
    attempt_id: reconciled_attempt,
    committed_state: reconciled_state,
    business_failure_cause: reconciled_cause,
    ..,
  )) = postgres.reconcile_acknowledgement(database, handle, first_command_id)
  reconciled_command |> should.equal(first_command_id)
  reconciled_attempt |> should.equal(first_attempt)
  reconciled_state |> should.equal(job.Retryable)
  reconciled_cause |> should.equal(None)
  let assert Ok(counters) =
    pog.query(
      "SELECT attempt_count, max_attempts, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use max_attempts <- decode.field(1, decode.int)
      use delivery_count <- decode.field(2, decode.int)
      decode.success(#(attempt_count, max_attempts, delivery_count))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(2, 2, 3)] = counters.rows
  should.be_true(before_due > 0)
  mark_database_test_executed("worker-retry-first-attempt-scheduled")
}
