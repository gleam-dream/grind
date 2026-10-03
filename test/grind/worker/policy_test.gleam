import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/result
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/worker
import grind/support/queue_signals.{
  type RetryPolicyProbe, RetryPolicyInvoked, WorkerInvoked,
}
import grind/support/worker_failure.{type LookupFailure, AccountMissing}

pub fn invocation_preserves_the_application_error_test() {
  let assert Ok(input_codec) =
    worker.codec("account-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "account-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(account_lookup) =
    worker.define(
      "accounts.lookup",
      "v1",
      input_codec,
      output_codec,
      fn(account_id) { Error(AccountMissing(account_id)) },
    )

  worker.invoke(account_lookup, 42)
  |> should.equal(Error(AccountMissing(42)))
}

pub fn queue_response_adapter_keeps_the_ordinary_worker_result_test() {
  let assert Ok(input_codec) =
    worker.codec(
      "queue-adapter-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "queue-adapter-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(delay) = worker.retry_delay(0)
  let probe = process.new_subject()
  let assert Ok(lookup) =
    worker.define(
      "accounts.queue-adapter",
      "v1",
      input_codec,
      output_codec,
      fn(account_id) {
        case account_id > 0 {
          True -> Ok("ordinary result")
          False -> Error(AccountMissing(account_id))
        }
      },
    )
  let queue_lookup =
    worker.with_queue_handler(lookup, fn(_) {
      process.send(probe, WorkerInvoked)
      worker.WorkerSnoozed(delay, "wait for account")
    })

  worker.invoke(queue_lookup, 42)
  |> should.equal(Ok("ordinary result"))
  worker.respond(queue_lookup, 42)
  |> should.equal(worker.WorkerSnoozed(delay, "wait for account"))
  process.receive(probe, within: 1000) |> should.equal(Ok(WorkerInvoked))
  worker.respond(lookup, 42)
  |> should.equal(worker.WorkerSucceeded("ordinary result"))
}

pub fn deterministic_default_retry_backoff_is_bounded_test() {
  worker.default_retry_delay_milliseconds(1) |> should.equal(15_000)
  worker.default_retry_delay_milliseconds(2) |> should.equal(30_000)
  worker.default_retry_delay_milliseconds(13) |> should.equal(61_440_000)
  worker.default_retry_delay_milliseconds(14) |> should.equal(86_400_000)
  worker.default_retry_delay_milliseconds(100) |> should.equal(86_400_000)
}

pub fn retry_settings_reject_invalid_values_before_resources_test() {
  let assert Ok(input_codec) =
    worker.codec(
      "retry-validation-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "retry-validation-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define("retry.validation", "v1", input_codec, output_codec, fn(v) {
      Ok(int.to_string(v))
    })

  worker.with_max_attempts(definition, 0)
  |> should.equal(Error(worker.AttemptLimitMustBePositive))
  let maximum_attempts = worker.max_attempts_supported_maximum()
  let assert Ok(_) = worker.with_max_attempts(definition, maximum_attempts)
  worker.with_max_attempts(definition, maximum_attempts + 1)
  |> should.equal(Error(worker.AttemptLimitExceedsSupportedMaximum))
  worker.retry_delay(-1)
  |> should.equal(Error(worker.RetryDelayMustNotBeNegative))
}

pub fn retry_delay_rejects_values_above_supported_precision_bound_test() {
  let maximum = worker.retry_delay_maximum_milliseconds()
  worker.retry_delay(maximum)
  |> result.map(worker.retry_delay_milliseconds)
  |> should.equal(Ok(maximum))
  worker.retry_delay(maximum + 1)
  |> should.equal(Error(worker.RetryDelayExceedsSupportedMaximum))
}

pub fn renewal_ticks_are_scoped_to_the_active_attempt_test() {
  queue.renewal_is_current(10, 2, 10, 2) |> should.equal(True)
  queue.renewal_is_current(10, 2, 11, 3) |> should.equal(False)
}

pub fn pending_shutdown_waiters_keep_the_original_deadline_test() {
  queue.next_shutdown_generation(7, True) |> should.equal(7)
  queue.next_shutdown_generation(7, False) |> should.equal(8)
}

pub fn attempt_resolution_exhausts_before_consulting_retry_policy_test() {
  let assert Ok(delay) = worker.retry_delay(500)
  let probe = process.new_subject()
  let definition = resolver_test_worker(probe, delay)
  let context = worker.RetryContext(2, 2, 3)

  worker.resolve_response(
    definition,
    worker.WorkerFailed(AccountMissing(42)),
    context,
  )
  |> should.equal(worker.ResolvedBusinessFailure(
    AccountMissing(42),
    worker.BudgetExhausted,
  ))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
}

pub fn snooze_is_not_a_retry_policy_decision_test() {
  let assert Ok(delay) = worker.retry_delay(250)
  let probe = process.new_subject()
  let definition = resolver_test_worker(probe, delay)
  let context = worker.RetryContext(2, 2, 3)

  worker.resolve_response(
    definition,
    worker.WorkerSnoozed(delay, "wait for account"),
    context,
  )
  |> should.equal(worker.ResolvedSnoozed(delay, "wait for account"))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
}

pub fn retry_policy_is_called_once_only_for_nonexhausted_business_failure_test() {
  let assert Ok(delay) = worker.retry_delay(500)
  let probe = process.new_subject()
  let definition = resolver_test_worker(probe, delay)
  let active_attempt = worker.RetryContext(1, 2, 0)
  let exhausted_attempt = worker.RetryContext(2, 2, 0)

  worker.resolve_response(
    definition,
    worker.WorkerSucceeded("ok"),
    active_attempt,
  )
  |> should.equal(worker.ResolvedSucceeded("ok"))
  worker.resolve_response(
    definition,
    worker.WorkerDiscarded("skip"),
    active_attempt,
  )
  |> should.equal(worker.ResolvedDiscarded("skip"))
  worker.resolve_response(
    definition,
    worker.WorkerCancelled("cancelled by worker"),
    active_attempt,
  )
  |> should.equal(worker.ResolvedCancelled("cancelled by worker"))
  worker.resolve_response(
    definition,
    worker.WorkerUncertain("effect may have happened"),
    active_attempt,
  )
  |> should.equal(worker.ResolvedUncertain("effect may have happened"))
  worker.resolve_response(
    definition,
    worker.WorkerFailed(AccountMissing(42)),
    exhausted_attempt,
  )
  |> should.equal(worker.ResolvedBusinessFailure(
    AccountMissing(42),
    worker.BudgetExhausted,
  ))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))

  worker.resolve_response(
    definition,
    worker.WorkerFailed(AccountMissing(43)),
    active_attempt,
  )
  |> should.equal(worker.ResolvedRetryable(AccountMissing(43), delay))
  process.receive(probe, within: 0)
  |> should.equal(Ok(RetryPolicyInvoked(1, 43)))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
}

fn resolver_test_worker(
  probe: process.Subject(RetryPolicyProbe),
  delay: worker.RetryDelay,
) -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input_codec) =
    worker.codec("resolver-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "resolver-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(base) =
    worker.define("worker.resolver", "v1", input_codec, output_codec, fn(_) {
      Error(AccountMissing(0))
    })
  let assert Ok(limited) = worker.with_max_attempts(base, 2)
  let policy =
    worker.retry_policy(fn(failure, context) {
      case failure {
        worker.BusinessFailure(AccountMissing(account_id)) -> {
          let worker.RetryContext(current_attempt:, ..) = context
          process.send(probe, RetryPolicyInvoked(current_attempt, account_id))
        }
      }
      worker.RetryAfter(delay)
    })
  worker.with_retry_policy(limited, policy)
}
