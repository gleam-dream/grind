import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/time/timestamp
import gleeunit
import gleeunit/should
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/submission
import grind/unique
import grind/worker
import grind_consumer.{
  type PaymentError, type PaymentRequest, PaymentRejected, PaymentRequest,
}
import sinal

pub fn main() -> Nil {
  gleeunit.main()
}

/// The current wall-clock time as Unix milliseconds, for building an
/// application-owned `job.AvailableAt` value. Public `gleam_time` only, no
/// direct Erlang FFI.
fn now_unix_ms() -> Int {
  let #(seconds, nanoseconds) =
    timestamp.system_time() |> timestamp.to_unix_seconds_and_nanoseconds
  seconds * 1000 + nanoseconds / 1_000_000
}

/// The manually-polled `ValidatedPolicy` every consumer in this suite that
/// does not need automatic polling starts under: no `Poll` timer of its own.
fn manual_policy() -> queue.ValidatedPolicy {
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_manual_polling
    |> queue.validate_policy
  policy
}

type Probe {
  ChargeInvoked
  ReportInvoked
}

/// A running handler's own barrier: it reports it has started (handing back
/// a release subject) and then blocks until the test releases it.
type Barrier {
  BarrierStarted(process.Subject(BarrierRelease))
}

type BarrierRelease {
  BarrierRelease
}

@external(erlang, "consumer_test_env", "database_url")
fn database_url() -> Result(String, Nil)

@external(erlang, "consumer_test_env", "storage_failure_url")
fn storage_failure_url() -> Result(String, Nil)

@external(erlang, "consumer_test_env", "mark")
fn mark(name: String) -> Nil

@external(erlang, "consumer_effect", "reset")
fn reset_effects() -> Nil

@external(erlang, "consumer_effect", "apply")
fn apply_synthetic_effect(key: String, amount: Int) -> #(String, Int)

@external(erlang, "consumer_effect", "count")
fn synthetic_effect_count(key: String) -> Int

/// Reads the application's own dedup record for a key without applying
/// anything: this is how the app inspects its own table during an audited
/// resolution, as opposed to calling `apply_synthetic_effect` again.
@external(erlang, "consumer_effect", "receipt")
fn synthetic_effect_receipt(key: String) -> Result(String, Nil)

/// Arms a one-shot crash: the next `apply_synthetic_effect` call for this
/// exact key applies (and retains) its effect first, then raises, killing
/// the calling worker process before Grind can acknowledge anything.
@external(erlang, "consumer_effect", "arm_crash_after_effect")
fn arm_crash_after_effect(key: String) -> Nil

/// Reports whether a crash was still armed for this key, consuming it if
/// so. Used here only to prove the fault is genuinely one-shot.
@external(erlang, "consumer_effect", "take_fault")
fn take_effect_fault(key: String) -> Bool

@external(erlang, "consumer_counter", "reset")
fn reset_counter(key: String) -> Nil

@external(erlang, "consumer_counter", "next")
fn next_counter(key: String) -> Int

@external(erlang, "consumer_counter", "value")
fn counter_value(key: String) -> Int

pub fn queue_policy_is_checked_before_start_test() {
  queue.default_policy()
  |> queue.with_poll_interval(0)
  |> queue.validate_policy
  |> should.equal(Error(queue.PollIntervalMustBePositive))

  queue.default_policy()
  |> queue.with_maximum_jobs_per_poll(-1)
  |> queue.validate_policy
  |> should.equal(Error(queue.MaximumJobsPerPollMustBePositive))

  queue.default_policy()
  |> queue.with_shutdown_grace(-1)
  |> queue.validate_policy
  |> should.equal(Error(queue.ShutdownGraceMustBeNonNegative))
}

pub fn public_consumer_executes_typed_workers_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_public_consumer_test(url)
  }
}

fn run_public_consumer_test(url: String) -> Nil {
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(60_000)
    |> queue.with_maximum_jobs_per_poll(4)
    |> queue.validate_policy
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  reset_effects()

  let payment_probe = process.new_subject()
  let report_probe = process.new_subject()
  let payment_worker = payment_worker(payment_probe)
  let report_worker = report_worker(report_probe)
  let assert Ok(workers) = registry.new("external-consumer")
  let assert Ok(workers) = registry.register(workers, payment_worker)
  let assert Ok(workers) = registry.register(workers, report_worker)

  let assert Ok(payment_handle) =
    postgres.submit(
      database,
      "external-consumer",
      payment_worker,
      PaymentRequest("payment/42", 500),
    )
  // A second, independently admitted job reuses the same application
  // idempotency key as `payment_handle`. Nothing is preseeded: this is what
  // actually exercises the app's own dedup table, because the worker's own
  // effect application for this second job is the one that must observe the
  // key already taken by the first job's own execution.
  let assert Ok(dedup_handle) =
    postgres.submit(
      database,
      "external-consumer",
      payment_worker,
      PaymentRequest("payment/42", 500),
    )
  let assert Ok(report_handle) =
    postgres.submit(database, "external-consumer", report_worker, 8)
  let assert Ok(failure_handle) =
    postgres.submit(
      database,
      "external-consumer",
      payment_worker,
      PaymentRequest("missing/99", 0),
    )

  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  process.receive(payment_probe, within: 5000)
  |> should.equal(Ok(ChargeInvoked))
  process.receive(payment_probe, within: 5000)
  |> should.equal(Ok(ChargeInvoked))
  process.receive(report_probe, within: 5000)
  |> should.equal(Ok(ReportInvoked))
  process.receive(payment_probe, within: 5000)
  |> should.equal(Ok(ChargeInvoked))

  // Handler probes arrive before their database acknowledgements. Wait for the
  // committed job states themselves before reading typed outcomes.
  await_state(database, payment_handle, job.Succeeded, 250)
  |> should.equal(True)
  await_state(database, dedup_handle, job.Succeeded, 250)
  |> should.equal(True)
  await_state(database, report_handle, job.Succeeded, 250)
  |> should.equal(True)
  await_state(database, failure_handle, job.BusinessFailed, 250)
  |> should.equal(True)
  // Both payment jobs called into the same synthetic effect for the same
  // key, yet the key was only ever actually applied once.
  synthetic_effect_count("payment/42") |> should.equal(1)
  // The receipt carries a unique token minted only inside the app's own
  // table (consumer_effect.erl), so reading it back here and asserting both
  // jobs' committed outcomes equal it proves the outcome's value actually
  // came from that table -- not merely a value this test could have
  // predicted as a pure function of the key.
  let assert Ok(payment_receipt) = synthetic_effect_receipt("payment/42")
  postgres.outcome(database, payment_handle)
  |> should.equal(Ok(job.SucceededWith(payment_receipt)))
  postgres.outcome(database, dedup_handle)
  |> should.equal(Ok(job.SucceededWith(payment_receipt)))
  postgres.outcome(database, report_handle)
  |> should.equal(Ok(job.SucceededWith(16)))
  postgres.outcome(database, failure_handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(
      PaymentRejected("missing/99"),
      worker.BudgetExhausted,
    )),
  )
  mark("two-worker-consumer-passed")
}

pub fn public_consumer_retry_and_running_cancellation_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_retry_and_cancellation_test(url)
  }
}

fn run_retry_and_cancellation_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  reset_effects()
  reset_counter("consumer-retry-attempt")

  let attempt_probe = process.new_subject()
  let retry_context_probe = process.new_subject()
  let started = process.new_subject()
  let retry_worker =
    retry_then_succeed_worker(attempt_probe, retry_context_probe)
  let cancel_worker = cancel_while_running_worker(started)
  let assert Ok(workers) = registry.new("consumer-retry-cancel")
  let assert Ok(workers) = registry.register(workers, retry_worker)
  let assert Ok(workers) = registry.register(workers, cancel_worker)

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // --- Job 1: a definition-bound retry policy retries once, then succeeds.
  let assert Ok(retry_handle) =
    postgres.submit(database, "consumer-retry-cancel", retry_worker, 21)

  await_claim(consumer, 0) |> should.equal(Ok(True))
  // The bound retry policy callback receives Grind's own persisted
  // `RetryContext` directly and reports it here: the first failed delivery's
  // `current_attempt` is genuinely 1, observed through this public callback.
  process.receive(retry_context_probe, within: 2000) |> should.equal(Ok(1))
  // The `perform` handler itself has no such access -- only a bound retry
  // policy callback ever receives a `RetryContext` (see
  // docs/IMPLEMENTATION-SCOPE.md, "Job lifecycle and attempt history") -- so
  // it tracks its own invocation count instead, which in this single-worker,
  // no-concurrent-claims scenario advances in lockstep with Grind's
  // persisted attempt count.
  process.receive(attempt_probe, within: 2000) |> should.equal(Ok(1))
  postgres.state(database, retry_handle) |> should.equal(Ok(job.Retryable))

  // The retry delay is a real 50ms wall-clock wait; bounded polling waits
  // for it to become due rather than sleeping a fixed duration as the
  // assertion itself.
  await_claim(consumer, 250) |> should.equal(Ok(True))
  process.receive(attempt_probe, within: 2000) |> should.equal(Ok(2))
  await_state(database, retry_handle, job.Succeeded, 250)
  |> should.equal(True)
  postgres.outcome(database, retry_handle)
  |> should.equal(Ok(job.SucceededWith("retry-succeeded-21")))

  // --- Job 2: cancellation requested against a running attempt. Grind
  // commits Cancelled at acknowledgement regardless of the worker's own
  // return value; it never undoes whatever the handler already did.
  let assert Ok(cancel_handle) =
    postgres.submit(
      database,
      "consumer-retry-cancel",
      cancel_worker,
      PaymentRequest("consumer-cancel/1", 250),
    )

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(BarrierStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, BarrierRelease) })

  postgres.state(database, cancel_handle) |> should.equal(Ok(job.Executing))
  postgres.cancel(database, cancel_handle)
  |> should.equal(Ok(postgres.CancellationRequested))

  process.send(release, BarrierRelease)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))

  postgres.state(database, cancel_handle) |> should.equal(Ok(job.Cancelled))
  postgres.outcome(database, cancel_handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))

  // The application's own effect record proves the handler's synthetic
  // effect actually ran and was retained: Grind's cancellation does not, and
  // cannot, undo it.
  synthetic_effect_count("consumer-cancel/1") |> should.equal(1)

  mark("consumer-retry-and-cancellation-passed")
}

pub fn public_consumer_effect_crash_uncertainty_audited_recovery_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_effect_crash_uncertainty_test(url)
  }
}

fn run_effect_crash_uncertainty_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  reset_effects()

  let job1_key = "consumer-uncertain/1"
  let job2_key = "consumer-uncertain/2"
  reset_counter(job1_key)
  reset_counter(job2_key)
  // A one-shot fault plan owned by the test application, not by Grind: the
  // very next effect application for each key applies (and retains) its
  // effect, then crashes the worker before any acknowledgement can commit.
  arm_crash_after_effect(job1_key)
  arm_crash_after_effect(job2_key)

  let fault_worker = fault_prone_payment_worker()
  let assert Ok(workers) = registry.new("consumer-uncertain")
  let assert Ok(workers) = registry.register(workers, fault_worker)

  let assert Ok(job1_handle) =
    postgres.submit(
      database,
      "consumer-uncertain",
      fault_worker,
      PaymentRequest(job1_key, 100),
    )
  let assert Ok(job2_handle) =
    postgres.submit(
      database,
      "consumer-uncertain",
      fault_worker,
      PaymentRequest(job2_key, 100),
    )

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_jobs_per_poll(2)
    |> queue.with_maximum_concurrency(2)
    |> queue.with_lease_duration(6100)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  // Each worker crashes (a genuine Erlang runtime error, not a typed business
  // failure) right after applying its effect. That kills the Temporary
  // worker child before it can return anything to Grind, so no
  // acknowledgement is ever attempted for that attempt. Only the short
  // lease's expiry, found by the automatic poller's own quarantine scan on a
  // later tick, moves each row to Uncertain.
  // Budget covers the (now longer) lease itself — see
  // `postgres.statement_deadline`/`queue.LeaseTooShortForDeadline` — plus a
  // margin for the quarantine scan's own next poll tick.
  await_state(database, job1_handle, job.Uncertain, 400)
  |> should.equal(True)
  await_state(database, job2_handle, job.Uncertain, 400)
  |> should.equal(True)
  // The crash surfaced as worker death and conservative recovery -- never as
  // an invalid-input or business-failure outcome.
  postgres.outcome(database, job1_handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  postgres.outcome(database, job2_handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  synthetic_effect_count(job1_key) |> should.equal(1)
  synthetic_effect_count(job2_key) |> should.equal(1)
  counter_value(job1_key) |> should.equal(1)
  counter_value(job2_key) |> should.equal(1)
  // The one-shot fault plan is already consumed by the crashing call.
  take_effect_fault(job1_key) |> should.equal(False)
  take_effect_fault(job2_key) |> should.equal(False)

  // The application inspects its own dedup table -- not a Grind API -- to
  // decide each resolution.
  let assert Ok(receipt1) = synthetic_effect_receipt(job1_key)
  let assert Ok(receipt2) = synthetic_effect_receipt(job2_key)

  // Job 1: confirmed successful from the application's own record. No rerun.
  let assert Ok(rebound1) =
    postgres.bind_handle(database, fault_worker, job.id_value(job1_handle))
  postgres.resolve_uncertain(
    database,
    rebound1,
    postgres.ResolutionRequest(
      "consumer-uncertain-resolution-1",
      "operator@example.test",
      "confirmed from the application's own dedup record",
      postgres.ConfirmSuccess(receipt1),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.state(database, rebound1) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, rebound1)
  |> should.equal(Ok(job.SucceededWith(receipt1)))

  // Job 2: authorized replay. The automatic consumer picks the requeued row
  // back up on its own next poll tick; the rerun calls apply with the same
  // key and receives the original receipt, so the synthetic effect count
  // for that key stays 1.
  let assert Ok(rebound2) =
    postgres.bind_handle(database, fault_worker, job.id_value(job2_handle))
  postgres.resolve_uncertain(
    database,
    rebound2,
    postgres.ResolutionRequest(
      "consumer-uncertain-resolution-2",
      "operator@example.test",
      "authorized replay after audited review",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))

  await_state(database, rebound2, job.Succeeded, 250)
  |> should.equal(True)
  postgres.outcome(database, rebound2)
  |> should.equal(Ok(job.SucceededWith(receipt2)))

  // Job 1's handler ran exactly once (the crashing attempt); job 2's ran
  // twice (the crashing attempt, then the authorized rerun). Neither key's
  // synthetic effect was ever applied more than once.
  counter_value(job1_key) |> should.equal(1)
  counter_value(job2_key) |> should.equal(2)
  synthetic_effect_count(job1_key) |> should.equal(1)
  synthetic_effect_count(job2_key) |> should.equal(1)

  mark("consumer-uncertainty-audited-recovery-passed")
}

fn await_state(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  expected: job.State,
  remaining_checks: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(state) ->
      case state == expected, remaining_checks > 0 {
        True, _ -> True
        False, True -> {
          process.sleep(20)
          await_state(database, handle, expected, remaining_checks - 1)
        }
        False, False -> False
      }
    Error(_) -> False
  }
}

/// Bounded polling for a manual consumer's next claimable job. Used to wait
/// for a real retry delay to become due without sleeping as the assertion
/// itself.
fn await_claim(
  consumer: queue.Consumer,
  remaining_checks: Int,
) -> Result(Bool, queue.ProcessError) {
  case queue.process_one(consumer) {
    Ok(True) -> Ok(True)
    Ok(False) ->
      case remaining_checks > 0 {
        True -> {
          process.sleep(20)
          await_claim(consumer, remaining_checks - 1)
        }
        False -> Ok(False)
      }
    Error(error) -> Error(error)
  }
}

fn payment_worker(
  probe: process.Subject(Probe),
) -> worker.Worker(PaymentRequest, String, PaymentError) {
  let assert Ok(input) =
    worker.codec(
      "payment-request-v1",
      encode_payment_request,
      decode_payment_request(),
    )
  let assert Ok(output) = worker.codec("receipt-v1", json.string, decode.string)
  let assert Ok(error) =
    worker.codec(
      "payment-error-v1",
      encode_payment_error,
      decode_payment_error(),
    )
  let assert Ok(definition) =
    worker.define_with_error_codec(
      "payments.charge",
      "v1",
      input,
      output,
      error,
      fn(request) {
        process.send(probe, ChargeInvoked)
        case request {
          PaymentRequest("missing/99", _) ->
            Error(PaymentRejected("missing/99"))
          PaymentRequest(key, amount) -> {
            let #(receipt, _) = apply_synthetic_effect(key, amount)
            Ok(receipt)
          }
        }
      },
    )
  let assert Ok(single_attempt) = worker.with_max_attempts(definition, 1)
  single_attempt
}

fn encode_payment_request(request: PaymentRequest) -> json.Json {
  case request {
    PaymentRequest(key, amount) ->
      json.object([
        #("idempotency_key", json.string(key)),
        #("amount", json.int(amount)),
      ])
  }
}

fn decode_payment_request() -> decode.Decoder(PaymentRequest) {
  use key <- decode.field("idempotency_key", decode.string)
  use amount <- decode.field("amount", decode.int)
  decode.success(PaymentRequest(key, amount))
}

fn report_worker(
  probe: process.Subject(Probe),
) -> worker.Worker(Int, Int, Nil) {
  let assert Ok(input) = worker.codec("report-count-v1", json.int, decode.int)
  let assert Ok(output) = worker.codec("report-total-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define("reports.total", "v3", input, output, fn(count) {
      process.send(probe, ReportInvoked)
      Ok(count * 2)
    })
  definition
}

fn encode_payment_error(error: PaymentError) -> json.Json {
  case error {
    PaymentRejected(key) ->
      json.object([
        #("kind", json.string("payment_rejected")),
        #("key", json.string(key)),
      ])
  }
}

fn decode_payment_error() -> decode.Decoder(PaymentError) {
  use kind <- decode.field("kind", decode.string)
  use key <- decode.field("key", decode.string)
  case kind {
    "payment_rejected" -> decode.success(PaymentRejected(key))
    _ -> decode.failure(PaymentRejected(key), "known payment error kind")
  }
}

/// A worker whose bound retry policy retries exactly once (a short real
/// delay) and then succeeds. The bound retry policy callback receives
/// Grind's own `RetryContext` and reports its `current_attempt` on
/// `retry_context_probe`. The `perform` handler itself has no such access
/// (see docs/IMPLEMENTATION-SCOPE.md, "Job lifecycle and attempt history"),
/// so it tracks its own invocation count on `attempt_probe` instead, which
/// in this single-worker, no-concurrent-claims scenario advances in
/// lockstep with Grind's persisted attempt count.
fn retry_then_succeed_worker(
  attempt_probe: process.Subject(Int),
  retry_context_probe: process.Subject(Int),
) -> worker.Worker(Int, String, Nil) {
  let assert Ok(input) = worker.codec("retry-input-v1", json.int, decode.int)
  let assert Ok(output) =
    worker.codec("retry-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("consumer.retry_then_succeed", "v1", input, output, fn(value) {
      let attempt = next_counter("consumer-retry-attempt")
      process.send(attempt_probe, attempt)
      case attempt {
        1 -> Error(Nil)
        _ -> Ok("retry-succeeded-" <> int.to_string(value))
      }
    })
  let assert Ok(short_delay) = worker.retry_delay(50)
  let policy =
    worker.retry_policy(fn(_failure, context) {
      process.send(retry_context_probe, context.current_attempt)
      worker.RetryAfter(short_delay)
    })
  worker.with_retry_policy(definition, policy)
}

/// A worker that reports it has started, blocks on a barrier, and only then
/// performs its (synthetic, idempotent) effect. Used to hold a job in
/// `Executing` state long enough for the test to request cancellation.
fn cancel_while_running_worker(
  started: process.Subject(Barrier),
) -> worker.Worker(PaymentRequest, String, Nil) {
  let assert Ok(input) =
    worker.codec(
      "cancel-running-request-v1",
      encode_payment_request,
      decode_payment_request(),
    )
  let assert Ok(output) =
    worker.codec("cancel-running-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "consumer.cancel_while_running",
      "v1",
      input,
      output,
      fn(request) {
        let release = process.new_subject()
        process.send(started, BarrierStarted(release))
        let PaymentRequest(key, amount) = request
        case process.receive(release, within: 10_000) {
          Ok(BarrierRelease) -> {
            let #(receipt, _) = apply_synthetic_effect(key, amount)
            Ok(receipt)
          }
          Error(Nil) -> Ok("released-by-timeout")
        }
      },
    )
  definition
}

/// A worker whose effect application may have been armed to crash right
/// after applying (see `arm_crash_after_effect`). Tracks its own invocation
/// count per key so the test can distinguish the crashing attempt from a
/// later authorized replay.
fn fault_prone_payment_worker() -> worker.Worker(PaymentRequest, String, Nil) {
  let assert Ok(input) =
    worker.codec(
      "fault-prone-request-v1",
      encode_payment_request,
      decode_payment_request(),
    )
  let assert Ok(output) =
    worker.codec("fault-prone-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "consumer.fault_prone_payment",
      "v1",
      input,
      output,
      fn(request) {
        let PaymentRequest(key, amount) = request
        let _ = next_counter(key)
        let #(receipt, _) = apply_synthetic_effect(key, amount)
        Ok(receipt)
      },
    )
  definition
}

pub fn storage_start_failure_is_reported_test() {
  case storage_failure_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(settings) =
        postgres.settings(url)
        |> postgres.validate
      let failure_observed = case postgres.start(settings) {
        Error(_) -> True
        Ok(database) -> {
          use <- exception.defer(fn() { postgres.close(database) })
          case postgres.migrate(database) {
            Error(_) -> True
            Ok(Nil) -> False
          }
        }
      }
      failure_observed |> should.equal(True)
      mark("consumer-storage-failure-passed")
    }
  }
}

// -- Uniqueness admission through the public API -----------------------------
//
// `grind/unique`/`postgres.submit_unique`/`postgres.reconcile_unique` are
// exercised here through public imports only, mirroring the rest of this
// file's discipline: no `@internal` function, no `grind/postgres.Database`
// internals, no raw `pog` connection. See `docs/UNIQUENESS-CONTRACT.md` for
// the full contract these calls implement.

/// The `Int` input / `String` output worker shape both uniqueness tests
/// below need.
fn unique_echo_worker(id: String) -> worker.Worker(Int, String, Nil) {
  let assert Ok(input) = worker.codec(id <> "-input-v1", json.int, decode.int)
  let assert Ok(output) =
    worker.codec(id <> "-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(id, "v1", input, output, fn(value) {
      Ok(int.to_string(value))
    })
  definition
}

pub fn public_consumer_unique_admission_existing_conflict_and_retry_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_unique_admission_existing_conflict_and_retry_test(url)
  }
}

fn run_unique_admission_existing_conflict_and_retry_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_def = unique_echo_worker("consumer.unique_echo")
  let assert Ok(workers) = registry.new("consumer-unique")
  let assert Ok(workers) = registry.register(workers, worker_def)

  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "consumer-unique"

  let assert Ok(first_submission) =
    submission.submission_id("consumer-unique-first")
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      first_submission,
      worker_def,
      42,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )

  // A second, independently identified admission with the same key hits the
  // still-queued conflict instead of inserting a new row.
  let assert Ok(second_submission) =
    submission.submission_id("consumer-unique-second")
  let assert Ok(submission.Existing(conflict)) =
    postgres.submit_unique(
      database,
      test_queue,
      second_submission,
      worker_def,
      42,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))

  // `Conflict` is not a handle: the caller rebinds it through the same
  // durable-id path used after a restart before reading typed state.
  let assert Ok(bound) =
    postgres.bind_handle(
      database,
      worker_def,
      submission.conflict_job_id(conflict),
    )
  postgres.state(database, bound) |> should.equal(Ok(job.Queued))

  // Replaying the *original* SubmissionId returns the receipt's own recorded
  // decision -- the original job id, as `Inserted` again -- rather than
  // treating the still-present row as a fresh conflict.
  let assert Ok(submission.Inserted(replayed)) =
    postgres.submit_unique(
      database,
      test_queue,
      first_submission,
      worker_def,
      42,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  job.id_value(replayed) |> should.equal(job.id_value(handle))

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  await_state(database, bound, job.Succeeded, 250) |> should.equal(True)
  postgres.outcome(database, bound) |> should.equal(Ok(job.SucceededWith("42")))

  mark("consumer-unique-admission-existing-conflict-retry-passed")
}

/// `postgres.submit_with_id` — the retry-safe plain submit path, exercised
/// from a consumer of only Grind's public API. A same-id, same-input retry
/// after a genuine first commit converges on the original job (`Inserted`,
/// the same id, no second row), unlike a plain `submit`/`submit_at` retry.
pub fn public_consumer_submit_with_id_retry_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_submit_with_id_retry_test(url)
  }
}

fn run_submit_with_id_retry_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_def = unique_echo_worker("consumer.submit_with_id_echo")
  let assert Ok(workers) = registry.new("consumer-submit-with-id")
  let assert Ok(workers) = registry.register(workers, worker_def)
  let test_queue = "consumer-submit-with-id"

  let assert Ok(submission_id) =
    submission.submission_id("consumer-with-id-once")
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_with_id(
      database,
      test_queue,
      submission_id,
      worker_def,
      42,
      submission.Immediately,
    )

  // A retry with the identical `SubmissionId` and request converges on the
  // exact same job -- no second row -- rather than risking a duplicate the
  // way a plain `submit` retry could.
  let assert Ok(submission.Inserted(retried)) =
    postgres.submit_with_id(
      database,
      test_queue,
      submission_id,
      worker_def,
      42,
      submission.Immediately,
    )
  job.id_value(retried) |> should.equal(job.id_value(handle))

  // A different request under the same id is a genuine conflict, not a
  // silent replay.
  postgres.submit_with_id(
    database,
    test_queue,
    submission_id,
    worker_def,
    43,
    submission.Immediately,
  )
  |> should.equal(Error(submission.SubmissionConflict))

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  await_state(database, handle, job.Succeeded, 250) |> should.equal(True)
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("42")))

  mark("consumer-submit-with-id-retry-passed")
}

pub fn public_consumer_unique_reschedule_across_queues_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_unique_reschedule_across_queues_test(url)
  }
}

fn run_unique_reschedule_across_queues_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_def = unique_echo_worker("consumer.unique_reschedule_echo")
  let assert Ok(workers) = registry.new("consumer-unique-across-a")
  let assert Ok(workers) = registry.register(workers, worker_def)

  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.AcrossQueues,
      period,
      unique.ScheduledOnly,
    )

  // Seeded far enough in the future that it is genuinely `scheduled`, not
  // already due.
  let far_future_ms = now_unix_ms() + 3_600_000
  let assert Ok(far_future_at) = job.available_at(far_future_ms)
  let assert Ok(seed_submission) =
    submission.submission_id("consumer-unique-across-seed")
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      "consumer-unique-across-a",
      seed_submission,
      worker_def,
      7,
      submission.At(far_future_at),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  // A second submission through a *different* queue, under `AcrossQueues`,
  // reschedules the same key's still-scheduled row to a near-future time --
  // it never inserts a second row in its own submitting queue.
  let soon_ms = now_unix_ms() + 50
  let assert Ok(soon_at) = job.available_at(soon_ms)
  let assert Ok(reschedule_submission) =
    submission.submission_id("consumer-unique-across-reschedule")
  let assert Ok(submission.Rescheduled(conflict)) =
    postgres.submit_unique(
      database,
      "consumer-unique-across-b",
      reschedule_submission,
      worker_def,
      7,
      submission.Immediately,
      policy,
      unique.RescheduleScheduledTo(soon_at),
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))
  // The row's actual queue is the one it was originally inserted under, not
  // the rescheduling submission's own queue.
  submission.conflict_queue(conflict)
  |> should.equal("consumer-unique-across-a")

  let assert Ok(bound) =
    postgres.bind_handle(
      database,
      worker_def,
      submission.conflict_job_id(conflict),
    )

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  await_claim(consumer, 250) |> should.equal(Ok(True))

  await_state(database, bound, job.Succeeded, 250) |> should.equal(True)
  postgres.outcome(database, bound) |> should.equal(Ok(job.SucceededWith("7")))

  mark("consumer-unique-reschedule-across-queues-passed")
}

/// Attaches `sinal.observe` to `grind/observation.acknowledged()` using only
/// public imports (`grind/observation`, `sinal`), runs one typed job through
/// the public consumer API, and decodes the resulting record — proving the
/// descriptor Grind owns is usable end to end from outside the package,
/// exactly as an application would use it.
pub fn public_consumer_observes_acknowledged_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_public_consumer_observation_test(url)
  }
}

fn run_public_consumer_observation_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let assert Ok(input_codec) =
    worker.codec("consumer-observation-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("consumer-observation-output-v1", json.string, decode.string)
  let assert Ok(echo_worker) =
    worker.define(
      "consumer.observation.echo",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("external-consumer-observation")
  let assert Ok(workers) = registry.register(workers, echo_worker)
  let assert Ok(handle) =
    postgres.submit(database, "external-consumer-observation", echo_worker, 41)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("consumer-observation-acknowledged")
  let assert Ok(attachment) =
    sinal.observe(id, observation.acknowledged(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() {
    let assert Ok(Nil) = sinal.detach(attachment)
    Nil
  })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.AcknowledgedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.ref.queue |> should.equal("external-consumer-observation")
  metadata.ref.worker_id |> should.equal("consumer.observation.echo")
  metadata.committed_state |> should.equal(job.Succeeded)
  metadata.proposed |> should.equal(observation.ProposedSuccess)
  metadata.confirmation |> should.equal(observation.Replied)
  mark("consumer-observes-acknowledged-passed")
}

pub fn public_consumer_observes_claimed_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_public_consumer_claimed_observation_test(url)
  }
}

/// Round 2's `[grind, job, claimed]` descriptor, attached the same way an
/// application would from outside the package (public imports only).
fn run_public_consumer_claimed_observation_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let assert Ok(input_codec) =
    worker.codec("consumer-claimed-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("consumer-claimed-output-v1", json.string, decode.string)
  let assert Ok(echo_worker) =
    worker.define(
      "consumer.claimed.echo",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("external-consumer-claimed")
  let assert Ok(workers) = registry.register(workers, echo_worker)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("consumer-observation-claimed")
  let assert Ok(attachment) =
    sinal.observe(id, observation.claimed(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() {
    let assert Ok(Nil) = sinal.detach(attachment)
    Nil
  })

  let assert Ok(handle) =
    postgres.submit(database, "external-consumer-claimed", echo_worker, 41)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.ClaimedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.ref.queue |> should.equal("external-consumer-claimed")
  metadata.ref.worker_id |> should.equal("consumer.claimed.echo")
  metadata.previous_state |> should.equal(job.Queued)
  mark("consumer-observes-claimed-passed")
}
