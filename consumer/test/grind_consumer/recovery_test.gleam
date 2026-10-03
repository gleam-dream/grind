import exception
import gleam/erlang/process
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind_consumer.{PaymentRequest}
import grind_consumer/support/env
import grind_consumer/support/wait
import grind_consumer/support/workers

pub fn public_consumer_retry_and_running_cancellation_test() {
  case env.database_url() {
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
  env.reset_effects()
  env.reset_counter("consumer-retry-attempt")

  let attempt_probe = process.new_subject()
  let retry_context_probe = process.new_subject()
  let started = process.new_subject()
  let retry_worker =
    workers.retry_then_succeed_worker(attempt_probe, retry_context_probe)
  let cancel_worker = workers.cancel_while_running_worker(started)
  let assert Ok(workers) = registry.new("consumer-retry-cancel")
  let assert Ok(workers) = registry.register(workers, retry_worker)
  let assert Ok(workers) = registry.register(workers, cancel_worker)

  let assert Ok(consumer) = queue.start(database, workers, env.manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // --- Job 1: a definition-bound retry policy retries once, then succeeds.
  let assert Ok(retry_handle) =
    postgres.submit(database, "consumer-retry-cancel", retry_worker, 21)

  wait.await_claim(consumer, 0) |> should.equal(Ok(True))
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
  wait.await_claim(consumer, 250) |> should.equal(Ok(True))
  process.receive(attempt_probe, within: 2000) |> should.equal(Ok(2))
  wait.await_state(database, retry_handle, job.Succeeded, 250)
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
  let assert Ok(workers.BarrierStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, workers.BarrierRelease) })

  postgres.state(database, cancel_handle) |> should.equal(Ok(job.Executing))
  postgres.cancel(database, cancel_handle)
  |> should.equal(Ok(postgres.CancellationRequested))

  process.send(release, workers.BarrierRelease)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))

  postgres.state(database, cancel_handle) |> should.equal(Ok(job.Cancelled))
  postgres.outcome(database, cancel_handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))

  // The application's own effect record proves the handler's synthetic
  // effect actually ran and was retained: Grind's cancellation does not, and
  // cannot, undo it.
  env.synthetic_effect_count("consumer-cancel/1") |> should.equal(1)

  env.mark("consumer-retry-and-cancellation-passed")
}

pub fn public_consumer_effect_crash_uncertainty_audited_recovery_test() {
  case env.database_url() {
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
  env.reset_effects()

  let job1_key = "consumer-uncertain/1"
  let job2_key = "consumer-uncertain/2"
  env.reset_counter(job1_key)
  env.reset_counter(job2_key)
  // A one-shot fault plan owned by the test application, not by Grind: the
  // very next effect application for each key applies (and retains) its
  // effect, then crashes the worker before any acknowledgement can commit.
  env.arm_crash_after_effect(job1_key)
  env.arm_crash_after_effect(job2_key)

  let fault_worker = workers.fault_prone_payment_worker()
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
  wait.await_state(database, job1_handle, job.Uncertain, 400)
  |> should.equal(True)
  wait.await_state(database, job2_handle, job.Uncertain, 400)
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
  env.synthetic_effect_count(job1_key) |> should.equal(1)
  env.synthetic_effect_count(job2_key) |> should.equal(1)
  env.counter_value(job1_key) |> should.equal(1)
  env.counter_value(job2_key) |> should.equal(1)
  // The one-shot fault plan is already consumed by the crashing call.
  env.take_effect_fault(job1_key) |> should.equal(False)
  env.take_effect_fault(job2_key) |> should.equal(False)

  // The application inspects its own dedup table -- not a Grind API -- to
  // decide each resolution.
  let assert Ok(receipt1) = env.synthetic_effect_receipt(job1_key)
  let assert Ok(receipt2) = env.synthetic_effect_receipt(job2_key)

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

  wait.await_state(database, rebound2, job.Succeeded, 250)
  |> should.equal(True)
  postgres.outcome(database, rebound2)
  |> should.equal(Ok(job.SucceededWith(receipt2)))

  // Job 1's handler ran exactly once (the crashing attempt); job 2's ran
  // twice (the crashing attempt, then the authorized rerun). Neither key's
  // synthetic effect was ever applied more than once.
  env.counter_value(job1_key) |> should.equal(1)
  env.counter_value(job2_key) |> should.equal(2)
  env.synthetic_effect_count(job1_key) |> should.equal(1)
  env.synthetic_effect_count(job2_key) |> should.equal(1)

  env.mark("consumer-uncertainty-audited-recovery-passed")
}
