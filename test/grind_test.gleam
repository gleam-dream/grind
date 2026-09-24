import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleeunit
import gleeunit/should
import grind
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/worker
import pog

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn version_test() {
  grind.version()
  |> should.equal("0.1.0")
}

pub type LookupFailure {
  AccountMissing(account_id: Int)
}

type WorkerProbe {
  WorkerInvoked
  LaterWorkerInvoked
}

type RetryPolicyProbe {
  RetryPolicyInvoked(Int, Int)
}

type LeaseCommand {
  ReleaseAttempt
}

type LeaseSignal {
  FirstAttemptStarted(process.Subject(LeaseCommand))
  TakeoverAttemptStarted(process.Subject(LeaseCommand))
}

type LongCallEvent {
  LongCallReturned(Result(Bool, queue.ProcessError))
  LongCallDown(process.Down)
}

type LongHandlerSignal {
  LongHandlerStarted(process.Subject(LeaseCommand))
}

type WorkerDeathSignal {
  WorkerDeathStarted(process.Pid, process.Subject(LeaseCommand))
}

type ConcurrentClaimSignal {
  ConcurrentClaimWorkerStarted(process.Subject(LeaseCommand))
}

type ClaimGateSignal {
  ClaimGateAcquired(process.Subject(LeaseCommand))
  ClaimGateReleased(Bool)
}

type CapacitySignal {
  CapacityWorkerStarted(Int, process.Subject(LeaseCommand))
}

type ConsumerOwnerStart {
  ConsumerOwnerStarted(process.Pid, queue.Consumer, process.Subject(Nil))
  ConsumerOwnerStopCompleted(Result(queue.StopOutcome, queue.StopError))
  ConsumerOwnerFailed(queue.StartError)
}

type CoordinatorLossSignal {
  CoordinatorLossStarted(process.Pid, process.Subject(LeaseCommand))
}

type OwnerPoolLossSignal {
  OwnerPoolLossStarted(process.Pid, process.Subject(LeaseCommand))
}

type OwnerPoolLossOwnerEvent {
  OwnerPoolLossOwnerReady(process.Pid, queue.Consumer)
  OwnerPoolLossOwnerFailed(queue.StartError)
}

pub fn invocation_preserves_the_application_error_test() {
  let assert Ok(input_codec) =
    worker.codec("account-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("account-output-v1", json.string, decode.string)
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
    worker.codec("queue-adapter-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("queue-adapter-output-v1", json.string, decode.string)
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
    worker.codec("retry-validation-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("retry-validation-output-v1", json.string, decode.string)
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
    worker.codec("resolver-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolver-output-v1", json.string, decode.string)
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

pub fn postgres_stopped_consumer_handle_does_not_retarget_after_restart_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_stale_consumer_handle_test(database_url)
  }
}

pub fn postgres_repeated_stop_after_coordinator_gone_reports_without_drain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_stop_without_drain_test(database_url)
  }
}

pub fn postgres_supervised_owner_restart_resumes_automatic_polling_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_owner_restart_test(database_url)
  }
}

pub fn postgres_foreign_process_cannot_stop_consumer_owner_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_foreign_stop_test(database_url)
  }
}

pub fn postgres_consumer_stop_timeout_is_reported_and_owner_survives_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_stop_timeout_test(database_url)
  }
}

pub fn postgres_consumer_stop_drains_active_attempt_before_supervisor_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_stop_drain_test(database_url)
  }
}

pub fn postgres_consumer_stop_reports_active_work_after_grace_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_forced_stop_test(database_url)
  }
}

pub fn postgres_forced_stop_releases_worker_and_pool_then_recovers_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_forced_stop_pool_cleanup_test(database_url)
  }
}

pub fn postgres_automatic_poll_pauses_and_renews_during_drain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_drain_test(database_url)
  }
}

pub fn postgres_coordinator_loss_with_active_work_quarantines_without_replay_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_coordinator_loss_test(database_url)
  }
}

pub fn postgres_owner_loss_recovers_on_fresh_consumer_after_pool_restart_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_owner_loss_recovers_after_pool_restart_test(database_url)
  }
}

pub fn postgres_stale_shutdown_grace_timer_does_not_end_a_later_drain_early_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_stale_shutdown_grace_timer_test(database_url)
  }
}

type ShutdownAttemptStarted {
  ShutdownWorkerStarted(
    pid: process.Pid,
    release: process.Subject(LeaseCommand),
  )
}

type ShutdownEvent {
  ShutdownSupervisorDown
  ShutdownWorkerDown
}

@external(erlang, "erlang", "suspend_process")
fn suspend_process(pid: process.Pid) -> Bool

@external(erlang, "erlang", "resume_process")
fn resume_process(pid: process.Pid) -> Bool

fn run_consumer_stop_timeout_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_stop_timeout")
  let settings = postgres.settings(database_url, pool_name)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stop-timeout-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stop-timeout-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("stop.timeout", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, ShutdownWorkerStarted(process.self(), release))
      case process.receive(release, within: 20_000) {
        Ok(ReleaseAttempt) -> Ok("completed-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("stop-timeout")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "stop-timeout", definition, 20)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_shutdown_grace(0)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(ShutdownWorkerStarted(worker_pid, _release)) =
    process.receive(started, within: 5000)
  let root_pid = queue.supervisor_pid(consumer)
  let root_monitor = process.monitor(root_pid)
  let worker_monitor = process.monitor(worker_pid)
  suspend_process(root_pid) |> should.equal(True)
  use <- exception.defer(fn() { resume_suspended_test_process(root_pid) })

  // A suspended supervisor makes the bounded stop wait expire. The operation
  // must report that uncertainty and leave its original linked owner alive.
  queue.stop(consumer)
  |> should.equal(Error(queue.ConsumerStopTimedOut))
  process.is_alive(process.self()) |> should.equal(True)
  case process.is_alive(root_pid) {
    True -> resume_process(root_pid) |> should.equal(True)
    False -> Nil
  }
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(root_monitor, fn(_) {
      ShutdownSupervisorDown
    })
    |> process.select_specific_monitor(worker_monitor, fn(_) {
      ShutdownWorkerDown
    })
  case process.selector_receive(selector, within: 10_000) {
    Ok(ShutdownSupervisorDown) ->
      process.selector_receive(selector, within: 10_000)
      |> should.equal(Ok(ShutdownWorkerDown))
    Ok(ShutdownWorkerDown) ->
      process.selector_receive(selector, within: 10_000)
      |> should.equal(Ok(ShutdownSupervisorDown))
    Error(Nil) -> should.fail()
  }
  process.is_alive(process.self()) |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  let _ = process.demonitor_process(root_monitor)
  let _ = process.demonitor_process(worker_monitor)
  mark_database_test_executed("consumer-stop-timeout-owner-survived")
}

fn run_consumer_stop_drain_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_stop_drain")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stop-drain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stop-drain-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("stop.drain", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, ShutdownWorkerStarted(process.self(), release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("drained-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("stop-drain")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "stop-drain", definition, 21)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_shutdown_grace(2000)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let process_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(process_reply, queue.process_one(consumer))
    })
  let assert Ok(ShutdownWorkerStarted(_, release)) =
    process.receive(started, within: 5000)
  let shutdown_seen = process.new_subject()
  let late_process_result = process.new_subject()
  let observer_ready = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(observer_ready, Nil)
      let shutting_down = wait_for_shutdown_state(consumer, 300)
      process.send(shutdown_seen, shutting_down)
      case shutting_down {
        True -> {
          process.send(late_process_result, queue.process_one(consumer))
          process.send(release, ReleaseAttempt)
        }
        False -> Nil
      }
    })
  process.receive(observer_ready, within: 1000) |> should.equal(Ok(Nil))

  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  process.receive(shutdown_seen, within: 1000) |> should.equal(Ok(True))
  process.receive(late_process_result, within: 1000)
  |> should.equal(Ok(Error(queue.QueueShuttingDown)))
  process.receive(process_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("consumer-stop-drained-active-worker")
}

fn run_consumer_forced_stop_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_stop_forced")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stop-forced-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stop-forced-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invocations = process.new_subject()
  let assert Ok(definition) =
    worker.define("stop.forced", "v1", input_codec, output_codec, fn(value) {
      process.send(invocations, value)
      let release = process.new_subject()
      process.send(started, ShutdownWorkerStarted(process.self(), release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("forced-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("stop-forced")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "stop-forced", definition, 22)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_shutdown_grace(0)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let process_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(process_reply, queue.process_one(consumer))
    })
  let assert Ok(ShutdownWorkerStarted(_, _release)) =
    process.receive(started, within: 5000)
  process.receive(invocations, within: 1000) |> should.equal(Ok(22))

  queue.stop(consumer)
  |> should.equal(Ok(queue.StoppedWithActiveWork(1)))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  process.receive(process_reply, within: 1000)
  |> should.equal(Ok(Error(queue.QueueActorExited)))
  process.receive(invocations, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("consumer-stop-forced-active-work-retained")
}

/// Polls (bounded, never a fixed sleep used as the assertion) for backends
/// belonging to Grind's own pool that are still visible in
/// `pg_stat_activity` after `postgres.close`. Grind never sets
/// `application_name` on its connections, so leftover backends are found by
/// exclusion instead: the observer's own backend (`pg_backend_pid()`) and
/// any non-client backend (autovacuum, walsender, background workers) are
/// excluded, leaving only ordinary client connections against this
/// database and user — which, in the disposable test cluster, are only ever
/// Grind's own pool connections plus this one observer. Returns `Ok(0)`
/// once none remain, or `Ok(leftover_count)` if bounded checks are
/// exhausted first — a nonzero result here is a real finding, not
/// something this test papers over.
fn poll_leftover_grind_backends(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [0] -> Ok(0)
        [count] ->
          case checks_remaining > 0 {
            True -> {
              process.sleep(20)
              poll_leftover_grind_backends(connection, checks_remaining - 1)
            }
            False -> Ok(count)
          }
        _ -> Error(Nil)
      }
  }
}

/// Forced shutdown (grace 0) releases both the worker process and Grind's
/// own connection pool, and a fresh pool/consumer on the same pool name
/// recovers the orphaned attempt as `Uncertain` with no second invocation —
/// the same no-replay contract as every other owner-loss recovery path,
/// now exercised across a real pool close/reopen rather than only a killed
/// owner process.
fn run_forced_stop_pool_cleanup_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_forced_stop_cleanup")
  let settings = postgres.settings(database_url, pool_name)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_pool_name =
    process.new_name("grind_forced_stop_cleanup_observer")
  let observer_settings =
    postgres.settings(database_url, observer_pool_name)
    |> postgres.pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = pog.named_connection(observer_pool_name)

  let assert Ok(input_codec) =
    worker.codec("forced-stop-cleanup-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("forced-stop-cleanup-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invocations = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "forced.stop.cleanup",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(invocations, WorkerInvoked)
        let release = process.new_subject()
        process.send(started, ShutdownWorkerStarted(process.self(), release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("cleanup-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("forced-stop-cleanup")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "forced-stop-cleanup", definition, 44)
  let job_id = job.id_value(handle)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_shutdown_grace(0)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  let process_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(process_reply, queue.process_one(consumer))
    })
  let assert Ok(ShutdownWorkerStarted(worker_pid, _release)) =
    process.receive(started, within: 5000)
  process.receive(invocations, within: 1000) |> should.equal(Ok(WorkerInvoked))
  let worker_monitor = process.monitor(worker_pid)

  queue.stop(consumer)
  |> should.equal(Ok(queue.StoppedWithActiveWork(1)))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(worker_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(..)) =
    process.selector_receive(down_selector, within: 5000)
  process.receive(process_reply, within: 1000)
  |> should.equal(Ok(Error(queue.QueueActorExited)))

  // Sanity-checks that the leftover-backend query below is not vacuously
  // always 0: with Grind's own pool still open (migrate/submit/state have
  // all just run queries through it), at least one client backend other
  // than the observer's own must be visible right now. A single check
  // (`checks_remaining: 0`) reuses the same query as the bounded poll below
  // instead of a bespoke one-off.
  let assert Ok(leftover_before_close) =
    poll_leftover_grind_backends(observer_connection, 0)
  should.be_true(leftover_before_close >= 1)

  // No ack ever ran (the worker died mid-attempt), so there is nothing to
  // reconcile against yet; the row is still `executing` with a live lease.
  postgres.close(database)

  let assert Ok(leftover) =
    poll_leftover_grind_backends(observer_connection, 300)
  leftover |> should.equal(0)

  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  postgres.state(reopened, handle) |> should.equal(Ok(job.Executing))

  let assert Ok(new_consumer) = queue.start_manual(reopened, workers)
  use <- exception.defer(fn() {
    let _ = queue.stop(new_consumer)
    Nil
  })
  // Drives expiry at the database boundary rather than sleeping past the
  // original lease duration.
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: observer_connection)
  forced_expiry.count |> should.equal(1)

  queue.process_one(new_consumer) |> should.equal(Ok(False))
  postgres.state(reopened, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invocations, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("forced-stop-pool-cleanup-recovered")
}

fn run_automatic_drain_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_auto_drain")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-drain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-drain-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("auto.drain", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CapacityWorkerStarted(value, release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("drained-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("auto-drain")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "auto-drain", definition, 31)
  let assert Ok(second_handle) =
    postgres.submit(database, "auto-drain", definition, 32)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_jobs_per_poll(2)
    |> queue.with_maximum_concurrency(1)
    |> queue.with_lease_duration(900)
    |> queue.with_shutdown_grace(2000)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let assert Ok(CapacityWorkerStarted(31, release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(release, ReleaseAttempt)
    Nil
  })
  let connection = pog.named_connection(pool_name)
  let shutdown_observed = process.new_subject()
  let observer_ready = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(observer_ready, Nil)
      let entered_drain = wait_for_shutdown_state(consumer, 300)
      let second_state = postgres.state(database, second_handle)
      let observed_expiry =
        lease_expiration(connection, job.id_value(first_handle))
      let renewed_during_drain = case observed_expiry {
        Ok(expiry) ->
          await_later_lease_expiry(
            connection,
            job.id_value(first_handle),
            expiry + 20,
            70,
          )
        Error(Nil) -> False
      }
      process.send(release, ReleaseAttempt)
      process.send(shutdown_observed, #(
        entered_drain,
        second_state,
        renewed_during_drain,
      ))
    })
  process.receive(observer_ready, within: 1000) |> should.equal(Ok(Nil))

  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  process.receive(shutdown_observed, within: 3000)
  |> should.equal(Ok(#(True, Ok(job.Queued), True)))
  postgres.state(database, first_handle) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, second_handle) |> should.equal(Ok(job.Queued))
  process.receive(started, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("automatic-drain-paused-poll-and-renewed")
}

fn wait_for_shutdown_state(
  consumer: queue.Consumer,
  checks_remaining: Int,
) -> Bool {
  case queue.shutdown_state(consumer) {
    Ok(True) -> True
    Ok(False) ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          wait_for_shutdown_state(consumer, checks_remaining - 1)
        }
        False -> False
      }
    Error(_) -> False
  }
}

fn resume_suspended_test_process(pid: process.Pid) -> Nil {
  // The successful explicit resume may finish the supervisor termination
  // before this failure-safe cleanup runs. OTP raises badarg if it resumes an
  // already resumed or dead process, which is harmless only in this cleanup.
  let _ = exception.rescue(fn() { resume_process(pid) })
  Nil
}

fn run_foreign_stop_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_foreign_stop")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("foreign-stop-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("foreign-stop-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("foreign.stop", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("foreign-stop")
  let assert Ok(workers) = registry.register(workers, definition)
  let started = process.new_subject()
  let owner =
    process.spawn_unlinked(fn() {
      case queue.start_manual(database, workers) {
        Error(error) -> process.send(started, ConsumerOwnerFailed(error))
        Ok(consumer) -> {
          let stop = process.new_subject()
          process.send(
            started,
            ConsumerOwnerStarted(process.self(), consumer, stop),
          )
          let _ = process.receive(stop, within: 60_000)
          let result = queue.stop(consumer)
          process.send(started, ConsumerOwnerStopCompleted(result))
        }
      }
    })
  let owner_monitor = process.monitor(owner)
  let assert Ok(ConsumerOwnerStarted(owner_pid, consumer, stop_owner)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    case process.is_alive(owner_pid) {
      False -> Nil
      True -> {
        process.send(stop_owner, Nil)
        let _ = process.receive(started, within: 5000)
        Nil
      }
    }
  })
  queue.stop(consumer)
  |> should.equal(Error(queue.ConsumerOwnedByAnotherProcess))
  // A foreign caller must not terminate a supervisor that remains linked to
  // the process that created the consumer.
  process.is_alive(owner_pid) |> should.equal(True)
  process.send(stop_owner, Nil)
  process.receive(started, within: 5000)
  |> should.equal(Ok(ConsumerOwnerStopCompleted(Ok(queue.StoppedCleanly))))
  let selector =
    process.new_selector()
    |> process.select_monitors(fn(_) { Nil })
  process.selector_receive(selector, within: 5000)
  |> should.equal(Ok(Nil))
  let _ = process.demonitor_process(owner_monitor)
  mark_database_test_executed("foreign-consumer-stop-owner-preserved")
}

fn run_owner_restart_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_owner_restart")
  let settings = postgres.settings(database_url, pool_name)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("owner-restart-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("owner-restart-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("owner.restart", "v1", input_codec, output_codec, fn(value) {
      Ok("restarted-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("owner-restart")
  let assert Ok(workers) = registry.register(workers, definition)
  let policy =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.validate_policy
  let assert Ok(policy) = policy
  let assert Ok(consumer) = queue.start_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let assert Ok(first_coordinator) = queue.coordinator_pid(consumer)
  process.kill(first_coordinator)
  // `Consumer.subject` is named, so it retargets to whichever coordinator
  // incarnation is currently registered: wait for the restart to land, then
  // `process_one` should reach the new, idle incarnation and report no due
  // work, rather than racing an immediate call against however far the
  // restart has progressed.
  let assert Ok(_) = await_new_coordinator_pid(consumer, first_coordinator, 500)
  queue.process_one(consumer) |> should.equal(Ok(False))

  let assert Ok(handle) =
    postgres.submit(database, "owner-restart", definition, 12)
  wait_for_succeeded(database, handle, 200) |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  let root_supervisor = queue.supervisor_pid(consumer)
  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  process.is_alive(root_supervisor) |> should.equal(False)
  mark_database_test_executed("supervised-owner-restart-resumed-polling")
}

fn wait_for_succeeded(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  checks_remaining: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(job.Succeeded) -> True
    _ ->
      case checks_remaining > 0 {
        False -> False
        True -> {
          process.sleep(25)
          wait_for_succeeded(database, handle, checks_remaining - 1)
        }
      }
  }
}

fn attempt_snapshot(
  connection: pog.Connection,
  id: Int,
) -> Result(#(Int, Int, Int, Option(String)), Nil) {
  pog.query(
    "SELECT attempt_id, attempt_epoch, (extract(epoch FROM lease_expires_at) * 1000)::bigint, attempt_owner FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use attempt_id <- decode.field(0, decode.int)
    use attempt_epoch <- decode.field(1, decode.int)
    use lease_expires_at <- decode.field(2, decode.int)
    use attempt_owner <- decode.field(3, decode.optional(decode.string))
    decode.success(#(attempt_id, attempt_epoch, lease_expires_at, attempt_owner))
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [snapshot] -> Ok(snapshot)
      _ -> Error(Nil)
    }
  })
}

fn database_time_ms(connection: pog.Connection) -> Result(Int, Nil) {
  pog.query("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
  |> pog.returning({
    use now <- decode.field(0, decode.int)
    decode.success(now)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [now] -> Ok(now)
      _ -> Error(Nil)
    }
  })
}

/// Polls the database clock (never local wall-clock time) until it passes
/// `target_unix_ms`, bounded by `checks_remaining` 10ms polls.
fn await_database_time_past(
  connection: pog.Connection,
  target_unix_ms: Int,
  checks_remaining: Int,
) -> Bool {
  case database_time_ms(connection) {
    Ok(now) if now >= target_unix_ms -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          await_database_time_past(
            connection,
            target_unix_ms,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

fn attempt_accounting(
  connection: pog.Connection,
  id: Int,
) -> Result(#(Int, Int), Nil) {
  pog.query(
    "SELECT attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use attempt_count <- decode.field(0, decode.int)
    use delivery_count <- decode.field(1, decode.int)
    decode.success(#(attempt_count, delivery_count))
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [accounting] -> Ok(accounting)
      _ -> Error(Nil)
    }
  })
}

fn await_new_coordinator_pid(
  consumer: queue.Consumer,
  previous: process.Pid,
  checks_remaining: Int,
) -> Result(process.Pid, Nil) {
  case queue.coordinator_pid(consumer) {
    Ok(pid) if pid != previous -> Ok(pid)
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          await_new_coordinator_pid(consumer, previous, checks_remaining - 1)
        }
        False -> Error(Nil)
      }
  }
}

fn run_coordinator_loss_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_coordinator_loss")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("coordinator-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("coordinator-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "coordinator.loss",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(invoked, WorkerInvoked)
        process.send(started, CoordinatorLossStarted(process.self(), release))
        case process.receive(release, within: 20_000) {
          Ok(ReleaseAttempt) -> Ok("coordinator-loss-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("coordinator-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "coordinator-loss", definition, 91)
  let lease_duration_ms = 2000
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_concurrency(1)
    |> queue.with_lease_duration(lease_duration_ms)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(CoordinatorLossStarted(worker_pid, _first_release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))

  let connection = pog.named_connection(pool_name)
  let job_id = job.id_value(handle)

  let worker_monitor = process.monitor(worker_pid)
  let assert Ok(first_coordinator) = queue.coordinator_pid(consumer)
  process.kill(first_coordinator)

  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(worker_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(..)) =
    process.selector_receive(down_selector, within: 5000)

  // Snapshotted only after the worker-DOWN barrier confirms the old
  // incarnation is gone, closing the window where a renewal from that
  // incarnation landing between an earlier snapshot and the kill would make
  // this snapshot's lease stale before it is ever compared against.
  let assert Ok(#(first_attempt_id, first_epoch, first_lease, _)) =
    attempt_snapshot(connection, job_id)

  let assert Ok(second_coordinator) =
    await_new_coordinator_pid(consumer, first_coordinator, 500)
  second_coordinator |> should.not_equal(first_coordinator)

  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  let assert Ok(#(still_attempt_id, still_epoch, _, _)) =
    attempt_snapshot(connection, job_id)
  still_attempt_id |> should.equal(first_attempt_id)
  still_epoch |> should.equal(first_epoch)

  // The restarted incarnation starts with no active attempts, so any stale
  // renewal timer the dead incarnation already scheduled lands on a
  // coordinator that has nothing to renew, and the orphaned lease is left
  // untouched. `first_lease = claim_time + lease_duration_ms`, so waiting
  // (via a database-time barrier, not a fixed sleep) until the database
  // clock passes `first_lease - lease_duration_ms + 2 * renewal_interval_ms`
  // is a wait past at least one full renewal tick and comfortably short of
  // the lease's own natural expiry.
  let renewal_interval_ms = lease_duration_ms / 3
  let past_one_renewal_tick =
    first_lease - lease_duration_ms + 2 * renewal_interval_ms
  await_database_time_past(connection, past_one_renewal_tick, 400)
  |> should.equal(True)
  let assert Ok(#(_, _, after_wait_lease, _)) =
    attempt_snapshot(connection, job_id)
  after_wait_lease |> should.equal(first_lease)

  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  wait_for_job_state(database, handle, job.Uncertain, 250)
  |> should.equal(True)
  process.receive(invoked, within: 200) |> should.equal(Error(Nil))

  let assert Ok(rebound) = postgres.bind_handle(database, definition, job_id)
  postgres.resolve_uncertain(
    database,
    rebound,
    "coordinator-loss-authorized-replay",
    "on-call",
    "inspect the external effect before authorizing a new delivery",
    postgres.AuthorizeReplay,
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))

  let assert Ok(CoordinatorLossStarted(_, second_release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))
  process.send(second_release, ReleaseAttempt)

  wait_for_job_state(database, handle, job.Succeeded, 250)
  |> should.equal(True)
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  let assert Ok(#(final_attempt_count, final_delivery_count)) =
    attempt_accounting(connection, job_id)
  final_attempt_count |> should.equal(2)
  final_delivery_count |> should.equal(2)

  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  mark_database_test_executed("coordinator-loss-quarantined-no-replay")
}

fn run_stop_without_drain_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_stop_without_drain")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stop-without-drain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stop-without-drain-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "stop.without-drain",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("stop-without-drain")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start_manual(database, workers)

  // First call: a genuine clean stop with no active work. `stop`'s own
  // `stop_consumer_supervisor` blocks until the supervisor (and therefore
  // the coordinator, its child) is actually terminated before returning, so
  // by the time the second call runs there is no race about whether the
  // coordinator's name is still registered.
  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))

  // Second call: the same owner, the same handle, but the coordinator this
  // consumer named is now fully gone. This must not panic (a named subject
  // send with nobody registered panics) and must not be reported as an
  // ordinary clean drain, since nothing was actually drained.
  queue.stop(consumer) |> should.equal(Ok(queue.StoppedWithoutDrain))
  mark_database_test_executed("stop-after-coordinator-gone-without-drain")
}

fn run_stale_shutdown_grace_timer_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_stale_shutdown_grace")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stale-grace-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stale-grace-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("stale.grace", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CoordinatorLossStarted(process.self(), release))
      case process.receive(release, within: 30_000) {
        Ok(ReleaseAttempt) -> Ok("stale-grace-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("stale-grace")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "stale-grace", definition, 71)
  let shutdown_grace_ms = 4000
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_concurrency(1)
    |> queue.with_lease_duration(15_000)
    |> queue.with_shutdown_grace(shutdown_grace_ms)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(CoordinatorLossStarted(_first_worker_pid, _first_release)) =
    process.receive(started, within: 5000)

  // Put the first incarnation into "draining with active work" so it
  // schedules a generation-1 grace timer, then kill it before that timer
  // ever fires: the timer is a plain `erlang:send_after`, unaffected by the
  // death of the process that scheduled it, so it keeps ticking regardless.
  let assert Ok(first_coordinator) = queue.coordinator_pid(consumer)
  let stale_reply = process.new_subject()
  queue.begin_shutdown_for_test(consumer, stale_reply)
  |> should.equal(Ok(Nil))
  wait_for_shutdown_state(consumer, 200) |> should.equal(True)
  process.kill(first_coordinator)

  let assert Ok(second_coordinator) =
    await_new_coordinator_pid(consumer, first_coordinator, 500)
  second_coordinator |> should.not_equal(first_coordinator)

  let assert Ok(second_handle) =
    postgres.submit(database, "stale-grace", definition, 72)
  let assert Ok(CoordinatorLossStarted(_second_worker_pid, _second_release)) =
    process.receive(started, within: 5000)

  // Wait well clear of the restart-and-reclaim overhead above before
  // starting the new incarnation's own drain, so its generation-1 deadline
  // sits comfortably later than the first incarnation's orphaned one. Both
  // incarnations reach shutdown generation 1 on this, their first-ever
  // drain with active work, so if the stale timer is not properly scoped to
  // its own incarnation, it will match this one's generation too.
  process.sleep(1000)
  let begin_at = monotonic_ms()
  let fresh_reply = process.new_subject()
  queue.begin_shutdown_for_test(consumer, fresh_reply)
  |> should.equal(Ok(Nil))

  let outcome = process.receive(fresh_reply, within: shutdown_grace_ms + 3000)
  let elapsed_ms = monotonic_ms() - begin_at

  outcome |> should.equal(Ok(queue.ShutdownForced(1)))
  // A correct implementation cannot report forced before its own
  // `shutdown_grace_ms` has elapsed since `begin_at`, so its `elapsed_ms` is
  // always close to `shutdown_grace_ms` (only ordinary scheduling/delivery
  // jitter below it). Under the pre-fix bug, the first incarnation's
  // orphaned generation-1 timer fires at a fixed point in time set long
  // before `begin_at` (when that incarnation's own drain began), so its
  // contribution to `elapsed_ms` is `shutdown_grace_ms - (time already
  // spent on the kill, restart, and resubmit above, plus the 1000ms sleep)`
  // — structurally at most `shutdown_grace_ms - 1000`, however fast that
  // setup runs, since the 1000ms sleep alone already accounts for that much
  // of the gap. The threshold below sits with an ordinary jitter margin
  // under the correct value and a hard structural margin (not a jitter
  // margin) above the bug's own ceiling: it can only fail to catch the bug
  // if that setup work took under 400ms, which it does not in practice.
  { elapsed_ms >= shutdown_grace_ms - 600 } |> should.equal(True)

  postgres.state(database, first_handle) |> should.equal(Ok(job.Executing))
  postgres.state(database, second_handle) |> should.equal(Ok(job.Executing))
  // Clears the way for `stop`'s own final drain (in the deferred cleanup
  // above) to reach a fresh, idle third incarnation and return promptly
  // instead of waiting out another full grace period for the still-blocked
  // second worker.
  process.kill(second_coordinator)
  mark_database_test_executed(
    "stale-shutdown-grace-timer-scoped-to-incarnation",
  )
}

fn run_stale_consumer_handle_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_stale_consumer_handle")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stale-consumer-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stale-consumer-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("stale.consumer", "v1", input_codec, output_codec, fn(value) {
      Ok("generation-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("stale-consumer")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "stale-consumer", definition, 1)
  let assert Ok(first_consumer) = queue.start_manual(database, workers)
  let _ = queue.stop(first_consumer)
  let assert Ok(second_consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(second_consumer) })

  queue.process_one(first_consumer)
  |> should.equal(Error(queue.QueueActorExited))
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  queue.process_one(second_consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("stale-consumer-handle-rejected")
}

pub fn postgres_manual_wait_survives_a_handler_longer_than_thirty_seconds_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_long_handler_wait_test(database_url)
  }
}

fn run_long_handler_wait_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_long_handler")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("long-handler-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("long-handler-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let finished = process.new_subject()
  let assert Ok(definition) =
    worker.define("long.handler", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, LongHandlerStarted(release))
      let _ = process.receive(release, within: 60_000)
      process.send(finished, Nil)
      Ok("long-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("long-handler")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "long-handler", definition, 9)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let caller =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let monitor = process.monitor(caller)
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(release, ReleaseAttempt)
    let _ = process.receive(finished, within: 5000)
  })
  let selector =
    process.new_selector()
    |> process.select_map(reply, fn(value) { LongCallReturned(value) })
    |> process.select_monitors(fn(down) { LongCallDown(down) })
  // process_one has no implicit 30-second deadline. If its caller exits due
  // the OTP call timeout while this valid worker is active, the monitor wins.
  process.selector_receive(selector, within: 31_000)
  |> should.equal(Error(Nil))
  process.send(release, ReleaseAttempt)
  process.receive(finished, within: 5000) |> should.equal(Ok(Nil))
  process.selector_receive(selector, within: 5000)
  |> should.equal(Ok(LongCallReturned(Ok(True))))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  let _ = process.demonitor_process(monitor)
  mark_database_test_executed("long-handler-wait-passed")
}

@external(erlang, "grind_test_env", "database_url")
fn database_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "queue_database_url")
fn queue_database_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "owner_a_url")
fn owner_a_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "owner_b_url")
fn owner_b_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_bad_url")
fn schema_bad_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_fresh_url")
fn schema_fresh_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_markers_url")
fn schema_markers_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_jobs_url")
fn schema_missing_jobs_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_migrations_url")
fn schema_missing_migrations_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_resolutions_url")
fn schema_missing_resolutions_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_acknowledgements_url")
fn schema_missing_acknowledgements_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_attempt_sequence_url")
fn schema_missing_attempt_sequence_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_atomic_url")
fn schema_atomic_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "resolution_route_a_url")
fn resolution_route_a_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "resolution_route_b_url")
fn resolution_route_b_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "mark_database_test_executed")
fn mark_database_test_executed(contract: String) -> Nil

@external(erlang, "grind_test_env", "monotonic_ms")
fn monotonic_ms() -> Int

pub fn postgres_admission_round_trips_typed_arguments_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_postgres_admission_test(database_url)
  }
}

fn run_postgres_admission_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_test_pool")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("integer-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("text-output-v1", json.string, decode.string)
  let assert Ok(counter) =
    worker.define(
      "counter.increment",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value + 1)) },
    )
  let assert Ok(handle) = postgres.submit(database, "default", counter, 41)

  postgres.arguments(database, handle)
  |> should.equal(Ok(41))

  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET worker_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("v2"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)
  postgres.arguments(database, handle)
  |> should.equal(
    Error(postgres.WorkerContractMismatch(
      expected_id: "counter.increment",
      expected_version: "v1",
      actual_id: "counter.increment",
      actual_version: "v2",
    )),
  )
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET worker_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("v1"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)

  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET input_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("integer-input-v2"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)
  postgres.arguments(database, handle)
  |> should.equal(
    Error(
      postgres.ArgumentCodecFailed(worker.CodecVersionMismatch(
        expected: "integer-input-v1",
        got: "integer-input-v2",
      )),
    ),
  )
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET input_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("integer-input-v1"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)

  postgres.state(database, handle)
  |> should.equal(Ok(job.Queued))
  postgres.close(database)
  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  postgres.state(reopened, handle)
  |> should.equal(Ok(job.Queued))
  mark_database_test_executed("admission-read-passed")
}

pub fn postgres_handles_are_bound_to_storage_owner_test() {
  case owner_a_url(), owner_b_url() {
    Ok(database_url_a), Ok(database_url_b) ->
      run_storage_owner_test(database_url_a, database_url_b)
    _, _ -> Nil
  }
}

fn run_storage_owner_test(
  database_url_a: String,
  database_url_b: String,
) -> Nil {
  let assert Ok(validated_a) =
    postgres.settings(database_url_a, process.new_name("grind_owner_a"))
    |> postgres.validate
  let assert Ok(database_a) = postgres.start(validated_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(validated_b) =
    postgres.settings(database_url_b, process.new_name("grind_owner_b"))
    |> postgres.validate
  let assert Ok(database_b) = postgres.start(validated_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(Nil) = postgres.migrate(database_b)
  let assert Ok(input_codec) =
    worker.codec("integer-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("text-output-v1", json.string, decode.string)
  let assert Ok(counter) =
    worker.define(
      "counter.increment",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value + 1)) },
    )
  let assert Ok(handle_a) = postgres.submit(database_a, "default", counter, 41)
  let assert Ok(_handle_b) = postgres.submit(database_b, "default", counter, 42)

  postgres.arguments(database_b, handle_a)
  |> should.equal(Error(postgres.StorageOwnerMismatch))
  postgres.state(database_b, handle_a)
  |> should.equal(Error(postgres.StateStorageOwnerMismatch))
  mark_database_test_executed("storage-owner-passed")
}

pub fn postgres_migration_rejects_incompatible_existing_schema_test() {
  case schema_bad_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_incompatible_schema_test(database_url)
  }
}

pub fn postgres_migration_installs_schema_v10_test() {
  case schema_fresh_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_v10_install_test(database_url)
  }
}

fn run_schema_v10_install_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_v10")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = pog.named_connection(pool_name)
  let assert Ok(schema) =
    pog.query(
      "SELECT (SELECT count(*) = 1 AND min(version) = 10 AND max(version) = 10 FROM grind_schema_migrations), (SELECT count(*) = 4 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname IN ('grind_schema_migrations', 'grind_jobs', 'grind_job_resolutions', 'grind_job_acknowledgements') AND c.relkind = 'r'), (SELECT count(*) = 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_sequence s ON s.seqrelid = c.oid WHERE n.nspname = current_schema() AND c.relname = 'grind_attempts_id_seq' AND c.relkind = 'S' AND s.seqtypid = 'bigint'::regtype AND s.seqstart = 1 AND s.seqincrement = 1 AND s.seqmin = 1 AND s.seqcache = 1 AND NOT s.seqcycle), (SELECT count(*) = 13 AND count(*) FILTER (WHERE column_name IN ('storage_owner', 'command_id', 'queue', 'job_id', 'worker_id', 'worker_version', 'attempt_id', 'attempt_epoch', 'attempt_owner', 'committed_state', 'failure_cause', 'committed_at', 'proposal_sha256')) = 13 AND count(*) FILTER (WHERE column_name IN ('proposed_state', 'output', 'output_version', 'error', 'error_version', 'failure_description', 'committed_description', 'requested_delay_ms')) = 0 AND count(*) FILTER (WHERE column_name = 'proposal_sha256' AND udt_name = 'bytea') = 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_job_acknowledgements'), (SELECT count(*) = 12 FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND c.convalidated AND c.conname IN ('grind_schema_migrations_pkey', 'grind_jobs_pkey', 'grind_jobs_state_check', 'grind_jobs_max_attempts_check', 'grind_job_resolutions_pkey', 'grind_job_resolutions_decision_check', 'grind_job_resolutions_target_state_check', 'grind_job_acknowledgements_pkey', 'grind_job_acknowledgements_attempt_key', 'grind_job_acknowledgements_committed_state_check', 'grind_job_acknowledgements_failure_cause_check', 'grind_job_acknowledgements_proposal_sha256_check'))",
    )
    |> pog.returning({
      use version <- decode.field(0, decode.bool)
      use tables <- decode.field(1, decode.bool)
      use sequence <- decode.field(2, decode.bool)
      use receipt_columns <- decode.field(3, decode.bool)
      use constraints <- decode.field(4, decode.bool)
      decode.success(#(version, tables, sequence, receipt_columns, constraints))
    })
    |> pog.execute(on: connection)
  let assert [installed] = schema.rows
  installed |> should.equal(#(True, True, True, True, True))

  let assert Ok(sequence) =
    pog.query("SELECT nextval('grind_attempts_id_seq')")
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      decode.success(attempt_id)
    })
    |> pog.execute(on: connection)
  let assert [attempt_id] = sequence.rows
  let assert Ok(inserted_job) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, output, state, available_at) VALUES ('schema-owner', 'default', 'schema.worker', 'v1', 'schema-input-v1', '1'::jsonb, 'schema-output-v1', '\"kept\"'::jsonb, 'succeeded', clock_timestamp()) RETURNING id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  let assert [job_id] = inserted_job.rows
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('schema-owner', 'schema-command', 'default', $1, 'schema.worker', 'v1', $2, 1, 'schema-attempt-owner', 'succeeded', sha256(convert_to('synthetic proposal', 'UTF8'))) ",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.execute(on: connection)
  let assert Ok(before) =
    pog.query("SELECT last_value, is_called FROM grind_attempts_id_seq")
    |> pog.returning({
      use last_value <- decode.field(0, decode.int)
      use is_called <- decode.field(1, decode.bool)
      decode.success(#(last_value, is_called))
    })
    |> pog.execute(on: connection)
  let assert [sequence_before] = before.rows

  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  let assert Ok(preserved) =
    pog.query(
      "SELECT (SELECT count(*) = 1 AND min(version) = 10 AND max(version) = 10 FROM grind_schema_migrations), (SELECT count(*) = 1 FROM grind_jobs WHERE id = $1 AND state = 'succeeded'), (SELECT count(*) = 1 FROM grind_job_acknowledgements WHERE command_id = 'schema-command' AND job_id = $1 AND attempt_id = $2 AND proposal_sha256 = sha256(convert_to('synthetic proposal', 'UTF8'))), (SELECT last_value = $2 AND is_called FROM grind_attempts_id_seq)",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.returning({
      use version <- decode.field(0, decode.bool)
      use job <- decode.field(1, decode.bool)
      use receipt <- decode.field(2, decode.bool)
      use sequence <- decode.field(3, decode.bool)
      decode.success(#(version, job, receipt, sequence))
    })
    |> pog.execute(on: connection)
  let assert [preserved_data] = preserved.rows
  preserved_data |> should.equal(#(True, True, True, True))
  let assert Ok(after) =
    pog.query("SELECT last_value, is_called FROM grind_attempts_id_seq")
    |> pog.returning({
      use last_value <- decode.field(0, decode.int)
      use is_called <- decode.field(1, decode.bool)
      decode.success(#(last_value, is_called))
    })
    |> pog.execute(on: connection)
  after.rows |> should.equal([sequence_before])
  mark_database_test_executed(
    "schema-v10-conservative-recovery-installed-and-idempotent",
  )
}

pub fn postgres_migration_rejects_legacy_and_future_markers_test() {
  case schema_markers_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_marker_rejection_test(database_url)
  }
}

fn run_schema_marker_rejection_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_markers")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_schema_migrations (version) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)",
    )
    |> pog.execute(on: connection)
  let assert Error(_) = postgres.migrate(database)
  let assert Ok(legacy_marker) =
    pog.query(
      "SELECT count(*)::bigint, min(version)::bigint, max(version)::bigint FROM grind_schema_migrations",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      use minimum <- decode.field(1, decode.int)
      use maximum <- decode.field(2, decode.int)
      decode.success(#(count, minimum, maximum))
    })
    |> pog.execute(on: connection)
  legacy_marker.rows |> should.equal([#(9, 1, 9)])

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (8)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(8)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (9)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(9)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (10)")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Ok(Nil))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (11)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(11)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (10)")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (8)")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  mark_database_test_executed("legacy-future-schema-markers-rejected")
}

pub fn postgres_migration_refuses_missing_owned_artifacts_test() {
  case
    schema_missing_jobs_url(),
    schema_missing_migrations_url(),
    schema_missing_resolutions_url(),
    schema_missing_acknowledgements_url(),
    schema_missing_attempt_sequence_url()
  {
    Ok(jobs),
      Ok(migrations),
      Ok(resolutions),
      Ok(acknowledgements),
      Ok(sequence)
    -> {
      run_missing_schema_artifact_test(jobs, "grind_jobs", False)
      run_missing_schema_artifact_test(
        migrations,
        "grind_schema_migrations",
        False,
      )
      run_missing_schema_artifact_test(
        resolutions,
        "grind_job_resolutions",
        False,
      )
      run_missing_schema_artifact_test(
        acknowledgements,
        "grind_job_acknowledgements",
        False,
      )
      run_missing_schema_artifact_test(sequence, "grind_attempts_id_seq", True)
      mark_database_test_executed("missing-schema-artifacts-not-repaired")
    }
    _, _, _, _, _ -> Nil
  }
}

fn run_missing_schema_artifact_test(
  database_url: String,
  artifact: String,
  is_sequence: Bool,
) -> Nil {
  let pool_name = process.new_name("grind_schema_partial")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = pog.named_connection(pool_name)
  let drop_statement = case is_sequence {
    True -> "DROP SEQUENCE " <> artifact
    False -> "DROP TABLE " <> artifact
  }
  let assert Ok(_) = pog.query(drop_statement) |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(missing) =
    pog.query("SELECT to_regclass(current_schema() || '.' || $1) IS NULL")
    |> pog.parameter(pog.text(artifact))
    |> pog.returning({
      use absent <- decode.field(0, decode.bool)
      decode.success(absent)
    })
    |> pog.execute(on: connection)
  missing.rows |> should.equal([True])
}

pub fn postgres_migration_fresh_install_is_atomic_test() {
  case schema_atomic_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_atomic_install_test(database_url)
  }
}

fn run_schema_atomic_install_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_atomic")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION fail_grind_ack_table_creation() RETURNS event_trigger LANGUAGE plpgsql AS $body$ DECLARE command record; BEGIN FOR command IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP IF command.object_identity LIKE '%grind_job_acknowledgements' THEN RAISE EXCEPTION 'injected Grind schema failure'; END IF; END LOOP; END $body$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE EVENT TRIGGER fail_grind_ack_table_creation ON ddl_command_end EXECUTE FUNCTION fail_grind_ack_table_creation()",
    )
    |> pog.execute(on: connection)
  let assert Error(_) = postgres.migrate(database)
  let assert Ok(_) =
    pog.query("DROP EVENT TRIGGER fail_grind_ack_table_creation")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP FUNCTION fail_grind_ack_table_creation()")
    |> pog.execute(on: connection)
  let assert Ok(rolled_back) =
    pog.query(
      "SELECT count(*)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname IN ('grind_schema_migrations', 'grind_jobs', 'grind_job_resolutions', 'grind_job_acknowledgements', 'grind_attempts_id_seq')",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  rolled_back.rows |> should.equal([0])
  mark_database_test_executed("failed-fresh-install-rolled-back")
}

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

pub fn postgres_worker_snooze_commits_scheduled_state_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_snooze_test(database_url)
  }
}

pub fn postgres_automatic_queue_skips_incompatible_job_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_queue_fairness_test(database_url)
  }
}

pub fn postgres_queue_policy_limits_jobs_per_tick_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_queue_batch_policy_test(database_url)
  }
}

pub fn postgres_scheduled_jobs_observe_database_due_time_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_scheduled_due_time_test(database_url)
  }
}

pub fn postgres_automatic_consumer_wakes_for_database_deadline_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_wakeup_test(database_url)
  }
}

pub fn postgres_queue_renews_running_attempt_before_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_lease_renewal_test(database_url)
  }
}

pub fn postgres_expired_renewal_keeps_worker_fenced_and_returns_proposal_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_lease_renewal_loss_test(database_url)
  }
}

pub fn postgres_renewal_storage_error_is_unknown_then_retried_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_renewal_storage_error_test(database_url)
  }
}

pub fn postgres_closed_pool_renewal_recovers_without_rerun_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_closed_pool_renewal_test(database_url)
  }
}

pub fn postgres_known_worker_start_failure_releases_unstarted_claim_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_start_failure_test(database_url)
  }
}

pub fn postgres_temporary_worker_death_quarantines_without_replay_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_temporary_worker_death_test(database_url)
  }
}

pub fn postgres_independent_consumers_compete_for_one_live_claim_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_independent_consumer_claim_test(database_url)
  }
}

pub fn postgres_overlapping_claim_transactions_respect_skip_locked_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_overlapping_claim_test(database_url)
  }
}

pub fn postgres_dead_idle_worker_is_released_before_activation_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_dead_idle_worker_test(database_url)
  }
}

fn run_dead_idle_worker_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_dead_idle_worker")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("dead-idle-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("dead-idle-output-v1", json.string, decode.string)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("dead.idle", "v1", input_codec, output_codec, fn(value) {
      process.send(invoked, WorkerInvoked)
      Ok("activated-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("dead-idle")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) = postgres.submit(database, "dead-idle", definition, 17)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.kill_next_worker_before_monitor(consumer) |> should.equal(Ok(Nil))
  queue.process_one(consumer)
  |> should.equal(Error(queue.QueueWorkerExitedBeforeActivation))
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))
  mark_database_test_executed("dead-idle-worker-claim-released")
}

pub fn postgres_consumer_enforces_configured_capacity_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_capacity_test(database_url)
  }
}

pub fn postgres_automatic_consumer_fills_only_available_slots_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_consumer_capacity_test(database_url)
  }
}

fn run_consumer_capacity_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_consumer_capacity")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("capacity-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("capacity-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("capacity.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CapacityWorkerStarted(value, release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("capacity-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("consumer-capacity")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "consumer-capacity", definition, 1)
  let assert Ok(second_handle) =
    postgres.submit(database, "consumer-capacity", definition, 2)
  let assert Ok(third_handle) =
    postgres.submit(database, "consumer-capacity", definition, 3)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(2)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let first_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(first_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, first_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(first_release, ReleaseAttempt)
    Nil
  })
  let second_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(second_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, second_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(second_release, ReleaseAttempt)
    Nil
  })
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  process.receive(started, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Queued))

  process.send(first_release, ReleaseAttempt)
  process.send(second_release, ReleaseAttempt)
  process.receive(first_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.receive(second_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, first_handle) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, second_handle) |> should.equal(Ok(job.Succeeded))
  let third_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(third_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, third_release)) =
    process.receive(started, within: 5000)
  process.send(third_release, ReleaseAttempt)
  process.receive(third_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("consumer-capacity-two-enforced")
}

fn run_automatic_consumer_capacity_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_auto_capacity")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-capacity-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-capacity-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("auto.capacity", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CapacityWorkerStarted(value, release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("automatic-capacity-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("consumer-capacity-auto")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 11)
  let assert Ok(second_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 12)
  let assert Ok(third_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 13)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(60_000)
    |> queue.with_maximum_jobs_per_poll(3)
    |> queue.with_maximum_concurrency(2)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let assert Ok(CapacityWorkerStarted(11, first_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(first_release, ReleaseAttempt)
    Nil
  })
  let assert Ok(CapacityWorkerStarted(12, second_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(second_release, ReleaseAttempt)
    Nil
  })
  process.receive(started, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Queued))

  // Completing one child releases one local slot, which the same automatic
  // poll batch fills while the other child remains blocked.
  process.send(first_release, ReleaseAttempt)
  let assert Ok(CapacityWorkerStarted(13, third_release)) =
    process.receive(started, within: 5000)
  process.send(second_release, ReleaseAttempt)
  process.send(third_release, ReleaseAttempt)
  wait_for_job_state(database, first_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait_for_job_state(database, second_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait_for_job_state(database, third_handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed("automatic-consumer-capacity-two-enforced")
}

fn wait_for_job_state(
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
          wait_for_job_state(database, handle, expected, remaining_checks - 1)
        }
        False, False -> False
      }
    Error(_) -> False
  }
}

fn run_independent_consumer_claim_test(database_url: String) -> Nil {
  let pool_a = process.new_name("grind_claim_race_a")
  let pool_b = process.new_name("grind_claim_race_b")
  let assert Ok(settings_a) =
    postgres.settings(database_url, pool_a) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_url, pool_b) |> postgres.validate
  let assert Ok(database_a) = postgres.start(settings_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(database_b) = postgres.start(settings_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(input_codec) =
    worker.codec("claim-race-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("claim-race-output-v1", json.string, decode.string)
  let signals = process.new_subject()
  let assert Ok(definition) =
    worker.define("claim.race", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, ConcurrentClaimWorkerStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("single-owner-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers_a) = registry.new("claim-race")
  let assert Ok(workers_a) = registry.register(workers_a, definition)
  let assert Ok(workers_b) = registry.new("claim-race")
  let assert Ok(workers_b) = registry.register(workers_b, definition)
  let assert Ok(handle) =
    postgres.submit(database_a, "claim-race", definition, 44)
  let assert Ok(consumer_a) = queue.start_manual(database_a, workers_a)
  use <- exception.defer(fn() { queue.stop(consumer_a) })
  let assert Ok(consumer_b) = queue.start_manual(database_b, workers_b)
  use <- exception.defer(fn() { queue.stop(consumer_b) })

  let ready = process.new_subject()
  let results = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let go = process.new_subject()
      process.send(ready, #("a", go))
      let _ = process.receive(go, within: 10_000)
      process.send(results, #("a", queue.process_one(consumer_a)))
    })
  let _ =
    process.spawn_unlinked(fn() {
      let go = process.new_subject()
      process.send(ready, #("b", go))
      let _ = process.receive(go, within: 10_000)
      process.send(results, #("b", queue.process_one(consumer_b)))
    })
  let assert Ok(first_ready) = process.receive(ready, within: 1000)
  let assert Ok(second_ready) = process.receive(ready, within: 1000)
  case first_ready, second_ready {
    #("a", go_a), #("b", go_b) -> {
      process.send(go_a, Nil)
      process.send(go_b, Nil)
    }
    #("b", go_b), #("a", go_a) -> {
      process.send(go_a, Nil)
      process.send(go_b, Nil)
    }
    _, _ -> panic as "unexpected concurrent claim synchronization messages"
  }

  let assert Ok(ConcurrentClaimWorkerStarted(release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    process.send(release, ReleaseAttempt)
    Nil
  })
  case process.receive(results, within: 5000) {
    Ok(#(_, Ok(False))) -> Nil
    other -> other |> should.equal(Ok(#("no-second-owner", Ok(False))))
  }
  process.receive(signals, within: 0) |> should.equal(Error(Nil))
  process.send(release, ReleaseAttempt)
  case process.receive(results, within: 5000) {
    Ok(#(_, Ok(True))) -> Nil
    other -> other |> should.equal(Ok(#("no-winner", Ok(True))))
  }
  postgres.state(database_a, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(signals, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("independent-consumers-single-live-claim")
}

fn run_overlapping_claim_test(database_url: String) -> Nil {
  let pool_a = process.new_name("grind_claim_overlap_a")
  let pool_b = process.new_name("grind_claim_overlap_b")
  let assert Ok(settings_a) =
    postgres.settings(database_url, pool_a) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_url, pool_b) |> postgres.validate
  let assert Ok(database_a) = postgres.start(settings_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(database_b) = postgres.start(settings_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(input_codec) =
    worker.codec("claim-overlap-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("claim-overlap-output-v1", json.string, decode.string)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("claim.overlap", "v1", input_codec, output_codec, fn(value) {
      process.send(invoked, value)
      Ok("overlap-" <> int.to_string(value))
    })
  let assert Ok(workers_a) = registry.new("claim-overlap")
  let assert Ok(workers_a) = registry.register(workers_a, definition)
  let assert Ok(workers_b) = registry.new("claim-overlap")
  let assert Ok(workers_b) = registry.register(workers_b, definition)
  let assert Ok(handle) =
    postgres.submit(database_a, "claim-overlap", definition, 45)

  let connection = pog.named_connection(pool_a)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_claim_overlap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND NEW.state = 'executing' THEN PERFORM pg_advisory_xact_lock(74126, 31); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_claim_overlap BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION grind_test_claim_overlap()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS grind_test_claim_overlap ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_claim_overlap()")
      |> pog.execute(on: connection)
    Nil
  })

  let lock_ready = process.new_subject()
  let lock_finished = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let release_lock = process.new_subject()
      let transaction_result =
        pog.transaction(connection, fn(transaction_connection) {
          case
            pog.query(
              "SELECT 1 FROM (SELECT pg_advisory_xact_lock(74126, 31)) AS held",
            )
            |> pog.execute(on: transaction_connection)
          {
            Error(_) -> Error(Nil)
            Ok(_) -> {
              process.send(lock_ready, ClaimGateAcquired(release_lock))
              case process.receive(release_lock, within: 10_000) {
                Ok(ReleaseAttempt) -> Ok(Nil)
                Error(Nil) -> Error(Nil)
              }
            }
          }
        })
      process.send(
        lock_finished,
        ClaimGateReleased(result.is_ok(transaction_result)),
      )
    })
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)

  let assert Ok(consumer_a) = queue.start_manual(database_a, workers_a)
  use <- exception.defer(fn() { queue.stop(consumer_a) })
  let assert Ok(consumer_b) = queue.start_manual(database_b, workers_b)
  use <- exception.defer(fn() { queue.stop(consumer_b) })
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let first_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(first_reply, queue.process_one(consumer_a))
    })
  await_claim_waiting_on_advisory(connection, 250) |> should.equal(True)

  // The first production claim has selected and locked the row, then blocks
  // in its UPDATE trigger. The second independent pool must skip that row.
  queue.process_one(consumer_b) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))
  process.receive(first_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.receive(invoked, within: 1000) |> should.equal(Ok(45))
  postgres.state(database_a, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("overlapping-claims-skip-locked")
}

fn await_claim_waiting_on_advisory(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Bool {
  let waiting =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND query LIKE 'WITH candidate AS (%')",
    )
    |> pog.returning({
      use waiting <- decode.field(0, decode.bool)
      decode.success(waiting)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [waiting] -> Ok(waiting)
        _ -> Error(Nil)
      }
    })
  case waiting {
    Ok(True) -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_claim_waiting_on_advisory(connection, checks_remaining - 1)
        }
        False -> False
      }
  }
}

fn run_temporary_worker_death_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_worker_death")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("worker-death-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("worker-death-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("worker.death", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, WorkerDeathStarted(process.self(), release))
      process.send(invoked, WorkerInvoked)
      case process.receive(release, within: 30_000) {
        Ok(ReleaseAttempt) -> Ok("must-not-commit-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("worker-death")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "worker-death", definition, 31)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(WorkerDeathStarted(worker_pid, release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(release, ReleaseAttempt)
    Nil
  })
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))
  process.kill(worker_pid)
  process.receive(reply, within: 5000)
  |> should.equal(Ok(Error(queue.QueueWorkerExited)))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("temporary-worker-death-quarantined-no-replay")
}

pub fn postgres_acknowledgement_persists_a_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_receipt_test(database_url)
  }
}

pub fn postgres_ack_commit_connection_loss_is_unknown_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_commit_connection_loss_test(database_url)
  }
}

fn run_ack_commit_connection_loss_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_ack_commit_loss")
  let settings = postgres.settings(database_url, pool_name)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ack-commit-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-commit-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("ack.commit.loss", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      process.send(invoked, WorkerInvoked)
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("terminated-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("ack-commit-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-commit-loss", definition, 21)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let connection = pog.named_connection(pool_name)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_kill_ack_backend() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_ack_backend AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_ack_backend()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_ack_backend ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_kill_ack_backend()")
      |> pog.execute(on: connection)
    Nil
  })

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 100)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckUnknown(
    command_id,
    proposed,
  )))) = process.receive(reply, within: 10_000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "ack-commit-loss-output-v1",
    "\"terminated-21\"",
  ))
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(Error(postgres.AckReceiptNotFound))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("ack-commit-connection-loss-unknown-passed")
}

fn wait_for_commit_trigger_backend(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND state = 'active' AND wait_event = 'PgSleep' ORDER BY query_start DESC LIMIT 1",
    )
    |> pog.returning({
      use pid <- decode.field(0, decode.int)
      decode.success(pid)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [pid] -> Ok(pid)
        [] ->
          case checks_remaining > 0 {
            False -> Error(Nil)
            True -> {
              process.sleep(10)
              wait_for_commit_trigger_backend(connection, checks_remaining - 1)
            }
          }
        _ -> Error(Nil)
      }
  }
}

fn backend_pid_is_alive(connection: pog.Connection, pid: Int) -> Bool {
  let query =
    pog.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
    |> pog.parameter(pog.int(pid))
    |> pog.returning({
      use alive <- decode.field(0, decode.bool)
      decode.success(alive)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> True
    Ok(returned) ->
      case returned.rows {
        [alive] -> alive
        _ -> True
      }
  }
}

fn terminate_backend(connection: pog.Connection, pid: Int) -> Bool {
  pog.query("SELECT pg_terminate_backend($1)")
  |> pog.parameter(pog.int(pid))
  |> pog.returning({
    use terminated <- decode.field(0, decode.bool)
    decode.success(terminated)
  })
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    case returned.rows {
      [terminated] -> terminated
      _ -> False
    }
  })
  |> result.unwrap(False)
}

/// Blocks (bounded) until `pid` no longer appears in `pg_stat_activity`.
/// `pg_terminate_backend` only sends the termination signal and returns
/// immediately; it does not wait for the target to actually finish
/// committing and exit. Callers that need PostgreSQL's own commit-visibility
/// side effects (ProcArray removal) to have happened before they proceed —
/// rather than relying on incidentally observing the same backend's own
/// socket close, as the reconciling-from-receipt test does — must wait for
/// this instead of proceeding immediately after termination.
fn wait_for_backend_gone(
  connection: pog.Connection,
  pid: Int,
  checks_remaining: Int,
) -> Result(Nil, Nil) {
  case backend_pid_is_alive(connection, pid) {
    False -> Ok(Nil)
    True ->
      case checks_remaining > 0 {
        False -> Error(Nil)
        True -> {
          process.sleep(10)
          wait_for_backend_gone(connection, pid, checks_remaining - 1)
        }
      }
  }
}

fn wait_for_syncrep_trigger_backend(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND wait_event = 'SyncRep' ORDER BY query_start DESC LIMIT 1",
    )
    |> pog.returning({
      use pid <- decode.field(0, decode.int)
      decode.success(pid)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [pid] -> Ok(pid)
        [] ->
          case checks_remaining > 0 {
            False -> Error(Nil)
            True -> {
              process.sleep(10)
              wait_for_syncrep_trigger_backend(connection, checks_remaining - 1)
            }
          }
        _ -> Error(Nil)
      }
  }
}

/// Fails clearly, instead of an opaque `let assert` mismatch far from the
/// real cause, when the disposable cluster was not started the way the
/// Increment 2 lost-reply tests require it. Without
/// `synchronous_standby_names=grind_never_standby`, a transaction that
/// raises its own `synchronous_commit` to `on` would either commit
/// immediately (a real standby present) or never proceed past `SyncRep` at
/// all in a way these tests can distinguish from a hang.
fn require_syncrep_cluster_configured(connection: pog.Connection) -> Nil {
  let assert Ok(returned) =
    pog.query("SHOW synchronous_standby_names")
    |> pog.returning({
      use value <- decode.field(0, decode.string)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  case returned.rows {
    ["grind_never_standby"] -> Nil
    [other] -> {
      let message =
        "scripts/test-postgres.sh must start PostgreSQL with -c synchronous_standby_names=grind_never_standby -c synchronous_commit=local for the SyncRep-based lost-reply tests to be meaningful; synchronous_standby_names was \""
        <> other
        <> "\" instead"
      panic as message
    }
    _ ->
      panic as "could not read synchronous_standby_names from the test cluster; scripts/test-postgres.sh must start PostgreSQL with -c synchronous_standby_names=grind_never_standby -c synchronous_commit=local"
  }
}

/// Installs a deferred constraint trigger on `grind_job_acknowledgements`,
/// scoped to `job_id`, whose function raises only that one ack transaction's
/// `synchronous_commit` to `on` — see the Increment 2 tests below. Returns a
/// cleanup thunk for the caller to register with `exception.defer`, which
/// first terminates any backend this same trigger still has parked in
/// `SyncRep` (so a failing assertion earlier in the test cannot hang the
/// whole gate run waiting on a standby that will never connect) and caps the
/// DROP itself with a lock timeout before dropping the trigger and function.
fn install_syncrep_reply_trigger(
  connection: pog.Connection,
  name: String,
  job_id: Int,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM set_config('synchronous_commit', 'on', true); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> name
      <> " AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> name
      <> "()",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ = case wait_for_syncrep_trigger_backend(connection, 0) {
      Ok(stuck_pid) -> terminate_backend(connection, stuck_pid)
      Error(Nil) -> True
    }
    let _ = pog.query("SET lock_timeout = '2s'") |> pog.execute(on: connection)
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS " <> name <> " ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
}

fn stored_attempt_identity(
  connection: pog.Connection,
  job_id: Int,
) -> Result(#(Int, Int), Nil) {
  pog.query("SELECT attempt_id, attempt_epoch FROM grind_jobs WHERE id = $1")
  |> pog.parameter(pog.int(job_id))
  |> pog.returning({
    use attempt_id <- decode.field(0, decode.int)
    use epoch <- decode.field(1, decode.int)
    decode.success(#(attempt_id, epoch))
  })
  |> pog.execute(on: connection)
  |> result.replace_error(Nil)
  |> result.try(fn(returned) {
    case returned.rows {
      [row] -> Ok(row)
      _ -> Error(Nil)
    }
  })
}

/// Increment 2: a genuinely successful ack whose reply is lost after
/// PostgreSQL has already committed locally. The disposable cluster is
/// started with `synchronous_standby_names=grind_never_standby` and
/// `synchronous_commit=local` (scripts/test-postgres.sh), so an ordinary
/// commit stays local, but a deferred constraint trigger scoped to this
/// job's acknowledgement row raises this one transaction's own
/// `synchronous_commit` to `on` (session-local, `set_config(..., true)`)
/// just before COMMIT. Because the configured standby name never connects,
/// that COMMIT parks in PostgreSQL's `SyncRep` wait *after* its WAL record is
/// already locally flushed — genuinely committed, reply not yet sent.
/// Terminating that backend at that exact moment (observed by polling
/// `pg_stat_activity` for `wait_event = 'SyncRep'`) reproduces "PostgreSQL
/// committed, but the client's connection closed before it saw the reply"
/// without a TCP proxy or any production test hook: the client observes a
/// closed connection during COMMIT, exactly like the existing aborted-commit
/// test, but this time a receipt genuinely exists to reconcile from.
pub fn postgres_ack_committed_reply_lost_reconciles_from_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_ack_committed_reply_lost_reconciles_test(database_url)
  }
}

fn run_ack_committed_reply_lost_reconciles_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_ack_reply_lost")
  let settings = postgres.settings(database_url, pool_name)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = pog.named_connection(pool_name)
  require_syncrep_cluster_configured(connection)
  let assert Ok(input_codec) =
    worker.codec("ack-reply-lost-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-reply-lost-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("ack.reply.lost", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      process.send(invoked, WorkerInvoked)
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("reply-lost-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("ack-reply-lost")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-reply-lost", definition, 33)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let job_id = job.id_value(handle)
  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_syncrep_reply_lost",
    job_id,
  ))

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  process.receive(reply, within: 10_000) |> should.equal(Ok(Ok(True)))
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("reply-lost-33")))
  let assert Ok(#(attempt_id, epoch)) =
    stored_attempt_identity(connection, job_id)
  let command_id =
    postgres.acknowledgement_command_id(job_id, attempt_id, epoch)
  let assert Ok(postgres.AcknowledgementReceipt(committed_state:, ..)) =
    postgres.reconcile_acknowledgement(database, handle, command_id)
  committed_state |> should.equal(job.Succeeded)
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("ack-committed-reply-lost-reconciled-passed")
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
      run_ack_committed_reply_lost_with_store_unavailable_test(database_url)
  }
}

fn run_ack_committed_reply_lost_with_store_unavailable_test(
  database_url: String,
) -> Nil {
  let pool_name = process.new_name("grind_ack_reply_lost_unavailable")
  let settings = postgres.settings(database_url, pool_name)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_pool_name =
    process.new_name("grind_ack_reply_lost_unavailable_observer")
  let observer_settings =
    postgres.settings(database_url, observer_pool_name)
    |> postgres.pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = pog.named_connection(observer_pool_name)
  require_syncrep_cluster_configured(observer_connection)

  let assert Ok(input_codec) =
    worker.codec("ack-reply-lost-unavailable-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "ack-reply-lost-unavailable-output-v1",
      json.string,
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
  let assert Ok(workers) = registry.new("ack-reply-lost-unavailable")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-reply-lost-unavailable", definition, 34)
  let attempt_owner = "ack-reply-lost-unavailable-owner"
  // Claimed directly through the postgres-level API (not `queue`), so the
  // opaque `ClaimedJob`/`Execution` values stay in scope for the same-command
  // retry through `postgres.acknowledge_claim` after the pool is reopened,
  // below. `claim_one` itself does not block; only the worker's own handler
  // (invoked by `execute_claim`, in the spawned process) does.
  let assert Ok(Some(claimed)) =
    postgres.claim_one(
      database,
      "ack-reply-lost-unavailable",
      workers,
      attempt_owner,
      30_000,
    )
  let #(claimed_id, attempt_id, epoch) = postgres.claim_identity(claimed)
  let command_id =
    postgres.acknowledgement_command_id(claimed_id, attempt_id, epoch)
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let execution = postgres.execute_claim(claimed)
      let ack_result =
        postgres.acknowledge_claim(
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

  let job_id = job.id_value(handle)
  use <- exception.defer(install_syncrep_reply_trigger(
    observer_connection,
    "grind_test_syncrep_reply_lost_unavailable",
    job_id,
  ))

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) =
    wait_for_syncrep_trigger_backend(observer_connection, 300)

  postgres.close(database)

  let assert Ok(#(execution, ack_result)) =
    process.receive(reply, within: 10_000)
  execution
  |> should.equal(worker.ExecutedSuccess(
    "ack-reply-lost-unavailable-output-v1",
    "\"unavailable-34\"",
  ))
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
  committed_state |> should.equal(job.Succeeded)
  postgres.outcome(reopened, handle)
  |> should.equal(Ok(job.SucceededWith("unavailable-34")))

  // The same command, retried end to end through the reopened store: proves
  // idempotent replay, not just that the receipt can be read back.
  postgres.acknowledge_claim(
    reopened,
    "ack-reply-lost-unavailable",
    attempt_owner,
    claimed,
    execution,
  )
  |> should.equal(Ok(True))

  let assert Ok(fresh_consumer) = queue.start_manual(reopened, workers)
  use <- exception.defer(fn() {
    let _ = queue.stop(fresh_consumer)
    Nil
  })
  queue.process_one(fresh_consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed(
    "ack-committed-reply-lost-store-unavailable-unknown-passed",
  )
}

fn run_ack_receipt_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_ack_receipt")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ack-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-output-v1", json.string, decode.string)
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let assert Ok(Some(claimed)) =
    postgres.claim_one(
      database,
      "ack-receipt",
      workers,
      "ack-receipt-owner",
      30_000,
    )
  let execution = postgres.execute_claim(claimed)
  process.receive(invocation, within: 1000) |> should.equal(Ok(WorkerInvoked))
  postgres.acknowledge_claim(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    worker.ExecutedSuccess("ack-output-v2", "\"wrong-codec\""),
  )
  |> should.equal(
    Error(postgres.QueueAckProposalCodecMismatch(
      "output",
      "ack-output-v1",
      "ack-output-v2",
    )),
  )
  let #(claimed_id, attempt_id, epoch) = postgres.claim_identity(claimed)
  let command_id =
    postgres.acknowledgement_command_id(claimed_id, attempt_id, epoch)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_test_reject_ack CHECK (command_id <> '"
      <> command_id
      <> "')",
    )
    |> pog.execute(on: connection)
  let assert Error(_) =
    postgres.acknowledge_claim(
      database,
      "ack-receipt",
      "ack-receipt-owner",
      claimed,
      execution,
    )
  let assert Ok(after_failed_ack) =
    pog.query(
      "SELECT state, (SELECT count(*) FROM grind_job_acknowledgements WHERE storage_owner = $2 AND command_id = $3)::bigint FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.parameter(pog.text(postgres.storage_owner(database)))
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
  postgres.acknowledge_claim(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  // A retry with the same stable command and exact proposal is idempotent.
  postgres.acknowledge_claim(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  postgres.acknowledge_claim(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    worker.ExecutedSuccess("ack-output-v1", "\"tampered\""),
  )
  |> should.equal(Error(postgres.QueueAckCommandConflict))
  let assert Ok(receipts) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, command_id, attempt_owner, queue, worker_id, worker_version, committed_state, failure_cause, octet_length(proposal_sha256), to_char(committed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM grind_job_acknowledgements WHERE storage_owner = $1 AND job_id = $2",
    )
    |> pog.parameter(pog.text(postgres.storage_owner(database)))
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
      use committed_at <- decode.field(10, decode.string)
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
  committed_at |> should.not_equal("")
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: receipt_command,
    attempt_id: receipt_attempt,
    attempt_epoch: receipt_epoch,
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at: receipt_time,
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

fn run_lease_renewal_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_lease_renewal")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("renewal-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("renewal-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define("lease.renewal", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("finished-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("lease-renewal")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "lease-renewal", slow_worker, 7)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(1500)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  let connection = pog.named_connection(pool_name)
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let observed_renewal = case
    lease_expiration(connection, job.id_value(handle))
  {
    Ok(initial_expiry) ->
      await_later_lease_expiry(
        connection,
        job.id_value(handle),
        initial_expiry + 50,
        125,
      )
    Error(Nil) -> False
  }
  process.send(release, ReleaseAttempt)
  let assert Ok(Ok(True)) = process.receive(reply, within: 5000)
  observed_renewal |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("lease-renewal-before-ack-passed")
}

fn run_lease_renewal_loss_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_lease_renewal_loss")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("renewal-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("renewal-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.renewal.loss",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("lost-lease-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("lease-renewal-loss")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "lease-renewal-loss", slow_worker, 7)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(900)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  let connection = pog.named_connection(pool_name)
  force_lease_expired(connection, job.id_value(handle))
  |> should.equal(Ok(Nil))
  await_renewal_lost(consumer, 100) |> should.equal(True)
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))

  process.send(release, ReleaseAttempt)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
    proposed,
    postgres.AckLeaseExpired(..),
  )))) = process.receive(reply, within: 5000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "renewal-loss-output-v1",
    "\"lost-lease-7\"",
  ))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_database_test_executed("lease-renewal-loss-fenced-passed")
}

fn run_renewal_storage_error_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_renewal_storage_error")
  let settings = postgres.settings(database_url, pool_name)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("renewal-storage-error-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("renewal-storage-error-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.renewal.storage-error",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("reconnected-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("renewal-storage-error")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "renewal-storage-error", slow_worker, 18)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(1500)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  // A PostgreSQL trigger returns a real query error for lease renewal while
  // leaving the connection and coordinator alive. This exercises the storage
  // error result path without conflating it with process death.
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_reject_renewal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.state = 'executing' AND NEW.state = 'executing' AND NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at THEN RAISE EXCEPTION 'injected renewal query failure'; END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_reject_renewal BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION grind_test_reject_renewal()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_reject_renewal ON grind_jobs",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_reject_renewal()")
      |> pog.execute(on: connection)
    Nil
  })
  await_renewal_status(consumer, queue.LeaseRenewalUnknown, 100)
  |> should.equal(True)
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  let assert Ok(_) =
    pog.query("DROP TRIGGER grind_test_reject_renewal ON grind_jobs")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP FUNCTION grind_test_reject_renewal()")
    |> pog.execute(on: connection)
  await_renewal_status(consumer, queue.LeaseRenewalConfirmed, 100)
  |> should.equal(True)
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("renewal-storage-error-retried-passed")
}

fn run_closed_pool_renewal_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_closed_pool_renewal")
  let settings = postgres.settings(database_url, pool_name)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("closed-pool-renewal-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("closed-pool-renewal-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.closed-pool.renewal",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("recovered-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("closed-pool-renewal")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "closed-pool-renewal", slow_worker, 19)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(1500)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  // A missing named pool used to let pgo_pool:checkout exit through Pog and
  // kill the queue coordinator. The typed consumer must retain the active
  // claim, report uncertainty, and recover after the same pool is reopened.
  postgres.close(database)
  postgres.state(database, handle)
  |> should.equal(Error(postgres.StateQueryFailed(pog.ConnectionUnavailable)))
  await_renewal_status(consumer, queue.LeaseRenewalUnknown, 100)
  |> should.equal(True)
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  let assert Ok(reopened_database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened_database) })
  await_renewal_status(consumer, queue.LeaseRenewalConfirmed, 100)
  |> should.equal(True)
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(reopened_database, handle)
  |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("closed-pool-renewal-recovered-passed")
}

/// The owner and pool losses here are sequential, not concurrent: the pool is
/// only closed and reopened after the owner and its cascaded worker are both
/// confirmed dead, as recovery plumbing following that death, not as a second
/// failure landing during active work.
fn run_owner_loss_recovers_after_pool_restart_test(
  database_url: String,
) -> Nil {
  let pool_name = process.new_name("grind_owner_pool_loss")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("owner-pool-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("owner-pool-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("owner.pool.loss", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(invoked, WorkerInvoked)
      process.send(started, OwnerPoolLossStarted(process.self(), release))
      case process.receive(release, within: 20_000) {
        Ok(ReleaseAttempt) -> Ok("owner-pool-loss-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("owner-pool-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "owner-pool-loss", definition, 61)
  let job_id = job.id_value(handle)

  let owner_ready = process.new_subject()
  let owner =
    process.spawn_unlinked(fn() {
      let assert Ok(policy) =
        queue.default_policy()
        |> queue.with_poll_interval(20)
        |> queue.with_maximum_concurrency(1)
        |> queue.with_lease_duration(5000)
        |> queue.validate_policy
      case queue.start_with_policy(database, workers, policy) {
        Error(error) ->
          process.send(owner_ready, OwnerPoolLossOwnerFailed(error))
        Ok(consumer) -> {
          process.send(
            owner_ready,
            OwnerPoolLossOwnerReady(process.self(), consumer),
          )
          process.sleep(60_000)
        }
      }
    })
  let owner_monitor = process.monitor(owner)
  let assert Ok(OwnerPoolLossOwnerReady(started_owner, _consumer)) =
    process.receive(owner_ready, within: 5000)
  started_owner |> should.equal(owner)

  let assert Ok(OwnerPoolLossStarted(worker_pid, _release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))
  let worker_monitor = process.monitor(worker_pid)

  // The owner is not part of this test process's own supervision tree, so
  // killing it must be observed rather than assumed: the consumer's
  // top-level supervisor is linked to whichever process called
  // `queue.start_with_policy`, so the owner's death cascades down through
  // that supervisor, the coordinator, and the coordinator's own linked
  // worker factory, ending in the blocked worker's death too.
  process.kill(owner)
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(owner_monitor, fn(down) {
      #("owner", down)
    })
    |> process.select_specific_monitor(worker_monitor, fn(down) {
      #("worker", down)
    })
  // Collected order-independently: the owner's death and the worker's death
  // are two separate cascading events from this test's observation point,
  // and only their causal order (owner, then worker) is guaranteed, not the
  // order in which their DOWN messages are scheduled into this mailbox.
  let assert Ok(#(first_down_tag, _)) =
    process.selector_receive(down_selector, within: 5000)
  let assert Ok(#(second_down_tag, _)) =
    process.selector_receive(down_selector, within: 5000)
  { first_down_tag != second_down_tag } |> should.equal(True)
  { first_down_tag == "owner" || first_down_tag == "worker" }
  |> should.equal(True)
  { second_down_tag == "owner" || second_down_tag == "worker" }
  |> should.equal(True)

  postgres.close(database)
  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  let connection = pog.named_connection(pool_name)
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  let assert Ok(fresh_consumer) = queue.start_manual(reopened, workers)
  use <- exception.defer(fn() {
    let _ = queue.stop(fresh_consumer)
    Nil
  })

  queue.process_one(fresh_consumer) |> should.equal(Ok(False))
  postgres.state(reopened, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))

  let assert Ok(rebound) = postgres.bind_handle(reopened, definition, job_id)
  postgres.resolve_uncertain(
    reopened,
    rebound,
    "owner-pool-loss-authorized-replay",
    "on-call",
    "inspect the external effect before authorizing a new delivery",
    postgres.AuthorizeReplay,
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))

  let replay_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(replay_reply, queue.process_one(fresh_consumer))
    })
  let assert Ok(OwnerPoolLossStarted(_, replay_release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))
  process.send(replay_release, ReleaseAttempt)
  process.receive(replay_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(reopened, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  let assert Ok(#(final_attempt_count, final_delivery_count)) =
    attempt_accounting(connection, job_id)
  final_attempt_count |> should.equal(2)
  final_delivery_count |> should.equal(2)
  let _ = process.demonitor_process(owner_monitor)
  mark_database_test_executed("owner-loss-pool-restart-quarantined-no-replay")
}

fn run_worker_start_failure_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_worker_start_failure")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("start-failure-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("start-failure-output-v1", json.string, decode.string)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("start.failure", "v1", input_codec, output_codec, fn(value) {
      process.send(invoked, Nil)
      Ok("started-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("start-failure")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "start-failure", definition, 15)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'scheduled', available_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.fail_next_worker_start(consumer) |> should.equal(Ok(Nil))
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueWorkerStartFailed(actor.InitFailed("injected start failure")),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let connection = pog.named_connection(pool_name)
  attempt_count_for(connection, job.id_value(handle))
  |> should.equal(Ok(0))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(invoked, within: 0) |> should.equal(Ok(Nil))
  mark_database_test_executed("unstarted-worker-claim-released-passed")
}

fn attempt_count_for(connection: pog.Connection, id: Int) -> Result(Int, Nil) {
  let query =
    pog.query("SELECT attempt_count FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [count] -> Ok(count)
        _ -> Error(Nil)
      }
  }
}

fn await_renewal_lost(consumer: queue.Consumer, checks_remaining: Int) -> Bool {
  case queue.renewal_status(consumer) {
    Ok(Some(queue.LeaseRenewalLost)) -> True
    _ ->
      case checks_remaining > 0 {
        False -> False
        True -> {
          process.sleep(10)
          await_renewal_lost(consumer, checks_remaining - 1)
        }
      }
  }
}

fn await_renewal_status(
  consumer: queue.Consumer,
  expected: queue.RenewalStatus,
  checks_remaining: Int,
) -> Bool {
  case queue.renewal_status(consumer) {
    Ok(Some(actual)) ->
      case actual == expected {
        True -> True
        False -> retry_renewal_status(consumer, expected, checks_remaining)
      }
    _ -> retry_renewal_status(consumer, expected, checks_remaining)
  }
}

fn retry_renewal_status(
  consumer: queue.Consumer,
  expected: queue.RenewalStatus,
  checks_remaining: Int,
) -> Bool {
  case checks_remaining > 0 {
    False -> False
    True -> {
      process.sleep(10)
      await_renewal_status(consumer, expected, checks_remaining - 1)
    }
  }
}

fn force_lease_expired(
  connection: pog.Connection,
  id: Int,
) -> Result(Nil, Nil) {
  pog.query(
    "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.map(fn(_) { Nil })
}

fn lease_expiration(connection: pog.Connection, id: Int) -> Result(Int, Nil) {
  pog.query(
    "SELECT (extract(epoch FROM lease_expires_at) * 1000)::bigint FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use expiry <- decode.field(0, decode.int)
    decode.success(expiry)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [expiry] -> Ok(expiry)
      _ -> Error(Nil)
    }
  })
}

fn await_later_lease_expiry(
  connection: pog.Connection,
  id: Int,
  threshold: Int,
  remaining_checks: Int,
) -> Bool {
  case lease_expiration(connection, id) {
    Ok(expiry) ->
      case expiry > threshold, remaining_checks > 0 {
        True, _ -> True
        False, True -> {
          process.sleep(20)
          await_later_lease_expiry(
            connection,
            id,
            threshold,
            remaining_checks - 1,
          )
        }
        False, False -> False
      }
    Error(Nil) -> False
  }
}

fn run_scheduled_due_time_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_scheduled_boundary")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("scheduled-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("scheduled-output-v1", json.int, decode.int)
  let probe = process.new_subject()
  let assert Ok(scheduled_worker) =
    worker.define("scheduled.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(value)
    })
  let assert Ok(workers) = registry.new("scheduled-boundary")
  let assert Ok(workers) = registry.register(workers, scheduled_worker)
  let connection = pog.named_connection(pool_name)
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint + 60000",
    )
    |> pog.returning({
      use value <- decode.field(0, decode.int)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  let assert [future_unix_ms] = returned.rows
  let assert Ok(available_at) = job.available_at(future_unix_ms)
  let assert Ok(handle) =
    postgres.submit_at(
      database,
      "scheduled-boundary",
      scheduled_worker,
      17,
      available_at,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2 AND state = 'scheduled'",
    )
    |> pog.parameter(pog.text("scheduled.echo"))
    |> pog.parameter(pog.text("scheduled-boundary"))
    |> pog.execute(on: connection)

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(probe, within: 0) |> should.equal(Ok(WorkerInvoked))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("scheduled-due-time-passed")
}

/// Proves the automatic consumer actually waits for the database's own
/// clock to reach `available_at` before claiming, rather than merely being
/// eligible to run afterward. Honest limit: Grind has no LISTEN/NOTIFY
/// wakeup path (confirmed by reading the source — `claim_registered_job`
/// and the coordinator's self-scheduled `Poll` timer are the only ways a
/// row ever gets claimed; no PostgreSQL channel is ever subscribed to);
/// this proves wakeup via polling after the deadline elapses, not a
/// notification-driven wakeup. Two independent database-time observations
/// back this claim: (1) immediately after the consumer starts, the job is
/// still `Scheduled` and the database clock is still before `available_at`
/// — proving at least one pre-deadline poll tick genuinely skipped the row
/// (this assertion is not retried, so a too-slow environment fails it
/// honestly instead of silently passing); (2) the handler itself, at the
/// moment it actually runs, compares `available_at` against the row's own
/// recorded *claim* time (`lease_expires_at - lease_duration`) rather than
/// its own later `clock_timestamp()` call, so the observation is pinned to
/// when the claim SQL actually admitted the row, not to whatever moment the
/// handler happens to be scheduled afterward.
fn run_automatic_wakeup_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_automatic_wakeup")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-wakeup-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-wakeup-output-v1", json.bool, decode.bool)
  let connection = pog.named_connection(pool_name)
  let observed = process.new_subject()
  let lease_duration_ms = 5000
  let assert Ok(auto_worker) =
    worker.define("auto.wakeup", "v1", input_codec, output_codec, fn(_value) {
      let assert Ok(returned) =
        pog.query(
          "SELECT (lease_expires_at - ($1::double precision * interval '1 millisecond')) >= available_at FROM grind_jobs WHERE worker_id = 'auto.wakeup' AND queue = 'auto-wakeup'",
        )
        |> pog.parameter(pog.int(lease_duration_ms))
        |> pog.returning({
          use due <- decode.field(0, decode.bool)
          decode.success(due)
        })
        |> pog.execute(on: connection)
      let assert [due] = returned.rows
      process.send(observed, due)
      Ok(due)
    })
  let assert Ok(workers) = registry.new("auto-wakeup")
  let assert Ok(workers) = registry.register(workers, auto_worker)
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint + 300",
    )
    |> pog.returning({
      use value <- decode.field(0, decode.int)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  let assert [future_unix_ms] = returned.rows
  let assert Ok(available_at) = job.available_at(future_unix_ms)
  let assert Ok(handle) =
    postgres.submit_at(database, "auto-wakeup", auto_worker, 1, available_at)
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_lease_duration(lease_duration_ms)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  // Not retried: this proves a pre-deadline poll tick genuinely observed
  // the row as not-yet-due. A too-slow environment (already past the
  // ~300ms deadline by the time this runs) fails this assertion honestly
  // rather than the test silently skipping the proof.
  let assert Ok(pre_deadline) =
    pog.query(
      "SELECT clock_timestamp() < available_at FROM grind_jobs WHERE worker_id = 'auto.wakeup' AND queue = 'auto-wakeup'",
    )
    |> pog.returning({
      use before_deadline <- decode.field(0, decode.bool)
      decode.success(before_deadline)
    })
    |> pog.execute(on: connection)
  let assert [before_deadline] = pre_deadline.rows
  before_deadline |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  process.receive(observed, within: 5000) |> should.equal(Ok(True))
  wait_for_job_state(database, handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed("automatic-wakeup-database-deadline-passed")
}

pub fn postgres_manual_batch_reports_acknowledged_prefix_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_batch_partial_error_test(database_url)
  }
}

fn run_batch_partial_error_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_batch_partial")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("partial-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("partial-output-v1", json.int, decode.int)
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
  let connection = pog.named_connection(pool_name)
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
    |> queue.with_maximum_jobs_per_poll(3)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_available(consumer)
  |> should.equal(queue.BatchStopped(
    acknowledged_before_error: 1,
    error: queue.QueueProcessFailed(postgres.QueueCodecMismatch(
      kind: "output",
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

pub fn postgres_expired_attempt_requires_audited_replay_and_stale_ack_is_fenced_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_takeover_fencing_test(database_url)
  }
}

fn run_takeover_fencing_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_takeover_fence")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("takeover-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("takeover-output-v1", json.string, decode.string)
  let signals = process.new_subject()
  let assert Ok(first_worker) =
    worker.define("takeover.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, FirstAttemptStarted(release))
      case process.receive(release, within: 15_000) {
        Ok(ReleaseAttempt) -> Ok("obsolete-" <> int.to_string(value))
        Error(Nil) -> Error(Nil)
      }
    })
  let assert Ok(replay_worker) =
    worker.define("takeover.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, TakeoverAttemptStarted(release))
      case process.receive(release, within: 15_000) {
        Ok(ReleaseAttempt) -> Ok("current-" <> int.to_string(value))
        Error(Nil) -> Error(Nil)
      }
    })
  let assert Ok(first_registry) = registry.new("takeover-fence")
  let assert Ok(first_registry) =
    registry.register(first_registry, first_worker)
  let assert Ok(replay_registry) = registry.new("takeover-fence")
  let assert Ok(replay_registry) =
    registry.register(replay_registry, replay_worker)
  let assert Ok(first_consumer) = queue.start_manual(database, first_registry)
  use <- exception.defer(fn() { queue.stop(first_consumer) })
  let assert Ok(replay_consumer) = queue.start_manual(database, replay_registry)
  use <- exception.defer(fn() { queue.stop(replay_consumer) })
  let assert Ok(handle) =
    postgres.submit(database, "takeover-fence", first_worker, 7)
  let first_reply = process.new_subject()
  let first_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(first_reply, queue.process_one(first_consumer))
    })
  let assert Ok(FirstAttemptStarted(first_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(first_finished, first_release, first_reply)
  })

  let connection = pog.named_connection(pool_name)
  let assert Ok(first_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use attempt_count <- decode.field(3, decode.int)
      use delivery_count <- decode.field(4, decode.int)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        attempt_count,
        delivery_count,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#(first_attempt_id, first_epoch, first_owner, 1, 1)] =
    first_claim.rows
  first_epoch |> should.equal(1)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)

  replay_consumer
  |> queue.process_one
  |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  process.receive(signals, within: 0) |> should.equal(Error(Nil))

  postgres.resolve_uncertain(
    database,
    handle,
    "takeover-fence-authorized-replay",
    "on-call",
    "inspect the external effect before authorizing a new delivery",
    postgres.AuthorizeReplay,
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(authorized_accounting) =
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
  let assert [#(1, 1)] = authorized_accounting.rows

  let replay_reply = process.new_subject()
  let replay_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(replay_reply, queue.process_one(replay_consumer))
    })
  let assert Ok(TakeoverAttemptStarted(replay_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(replay_finished, replay_release, replay_reply)
  })
  let assert Ok(replay_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use attempt_count <- decode.field(3, decode.int)
      use delivery_count <- decode.field(4, decode.int)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        attempt_count,
        delivery_count,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#(replay_attempt_id, replay_epoch, replay_owner, 2, 2)] =
    replay_claim.rows
  replay_attempt_id |> should.not_equal(first_attempt_id)
  replay_epoch |> should.equal(first_epoch + 1)
  replay_owner |> should.not_equal(first_owner)

  process.send(first_release, ReleaseAttempt)
  process.receive(first_reply, within: 5000)
  |> should.equal(
    Ok(
      Error(
        queue.QueueProcessFailed(postgres.QueueAckStale(
          worker.ExecutedSuccess("takeover-output-v1", "\"obsolete-7\""),
          postgres.AckOwnershipChanged(
            state: "executing",
            attempt_id: Some(replay_attempt_id),
            epoch: Some(replay_epoch),
            owner: Some(replay_owner),
          ),
        )),
      ),
    ),
  )
  process.send(first_finished, Nil)

  process.send(replay_release, ReleaseAttempt)
  process.receive(replay_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.send(replay_finished, Nil)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("current-7")))
  mark_database_test_executed("expired-attempt-audited-replay-passed")
}

pub fn postgres_expired_attempt_requires_reconciliation_by_default_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_expired_attempt_quarantine_test(database_url)
  }
}

pub fn postgres_expiry_quarantine_is_bounded_per_attempt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_bounded_quarantine_test(database_url)
  }
}

pub fn postgres_acknowledgement_rejects_exact_database_expiry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_exact_expiry_test(database_url)
  }
}

pub fn postgres_ack_after_database_expiry_is_stale_without_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_after_database_expiry_test(database_url)
  }
}

pub fn postgres_uncertain_replay_requires_audited_resolution_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_uncertain_resolution_test(database_url)
  }
}

fn run_uncertain_resolution_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_uncertain_resolution")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolve-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolve-output-v1", json.string, decode.string)
  let invocations = process.new_subject()
  let assert Ok(worker) =
    worker.define("resolve.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(invocations, WorkerInvoked)
      Ok("resolved-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("uncertain-resolution")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(handle) =
    postgres.submit(database, "uncertain-resolution", worker, 12)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 121, attempt_epoch = 6, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))

  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-121",
    "on-call",
    "confirm external idempotency record before replay",
    postgres.AuthorizeReplay,
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-121",
    "on-call",
    "confirm external idempotency record before replay",
    postgres.AuthorizeReplay,
  )
  |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))
  let assert Ok(audit) =
    pog.query(
      "SELECT resolution_id, job_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at IS NOT NULL, decision, resolved_by, details FROM grind_job_resolutions WHERE job_id = $1 AND resolution_id = $2",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text("resolution-121"))
    |> pog.returning({
      use resolution_id <- decode.field(0, decode.string)
      use job_id <- decode.field(1, decode.int)
      use attempt_id <- decode.field(2, decode.int)
      use attempt_epoch <- decode.field(3, decode.int)
      use attempt_owner <- decode.field(4, decode.string)
      use expiry_retained <- decode.field(5, decode.bool)
      use decision <- decode.field(6, decode.string)
      use resolved_by <- decode.field(7, decode.string)
      use details <- decode.field(8, decode.string)
      decode.success(#(
        resolution_id,
        job_id,
        attempt_id,
        attempt_epoch,
        attempt_owner,
        expiry_retained,
        decision,
        resolved_by,
        details,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      resolution_id,
      job_id,
      attempt_id,
      attempt_epoch,
      attempt_owner,
      expiry_retained,
      decision,
      resolved_by,
      details,
    ),
  ] = audit.rows
  resolution_id |> should.equal("resolution-121")
  job_id |> should.equal(id)
  attempt_id |> should.equal(121)
  attempt_epoch |> should.equal(6)
  attempt_owner |> should.equal("lost-consumer")
  expiry_retained |> should.equal(True)
  decision |> should.equal("authorize_replay")
  resolved_by |> should.equal("on-call")
  details |> should.equal("confirm external idempotency record before replay")
  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(invocations, within: 0) |> should.equal(Ok(WorkerInvoked))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("resolved-12")))
  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-121",
    "on-call",
    "confirm external idempotency record before replay",
    postgres.AuthorizeReplay,
  )
  |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))
  mark_database_test_executed("audited-uncertain-resolution-passed")
}

pub fn postgres_resolution_command_binds_typed_payload_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_resolution_payload_test(database_url)
  }
}

pub fn postgres_resolution_rebind_checks_storage_owner_test() {
  case resolution_route_a_url(), resolution_route_b_url() {
    Ok(database_a_url), Ok(database_b_url) ->
      run_resolution_rebind_route_test(database_a_url, database_b_url)
    _, _ -> Nil
  }
}

fn run_resolution_rebind_route_test(
  database_a_url: String,
  database_b_url: String,
) -> Nil {
  let pool_a = process.new_name("grind_resolution_route_a")
  let pool_b = process.new_name("grind_resolution_route_b")
  let pool_a_after_restart =
    process.new_name("grind_resolution_route_a_rebound")
  let assert Ok(settings_a) =
    postgres.settings(database_a_url, pool_a) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_b_url, pool_b) |> postgres.validate
  let assert Ok(settings_a_after_restart) =
    postgres.settings(database_a_url, pool_a_after_restart) |> postgres.validate
  let assert Ok(database_a) = postgres.start(settings_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(database_b) = postgres.start(settings_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(database_a_after_restart) =
    postgres.start(settings_a_after_restart)
  use <- exception.defer(fn() { postgres.close(database_a_after_restart) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(Nil) = postgres.migrate(database_b)
  let assert Ok(input_codec) =
    worker.codec("route-recovery-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("route-recovery-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("route.recovery", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(other_worker) =
    worker.define("route.other", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(wrong_input_codec) =
    worker.codec("route-recovery-input-v2", json.int, decode.int)
  let assert Ok(wrong_codec_worker) =
    worker.define(
      "route.recovery",
      "v1",
      wrong_input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle_a) =
    postgres.submit(database_a, "route-recovery", definition, 3)
  let assert Ok(handle_b) =
    postgres.submit(database_b, "route-recovery", definition, 3)
  let durable_id = job.id_value(handle_a)
  job.id_value(handle_b) |> should.equal(durable_id)
  let connection_a = pog.named_connection(pool_a)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 302, attempt_epoch = 5, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp() WHERE storage_owner = $1 AND id = $2",
    )
    |> pog.parameter(pog.text(postgres.storage_owner(database_a)))
    |> pog.parameter(pog.int(durable_id))
    |> pog.execute(on: connection_a)
  postgres.bind_handle(database_a_after_restart, other_worker, durable_id)
  |> should.equal(Error(postgres.HandleBindWorkerContractMismatch))
  postgres.bind_handle(database_a_after_restart, wrong_codec_worker, durable_id)
  |> should.equal(Error(postgres.HandleBindCodecContractMismatch))
  let rebound = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(
        rebound,
        postgres.bind_handle(database_a_after_restart, definition, durable_id),
      )
    })
  let assert Ok(Ok(recovered_handle)) = process.receive(rebound, within: 5000)
  postgres.state(database_a_after_restart, recovered_handle)
  |> should.equal(Ok(job.Uncertain))
  postgres.resolve_uncertain(
    database_a_after_restart,
    recovered_handle,
    "same-id-different-store",
    "operator",
    "rebind after storage owner restart",
    postgres.ConfirmSuccess("approved"),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.resolve_uncertain(
    database_a_after_restart,
    handle_b,
    "same-id-different-store",
    "operator",
    "rebind after storage owner restart",
    postgres.ConfirmSuccess("approved"),
  )
  |> should.equal(Error(postgres.ResolutionRouteMismatch))
  mark_database_test_executed("resolution-rebind-owner-checked")
}

fn run_resolution_payload_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_resolution_payload")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolution-payload-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolution-payload-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define(
      "resolution.payload",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "resolution-payload", worker, 2)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 222, attempt_epoch = 3, attempt_owner = 'lost-payload-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolution-payload")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-payload-222",
    "on-call",
    "operator observed committed application key",
    postgres.ConfirmSuccess("approved"),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-payload-222",
    "on-call",
    "operator observed committed application key",
    postgres.ConfirmSuccess("different"),
  )
  |> should.equal(Error(postgres.ResolutionCommandConflict))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("approved")))
  mark_database_test_executed("resolution-payload-bound")
}

fn run_exact_expiry_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_exact_expiry")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("exact-expiry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("exact-expiry-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define(
      "exact-expiry.echo",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) = postgres.submit(database, "exact-expiry", worker, 5)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = pog.named_connection(pool_name)
  let assert Ok(boundary) =
    pog.query(
      "WITH database_time AS MATERIALIZED (SELECT clock_timestamp() AS instant), boundary AS MATERIALIZED (UPDATE grind_jobs AS job SET lease_expires_at = database_time.instant FROM database_time WHERE job.id = $1 RETURNING job.lease_expires_at, database_time.instant) SELECT lease_expires_at = instant, "
      <> postgres.live_lease_predicate("instant")
      <> ", "
      <> postgres.expired_lease_predicate("instant")
      <> " FROM boundary",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use exact <- decode.field(0, decode.bool)
      use acknowledgement_allowed <- decode.field(1, decode.bool)
      use quarantine_eligible <- decode.field(2, decode.bool)
      decode.success(#(exact, acknowledgement_allowed, quarantine_eligible))
    })
    |> pog.execute(on: connection)
  let assert [#(exact, acknowledgement_allowed, quarantine_eligible)] =
    boundary.rows
  exact |> should.equal(True)
  acknowledgement_allowed |> should.equal(False)
  // At the exact boundary, `expired_lease_predicate` must be the complement
  // of `live_lease_predicate` (a strict `<=` and a strict `>` on the same
  // pair can never both be true or both be false), proving the quarantine
  // scan's own fragment agrees with the acknowledgement fragment on exactly
  // this tie instead of merely happening not to disagree elsewhere.
  quarantine_eligible |> should.equal(!acknowledgement_allowed)
  quarantine_eligible |> should.equal(True)
  mark_database_test_executed("exact-expiry-rejected")
}

/// Proves the same fenced-lease predicate rejects acknowledgement once the
/// lease has already expired by database time, on the *production*
/// acknowledgement path (the test above only exercises the SQL predicate
/// directly). The lease is deliberately much longer than this test's whole
/// run so no automatic renewal tick can fire and confuse the result with a
/// renewal-detected loss instead of the forced write below. Honest wording:
/// this proves the "lease already expired" side of the boundary on the real
/// `acknowledge_claim` path, not exact-instant equality — real time elapses
/// between the forced write below and the ack transaction's own later
/// `clock_timestamp()` call, so by the time production code evaluates the
/// predicate the lease is already in the past, not tied to it. Exact
/// equality at a single instant is what the predicate-only test above
/// proves, against this same shared fragment.
fn run_ack_after_database_expiry_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_ack_after_expiry")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ack-after-expiry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-after-expiry-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "ack.after.expiry",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("after-expiry-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("ack-after-expiry")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "ack-after-expiry", slow_worker, 9)
  let job_id = job.id_value(handle)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(30_000)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let connection = pog.named_connection(pool_name)
  let assert Ok(#(attempt_id, epoch, _, Some(attempt_owner))) =
    attempt_snapshot(connection, job_id)

  // Tightest reachable forced expiry: the row's lease is set to the
  // database's own "now" rather than a value already further in the past.
  // The elapsed time between this UPDATE committing and the ack
  // transaction's own later clock_timestamp() call is what pushes the
  // lease into the past by the time production code evaluates it.
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  // No renewal tick has fired yet (lease_duration_ms / 3 is far outside this
  // test's whole run), so the coordinator's own renewal status is still
  // whatever the claim left it at. This confirms the ack rejection below
  // comes from the forced write, not from a renewal loss the coordinator
  // already detected on its own.
  queue.renewal_status(consumer)
  |> should.equal(Ok(Some(queue.LeaseRenewalConfirmed)))

  process.send(release, ReleaseAttempt)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
    proposed,
    postgres.AckLeaseExpired(stale_attempt_id, stale_epoch, stale_owner),
  )))) = process.receive(reply, within: 5000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "ack-after-expiry-output-v1",
    "\"after-expiry-9\"",
  ))
  stale_attempt_id |> should.equal(attempt_id)
  stale_epoch |> should.equal(epoch)
  stale_owner |> should.equal(attempt_owner)

  let command_id =
    postgres.acknowledgement_command_id(job_id, attempt_id, epoch)
  let assert Ok(receipt_rows) =
    pog.query(
      "SELECT count(*) FROM grind_job_acknowledgements WHERE command_id = $1",
    )
    |> pog.parameter(pog.text(command_id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  let assert [0] = receipt_rows.rows

  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(Error(postgres.AckReceiptNotFound))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed(
    "ack-after-database-expiry-stale-no-receipt-passed",
  )
}

fn run_bounded_quarantine_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_bounded_quarantine")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("bounded-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("bounded-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define("bounded.echo", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("bounded-quarantine")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(first) =
    postgres.submit(database, "bounded-quarantine", worker, 1)
  let assert Ok(second) =
    postgres.submit(database, "bounded-quarantine", worker, 2)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'expired-owner', lease_expires_at = clock_timestamp(), attempt_count = 1, delivery_count = 1, cancel_requested_at = CASE WHEN input = '1'::jsonb THEN clock_timestamp() ELSE NULL END WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("bounded.echo"))
    |> pog.parameter(pog.text("bounded-quarantine"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  let states = [
    postgres.state(database, first),
    postgres.state(database, second),
  ]
  let uncertain_count =
    list.count(states, fn(state) { state == Ok(job.Uncertain) })
  uncertain_count |> should.equal(1)
  let executing_count =
    list.count(states, fn(state) { state == Ok(job.Executing) })
  executing_count |> should.equal(1)
  postgres.state(database, first) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, first)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired after cancellation request; prior effect unknown",
    )),
  )

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, second) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, second)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  mark_database_test_executed("quarantine-bounded-passed")
}

fn run_expired_attempt_quarantine_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_expired_quarantine")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("quarantine-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("quarantine-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(worker) =
    worker.define("quarantine.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("expired-quarantine")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(handle) =
    postgres.submit(database, "expired-quarantine", worker, 9)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'dead-consumer', lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("quarantine.echo"))
    |> pog.parameter(pog.text("expired-quarantine"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  postgres.arguments(database, handle) |> should.equal(Ok(9))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  let assert Ok(expired_attempt) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, lease_expires_at <= clock_timestamp(), failure_description, uncertain_at IS NOT NULL FROM grind_jobs WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("quarantine.echo"))
    |> pog.parameter(pog.text("expired-quarantine"))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use lease_expired <- decode.field(3, decode.bool)
      use reason <- decode.field(4, decode.string)
      use uncertainty_time_recorded <- decode.field(5, decode.bool)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        lease_expired,
        reason,
        uncertainty_time_recorded,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      attempt_id,
      attempt_epoch,
      attempt_owner,
      lease_expired,
      reason,
      uncertainty_time_recorded,
    ),
  ] = expired_attempt.rows
  attempt_id |> should.not_equal(0)
  attempt_epoch |> should.equal(1)
  attempt_owner |> should.equal("dead-consumer")
  lease_expired |> should.equal(True)
  reason |> should.equal("expired attempt requires outcome reconciliation")
  uncertainty_time_recorded |> should.equal(True)
  mark_database_test_executed("expired-attempt-quarantine-passed")
}

fn settle_attempt(
  finished: process.Subject(Nil),
  release: process.Subject(LeaseCommand),
  reply: process.Subject(Result(Bool, queue.ProcessError)),
) -> Nil {
  case process.receive(finished, within: 0) {
    Ok(Nil) -> Nil
    Error(Nil) -> {
      process.send(release, ReleaseAttempt)
      let _ = process.receive(reply, within: 5000)
      Nil
    }
  }
}

fn run_queue_batch_policy_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_queue_batch")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("batch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("batch-output-v1", json.int, decode.int)
  let assert Ok(increment) =
    worker.define("batch.increment", "v1", input_codec, output_codec, fn(value) {
      Ok(value + 1)
    })
  let assert Ok(workers) = registry.new("batch-policy")
  let assert Ok(workers) = registry.register(workers, increment)
  let assert Ok(first) = postgres.submit(database, "batch-policy", increment, 1)
  let assert Ok(second) =
    postgres.submit(database, "batch-policy", increment, 2)
  let assert Ok(third) = postgres.submit(database, "batch-policy", increment, 3)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_jobs_per_poll(2)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_available(consumer)
  |> should.equal(queue.BatchCompleted(2))
  postgres.state(database, first) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, second) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, third) |> should.equal(Ok(job.Queued))
  queue.process_available(consumer)
  |> should.equal(queue.BatchCompleted(1))
  postgres.state(database, third) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("queue-batch-policy-passed")
}

fn run_automatic_queue_fairness_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_queue_fairness")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fairness-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fairness-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(incompatible) =
    worker.define("queue.drift", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(later) =
    worker.define("queue.later", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, LaterWorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("automatic-fairness")
  let assert Ok(workers) = registry.register(workers, incompatible)
  let assert Ok(workers) = registry.register(workers, later)
  let assert Ok(incompatible_handle) =
    postgres.submit(database, "automatic-fairness", incompatible, 1)
  let assert Ok(later_handle) =
    postgres.submit(database, "automatic-fairness", later, 2)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("fairness-output-v2"))
    |> pog.parameter(pog.text("queue.drift"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  process.receive(probe, within: 5000)
  |> should.equal(Ok(LaterWorkerInvoked))
  wait_for_job_state(database, incompatible_handle, job.ContractMismatch, 250)
  |> should.equal(True)
  wait_for_job_state(database, later_handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed("automatic-contract-skip-passed")
}

fn run_business_failure_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_business_failure")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(42), job.BudgetExhausted)),
  )
  postgres.state(database, handle)
  |> should.equal(Ok(job.BusinessFailed))
  mark_database_test_executed("typed-business-failure-passed")
}

fn run_worker_discard_outcome_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_worker_discard_outcome")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert [#(Some("not needed"), "discarded", None, 32, True, True)] =
    receipt.rows
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.DiscardedWithReason("not needed")))
  mark_database_test_executed("worker-discard-distinct-outcome-passed")
}

fn run_worker_cancel_outcome_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_worker_cancel_outcome")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert [
    #(Some("worker declined the job"), "cancelled", None, 32, True, True),
  ] = receipt.rows
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("worker declined the job")))
  mark_database_test_executed("worker-cancel-distinct-outcome-passed")
}

fn run_worker_uncertainty_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_worker_uncertainty")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
    |> pog.execute(on: pog.named_connection(pool_name))
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
    committed_at: _,
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

fn run_cancel_before_execution_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_cancel_before_run")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert [#(0, 0, True, True)] = accounting.rows
  mark_database_test_executed("cancel-before-run-committed")
}

fn run_cancel_after_completion_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_cancel_after_completion")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
  let pool_name = process.new_name("grind_cancel_running")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert [#(command_id, attempt_id, attempt_epoch, "cancelled", None, 32)] =
    receipt.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: receipt_command,
    attempt_id: receipt_attempt,
    attempt_epoch: receipt_epoch,
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at: committed_at,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_command |> should.equal(command_id)
  receipt_attempt |> should.equal(attempt_id)
  receipt_epoch |> should.equal(attempt_epoch)
  receipt_state |> should.equal(job.Cancelled)
  receipt_cause |> should.equal(None)
  committed_at |> should.not_equal("")
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET failure_description = 'changed current job diagnostic' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: pog.named_connection(pool_name))
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(
    Ok(postgres.AcknowledgementReceipt(
      command_id: receipt_command,
      attempt_id: receipt_attempt,
      attempt_epoch: receipt_epoch,
      committed_state: receipt_state,
      business_failure_cause: receipt_cause,
      committed_at:,
    )),
  )
  mark_database_test_executed("cancel-running-ack-wins")
}

fn run_cancel_running_uncertain_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_cancel_running_uncertain")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert [#(command_id, "cancelled", None, 32, True, True)] = evidence.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at: _,
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
    |> pog.execute(on: pog.named_connection(pool_name))
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
  let pool_name = process.new_name("grind_cancel_expired")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
    |> pog.execute(on: pog.named_connection(pool_name))

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
    |> pog.execute(on: pog.named_connection(pool_name))
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
    "cancel-pending-replay",
    "on-call",
    "the cancellation request blocks replay until effect evidence is reviewed",
    postgres.AuthorizeReplay,
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
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert [0] = no_audit.rows
  postgres.resolve_uncertain(
    database,
    handle,
    "cancel-pending-confirmed",
    "on-call",
    "external effect evidence confirms the known result",
    postgres.ConfirmSuccess("confirmed-without-replay"),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("confirmed-without-replay")))
  mark_database_test_executed("cancel-pending-expiry-quarantined")
}

fn run_worker_snooze_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_worker_snooze")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("snooze-output-v1", json.string, decode.string)
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let before_ack_ms =
    database_time_milliseconds(pog.named_connection(pool_name))
  queue.process_one(consumer) |> should.equal(Ok(True))
  let after_ack_ms = database_time_milliseconds(pog.named_connection(pool_name))
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
    |> pog.execute(on: pog.named_connection(pool_name))
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

fn database_time_milliseconds(connection: pog.Connection) -> Int {
  let assert Ok(sample) =
    pog.query(
      "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
    )
    |> pog.returning({
      use milliseconds <- decode.field(0, decode.int)
      decode.success(milliseconds)
    })
    |> pog.execute(on: connection)
  let assert [milliseconds] = sample.rows
  milliseconds
}

pub fn postgres_worker_snooze_receipt_write_failure_rolls_back_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_snooze_receipt_rollback_test(database_url)
  }
}

fn run_snooze_receipt_rollback_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_snooze_rollback")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let connection = pog.named_connection(pool_name)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_reject_snooze_receipt ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_reject_snooze_receipt()")
      |> pog.execute(on: connection)
    postgres.close(database)
  })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-rollback-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("snooze-rollback-output-v1", json.string, decode.string)
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
    postgres.claim_one(
      database,
      "snooze-rollback",
      workers,
      attempt_owner,
      30_000,
    )
  let proposed = postgres.execute_claim(claimed)
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
    postgres.acknowledge_claim(
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
  let pool_name = process.new_name("grind_snooze_audited_replay")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-replay-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("snooze-replay-output-v1", json.string, decode.string)
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
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 9, attempt_owner = 'expired-snooze-owner', lease_expires_at = clock_timestamp(), attempt_count = 1, delivery_count = 1 WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.resolve_uncertain(
    database,
    handle,
    "snooze-audited-replay",
    "on-call",
    "inspect the prior effect before authorizing a new delivery",
    postgres.AuthorizeReplay,
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
  let pool_name = process.new_name("grind_default_retry")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("default-retry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("default-retry-output-v1", json.string, decode.string)
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let connection = pog.named_connection(pool_name)
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
  let pool_name = process.new_name("grind_retry_delay_maximum")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("maximum-delay-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("maximum-delay-output-v1", json.string, decode.string)
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let connection = pog.named_connection(pool_name)
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
  let pool_name = process.new_name("grind_retry_declined")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("retry-declined-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("retry-declined-output-v1", json.string, decode.string)
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
      encode_lookup_failure,
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(policy_calls, within: 0) |> should.equal(Ok(91))
  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.FailedOperationallyWithCause(
      "worker returned an application error",
      job.RetryDeclined,
    )),
  )
  process.receive(policy_calls, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(policy_calls, within: 0) |> should.equal(Ok(92))
  postgres.outcome(database, typed_handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(92), job.RetryDeclined)),
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
    |> pog.execute(on: pog.named_connection(pool_name))
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
    |> pog.execute(on: pog.named_connection(pool_name))
  typed_failure.rows
  |> should.equal([
    #(Some("retry_declined"), True, Some("retry-declined-error-v1")),
  ])
  mark_database_test_executed("worker-retry-declined-without-error-codec")
}

fn run_business_retry_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_business_retry")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("business-retry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("business-retry-output-v1", json.string, decode.string)
  let assert Ok(error_codec) =
    worker.codec(
      "business-retry-error-v1",
      encode_lookup_failure,
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
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
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert [#("retryable", 1, 2, 1)] = first_attempt.rows
  let before_due = database_time_milliseconds(pog.named_connection(pool_name))
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(retry_probe, within: 0) |> should.equal(Error(Nil))

  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: pog.named_connection(pool_name))
  queue.fail_next_worker_start(consumer) |> should.equal(Ok(Nil))
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
    |> pog.execute(on: pog.named_connection(pool_name))
  after_unstarted_retry.rows |> should.equal([#(1, 2)])
  process.receive(retry_probe, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(42), job.BudgetExhausted)),
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
    |> pog.execute(on: pog.named_connection(pool_name))
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
    |> pog.execute(on: pog.named_connection(pool_name))
  let assert [#(2, 2, 3)] = counters.rows
  should.be_true(before_due > 0)
  mark_database_test_executed("worker-retry-first-attempt-scheduled")
}

fn run_snooze_delay_receipt_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_snooze_delay_receipt")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-delay-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("snooze-delay-output-v1", json.string, decode.string)
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
    postgres.claim_one(
      database,
      "snooze-delay-receipt",
      workers,
      attempt_owner,
      30_000,
    )
  let proposal = worker.ExecutedSnoozed(60_000, "receipt payload conflict")
  postgres.acknowledge_claim(
    database,
    "snooze-delay-receipt",
    attempt_owner,
    claimed,
    proposal,
  )
  |> should.equal(Ok(True))
  postgres.acknowledge_claim(
    database,
    "snooze-delay-receipt",
    attempt_owner,
    claimed,
    worker.ExecutedSnoozed(70_000, "receipt payload conflict"),
  )
  |> should.equal(Error(postgres.QueueAckCommandConflict))
  postgres.acknowledge_claim(
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
    |> pog.execute(on: pog.named_connection(pool_name))
  receipt.rows |> should.equal([#("scheduled", 0, 1, "scheduled", 32)])
  mark_database_test_executed("worker-snooze-delay-receipt-conflict-passed")
}

fn encode_lookup_failure(error: LookupFailure) -> json.Json {
  case error {
    AccountMissing(account_id) ->
      json.object([
        #("kind", json.string("account_missing")),
        #("account_id", json.int(account_id)),
      ])
  }
}

fn decode_lookup_failure() -> decode.Decoder(LookupFailure) {
  use kind <- decode.field("kind", decode.string)
  use account_id <- decode.field("account_id", decode.int)
  case kind {
    "account_missing" -> decode.success(AccountMissing(account_id))
    _ -> decode.failure(AccountMissing(account_id), "known lookup failure kind")
  }
}

fn run_output_codec_mismatch_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_codec_mismatch")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("mismatch-output-v2"))
    |> pog.parameter(pog.text("codec.drift"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: "output",
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
  let pool_name = process.new_name("grind_queue_success")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.state(database, handle)
  |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("42")))
  mark_database_test_executed("committed-success-passed")
}

fn run_incompatible_schema_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_bad_schema")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE grind_jobs (id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, output_version text NOT NULL, output jsonb, error_version text, error jsonb, state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state <> 'executing' AND state <> 'succeeded'), available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, failure_description text)",
    )
    |> pog.execute(on: connection)

  postgres.migrate(database)
  |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(unrepaired) =
    pog.query(
      "SELECT to_regclass(current_schema() || '.grind_schema_migrations') IS NULL, to_regclass(current_schema() || '.grind_job_resolutions') IS NULL, to_regclass(current_schema() || '.grind_job_acknowledgements') IS NULL, to_regclass(current_schema() || '.grind_attempts_id_seq') IS NULL",
    )
    |> pog.returning({
      use migrations <- decode.field(0, decode.bool)
      use resolutions <- decode.field(1, decode.bool)
      use acknowledgements <- decode.field(2, decode.bool)
      use attempt_sequence <- decode.field(3, decode.bool)
      decode.success(#(
        migrations,
        resolutions,
        acknowledgements,
        attempt_sequence,
      ))
    })
    |> pog.execute(on: connection)
  unrepaired.rows |> should.equal([#(True, True, True, True)])
  mark_database_test_executed("incompatible-schema-rejected")
}
