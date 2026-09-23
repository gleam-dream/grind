import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{Some}
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

type LeaseCommand {
  ReleaseAttempt
}

type LeaseSignal {
  FirstAttemptStarted(process.Subject(LeaseCommand))
  TakeoverAttemptStarted(process.Subject(LeaseCommand))
}

type PolicyStartSignal {
  PolicyStarterReady
  StartPolicyRace
  PolicyStartResult(Result(Bool, queue.StartError))
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

pub fn renewal_ticks_are_scoped_to_the_active_attempt_test() {
  queue.renewal_is_current(10, 2, 10, 2) |> should.equal(True)
  queue.renewal_is_current(10, 2, 11, 3) |> should.equal(False)
}

pub fn pending_shutdown_waiters_keep_the_original_deadline_test() {
  queue.next_shutdown_generation(7, True) |> should.equal(7)
  queue.next_shutdown_generation(7, False) |> should.equal(8)
}

pub fn postgres_stopped_consumer_handle_does_not_retarget_after_restart_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_stale_consumer_handle_test(database_url)
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

pub fn postgres_automatic_poll_pauses_and_renews_during_drain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_drain_test(database_url)
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
  queue.process_one(consumer)
  |> should.equal(Error(queue.QueueActorExited))

  let assert Ok(handle) =
    postgres.submit(database, "owner-restart", definition, 12)
  wait_for_succeeded(database, handle, 200) |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  let root_supervisor = queue.supervisor_pid(consumer)
  let _ = queue.stop(consumer)
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

@external(erlang, "grind_test_env", "schema_v1_url")
fn schema_v1_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_v2_url")
fn schema_v2_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_v3_url")
fn schema_v3_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_v4_missing_receipt_url")
fn schema_v4_missing_receipt_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "resolution_route_a_url")
fn resolution_route_a_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "resolution_route_b_url")
fn resolution_route_b_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "mark_database_test_executed")
fn mark_database_test_executed(contract: String) -> Nil

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

pub fn postgres_migration_upgrades_v1_without_losing_live_attempt_test() {
  case schema_v1_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_v1_migration_test(database_url)
  }
}

pub fn postgres_migration_preserves_legacy_resolution_receipt_test() {
  case schema_v2_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_v2_migration_test(database_url)
  }
}

pub fn postgres_v3_upgrade_creates_ack_receipt_table_test() {
  case schema_v3_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_v3_upgrade_ack_table_test(database_url)
  }
}

fn run_v3_upgrade_ack_table_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_v3")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations WHERE version = 4")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP TABLE grind_job_acknowledgements")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Ok(Nil))
  let assert Ok(upgraded) =
    pog.query(
      "SELECT (SELECT count(*) = 4 FROM grind_schema_migrations), to_regclass(current_schema() || '.grind_job_acknowledgements') IS NOT NULL",
    )
    |> pog.returning({
      use all_versions <- decode.field(0, decode.bool)
      use receipt_table <- decode.field(1, decode.bool)
      decode.success(#(all_versions, receipt_table))
    })
    |> pog.execute(on: connection)
  let assert [#(True, True)] = upgraded.rows
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations WHERE version = 4")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP TABLE grind_job_acknowledgements")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP SEQUENCE grind_attempts_id_seq")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(sequence_still_missing) =
    pog.query(
      "SELECT to_regclass(current_schema() || '.grind_attempts_id_seq') IS NULL",
    )
    |> pog.returning({
      use missing <- decode.field(0, decode.bool)
      decode.success(missing)
    })
    |> pog.execute(on: connection)
  let assert [True] = sequence_still_missing.rows
  mark_database_test_executed("v3-upgrade-created-ack-table")
  mark_database_test_executed("v3-missing-attempt-sequence-rejected")
}

pub fn postgres_v4_migration_rejects_missing_ack_receipt_table_test() {
  case schema_v4_missing_receipt_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_missing_v4_ack_table_test(database_url)
  }
}

fn run_missing_v4_ack_table_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_v4_missing_receipt")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("DROP TABLE grind_job_acknowledgements")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(missing) =
    pog.query(
      "SELECT to_regclass(current_schema() || '.grind_job_acknowledgements') IS NULL",
    )
    |> pog.returning({
      use absent <- decode.field(0, decode.bool)
      decode.success(absent)
    })
    |> pog.execute(on: connection)
  let assert [True] = missing.rows
  mark_database_test_executed("v4-missing-ack-table-rejected")
}

fn run_v1_migration_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_v1")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE grind_schema_migrations (version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp())",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE grind_jobs (id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, output_version text NOT NULL, output jsonb, error_version text, error jsonb, state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state IN ('queued', 'scheduled', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled')), available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, failure_description text)",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("CREATE SEQUENCE grind_attempts_id_seq")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (1)")
    |> pog.execute(on: connection)
  let assert Ok(input_codec) =
    worker.codec("legacy-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("legacy-output-v1", json.string, decode.string)
  let assert Ok(legacy_worker) =
    worker.define("legacy.echo", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(handle) = postgres.submit(database, "legacy", legacy_worker, 73)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 77, attempt_epoch = 4, attempt_owner = 'legacy-consumer', lease_expires_at = clock_timestamp() + interval '30 seconds', attempt_count = 2 WHERE id = $1",
    )
    |> pog.parameter(pog.int(1))
    |> pog.execute(on: connection)

  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.arguments(database, handle) |> should.equal(Ok(73))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  let assert Ok(migrated_attempt) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, lease_expires_at > clock_timestamp(), uncertain_at IS NULL, (SELECT count(*) = 4 FROM grind_schema_migrations) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(1))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use lease_live <- decode.field(3, decode.bool)
      use no_uncertain_at <- decode.field(4, decode.bool)
      use versions_present <- decode.field(5, decode.bool)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        lease_live,
        no_uncertain_at,
        versions_present,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      attempt_id,
      attempt_epoch,
      attempt_owner,
      lease_live,
      no_uncertain_at,
      versions_present,
    ),
  ] = migrated_attempt.rows
  attempt_id |> should.equal(77)
  attempt_epoch |> should.equal(4)
  attempt_owner |> should.equal("legacy-consumer")
  lease_live |> should.equal(True)
  no_uncertain_at |> should.equal(True)
  versions_present |> should.equal(True)
  mark_database_test_executed("v1-migration-preserved-data")
}

fn run_v2_migration_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_v2")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = pog.named_connection(pool_name)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("legacy-resolution-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("legacy-resolution-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "legacy.resolution",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) = postgres.submit(database, "legacy", definition, 7)
  let durable_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'succeeded', output = '\"legacy-value\"'::jsonb WHERE id = $1",
    )
    |> pog.parameter(pog.int(durable_id))
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_resolutions DROP CONSTRAINT grind_job_resolutions_target_state_check",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_resolutions DROP COLUMN worker_id, DROP COLUMN worker_version, DROP COLUMN target_state, DROP COLUMN payload_version, DROP COLUMN payload",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations WHERE version = 3")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations WHERE version = 4")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, resolved_by, details) VALUES ($1, 'legacy', $2, 'legacy-resolution', 91, 6, 'legacy-owner', clock_timestamp() - interval '1 minute', 'confirm_success', 'operator', 'approved by prior operator')",
    )
    |> pog.parameter(pog.text(postgres.storage_owner(database)))
    |> pog.parameter(pog.int(durable_id))
    |> pog.execute(on: connection)

  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  let assert Ok(legacy_receipt) =
    pog.query(
      "SELECT target_state, payload_version IS NULL, payload IS NULL, (SELECT count(*) = 4 FROM grind_schema_migrations), decision, details, worker_id, worker_version FROM grind_job_resolutions WHERE resolution_id = 'legacy-resolution'",
    )
    |> pog.returning({
      use target_state <- decode.field(0, decode.string)
      use no_payload_version <- decode.field(1, decode.bool)
      use no_payload <- decode.field(2, decode.bool)
      use all_versions <- decode.field(3, decode.bool)
      use decision <- decode.field(4, decode.string)
      use details <- decode.field(5, decode.string)
      use stored_worker_id <- decode.field(6, decode.optional(decode.string))
      use stored_worker_version <- decode.field(
        7,
        decode.optional(decode.string),
      )
      decode.success(#(
        target_state,
        no_payload_version,
        no_payload,
        all_versions,
        decision,
        details,
        stored_worker_id,
        stored_worker_version,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      target_state,
      no_payload_version,
      no_payload,
      all_versions,
      decision,
      details,
      stored_worker_id,
      stored_worker_version,
    ),
  ] = legacy_receipt.rows
  target_state |> should.equal("succeeded")
  no_payload_version |> should.equal(True)
  no_payload |> should.equal(True)
  all_versions |> should.equal(True)
  decision |> should.equal("confirm_success")
  details |> should.equal("approved by prior operator")
  stored_worker_id |> should.equal(Some("legacy.resolution"))
  stored_worker_version |> should.equal(Some("v1"))
  postgres.resolve_uncertain(
    database,
    handle,
    "legacy-resolution",
    "operator",
    "approved by prior operator",
    postgres.ConfirmSuccess("legacy-value"),
  )
  |> should.equal(Error(postgres.ResolutionCommandConflict))
  mark_database_test_executed("v2-legacy-resolution-preserved")
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
  let assert Ok(True) =
    pog.query("SELECT pg_terminate_backend($1)")
    |> pog.parameter(pog.int(backend_pid))
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
      False,
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
    "grind-ack:"
    <> int.to_string(claimed_id)
    <> ":"
    <> int.to_string(attempt_id)
    <> ":"
    <> int.to_string(epoch)
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
  postgres.acknowledge_claim_with_lost_reply_after_commit(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    execution,
  )
  |> should.equal(Error(postgres.QueueAckUnknown(command_id, execution)))
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
      "SELECT attempt_id, attempt_epoch, command_id, attempt_owner, queue, worker_id, worker_version, proposed_state, committed_state, output_version, output::text FROM grind_job_acknowledgements WHERE storage_owner = $1 AND job_id = $2",
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
      use proposed_state <- decode.field(7, decode.string)
      use committed_state <- decode.field(8, decode.string)
      use output_version <- decode.field(9, decode.string)
      use output <- decode.field(10, decode.string)
      decode.success(#(
        attempt_id,
        epoch,
        command_id,
        attempt_owner,
        queue,
        worker_id,
        worker_version,
        proposed_state,
        committed_state,
        output_version,
        output,
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
      proposed,
      committed,
      output_version,
      output,
    ),
  ] = receipts.rows
  should.be_true(attempt_id > 0)
  epoch |> should.equal(1)
  command_id |> should.not_equal("")
  attempt_owner |> should.equal("ack-receipt-owner")
  queue |> should.equal("ack-receipt")
  worker_id |> should.equal("ack.receipt")
  worker_version |> should.equal("v1")
  proposed |> should.equal("succeeded")
  committed |> should.equal("succeeded")
  output_version |> should.equal("ack-output-v1")
  output |> should.equal("\"result-8\"")
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(
    Ok(postgres.AcknowledgementReceipt(
      command_id:,
      attempt_id:,
      attempt_epoch: epoch,
      outcome: job.SucceededWith("result-8"),
    )),
  )
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
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.fail_next_worker_start(consumer) |> should.equal(Ok(Nil))
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueWorkerStartFailed(actor.InitFailed("injected start failure")),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
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

pub fn postgres_expired_attempt_is_taken_over_and_stale_ack_is_fenced_test() {
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
  let assert Ok(takeover_worker) =
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
  let assert Ok(takeover_registry) = registry.new("takeover-fence")
  let assert Ok(takeover_registry) =
    registry.register(takeover_registry, takeover_worker)
  let assert Ok(replay_policy) =
    queue.default_policy()
    |> queue.with_expired_attempt_policy(queue.ReplayAtLeastOnce)
    |> queue.validate_policy
  let assert Ok(first_consumer) =
    queue.start_manual_with_policy(database, first_registry, replay_policy)
  use <- exception.defer(fn() { queue.stop(first_consumer) })
  let assert Ok(takeover_consumer) =
    queue.start_manual_with_policy(database, takeover_registry, replay_policy)
  use <- exception.defer(fn() { queue.stop(takeover_consumer) })
  let assert Ok(competing_consumer) =
    queue.start_manual_with_policy(database, takeover_registry, replay_policy)
  use <- exception.defer(fn() { queue.stop(competing_consumer) })
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

  queue.process_one(competing_consumer) |> should.equal(Ok(False))
  let connection = pog.named_connection(pool_name)
  let assert Ok(first_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner FROM grind_jobs WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("takeover.echo"))
    |> pog.parameter(pog.text("takeover-fence"))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      decode.success(#(attempt_id, attempt_epoch, attempt_owner))
    })
    |> pog.execute(on: connection)
  let assert [#(first_attempt_id, first_epoch, first_owner)] = first_claim.rows
  first_epoch |> should.equal(1)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2 AND state = 'executing'",
    )
    |> pog.parameter(pog.text("takeover.echo"))
    |> pog.parameter(pog.text("takeover-fence"))
    |> pog.execute(on: connection)

  let takeover_reply = process.new_subject()
  let takeover_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(takeover_reply, queue.process_one(takeover_consumer))
    })
  let assert Ok(TakeoverAttemptStarted(takeover_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(takeover_finished, takeover_release, takeover_reply)
  })
  let assert Ok(takeover_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner FROM grind_jobs WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("takeover.echo"))
    |> pog.parameter(pog.text("takeover-fence"))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      decode.success(#(attempt_id, attempt_epoch, attempt_owner))
    })
    |> pog.execute(on: connection)
  let assert [#(takeover_attempt_id, takeover_epoch, takeover_owner)] =
    takeover_claim.rows
  takeover_attempt_id |> should.not_equal(first_attempt_id)
  takeover_epoch |> should.equal(first_epoch + 1)
  takeover_owner |> should.not_equal(first_owner)
  queue.process_one(competing_consumer) |> should.equal(Ok(False))

  process.send(first_release, ReleaseAttempt)
  let first_ack = process.receive(first_reply, within: 5000)
  process.send(first_finished, Nil)
  first_ack
  |> should.equal(
    Ok(
      Error(
        queue.QueueProcessFailed(postgres.QueueAckStale(
          worker.ExecutedSuccess("takeover-output-v1", "\"obsolete-7\""),
          postgres.AckOwnershipChanged(
            state: "executing",
            attempt_id: Some(takeover_attempt_id),
            epoch: Some(takeover_epoch),
            owner: Some(takeover_owner),
          ),
        )),
      ),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  process.send(takeover_release, ReleaseAttempt)
  let takeover_ack = process.receive(takeover_reply, within: 5000)
  process.send(takeover_finished, Nil)
  takeover_ack |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("current-7")))
  mark_database_test_executed("expired-attempt-takeover-passed")
}

pub fn postgres_expired_attempt_requires_reconciliation_by_default_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_expired_attempt_quarantine_test(database_url)
  }
}

pub fn postgres_mixed_consumers_reject_replay_policy_conflict_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_mixed_consumer_policy_test(database_url)
  }
}

pub fn postgres_concurrent_queue_policy_start_has_one_winner_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_concurrent_policy_start_test(database_url)
  }
}

fn run_concurrent_policy_start_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_policy_start_race")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("policy-race-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("policy-race-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("policy.race", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("durable-policy-race")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(default_policy) =
    queue.default_policy() |> queue.validate_policy
  let assert Ok(replay_policy) =
    queue.default_policy()
    |> queue.with_expired_attempt_policy(queue.ReplayAtLeastOnce)
    |> queue.validate_policy

  let ready = process.new_subject()
  let results = process.new_subject()
  let default_gate = process.new_name("grind_policy_start_default_gate")
  let replay_gate = process.new_name("grind_policy_start_replay_gate")
  let _ =
    process.spawn(fn() {
      let assert Ok(Nil) = process.register(process.self(), default_gate)
      process.send(ready, PolicyStarterReady)
      let _ = process.receive(process.named_subject(default_gate), within: 5000)
      let _ = process.unregister(default_gate)
      let result =
        queue.start_manual_with_policy(database, workers, default_policy)
      let outcome = case result {
        Ok(consumer) -> {
          let _ = queue.stop(consumer)
          Ok(True)
        }
        Error(error) -> Error(error)
      }
      process.send(results, PolicyStartResult(outcome))
    })
  let _ =
    process.spawn(fn() {
      let assert Ok(Nil) = process.register(process.self(), replay_gate)
      process.send(ready, PolicyStarterReady)
      let _ = process.receive(process.named_subject(replay_gate), within: 5000)
      let _ = process.unregister(replay_gate)
      let result =
        queue.start_manual_with_policy(database, workers, replay_policy)
      let outcome = case result {
        Ok(consumer) -> {
          let _ = queue.stop(consumer)
          Ok(True)
        }
        Error(error) -> Error(error)
      }
      process.send(results, PolicyStartResult(outcome))
    })
  process.receive(ready, within: 5000) |> should.equal(Ok(PolicyStarterReady))
  process.receive(ready, within: 5000) |> should.equal(Ok(PolicyStarterReady))
  process.send(process.named_subject(default_gate), StartPolicyRace)
  process.send(process.named_subject(replay_gate), StartPolicyRace)
  let assert Ok(PolicyStartResult(first)) =
    process.receive(results, within: 10_000)
  let assert Ok(PolicyStartResult(second)) =
    process.receive(results, within: 10_000)
  let first_started = case first {
    Ok(True) -> True
    _ -> False
  }
  let second_started = case second {
    Ok(True) -> True
    _ -> False
  }
  let conflict_count =
    case first {
      Error(queue.QueueConfigurationFailed(
        postgres.ExpiredAttemptPolicyConflict,
      )) -> 1
      _ -> 0
    }
    + case second {
      Error(queue.QueueConfigurationFailed(
        postgres.ExpiredAttemptPolicyConflict,
      )) -> 1
      _ -> 0
    }
  case first_started, second_started {
    True, False -> Nil
    False, True -> Nil
    _, _ -> should.fail()
  }
  conflict_count |> should.equal(1)
  mark_database_test_executed("concurrent-policy-start-single-winner")
}

fn run_mixed_consumer_policy_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_mixed_queue_policy")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("policy-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("policy-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define("policy.echo", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("durable-policy")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(default_consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(default_consumer) })
  let assert Ok(replay_policy) =
    queue.default_policy()
    |> queue.with_expired_attempt_policy(queue.ReplayAtLeastOnce)
    |> queue.validate_policy

  queue.start_manual_with_policy(database, workers, replay_policy)
  |> should.equal(
    Error(queue.QueueConfigurationFailed(postgres.ExpiredAttemptPolicyConflict)),
  )
  mark_database_test_executed("mixed-consumer-policy-rejected")
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
      "WITH database_time AS MATERIALIZED (SELECT clock_timestamp() AS instant), boundary AS MATERIALIZED (UPDATE grind_jobs AS job SET lease_expires_at = database_time.instant FROM database_time WHERE job.id = $1 RETURNING job.lease_expires_at, database_time.instant) SELECT lease_expires_at = instant, lease_expires_at > instant FROM boundary",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use exact <- decode.field(0, decode.bool)
      use acknowledgement_allowed <- decode.field(1, decode.bool)
      decode.success(#(exact, acknowledgement_allowed))
    })
    |> pog.execute(on: connection)
  let assert [#(exact, acknowledgement_allowed)] = boundary.rows
  exact |> should.equal(True)
  acknowledgement_allowed |> should.equal(False)
  mark_database_test_executed("exact-expiry-rejected")
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
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'expired-owner', lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2",
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
  // A request queued to the same actor returns only after the handler and its
  // acknowledgement have completed, so the following state reads are committed.
  queue.process_one(consumer)
  |> should.equal(Ok(False))
  postgres.state(database, incompatible_handle)
  |> should.equal(Ok(job.ContractMismatch))
  postgres.state(database, later_handle)
  |> should.equal(Ok(job.Succeeded))
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
  let assert Ok(workers) = registry.new("business-failures")
  let assert Ok(workers) = registry.register(workers, lookup)
  let assert Ok(handle) =
    postgres.submit(database, "business-failures", lookup, 42)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.BusinessFailedWith(AccountMissing(42))))
  postgres.state(database, handle)
  |> should.equal(Ok(job.BusinessFailed))
  mark_database_test_executed("typed-business-failure-passed")
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
  mark_database_test_executed("incompatible-schema-rejected")
}
