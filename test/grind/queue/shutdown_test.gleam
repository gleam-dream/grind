import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/concurrency.{type LeaseCommand, ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, monotonic_ms, queue_database_url,
}
import grind/support/lease_queries.{await_later_lease_expiry, lease_expiration}
import grind/support/queue_signals.{
  CapacityWorkerStarted, ConsumerOwnerFailed, ConsumerOwnerStarted,
  ConsumerOwnerStopCompleted, CoordinatorLossStarted, WorkerInvoked,
}
import grind/support/queue_timing.{
  await_new_coordinator_pid, wait_for_shutdown_state,
}
import grind/support/worker_failure.{AccountMissing}
import grind/worker
import pog

pub fn postgres_repeated_stop_after_coordinator_gone_reports_without_drain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_stop_without_drain_test(database_url)
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
  let settings = postgres.settings(database_url)
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
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
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
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
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
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
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
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_settings =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = postgres.connection(observer)

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
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
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
  let _ = postgres.close(database)

  let assert Ok(leftover) =
    poll_leftover_grind_backends(observer_connection, 300)
  leftover |> should.equal(0)

  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  postgres.state(reopened, handle) |> should.equal(Ok(job.Executing))

  let assert Ok(new_consumer) = queue.start(reopened, workers, manual_policy())
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
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
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
    |> queue.with_maximum_concurrency(1)
    |> queue.with_lease_duration(4008)
    |> queue.with_shutdown_grace(2000)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
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
  let connection = postgres.connection(database)
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

fn resume_suspended_test_process(pid: process.Pid) -> Nil {
  // The successful explicit resume may finish the supervisor termination
  // before this failure-safe cleanup runs. OTP raises badarg if it resumes an
  // already resumed or dead process, which is harmless only in this cleanup.
  let _ = exception.rescue(fn() { resume_process(pid) })
  Nil
}

fn run_foreign_stop_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
      case queue.start(database, workers, manual_policy()) {
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

fn run_stop_without_drain_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())

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
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
    |> queue.with_lease_duration(16_000)
    |> queue.with_shutdown_grace(shutdown_grace_ms)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
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
