import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/result
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/concurrency.{LongHandlerStarted, ReleaseAttempt}
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/job_state.{
  attempt_snapshot, wait_for_job_state, wait_for_succeeded,
}
import grind/support/queue_signals.{
  CoordinatorLossStarted, LongCallDown, LongCallReturned,
  OwnerPoolLossOwnerFailed, OwnerPoolLossOwnerReady, OwnerPoolLossStarted,
  WorkerInvoked,
}
import grind/support/queue_timing.{await_new_coordinator_pid, database_time_ms}
import grind/support/worker_failure.{AccountMissing}
import grind/worker
import pog

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

fn run_owner_restart_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
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
  let assert Ok(consumer) = queue.start(database, workers, policy)
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

fn run_coordinator_loss_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
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
  let lease_duration_ms = 4008
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_concurrency(1)
    |> queue.with_lease_duration(lease_duration_ms)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(CoordinatorLossStarted(worker_pid, _first_release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))

  let connection = postgres.connection(database)
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
    postgres.ResolutionRequest(
      "coordinator-loss-authorized-replay",
      "on-call",
      "inspect the external effect before authorizing a new delivery",
      postgres.AuthorizeReplay,
    ),
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

fn run_stale_consumer_handle_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
  let assert Ok(first_consumer) =
    queue.start(database, workers, manual_policy())
  let _ = queue.stop(first_consumer)
  let assert Ok(second_consumer) =
    queue.start(database, workers, manual_policy())
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
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
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

/// The owner and pool losses here are sequential, not concurrent: the pool is
/// only closed and reopened after the owner and its cascaded worker are both
/// confirmed dead, as recovery plumbing following that death, not as a second
/// failure landing during active work.
fn run_owner_loss_recovers_after_pool_restart_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
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
      case queue.start(database, workers, policy) {
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
  // `queue.start`, so the owner's death cascades down through
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

  let _ = postgres.close(database)
  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  let connection = postgres.connection(reopened)
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  let assert Ok(fresh_consumer) =
    queue.start(reopened, workers, manual_policy())
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
    postgres.ResolutionRequest(
      "owner-pool-loss-authorized-replay",
      "on-call",
      "inspect the external effect before authorizing a new delivery",
      postgres.AuthorizeReplay,
    ),
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
