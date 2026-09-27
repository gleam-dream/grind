import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/otp/actor
import gleam/result
import gleeunit/should
import grind/internal/consumer_hooks
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt, spawn_lock_holder,
}
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/lease_queries.{attempt_count_for}
import grind/support/lock_wait.{await_claim_waiting_on_advisory}
import grind/support/queue_signals.{
  ConcurrentClaimWorkerStarted, WorkerDeathStarted, WorkerInvoked,
}
import grind/support/worker_failure.{AccountMissing}
import grind/worker
import one_shot
import pog

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
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
  let kill_next_worker = one_shot.armed()
  let hooks =
    consumer_hooks.Hooks(
      before_worker_start: fn() { Ok(Nil) },
      after_worker_start: fn(pid) {
        case one_shot.take(kill_next_worker) {
          True -> {
            process.kill(pid)
            queue.wait_for_worker_exit(pid, 1000)
          }
          False -> Nil
        }
      },
    )
  let assert Ok(consumer) =
    queue.start_with_hooks(database, workers, manual_policy(), hooks)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Error(queue.QueueWorkerExitedBeforeActivation))
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))
  mark_database_test_executed("dead-idle-worker-claim-released")
}

pub fn postgres_automatic_fill_yields_to_shutdown_between_claims_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_automatic_fill_yields_to_shutdown_between_claims_test(database_url)
  }
}

/// Proves the actual fix, not just its absence-of-regression companion
/// above: a `BeginShutdown` sent while one automatic claim is genuinely in
/// flight is handled *before* this coordinator's own next `FillSlots`
/// message, even though there is a whole backlog (`job_count`, well above
/// `maximum_concurrency`) still waiting to be filled. The first claim's own
/// `UPDATE ... SET state = 'executing'` is forced to block on a
/// test-held `pg_advisory_xact_lock` (the same barrier shape
/// `run_overlapping_claim_test` uses); while it is genuinely waiting
/// (confirmed via `pg_stat_activity`, not a timing guess),
/// `begin_shutdown_for_test` sends `BeginShutdown` — which can only land in
/// this coordinator's mailbox, since the coordinator itself is fully
/// occupied running the blocked claim. Releasing the lock lets that one
/// claim complete; `continue_after_start` then asks for another fill via a
/// `FillSlots` message sent *after* `BeginShutdown` was already sent, so
/// FIFO delivery hands the coordinator `BeginShutdown` first. With the old,
/// directly-recursive `fill_automatic_slots`, nothing would have stopped it
/// from claiming straight through the rest of the backlog before ever
/// looking at its mailbox again.
fn run_automatic_fill_yields_to_shutdown_between_claims_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fill-yield-shutdown-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fill-yield-shutdown-output-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define(
      "fill.yield-shutdown",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value) },
    )
  let assert Ok(workers) = registry.new("fill-yield-shutdown")
  let assert Ok(workers) = registry.register(workers, definition)
  let job_count = 10
  submit_fill_yield_backlog(database, definition, job_count)

  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_fill_yield_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.queue = 'fill-yield-shutdown' AND NEW.state = 'executing' THEN PERFORM pg_advisory_xact_lock(74911, 62); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_fill_yield_barrier BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION grind_test_fill_yield_barrier()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_fill_yield_barrier ON grind_jobs",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_fill_yield_barrier()")
      |> pog.execute(on: connection)
    Nil
  })

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      connection,
      pog.query(
        "SELECT 1 FROM (SELECT pg_advisory_xact_lock(74911, 62)) AS held",
      ),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(job_count)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  await_claim_waiting_on_advisory(connection, 250) |> should.equal(True)

  let shutdown_reply = process.new_subject()
  let assert Ok(Nil) = queue.begin_shutdown_for_test(consumer, shutdown_reply)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(_) = process.receive(shutdown_reply, within: 10_000)

  let assert Ok(claimed) =
    count_non_queued_in_queue(connection, "fill-yield-shutdown")
  // The whole point: fewer than the full backlog was claimed once shutdown
  // landed, because `BeginShutdown` was handled ahead of the next
  // `FillSlots` message instead of being starved behind a burst of claims.
  should.be_true(claimed < job_count)
  should.equal(claimed, 1)
  mark_database_test_executed("automatic-fill-yields-to-shutdown")
}

fn submit_fill_yield_backlog(
  database: postgres.Database,
  definition: worker.Worker(Int, Int, error),
  remaining: Int,
) -> Nil {
  case remaining > 0 {
    False -> Nil
    True -> {
      let assert Ok(_) =
        postgres.submit(database, "fill-yield-shutdown", definition, remaining)
      submit_fill_yield_backlog(database, definition, remaining - 1)
    }
  }
}

fn count_non_queued_in_queue(
  connection: pog.Connection,
  queue_name: String,
) -> Result(Int, Nil) {
  pog.query(
    "SELECT count(*) FROM grind_jobs WHERE queue = $1 AND state <> 'queued'",
  )
  |> pog.parameter(pog.text(queue_name))
  |> pog.returning({
    use count <- decode.field(0, decode.int)
    decode.success(count)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [count] -> Ok(count)
      _ -> Error(Nil)
    }
  })
}

fn run_independent_consumer_claim_test(database_url: String) -> Nil {
  let assert Ok(settings_a) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_url) |> postgres.validate
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
  let assert Ok(consumer_a) =
    queue.start(database_a, workers_a, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer_a) })
  let assert Ok(consumer_b) =
    queue.start(database_b, workers_b, manual_policy())
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
  let assert Ok(settings_a) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_url) |> postgres.validate
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

  let connection = postgres.connection(database_a)
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

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      connection,
      pog.query(
        "SELECT 1 FROM (SELECT pg_advisory_xact_lock(74126, 31)) AS held",
      ),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(consumer_a) =
    queue.start(database_a, workers_a, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer_a) })
  let assert Ok(consumer_b) =
    queue.start(database_b, workers_b, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer_b) })

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

fn run_temporary_worker_death_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
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

  let connection = postgres.connection(database)
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

fn run_worker_start_failure_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
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
    |> pog.execute(on: postgres.connection(database))
  let fail_next_worker_start = one_shot.armed()
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
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueWorkerStartFailed(actor.InitFailed("injected start failure")),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let connection = postgres.connection(database)
  attempt_count_for(connection, job.id_value(handle))
  |> should.equal(Ok(0))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(invoked, within: 0) |> should.equal(Ok(Nil))
  mark_database_test_executed("unstarted-worker-claim-released-passed")
}
