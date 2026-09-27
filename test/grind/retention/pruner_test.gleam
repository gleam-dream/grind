import exception
import gleam/erlang/process
import gleeunit/should
import grind/observation
import grind/postgres
import grind/pruner
import grind/support/env.{mark_database_test_executed, monotonic_ms, prune_url}
import grind/support/observers.{detach}
import grind/support/retention_rows.{job_row_exists, seed_terminal_job}
import grind/worker
import pog
import sinal

// -- grind/pruner (the supervised background pruner) -------------------

pub fn pruner_validate_policy_rejects_out_of_range_values_test() {
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 0,
    limit: 10_000,
    max_age_ms: 60_000,
  ))
  |> should.equal(Error(pruner.NonPositiveInterval))
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 30_000,
    limit: 0,
    max_age_ms: 60_000,
  ))
  |> should.equal(Error(pruner.NonPositivePruneLimit))
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 30_000,
    limit: postgres.prune_limit_maximum() + 1,
    max_age_ms: 60_000,
  ))
  |> should.equal(Error(pruner.PruneLimitTooLarge))
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 30_000,
    limit: 10_000,
    max_age_ms: 0,
  ))
  |> should.equal(Error(pruner.NonPositiveMaxAge))
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 30_000,
    limit: 10_000,
    max_age_ms: worker.retry_delay_maximum_milliseconds() + 1,
  ))
  |> should.equal(Error(pruner.MaxAgeAbovePrecisionBound))
  pruner.validate_policy(pruner.default_policy()) |> should.be_ok()
}

/// Polls (bounded) until `id` no longer has a `grind_jobs` row, or gives up
/// and reports whatever `job_row_exists` last saw — used to observe the
/// supervised pruner's own timer doing this without the test itself calling
/// `prune_finished`.
fn await_job_pruned(
  connection: pog.Connection,
  id: Int,
  checks_remaining: Int,
) -> Bool {
  case job_row_exists(connection, id) {
    False -> True
    True ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_job_pruned(connection, id, checks_remaining - 1)
        }
        // Retries exhausted and the row still exists: this must report
        // "not pruned", not "pruned" — the opposite is a false pass that
        // would never actually observe the timer doing anything.
        False -> False
      }
  }
}

pub fn postgres_supervised_pruner_ticks_and_prunes_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_supervised_pruner_test(database_url)
  }
}

fn run_supervised_pruner_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  let old_id =
    seed_terminal_job(
      connection,
      "pruner-old",
      "pruner.worker",
      "succeeded",
      5000,
    )
  // Seeded effectively "now": with a 2000ms `max_age_ms`, this row cannot
  // cross that threshold before the test has already finished observing
  // `old_id` disappear (typically within the first one or two 50ms ticks),
  // giving a wide, non-flaky margin rather than a tight race between the
  // two rows' own ages.
  let young_id =
    seed_terminal_job(
      connection,
      "pruner-young",
      "pruner.worker",
      "succeeded",
      0,
    )

  let signal = process.new_subject()
  let assert Ok(handler_id) =
    sinal.handler_id("grind-test-supervised-pruner-completed")
  let assert Ok(attachment) =
    sinal.observe(
      handler_id,
      observation.prune_completed(),
      fn(measurements, _metadata) { process.send(signal, measurements) },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(policy) =
    pruner.default_policy()
    |> pruner.with_interval(50)
    |> pruner.with_limit(10)
    |> pruner.with_max_age(2000)
    |> pruner.validate_policy()
  let assert Ok(running) = pruner.start(database, policy)
  use <- exception.defer(fn() { pruner.stop(running) |> should.equal(Ok(Nil)) })

  // The old row (5000ms old, over the 2000ms max_age) is pruned by the
  // supervised timer itself — this test never calls `prune_finished`.
  await_job_pruned(connection, old_id, 250) |> should.equal(True)
  // The young row (0ms old) never crosses `max_age_ms`, so it survives
  // every tick this test observes.
  job_row_exists(connection, young_id) |> should.equal(True)

  // The timer path itself emits `[grind, prune, completed]`, not only a
  // direct `prune_finished` call — the tick that pruned `old_id` must have
  // reported at least one deleted job.
  let assert Ok(observation.PruneCompletedMeasurements(jobs:)) =
    process.receive(signal, within: 5000)
  { jobs >= 1 } |> should.equal(True)

  mark_database_test_executed("supervised-pruner-ticks-and-prunes")
}

/// Drains `signal` until `deadline_ms` (`monotonic_ms()`), counting however
/// many messages arrive — used to prove a bounded window's own tick count
/// exactly, rather than inferring it from a single `process.receive`.
fn count_events_until(signal: process.Subject(a), deadline_ms: Int) -> Int {
  let remaining = deadline_ms - monotonic_ms()
  case remaining > 0 {
    False -> 0
    True ->
      case process.receive(signal, within: remaining) {
        Ok(_) -> 1 + count_events_until(signal, deadline_ms)
        Error(Nil) -> 0
      }
  }
}

fn await_new_pruner_pid(
  running: pruner.Pruner,
  previous: process.Pid,
  checks_remaining: Int,
) -> Result(process.Pid, Nil) {
  case pruner.actor_pid(running) {
    Ok(pid) if pid != previous -> Ok(pid)
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          await_new_pruner_pid(running, previous, checks_remaining - 1)
        }
        False -> Error(Nil)
      }
  }
}

/// The race a self-scheduled `Tick` timer targeting the *named* subject
/// (rather than a fresh, per-incarnation one) would create: `process
/// .send_after` against a named subject resolves to whichever process holds
/// that name at *delivery* time, not at scheduling time, so a timer an old,
/// killed incarnation scheduled for itself would still fire and land on
/// whatever later incarnation is running when it does — one genuine
/// post-restart tick plus one leaked one, arriving close together (a
/// restart is on the order of a few ms; both timers were scheduled to fire
/// roughly `interval_ms` after nearly the same moment). Proven by counting
/// `[grind, prune, completed]` events in a bounded window after a real,
/// killed-and-restarted incarnation: exactly one.
pub fn postgres_supervised_pruner_restart_ticks_exactly_once_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_supervised_pruner_restart_test(database_url)
  }
}

fn run_supervised_pruner_restart_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let signal = process.new_subject()
  let assert Ok(handler_id) =
    sinal.handler_id("grind-test-supervised-pruner-restart")
  let assert Ok(attachment) =
    sinal.observe(
      handler_id,
      observation.prune_completed(),
      fn(_measurements, _metadata) { process.send(signal, Nil) },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let interval_ms = 200
  let assert Ok(policy) =
    pruner.default_policy()
    |> pruner.with_interval(interval_ms)
    |> pruner.validate_policy()
  let assert Ok(running) = pruner.start(database, policy)
  use <- exception.defer(fn() { pruner.stop(running) |> should.equal(Ok(Nil)) })

  // The first tick, and the moment right after it a fresh timer was
  // scheduled — killing the actor here maximises the window a leaked old
  // timer (if the bug were present) would still be pending in.
  let assert Ok(Nil) = process.receive(signal, within: 5000)
  let assert Ok(first_pid) = pruner.actor_pid(running)
  process.kill(first_pid)
  let assert Ok(_second_pid) = await_new_pruner_pid(running, first_pid, 500)

  // A window comfortably covering exactly one legitimate post-restart tick
  // (interval_ms out from *either* the kill or the restart, they are only
  // ever a few ms apart) without also reaching the next legitimate one.
  let deadline_ms = monotonic_ms() + interval_ms + interval_ms / 2
  count_events_until(signal, deadline_ms) |> should.equal(1)

  mark_database_test_executed("supervised-pruner-restart-ticks-once")
}
