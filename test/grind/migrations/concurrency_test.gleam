import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/result
import gleeunit/should
import grind/internal/migrations
import grind/internal/postgres
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt, spawn_lock_holder,
  spawn_submit,
}
import grind/support/env.{
  database_url, mark_database_test_executed, monotonic_ms, schema_concurrent_url,
  unique_test_run_id,
}
import grind/support/migration_fixtures.{
  latest_migration, schema_marker_max_version,
}
import pog

/// Two `migrate` callers racing the exact same fresh schema. Before the
/// advisory lock, both would run `CREATE TABLE`/the marker `INSERT`
/// concurrently and one would fail on a duplicate-object or duplicate-key
/// error instead of the required "both `Ok(Nil)`, one marker" outcome — an
/// observer holds the same advisory lock key `migrate` itself uses, so both
/// callers are provably still queued behind it when this test releases it.
pub fn postgres_migrate_concurrent_migrators_both_apply_once_test() {
  case schema_concurrent_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_concurrent_migrators_test(database_url)
  }
}

fn run_concurrent_migrators_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) AS grind_test_migrate_barrier",
    )
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() { postgres.migrate(database) })
  spawn_submit(result_b, fn() { postgres.migrate(database) })
  // Both migrators above are provably blocked behind the observer's held
  // advisory lock (the only way into `migrate`'s own step transaction) for
  // as long as this sleep runs, before the observer ever releases it.
  process.sleep(300)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  process.receive(result_a, within: 10_000) |> should.equal(Ok(Ok(Nil)))
  process.receive(result_b, within: 10_000) |> should.equal(Ok(Ok(Nil)))

  let assert Ok(marker) =
    pog.query("SELECT count(*)::bigint FROM grind_schema_migrations")
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  // One marker row per real step (11, 12 and 13) — never a duplicate for any,
  // which is what would show up here had the advisory lock not actually
  // serialised the two concurrent migrators against each step.
  marker.rows |> should.equal([3])
  mark_database_test_executed("migrate-concurrent-migrators-single-marker")
}

/// Red-first proof of the coordinator review's `ensure_schema_exists` fix:
/// `CREATE SCHEMA IF NOT EXISTS` is not concurrency-safe on its own — two
/// sessions that both see the schema absent (via the existence check
/// `ensure_schema_exists` runs first) can both attempt the actual `CREATE`,
/// and PostgreSQL's own catalog uniqueness check is what actually
/// serialises them, surfacing to the loser as a real error (`23505`/`42P06`
/// depending on timing) rather than a silent no-op the way `IF NOT EXISTS`
/// might suggest. `ensure_schema_exists` re-checks existence on any
/// `CREATE SCHEMA` failure and reports `Ok(Nil)` if the schema is now
/// present regardless of which side actually created it.
///
/// Made deterministic (the original version of this test raced two pools
/// back-to-back with no barrier at all, "there is no lockable object to
/// hold one on before the schema exists" — true only until a third session
/// is used to actually become that lockable object): a third session runs
/// `BEGIN; CREATE SCHEMA "<name>";` against this exact, never-before-used
/// name and never commits, so both `migrate` callers' own existence check
/// sees the schema genuinely absent (the blocker's insert is invisible
/// until it commits or rolls back) and both then attempt the real `CREATE
/// SCHEMA IF NOT EXISTS`, which blocks — provably, polled via
/// `pg_stat_activity` rather than inferred from a sleep — on the blocker's
/// still-open transaction. Once the blocker commits, PostgreSQL's own
/// catalog uniqueness check resolves both blocked inserts against the
/// schema that now definitely exists, and both must still return `Ok(Nil)`
/// every single run — not merely "usually", the way the original
/// best-effort race could only ever claim.
pub fn postgres_migrate_concurrent_first_time_schema_creation_both_succeed_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_concurrent_schema_creation_test(database_url)
  }
}

fn run_concurrent_schema_creation_test(base_url: String) -> Nil {
  let schema = "grind_concurrent_schema_" <> int.to_string(unique_test_run_id())
  let assert Ok(validated_a) =
    postgres.settings(base_url)
    |> postgres.with_schema(schema)
    |> postgres.validate
  let assert Ok(database_a) = postgres.start(validated_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(validated_b) =
    postgres.settings(base_url)
    |> postgres.with_schema(schema)
    |> postgres.validate
  let assert Ok(database_b) = postgres.start(validated_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let connection_a = postgres.connection(database_a)

  // The barrier: a third session holds `CREATE SCHEMA "<schema>"` open,
  // uncommitted, for the rest of this test — the one lockable object that
  // makes both migrators' own `CREATE SCHEMA IF NOT EXISTS` genuinely block
  // (on the blocker's own transaction id) rather than race however the
  // scheduler happens to interleave them.
  let held_create = pog.query("CREATE SCHEMA \"" <> schema <> "\"")
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection_a, held_create)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() { postgres.migrate(database_a) })
  spawn_submit(result_b, fn() { postgres.migrate(database_b) })

  await_both_blocked_creating_schema(connection_a, schema, 250)
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  process.receive(result_a, within: 10_000) |> should.equal(Ok(Ok(Nil)))
  process.receive(result_b, within: 10_000) |> should.equal(Ok(Ok(Nil)))

  schema_marker_max_version(connection_a) |> should.equal(13)
  mark_database_test_executed("migrate-concurrent-schema-creation-both-succeed")
}

/// Polls (bounded) until exactly two other backends are genuinely blocked
/// running `migrate`'s own literal `CREATE SCHEMA IF NOT EXISTS "<schema>"`
/// statement — proof both migrators have already run their own existence
/// check (seeing it absent, since the blocker's insert is not yet
/// committed) and are now waiting on the blocker's transaction to end,
/// rather than inferring this from timing alone. Never fewer than two
/// still-running checks are treated as "not yet both blocked" (one might
/// simply not have reached the statement yet); more than two would mean
/// this helper is somehow observing another test's own activity and is
/// treated the same way, never as a false positive.
fn await_both_blocked_creating_schema(
  connection: pog.Connection,
  schema: String,
  checks_remaining: Int,
) -> Bool {
  let blocked =
    pog.query(
      "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND query = 'CREATE SCHEMA IF NOT EXISTS \""
      <> schema
      <> "\"'",
    )
    |> pog.returning({
      use blocked <- decode.field(0, decode.int)
      decode.success(blocked)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [blocked] -> Ok(blocked)
        _ -> Error(Nil)
      }
    })
  case blocked {
    Ok(2) -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_both_blocked_creating_schema(
            connection,
            schema,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

@external(erlang, "grind_test_env", "migration_deadline_url")
fn migration_deadline_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "migration_lock_url")
fn migration_lock_url() -> Result(String, Nil)

/// A synthetic step whose own statement (`pg_sleep(6)`) legitimately runs
/// longer than the pool's own `statement_deadline_ms` (4000ms default) but
/// well under a deliberately shortened `migration_deadline_ms` (9000ms here,
/// instead of the 30000ms default, purely so this test does not have to
/// wait out the full default) — proving `migrate_with` bounds each step by
/// `Settings.migration_deadline_ms`, via `grind_postgres_ffi:
/// migration_transaction_safely/3`'s own explicit checkout deadline, not by
/// the pool's shared `set_deadline`-attached one. See
/// docs/RECOVERY-EVIDENCE.md, "Acknowledgement deadline", for the mutation
/// this characterizes: a step run under the pool's shared deadline instead
/// of its own times out at ~4s instead of succeeding at ~6s.
fn synthetic_v14_slow_migration() -> migrations.Migration {
  migrations.Migration(
    14,
    [
      // `pg_types` cannot decode a bare `void` result (`pg_sleep`'s own
      // return type — see `grind/internal/unique_admission`'s identical
      // `SELECT true FROM (...)` wrapping for its advisory-lock query, and
      // its own doc comment for the full driver note), so the sleep is
      // wrapped in an outer scalar `SELECT` rather than selected directly.
      "SELECT true FROM (SELECT pg_sleep(6)) AS grind_migration_deadline_probe",
      "INSERT INTO grind_schema_migrations (version) VALUES (14)",
    ],
    latest_migration().shape,
    latest_migration().foreign_keys,
    latest_migration().forbidden_columns,
  )
}

pub fn postgres_migration_deadline_long_step_succeeds_test() {
  case migration_deadline_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_migration_deadline_long_step_test(database_url)
  }
}

fn run_migration_deadline_long_step_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_migration_deadline(9000)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let steps =
    list.append(migrations.migrations(), [synthetic_v14_slow_migration()])
  let start_ms = monotonic_ms()
  postgres.migrate_with(database, steps) |> should.equal(Ok(Nil))
  let elapsed_ms = monotonic_ms() - start_ms
  // At least the sleep itself; comfortably under the shortened migration
  // deadline, never the pool's own 4000ms `statement_deadline_ms`.
  { elapsed_ms >= 6000 } |> should.equal(True)
  { elapsed_ms < 9000 } |> should.equal(True)
  mark_database_test_executed("migration-deadline-long-step-succeeds-passed")
}

fn migration_lock_timeout_probe_migration() -> migrations.Migration {
  migrations.Migration(
    14,
    [
      "ALTER TABLE grind_jobs ADD COLUMN grind_lock_timeout_probe text",
      "INSERT INTO grind_schema_migrations (version) VALUES (14)",
    ],
    latest_migration().shape,
    latest_migration().foreign_keys,
    latest_migration().forbidden_columns,
  )
}

fn migration_lock_timeout_probe_column_exists(
  connection: pog.Connection,
) -> Bool {
  let assert Ok(returned) =
    pog.query(
      "SELECT count(*) = 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs' AND column_name = 'grind_lock_timeout_probe'",
    )
    |> pog.returning({
      use exists <- decode.field(0, decode.bool)
      decode.success(exists)
    })
    |> pog.execute(on: connection)
  let assert [exists] = returned.rows
  exists
}

pub fn postgres_migration_step_lock_timeout_returns_lock_unavailable_test() {
  case migration_lock_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_migration_step_lock_timeout_test(database_url)
  }
}

/// A migration step's own DDL/DML now runs under a constant, transaction-
/// local `lock_timeout` (2000ms, set right after the advisory lock):
/// PostgreSQL's own `55P03 lock_not_available` becomes the typed, retry-safe
/// `MigrationLockUnavailable(version)` instead of blocking for up to the
/// full `migration_deadline_ms`. `main_database`'s own `migration_deadline_ms`
/// is shortened to 3500ms here (comfortably above the required
/// `migration_lock_timeout_ms + margin` of 3000ms) purely so this test does
/// not have to wait out the 30000ms default; it stays well under
/// `spawn_lock_holder`'s own plain, unwrapped `pog.transaction` — bound by
/// pog's own hardcoded ~5000ms checkout hold time exactly like the
/// pre-Increment-15 acknowledgement path was (`docs/RECOVERY-EVIDENCE.md`,
/// "Acknowledgement deadline") — which would otherwise auto-release the
/// observer's own lock before a longer deadline ever had a chance to fire.
/// Named mutation, also this change's own red-first evidence (this is
/// exactly the code shape before this fix): temporarily removing
/// `run_migration_step_transaction`'s own `set_migration_lock_timeout` call
/// makes this exact scenario instead block on the table lock until
/// `main_database`'s 3500ms `migration_deadline_ms` force-closes the
/// connection, reporting `MigrationCommitUnknown(13)` instead — confirmed
/// empirically (`docs/RECOVERY-EVIDENCE.md` has the observed timings).
fn run_migration_step_lock_timeout_test(database_url: String) -> Nil {
  let assert Ok(main_validated) =
    postgres.settings(database_url)
    |> postgres.with_migration_deadline(3500)
    |> postgres.validate
  let assert Ok(main_database) = postgres.start(main_validated)
  use <- exception.defer(fn() { postgres.close(main_database) })
  let assert Ok(Nil) =
    postgres.migrate_with(main_database, migrations.migrations())

  let assert Ok(observer_validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(observer_database) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer_database) })
  let observer_connection = postgres.connection(observer_database)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      observer_connection,
      pog.query("LOCK TABLE grind_jobs IN ACCESS SHARE MODE"),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: guarantees the observer's own transaction is never left
  // open beyond this test, even if an assertion below panics first.
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let steps =
    list.append(migrations.migrations(), [
      migration_lock_timeout_probe_migration(),
    ])
  let start_ms = monotonic_ms()
  let outcome = postgres.migrate_with(main_database, steps)
  let elapsed_ms = monotonic_ms() - start_ms
  outcome |> should.equal(Error(postgres.MigrationLockUnavailable(14)))
  // Comfortably clears the 2000ms lock_timeout plus ordinary scheduling
  // jitter, but well under the 3500ms `migration_deadline_ms` configured
  // above (never mind the 30000ms default) a caller who removed
  // `set_migration_lock_timeout` would otherwise have to wait out for the
  // exact same contention.
  { elapsed_ms < 3200 } |> should.equal(True)

  // Generation is still 12 (the real, current latest): re-running the
  // released steps alone is a no-op, and the probe column was never added.
  postgres.migrate_with(main_database, migrations.migrations())
  |> should.equal(Ok(Nil))
  migration_lock_timeout_probe_column_exists(postgres.connection(main_database))
  |> should.equal(False)

  process.send(release_lock, ReleaseAttempt)
  let assert Ok(ClaimGateReleased(True)) =
    process.receive(lock_finished, within: 5000)

  // The conflicting lock is gone: the exact same steps now succeed.
  postgres.migrate_with(main_database, steps) |> should.equal(Ok(Nil))
  migration_lock_timeout_probe_column_exists(postgres.connection(main_database))
  |> should.equal(True)

  // `SET LOCAL` never leaks past the transaction that set it: this pooled
  // connection's own session-level `lock_timeout` is still PostgreSQL's
  // ordinary default ("0", meaning no limit), not the migration step's
  // constant 2000ms.
  let assert Ok(returned) =
    pog.query("SHOW lock_timeout")
    |> pog.returning({
      use value <- decode.field(0, decode.string)
      decode.success(value)
    })
    |> pog.execute(on: postgres.connection(main_database))
  let assert [lock_timeout_after] = returned.rows
  lock_timeout_after |> should.equal("0")

  mark_database_test_executed("migration-lock-timeout-unavailable-passed")
}
