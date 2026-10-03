import cigogne
import cigogne/config
import cigogne/migration
import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/migrations
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/concurrency.{spawn_submit}
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed}
import grind/support/migration_fixtures.{grind_catalog_digest}
import pog
import simplifile

/// Cigogne's own pinned sha256 for each *released* migration file
/// (`shasum -a 256`, uppercased to match `binary:encode_hex/1`) — a
/// Cigogne's own pinned sha256 (`shasum -a 256`, uppercased to match
/// `binary:encode_hex/1`) for every *released* migration file — one not
/// still under active development in this same change. Add one entry here,
/// computed once, whenever a migration file is released (see AGENTS.md,
/// "Adding a migration"); never recomputed from the file itself, or the pin
/// would be meaningless. `grind_migrations_conformance_test` requires a
/// pinned entry for every file except the newest (highest) version — that
/// one may still be edited in this same change, as v12 currently is.
const released_migration_sha256 = [
  #(
    "20260925000000-grind_v11.sql",
    "2B79E6CBD28A36850E31E1D69CC0C353D9CCFC4A4D8CEC688B0DC9C4ECEF17A0",
  ),
  #(
    "20260926000000-grind_v12.sql",
    "47364E79BB2EDB52C1BB99BA0D2E3498AB0B73ECF4B6EB0B3F39C5DD5F4DB32F",
  ),
]

/// Proves `grind/internal/migrations.migrations()` (what `postgres.migrate`
/// actually executes) stays in lockstep with the cigogne-format files under
/// `priv/migrations/`, using cigogne's own public parser and its own
/// `config.get("grind")` (exercising the real `priv/cigogne.toml`) rather
/// than Grind's own ad hoc parsing or a hand-built config — so a change to
/// cigogne's file format, or to `priv/cigogne.toml` itself, is caught here
/// too. Runs with no database:
///
/// - Every file's `up` statements (trailing `;` stripped) equal the
///   matching `migrations()` entry, in order.
/// - The version the file's own name encodes (`<timestamp>-grind_v<N>.sql`)
///   equals both the `migrations()` key and the version its own trailing
///   marker statement records — which must itself be a
///   `INSERT INTO grind_schema_migrations` statement, not merely end with a
///   number that happens to parse.
/// - Every file except the newest (highest) version has a pinned sha256 in
///   `released_migration_sha256` above, and it matches.
/// - The frozen upgrade-harness fixture (`test/fixtures/schema/v11.sql`)
///   equals the pinned v11 migration's own `up` section exactly — so the
///   two can never silently drift apart.
pub fn grind_migrations_conformance_test() {
  let assert Ok(config) = config.get("grind")
  let assert Ok(files) = cigogne.read_migrations(config)
  let sorted = list.sort(files, migration.compare)
  let defined = migrations.migrations()
  list.length(sorted) |> should.equal(list.length(defined))
  let newest_version =
    list.fold(defined, 0, fn(highest, step) { int.max(highest, step.version) })
  list.zip(sorted, defined)
  |> list.each(fn(pair) {
    let #(file, step) = pair
    let file_version = version_from_migration_name(file.name)
    file_version |> should.equal(step.version)
    let up_statements =
      file.queries_up
      |> list.map(fn(statement) { drop_trailing_semicolon(statement) })
    up_statements |> should.equal(step.statements)
    let assert Ok(marker_statement) = list.last(up_statements)
    string.starts_with(marker_statement, "INSERT INTO grind_schema_migrations")
    |> should.be_true()
    let assert Some(marker_version) = marker_insert_version(marker_statement)
    marker_version |> should.equal(step.version)
    let pinned_sha256 =
      list.key_find(released_migration_sha256, filename_from_path(file.path))
    case step.version == newest_version {
      True ->
        case pinned_sha256 {
          Error(Nil) -> Nil
          Ok(sha256) -> file.sha256 |> should.equal(sha256)
        }
      False -> {
        let assert Ok(sha256) = pinned_sha256
        file.sha256 |> should.equal(sha256)
      }
    }
  })
  let assert Ok(v11_step) = list.find(defined, fn(step) { step.version == 11 })
  let assert Ok(fixture_contents) =
    simplifile.read(from: "test/fixtures/schema/v11.sql")
  let fixture_statements =
    fixture_contents
    |> string.split("\n")
    |> list.filter(fn(line) { string.trim(line) != "" })
    |> string.join("")
    |> string.split(";")
    |> list.map(string.trim)
    |> list.filter(fn(statement) { statement != "" })
  fixture_statements |> should.equal(v11_step.statements)
}

fn drop_trailing_semicolon(statement: String) -> String {
  case string.ends_with(statement, ";") {
    True -> string.drop_end(statement, 1)
    False -> statement
  }
}

fn filename_from_path(path: String) -> String {
  path |> string.split("/") |> list.last() |> result.unwrap(path)
}

/// Parses the version out of a migration's own `name` field (the part of
/// its filename after `<timestamp>-`), which this repository's naming
/// convention (`grind_v<N>`) always encodes — see AGENTS.md.
fn version_from_migration_name(name: String) -> Int {
  let assert Ok(digits) = string.split_once(name, "grind_v")
  let assert Ok(version) = int.parse(digits.1)
  version
}

/// Parses the version out of the migration's own trailing marker insert
/// (`INSERT INTO grind_schema_migrations (version) VALUES (<N>)`), so the
/// conformance test can independently cross-check the file name against
/// what the file's own last statement actually records.
fn marker_insert_version(statement: String) -> Option(Int) {
  case string.split_once(statement, "VALUES (") {
    Error(Nil) -> None
    Ok(#(_, rest)) ->
      case string.split_once(rest, ")") {
        Error(Nil) -> None
        Ok(#(digits, _)) -> int.parse(string.trim(digits)) |> option.from_result
      }
  }
}

// -- Cigogne end-to-end (docs/RELEASE-READINESS.md, "Migration gaps") ------
//
// Grind ships `priv/migrations/*.sql` (cigogne format) as a mirror for an
// application that wants to apply Grind's schema through cigogne instead of
// `postgres.migrate` (see README, "Migrations").
// `grind_migrations_conformance_test` above already proves the two sources
// stay byte-for-byte in lockstep with no database — these tests prove the
// two mechanisms actually *interoperate* against a real PostgreSQL server:
// cigogne genuinely applies the files, Grind's own `read_schema_generation`
// (exercised through a follow-up `migrate`) accepts the result and treats it
// as a no-op, ordinary API traffic works against the cigogne-applied
// schema, cigogne's own down-then-up of `grind_v12` round-trips, and the two
// mechanisms' shared advisory lock genuinely serializes a concurrent caller
// of the other one.

@external(erlang, "grind_test_env", "cigogne_e2e_url")
fn cigogne_e2e_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "cigogne_e2e_fresh_url")
fn cigogne_e2e_fresh_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "cigogne_concurrent_url")
fn cigogne_concurrent_url() -> Result(String, Nil)

/// Builds the `cigogne.Config` for Grind's own package — reading the real
/// `priv/cigogne.toml` via `config.get`, exactly like
/// `grind_migrations_conformance_test` — pointed at `connection` instead of
/// opening a second pool of its own: a plain `pog.Connection` is already
/// shareable (every raw-SQL helper in this suite passes one around the same
/// way), so cigogne and the `postgres.Database` under test genuinely share
/// one pool rather than each managing an independent connection to the same
/// server. This is also the shape an application would use to run cigogne
/// against a `Database` it already started with `postgres.connection`,
/// rather than pointing cigogne at `DATABASE_URL`/`PGHOST` etc. separately
/// (see README, "Migrations").
fn cigogne_config_for(connection: pog.Connection) -> config.Config {
  let assert Ok(base) = config.get("grind")
  config.Config(..base, database: config.ConnectionDbConfig(connection))
}

fn schema_marker_count(connection: pog.Connection) -> Result(Int, Nil) {
  pog.query("SELECT count(*)::bigint FROM grind_schema_migrations")
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

/// Cigogne itself applies every file under `priv/migrations/` to a fresh
/// schema, Grind accepts the result (a follow-up `migrate` is a genuine
/// no-op, never `IncompatibleSchema`/`UnsupportedSchemaVersion`), the
/// cigogne-applied schema is fully functional for ordinary API traffic, its
/// catalog shape matches a fresh `postgres.migrate_with` install of the same
/// steps exactly, and cigogne's own down-then-up of `grind_v12` round-trips.
pub fn cigogne_applies_grind_files_then_migrate_is_noop_test() {
  case cigogne_e2e_url(), cigogne_e2e_fresh_url() {
    Ok(e2e_url), Ok(fresh_url) -> run_cigogne_e2e_test(e2e_url, fresh_url)
    _, _ -> Nil
  }
}

fn run_cigogne_e2e_test(e2e_url: String, fresh_url: String) -> Nil {
  let assert Ok(validated) = postgres.settings(e2e_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)

  // 1. Cigogne itself applies every file under `priv/migrations/` — real
  // DDL, through cigogne's own public engine, not Grind's `migrate`.
  let cigogne_config = cigogne_config_for(connection)
  let assert Ok(engine) = cigogne.create_engine(cigogne_config)
  cigogne.get_unapplied_migrations(engine) |> list.length |> should.equal(3)
  let assert Ok(Nil) = cigogne.apply_all(engine)

  // 2. Grind accepts the result: `read_schema_generation` (exercised via
  // `migrate`'s own version read) recognizes the schema as a legitimate,
  // fully up-to-date v13 install rather than `IncompatibleSchema`/
  // `UnsupportedSchemaVersion` — and running `migrate` against it is a
  // genuine no-op: no new marker rows, `Ok(Nil)` with zero steps run.
  let assert Ok(marker_count_before) = schema_marker_count(connection)
  marker_count_before |> should.equal(3)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(marker_count_after) = schema_marker_count(connection)
  marker_count_after |> should.equal(3)

  // 3. Fully functional for ordinary API traffic: submit, claim, and run a
  // job to completion against the cigogne-applied schema.
  let assert Ok(input_codec) =
    worker.codec(
      "cigogne-e2e-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "cigogne-e2e-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "cigogne.e2e-worker",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("cigogne-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("cigogne-e2e")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cigogne-e2e", definition, 9)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("cigogne-9")))

  // 4. Catalog shape matches a fresh `postgres.migrate_with` install of the
  // exact same steps — cigogne's own real DDL execution produced the exact
  // same schema `migrate` itself would have, not merely "a schema that
  // happens to pass validation".
  let assert Ok(fresh_validated) =
    postgres.settings(fresh_url) |> postgres.validate
  let assert Ok(fresh_database) = postgres.start(fresh_validated)
  use <- exception.defer(fn() { postgres.close(fresh_database) })
  let assert Ok(Nil) = postgres.migrate(fresh_database)
  grind_catalog_digest(connection)
  |> should.equal(grind_catalog_digest(postgres.connection(fresh_database)))

  // 5. Cigogne's own down migration of v12, then up again, round-trips: the
  // schema returns to a fully valid, functional v12 install again, and the
  // job seeded above survives (v12's own down section only drops
  // columns/constraints/indexes it itself added, never a whole table — see
  // README, "Migrations", on `grind_v11`'s own down section being the
  // destructive one, not v12's). Deliberately *not* a raw catalog-digest
  // comparison against the fresh install here — a genuine, inherent
  // PostgreSQL property, not a Grind defect, makes that comparison the
  // wrong check: dropping and re-adding a column never reuses its old
  // `ordinal_position` (`pg_attribute.attnum` only ever increases), so a
  // schema that has been through a real down/up cycle ends up with
  // different (higher) column numbers for `storage_owner`/`finished_at`
  // than a schema that never dropped them at all, even though every column,
  // constraint, index, and default is otherwise identical. `migrate` being
  // a genuine no-op again below is the right proof instead: it re-runs
  // `read_schema_generation`'s own shape check (existence-based, not
  // ordinal-position-based) against the round-tripped schema and would
  // report `IncompatibleSchema`/`MigrationShapeMismatch` were anything
  // actually missing.
  let assert Ok(engine_at_v12) = cigogne.create_engine(cigogne_config)
  let assert Ok(Nil) = cigogne.rollback(engine_at_v12)
  let assert Ok(marker_after_rollback) = schema_marker_count(connection)
  marker_after_rollback |> should.equal(2)
  let assert Ok(engine_at_v11) = cigogne.create_engine(cigogne_config)
  let assert Ok(Nil) = cigogne.apply(engine_at_v11)
  let assert Ok(marker_after_reapply) = schema_marker_count(connection)
  marker_after_reapply |> should.equal(3)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(marker_after_migrate_noop) = schema_marker_count(connection)
  marker_after_migrate_noop |> should.equal(3)
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("cigogne-9")))

  mark_database_test_executed("cigogne-e2e-migrate-noop-passed")
}

/// A 64-bit advisory key appears in `pg_locks` as two unsigned 32-bit halves;
/// `objsubid = 1` distinguishes it from PostgreSQL's two-key advisory locks.
/// Both observers below use this same exact key and the current database.
const migration_lock_identity = "held.locktype = 'advisory' AND held.mode = 'ExclusiveLock' AND held.granted "
  <> "AND held.database = (SELECT oid FROM pg_database WHERE datname = current_database()) "
  <> "AND held.classid::bigint = ((hashtextextended('grind-migrate-v1:' || current_schema(), 0) >> 32) & 4294967295) "
  <> "AND held.objid::bigint = (hashtextextended('grind-migrate-v1:' || current_schema(), 0) & 4294967295) "
  <> "AND held.objsubid = 1"

// Each observation allows 100 sleeps of 20 ms, plus query time. This leaves
// room to roll back a missing-edge assertion before the connection checkout
// reaches its default five-second deadline.
const lock_observation_checks = 100

fn cigogne_blocked_behind_table_holder(
  connection: pog.Connection,
  holder_pid: Int,
) -> Option(Int) {
  let assert Ok(returned) =
    pog.query(
      "SELECT held.pid FROM pg_locks held "
      <> "JOIN pg_locks table_wait ON table_wait.pid = held.pid "
      <> "WHERE "
      <> migration_lock_identity
      <> " AND table_wait.locktype = 'relation' "
      <> "AND table_wait.relation = 'grind_jobs'::regclass "
      <> "AND table_wait.mode = 'AccessExclusiveLock' AND NOT table_wait.granted "
      <> "AND $1::int = ANY(pg_blocking_pids(held.pid)) LIMIT 1",
    )
    |> pog.parameter(pog.int(holder_pid))
    |> pog.returning({
      use pid <- decode.field(0, decode.int)
      decode.success(pid)
    })
    |> pog.execute(on: connection)
  case returned.rows {
    [pid, ..] -> Some(pid)
    [] -> None
  }
}

fn await_cigogne_held_lock(
  connection: pog.Connection,
  holder_pid: Int,
  checks_remaining: Int,
) -> Option(Int) {
  case cigogne_blocked_behind_table_holder(connection, holder_pid) {
    Some(pid) -> Some(pid)
    None if checks_remaining > 0 -> {
      process.sleep(20)
      await_cigogne_held_lock(connection, holder_pid, checks_remaining - 1)
    }
    None -> None
  }
}

fn migrate_waiting_on_cigogne(
  connection: pog.Connection,
  cigogne_pid: Int,
) -> Bool {
  let assert Ok(returned) =
    pog.query(
      "SELECT count(*) FROM pg_locks held "
      <> "JOIN pg_locks waiting ON waiting.database = held.database "
      <> "AND waiting.classid = held.classid AND waiting.objid = held.objid "
      <> "AND waiting.objsubid = held.objsubid "
      <> "WHERE "
      <> migration_lock_identity
      <> " AND held.pid = $1::int AND waiting.pid <> held.pid "
      <> "AND waiting.locktype = 'advisory' AND waiting.mode = 'ExclusiveLock' "
      <> "AND NOT waiting.granted "
      <> "AND held.pid = ANY(pg_blocking_pids(waiting.pid))",
    )
    |> pog.parameter(pog.int(cigogne_pid))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  case returned.rows {
    [count] -> count == 1
    _ -> False
  }
}

fn await_migrate_waiting_on_cigogne(
  connection: pog.Connection,
  cigogne_pid: Int,
  checks_remaining: Int,
) -> Bool {
  case migrate_waiting_on_cigogne(connection, cigogne_pid) {
    True -> True
    False if checks_remaining > 0 -> {
      process.sleep(20)
      await_migrate_waiting_on_cigogne(
        connection,
        cigogne_pid,
        checks_remaining - 1,
      )
    }
    False -> False
  }
}

/// Cigogne applies `grind_v11` alone first (synchronously, no race — this is
/// the realistic "an app has already been running a while" starting point,
/// not a from-scratch install), then races cigogne applying `grind_v12`
/// alone against a concurrent `postgres.migrate` caller. A test transaction
/// holds an ACCESS SHARE lock on `grind_jobs`. Cigogne's real v12 migration
/// first takes the shared advisory key, then waits at its first ALTER TABLE
/// for that test lock. While it is held there, the test proves Cigogne owns
/// the exact advisory key and then proves `migrate` waits on that same key,
/// blocked by Cigogne's backend. Committing the test transaction releases
/// Cigogne to finish, after which `migrate` re-reads v12 and skips it.
pub fn cigogne_apply_serializes_with_concurrent_migrate_test() {
  case cigogne_concurrent_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cigogne_concurrent_test(database_url)
  }
}

fn run_cigogne_concurrent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)

  let assert Ok(engine_v11) =
    cigogne.create_engine(cigogne_config_for(connection))
  let assert Ok(Nil) = cigogne.apply(engine_v11)
  let assert Ok(marker_after_v11) = schema_marker_count(connection)
  marker_after_v11 |> should.equal(1)

  let engine_v12 = cigogne.create_engine(cigogne_config_for(connection))
  let assert Ok(engine_v12) = engine_v12
  cigogne.get_unapplied_migrations(engine_v12)
  |> list.length
  |> should.equal(2)

  let cigogne_result = process.new_subject()
  let migrate_result = process.new_subject()
  let assert Ok(Nil) =
    pog.transaction(connection, fn(holder_connection) {
      let assert Ok(_) =
        pog.query("LOCK TABLE grind_jobs IN ACCESS SHARE MODE")
        |> pog.execute(on: holder_connection)
      let assert Ok(pid_result) =
        pog.query("SELECT pg_backend_pid()")
        |> pog.returning({
          use pid <- decode.field(0, decode.int)
          decode.success(pid)
        })
        |> pog.execute(on: holder_connection)
      let assert [holder_pid] = pid_result.rows

      spawn_submit(cigogne_result, fn() { cigogne.apply(engine_v12) })
      let assert Some(cigogne_pid) =
        await_cigogne_held_lock(
          holder_connection,
          holder_pid,
          lock_observation_checks,
        )

      spawn_submit(migrate_result, fn() { postgres.migrate(database) })
      await_migrate_waiting_on_cigogne(
        holder_connection,
        cigogne_pid,
        lock_observation_checks,
      )
      |> should.equal(True)
      // The table lock remains held until this callback returns, so Cigogne
      // must still own the advisory lock when we explicitly release it.
      cigogne_blocked_behind_table_holder(holder_connection, holder_pid)
      |> should.equal(Some(cigogne_pid))
      Ok(Nil)
    })

  process.receive(cigogne_result, within: 10_000) |> should.equal(Ok(Ok(Nil)))
  process.receive(migrate_result, within: 10_000) |> should.equal(Ok(Ok(Nil)))

  let assert Ok(marker_count) = schema_marker_count(connection)
  marker_count |> should.equal(3)
  mark_database_test_executed("cigogne-migrate-concurrent-serialize-passed")
}
