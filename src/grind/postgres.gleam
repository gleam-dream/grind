//// Public PostgreSQL facade: configuration, database ownership, and typed
//// storage operations. Migration execution, audited resolution, and job
//// reads live in `grind/internal/postgres`.

import gleam/bit_array
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import grind/internal/lease
import grind/internal/migrations
import grind/internal/postgres/job_reads as postgres_job_reads
import grind/internal/postgres/migration as postgres_migration
import grind/internal/postgres/resolution as postgres_resolution
import grind/internal/sql
import grind/internal/store
import grind/internal/unique_admission
import grind/job.{type JobHandle, type State}
import grind/observation
import grind/submission
import grind/unique
import grind/worker.{type Worker}
import pog
import sinal/forwarder.{type Forwarder}

/// Pure PostgreSQL pool settings. Validation does not acquire a resource.
/// Carries no pog type of its own — not `process.Name(pog.Message)` or
/// anything else pog-specific — so a caller never has to reach into pog's
/// own API just to configure Grind. The pool's own name is created once,
/// inside `validate`, and stays stable for that resulting `ValidatedSettings`
/// value's entire lifetime, across every `start`/`close` cycle; see
/// `validate` and `start`.
pub type Settings {
  Settings(
    database_url: String,
    pool_size: Int,
    unique_lock_wait_ms: Int,
    observation_capacity: Int,
    statement_deadline_ms: Int,
    migration_deadline_ms: Int,
    schema: String,
  )
}

/// `statement_deadline_ms` defaults to 4000, not pog's own hardcoded
/// 5000 — chosen so the default `unique_lock_wait_ms` (2000) clears
/// `validate`'s own margin against it, and so `queue.start`'s lease rule
/// (`6 * statement_deadline_ms` at `maximum_concurrency > 1`) stays
/// comfortably under `queue.default_policy`'s 30 000 ms lease without
/// having to raise that default. See docs/RELEASE-READINESS.md,
/// "Acknowledgement deadline".
pub fn settings(database_url: String) -> Settings {
  Settings(
    database_url:,
    pool_size: 10,
    unique_lock_wait_ms: 2000,
    observation_capacity: 1024,
    statement_deadline_ms: 4000,
    migration_deadline_ms: 30_000,
    schema: "public",
  )
}

pub fn with_pool_size(settings: Settings, pool_size: Int) -> Settings {
  Settings(..settings, pool_size:)
}

/// Sets the bounded wait for `submit_unique`'s admission lock, in
/// milliseconds. Exceeding it surfaces as `AdmissionContended` rather
/// than blocking indefinitely. Also bounds `submit_with_id`'s internal wait
/// on the `grind_unique_submissions` primary key when a concurrent same-id
/// writer's insert is still uncommitted — `submit_with_id` has no
/// domain-wide advisory lock of its own (see
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts"), so this same
/// setting is what keeps that wait bounded too. Validated positive by
/// `validate`, before any pool starts.
pub fn with_unique_lock_wait(
  settings: Settings,
  milliseconds: Int,
) -> Settings {
  Settings(..settings, unique_lock_wait_ms: milliseconds)
}

/// Sets the Grind-owned checkout deadline, in milliseconds, that bounds
/// every Grind storage call against this pool — inline SQL, every
/// Squirrel-generated call, and a whole `pog.transaction` including its own
/// `BEGIN`/`COMMIT` (`src/grind_postgres_ffi.erl`). A lost reply on a
/// half-open socket (`docs/RECOVERY-EVIDENCE.md`, "Acknowledgement
/// deadline") surfaces as `QueueAckUnknown`/`pog.QueryTimeout` within
/// roughly this bound instead of hanging indefinitely. Validated positive,
/// and against `unique_lock_wait_ms`, by `validate`. Also sets the pooled
/// connection's own `idle_in_transaction_session_timeout` startup parameter
/// to twice this value, so a request whose *reply* is lost but which never
/// even reached PostgreSQL (the request itself dropped) does not leave a
/// real server session idle in transaction, holding row locks, forever —
/// see DEFECT 3 in docs/RELEASE-READINESS.md.
pub fn with_statement_deadline(
  settings: Settings,
  milliseconds: Int,
) -> Settings {
  Settings(..settings, statement_deadline_ms: milliseconds)
}

/// Sets the checkout deadline used only by `migrate`'s own transactions, in
/// milliseconds — separate from `statement_deadline_ms` because a schema
/// migration's DDL step can legitimately need longer than an ordinary
/// job-lifecycle statement. Applies *per step*, not to the whole
/// `migrate`/`migrate_with` call: each version in `migrations()` runs in its
/// own transaction (see `migrate_with`'s own doc comment), and this deadline
/// bounds each one independently rather than the sum across every version
/// applied in one call. Validated positive by `validate`.
pub fn with_migration_deadline(
  settings: Settings,
  milliseconds: Int,
) -> Settings {
  Settings(..settings, migration_deadline_ms: milliseconds)
}

/// Sets the bounded number of `[grind, job, *]` observations the package's
/// own `sinal/forwarder.Forwarder` holds in flight at once. Exceeding it
/// drops the observation and is reported once per drain via
/// `sinal/forwarder.dropped_event` (`[sinal, forwarder, dropped]`); it never
/// affects job outcomes. Validated positive by `validate`, before any
/// process starts.
pub fn with_observation_capacity(
  settings: Settings,
  capacity: Int,
) -> Settings {
  Settings(..settings, observation_capacity: capacity)
}

/// Sets the PostgreSQL schema every pooled connection's `search_path` is
/// pinned to exactly (`validate`, via a `pog.connection_parameter`) — the one
/// schema this `Database` reads and writes, and the schema `migrate`/
/// `migrate_with` creates if it does not yet exist. Defaults to `"public"`.
/// See `README.md`, "Isolation": the schema, not any value derived from the
/// database URL or role, is the whole unit of isolation between logically
/// distinct Grind installations sharing one PostgreSQL cluster. Validated
/// non-empty by `validate`; never SQL-injected — every use of `schema` is
/// either a properly quoted identifier (`CREATE SCHEMA IF NOT EXISTS`, the
/// `search_path` connection parameter) or an ordinary bound query parameter
/// (the uniqueness admission lock key, see
/// `grind/internal/unique_admission/query.lock_key_sql`), never spliced as
/// unescaped text.
pub fn with_schema(settings: Settings, schema: String) -> Settings {
  Settings(..settings, schema:)
}

pub type ConfigError {
  InvalidDatabaseUrl
  InvalidPoolSize
  InvalidUniqueLockWait
  InvalidObservationCapacity
  InvalidStatementDeadline
  InvalidMigrationDeadline
  /// `Settings.schema` is empty, exceeds PostgreSQL's own 63-*byte*
  /// identifier limit (`NAMEDATALEN` 64, minus the terminator — checked in
  /// bytes, not characters, since a multibyte name can cross that limit
  /// well under 63 characters), contains a NUL byte (PostgreSQL text values
  /// cannot hold one at all — a driver or C-level truncation at that byte
  /// would otherwise silently change which schema this actually names, a
  /// confusion Grind would rather reject outright than risk), is exactly
  /// `"$user"` (PostgreSQL's own `search_path` treats an unquoted `$user`
  /// token as "substitute the current role's own name" — but
  /// `with_schema`'s value is always double-quoted when it reaches
  /// `search_path` (`quote_ident`), which makes a *literal* schema named
  /// `$user` behave differently again: a quoted `"$user"` is looked up as
  /// that literal name, not substituted, and PostgreSQL's own migration
  /// advisory lock key derivation for such a name can end up `NULL` in
  /// edge cases — configuring it at all is never something a caller
  /// actually means, only ever a copy-paste of the *unquoted* convention
  /// documented in risk 7), or starts with the reserved `pg_` prefix
  /// (PostgreSQL reserves every `pg_`-prefixed schema name for its own
  /// system and temporary schemas; `CREATE SCHEMA` refuses one outright,
  /// so accepting it here would only defer that same rejection to
  /// `migrate`, with a less specific error). See `with_schema` and
  /// `docs/RISKS.md` risk 7.
  InvalidSchema
  /// `unique_lock_wait_ms` is within roughly one second of
  /// `statement_deadline_ms` (DEFECT 1, docs/RELEASE-READINESS.md): default
  /// PostgreSQL's own `55P03 lock_not_available` (raised once
  /// `unique_lock_wait_ms` elapses, mapped to `submission.AdmissionContended`)
  /// would otherwise race the checkout deadline itself force-closing the
  /// connection first, surfacing as `NotCommitted(QueryTimeout)` or a
  /// commit-unknown outcome instead of the typed contention outcome.
  UniqueLockWaitTooCloseToDeadline
  /// `migration_deadline_ms` is within roughly one second of the constant
  /// migration-step `lock_timeout` (`migration_lock_timeout_ms`, 2000ms):
  /// PostgreSQL's own `55P03 lock_not_available` would otherwise race the
  /// migration checkout deadline itself force-closing the connection first,
  /// surfacing as `MigrationCommitUnknown` instead of the typed
  /// `MigrationLockUnavailable`.
  MigrationDeadlineTooCloseToLockTimeout
}

pub opaque type ValidatedSettings {
  ValidatedSettings(
    pog.Config,
    unique_lock_wait_ms: Int,
    observation_capacity: Int,
    statement_deadline_ms: Int,
    migration_deadline_ms: Int,
    schema: String,
  )
}

/// Double-quotes a PostgreSQL identifier, doubling any embedded double
/// quote, so it round-trips exactly through both a `CREATE SCHEMA` statement
/// and a `search_path` connection-parameter value regardless of case or
/// special characters — never trusted as already safe, and never spliced
/// unquoted anywhere `schema` reaches SQL text.
fn quote_ident(name: String) -> String {
  "\"" <> string.replace(name, "\"", "\"\"") <> "\""
}

/// The margin `validate` requires between `unique_lock_wait_ms` and
/// `statement_deadline_ms` (DEFECT 1). Not itself configurable: it exists so
/// PostgreSQL's own lock-wait error has time to actually surface and be
/// mapped to `submission.AdmissionContended` before the checkout deadline would
/// otherwise force-close the connection first.
const unique_lock_wait_margin_ms = 1000

/// PostgreSQL's own identifier length limit: `NAMEDATALEN` is 64 bytes
/// including the C string's own terminator, so any identifier — a schema
/// name among them — is silently truncated to 63 bytes if it is any longer.
/// `validate` rejects a longer `Settings.schema` outright rather than
/// letting it silently resolve to a truncated, different schema name.
const max_schema_name_bytes = 63

/// `Settings.schema` must be non-empty, at or under PostgreSQL's own
/// 63-byte identifier limit, free of NUL bytes, not the literal `"$user"`,
/// and not `pg_`-prefixed — see `ConfigError`'s own `InvalidSchema` doc
/// comment for why each of these matters.
fn schema_is_valid(schema: String) -> Bool {
  schema != ""
  && bit_array.byte_size(bit_array.from_string(schema)) <= max_schema_name_bytes
  && !string.contains(schema, "\u{0}")
  && schema != "$user"
  && !string.starts_with(schema, "pg_")
}

/// The constant transaction-local `lock_timeout` (milliseconds) every
/// migration step's own transaction sets, right after its advisory lock and
/// before running that step's statements — never itself configurable
/// (unlike `unique_lock_wait_ms`): a migration step's own failure mode
/// should stay fast and predictable regardless of how a caller tunes
/// `migration_deadline_ms`. Bounds a DDL statement that would otherwise wait
/// on a conflicting lock — an `ALTER`/`CREATE INDEX` against a large,
/// actively used table under concurrent access — for up to the full
/// `migration_deadline_ms`, or until the pooled connection is force-closed
/// if that elapses first; hitting it surfaces PostgreSQL's own `55P03` as
/// the typed `MigrationLockUnavailable`, safe to retry. Not applied to
/// `priv/migrations/*.sql` run directly through cigogne — see README,
/// "Migrations".
const migration_lock_timeout_ms = 2000

/// The margin `validate` requires between `migration_lock_timeout_ms` and
/// `migration_deadline_ms`, mirroring `unique_lock_wait_margin_ms`: without
/// it, a caller-configured `migration_deadline_ms` at or below the constant
/// lock timeout would force-close the connection before PostgreSQL's own
/// `55P03` ever has a chance to surface as `MigrationLockUnavailable`.
const migration_lock_timeout_margin_ms = 1000

/// Checks the URL and pool bound before any PostgreSQL process is started.
/// Each successful call creates one new Erlang atom for the pool's own name
/// (`process.new_name`, below) — atoms are never garbage-collected or
/// reclaimed by the runtime, so calling `validate` in a hot loop (rather than
/// once per logical database and reusing the resulting `ValidatedSettings`
/// across `start`/`close` cycles) leaks one atom per call for the life of
/// the node.
///
/// Every pooled connection is also given a `default_transaction_isolation
/// = 'read committed'` startup parameter (`pog.connection_parameter`),
/// overriding whatever the connecting role or database's own
/// `default_transaction_isolation` is configured to. This is not defensive
/// decoration: several of Grind's transactions depend on `READ COMMITTED`
/// semantics — a plain read after a wait must see what committed during
/// that wait (the uniqueness admission transaction's domain lock), and a
/// fenced `UPDATE` racing a concurrent retry of the exact same command must
/// not surface PostgreSQL's `REPEATABLE READ`/`SERIALIZABLE` conflict
/// handling (`40001 serialization_failure`) in place of the idempotent
/// result that retry is supposed to get (the acknowledgement path) — and
/// neither depends on a caller never configuring their role or database
/// with a non-default isolation level. See `docs/UNIQUENESS-CONTRACT.md`,
/// "Admission transaction" step 1, and `docs/RECOVERY-EVIDENCE.md`,
/// "Isolation-level pinning", for the full rationale and mutation evidence.
/// `grind/internal/unique_admission`'s own `pin_read_committed` (`SET
/// TRANSACTION ISOLATION LEVEL READ COMMITTED` as that transaction's own
/// first statement) is kept as defense in depth on top of this — a
/// connection pooler between Grind and PostgreSQL could drop or ignore a
/// startup parameter, where an in-transaction `SET TRANSACTION` cannot be
/// silently dropped the same way.
pub fn validate(settings: Settings) -> Result(ValidatedSettings, ConfigError) {
  case
    settings.pool_size > 0,
    settings.statement_deadline_ms > 0,
    settings.migration_deadline_ms > 0,
    settings.unique_lock_wait_ms > 0,
    settings.unique_lock_wait_ms + unique_lock_wait_margin_ms
    < settings.statement_deadline_ms,
    settings.observation_capacity > 0,
    migration_lock_timeout_ms + migration_lock_timeout_margin_ms
    < settings.migration_deadline_ms,
    schema_is_valid(settings.schema)
  {
    False, _, _, _, _, _, _, _ -> Error(InvalidPoolSize)
    True, False, _, _, _, _, _, _ -> Error(InvalidStatementDeadline)
    True, True, False, _, _, _, _, _ -> Error(InvalidMigrationDeadline)
    True, True, True, False, _, _, _, _ -> Error(InvalidUniqueLockWait)
    True, True, True, True, False, _, _, _ ->
      Error(UniqueLockWaitTooCloseToDeadline)
    True, True, True, True, True, False, _, _ ->
      Error(InvalidObservationCapacity)
    True, True, True, True, True, True, False, _ ->
      Error(MigrationDeadlineTooCloseToLockTimeout)
    True, True, True, True, True, True, True, False -> Error(InvalidSchema)
    True, True, True, True, True, True, True, True ->
      // The pool's own name, created here rather than accepted from the
      // caller (`Settings` carries no pog type of its own) — and created
      // once, here, rather than fresh on every `start`: a `ValidatedSettings`
      // value that is `start`ed more than once (closing and reopening
      // "the same" database — see e.g.
      // `postgres_closed_pool_renewal_recovers_without_rerun_test`) must
      // reopen under the *same* name each time, because a `Consumer`
      // captures its `Database`'s `connection` (`{pool, Name}`) once, at
      // `queue.start`, and never learns about a later, separate `Database`
      // value — its own renewal/claim/ack calls keep addressing that one
      // captured name for as long as the consumer runs. A fresh name per
      // `start` would silently orphan any consumer across a close/reopen
      // cycle instead of letting it recover once the same name is
      // re-registered.
      case
        pog.url_config(
          process.new_name("grind_postgres_pool"),
          settings.database_url,
        )
      {
        Error(_) -> Error(InvalidDatabaseUrl)
        Ok(config) ->
          Ok(ValidatedSettings(
            config
              |> pog.pool_size(settings.pool_size)
              |> pog.connection_parameter(
                name: "default_transaction_isolation",
                value: "read committed",
              )
              |> pog.connection_parameter(
                name: "idle_in_transaction_session_timeout",
                value: int.to_string(2 * settings.statement_deadline_ms),
              )
              // Pins every pooled connection's `search_path` to exactly this
              // one configured schema — see `with_schema`'s own doc comment
              // and `docs/RISKS.md` #7. This is what makes the advisory lock
              // key's bound `schema` parameter (see
              // `grind/internal/unique_admission/query.lock_key_sql`) and every
              // `current_schema()`-based query elsewhere in this module
              // (`read_schema_generation` and friends) agree by
              // construction: there is exactly one schema in `search_path`,
              // so `current_schema()` can only ever resolve to it (or to
              // nothing at all, before `migrate` first creates it) — never
              // silently fall through to an unrelated schema earlier in a
              // longer `search_path`, the `$user` hazard this pinning
              // exists to close.
              |> pog.connection_parameter(
                name: "search_path",
                value: quote_ident(settings.schema),
              ),
            unique_lock_wait_ms: settings.unique_lock_wait_ms,
            observation_capacity: settings.observation_capacity,
            statement_deadline_ms: settings.statement_deadline_ms,
            migration_deadline_ms: settings.migration_deadline_ms,
            schema: settings.schema,
          ))
      }
  }
}

pub opaque type Database {
  Database(
    connection: pog.Connection,
    supervisor_pid: process.Pid,
    installation: job.Installation,
    unique_lock_wait_ms: Int,
    statement_deadline_ms: Int,
    migration_deadline_ms: Int,
    forwarder: Forwarder,
  )
}

/// This `Database`'s own installation token — see `grind/job`'s
/// `Installation` type doc comment. Exposed only so the test suite can hold
/// the exact same uniqueness advisory lock a real `submit_unique` call
/// would (`unique_domain_lock_query` in `test/grind/support/lock_wait.gleam`,
/// exercised by `test/grind/unique/lock_contention_test.gleam`); never part
/// of the public API.
@internal
pub fn installation(database: Database) -> job.Installation {
  let Database(installation:, ..) = database
  installation
}

/// The Grind-owned checkout deadline (milliseconds) bounding every storage
/// call made against this `Database` — see `statement_deadline` and
/// `src/grind_postgres_ffi.erl`. `queue.start` reads this to enforce the
/// lease rule (`LeaseTooShortForDeadline`).
pub fn statement_deadline_ms(database: Database) -> Int {
  let Database(statement_deadline_ms:, ..) = database
  statement_deadline_ms
}

pub type StartError {
  PoolStartFailed(actor.StartError)
  /// The pool started, but the one-time query reading this database's own
  /// OID (`current_database()`, used to build this `Database`'s in-memory
  /// `Installation` token — see `grind/job`'s doc comment for that type)
  /// failed or its reply was lost. The pool itself is left running; a
  /// caller may retry `start` (after `close`ing this attempt's pool, to
  /// avoid `PoolStartFailed` on the retry's own `start` — see `start`'s own
  /// doc comment on reusing `ValidatedSettings`' pool name) once the store
  /// is reachable.
  InstallationQueryFailed(pog.QueryError)
}

/// Starts a package-owned PostgreSQL pool after settings have been
/// validated. Reuses `validate`'s own pool name (`ValidatedSettings` never
/// exposes it — `Settings` carries no pog type of its own, and this
/// function's own caller never needs to know or choose it either) rather
/// than creating a new one here: a `Consumer` captures its `Database`'s
/// `connection` (`{pool, Name}`) once, at `queue.start`, and never learns
/// about a later, separate `Database` value, so reopening "the same"
/// database — closing, then calling `start` again with the *same*
/// `ValidatedSettings` — must reuse the same name for an existing consumer
/// to ever recover; see `validate`'s own doc comment. Starting two
/// independent databases still means calling `settings`/`validate` twice,
/// each producing its own distinct name. Starting the *same*
/// `ValidatedSettings` a second time without an intervening `close` fails
/// with `PoolStartFailed`, since the pool's own atom name is already
/// registered to the still-running first pool.
///
/// Isolation between logically distinct Grind installations sharing one
/// PostgreSQL cluster is the schema this pool's `search_path` resolves to —
/// see `README.md`, "Isolation" — not a value this call derives or accepts;
/// every job row, quarantine scan, uniqueness domain, and retention sweep
/// this `Database` performs is simply whatever `grind_jobs` and its sibling
/// tables in that one schema hold. Also starts this `Database`'s own
/// `sinal/forwarder.Forwarder`
/// — see `grind/observation` for the events it carries — nested under its
/// own dedicated supervisor, added to the root as a `Temporary` child.
/// `observation_capacity` was already checked positive by `validate`, so
/// `forwarder.new` cannot fail here.
///
/// The nesting matters: a handler attached to an observation event that
/// itself exits or is killed (rather than merely raising, which native
/// `:telemetry` does isolate) can take the forwarder process down — see
/// `sinal/forwarder`'s own module documentation. If the forwarder were a
/// plain sibling of the PostgreSQL pool under one shared supervisor,
/// repeatedly crashing it would exhaust *that* supervisor's own restart
/// intensity (2 restarts / 5 seconds by default) and terminate every child,
/// including the pool. Nesting the forwarder under its own supervisor and
/// marking that nested supervisor `Temporary` under the root means: on
/// ordinary crashes the nested supervisor keeps restarting the forwarder
/// exactly as before; if the forwarder crashes so persistently that the
/// *nested* supervisor exhausts its own restart intensity and terminates
/// itself, the root supervisor — per `Temporary`'s contract — never
/// restarts it and never counts that termination against its own restart
/// budget, so the pool is never affected. In that degraded state, further
/// observations are simply unavailable: `forwarder.emit` reports
/// `ForwarderUnavailable`, which `grind/postgres` already ignores (an
/// observation is a diagnostic side channel, never a policy decision) —
/// jobs keep being admitted, claimed, and acknowledged normally. See
/// `postgres_forwarder_crash_loop_does_not_stop_the_pool_test`
/// (`test/grind/observations/delivery_test.gleam`) and
/// `docs/RECOVERY-EVIDENCE.md`, "Acknowledged
/// observation".
pub fn start(settings: ValidatedSettings) -> Result(Database, StartError) {
  let ValidatedSettings(
    config,
    unique_lock_wait_ms:,
    observation_capacity:,
    statement_deadline_ms:,
    migration_deadline_ms:,
    schema:,
  ) = settings
  let pog.Config(pool_name:, ..) = config
  let forwarder_name = process.new_name("grind_postgres_observation_forwarder")
  let assert Ok(fwd) = forwarder.new(forwarder_name, observation_capacity)
  let forwarder_supervisor =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(forwarder.supervised(fwd))
    |> static_supervisor.supervised()
    |> supervision.restart(supervision.Temporary)
  let supervisor =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pog.supervised(config))
    |> static_supervisor.add(forwarder_supervisor)
  let connection = pog.named_connection(pool_name)
  // Attached before the pool itself starts accepting checkouts, so no
  // in-flight checkout against this pool's name can ever run before a
  // deadline is attached and silently fall back to the FFI's own hardcoded
  // default (5000ms).
  store.set_deadline(connection, statement_deadline_ms)
  case static_supervisor.start(supervisor) {
    Ok(started) -> {
      process.unlink(started.pid)
      case read_database_oid(connection) {
        Error(error) -> Error(InstallationQueryFailed(error))
        Ok(database_oid) ->
          Ok(Database(
            connection,
            started.pid,
            job.new_installation(
              database_oid,
              schema,
              read_cluster_identifier(connection),
            ),
            unique_lock_wait_ms,
            statement_deadline_ms,
            migration_deadline_ms,
            fwd,
          ))
      }
    }
    Error(error) -> Error(PoolStartFailed(error))
  }
}

/// This physical database's own OID (`pg_database.oid`), used only to build
/// this `Database`'s in-memory `Installation` token — see `grind/job`'s doc
/// comment for what it is for. Cheap (a single scalar read against a tiny,
/// always-cached system catalog) and run exactly once, right after the pool
/// starts.
fn read_database_oid(
  connection: pog.Connection,
) -> Result(Int, pog.QueryError) {
  let query =
    pog.query(
      "SELECT oid::int4 FROM pg_database WHERE datname = current_database()",
    )
    |> pog.returning({
      use oid <- decode.field(0, decode.int)
      decode.success(oid)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(error)
    Ok(returned) ->
      case returned.rows {
        [oid] -> Ok(oid)
        _ ->
          Error(pog.PostgresqlError(
            "",
            "",
            "current_database() did not resolve to exactly one pg_database row",
          ))
      }
  }
}

/// This PostgreSQL *cluster*'s own identity (`pg_control_system()`'s
/// `system_identifier`), used only to disambiguate two different clusters
/// that happen to collide on database OID and configured schema — see
/// `job.Installation`'s own doc comment. Best-effort only: on some
/// deployments (managed/hardened PostgreSQL commonly revokes `EXECUTE` on
/// `pg_control_system()` from `PUBLIC`, though stock, unmodified PostgreSQL
/// does not restrict it at all by default) an ordinary connecting role
/// cannot call it; any failure (a permission error, an unexpected row
/// shape) is swallowed into `None` here rather than ever failing
/// `postgres.start` — this read is a client-side improvement, not
/// something `start` itself depends on.
///
/// Checked with `has_function_privilege` *first*, as its own separate
/// query, before ever sending the actual `pg_control_system()` call —
/// deliberately not a single query with the call nested inside a `CASE
/// WHEN has_function_privilege(...) THEN (...) END` guard, which looks
/// like it should short-circuit but does not: PostgreSQL performs a
/// function's own ACL check at expression-initialization time, for every
/// function call node the query plan contains, regardless of which
/// `CASE` branch is actually reached at runtime — a role denied `EXECUTE`
/// still gets the real `ERROR:  permission denied for function
/// pg_control_system`, and PostgreSQL still logs it server-side, even
/// though the branch calling it would never have been taken (confirmed
/// empirically against a role with the privilege explicitly revoked; see
/// `docs/RECOVERY-EVIDENCE.md`). Never sending the restricted call's own
/// query text at all, once the separate privilege check reports `False`,
/// is what actually avoids both the client-visible error (already
/// swallowed into `None` either way) and the server-side log line — the
/// genuine defect this function's own two-query shape exists to close on
/// an ordinary least-privilege installation.
fn read_cluster_identifier(connection: pog.Connection) -> Option(Int) {
  case has_pg_control_system_privilege(connection) {
    False -> None
    True -> {
      let query =
        pog.query("SELECT system_identifier FROM pg_control_system()")
        |> pog.returning({
          use system_identifier <- decode.field(0, decode.int)
          decode.success(system_identifier)
        })
      case store.execute_safely(query, on: connection) {
        Error(_) -> None
        Ok(returned) ->
          case returned.rows {
            [system_identifier] -> Some(system_identifier)
            _ -> None
          }
      }
    }
  }
}

/// Whether this connection's own role currently has `EXECUTE` on
/// `pg_control_system()` — `has_function_privilege` is a plain,
/// always-callable introspection function (no privilege of its own is
/// needed to ask this question), so this never itself raises or logs an
/// error the way actually calling the restricted function would for a
/// role that lacks it. Any failure here (an unexpected row shape, a lost
/// reply) is conservatively `False`, since the only consequence is
/// `read_cluster_identifier` skipping a best-effort read it would
/// otherwise have attempted.
fn has_pg_control_system_privilege(connection: pog.Connection) -> Bool {
  let query =
    pog.query("SELECT has_function_privilege('pg_control_system()', 'execute')")
    |> pog.returning({
      use allowed <- decode.field(0, decode.bool)
      decode.success(allowed)
    })
  case store.execute_safely(query, on: connection) {
    Error(_) -> False
    Ok(returned) ->
      case returned.rows {
        [allowed] -> allowed
        _ -> False
      }
  }
}

pub type CloseError {
  /// The pool's supervisor did not confirm its own shutdown within the FFI's
  /// own bounded wait (6000ms, `src/grind_postgres_ffi.erl`). The pool
  /// process may still be terminating; this `Database` value's own deadline
  /// entry is left in place rather than risking erasing a name a fresh
  /// `start` may since have reused (see `close`'s own doc comment).
  StopTimedOut
}

/// Stops the pool process owned by this `Database` value. `Ok(True)` means
/// this call actually stopped a still-live process; `Ok(False)` means it was
/// already gone. `close` only erases the pool's own deadline entry in the
/// first case — see `close`'s own doc comment for why.
@external(erlang, "grind_postgres_ffi", "stop_supervisor")
fn stop_supervisor(pid: process.Pid) -> Result(Bool, Nil)

/// Stops this `Database`'s own PostgreSQL pool and observation forwarder,
/// and erases the checkout deadline `start` attached to the pool's name —
/// but only when this call itself is the one that actually stopped a live
/// process. `ValidatedSettings` reuses the same pool name across every
/// `start`/`close` cycle (see `validate`), so calling `close` on a stale
/// `Database` handle whose supervisor has already stopped (kept around
/// after an intervening `start` reopened the same `ValidatedSettings` under
/// a fresh supervisor) is a no-op here rather than erasing the *live*
/// pool's deadline entry out from under it. Before this, that erasure ran
/// unconditionally: a stray double-`close` on an old handle silently
/// downgraded a still-running pool's every subsequent storage call to the
/// FFI's own hardcoded 5000ms default until that pool was itself closed.
/// Returns `Error(StopTimedOut)` if the supervisor does not confirm its own
/// shutdown in time; the deadline is left in place in that case too, since
/// the pool may still be alive.
pub fn close(database: Database) -> Result(Nil, CloseError) {
  let Database(supervisor_pid:, connection:, ..) = database
  case stop_supervisor(supervisor_pid) {
    Error(Nil) -> Error(StopTimedOut)
    Ok(True) -> {
      store.clear_deadline(connection)
      Ok(Nil)
    }
    Ok(False) -> Ok(Nil)
  }
}

pub type StorageError {
  /// A storage call made while reading the schema's current generation,
  /// while acquiring the migration advisory lock, pinning `READ COMMITTED`,
  /// or setting this transaction's own constant `lock_timeout` — never one
  /// of a migration step's own DDL/DML statements, see `MigrationStepFailed`
  /// and `MigrationLockUnavailable` for those — failed or its reply was
  /// lost.
  MigrationQueryFailed(pog.QueryError)
  IncompatibleSchema
  UnsupportedSchemaVersion(Int)
  /// `migrate`/`migrate_with`'s own leading `CREATE SCHEMA IF NOT EXISTS`
  /// (see `ensure_schema_exists`) failed or its reply was lost — typically a
  /// role without `CREATE` privilege on the database, for a schema that does
  /// not yet exist. Never returned once the schema exists (the statement is
  /// then a no-op success, run again every call). Safe to retry once the
  /// underlying cause (usually a privilege grant) is fixed.
  SchemaCreationFailed(pog.QueryError)
  /// One migration step's own statements failed and that step's transaction
  /// was rolled back cleanly — every earlier step already committed its own
  /// transaction and stays applied; this step and every later one did not
  /// run. Safe to fix the underlying cause and re-run `migrate`.
  MigrationStepFailed(Int, pog.QueryError)
  /// A migration step's own DDL/DML hit PostgreSQL's `55P03
  /// lock_not_available` after this transaction's own constant
  /// `lock_timeout` (`migration_lock_timeout_ms`, 2000ms) elapsed waiting on
  /// a conflicting lock — typically an `ALTER`/`CREATE INDEX` against a
  /// large, actively used table under concurrent access. This step's own
  /// transaction rolled back cleanly, exactly like `MigrationStepFailed`;
  /// safe to retry `migrate` once the conflicting lock clears.
  MigrationLockUnavailable(Int)
  /// A migration step may or may not have committed. Covers three distinct
  /// shapes, all safe to resolve the same way: a checkout failure before
  /// `BEGIN` ever ran (definitely not committed, reported here anyway since
  /// retrying is equally safe either way); `BEGIN` itself failing or losing
  /// its reply; and a failed `ROLLBACK` after a statement error (the
  /// deadline force-closing the connection, so the rollback's own outcome
  /// is unknown too). Re-running `migrate` is always safe regardless of
  /// which shape occurred: a step that did commit is detected by its own
  /// re-read of the schema generation and skipped; one that did not is
  /// simply retried.
  MigrationCommitUnknown(Int)
}

pub type Resolution(output, error) {
  ConfirmSuccess(output)
  ConfirmBusinessFailure(error)
  AuthorizeReplay
}

pub type ResolutionResult {
  ResolutionApplied(State)
  ResolutionAlreadyApplied(State)
}

pub type ResolutionError {
  EmptyResolutionId
  EmptyResolver
  EmptyResolutionDetails
  ReconciliationQueryFailed(pog.QueryError)
  ReconciliationNotRequired
  ResolutionCommandConflict
  ResolutionRouteMismatch
  ResolutionWorkerContractMismatch
  ResolutionCodecMismatch
  ResolutionRequiresErrorCodec
  ResolutionCancellationPending
  ResolutionAttemptMetadataMissing
  ResolutionWriteRejected
  ResolutionCommitUnknown(resolution_id: String)
  /// See `JobReadError`'s `HandleFromAnotherInstallation` — the same
  /// client-side check, for `resolve_uncertain`'s own handle.
  ResolutionFromAnotherInstallation
}

/// One audited operator decision: `resolution_id` identifies this exact
/// decision for idempotent replay after a lost reply (see
/// `ResolutionCommitUnknown`); `resolved_by` and `details` are attributed,
/// non-empty audit fields; `decision` is the outcome itself.
pub type ResolutionRequest(output, error) {
  ResolutionRequest(
    resolution_id: String,
    resolved_by: String,
    details: String,
    decision: Resolution(output, error),
  )
}

/// Applies an audited operator decision to an uncertain job.
pub fn resolve_uncertain(
  database: Database,
  handle: JobHandle(input, output, error),
  request: ResolutionRequest(output, error),
) -> Result(ResolutionResult, ResolutionError) {
  let ResolutionRequest(resolution_id:, resolved_by:, details:, decision:) =
    request
  let #(_, handle_installation, _, _, _, _, _) =
    job.reconciliation_fields(handle)
  let Database(installation: database_installation, ..) = database
  case resolution_id, resolved_by, details {
    "", _, _ -> Error(EmptyResolutionId)
    _, "", _ -> Error(EmptyResolver)
    _, _, "" -> Error(EmptyResolutionDetails)
    _, _, _ ->
      case job.same_installation(handle_installation, database_installation) {
        False -> Error(ResolutionFromAnotherInstallation)
        True -> resolve_uncertain_checked(database, handle, request, decision)
      }
  }
}

fn resolve_uncertain_checked(
  database: Database,
  handle: JobHandle(input, output, error),
  request: ResolutionRequest(output, error),
  decision: Resolution(output, error),
) -> Result(ResolutionResult, ResolutionError) {
  let ResolutionRequest(resolution_id:, resolved_by:, details:, ..) = request
  let Database(connection:, forwarder:, ..) = database
  let decision = case decision {
    ConfirmSuccess(value) -> postgres_resolution.ConfirmSuccess(value)
    ConfirmBusinessFailure(value) ->
      postgres_resolution.ConfirmBusinessFailure(value)
    AuthorizeReplay -> postgres_resolution.AuthorizeReplay
  }
  postgres_resolution.resolve_uncertain(
    connection,
    forwarder,
    handle,
    postgres_resolution.Request(
      resolution_id:,
      resolved_by:,
      details:,
      decision:,
    ),
  )
  |> result.map(fn(result) {
    case result {
      postgres_resolution.ResolutionApplied(state) -> ResolutionApplied(state)
      postgres_resolution.ResolutionAlreadyApplied(state) ->
        ResolutionAlreadyApplied(state)
    }
  })
  |> result.map_error(fn(error) {
    case error {
      postgres_resolution.ReconciliationQueryFailed(reason) ->
        ReconciliationQueryFailed(reason)
      postgres_resolution.ReconciliationNotRequired -> ReconciliationNotRequired
      postgres_resolution.ResolutionCommandConflict -> ResolutionCommandConflict
      postgres_resolution.ResolutionRouteMismatch -> ResolutionRouteMismatch
      postgres_resolution.ResolutionWorkerContractMismatch ->
        ResolutionWorkerContractMismatch
      postgres_resolution.ResolutionCodecMismatch -> ResolutionCodecMismatch
      postgres_resolution.ResolutionRequiresErrorCodec ->
        ResolutionRequiresErrorCodec
      postgres_resolution.ResolutionCancellationPending ->
        ResolutionCancellationPending
      postgres_resolution.ResolutionAttemptMetadataMissing ->
        ResolutionAttemptMetadataMissing
      postgres_resolution.ResolutionWriteRejected -> ResolutionWriteRejected
      postgres_resolution.ResolutionCommitUnknown(resolution_id) ->
        ResolutionCommitUnknown(resolution_id)
    }
  })
}

/// Applies every not-yet-applied step of `grind/internal/migrations.migrations()`
/// in ascending version order. Repeated calls are safe; older experimental
/// schema versions and any tampered or foreign schema fail closed. See
/// `migrate_with` for the mechanism.
pub fn migrate(database: Database) -> Result(Nil, StorageError) {
  migrate_with(database, migrations.migrations())
}

/// Grind's migration runner, generalised over an explicit step list so the
/// test suite can exercise it against synthetic steps appended after the
/// real ones. `migrate` is exactly `migrate_with(database, migrations())`.
///
/// `steps` must be exactly the contiguous range
/// `{baseline_schema_version..latest}` with no gaps or duplicate versions —
/// this is `migrate_with`'s own precondition on its caller (always satisfied
/// by `migrations()` itself, proven by `grind_migrations_conformance_test`;
/// the test suite's own synthetic step lists must satisfy it too), checked
/// with `let assert` rather than a typed error: a malformed step list is a
/// programming error in the caller, never a runtime schema condition.
///
/// Runs each step in its own transaction, in ascending version order, under
/// `Settings.migration_deadline_ms`:
///
/// 1. Pin this transaction to `READ COMMITTED` as its own literal first
///    statement (defence in depth, exactly like the uniqueness admission
///    transaction — see `grind/internal/unique_admission`'s
///    `pin_read_committed`), then acquire a transaction-scoped PostgreSQL
///    advisory lock (`pg_advisory_xact_lock`, keyed by a fixed Grind string
///    and the current schema) so two concurrent `migrate`/`migrate_with`
///    callers — in this process or another — serialise instead of racing
///    the same step. The identical lock statement is also this step's own
///    first statement in `migrations()`/`priv/migrations/*.sql`, so an
///    application applying migrations directly through cigogne serialises
///    against a concurrent `postgres.migrate` caller too; re-acquiring the
///    same transaction-scoped lock a second time in the same transaction is
///    a no-op, never a self-deadlock.
/// 2. Re-read the schema's current generation (see `read_schema_generation`)
///    now that the lock is held, and skip this step if it is already
///    applied — necessary both for plain idempotent re-runs and for a
///    concurrent migrator that was waiting on the lock behind this one.
/// 3. Otherwise run this step's own statements, in order, including its
///    trailing `grind_schema_migrations` marker insert.
/// 4. Re-read the schema's generation once more, in the same transaction,
///    and require it now reports exactly `AtVersion(this step's version)`
///    before committing — catches a step whose statements ran without error
///    but produced the wrong marker or the wrong physical shape (a missing
///    object, a missing key column) that a later step or caller would
///    otherwise silently trust.
///
/// `@internal`: exposed only for the test suite (concurrent migrators,
/// injected partial failures, and the upgrade harness), never part of the
/// public API.
@internal
pub fn migrate_with(
  database: Database,
  steps: List(migrations.Migration),
) -> Result(Nil, StorageError) {
  let Database(connection:, installation:, migration_deadline_ms:, ..) =
    database
  let schema = job.installation_schema(installation)
  postgres_migration.run(
    connection,
    schema,
    quote_ident(schema),
    migration_deadline_ms,
    migration_lock_timeout_ms,
    steps,
  )
  |> result.map_error(migration_error)
}

fn migration_error(error: postgres_migration.RunnerError) -> StorageError {
  case error {
    postgres_migration.MigrationQueryFailed(error) ->
      MigrationQueryFailed(error)
    postgres_migration.IncompatibleSchema -> IncompatibleSchema
    postgres_migration.UnsupportedSchemaVersion(version) ->
      UnsupportedSchemaVersion(version)
    postgres_migration.SchemaCreationFailed(error) ->
      SchemaCreationFailed(error)
    postgres_migration.MigrationStepFailed(version, error) ->
      MigrationStepFailed(version, error)
    postgres_migration.MigrationLockUnavailable(version) ->
      MigrationLockUnavailable(version)
    postgres_migration.MigrationCommitUnknown(version) ->
      MigrationCommitUnknown(version)
  }
}

/// Persists an immediate job without invoking its worker. May return
/// `submission.EmptyQueueName` (an empty `queue`) or
/// `submission.CommitUnknownWithoutId` (the insert's own query failed, its
/// reply was lost, or its checkout failed outright — see that variant's doc
/// comment); never `submission.AdmissionContended`, `submission.SubmissionConflict`,
/// `submission.NotCommitted`, or `submission.CommitUnknown`, which only ever come
/// from `submit_unique`/`submit_with_id`'s admission transaction.
pub fn submit(
  database: Database,
  queue: String,
  worker: Worker(input, output, error),
  input: input,
) -> Result(
  JobHandle(input, output, error),
  submission.SubmitError(input, output, error),
) {
  submit_with_availability(database, queue, worker, input, None)
}

/// Persists a job at an absolute Unix-millisecond availability time. May
/// return `submission.EmptyQueueName` or `submission.CommitUnknownWithoutId`; never
/// `submission.AdmissionContended`, `submission.SubmissionConflict`,
/// `submission.NotCommitted`, or `submission.CommitUnknown` — the same possible and
/// impossible variants as `submit`.
pub fn submit_at(
  database: Database,
  queue: String,
  worker: Worker(input, output, error),
  input: input,
  available_at: job.AvailableAt,
) -> Result(
  JobHandle(input, output, error),
  submission.SubmitError(input, output, error),
) {
  submit_with_availability(
    database,
    queue,
    worker,
    input,
    Some(job.available_at_unix_milliseconds(available_at)),
  )
}

fn submit_with_availability(
  database: Database,
  queue: String,
  worker: Worker(input, output, error),
  input: input,
  available_at_unix_ms: Option(Int),
) -> Result(
  JobHandle(input, output, error),
  submission.SubmitError(input, output, error),
) {
  case queue {
    "" -> Error(submission.EmptyQueueName)
    _ -> {
      let Database(connection:, forwarder:, installation:, ..) = database
      let worker.Metadata(
        id: worker_id,
        worker_version:,
        input_version:,
        output_version:,
        error_version:,
        max_attempts:,
      ) = worker.metadata(worker)
      let error_parameter = case error_version {
        Some(version) -> pog.text(version)
        None -> pog.null()
      }
      let availability_parameter = case available_at_unix_ms {
        Some(availability) -> pog.int(availability)
        None -> pog.null()
      }
      let sql =
        "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, error_version, max_attempts, state, available_at) "
        <> "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, "
        <> "CASE WHEN $9::bigint IS NULL OR $9::bigint <= (extract(epoch FROM clock_timestamp()) * 1000)::bigint THEN 'queued' ELSE 'scheduled' END, "
        <> "CASE WHEN $9::bigint IS NULL THEN clock_timestamp() ELSE to_timestamp($9::double precision / 1000.0) END) RETURNING id, state, (extract(epoch FROM available_at) * 1000)::bigint"
      let query =
        pog.query(sql)
        |> pog.parameter(pog.text(queue))
        |> pog.parameter(pog.text(worker_id))
        |> pog.parameter(pog.text(worker_version))
        |> pog.parameter(pog.text(input_version))
        |> pog.parameter(pog.text(worker.encode_input(worker, input)))
        |> pog.parameter(pog.text(output_version))
        |> pog.parameter(error_parameter)
        |> pog.parameter(pog.int(max_attempts))
        |> pog.parameter(availability_parameter)
        |> pog.returning({
          use id <- decode.field(0, decode.int)
          use state <- decode.field(1, decode.string)
          use available_at_ms <- decode.field(2, decode.int)
          decode.success(#(id, state, available_at_ms))
        })
      case store.execute_safely(query, on: connection) {
        Error(error) -> Error(submission.CommitUnknownWithoutId(error))
        Ok(returned) ->
          case returned.rows {
            [#(id, state, available_at_ms)] -> {
              case job.state_of_stored(state) {
                Error(Nil) -> Nil
                Ok(committed_state) ->
                  emit_admitted(
                    forwarder,
                    queue,
                    id,
                    worker_id,
                    worker_version,
                    committed_state,
                    Some(available_at_ms),
                    None,
                    observation.Replied,
                  )
              }
              Ok(job.new_handle(id, installation, queue, worker))
            }
            _ ->
              panic as "submit: unconditional single-row INSERT ... RETURNING returned a row count other than one"
          }
      }
    }
  }
}

/// Builds and forwards `[grind, job, admitted]`, shared by a plain
/// `submit`/`submit_at` admission and every `submit_unique` decision.
/// `submission_id`/`confirmation` are `None`/always `Replied` for a plain
/// submission (there is no receipt concept there — every plain submission is
/// its own fresh commit); see `AdmittedMetadata` for the unique case.
fn emit_admitted(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  committed_state: State,
  available_at_unix_ms: Option(Int),
  submission_id: Option(String),
  confirmation: observation.Confirmation,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.admitted(),
      observation.AdmittedMeasurements(count: 1),
      observation.AdmittedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        committed_state:,
        available_at_unix_ms:,
        submission_id:,
        confirmation:,
      ),
    )
  Nil
}

/// The shared read-path error space for `bind_handle`, `arguments`, `state`,
/// `outcome`, and `reconcile_acknowledgement` — every function that reads an
/// already-admitted job by its durable identity. Not every variant is
/// reachable from every function; each function's own doc comment says
/// which.
pub type JobReadError {
  JobReadQueryFailed(pog.QueryError)
  /// No row matches this id.
  JobNotFound
  /// The row's actual queue does not match this handle's own recorded
  /// queue (a handle constructed, or read back, against the wrong queue).
  QueueRouteMismatch(expected: String, actual: String)
  WorkerContractMismatch(
    expected_id: String,
    expected_version: String,
    actual_id: String,
    actual_version: String,
  )
  /// `bind_handle` only: the stored input/output/error codec version for
  /// `kind` does not match the worker definition's own (the first of the
  /// three that disagrees, checked input/output/error in that order). No
  /// decode is attempted — `bind_handle` never reads a codec'd value, only
  /// checks that a future read through the returned handle would be
  /// compatible.
  CodecContractMismatch(
    kind: worker.CodecKind,
    expected: String,
    actual: String,
  )
  /// A codec'd value's own version matched, but its stored JSON failed to
  /// decode under it — or its version did not match after all (see
  /// `worker.StoredCodecError`).
  CodecFailed(worker.StoredCodecError)
  /// The stored job state, or another stored enum-shaped column (a
  /// `business_failed` row's `failure_cause`), is not one of the values this
  /// code recognizes — unreachable without direct tampering, since these
  /// columns carry a `CHECK` constraint against the same closed vocabulary.
  /// The payload is always the raw stored value that failed to parse.
  InvalidStoredState(String)
  /// `outcome` only: a `succeeded` row has no stored output at all —
  /// unreachable without direct tampering, since every `succeeded` write
  /// site also writes the output in the same statement.
  SucceededOutputMissing
  /// `reconcile_acknowledgement` only: no acknowledgement receipt exists
  /// under this command ID — the acknowledgement it names never committed
  /// (or has since been pruned). Distinct from `JobNotFound`: a missing
  /// receipt says nothing about whether the job row itself exists.
  ReceiptNotFound
  /// `reconcile_acknowledgement` only: a receipt exists under this command
  /// ID, but its own recorded job ID does not match this handle's job ID —
  /// the command ID was read back against the wrong handle.
  ReceiptJobMismatch(expected: Int, actual: Int)
  /// This handle was minted against a different `Database` (a different
  /// physical database, or the same database under a different configured
  /// schema — see `with_schema`) than the one it was just used against.
  /// Checked before any storage call is made, purely from the two in-memory
  /// installation tokens — see `grind/job`'s `Installation` type doc comment
  /// for what this client-side check does and, more importantly, does not
  /// guarantee (the real isolation boundary is the PostgreSQL schema itself,
  /// see `README.md`, "Isolation"). Reachable from every `JobReadError`-
  /// returning function except `bind_handle`, which mints a fresh handle
  /// against the `Database` it is called on rather than checking one handed
  /// in.
  HandleFromAnotherInstallation
}

/// Exhaustive conversion keeps the public error constructors owned here.
fn job_read_error(error: postgres_job_reads.JobReadError) -> JobReadError {
  case error {
    postgres_job_reads.JobReadQueryFailed(reason) -> JobReadQueryFailed(reason)
    postgres_job_reads.JobNotFound -> JobNotFound
    postgres_job_reads.QueueRouteMismatch(expected:, actual:) ->
      QueueRouteMismatch(expected:, actual:)
    postgres_job_reads.WorkerContractMismatch(
      expected_id:,
      expected_version:,
      actual_id:,
      actual_version:,
    ) ->
      WorkerContractMismatch(
        expected_id:,
        expected_version:,
        actual_id:,
        actual_version:,
      )
    postgres_job_reads.CodecContractMismatch(kind:, expected:, actual:) ->
      CodecContractMismatch(kind:, expected:, actual:)
    postgres_job_reads.CodecFailed(reason) -> CodecFailed(reason)
    postgres_job_reads.InvalidStoredState(state) -> InvalidStoredState(state)
    postgres_job_reads.SucceededOutputMissing -> SucceededOutputMissing
    postgres_job_reads.ReceiptNotFound -> ReceiptNotFound
    postgres_job_reads.ReceiptJobMismatch(expected:, actual:) ->
      ReceiptJobMismatch(expected:, actual:)
    postgres_job_reads.HandleFromAnotherInstallation ->
      HandleFromAnotherInstallation
  }
}

/// Reconstructs a typed handle from durable identity after an application
/// restart, scoped to whatever schema this `Database`'s pool connects to —
/// see `README.md`, "Isolation". The current worker definition must exactly
/// match the stored worker and codec contract. May return
/// `JobReadQueryFailed`, `JobNotFound`, `WorkerContractMismatch`, or
/// `CodecContractMismatch`; never `QueueRouteMismatch`, `CodecFailed`,
/// `InvalidStoredState`, `SucceededOutputMissing`, `ReceiptNotFound`, or
/// `ReceiptJobMismatch`, which only ever come from the other four
/// `JobReadError`-returning functions.
///
/// **Always call this against the same installation that originally minted
/// the id being rebound.** `id` here is a bare `Int` (see
/// `job.id_value`) — an application's own durable storage for it (a row in
/// its own database, a message queue payload, ...) carries no
/// `job.Installation` of its own to check against, unlike a live
/// `JobHandle`/`PendingSubmission` value passed directly between calls in
/// one running process. `bind_handle` therefore *mints* a fresh handle
/// stamped with whichever `Database` it is called on (see
/// `HandleFromAnotherInstallation`'s own doc comment) rather than checking
/// one handed in — if the same numeric id also happens to name a row in a
/// *different* schema than the one the id was originally minted against,
/// calling `bind_handle` against that wrong `Database` succeeds silently,
/// binding to the wrong row, rather than failing the way passing a live
/// `JobHandle` to the wrong `Database` would. Persisting which installation
/// (which `Settings.schema`, and which database URL/cluster) an id came
/// from, alongside the id itself, is an application-level responsibility
/// this function cannot enforce for it.
pub fn bind_handle(
  database: Database,
  worker: Worker(input, output, error),
  id: Int,
) -> Result(JobHandle(input, output, error), JobReadError) {
  let Database(connection:, installation:, ..) = database
  postgres_job_reads.bind_handle(connection, installation, worker, id)
  |> result.map_error(job_read_error)
}

/// Reloads the typed, version-checked input from PostgreSQL. May return
/// `JobReadQueryFailed`, `JobNotFound`, `QueueRouteMismatch`,
/// `WorkerContractMismatch`, or `CodecFailed`; never `CodecContractMismatch`
/// (`bind_handle`'s own pre-decode check), `InvalidStoredState`,
/// `SucceededOutputMissing`, `ReceiptNotFound`, or `ReceiptJobMismatch` —
/// `arguments` never parses the stored job state or a receipt.
pub fn arguments(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(input, JobReadError) {
  let Database(connection:, installation:, ..) = database
  postgres_job_reads.arguments(connection, installation, handle)
  |> result.map_error(job_read_error)
}

pub type CancellationResult {
  CancelledBeforeRun
  CancellationRequested
  AlreadyCancelled
  AlreadyUncertain
  AlreadyFinished(job.State)
}

pub type CancellationError {
  CancellationQueryFailed(pog.QueryError)
  CancellationCommitUnknown
  CancellationWriteRejected
  CancellationQueueMismatch
  CancellationWorkerContractMismatch
  CancellationJobNotFound
  CancellationInvalidStoredState(String)
  /// See `JobReadError`'s `HandleFromAnotherInstallation` — the same
  /// client-side check, for `cancel`'s own handle.
  CancellationFromAnotherInstallation
}

/// Request cancellation without conflating a running worker's proposal with
/// the state committed by its later acknowledgement.
pub fn cancel(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(CancellationResult, CancellationError) {
  let Database(connection:, forwarder:, installation: database_installation, ..) =
    database
  let #(id, handle_installation, queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  case job.same_installation(handle_installation, database_installation) {
    False -> Error(CancellationFromAnotherInstallation)
    True ->
      case
        store.transaction_safely(connection, fn(transaction) {
          cancel_transaction(transaction, id, queue, worker_id, worker_version)
        })
      {
        Ok(#(result, previous_state)) -> {
          case
            cancellation_outcome_of(result),
            job.state_of_stored(previous_state)
          {
            Some(outcome), Ok(previous_state) ->
              emit_cancellation(
                forwarder,
                queue,
                id,
                worker_id,
                worker_version,
                previous_state,
                outcome,
              )
            _, _ -> Nil
          }
          Ok(result)
        }
        Error(pog.TransactionQueryError(_)) -> Error(CancellationCommitUnknown)
        Error(pog.TransactionRolledBack(error)) -> Error(error)
      }
  }
}

/// Builds and forwards `[grind, job, cancellation_decided]` from a proven-committed
/// `CancellationResult`. Called only from `cancel`, strictly after
/// `transaction_safely` has already returned — never from inside a
/// transaction callback (the same discipline `acknowledge` and
/// `resolve_uncertain` follow). Only `CancelledBeforeRun` and
/// `CancellationRequested` are genuine writes; every read-only outcome emits
/// nothing.
/// Which `[grind, job, cancellation_decided]` outcome, if any, a `CancellationResult`
/// reports. Only `CancelledBeforeRun`/`CancellationRequested` are genuine
/// writes; every read-only outcome (`AlreadyCancelled`, `AlreadyUncertain`,
/// `AlreadyFinished`) maps to `None` and must never emit.
fn cancellation_outcome_of(
  result: CancellationResult,
) -> Option(observation.CancellationOutcome) {
  case result {
    CancelledBeforeRun -> Some(observation.CancellationDecidedBeforeRun)
    CancellationRequested -> Some(observation.CancellationDecidedWhileRunning)
    AlreadyCancelled | AlreadyUncertain | AlreadyFinished(_) -> None
  }
}

fn emit_cancellation(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  previous_state: State,
  outcome: observation.CancellationOutcome,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.cancellation_decided(),
      observation.CancellationMeasurements(count: 1),
      observation.CancellationMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        previous_state:,
        outcome:,
      ),
    )
  Nil
}

fn cancel_transaction(
  connection: pog.Connection,
  id: Int,
  queue: String,
  worker_id: String,
  worker_version: String,
) -> Result(#(CancellationResult, String), CancellationError) {
  use returned <- result.try(
    case
      store.call_safely(connection, fn(connection) {
        sql.cancel_lock(connection, id)
      })
    {
      Error(error) -> Error(CancellationQueryFailed(error))
      Ok(returned) -> Ok(returned)
    },
  )
  case returned.rows {
    [] -> Error(CancellationJobNotFound)
    [
      sql.CancelLockRow(
        queue: stored_queue,
        worker_id: stored_worker,
        worker_version: stored_version,
        state:,
      ),
    ] ->
      case stored_queue == queue {
        False -> Error(CancellationQueueMismatch)
        True ->
          case stored_worker == worker_id && stored_version == worker_version {
            False -> Error(CancellationWorkerContractMismatch)
            True -> {
              use result <- result.try(cancel_locked_state(
                connection,
                id,
                state,
              ))
              Ok(#(result, state))
            }
          }
      }
    _ -> Error(CancellationJobNotFound)
  }
}

fn cancel_locked_state(
  connection: pog.Connection,
  id: Int,
  state: String,
) -> Result(CancellationResult, CancellationError) {
  case state {
    "queued" | "scheduled" | "retryable" -> {
      use returned <- result.try(
        case
          store.call_safely(connection, fn(connection) {
            sql.cancel_before_run(connection, id)
          })
        {
          Error(error) -> Error(CancellationQueryFailed(error))
          Ok(returned) -> Ok(returned)
        },
      )
      case returned.rows {
        [_] -> Ok(CancelledBeforeRun)
        _ -> Error(CancellationWriteRejected)
      }
    }
    "executing" -> {
      use returned <- result.try(
        case
          store.call_safely(connection, fn(connection) {
            sql.cancel_executing(connection, id)
          })
        {
          Error(error) -> Error(CancellationQueryFailed(error))
          Ok(returned) -> Ok(returned)
        },
      )
      case returned.rows {
        [_] -> Ok(CancellationRequested)
        _ -> Error(CancellationWriteRejected)
      }
    }
    "cancelled" -> Ok(AlreadyCancelled)
    "succeeded" -> Ok(AlreadyFinished(job.Succeeded))
    "business_failed" -> Ok(AlreadyFinished(job.BusinessFailed))
    "runtime_failed" -> Ok(AlreadyFinished(job.RuntimeFailed))
    "contract_mismatch" -> Ok(AlreadyFinished(job.ContractMismatch))
    "uncertain" -> Ok(AlreadyUncertain)
    "discarded" -> Ok(AlreadyFinished(job.Discarded))
    other -> Error(CancellationInvalidStoredState(other))
  }
}

/// Failures returned while running one claimed job. `QueueCodecMismatch` is
/// returned after the row is durably marked `ContractMismatch`; it is an error
/// to the batch caller but a committed job disposition.
pub type QueueRunError {
  QueueClaimFailed(pog.QueryError)
  QueueClaimReleaseFailed(pog.QueryError)
  QueueClaimReleaseRejected
  /// A query inside the acknowledgement transaction itself failed, so the
  /// callback returned `Error` and `COMMIT` was never sent — pog reports
  /// this as a controlled `TransactionRolledBack`, never routed through the
  /// `QueueAckUnknown` reconciliation path. Genuinely did not commit, the
  /// same "callback failed, therefore no commit was ever attempted"
  /// reasoning `submission.NotCommitted`'s doc comment gives for the
  /// uniqueness-admission path. Safe to retry the same acknowledgement.
  QueueAckFailed(pog.QueryError)
  /// The ACK transaction may have committed although its reply was lost.
  /// Reconcile this stable command ID before considering any replay.
  QueueAckUnknown(command_id: String, proposed: worker.Execution)
  /// The exact worker proposal was not committed because the claim no longer
  /// owned a live, executing row. The attached reason is a database snapshot.
  QueueAckStale(proposed: worker.Execution, reason: AckRejection)
  /// A fenced query returned a row count this module does not treat as
  /// ordinary contention: an unregistered worker at claim time (candidate
  /// selection should have excluded it) or more than one row matched by a
  /// primary-key-fenced update (impossible, since `id` is a primary key) —
  /// both a storage-layer contract this module itself relies on being
  /// violated. `mark_contract_mismatch` also reports this for zero rows:
  /// the claim's own fence no longer matched between claiming the row and
  /// recording the mismatch — a lost-fence race, not an impossible state,
  /// just one this path has no more specific recovery for today.
  QueueStorageInvariantViolated
  QueueAckCommandConflict
  QueueAckProposalCodecMismatch(
    kind: worker.CodecKind,
    expected: String,
    actual: String,
  )
  QueueCodecMismatch(kind: worker.CodecKind, expected: String, actual: String)
}

pub type AckRejection {
  /// No row at all was found for this job id — there is no row to read a
  /// state, fence, or lease from.
  AckRecordMissing
  AckLeaseExpired(attempt_id: Int, epoch: Int, owner: String)
  /// The row was still `executing`, but its own fencing fields no longer
  /// match this attempt. Every other variant but `AckStateChanged` is only
  /// ever constructed while the row is `executing` too, so `state` is not
  /// repeated on any of them — it would carry no information.
  AckOwnershipChanged(
    attempt_id: Option(Int),
    epoch: Option(Int),
    owner: Option(String),
  )
  AckStateChanged(state: job.State)
  /// The row was still `executing`, still fenced to this exact attempt,
  /// and its lease was still live — every check this module can make
  /// passed — yet the acknowledgement's own fenced write still matched no
  /// row. Genuinely unexplained by anything this re-read can see. Carries
  /// no fields: by construction, the re-read's own attempt_id/epoch/owner
  /// already equal the caller's own claim, so repeating them back would
  /// carry no information.
  AckRecordChanged
}

/// Durable attribution for one acknowledged attempt. The typed value is read
/// from the job's current outcome; the receipt retains only the committed
/// disposition needed to reconcile an ACK after its reply is lost.
pub type AcknowledgementReceipt {
  AcknowledgementReceipt(
    command_id: String,
    attempt_id: Int,
    attempt_epoch: Int,
    committed_state: job.State,
    business_failure_cause: Option(worker.BusinessFailureCause),
    committed_at_unix_ms: Int,
  )
}

/// Exposed only so `grind/internal/attempt` can read a connection out of an
/// opaque `Database` — that module cannot pattern-match `Database`'s own
/// constructor, which stays private to this module.
@internal
pub fn connection(database: Database) -> pog.Connection {
  let Database(connection:, ..) = database
  connection
}

/// Exposed only so `grind/internal/attempt` can read a forwarder out of an
/// opaque `Database`; see `connection`.
@internal
pub fn forwarder(database: Database) -> Forwarder {
  let Database(forwarder:, ..) = database
  forwarder
}

pub type QuarantineError {
  /// `limit` was not positive; nothing was touched.
  NonPositiveLimit
  QuarantineQueryFailed(pog.QueryError)
}

/// Quarantines up to `limit` expired `executing` rows across every queue in
/// this schema — not only the queues some running consumer happens to poll
/// (see `README.md`, "Isolation"). Intended for queues no consumer currently
/// polls (an idle queue, a worker retired without a replacement consumer,
/// or an operational sweep run outside any consumer's own poll loop); a
/// polled queue's own consumer already quarantines its expired rows as part
/// of `claim_one`, so calling this for a polled queue too is safe (it is the
/// same fenced, idempotent `UPDATE ... WHERE state = 'executing' AND
/// <expired>`, `SKIP LOCKED` against a concurrent claim or another
/// quarantine scan) but redundant. Like every other quarantine scan, this
/// never decodes or runs any worker code and is not restricted to any
/// registered worker identity or version — see
/// `grind/internal/lease.quarantine_expired_in_queue`, which shares this
/// function's `expired_lease_predicate` and `UPDATE` fragment
/// (`quarantine_update_sql`). Returns the number of rows actually
/// quarantined, which can be fewer than `limit` (nothing else expired) —
/// never more.
pub fn quarantine_expired(
  database: Database,
  limit limit: Int,
) -> Result(Int, QuarantineError) {
  case limit > 0 {
    False -> Error(NonPositiveLimit)
    True -> {
      let Database(connection:, forwarder:, ..) = database
      // `FOR NO KEY UPDATE`: see `lease.quarantine_expired_in_queue`'s
      // identical reasoning for its own candidate lock.
      let sql =
        lease.quarantine_update_sql(
          "SELECT id FROM grind_jobs WHERE state = 'executing' AND "
          <> lease.expired_lease_predicate("clock_timestamp()")
          <> " ORDER BY id FOR NO KEY UPDATE SKIP LOCKED LIMIT $1",
        )
      let query =
        pog.query(sql)
        |> pog.parameter(pog.int(limit))
        |> pog.returning(lease.quarantine_row_decoder())
      case store.execute_safely(query, on: connection) {
        Error(error) -> Error(QuarantineQueryFailed(error))
        Ok(returned) -> {
          list.each(returned.rows, fn(row) {
            lease.emit_quarantined(forwarder, row)
          })
          Ok(list.length(returned.rows))
        }
      }
    }
  }
}

/// The upper bound `prune_finished`'s own `limit` argument accepts per call
/// — matched to Oban's own pruner default (`limit: 10_000`); this package's
/// documentation recommends starting closer to 1,000 and looping while
/// `report.jobs == limit` rather than reaching for this ceiling directly.
pub fn prune_limit_maximum() -> Int {
  10_000
}

pub type PruneReport {
  /// One `prune_finished` call's own count of rows deleted from
  /// `grind_jobs`. Each deleted job's own acknowledgement, uniqueness-
  /// submission, and resolution receipts are deleted alongside it too, via
  /// each receipt table's own `ON DELETE CASCADE` foreign key
  /// (`grind_v12`) — not counted here, and not counted separately anywhere,
  /// since a single `DELETE ... RETURNING` naming only `grind_jobs` cannot
  /// see rows a cascade removes as a side effect of the same statement.
  PruneReport(jobs: Int)
}

pub type PruneError {
  /// `older_than_ms` was not positive; nothing was touched. There is no
  /// minimum retention floor beyond this (matching Oban's own `max_age`,
  /// which is likewise only required to be positive) — see README,
  /// "Retention", for what a very short retention window can do to a late
  /// commit-unknown reconciliation.
  NonPositiveRetention
  /// `older_than_ms` exceeds `worker.retry_delay_maximum_milliseconds()`,
  /// the same millisecond-to-microsecond precision bound every other
  /// Grind-owned duration enforces (it is converted the same way, via
  /// `* interval '1 millisecond'`).
  RetentionAbovePrecisionBound
  /// `limit` was not positive; nothing was touched.
  NonPositivePruneLimit
  /// `limit` exceeds `prune_limit_maximum()`; nothing was touched.
  PruneLimitTooLarge
  PruneQueryFailed(pog.QueryError)
}

/// Deletes up to `limit` rows in this schema (see `README.md`, "Isolation")
/// that finished (reached one of the six terminal states — see
/// `grind/internal/terminal`) more than `older_than_ms` milliseconds ago.
/// Each deleted job's own acknowledgement, uniqueness-submission, and
/// resolution receipts are removed by the database itself, via each receipt
/// table's own `ON DELETE CASCADE` foreign key on `job_id` (`grind_v12`) —
/// not by a second, explicit delete this function also issues. Never scoped
/// by queue: a retention policy is a property of the whole schema, not of
/// any one queue.
///
/// One call is one bounded batch, never an unbounded sweep: `report.jobs`
/// can be fewer than `limit` (nothing else was old enough) but never more.
/// A caller wanting to drain everything currently prunable loops while
/// `report.jobs == limit`:
///
/// ```gleam
/// fn prune_until_caught_up(database, older_than_ms, limit) {
///   case postgres.prune_finished(database, older_than_ms:, limit:) {
///     Ok(postgres.PruneReport(jobs:)) if jobs == limit ->
///       prune_until_caught_up(database, older_than_ms, limit)
///     result -> result
///   }
/// }
/// ```
///
/// Candidates are selected oldest-`finished_at`-first
/// (`grind_jobs_finished_idx`) under `FOR UPDATE SKIP LOCKED`, so a
/// concurrently claiming consumer or another concurrent `prune_finished`
/// call never blocks this one (or is blocked by it) — each simply skips
/// whatever the other already holds. There is no leader election and no
/// single designated pruner process: unlike Oban's own plugin (which only
/// ever prunes from its cluster's elected leader), it is always safe to run
/// this — or the supervised pruner that calls it — on every node at once.
/// Emits one aggregate `[grind, prune, completed]` observation per
/// successful call (through the database's own forwarder) once the delete
/// has committed, carrying the same count as the returned `PruneReport` —
/// never one observation per deleted job. This function itself never emits
/// on its own `Error` path — a direct caller already gets `PruneError`
/// synchronously; `grind/pruner`, which has no caller to return it to,
/// emits `[grind, prune, failed]` instead when the call it drives fails.
///
/// **Once a job is pruned, everything about it is gone, not merely
/// hidden**: `state`/`outcome`/`bind_handle`/`arguments` all report not
/// found, `reconcile_acknowledgement` reports `ReceiptNotFound`, a
/// `submit_with_id` replay of the same `SubmissionId` inserts a genuinely
/// new row (the idempotency window is exactly the retention window), a
/// pending `reconcile_unique` call for a since-pruned submission id can
/// never resolve, and an automatic acknowledgement retry that reaches the
/// server after its job was pruned reports `QueueAckStale(AckRecordMissing)`
/// — the same shape an ordinary lost/reassigned row already produces, not a
/// new failure mode. See README, "Retention", for the full list.
pub fn prune_finished(
  database: Database,
  older_than_ms older_than_ms: Int,
  limit limit: Int,
) -> Result(PruneReport, PruneError) {
  use _ <- result.try(validate_retention_ms(older_than_ms))
  use _ <- result.try(validate_prune_limit(limit))
  run_prune(database, older_than_ms, limit)
}

/// The `older_than_ms` half of `prune_finished`'s own validation, factored
/// out so `grind/pruner.validate_policy` can enforce the identical bounds
/// on its own `max_age_ms` field without a second, independently
/// maintained copy of them.
@internal
pub fn validate_retention_ms(older_than_ms: Int) -> Result(Nil, PruneError) {
  case older_than_ms <= 0 {
    True -> Error(NonPositiveRetention)
    False ->
      case older_than_ms > worker.retry_delay_maximum_milliseconds() {
        True -> Error(RetentionAbovePrecisionBound)
        False -> Ok(Nil)
      }
  }
}

/// The `limit` half of `prune_finished`'s own validation — see
/// `validate_retention_ms`'s own doc comment for why this is `@internal`
/// and shared with `grind/pruner`.
@internal
pub fn validate_prune_limit(limit: Int) -> Result(Nil, PruneError) {
  case limit <= 0 {
    True -> Error(NonPositivePruneLimit)
    False ->
      case limit > prune_limit_maximum() {
        True -> Error(PruneLimitTooLarge)
        False -> Ok(Nil)
      }
  }
}

/// `sql.prune_finished` (`WITH doomed AS (...), deleted AS (DELETE ...
/// RETURNING 1) SELECT count(*) FROM deleted`) counts server-side rather
/// than `RETURNING x.id` and `list.length`-ing the rows here: the report
/// only ever needs a count, never the ids themselves, so this saves
/// transferring up to `limit` rows over the wire on a large batch just to
/// throw the values away. `deleted` always returns exactly one row (`0` for
/// an empty batch, never no rows at all), so decoding it is an `assert`,
/// not a fallible match.
fn run_prune(
  database: Database,
  older_than_ms: Int,
  limit: Int,
) -> Result(PruneReport, PruneError) {
  let Database(connection:, forwarder:, ..) = database
  case
    store.call_safely(connection, fn(connection) {
      sql.prune_finished(connection, older_than_ms, limit)
    })
  {
    Error(error) -> Error(PruneQueryFailed(error))
    Ok(returned) -> {
      let assert [sql.PruneFinishedRow(count:)] = returned.rows
      let report = PruneReport(jobs: count)
      emit_prune_completed(forwarder, report, older_than_ms, limit)
      Ok(report)
    }
  }
}

/// Builds and forwards `[grind, prune, completed]` for one committed
/// `prune_finished` call. Called only after `run_prune`'s own autocommitted
/// statement already returned its rows.
fn emit_prune_completed(
  fwd: Forwarder,
  report: PruneReport,
  older_than_ms: Int,
  limit: Int,
) -> Nil {
  let PruneReport(jobs:) = report
  let _ =
    forwarder.emit(
      fwd,
      observation.prune_completed(),
      observation.PruneCompletedMeasurements(jobs:),
      observation.PruneCompletedMetadata(older_than_ms:, limit:),
    )
  Nil
}

/// May return `JobReadQueryFailed`, `JobNotFound`, `QueueRouteMismatch`,
/// `WorkerContractMismatch`, or `InvalidStoredState`; never
/// `CodecContractMismatch` or `CodecFailed` (`state` never touches a
/// codec'd value), `SucceededOutputMissing`, `ReceiptNotFound`, or
/// `ReceiptJobMismatch`.
pub fn state(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(State, JobReadError) {
  let Database(connection:, installation:, ..) = database
  postgres_job_reads.state(connection, installation, handle)
  |> result.map_error(job_read_error)
}

/// Reads the last committed result without confusing it with a handler
/// return. May return `JobReadQueryFailed`, `JobNotFound`,
/// `QueueRouteMismatch`, `WorkerContractMismatch`,
/// `CodecFailed`, `InvalidStoredState`, or `SucceededOutputMissing`; never
/// `CodecContractMismatch` (`bind_handle`'s own pre-decode check),
/// `ReceiptNotFound`, or `ReceiptJobMismatch`.
pub fn outcome(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(job.Outcome(output, error), JobReadError) {
  let Database(connection:, installation:, ..) = database
  postgres_job_reads.outcome(connection, installation, handle)
  |> result.map_error(job_read_error)
}

/// Reads a compact acknowledgement receipt by its stable command ID.
/// Historical typed values are not retained here; `outcome` reads the job's
/// current typed result independently. May return `JobReadQueryFailed`,
/// `QueueRouteMismatch`, `WorkerContractMismatch`, `InvalidStoredState`,
/// `ReceiptNotFound` (no receipt exists under this command ID), or
/// `ReceiptJobMismatch` (a receipt exists but names a different job); never
/// `JobNotFound`, `CodecContractMismatch`, `CodecFailed`, or
/// `SucceededOutputMissing`, since a receipt carries no codec'd value of its
/// own and is looked up by command ID, not job ID.
pub fn reconcile_acknowledgement(
  database: Database,
  handle: JobHandle(input, output, error),
  command_id: String,
) -> Result(AcknowledgementReceipt, JobReadError) {
  let Database(connection:, installation:, ..) = database
  postgres_job_reads.reconcile_acknowledgement(
    connection,
    installation,
    handle,
    command_id,
  )
  |> result.map(fn(receipt) {
    let postgres_job_reads.AcknowledgementReceipt(
      command_id:,
      attempt_id:,
      attempt_epoch:,
      committed_state:,
      business_failure_cause:,
      committed_at_unix_ms:,
    ) = receipt
    AcknowledgementReceipt(
      command_id:,
      attempt_id:,
      attempt_epoch:,
      committed_state:,
      business_failure_cause:,
      committed_at_unix_ms:,
    )
  })
  |> result.map_error(job_read_error)
}

// -- Uniqueness admission ----------------------------------------------------
//
// The admission transaction itself lives in `grind/internal/unique_admission`
// (which has no dependency on this module, so `grind/postgres` depends on it
// instead). That module returns `grind/unique`'s public
// `Admission`/`Conflict`/`SubmitError`/`PendingSubmission` types
// directly, so `submit_unique`/`reconcile_unique` below are thin entry
// points, not a second translating layer. See `docs/UNIQUENESS-CONTRACT.md`
// for the full contract.

/// Admits one job under a uniqueness policy. Rejects an empty queue name
/// before acquiring any resource. See `docs/UNIQUENESS-CONTRACT.md` for the
/// full admission transaction. May return `submission.EmptyQueueName`,
/// `submission.AdmissionContended`, `submission.SubmissionConflict`,
/// `submission.NotCommitted`, or `submission.CommitUnknown`; never
/// `submission.CommitUnknownWithoutId`, which is reserved for `submit`/
/// `submit_at`'s own no-identity uncertain case.
pub fn submit_unique(
  database: Database,
  queue: String,
  submission_id: submission.SubmissionId,
  worker: Worker(input, output, error),
  input: input,
  availability: submission.Availability,
  policy: unique.Policy(input),
  on_conflict: unique.ConflictAction,
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let Database(connection:, unique_lock_wait_ms:, forwarder:, installation:, ..) =
    database
  let worker.Metadata(id: worker_id, worker_version:, ..) =
    worker.metadata(worker)
  case
    unique_admission.submit(
      connection,
      installation,
      unique_lock_wait_ms,
      queue,
      submission_id,
      worker,
      input,
      availability,
      policy,
      on_conflict,
    )
  {
    Ok(unique_admission.Commit(
      outcome:,
      committed_state:,
      available_at_unix_ms:,
      via_receipt_match:,
    )) -> {
      let confirmation = case via_receipt_match {
        True -> observation.Reconciled
        False -> observation.Replied
      }
      emit_admitted(
        forwarder,
        queue,
        admission_job_id(outcome),
        worker_id,
        worker_version,
        committed_state,
        available_at_unix_ms,
        Some(submission.submission_id_value(submission_id)),
        confirmation,
      )
      Ok(outcome)
    }
    Error(error) -> Error(error)
  }
}

/// Persists a job (immediately, or at an absolute availability time — see
/// `availability`) with a caller-supplied `SubmissionId`, no uniqueness
/// policy. Unlike `submit`/`submit_at`, a retry of this exact call (same id,
/// same request) converges on the original commit instead of risking a
/// duplicate row: it reuses `submit_unique`'s own admission receipt, request
/// fingerprint, and reconciliation machinery — the same unified admission
/// transaction in `grind/internal/unique_admission`, taken with no policy
/// (`Request.policy: None`), not a second implementation — see
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts", for the full
/// contract, including what lock (if any) protects a concurrent same-id
/// retry with no uniqueness key to serialize on. Returns `submission.Inserted`
/// on this call's own first commit, or on a matching replay;
/// `submit_unique`'s `Existing`/`Rescheduled` variants never occur here —
/// there is no policy to conflict against, so every resolved call is
/// `Inserted`. May return `submission.EmptyQueueName`,
/// `submission.AdmissionContended`, `submission.SubmissionConflict`,
/// `submission.NotCommitted`, or `submission.CommitUnknown` (recoverable with
/// `reconcile_unique`, or a plain retry of the same `SubmissionId`); never
/// `submission.CommitUnknownWithoutId`, which is reserved for `submit`/
/// `submit_at`'s own no-identity uncertain case — use this function (or
/// `submit_unique`) instead of `submit`/`submit_at` for a job that might
/// need to be resubmitted safely.
pub fn submit_with_id(
  database: Database,
  queue: String,
  submission_id: submission.SubmissionId,
  worker: Worker(input, output, error),
  input: input,
  availability: submission.Availability,
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let Database(connection:, unique_lock_wait_ms:, forwarder:, installation:, ..) =
    database
  let worker.Metadata(id: worker_id, worker_version:, ..) =
    worker.metadata(worker)
  case
    unique_admission.submit_plain(
      connection,
      installation,
      unique_lock_wait_ms,
      queue,
      submission_id,
      worker,
      input,
      availability,
    )
  {
    Ok(unique_admission.Commit(
      outcome:,
      committed_state:,
      available_at_unix_ms:,
      via_receipt_match:,
    )) -> {
      let confirmation = case via_receipt_match {
        True -> observation.Reconciled
        False -> observation.Replied
      }
      emit_admitted(
        forwarder,
        queue,
        admission_job_id(outcome),
        worker_id,
        worker_version,
        committed_state,
        available_at_unix_ms,
        Some(submission.submission_id_value(submission_id)),
        confirmation,
      )
      Ok(outcome)
    }
    Error(error) -> Error(error)
  }
}

/// The persisted job id an `Admission` decision is about, whichever variant
/// it is — `Inserted` carries a full `JobHandle`, `Existing`/`Rescheduled`
/// carry a `Conflict`, both identify exactly one row.
fn admission_job_id(
  admission: submission.Admission(input, output, error),
) -> Int {
  case admission {
    submission.Inserted(handle) -> job.id_value(handle)
    submission.Existing(conflict) -> submission.conflict_job_id(conflict)
    submission.Rescheduled(conflict) -> submission.conflict_job_id(conflict)
  }
}

/// Re-reads the receipt a `CommitUnknown` command would have written,
/// without repeating the admission transaction. A receipt matching the
/// retained request returns its recorded decision; a receipt that does not
/// match, or no receipt yet, both surface the same way a fresh `submit_unique`
/// call would (`SubmissionConflict` / another `CommitUnknown`).
pub fn reconcile_unique(
  database: Database,
  pending: submission.PendingSubmission(input, output, error),
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let Database(connection:, installation: database_installation, ..) = database
  let pending_installation = submission.pending_submission_installation(pending)
  case job.same_installation(pending_installation, database_installation) {
    False -> Error(submission.HandleFromAnotherInstallation)
    True -> unique_admission.reconcile(connection, pending)
  }
}
