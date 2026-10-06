//// Applies each migration under its own transaction and verifies the
//// physical schema generation. The public StorageError remains in postgres.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result
import grind/internal/migrations
import grind/internal/postgres/schema_probe as postgres_schema_probe
import grind/internal/sql
import grind/internal/store
import pog

pub type RunnerError {
  MigrationQueryFailed(pog.QueryError)
  IncompatibleSchema
  UnsupportedSchemaVersion(Int)
  SchemaCreationFailed(pog.QueryError)
  MigrationStepFailed(Int, pog.QueryError)
  MigrationLockUnavailable(Int)
  MigrationCommitUnknown(Int)
}

pub fn run(
  connection: pog.Connection,
  schema: String,
  quoted_schema: String,
  migration_deadline_ms: Int,
  lock_timeout_ms: Int,
  steps: List(migrations.Migration),
) -> Result(Nil, RunnerError) {
  let assert True = steps_are_contiguous_from_baseline(steps)
    as "migrate_with's steps must be exactly the contiguous range {baseline_schema_version..latest}, with no gaps or duplicate versions"
  use _ <- result.try(ensure_schema_exists(connection, schema, quoted_schema))
  steps
  |> list.sort(fn(a, b) { int.compare(a.version, b.version) })
  |> list.try_each(fn(step) {
    run_migration_step(
      connection,
      migration_deadline_ms,
      lock_timeout_ms,
      steps,
      step,
    )
  })
}

/// Creates the configured schema (`postgres.with_schema`, default
/// `"public"`) if it does not already exist — `migrate`/`migrate_with`'s own
/// first action, run before the advisory lock or any `current_schema()`-based
/// read: with `search_path` pinned (`validate`) to exactly this one schema,
/// `current_schema()` resolves to nothing at all until the schema physically
/// exists, so every later step in this call depends on this having already
/// run. `CREATE SCHEMA IF NOT EXISTS` is naturally idempotent, so running it
/// on every `migrate_with` call (including once the schema is long since
/// created) is a cheap no-op, not repeated work to guard against. `schema`
/// is quoted (`quote_ident`), never spliced unescaped. `postgres.start`
/// itself never creates a schema — only `migrate`/`migrate_with` do, per
/// this package's documented "migrate creates the schema if absent; other
/// calls fail typed if tables are missing" split (see `README.md`,
/// "Isolation").
fn ensure_schema_exists(
  connection: pog.Connection,
  schema: String,
  quoted_schema: String,
) -> Result(Nil, RunnerError) {
  use exists <- result.try(schema_exists(connection, schema))
  case exists {
    True -> Ok(Nil)
    False -> {
      let query = pog.query("CREATE SCHEMA IF NOT EXISTS " <> quoted_schema)
      case store.execute_safely(query, on: connection) {
        Ok(_) -> Ok(Nil)
        // `IF NOT EXISTS` does not make this statement concurrency-safe on
        // its own: two sessions can both run the existence check above,
        // both see it absent, and both attempt the actual `CREATE` — one of
        // them loses to PostgreSQL's own catalog uniqueness check and gets
        // a real error back (observed as `42P06 duplicate_schema` or
        // `23505 unique_violation` depending on timing), not a silent
        // no-op the way `IF NOT EXISTS` might suggest. Re-checking
        // existence here, rather than trusting this statement's own error
        // as fatal, is what makes two concurrent first-time `migrate`
        // callers both succeed regardless of which one actually created the
        // schema — see `postgres_migrate_concurrent_first_time_schema_creation_both_succeed_test`.
        Error(create_error) ->
          case schema_exists(connection, schema) {
            Ok(True) -> Ok(Nil)
            // The recheck itself failing is never more informative than the
            // `CREATE` failure that prompted it — surface the original
            // `create_error` (never `recheck_error`, and never
            // `schema_exists`'s own `IncompatibleSchema` for a malformed
            // row shape) so a caller sees why the schema could not be
            // created, not why a secondary existence probe also failed.
            Ok(False) | Error(_) -> Error(SchemaCreationFailed(create_error))
          }
      }
    }
  }
}

/// Checked separately from, and before, `CREATE SCHEMA IF NOT EXISTS`
/// itself: PostgreSQL's own `CREATE SCHEMA` (even with `IF NOT EXISTS`)
/// checks the connecting role's `CREATE` privilege on the *database* before
/// it ever checks whether the schema already exists, so a role that owns
/// its own already-existing schema — the exact least-privilege,
/// non-superuser "recommended setup" README, "Isolation" points to (a role
/// with `CREATE SCHEMA AUTHORIZATION <role>` run once by an administrator,
/// but no broader database-level `CREATE` grant) — would otherwise get
/// `42501 insufficient_privilege` from `ensure_schema_exists` on every
/// single `migrate` call, even though nothing would actually need
/// creating. A plain `pg_namespace` lookup needs no special privilege
/// (schema names are not access-controlled information), so checking first
/// and only ever attempting `CREATE SCHEMA` for a genuinely absent schema
/// keeps the "migrate creates the schema if absent" contract while never
/// demanding a privilege an already-provisioned installation has no reason
/// to hold. See `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/RECOVERY-EVIDENCE.md` for the real failure this fixes.
fn schema_exists(
  connection: pog.Connection,
  schema: String,
) -> Result(Bool, RunnerError) {
  let query =
    pog.query("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
    |> pog.parameter(pog.text(schema))
    |> pog.returning({
      use exists <- decode.field(0, decode.bool)
      decode.success(exists)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(SchemaCreationFailed(error))
    Ok(returned) ->
      case returned.rows {
        [exists] -> Ok(exists)
        _ -> Error(IncompatibleSchema)
      }
  }
}

fn steps_are_contiguous_from_baseline(
  steps: List(migrations.Migration),
) -> Bool {
  let versions =
    steps
    |> list.map(fn(step) { step.version })
    |> list.sort(int.compare)
  case versions {
    [] -> False
    [first, ..] if first != baseline_schema_version -> False
    _ ->
      versions
      |> list.index_map(fn(version, index) {
        version == baseline_schema_version + index
      })
      |> list.all(fn(matches) { matches })
  }
}

fn run_migration_step(
  connection: pog.Connection,
  migration_deadline_ms: Int,
  lock_timeout_ms: Int,
  steps: List(migrations.Migration),
  step: migrations.Migration,
) -> Result(Nil, RunnerError) {
  case
    store.migration_transaction_safely(
      connection,
      migration_deadline_ms,
      fn(transaction) {
        run_migration_step_transaction(
          transaction,
          lock_timeout_ms,
          steps,
          step,
        )
      },
    )
  {
    Ok(Nil) -> Ok(Nil)
    // `BEGIN`/`COMMIT` itself failing or losing its reply is one of three
    // shapes this covers — see `MigrationCommitUnknown`'s own doc comment
    // for the other two (a checkout failure before `BEGIN` ever ran, and a
    // failed `ROLLBACK` after a statement error). Re-running `migrate` is
    // always safe regardless of which one occurred.
    Error(pog.TransactionQueryError(_)) ->
      Error(MigrationCommitUnknown(step.version))
    Error(pog.TransactionRolledBack(error)) -> Error(error)
  }
}

fn run_migration_step_transaction(
  connection: pog.Connection,
  lock_timeout_ms: Int,
  steps: List(migrations.Migration),
  step: migrations.Migration,
) -> Result(Nil, RunnerError) {
  use _ <- result.try(pin_read_committed(connection))
  use _ <- result.try(acquire_migration_lock(connection))
  use _ <- result.try(set_migration_lock_timeout(connection, lock_timeout_ms))
  use generation <- result.try(read_schema_generation(connection, steps))
  case generation_at_least(generation, step.version) {
    True -> Ok(Nil)
    False -> {
      use _ <- result.try(
        list.try_each(step.statements, fn(statement) {
          case store.execute_safely(pog.query(statement), on: connection) {
            Ok(_) -> Ok(Nil)
            Error(pog.PostgresqlError("55P03", _, _)) ->
              Error(MigrationLockUnavailable(step.version))
            Error(query_error) ->
              Error(MigrationStepFailed(step.version, query_error))
          }
        }),
      )
      use applied <- result.try(read_schema_generation(connection, steps))
      case applied {
        AtVersion(version) if version == step.version -> Ok(Nil)
        _ -> Error(IncompatibleSchema)
      }
    }
  }
}

/// Pins this transaction to `READ COMMITTED`, as its own literal first
/// statement — reusing the same Squirrel-generated query and rationale as
/// `grind/internal/unique_admission`'s own `pin_read_committed`. Not
/// load-bearing the same way there (a migration step's later reads have no
/// concurrent-commit-visibility requirement `READ COMMITTED` specifically
/// satisfies), but kept as the same defence in depth against a role or
/// database whose own `default_transaction_isolation` is not already
/// `READ COMMITTED`, for consistency and because the pool-level pin
/// (`postgres.validate`) — the two connections' first line of defence — can
/// still be silently dropped by a pooler between Grind and PostgreSQL.
fn pin_read_committed(connection: pog.Connection) -> Result(Nil, RunnerError) {
  case
    store.call_safely(connection, fn(connection) {
      sql.pin_read_committed(connection)
    })
  {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(_) -> Ok(Nil)
  }
}

/// Serialises concurrent migrators (this process or another) against one
/// another: an ordinary PostgreSQL transaction-scoped advisory lock, held
/// until this step's own transaction commits or rolls back, keyed by a
/// fixed Grind string plus the current schema so unrelated schemas (or a
/// non-Grind advisory lock user) never collide with it. The identical
/// statement text is also embedded as every version's own first entry in
/// `migrations()`/`priv/migrations/*.sql`
/// (`migrations.advisory_lock_statement()`) so cigogne serialises the same
/// way; running it here too, unconditionally, is what makes `migrate_with`
/// itself safe even for a caller that only ever uses `postgres.migrate`.
fn acquire_migration_lock(
  connection: pog.Connection,
) -> Result(Nil, RunnerError) {
  let query =
    pog.query(migrations.advisory_lock_statement())
    |> pog.returning({
      use locked <- decode.field(0, decode.bool)
      decode.success(locked)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(_) -> Ok(Nil)
  }
}

/// Sets this step's transaction to the constant `migration_lock_timeout_ms`
/// (2000ms), transaction-local (`SET LOCAL`, via the same
/// `grind/internal/unique_admission` uses for `unique_lock_wait_ms`) and
/// therefore never leaking onto the pooled connection once this transaction
/// commits or rolls back. Run after `acquire_migration_lock` so the wait for
/// that advisory lock itself is unaffected — only this step's own
/// statements are bounded by it.
fn set_migration_lock_timeout(
  connection: pog.Connection,
  lock_timeout_ms: Int,
) -> Result(Nil, RunnerError) {
  case
    store.call_safely(connection, fn(connection) {
      sql.set_lock_timeout(connection, int.to_string(lock_timeout_ms))
    })
  {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(_) -> Ok(Nil)
  }
}

/// The lowest schema version `migrate`/`migrate_with` ever accepts as an
/// already-installed generation. Anything below it (including the legacy,
/// never-migrated v10 experimental marker) fails closed as
/// `UnsupportedSchemaVersion`; there is no migration path onto v11.
const baseline_schema_version = 11

type SchemaGeneration {
  Fresh
  AtVersion(Int)
}

fn generation_at_least(generation: SchemaGeneration, version: Int) -> Bool {
  case generation {
    Fresh -> False
    AtVersion(current) -> current >= version
  }
}

/// Reads the schema's current generation against `steps` (the full step
/// list this `migrate_with` call was given — `migrate` itself always passes
/// `migrations()`; `latest` is the highest version among them):
///
/// - No `grind_schema_migrations` marker table: `Fresh` if no other
///   `grind_`-prefixed object exists either, else `IncompatibleSchema` (a
///   foreign or partial schema that never reached Grind's own bookkeeping).
/// - A marker table whose rows are not exactly the contiguous range
///   `{baseline_schema_version..max}` (a gap, an empty table, or a marker
///   starting below the baseline while also reaching at or above it):
///   `IncompatibleSchema`.
/// - `max` above `latest`: `UnsupportedSchemaVersion(max)` — a schema newer
///   than this call knows how to run against. Checked *before* any physical
///   shape check runs, so a schema tagged with a foreign or future marker
///   is rejected correctly even alongside unrelated foreign objects or a
///   broken shape at an earlier, otherwise-valid version.
/// - `max` below `baseline_schema_version` (the legacy, never-migrated v10
///   experimental marker, or any older one): `UnsupportedSchemaVersion(max)`.
/// - Otherwise, `max` is a version present in `steps`: `AtVersion(max)` once
///   the actual `grind_`-prefixed relation set (name and kind) is *exactly*
///   (no fewer, no more) that version's own `Migration.shape`, and every
///   `key_columns` entry in it is present — a defensive, fail-closed check
///   against a tampered or partially-repaired schema, never trusted from
///   the marker alone. A version absent from `steps` at this point cannot
///   occur: `max <= latest` and `max >= baseline_schema_version` were just
///   checked, and `migrate_with`'s own precondition guarantees `steps`
///   covers every version in that range — but is still handled as
///   `IncompatibleSchema` rather than a crash, defensively.
fn read_schema_generation(
  connection: pog.Connection,
  steps: List(migrations.Migration),
) -> Result(SchemaGeneration, RunnerError) {
  let latest =
    list.fold(steps, 0, fn(highest, step) { int.max(highest, step.version) })
  use marker_table_exists <- result.try(schema_migrations_table_exists(
    connection,
  ))
  case marker_table_exists {
    False ->
      case read_grind_relations(connection) {
        Error(error) -> Error(error)
        Ok([]) -> Ok(Fresh)
        Ok(_) -> Error(IncompatibleSchema)
      }
    True -> {
      use #(count, minimum, maximum) <- result.try(read_schema_marker(
        connection,
      ))
      case count, minimum, maximum {
        0, _, _ -> Error(IncompatibleSchema)
        count, minimum, maximum if count != maximum - minimum + 1 ->
          Error(IncompatibleSchema)
        _, _, maximum if maximum > latest ->
          Error(UnsupportedSchemaVersion(maximum))
        _, _, maximum if maximum < baseline_schema_version ->
          Error(UnsupportedSchemaVersion(maximum))
        _, minimum, _ if minimum < baseline_schema_version ->
          Error(IncompatibleSchema)
        _, minimum, _ if minimum > baseline_schema_version ->
          Error(IncompatibleSchema)
        _, _, maximum -> validate_expected_shape(connection, maximum, steps)
      }
    }
  }
}

/// The per-version physical-shape check backing `read_schema_generation`'s
/// fail-closed guarantee: `version`'s own `Migration.shape`, looked up from
/// `steps`, must match the current schema's actual `grind_`-prefixed
/// relation set *exactly* (a stray relation this shape does not list — a
/// leftover from a version this schema was never fully repaired from, or
/// genuinely foreign clutter — fails closed exactly like a missing one).
fn probe_error(error: postgres_schema_probe.ProbeError) -> RunnerError {
  case error {
    postgres_schema_probe.ProbeQueryFailed(error) -> MigrationQueryFailed(error)
    postgres_schema_probe.ProbeMalformed -> IncompatibleSchema
  }
}

fn schema_migrations_table_exists(
  connection: pog.Connection,
) -> Result(Bool, RunnerError) {
  postgres_schema_probe.schema_migrations_table_exists(connection)
  |> result.map_error(probe_error)
}

fn read_grind_relations(
  connection: pog.Connection,
) -> Result(List(#(String, String)), RunnerError) {
  postgres_schema_probe.read_grind_relations(connection)
  |> result.map_error(probe_error)
}

fn read_schema_marker(
  connection: pog.Connection,
) -> Result(#(Int, Int, Int), RunnerError) {
  postgres_schema_probe.read_schema_marker(connection)
  |> result.map_error(probe_error)
}

fn validate_expected_shape(
  connection: pog.Connection,
  version: Int,
  steps: List(migrations.Migration),
) -> Result(SchemaGeneration, RunnerError) {
  case list.find(steps, fn(step) { step.version == version }) {
    Error(Nil) -> Error(IncompatibleSchema)
    Ok(step) -> {
      use shape_ok <- result.try(
        postgres_schema_probe.relation_shape_matches(connection, step.shape)
        |> result.map_error(probe_error),
      )
      case shape_ok {
        False -> Error(IncompatibleSchema)
        True ->
          case
            postgres_schema_probe.relation_foreign_keys_match(
              connection,
              step.foreign_keys,
            )
            |> result.map_error(probe_error)
          {
            Error(error) -> Error(error)
            Ok(False) -> Error(IncompatibleSchema)
            Ok(True) ->
              case
                postgres_schema_probe.forbidden_columns_absent(
                  connection,
                  step.forbidden_columns,
                )
                |> result.map_error(probe_error)
              {
                Error(error) -> Error(error)
                Ok(False) -> Error(IncompatibleSchema)
                Ok(True) -> Ok(AtVersion(version))
              }
          }
      }
    }
  }
}
