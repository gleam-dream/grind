import gleam/dynamic/decode
import gleam/list
import gleam/string
import grind/internal/migrations
import pog
import simplifile

/// The real, highest-numbered step `migrations.migrations()` currently
/// defines (`v12` as of this change) — each synthetic migration test step
/// appends *after* this one, so its own `shape` must extend this step's own
/// cumulative shape, not `v11`'s, or `validate_expected_shape` would see the
/// synthetic step's declared shape omit every real relation `v12` itself
/// added.
pub fn latest_migration() -> migrations.Migration {
  let assert Ok(step) = list.last(migrations.migrations())
  step
}

/// The frozen up-dump of the real `grind_v11` migration, applied with plain
/// `pog.execute` statement by statement — deliberately independent of
/// `grind/internal/migrations` and `priv/migrations/`, so this test still
/// exercises "a schema a previous Grind release actually installed" even if
/// a future baseline's statement text ever moved on.
pub fn read_sql_statements_from_file(path: String) -> List(String) {
  let assert Ok(contents) = simplifile.read(from: path)
  contents
  |> string.split("\n")
  |> list.filter(fn(line) {
    let trimmed = string.trim(line)
    trimmed != "" && !string.starts_with(trimmed, "--")
  })
  |> string.join("")
  |> string.split(";")
  |> list.map(string.trim)
  |> list.filter(fn(statement) { statement != "" })
}

pub fn apply_sql_statements(
  connection: pog.Connection,
  statements: List(String),
) -> Nil {
  list.each(statements, fn(statement) {
    let assert Ok(_) = pog.query(statement) |> pog.execute(on: connection)
    Nil
  })
}

/// One digest per catalog kind (`information_schema.columns` — including
/// `column_default` and `ordinal_position`, so a reordered or
/// differently-defaulted column would be caught, not only a renamed or
/// retyped one — `pg_constraint`, `pg_indexes`, and `pg_sequences`), scoped
/// to every `grind_`-prefixed object in the current schema,
/// order-independent by construction (`string_agg` with an explicit
/// `ORDER BY`) — compared between the seeded-then-upgraded database and a
/// fresh `migrate_with` install of the exact same step list.
pub fn grind_catalog_digest(
  connection: pog.Connection,
) -> #(String, String, String, String) {
  let columns =
    catalog_digest_query(
      connection,
      "SELECT coalesce(string_agg(table_name || ':' || ordinal_position || ':' || column_name || ':' || data_type || ':' || is_nullable || ':' || coalesce(column_default, ''), ',' ORDER BY table_name, ordinal_position), '') FROM information_schema.columns WHERE table_schema = current_schema() AND table_name ~ '^grind_'",
    )
  let constraints =
    catalog_digest_query(
      connection,
      "SELECT coalesce(string_agg(conrelid::regclass::text || ':' || conname || ':' || contype::text || ':' || pg_get_constraintdef(oid), ',' ORDER BY conrelid::regclass::text, conname), '') FROM pg_constraint WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND conrelid::regclass::text ~ '^grind_'",
    )
  let indexes =
    catalog_digest_query(
      connection,
      "SELECT coalesce(string_agg(indexname || ':' || indexdef, ',' ORDER BY indexname), '') FROM pg_indexes WHERE schemaname = current_schema() AND tablename ~ '^grind_'",
    )
  let sequences =
    catalog_digest_query(
      connection,
      "SELECT coalesce(string_agg(sequencename || ':' || data_type || ':' || start_value || ':' || min_value || ':' || max_value || ':' || increment_by || ':' || cache_size || ':' || cycle, ',' ORDER BY sequencename), '') FROM pg_sequences WHERE schemaname = current_schema() AND sequencename ~ '^grind_'",
    )
  #(columns, constraints, indexes, sequences)
}

pub fn catalog_digest_query(connection: pog.Connection, sql: String) -> String {
  let assert Ok(returned) =
    pog.query(sql)
    |> pog.returning({
      use value <- decode.field(0, decode.string)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  let assert [value] = returned.rows
  value
}

pub fn schema_marker_max_version(connection: pog.Connection) -> Int {
  let assert Ok(returned) =
    pog.query(
      "SELECT COALESCE(max(version), 0)::bigint FROM grind_schema_migrations",
    )
    |> pog.returning({
      use max_version <- decode.field(0, decode.int)
      decode.success(max_version)
    })
    |> pog.execute(on: connection)
  let assert [max_version] = returned.rows
  max_version
}
