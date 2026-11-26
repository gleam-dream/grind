//// Version 15 builds the settler's due-time index concurrently, outside
//// the version's transaction, so building it never blocks job writes.

import exception
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleeunit/should
import grind/internal/migrations
import grind/internal/postgres
import grind/support/env.{
  database_url, mark_database_test_executed, unique_test_run_id,
}
import grind/support/migration_fixtures.{schema_marker_max_version}
import pog

const index = "grind_jobs_settle_due_idx"

pub fn postgres_migrate_builds_the_settle_due_index_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      use database <- with_fresh_schema(url, "concurrent_fresh")
      postgres.migrate(database) |> should.equal(Ok(Nil))
      let connection = postgres.connection(database)
      index_validity(connection) |> should.equal([True])
      schema_marker_max_version(connection) |> should.equal(15)
      // Applying again changes nothing.
      postgres.migrate(database) |> should.equal(Ok(Nil))
      index_validity(connection) |> should.equal([True])
      mark_database_test_executed("concurrent-index-built")
    }
  }
}

/// A concurrent build that failed leaves an invalid index behind. The next
/// `migrate` drops it and builds it again before it writes the marker.
pub fn postgres_migrate_rebuilds_an_invalid_concurrent_index_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      use database <- with_fresh_schema(url, "concurrent_invalid")
      let before =
        list.filter(migrations.migrations(), fn(step) { step.version < 15 })
      postgres.migrate_with(database, before) |> should.equal(Ok(Nil))
      let connection = postgres.connection(database)
      let assert Ok(_) =
        pog.query(
          "CREATE INDEX " <> index <> " ON grind_jobs (queue, settle_at, id)",
        )
        |> pog.execute(on: connection)
      let assert Ok(_) =
        pog.query(
          "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass(quote_ident(current_schema()) || '."
          <> index
          <> "')",
        )
        |> pog.execute(on: connection)
      index_validity(connection) |> should.equal([False])

      postgres.migrate(database) |> should.equal(Ok(Nil))
      index_validity(connection) |> should.equal([True])
      schema_marker_max_version(connection) |> should.equal(15)
      mark_database_test_executed("concurrent-index-rebuilt")
    }
  }
}

fn index_validity(connection: pog.Connection) -> List(Bool) {
  let assert Ok(returned) =
    pog.query(
      "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname = $1",
    )
    |> pog.parameter(pog.text(index))
    |> pog.returning(decode.at([0], decode.bool))
    |> pog.execute(on: connection)
  returned.rows
}

fn with_fresh_schema(
  url: String,
  prefix: String,
  run: fn(postgres.Database) -> Nil,
) -> Nil {
  let schema = prefix <> "_" <> int.to_string(unique_test_run_id())
  let assert Ok(validated) =
    postgres.settings(url)
    |> postgres.with_schema(schema)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  run(database)
}
