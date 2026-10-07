//// Scopes resolution statements inside a caller-owned transaction.

import gleam/result
import gleam/string
import grind/internal/job
import grind/internal/sql
import grind/internal/store
import pog

pub type Error {
  NotInTransaction
  IsolationUnsupported(String)
  WrongDatabase
  QueryFailed(pog.QueryError)
}

pub opaque type Settings {
  Settings(search_path: String, lock_timeout: String, statement_timeout: String)
}

@external(erlang, "grind_postgres_ffi", "is_single_connection")
fn is_single_connection(connection: pog.Connection) -> Bool

pub fn enter(
  tx: pog.Connection,
  installation: job.Installation,
  deadline_ms: Int,
) -> Result(Settings, Error) {
  use Nil <- result.try(case is_single_connection(tx) {
    False -> Error(NotInTransaction)
    True -> Ok(Nil)
  })
  use rows <- result.try(
    store.call_safely(tx, sql.resolution_transaction_settings)
    |> result.map_error(QueryFailed),
  )
  let assert [settings] = rows.rows
  use Nil <- result.try(case settings.isolation {
    "read committed" -> Ok(Nil)
    level -> Error(IsolationUnsupported(level))
  })
  use Nil <- result.try(
    case settings.database_oid == job.installation_database_oid(installation) {
      True -> Ok(Nil)
      False -> Error(WrongDatabase)
    },
  )
  let schema =
    "\""
    <> string.replace(job.installation_schema(installation), "\"", "\"\"")
    <> "\""
  use _ <- result.try(
    store.call_safely(tx, sql.scope_resolution_transaction(
      _,
      schema,
      deadline_ms,
    ))
    |> result.map_error(QueryFailed),
  )
  Ok(Settings(
    settings.search_path,
    settings.lock_timeout,
    settings.statement_timeout,
  ))
}

pub fn restore(
  tx: pog.Connection,
  settings: Settings,
) -> Result(Nil, pog.QueryError) {
  store.call_safely(tx, sql.restore_resolution_transaction(
    _,
    settings.search_path,
    settings.lock_timeout,
    settings.statement_timeout,
  ))
  |> result.replace(Nil)
}
