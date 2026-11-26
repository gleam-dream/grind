import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleeunit/should
import grind/internal/postgres
import grind/internal/store
import grind/support/env.{mark_database_test_executed, queue_database_url}
import pog

/// A migration session that ends with `RetireSession` is disconnected, so
/// the server drops the session lock it holds; one that ends with
/// `ReturnSession` goes back to the pool and keeps it. The concurrent index
/// build returns its session only after a confirmed unlock.
pub fn postgres_retired_migration_session_drops_its_lock_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> {
      let assert Ok(validated) =
        postgres.settings(database_url) |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let connection = postgres.connection(database)

      let assert Ok(Nil) = with_lock(connection, 731_001, store.ReturnSession)
      held(connection, 731_001) |> should.equal(True)

      let assert Ok(Nil) = with_lock(connection, 731_002, store.RetireSession)
      released(connection, 731_002, 100) |> should.equal(True)
      mark_database_test_executed("retired-migration-session-drops-lock")
    }
  }
}

fn with_lock(
  connection: pog.Connection,
  key: Int,
  end: store.SessionEnd,
) -> Result(Nil, pog.QueryError) {
  store.migration_session_safely(connection, 5000, fn(session) {
    let taken =
      pog.query("SELECT pg_try_advisory_lock($1)")
      |> pog.parameter(pog.int(key))
      |> pog.execute(on: session)
    case taken {
      Ok(_) -> #(end, Ok(Nil))
      Error(error) -> #(store.RetireSession, Error(error))
    }
  })
}

fn held(connection: pog.Connection, key: Int) -> Bool {
  let assert Ok(pog.Returned(rows: [count], ..)) =
    pog.query(
      "SELECT count(*)::int FROM pg_locks WHERE locktype = 'advisory' AND classid = 0 AND objid::bigint = $1 AND objsubid = 1",
    )
    |> pog.parameter(pog.int(key))
    |> pog.returning(decode.at([0], decode.int))
    |> pog.execute(on: connection)
  count > 0
}

fn released(connection: pog.Connection, key: Int, remaining: Int) -> Bool {
  case held(connection, key), remaining > 0 {
    False, _ -> True
    True, True -> {
      process.sleep(20)
      released(connection, key, remaining - 1)
    }
    True, False -> False
  }
}
