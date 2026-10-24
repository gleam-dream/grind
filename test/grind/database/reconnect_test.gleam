import exception
import gleam/erlang/process
import gleam/list
import gleam/otp/static_supervisor
import gleam/result
import gleeunit/should
import grind/internal/pool
import grind/internal/store
import grind/support/env.{database_url, mark_database_test_executed}
import pog

@external(erlang, "grind_reconnect_probe", "stale_holder_queries")
fn stale_holder_queries(
  connection: pog.Connection,
  kill_owner: Bool,
  query: fn() -> Bool,
) -> List(Bool)

@external(erlang, "grind_reconnect_probe", "multiple_stale_single_call")
fn multiple_stale_single_call(connection: pog.Connection) -> Bool

@external(erlang, "grind_reconnect_probe", "stale_holder_deadline")
fn stale_holder_deadline(connection: pog.Connection) -> Bool

/// A crashed idle connection leaves its old holder in pinned pgo's pool queue.
/// A replacement must make progress without handing that dead entry out forever.
pub fn postgres_retires_dead_connection_holder_after_failure_test() {
  run_reconnect_probe(True, "retires-dead-connection-holder")
}

/// A connection process may reconnect without changing PID. Its old socket is
/// still unusable, so checking only whether the connection owner lives is unsafe.
pub fn postgres_retires_closed_socket_holder_with_live_owner_test() {
  run_reconnect_probe(False, "retires-closed-socket-holder")
}

pub fn postgres_discards_multiple_stale_holders_without_repeating_callback_test() {
  run_additional_probe(
    multiple_stale_single_call,
    "multiple-stale-holders-callback-once",
  )
}

pub fn postgres_stale_holder_discard_preserves_original_deadline_test() {
  run_additional_probe(stale_holder_deadline, "stale-holder-original-deadline")
}

fn run_additional_probe(
  probe: fn(pog.Connection) -> Bool,
  marker: String,
) -> Nil {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let name = process.new_name("grind_postgres_multi_reconnect_probe")
      let assert Ok(config) = pog.url_config(name, url)
      let config = pog.Config(..config, pool_size: 1, idle_interval: 60_000)
      let root =
        static_supervisor.new(static_supervisor.OneForOne)
        |> static_supervisor.add(pool.supervised(config, 2000))
      let assert Ok(started) =
        pool.start_unlinked(fn() { static_supervisor.start(root) })
      use <- exception.defer(fn() { pool.abort_start(started.pid) })
      probe(pog.named_connection(name)) |> should.be_true()
      mark_database_test_executed(marker)
    }
  }
}

fn run_reconnect_probe(kill_owner: Bool, marker: String) -> Nil {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let name = process.new_name("grind_postgres_reconnect_probe")
      let assert Ok(config) = pog.url_config(name, url)
      // Suppress pgo's incidental idle sweep: retirement must follow the failed
      // managed checkout, even while frequent callers keep the pool non-idle.
      let config = pog.Config(..config, pool_size: 1, idle_interval: 60_000)
      let root =
        static_supervisor.new(static_supervisor.OneForOne)
        |> static_supervisor.add(pool.supervised(config, 1500))
      let assert Ok(started) =
        pool.start_unlinked(fn() { static_supervisor.start(root) })
      use <- exception.defer(fn() { pool.abort_start(started.pid) })
      let connection = pog.named_connection(name)
      let query = fn() {
        store.call_safely(connection, fn(checked_out) {
          pog.query("SELECT 99431 AS reconnect_holder_probe")
          |> pog.execute(on: checked_out)
        })
        |> result.is_ok
      }
      query() |> should.be_true()
      let results = stale_holder_queries(connection, kill_owner, query)
      list.length(results) |> should.equal(6)
      { results |> list.count(fn(ok) { !ok }) <= 1 } |> should.be_true()
      // The same managed pool survives and the final repeated reads succeed.
      results |> list.drop(1) |> list.all(fn(ok) { ok }) |> should.be_true()
      mark_database_test_executed(marker)
    }
  }
}
