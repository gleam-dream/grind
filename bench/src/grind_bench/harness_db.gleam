//// The harness's own bookkeeping queries (ledger, drain polling, the
//// completion observer, audits): pog calls with a generous timeout that
//// survive a pgo pool restart.
////
//// A harness query used to run with pog's 5 s default timeout on a pool
//// shared by the 10 ms completion observer and the drain poller. Under
//// heavy machine load a query can run close to that deadline; pgo arms the
//// deadline at checkout and cancels it asynchronously at checkin, and a
//// deadline message that arrives after its connection was checked in or
//// replaced can crash pgo's pool process. While the pool restarts its name
//// is unregistered, so a concurrent query exits with `noproc`. That is the
//// bench L3 crash seen once while nine package gates ran in parallel. It is
//// a property of the harness's plain pog usage, not of Grind's own storage
//// calls, which check out connections through Grind's bounded FFI.
////
//// The harness now gives the observer and the drain poller separate pools,
//// runs every bookkeeping query with a 60 s timeout, so no deadline fires
//// near a slow query's completion, and retries a query that exits with
//// `noproc` for up to 5 s, counting each retry in `pool_restart_retries`.

import exception
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process
import pog

const harness_query_timeout_ms = 60_000

/// Runs `query` on a harness connection.
pub fn execute(
  query: pog.Query(a),
  on connection: pog.Connection,
) -> Result(pog.Returned(a), pog.QueryError) {
  attempt(pog.timeout(query, harness_query_timeout_ms), connection, 50)
}

fn attempt(
  query: pog.Query(a),
  connection: pog.Connection,
  remaining: Int,
) -> Result(pog.Returned(a), pog.QueryError) {
  case exception.rescue(fn() { pog.execute(query, connection) }) {
    Ok(result) -> result
    Error(exception.Exited(reason)) ->
      case is_noproc(reason) && remaining > 0 {
        True -> {
          bump_restart_retries()
          process.sleep(100)
          attempt(query, connection, remaining - 1)
        }
        False -> reraise_exit(reason)
      }
    Error(exception.Errored(reason)) -> reraise_error(reason)
    Error(exception.Thrown(reason)) -> reraise_throw(reason)
  }
}

/// How many harness queries were retried after a pgo pool restart.
@external(erlang, "grind_bench_harness_db_ffi", "restart_retries")
pub fn pool_restart_retries() -> Int

@external(erlang, "grind_bench_harness_db_ffi", "bump_restart_retries")
fn bump_restart_retries() -> Nil

@external(erlang, "grind_bench_harness_db_ffi", "is_noproc")
fn is_noproc(reason: Dynamic) -> Bool

@external(erlang, "erlang", "exit")
fn reraise_exit(reason: Dynamic) -> a

@external(erlang, "erlang", "error")
fn reraise_error(reason: Dynamic) -> a

@external(erlang, "erlang", "throw")
fn reraise_throw(reason: Dynamic) -> a
