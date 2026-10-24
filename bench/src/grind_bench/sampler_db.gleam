//// A 1-second PostgreSQL introspection sampler: `pg_stat_activity` counts by
//// state, waiting lock count, and `pg_stat_statements` call/time deltas for
//// statements touching Grind's own tables. Appends one JSON line per tick to
//// a caller-chosen path -- raw evidence, gitignored, never committed; see
//// `grind_bench/summarize` for the committed rollup.
////
//// Requires `pg_stat_statements` in `shared_preload_libraries` plus
//// `CREATE EXTENSION pg_stat_statements` in the target database (see
//// `scripts/bench-postgres.sh`). If the extension is unavailable, the
//// `pg_stat_statements` section of each line is simply omitted (`null`) --
//// `pg_stat_activity`/`pg_locks` sampling still runs, since it needs no
//// extension.

import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/list
import gleam/string
import gleam/time/timestamp
import grind_bench/harness_db
import pog
import simplifile

fn now_unix_ms() -> Int {
  let #(seconds, nanoseconds) =
    timestamp.system_time() |> timestamp.to_unix_seconds_and_nanoseconds
  seconds * 1000 + nanoseconds / 1_000_000
}

type StatementTotals =
  Dict(Int, #(Int, Float))

/// Appends one snapshot per `interval_ms` to `path` until `max_ticks` elapse
/// or this process is killed (`process.kill`) from outside, whichever comes
/// first -- see `grind_bench/sampler_beam.run`'s own doc comment for why a
/// `Subject`-based "stop" message does not work here (a `Subject` can only
/// be received from by the process that created it).
pub fn run(
  connection: pog.Connection,
  path: String,
  interval_ms: Int,
  max_ticks: Int,
) -> Nil {
  let _ = simplifile.create_directory_all(parent_dir(path))
  let _ = simplifile.write(to: path, contents: "")
  loop(connection, path, interval_ms, max_ticks, dict.new())
}

fn loop(
  connection: pog.Connection,
  path: String,
  interval_ms: Int,
  ticks_remaining: Int,
  previous_statements: StatementTotals,
) -> Nil {
  case ticks_remaining <= 0 {
    True -> Nil
    False -> {
      process.sleep(interval_ms)
      let #(line, next_statements) =
        snapshot_line(connection, previous_statements)
      let _ = simplifile.append(to: path, contents: line <> "\n")
      loop(connection, path, interval_ms, ticks_remaining - 1, next_statements)
    }
  }
}

fn snapshot_line(
  connection: pog.Connection,
  previous_statements: StatementTotals,
) -> #(String, StatementTotals) {
  let activity = activity_counts(connection)
  let waiting_locks = waiting_lock_count(connection)
  let #(statement_deltas, next_statements) =
    statement_deltas(connection, previous_statements)
  let line =
    json.object([
      #("unix_ms", json.int(now_unix_ms())),
      #(
        "activity_by_state",
        json.array(activity, fn(entry) {
          let #(state, count) = entry
          json.object([
            #("state", json.string(state)),
            #("count", json.int(count)),
          ])
        }),
      ),
      #("waiting_locks", json.int(waiting_locks)),
      #(
        "statements",
        json.array(statement_deltas, fn(entry) {
          let #(queryid, calls_delta, total_time_delta_ms) = entry
          json.object([
            #("queryid", json.int(queryid)),
            #("calls_delta", json.int(calls_delta)),
            #("total_time_delta_ms", json.float(total_time_delta_ms)),
          ])
        }),
      ),
    ])
    |> json.to_string
  #(line, next_statements)
}

fn activity_counts(connection: pog.Connection) -> List(#(String, Int)) {
  let query =
    pog.query(
      "SELECT coalesce(state, 'none'), count(*) FROM pg_stat_activity WHERE datname = current_database() GROUP BY state",
    )
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use count <- decode.field(1, decode.int)
      decode.success(#(state, count))
    })
  case harness_db.execute(query, connection) {
    Ok(returned) -> returned.rows
    Error(_) -> []
  }
}

fn waiting_lock_count(connection: pog.Connection) -> Int {
  let query =
    pog.query("SELECT count(*) FROM pg_locks WHERE NOT granted")
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case harness_db.execute(query, connection) {
    Ok(returned) ->
      case returned.rows {
        [count] -> count
        _ -> -1
      }
    Error(_) -> -1
  }
}

/// Statements referencing any Grind table (`grind_jobs` and its receipt
/// tables), by `queryid`, diffed against `previous_statements`. Absent
/// `pg_stat_statements` (extension not installed), this returns an empty
/// list and an unchanged (empty) map rather than failing the whole sample.
/// Hardcodes `public.pg_stat_statements`: `scripts/bench-postgres.sh` runs
/// `CREATE EXTENSION` with no explicit `SCHEMA`, which lands it in
/// `public` (the connecting role's own default `search_path`) --
/// unqualified `pg_stat_statements` silently returned zero rows the first
/// time this ran for real, since the querying connection's own
/// `search_path` is pinned to Grind's configured schema alone (see
/// `grind_bench.grind_settings`), which does not include `public`.
fn statement_deltas(
  connection: pog.Connection,
  previous_statements: StatementTotals,
) -> #(List(#(Int, Int, Float)), StatementTotals) {
  let query =
    pog.query(
      "SELECT queryid, calls, total_exec_time FROM public.pg_stat_statements WHERE query ILIKE '%grind_%' AND queryid IS NOT NULL",
    )
    |> pog.returning({
      use queryid <- decode.field(0, decode.int)
      use calls <- decode.field(1, decode.int)
      use total_exec_time <- decode.field(2, decode.float)
      decode.success(#(queryid, calls, total_exec_time))
    })
  case harness_db.execute(query, connection) {
    Error(_) -> #([], previous_statements)
    Ok(returned) -> {
      let next_statements =
        list.fold(returned.rows, dict.new(), fn(acc, row) {
          let #(queryid, calls, total_exec_time) = row
          dict.insert(acc, queryid, #(calls, total_exec_time))
        })
      let deltas =
        list.map(returned.rows, fn(row) {
          let #(queryid, calls, total_exec_time) = row
          let #(previous_calls, previous_total) = case
            dict.get(previous_statements, queryid)
          {
            Ok(previous) -> previous
            Error(Nil) -> #(calls, total_exec_time)
          }
          #(queryid, calls - previous_calls, total_exec_time -. previous_total)
        })
      #(deltas, next_statements)
    }
  }
}

/// The directory portion of a `/`-joined path, `"."` if `path` has no `/` at
/// all -- the same plain `gleam/string`/`gleam/list` approach as
/// `grind_bench/sampler_beam.parent_dir`.
fn parent_dir(path: String) -> String {
  case string.split(path, "/") {
    [] | [_] -> "."
    parts ->
      case list.take(parts, list.length(parts) - 1) {
        [] -> "."
        directories -> string.join(directories, "/")
      }
  }
}
