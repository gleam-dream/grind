//// Bench-owned instrumentation triggers for L6 (renewal starvation): a
//// lease-log trigger that mirrors every `grind_jobs` `UPDATE`'s old/new
//// `attempt_id`/`state`/`lease_expires_at` into the ledger
//// (`grind_bench.bench_lease_log`), and a slow-ack trigger that delays a
//// chosen set of `grind_job_acknowledgements` inserts by a configured
//// amount (`grind_bench.bench_slow_ack_targets`).
////
//// Both triggers -- and their own trigger *functions* -- are created
//// against one run's own dynamically-named Grind schema (`grind_schema`,
//// always `grind_bench.default_config`'s own `"bench_jobs_" <>
//// fresh_run_id()`, never caller-supplied free text -- safe to splice
//// directly into DDL the same way `grind_bench.drop_schema`'s own doc
//// comment reasons about `DROP SCHEMA`), so both are dropped automatically
//// when that schema is dropped (`grind_bench.drop_schema`'s own `CASCADE`)
//// even if a scenario's own explicit `drop_*` call is skipped by a crash.
//// This is a bench-only, opt-in DDL addition to a disposable database
//// under test -- it never touches `grind`'s own source, and is installed
//// only by scenarios that ask for it (L6), never L1-L5.
////
//// See `bench/test/grind_bench_instrumentation_test.gleam` for the
//// red/green proof that each trigger measures what it claims: a lease-log
//// row appears on a real `UPDATE` only once the trigger is installed, and
//// the slow-ack trigger delays a *marked* job's acknowledgement insert by
//// (at least) the configured amount while leaving an unmarked job's own
//// insert fast.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result
import pog

fn qualify(schema: String, name: String) -> String {
  "\"" <> schema <> "\".\"" <> name <> "\""
}

fn exec(
  connection: pog.Connection,
  sql: String,
) -> Result(Nil, pog.QueryError) {
  pog.execute(pog.query(sql), connection) |> result.map(fn(_) { Nil })
}

// -- Lease-log trigger -------------------------------------------------------

/// Installs the lease-log trigger on `grind_schema`'s own `grind_jobs`
/// table via `grind_connection` (must have DDL rights on that schema --
/// Grind's own pool connection in the bench harness always does, since it
/// owns the schema). Every `UPDATE` copies `OLD`/`NEW`'s `attempt_id`,
/// `state`, and `lease_expires_at` into `grind_bench.bench_lease_log`
/// (fully qualified from inside the trigger function, so it resolves
/// regardless of the updating connection's own `search_path`).
pub fn install_lease_log(
  grind_connection: pog.Connection,
  grind_schema: String,
) -> Result(Nil, pog.QueryError) {
  let jobs = qualify(grind_schema, "grind_jobs")
  let function_name = qualify(grind_schema, "bench_lease_log_fn")
  let function_sql =
    "CREATE OR REPLACE FUNCTION "
    <> function_name
    <> "() RETURNS trigger AS $$ BEGIN "
    <> "INSERT INTO grind_bench.bench_lease_log (job_id, old_attempt_id, new_attempt_id, old_state, new_state, old_lease_expires_at, new_lease_expires_at) "
    <> "VALUES (NEW.id, OLD.attempt_id, NEW.attempt_id, OLD.state, NEW.state, OLD.lease_expires_at, NEW.lease_expires_at); "
    <> "RETURN NEW; END; $$ LANGUAGE plpgsql"
  let trigger_sql =
    "CREATE TRIGGER bench_lease_log_trg AFTER UPDATE ON "
    <> jobs
    <> " FOR EACH ROW EXECUTE FUNCTION "
    <> function_name
    <> "()"
  // Two `pog.execute` calls, never one string with both statements: pog's
  // extended-protocol prepared statement rejects "cannot insert multiple
  // commands into a prepared statement" for a single call carrying more
  // than one SQL command.
  use Nil <- result.try(exec(grind_connection, function_sql))
  exec(grind_connection, trigger_sql)
}

pub fn drop_lease_log(
  grind_connection: pog.Connection,
  grind_schema: String,
) -> Result(Nil, pog.QueryError) {
  let jobs = qualify(grind_schema, "grind_jobs")
  exec(
    grind_connection,
    "DROP TRIGGER IF EXISTS bench_lease_log_trg ON " <> jobs,
  )
}

pub fn truncate_lease_log(
  ledger: pog.Connection,
) -> Result(Nil, pog.QueryError) {
  exec(ledger, "TRUNCATE grind_bench.bench_lease_log")
}

/// Per-renewal headroom in milliseconds: `old_lease_expires_at - <the
/// moment the renewal committed>` for every logged renewal (`old_state`
/// and `new_state` both `'executing'`, same `attempt_id`, lease extended,
/// i.e. not a claim and not a quarantine/terminal transition) touching any
/// of `job_ids`. Positive means the renewal landed with that much slack
/// before the old lease would otherwise have expired.
pub fn renewal_headroom_ms(
  ledger: pog.Connection,
  job_ids: List(Int),
) -> Result(List(Float), pog.QueryError) {
  case job_ids {
    [] -> Ok([])
    _ -> {
      let query =
        pog.query(
          "SELECT extract(epoch FROM (l.old_lease_expires_at - l.observed_at)) * 1000.0 "
          <> "FROM grind_bench.bench_lease_log l "
          <> "WHERE l.old_state = 'executing' AND l.new_state = 'executing' "
          <> "AND l.old_attempt_id = l.new_attempt_id "
          <> "AND l.new_lease_expires_at > l.old_lease_expires_at "
          <> "AND l.job_id = ANY($1)",
        )
        |> pog.parameter(pog.array(pog.int, job_ids))
        |> pog.returning({
          use value <- decode.field(0, decode.float)
          decode.success(value)
        })
      pog.execute(query, ledger) |> result.map(fn(returned) { returned.rows })
    }
  }
}

/// Count of logged transitions into `uncertain` (a lease-expiry
/// quarantine) touching any of `job_ids`.
pub fn quarantine_transition_count(
  ledger: pog.Connection,
  job_ids: List(Int),
) -> Result(Int, pog.QueryError) {
  case job_ids {
    [] -> Ok(0)
    _ -> {
      let query =
        pog.query(
          "SELECT count(*) FROM grind_bench.bench_lease_log l "
          <> "WHERE l.new_state = 'uncertain' AND l.job_id = ANY($1)",
        )
        |> pog.parameter(pog.array(pog.int, job_ids))
        |> pog.returning({
          use value <- decode.field(0, decode.int)
          decode.success(value)
        })
      use returned <- result.try(pog.execute(query, ledger))
      case returned.rows {
        [count] -> Ok(count)
        _ -> Ok(0)
      }
    }
  }
}

// -- Slow-ack trigger --------------------------------------------------------

/// Installs the slow-ack trigger on `grind_schema`'s own
/// `grind_job_acknowledgements` table: a `BEFORE INSERT` that
/// `pg_sleep`s `delay_ms` (baked into the function body as a literal --
/// set once per scenario, dropped with the rest of the schema) whenever
/// `NEW.job_id` is present in `grind_bench.bench_slow_ack_targets`, then
/// always lets the insert through unchanged.
pub fn install_slow_ack(
  grind_connection: pog.Connection,
  grind_schema: String,
  delay_ms: Int,
) -> Result(Nil, pog.QueryError) {
  let acks = qualify(grind_schema, "grind_job_acknowledgements")
  let function_name = qualify(grind_schema, "bench_slow_ack_fn")
  let delay_seconds_literal = float_literal_ms(delay_ms)
  let function_sql =
    "CREATE OR REPLACE FUNCTION "
    <> function_name
    <> "() RETURNS trigger AS $$ BEGIN "
    <> "IF EXISTS (SELECT 1 FROM grind_bench.bench_slow_ack_targets t WHERE t.job_id = NEW.job_id) THEN "
    <> "PERFORM pg_sleep("
    <> delay_seconds_literal
    <> "); END IF; RETURN NEW; END; $$ LANGUAGE plpgsql"
  let trigger_sql =
    "CREATE TRIGGER bench_slow_ack_trg BEFORE INSERT ON "
    <> acks
    <> " FOR EACH ROW EXECUTE FUNCTION "
    <> function_name
    <> "()"
  use Nil <- result.try(exec(grind_connection, function_sql))
  exec(grind_connection, trigger_sql)
}

/// `delay_ms` as a PostgreSQL `double precision` seconds literal
/// (`pg_sleep` takes seconds) -- e.g. `800` -> `"0.800"`. Always 3 decimal
/// places, integer millisecond precision throughout this module.
fn float_literal_ms(delay_ms: Int) -> String {
  let whole = delay_ms / 1000
  let remainder = delay_ms % 1000
  let remainder_str = case remainder < 10 {
    True ->
      case remainder < 100 {
        True -> "00" <> int.to_string(remainder)
        False -> "0" <> int.to_string(remainder)
      }
    False -> int.to_string(remainder)
  }
  int.to_string(whole) <> "." <> remainder_str
}

pub fn drop_slow_ack(
  grind_connection: pog.Connection,
  grind_schema: String,
) -> Result(Nil, pog.QueryError) {
  let acks = qualify(grind_schema, "grind_job_acknowledgements")
  exec(
    grind_connection,
    "DROP TRIGGER IF EXISTS bench_slow_ack_trg ON " <> acks,
  )
}

pub fn mark_slow_ack_targets(
  ledger: pog.Connection,
  job_ids: List(Int),
) -> Result(Nil, pog.QueryError) {
  list.try_each(job_ids, fn(job_id) {
    exec(
      ledger,
      "INSERT INTO grind_bench.bench_slow_ack_targets (job_id) VALUES ("
        <> int.to_string(job_id)
        <> ") ON CONFLICT DO NOTHING",
    )
  })
}

pub fn clear_slow_ack_targets(
  ledger: pog.Connection,
) -> Result(Nil, pog.QueryError) {
  exec(ledger, "TRUNCATE grind_bench.bench_slow_ack_targets")
}
