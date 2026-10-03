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
import grind_bench/harness_db
import pog

fn qualify(schema: String, name: String) -> String {
  "\"" <> schema <> "\".\"" <> name <> "\""
}

fn exec(
  connection: pog.Connection,
  sql: String,
) -> Result(Nil, pog.QueryError) {
  harness_db.execute(pog.query(sql), connection) |> result.map(fn(_) { Nil })
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
      harness_db.execute(query, ledger)
      |> result.map(fn(returned) { returned.rows })
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
      use returned <- result.try(harness_db.execute(query, ledger))
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
    <> "PERFORM nextval('grind_bench.bench_slow_ack_activations'); "
    <> "PERFORM nextval(format('%I.%I', TG_TABLE_SCHEMA, 'bench_slow_ack_target_' || NEW.job_id)::regclass); PERFORM pg_sleep("
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
pub fn float_literal_ms(delay_ms: Int) -> String {
  let whole = delay_ms / 1000
  let remainder = delay_ms % 1000
  let remainder_str = case remainder < 10 {
    True -> "00" <> int.to_string(remainder)
    False ->
      case remainder < 100 {
        True -> "0" <> int.to_string(remainder)
        False -> int.to_string(remainder)
      }
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
  grind_schema: String,
  job_ids: List(Int),
) -> Result(Nil, pog.QueryError) {
  // This untimed setup replaces the target set and resets its witnesses.
  // Sequences live in the run schema, so CASCADE also removes them after a
  // crashed run even when reset_ledger has already discarded the target IDs.
  use Nil <- result.try(clear_slow_ack_targets(ledger, grind_schema))
  list.try_each(job_ids, fn(job_id) {
    let assert True = job_id > 0
    use Nil <- result.try(exec(
      ledger,
      "CREATE SEQUENCE "
        <> slow_ack_target_sequence(grind_schema, job_id)
        <> " MINVALUE 0 START 0",
    ))
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
  grind_schema: String,
) -> Result(Nil, pog.QueryError) {
  let query =
    pog.query(
      "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relkind = 'S' AND c.relname ~ '^bench_slow_ack_target_[0-9]+$'",
    )
    |> pog.parameter(pog.text(grind_schema))
    |> pog.returning(decode.at([0], decode.string))
  use returned <- result.try(harness_db.execute(query, ledger))
  use Nil <- result.try(
    list.try_each(returned.rows, fn(name) {
      exec(ledger, "DROP SEQUENCE " <> qualify(grind_schema, name))
    }),
  )
  exec(ledger, "TRUNCATE grind_bench.bench_slow_ack_targets")
}

fn slow_ack_target_sequence(grind_schema: String, job_id: Int) -> String {
  qualify(grind_schema, "bench_slow_ack_target_" <> int.to_string(job_id))
}

/// One rollback-proof count per selected job. Repeated activations of one
/// job never provide evidence that another target reached its slow ACK.
pub fn slow_ack_target_activations(
  ledger: pog.Connection,
  grind_schema: String,
  job_ids: List(Int),
) -> Result(List(#(Int, Int)), pog.QueryError) {
  list.try_map(job_ids, fn(job_id) {
    let query =
      pog.query(
        "SELECT CASE WHEN is_called THEN last_value + 1 ELSE 0 END FROM "
        <> slow_ack_target_sequence(grind_schema, job_id),
      )
      |> pog.returning(decode.at([0], decode.int))
    use returned <- result.try(harness_db.execute(query, ledger))
    let assert [count] = returned.rows
    Ok(#(job_id, count))
  })
}

/// One sample for every tracked attempt, independently of renewal success.
/// The observer uses its own pool; negative executing headroom remains visible.
pub fn sample_leases(
  ledger: pog.Connection,
  schema: String,
) -> Result(Nil, pog.QueryError) {
  exec(
    ledger,
    "INSERT INTO grind_bench.bench_lease_samples (job_id, attempt_id, state, headroom_ms, handler_running, slow_acks) "
      <> "SELECT j.id, j.attempt_id, j.state, extract(epoch FROM (j.lease_expires_at - clock_timestamp())) * 1000.0, "
      <> "EXISTS (SELECT 1 FROM grind_bench.bench_effects e WHERE e.bench_index = s.bench_index AND e.finished_at IS NULL), "
      <> "(SELECT count(*)::int FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND query LIKE '%grind_job_acknowledgements%') "
      <> "FROM "
      <> qualify(schema, "grind_jobs")
      <> " j JOIN grind_bench.bench_submissions s ON s.job_id = j.id WHERE j.attempt_id IS NOT NULL",
  )
}

pub type LeaseEvidence {
  LeaseEvidence(
    attempts: Int,
    minimum_headroom_ms: Float,
    negative_samples: Int,
    overlap_samples: Int,
    renewals_during_slow_ack: Int,
  )
}

pub fn lease_evidence(
  ledger: pog.Connection,
  job_ids: List(Int),
) -> Result(LeaseEvidence, pog.QueryError) {
  let query =
    pog.query(
      "SELECT count(DISTINCT (s.job_id, s.attempt_id)), coalesce(min(s.headroom_ms) FILTER (WHERE s.state = 'executing'), -1.0)::float8, "
      <> "count(*) FILTER (WHERE s.state = 'executing' AND s.headroom_ms <= 0), "
      <> "count(*) FILTER (WHERE s.handler_running AND s.slow_acks > 0), "
      <> "(SELECT count(*) FROM grind_bench.bench_lease_log l WHERE l.job_id = ANY($1) AND l.old_state = 'executing' AND l.new_state = 'executing' AND l.new_lease_expires_at > l.old_lease_expires_at AND EXISTS (SELECT 1 FROM grind_bench.bench_lease_samples x WHERE x.slow_acks > 0 AND abs(extract(epoch FROM (x.sampled_at-l.observed_at))) < 0.1)) "
      <> "FROM grind_bench.bench_lease_samples s WHERE s.job_id = ANY($1)",
    )
    |> pog.parameter(pog.array(pog.int, job_ids))
    |> pog.returning({
      use attempts <- decode.field(0, decode.int)
      use minimum_headroom_ms <- decode.field(1, decode.float)
      use negative_samples <- decode.field(2, decode.int)
      use overlap_samples <- decode.field(3, decode.int)
      use renewals_during_slow_ack <- decode.field(4, decode.int)
      decode.success(LeaseEvidence(
        attempts:,
        minimum_headroom_ms:,
        negative_samples:,
        overlap_samples:,
        renewals_during_slow_ack:,
      ))
    })
  use returned <- result.try(harness_db.execute(query, ledger))
  let assert [evidence] = returned.rows
  Ok(evidence)
}

/// nextval is not rolled back: this proves the injected slow ACK executed
/// even when its acknowledgement transaction times out or rolls back.
pub fn slow_ack_activations(
  ledger: pog.Connection,
) -> Result(Int, pog.QueryError) {
  let query =
    pog.query(
      "SELECT CASE WHEN is_called THEN last_value + 1 ELSE 0 END FROM grind_bench.bench_slow_ack_activations",
    )
    |> pog.returning({
      use value <- decode.field(0, decode.int)
      decode.success(value)
    })
  use returned <- result.try(harness_db.execute(query, ledger))
  let assert [count] = returned.rows
  Ok(count)
}
