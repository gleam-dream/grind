//// Red/green proof for `grind_bench/instrumentation`'s two L6 triggers:
//// each test shows the mechanism absent (or untargeted) first (red -- the
//// effect it claims to produce does not happen), then installs it and
//// shows the effect happen exactly as claimed (green). Both tests act
//// directly on `grind_jobs`/`grind_job_acknowledgements` rows inserted by
//// raw SQL (never through a real consumer): the trigger mechanism itself
//// is what is under test here, not real renewal/ack timing, which L6's own
//// scenarios (`grind_bench/load`) exercise against a real running queue.
////
//// DB-backed, skips (rather than fails) when `GRIND_BENCH_TEST_DATABASE_URL`
//// is unset, matching every other DB-gated test in this project (see
//// `grind_bench_audit_test.gleam`'s own doc comment).

import exception
import gleam/dynamic/decode
import gleam/int
import gleeunit/should
import grind/job
import grind/postgres
import grind_bench
import grind_bench/instrumentation
import pog

@external(erlang, "bench_test_env", "database_url")
fn database_url() -> Result(String, Nil)

@external(erlang, "bench_test_env", "mark")
fn mark(name: String) -> Nil

@external(erlang, "grind_bench_ffi", "monotonic_ms")
fn monotonic_ms() -> Int

type TestHarness {
  TestHarness(database: postgres.Database, ledger: pog.Connection)
}

fn setup(url: String) -> TestHarness {
  let config =
    grind_bench.default_config(url) |> grind_bench.with_grind_pool_size(2)
  let assert Ok(ledger) = grind_bench.start_ledger_pool(config)
  let assert Ok(Nil) = grind_bench.drop_stale_bench_schemas(ledger)
  let assert Ok(settings) =
    grind_bench.grind_settings(config) |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(Nil) = grind_bench.reset_ledger(ledger)
  TestHarness(database:, ledger:)
}

fn schema_of(database: postgres.Database) -> String {
  job.installation_schema(postgres.installation(database))
}

fn teardown(database: postgres.Database, ledger: pog.Connection) -> Nil {
  let grind_schema = schema_of(database)
  let _ = postgres.close(database)
  let assert Ok(Nil) = grind_bench.drop_schema(ledger, grind_schema)
  Nil
}

fn raw_exec(connection: pog.Connection, sql: String) -> Nil {
  let assert Ok(_) = pog.execute(pog.query(sql), connection)
  Nil
}

fn int_str(value: Int) -> String {
  int.to_string(value)
}

/// Inserts one `grind_jobs` row directly, already `executing`, with a
/// caller-chosen `attempt_id`/`lease_expires_at` -- the lease-log trigger's
/// own target shape. Returns the new `id`.
fn insert_executing_job(connection: pog.Connection, attempt_id: Int) -> Int {
  let sql =
    "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, max_attempts, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at) "
    <> "VALUES ('bench-instrumentation', 'bench-instrumentation-worker', 'v1', 'v1', '{}'::jsonb, 'v1', 20, 'executing', clock_timestamp(), "
    <> int_str(attempt_id)
    <> ", 0, 'test-owner', clock_timestamp() + interval '30 seconds') RETURNING id"
  let query =
    pog.query(sql)
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  let assert Ok(pog.Returned(rows: [id], ..)) = pog.execute(query, connection)
  id
}

pub fn lease_log_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_lease_log(url)
  }
}

fn run_lease_log(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let connection = postgres.connection(database)

  let job_id = insert_executing_job(connection, 1)

  // Red: no trigger installed yet -- an ordinary lease-renewing UPDATE
  // produces no `bench_lease_log` row at all.
  raw_exec(
    connection,
    "UPDATE grind_jobs SET lease_expires_at = lease_expires_at + interval '5 seconds' WHERE id = "
      <> int_str(job_id),
  )
  let assert Ok([]) = instrumentation.renewal_headroom_ms(ledger, [job_id])

  // Green: install the trigger, do the identical shape of UPDATE again --
  // exactly one renewal row appears, with a positive headroom (the old
  // lease still had time left when the trigger observed it).
  let assert Ok(Nil) =
    instrumentation.install_lease_log(connection, grind_schema)
  raw_exec(
    connection,
    "UPDATE grind_jobs SET lease_expires_at = lease_expires_at + interval '5 seconds' WHERE id = "
      <> int_str(job_id),
  )
  let assert Ok([headroom]) =
    instrumentation.renewal_headroom_ms(ledger, [job_id])
  { headroom >. 0.0 } |> should.be_true

  // A transition into `uncertain` is logged as a quarantine, not a
  // renewal -- proves the two are distinguished, not just "any UPDATE".
  raw_exec(
    connection,
    "UPDATE grind_jobs SET state = 'uncertain' WHERE id = " <> int_str(job_id),
  )
  let assert Ok(1) =
    instrumentation.quarantine_transition_count(ledger, [job_id])
  let assert Ok([_]) = instrumentation.renewal_headroom_ms(ledger, [job_id])

  let assert Ok(Nil) = instrumentation.drop_lease_log(connection, grind_schema)
  mark("bench-instrumentation-lease-log-red-then-green-passed")
}

/// A minimal acknowledgement row satisfying every `NOT NULL`/`CHECK`
/// column, distinguished only by `command_id`/`job_id`/`attempt_epoch`.
fn insert_ack(
  connection: pog.Connection,
  command_id: String,
  job_id: Int,
  attempt_epoch: Int,
) -> Int {
  let sql =
    "INSERT INTO grind_job_acknowledgements (command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) "
    <> "VALUES ('"
    <> command_id
    <> "', 'bench-instrumentation', "
    <> int_str(job_id)
    <> ", 'bench-instrumentation-worker', 'v1', 1, "
    <> int_str(attempt_epoch)
    <> ", 'test-owner', 'succeeded', decode(repeat('ab', 32), 'hex'))"
  let before = monotonic_ms()
  raw_exec(connection, sql)
  monotonic_ms() - before
}

pub fn slow_ack_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_slow_ack(url)
  }
}

fn run_slow_ack(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let connection = postgres.connection(database)

  let job_a = insert_executing_job(connection, 10)
  let job_b = insert_executing_job(connection, 11)

  // Red: no trigger installed yet -- an ordinary ack insert for the job
  // that will later be marked "slow" is fast.
  let baseline_ms = insert_ack(connection, "cmd-a0", job_a, 0)
  { baseline_ms < 200 } |> should.be_true

  // Green: install a 300ms slow-ack trigger and mark only job_a -- its own
  // next ack insert (a different attempt_epoch, since
  // (job_id, attempt_id, attempt_epoch) is unique) takes at least 300ms...
  let assert Ok(Nil) =
    instrumentation.install_slow_ack(connection, grind_schema, 300)
  let assert Ok(Nil) = instrumentation.mark_slow_ack_targets(ledger, [job_a])
  let marked_ms = insert_ack(connection, "cmd-a1", job_a, 1)
  { marked_ms >= 300 } |> should.be_true

  // ...while job_b, never marked, stays fast even with the trigger
  // installed -- proving the delay is targeted, not global.
  let unmarked_ms = insert_ack(connection, "cmd-b0", job_b, 0)
  { unmarked_ms < 200 } |> should.be_true

  let assert Ok(Nil) = instrumentation.drop_slow_ack(connection, grind_schema)
  let assert Ok(Nil) = instrumentation.clear_slow_ack_targets(ledger)
  mark("bench-instrumentation-slow-ack-red-then-green-passed")
}
