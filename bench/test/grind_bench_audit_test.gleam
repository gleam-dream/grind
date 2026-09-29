//// Mutation-discipline proof for every invariant `grind_bench/audit`
//// implements: each test seeds a deliberate violation, shows the matching
//// check function turns red, then shows it green once the violation is
//// undone (or, for the pure counter-based checks, on a clean zero count).
////
//// Item 4's "mutation tests that exercise the real wiring" live at the
//// bottom of this file: they call `grind_bench/load.attach_audit_observers`
//// (the exact function a real scenario calls) and emit the real
//// `sinal`/`grind/observation` events through it, rather than calling the
//// counter-threshold check functions directly with a hand-picked count.
////
//// DB-backed tests skip (rather than fail) when `GRIND_BENCH_TEST_DATABASE_URL`
//// is unset, matching every DB-gated test elsewhere in this codebase
//// (`grind/test/grind_test.gleam`, `consumer/test/grind_consumer_test.gleam`);
//// `scripts/bench-postgres.sh` sets it and greps the marker file so a
//// silently-skipped run cannot be mistaken for a passing one.

import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleeunit/should
import grind/job
import grind/observation
import grind/postgres
import grind/worker
import grind_bench
import grind_bench/audit
import grind_bench/load
import pog
import sinal
import sinal/forwarder

@external(erlang, "bench_test_env", "database_url")
fn database_url() -> Result(String, Nil)

@external(erlang, "bench_test_env", "mark")
fn mark(name: String) -> Nil

fn echo_worker(id: String) -> worker.Worker(Int, Int, Nil) {
  let assert Ok(input) = worker.codec(id <> "-input-v1", json.int, decode.int)
  let assert Ok(output) = worker.codec(id <> "-output-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define(id, "v1", input, output, fn(value) { Ok(value) })
  definition
}

/// Admits one real job (never processed -- no consumer runs in this test
/// file) and returns its `job_id`. Used as the seed data every DB-backed
/// check below mutates directly with raw SQL.
fn admit_job(database: postgres.Database, id_suffix: String) -> Int {
  let worker_def = echo_worker("bench-audit-" <> id_suffix)
  let assert Ok(handle) =
    postgres.submit(database, "bench-audit-guard", worker_def, 1)
  job.id_value(handle)
}

/// Records `job_id` as its own `bench_index` too -- every test in this file
/// uses `job_id` directly as the correlation key, matching
/// `check_all_succeeded_with_expected_output`'s own contract (output must
/// equal `bench_index`).
fn record_submission(ledger: pog.Connection, job_id: Int) -> Nil {
  let query =
    pog.query(
      "INSERT INTO bench_submissions (bench_index, job_id, queue) VALUES ($1, $2, 'bench-audit-guard')",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.int(job_id))
  let assert Ok(_) = pog.execute(query, ledger)
  Nil
}

fn raw_exec(connection: pog.Connection, sql: String) -> Nil {
  let assert Ok(_) = pog.execute(pog.query(sql), connection)
  Nil
}

fn set_state(grind: pog.Connection, job_id: Int, state: String) -> Nil {
  raw_exec(
    grind,
    "UPDATE grind_jobs SET state = '"
      <> state
      <> "' WHERE id = "
      <> int_str(job_id),
  )
}

/// Sets a job to a terminal state without touching `output` -- used by tests
/// that only care about ack correlation (I5), not I1b's stricter
/// "succeeded with the expected output" contract.
fn set_terminal(grind: pog.Connection, job_id: Int, state: String) -> Nil {
  raw_exec(
    grind,
    "UPDATE grind_jobs SET state = '"
      <> state
      <> "', finished_at = clock_timestamp() WHERE id = "
      <> int_str(job_id),
  )
}

/// I1b's own healthy-run contract: `succeeded` with `output = bench_index`
/// (here, `job_id`, per `record_submission`'s own convention).
fn set_succeeded_with_output(grind: pog.Connection, job_id: Int) -> Nil {
  raw_exec(
    grind,
    "UPDATE grind_jobs SET state = 'succeeded', output = to_jsonb("
      <> int_str(job_id)
      <> "::bigint), finished_at = clock_timestamp() WHERE id = "
      <> int_str(job_id),
  )
}

fn insert_ack(
  grind: pog.Connection,
  job_id: Int,
  attempt_id: Int,
  committed_state: String,
  command_suffix: String,
) -> Nil {
  raw_exec(
    grind,
    "INSERT INTO grind_job_acknowledgements (command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('bench-guard-ack-"
      <> int_str(job_id)
      <> "-"
      <> command_suffix
      <> "', 'bench-audit-guard', "
      <> int_str(job_id)
      <> ", 'bench-audit', 'v1', "
      <> int_str(attempt_id)
      <> ", 0, 'bench-guard', '"
      <> committed_state
      <> "', decode(repeat('00', 32), 'hex'))",
  )
}

fn insert_authorized_replay_resolution(
  grind: pog.Connection,
  job_id: Int,
) -> Nil {
  raw_exec(
    grind,
    "INSERT INTO grind_job_resolutions (queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ('bench-audit-guard', "
      <> int_str(job_id)
      <> ", 'bench-audit', 'v1', 'bench-guard-replay-"
      <> int_str(job_id)
      <> "', 1, 0, 'bench-guard', clock_timestamp() + interval '1 minute', 'authorize_replay', 'queued', 'bench-guard', 'test-seeded')",
  )
}

fn int_str(value: Int) -> String {
  int.to_string(value)
}

/// A raw pog connection against Grind's own configured schema -- used only
/// by this test file to seed/undo violations directly, never by production
/// bench code (which only ever writes through `postgres.submit`/`preload`
/// or reads through `grind_bench/audit`'s own read-only queries).
fn grind_raw_connection(database: postgres.Database) -> pog.Connection {
  postgres.connection(database)
}

type TestHarness {
  TestHarness(database: postgres.Database, ledger: pog.Connection)
}

/// Every test using this harness closes `database` via `exception.defer`
/// (see each call site) so pools do not accumulate across this module's own
/// tests and exhaust the disposable cluster's `max_connections`, and drops
/// its own fresh Grind schema (item 1) so repeated `gleam test` runs against
/// a long-lived disposable database never accumulate `bench_jobs_*` schemas
/// forever. Also resets the ledger (`bench_submissions`/`bench_effects`) so
/// one test's own seeded rows can never be picked up by another test's
/// audit query against the same shared `GRIND_BENCH_TEST_DATABASE_URL`
/// database.
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

// -- I1a: MissingJobRows -----------------------------------------------------

pub fn i1a_missing_job_rows_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i1a(url)
  }
}

fn run_i1a(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let bogus_job_id = 999_999_001

  // Seed: a ledger submission pointing at a job_id that was never inserted.
  record_submission(ledger, bogus_job_id)
  let assert Ok(Error(audit.MissingJobRows(bad_indices))) =
    audit.check_no_missing_jobs(ledger, grind_schema)
  bad_indices |> should.equal([bogus_job_id])

  // Undo: remove the dangling ledger row.
  raw_exec(
    ledger,
    "DELETE FROM bench_submissions WHERE bench_index = "
      <> int_str(bogus_job_id),
  )
  audit.check_no_missing_jobs(ledger, grind_schema) |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i1a-red-then-green-passed")
}

// -- Item 2, bullet 3: ExtraJobRows -------------------------------------------

pub fn extra_job_rows_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_extra_job_rows(url)
  }
}

fn run_extra_job_rows(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)

  // Seed: a real grind_jobs row admitted without a matching ledger record --
  // only possible to detect at all because this run's own schema is fresh
  // (item 1): every row in it belongs to this test.
  let job_id = admit_job(database, "extra")
  let assert Ok(Error(audit.ExtraJobRows(extra_ids))) =
    audit.check_no_extra_jobs(ledger, grind_schema)
  extra_ids |> should.equal([job_id])

  // Undo: record the missing ledger row, as `preload_and_track` always does
  // for a real preloaded job.
  record_submission(ledger, job_id)
  audit.check_no_extra_jobs(ledger, grind_schema) |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-extra-job-rows-red-then-green-passed")
}

// -- Item 2, bullet 2: SubmissionCountMismatch --------------------------------

pub fn submission_count_mismatch_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_submission_count_mismatch(url)
  }
}

fn run_submission_count_mismatch(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let job_id = admit_job(database, "count")
  record_submission(ledger, job_id)

  // Seed: the driver believes it submitted 2 jobs; the ledger has 1.
  audit.check_submission_count(ledger, 2)
  |> should.equal(Ok(Error(audit.SubmissionCountMismatch(2, 1))))

  // Undo: the expected count matches reality.
  audit.check_submission_count(ledger, 1) |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-submission-count-red-then-green-passed")
}

// -- I1b: NotSucceededWithExpectedOutput --------------------------------------

pub fn i1b_not_succeeded_with_expected_output_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i1b(url)
  }
}

fn run_i1b(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i1b")
  record_submission(ledger, job_id)

  // Seed: the job is still `queued` -- never processed.
  let assert Ok(Error(audit.NotSucceededWithExpectedOutput(unfinished))) =
    audit.check_all_succeeded_with_expected_output(ledger, grind_schema)
  unfinished |> should.equal([job_id])

  // Still red: `succeeded` with the *wrong* output (not merely "terminal").
  set_terminal(grind_raw_connection(database), job_id, "succeeded")
  let assert Ok(Error(audit.NotSucceededWithExpectedOutput(still_unfinished))) =
    audit.check_all_succeeded_with_expected_output(ledger, grind_schema)
  still_unfinished |> should.equal([job_id])

  // Undo: succeeded with the expected output, as a real healthy drain would
  // leave it.
  set_succeeded_with_output(grind_raw_connection(database), job_id)
  audit.check_all_succeeded_with_expected_output(ledger, grind_schema)
  |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i1b-red-then-green-passed")
}

// -- I2: EffectCountMismatch --------------------------------------------------

pub fn i2_effect_count_mismatch_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i2(url)
  }
}

fn run_i2(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i2")
  record_submission(ledger, job_id)

  // Seed: zero effects recorded for a job that (per the ledger) was
  // submitted -- a missing handler invocation.
  let assert Ok(Error(audit.EffectCountMismatch(bad_jobs))) =
    audit.check_effect_counts(ledger, grind_schema)
  bad_jobs |> should.equal([job_id])

  // Undo: record exactly one effect, as one ordinary delivery would.
  raw_exec(
    ledger,
    "INSERT INTO bench_effects (bench_index, delivery_count, node) VALUES ("
      <> int_str(job_id)
      <> ", 1, 'test')",
  )
  audit.check_effect_counts(ledger, grind_schema) |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i2-red-then-green-passed")
}

/// Item 4: I2 duplicate effects -- two delivered effects for a job the
/// ledger never authorized a replay for.
pub fn i2_duplicate_effects_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i2_duplicate_effects(url)
  }
}

fn run_i2_duplicate_effects(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i2dup")
  record_submission(ledger, job_id)
  raw_exec(
    ledger,
    "INSERT INTO bench_effects (bench_index, delivery_count, node) VALUES ("
      <> int_str(job_id)
      <> ", 1, 'test')",
  )

  // Seed: a second, unauthorized delivery.
  raw_exec(
    ledger,
    "INSERT INTO bench_effects (bench_index, delivery_count, node) VALUES ("
      <> int_str(job_id)
      <> ", 2, 'test')",
  )
  let assert Ok(Error(audit.EffectCountMismatch(bad_jobs))) =
    audit.check_effect_counts(ledger, grind_schema)
  bad_jobs |> should.equal([job_id])

  // Undo: remove the extra delivery.
  raw_exec(
    ledger,
    "DELETE FROM bench_effects WHERE bench_index = "
      <> int_str(job_id)
      <> " AND delivery_count = 2",
  )
  audit.check_effect_counts(ledger, grind_schema) |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i2-duplicate-effects-red-then-green-passed")
}

/// Item 4: I2 with an authorized replay -- exactly 2 effects (`1 +
/// authorized_replays`) is the *healthy* outcome once Grind's own
/// `grind_job_resolutions` recorded an `authorize_replay` decision for the
/// job.
pub fn i2_authorized_replay_expects_two_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i2_authorized_replay(url)
  }
}

fn run_i2_authorized_replay(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i2replay")
  record_submission(ledger, job_id)
  insert_authorized_replay_resolution(grind_raw_connection(database), job_id)
  raw_exec(
    ledger,
    "INSERT INTO bench_effects (bench_index, delivery_count, node) VALUES ("
      <> int_str(job_id)
      <> ", 1, 'test')",
  )

  // Seed: only 1 effect recorded, but 1 authorized replay means 2 are
  // expected.
  let assert Ok(Error(audit.EffectCountMismatch(bad_jobs))) =
    audit.check_effect_counts(ledger, grind_schema)
  bad_jobs |> should.equal([job_id])

  // Undo: the replay's own second delivery is recorded too.
  raw_exec(
    ledger,
    "INSERT INTO bench_effects (bench_index, delivery_count, node) VALUES ("
      <> int_str(job_id)
      <> ", 2, 'test')",
  )
  audit.check_effect_counts(ledger, grind_schema) |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i2-authorized-replay-red-then-green-passed")
}

// -- I3 (first half): UncertainJobsPresent -----------------------------------

pub fn i3_uncertain_jobs_present_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i3(url)
  }
}

fn run_i3(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i3")
  record_submission(ledger, job_id)

  // Seed: the job is uncertain -- a healthy run should never see this.
  set_state(grind_raw_connection(database), job_id, "uncertain")
  audit.check_no_uncertain(ledger, grind_schema)
  |> should.equal(Ok(Error(audit.UncertainJobsPresent(1))))

  // Undo: back to queued.
  set_state(grind_raw_connection(database), job_id, "queued")
  audit.check_no_uncertain(ledger, grind_schema) |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i3-red-then-green-passed")
}

// -- I6 (counter half) / I7: pure counter checks ------------------------------
//
// No database needed -- these are plain threshold functions over a
// caller-supplied count. Seeding the violation is passing a nonzero count;
// undoing it is passing zero, exactly what a real healthy run's own zeroed
// counters would report. The *real wiring* that produces these counts is
// proven separately, below, through `grind_bench/load.attach_audit_observers`.

pub fn i3_quarantine_observed_red_then_green_test() {
  audit.check_no_quarantine_observed(1)
  |> should.equal(Error(audit.QuarantineObserved(1)))
  audit.check_no_quarantine_observed(0) |> should.equal(Ok(Nil))
  mark("bench-audit-i3-quarantine-red-then-green-passed")
}

pub fn i6_ledger_write_errors_red_then_green_test() {
  audit.check_no_ledger_write_errors(3)
  |> should.equal(Error(audit.LedgerWriteErrorsObserved(3)))
  audit.check_no_ledger_write_errors(0) |> should.equal(Ok(Nil))
  mark("bench-audit-i6-red-then-green-passed")
}

pub fn i7_forwarder_drops_red_then_green_test() {
  audit.check_no_forwarder_drops(2)
  |> should.equal(Error(audit.ForwarderDropsObserved(2)))
  audit.check_no_forwarder_drops(0) |> should.equal(Ok(Nil))
  mark("bench-audit-i7-red-then-green-passed")
}

// -- I6: postgres log scan -----------------------------------------------------

pub fn i6_postgres_log_scan_red_then_green_test() {
  let clean_window = [
    "2026-01-01 00:00:00 UTC LOG:  statement: SELECT 1",
    "2026-01-01 00:00:00 UTC LOG:  duration: 0.123 ms",
  ]
  audit.check_postgres_log(clean_window) |> should.equal(Ok(Nil))

  // Seed: a deadlock line and a lock-wait line, both PostgreSQL's own
  // wording (`log_lock_waits=on`/the deadlock detector).
  let dirty_window = [
    "2026-01-01 00:00:01 UTC ERROR:  deadlock detected",
    "2026-01-01 00:00:02 UTC LOG:  process 123 still waiting for ShareLock on transaction 456",
  ]
  let assert Error(audit.PostgresLogAnomaliesObserved(lines)) =
    audit.check_postgres_log(dirty_window)
  list.length(lines) |> should.equal(2)

  // Undo: the same window, minus the two anomalous lines.
  audit.check_postgres_log(clean_window) |> should.equal(Ok(Nil))

  mark("bench-audit-i6-log-scan-red-then-green-passed")
}

// -- I4: ExecutingAfterDrain --------------------------------------------------

pub fn i4_executing_after_drain_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i4(url)
  }
}

fn run_i4(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i4")
  record_submission(ledger, job_id)

  // Seed: the job is still executing -- a real drain must never leave this.
  set_state(grind_raw_connection(database), job_id, "executing")
  audit.check_no_executing_after_drain(ledger, grind_schema)
  |> should.equal(Ok(Error(audit.ExecutingAfterDrain(1))))

  // Undo: back to queued.
  set_state(grind_raw_connection(database), job_id, "queued")
  audit.check_no_executing_after_drain(ledger, grind_schema)
  |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i4-red-then-green-passed")
}

// -- I5, direction A: TerminalJobsMissingAck ----------------------------------

pub fn i5_terminal_jobs_missing_ack_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i5_missing_ack(url)
  }
}

fn run_i5_missing_ack(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i5missing")
  record_submission(ledger, job_id)
  set_terminal(grind_raw_connection(database), job_id, "succeeded")

  // Seed: terminal job, zero acknowledgement receipts (pruning is off, so
  // this should never happen for a real committed terminal job).
  let assert Ok(Error(audit.TerminalJobsMissingAck(missing))) =
    audit.check_terminal_jobs_have_ack(ledger, grind_schema)
  missing |> should.equal([job_id])

  // Undo: record the matching acknowledgement receipt a real commit would
  // have written. `decode(repeat('00', 32), 'hex')` is a plain 32-byte
  // zero-filled bytea satisfying `proposal_sha256`'s own length check --
  // pgcrypto is not required for this literal.
  insert_ack(grind_raw_connection(database), job_id, 1, "succeeded", "a")
  audit.check_terminal_jobs_have_ack(ledger, grind_schema)
  |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i5-missing-ack-red-then-green-passed")
}

/// Item 4: I5 "ack-without-terminal" -- a committed, terminal-state
/// acknowledgement exists for a job that is not (yet, or no longer) itself
/// terminal.
pub fn i5_ack_without_terminal_job_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i5_ack_without_terminal(url)
  }
}

fn run_i5_ack_without_terminal(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i5orphan")
  record_submission(ledger, job_id)

  // Seed: a committed terminal ack for a job that is still `queued` -- never
  // possible on a real ack-then-commit path, but exactly the shape a stale
  // or misattributed ack row would have.
  insert_ack(grind_raw_connection(database), job_id, 1, "succeeded", "a")
  let assert Ok(Error(audit.AckWithoutTerminalJob(orphaned))) =
    audit.check_acks_have_terminal_job(ledger, grind_schema)
  orphaned |> should.equal([job_id])

  // Undo: the job actually reaches the acknowledged terminal state.
  set_terminal(grind_raw_connection(database), job_id, "succeeded")
  audit.check_acks_have_terminal_job(ledger, grind_schema)
  |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i5-ack-without-terminal-red-then-green-passed")
}

/// Item 4: I5 "duplicate ack" -- more than one committed terminal ack for
/// the same job.
pub fn i5_duplicate_terminal_ack_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i5_duplicate_ack(url)
  }
}

fn run_i5_duplicate_ack(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i5dup")
  record_submission(ledger, job_id)
  set_terminal(grind_raw_connection(database), job_id, "succeeded")
  insert_ack(grind_raw_connection(database), job_id, 1, "succeeded", "a")

  // Seed: a second committed terminal ack for the same job (a different
  // attempt, so the unique constraint on (job_id, attempt_id, attempt_epoch)
  // still allows it -- exactly the shape a duplicated commit would have).
  insert_ack(grind_raw_connection(database), job_id, 2, "succeeded", "b")
  let assert Ok(Error(audit.DuplicateTerminalAck(duplicated))) =
    audit.check_at_most_one_terminal_ack(ledger, grind_schema)
  duplicated |> should.equal([job_id])

  // Undo: remove the duplicate.
  raw_exec(
    grind_raw_connection(database),
    "DELETE FROM grind_job_acknowledgements WHERE job_id = "
      <> int_str(job_id)
      <> " AND attempt_id = 2",
  )
  audit.check_at_most_one_terminal_ack(ledger, grind_schema)
  |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i5-duplicate-ack-red-then-green-passed")
}

/// Item 4: I5 "state mismatch" -- the job's committed terminal ack disagrees
/// with the job's own current state.
pub fn i5_ack_state_mismatch_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_i5_state_mismatch(url)
  }
}

fn run_i5_state_mismatch(url: String) -> Nil {
  let harness = setup(url)
  let TestHarness(database:, ledger:) = harness
  use <- exception.defer(fn() { teardown(database, ledger) })
  let grind_schema = schema_of(database)
  let job_id = admit_job(database, "i5mismatch")
  record_submission(ledger, job_id)
  set_terminal(grind_raw_connection(database), job_id, "succeeded")

  // Seed: the ack claims a different terminal outcome than the job's own
  // current state.
  insert_ack(grind_raw_connection(database), job_id, 1, "business_failed", "a")
  let assert Ok(Error(audit.AckStateMismatch(mismatched))) =
    audit.check_ack_state_matches_job(ledger, grind_schema)
  mismatched |> should.equal([job_id])

  // Undo: the ack agrees with the job's own current state.
  raw_exec(
    grind_raw_connection(database),
    "UPDATE grind_job_acknowledgements SET committed_state = 'succeeded' WHERE job_id = "
      <> int_str(job_id),
  )
  audit.check_ack_state_matches_job(ledger, grind_schema)
  |> should.equal(Ok(Ok(Nil)))

  mark("bench-audit-i5-state-mismatch-red-then-green-passed")
}

// -- Item 4: real observer wiring ---------------------------------------------
//
// Proves `grind_bench/load.attach_audit_observers` -- the exact function a
// real scenario calls -- actually wires `grind_bench_counter_ffi`'s own
// counters to the real `sinal`/`grind/observation` events, by emitting those
// events for real through `sinal.emit` rather than calling the
// counter-threshold check functions directly.

/// One test, one `attach_audit_observers` call: `sinal` handlers are never
/// detached (matching production -- see that function's own doc comment),
/// so attaching twice within this one long-lived `gleam test` process would
/// register two live handlers for the same event, and a single `sinal.emit`
/// would then bump the counter twice, not once. Both counters are proven
/// through the one attachment instead.
pub fn quarantine_and_forwarder_drop_counters_bumped_by_real_events_test() {
  load.attach_audit_observers("mutation-wiring")
  let quarantine_before = load.counter_value(load.quarantine_counter)
  let dropped_before = load.counter_value(load.forwarder_drop_counter)

  let assert Ok(Nil) =
    sinal.emit(
      observation.quarantined(),
      observation.QuarantinedMeasurements(count: 1),
      observation.QuarantinedMetadata(
        ref: observation.JobRef(
          job_id: 1,
          queue: "bench-audit-guard",
          worker_id: "bench-audit-wiring",
          worker_version: "v1",
        ),
        attempt: observation.AttemptRef(attempt_id: 1, epoch: 0, attempt: 1),
        cancellation_was_requested: False,
      ),
    )
  load.counter_value(load.quarantine_counter)
  |> should.equal(quarantine_before + 1)

  let assert Ok(Nil) =
    sinal.emit(
      forwarder.dropped_event(),
      forwarder.Dropped(rejected: 1, lost: 0, unavailable: 0),
      forwarder.DroppedMetadata(forwarder: "bench-audit-wiring"),
    )
  load.counter_value(load.forwarder_drop_counter)
  |> should.equal(dropped_before + 1)

  mark("bench-audit-quarantine-real-wiring-passed")
  mark("bench-audit-forwarder-dropped-real-wiring-passed")
}

/// The T2 audit must not turn incomplete jobs or mismatched receipts into
/// a healthy result merely because their effects were written once.
pub fn t2_final_classification_red_then_green_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let TestHarness(database:, ledger:) = setup(url)
      use <- exception.defer(fn() { teardown(database, ledger) })
      let schema = schema_of(database)
      let connection = postgres.connection(database)
      let healthy = admit_job(database, "t2-healthy")
      let slow = admit_job(database, "t2-slow")
      record_submission(ledger, healthy)
      record_submission(ledger, slow)
      let assert Ok([a, b]) = audit.fault_completions(ledger, schema, [slow])
      a.valid |> should.be_false
      b.valid |> should.be_false
      raw_exec(
        connection,
        "UPDATE grind_jobs SET worker_id='bench-audit', worker_version='v1', attempt_id=1, attempt_epoch=0 WHERE id IN ("
          <> int_str(healthy)
          <> ","
          <> int_str(slow)
          <> ")",
      )
      set_succeeded_with_output(connection, healthy)
      set_state(connection, slow, "uncertain")
      // Missing healthy receipt remains invalid, while expected uncertainty
      // is classified rather than excluded from the denominator.
      let assert Ok([a, b]) = audit.fault_completions(ledger, schema, [slow])
      a.valid |> should.be_false
      b.valid |> should.be_true
      insert_ack(connection, healthy, 1, "succeeded", "t2")
      let assert Ok([a, b]) = audit.fault_completions(ledger, schema, [slow])
      a.valid |> should.be_true
      b.valid |> should.be_true
      // A stale receipt for an uncertain target is inconsistent.
      insert_ack(connection, slow, 1, "succeeded", "t2")
      let assert Ok([_, b]) = audit.fault_completions(ledger, schema, [slow])
      b.valid |> should.be_false
      raw_exec(
        connection,
        "DELETE FROM grind_job_acknowledgements WHERE job_id=" <> int_str(slow),
      )
      // Healthy uncertainty is never acceptable, even in the fault suite.
      let assert Ok([_, b]) = audit.fault_completions(ledger, schema, [])
      b.valid |> should.be_false
      set_state(connection, slow, "executing")
      let assert Ok([_, b]) = audit.fault_completions(ledger, schema, [slow])
      b.valid |> should.be_false
      mark("bench-audit-t2-final-classification-passed")
    }
  }
}
