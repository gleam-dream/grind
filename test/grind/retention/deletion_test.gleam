import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/result
import gleeunit/should
import grind/internal/job
import grind/internal/observation
import grind/internal/postgres
import grind/internal/submission
import grind/internal/unique
import grind/internal/worker
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt, spawn_lock_holder,
  spawn_submit, unique_test_lock_key,
}
import grind/support/env.{
  mark_database_test_executed, prune_owner_b_url, prune_url,
}
import grind/support/observers.{detach}
import grind/support/retention_rows.{
  job_row_exists, seed_acknowledgement_receipt, seed_nonterminal_job,
  seed_resolution_receipt, seed_terminal_job, seed_unique_submission_receipt,
}
import grind/support/submissions.{submit_keep_existing, unique_test_worker}
import pog
import sinal

/// Pure validation runs before any query reaches the database at all: every
/// invalid-argument case below is checked against a pool this block closes
/// immediately after opening, the same "closed pool proves purity" pattern
/// `run_submit_unique_pre_storage_rejection_test` already uses.
pub fn postgres_prune_finished_validates_arguments_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_prune_validation_test(database_url)
  }
}

fn run_prune_validation_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let _ = postgres.close(database)

  postgres.prune_finished(database, older_than_ms: 0, limit: 100)
  |> should.equal(Error(postgres.NonPositiveRetention))
  postgres.prune_finished(database, older_than_ms: -1, limit: 100)
  |> should.equal(Error(postgres.NonPositiveRetention))
  postgres.prune_finished(
    database,
    older_than_ms: worker.retry_delay_maximum_milliseconds() + 1,
    limit: 100,
  )
  |> should.equal(Error(postgres.RetentionAbovePrecisionBound))
  postgres.prune_finished(database, older_than_ms: 1000, limit: 0)
  |> should.equal(Error(postgres.NonPositivePruneLimit))
  postgres.prune_finished(database, older_than_ms: 1000, limit: -1)
  |> should.equal(Error(postgres.NonPositivePruneLimit))
  postgres.prune_finished(
    database,
    older_than_ms: 1000,
    limit: postgres.prune_limit_maximum() + 1,
  )
  |> should.equal(Error(postgres.PruneLimitTooLarge))
  mark_database_test_executed("prune-finished-validates-arguments")
}

pub fn postgres_prune_finished_deletes_old_terminal_rows_test() {
  case prune_url(), prune_owner_b_url() {
    Ok(database_url), Ok(owner_b_url) ->
      run_prune_finished_test(database_url, owner_b_url)
    _, _ -> Nil
  }
}

fn run_prune_finished_test(database_url: String, owner_b_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  let assert Ok(owner_b_validated) =
    postgres.settings(owner_b_url) |> postgres.validate
  let assert Ok(owner_b_database) = postgres.start(owner_b_validated)
  use <- exception.defer(fn() { postgres.close(owner_b_database) })
  let assert Ok(Nil) = postgres.migrate(owner_b_database)
  let owner_b_connection = postgres.connection(owner_b_database)

  // -- Every terminal state, old enough to prune, one carrying real
  // -- receipts of every kind (proving the cascade counts) -----------------
  let old_succeeded =
    seed_terminal_job(
      connection,
      "prune-old",
      "prune.worker",
      "succeeded",
      3_600_000,
    )
  seed_acknowledgement_receipt(
    connection,
    "prune-old",
    old_succeeded,
    "prune.worker",
    "prune-cascade-command",
  )
  seed_unique_submission_receipt(
    connection,
    "prune-old",
    old_succeeded,
    "prune.worker",
    "prune-cascade-submission",
  )
  seed_resolution_receipt(
    connection,
    "prune-old",
    old_succeeded,
    "prune.worker",
    "prune-cascade-resolution",
  )
  let old_business_failed =
    seed_terminal_job(
      connection,
      "prune-old",
      "prune.worker",
      "business_failed",
      3_600_000,
    )
  let old_runtime_failed =
    seed_terminal_job(
      connection,
      "prune-old",
      "prune.worker",
      "runtime_failed",
      3_600_000,
    )
  let old_contract_mismatch =
    seed_terminal_job(
      connection,
      "prune-old",
      "prune.worker",
      "contract_mismatch",
      3_600_000,
    )
  let old_discarded =
    seed_terminal_job(
      connection,
      "prune-old",
      "prune.worker",
      "discarded",
      3_600_000,
    )
  let old_cancelled =
    seed_terminal_job(
      connection,
      "prune-old",
      "prune.worker",
      "cancelled",
      3_600_000,
    )
  let old_terminal_ids = [
    old_succeeded,
    old_business_failed,
    old_runtime_failed,
    old_contract_mismatch,
    old_discarded,
    old_cancelled,
  ]

  // Every terminal state again, but not old enough (100ms, under the 1000ms
  // retention this test prunes with below).
  let young_terminal_ids =
    list.map(
      [
        "succeeded", "business_failed", "runtime_failed", "contract_mismatch",
        "discarded", "cancelled",
      ],
      fn(state) {
        seed_terminal_job(connection, "prune-young", "prune.worker", state, 100)
      },
    )

  // Every non-terminal state: `finished_at` is always null, so never
  // prunable regardless of how much wall-clock time passes.
  let nonterminal_ids =
    list.map(
      ["queued", "scheduled", "retryable", "executing", "uncertain"],
      fn(state) {
        seed_nonterminal_job(
          connection,
          "prune-nonterminal",
          "prune.worker",
          state,
        )
      },
    )

  // A row in a completely separate database's own schema: `prune_finished`
  // only ever touches the connection it was called against, never anything
  // reachable only through a different pool.
  let other_owner_id =
    seed_terminal_job(
      owner_b_connection,
      "prune-old",
      "prune.worker",
      "succeeded",
      3_600_000,
    )

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.prune_completed(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(report) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  report.jobs |> should.equal(6)

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.PruneCompletedMeasurements(jobs: 6))
  metadata
  |> should.equal(observation.PruneCompletedMetadata(
    older_than_ms: 1000,
    limit: 100,
  ))

  // The six old terminal rows, and their receipts, are gone.
  list.each(old_terminal_ids, fn(id) {
    job_row_exists(connection, id) |> should.equal(False)
  })
  let assert Ok(remaining_receipts) =
    pog.query(
      "SELECT (SELECT count(*) FROM grind_job_acknowledgements WHERE job_id = $1), (SELECT count(*) FROM grind_unique_submissions WHERE job_id = $1), (SELECT count(*) FROM grind_job_resolutions WHERE job_id = $1)",
    )
    |> pog.parameter(pog.int(old_succeeded))
    |> pog.returning({
      use acknowledgements <- decode.field(0, decode.int)
      use submissions <- decode.field(1, decode.int)
      use resolutions <- decode.field(2, decode.int)
      decode.success(#(acknowledgements, submissions, resolutions))
    })
    |> pog.execute(on: connection)
  remaining_receipts.rows |> should.equal([#(0, 0, 0)])

  // Everything else survives: young terminal rows, every non-terminal
  // state, and the other database's own old terminal row.
  list.each(young_terminal_ids, fn(id) {
    job_row_exists(connection, id) |> should.equal(True)
  })
  list.each(nonterminal_ids, fn(id) {
    job_row_exists(connection, id) |> should.equal(True)
  })
  job_row_exists(owner_b_connection, other_owner_id) |> should.equal(True)

  // -- Batch size and ordering: oldest `finished_at` first ------------------
  let batch_ids =
    list.map([5, 4, 3, 2, 1], fn(hours) {
      seed_terminal_job(
        connection,
        "prune-batch",
        "prune.worker",
        "succeeded",
        hours * 3_600_000,
      )
    })
  let assert Ok(first_batch) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 2)
  first_batch.jobs |> should.equal(2)
  let assert Ok(second_batch) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 2)
  second_batch.jobs |> should.equal(2)
  let assert Ok(third_batch) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 2)
  third_batch.jobs |> should.equal(1)
  let assert Ok(fourth_batch) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 2)
  fourth_batch.jobs |> should.equal(0)
  list.each(batch_ids, fn(id) {
    job_row_exists(connection, id) |> should.equal(False)
  })

  // -- FOR UPDATE SKIP LOCKED: a concurrently locked candidate is skipped,
  // -- not blocked on, and survives until the lock clears -------------------
  let locked_id =
    seed_terminal_job(
      connection,
      "prune-locked",
      "prune.worker",
      "succeeded",
      3_600_000,
    )
  let lock_query =
    pog.query("SELECT id FROM grind_jobs WHERE id = $1 FOR UPDATE")
    |> pog.parameter(pog.int(locked_id))
  let #(lock_ready, lock_finished) = spawn_lock_holder(connection, lock_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })
  let assert Ok(skipped) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  skipped.jobs |> should.equal(0)
  job_row_exists(connection, locked_id) |> should.equal(True)
  process.send(release_lock, ReleaseAttempt)
  let assert Ok(ClaimGateReleased(True)) =
    process.receive(lock_finished, within: 5000)
  let assert Ok(unlocked) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  unlocked.jobs |> should.equal(1)
  job_row_exists(connection, locked_id) |> should.equal(False)

  // -- After a job is pruned: everything about it reports "not found",
  // -- never a different, misleading error -----------------------------------
  let assert Ok(gone_worker) =
    worker.codec("prune-gone-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(gone_output) =
    worker.codec(
      "prune-gone-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(gone_worker_def) =
    worker.define(
      "prune.gone-worker",
      "v1",
      gone_worker,
      gone_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let gone_handle =
    job.new_handle(
      old_succeeded,
      postgres.installation(database),
      "prune-old",
      gone_worker_def,
    )
  postgres.state(database, gone_handle)
  |> should.equal(Error(postgres.JobNotFound))
  postgres.outcome(database, gone_handle)
  |> should.equal(Error(postgres.JobNotFound))
  postgres.bind_handle(database, gone_worker_def, old_succeeded)
  |> should.equal(Error(postgres.JobNotFound))
  postgres.reconcile_acknowledgement(
    database,
    gone_handle,
    "prune-cascade-command",
  )
  |> should.equal(Error(postgres.ReceiptNotFound))

  // -- submit_with_id: idempotency only holds within the retention window --
  let assert Ok(replay_submission) = submission.submission_id("prune-replay")
  let assert Ok(replay_worker) =
    worker.codec(
      "prune-replay-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(replay_output) =
    worker.codec(
      "prune-replay-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(replay_worker_def) =
    worker.define(
      "prune.replay-worker",
      "v1",
      replay_worker,
      replay_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(submission.Inserted(first_replay_handle)) =
    postgres.submit_with_id(
      database,
      "prune-replay",
      replay_submission,
      replay_worker_def,
      41,
      submission.Immediately,
    )
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'succeeded', finished_at = clock_timestamp() - interval '1 hour' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(first_replay_handle)))
    |> pog.execute(on: connection)
  let assert Ok(replay_prune) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  replay_prune.jobs |> should.equal(1)
  let assert Ok(submission.Inserted(second_replay_handle)) =
    postgres.submit_with_id(
      database,
      "prune-replay",
      replay_submission,
      replay_worker_def,
      41,
      submission.Immediately,
    )
  { job.id_value(second_replay_handle) != job.id_value(first_replay_handle) }
  |> should.equal(True)

  // -- Uniqueness: an `AllRetained`/`while_retained()` key is only occupied
  // -- "until pruned", not forever -------------------------------------------
  let assert Ok(reopen_worker) =
    worker.define(
      "prune.reopen-worker",
      "v1",
      replay_worker,
      replay_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let reopen_policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      unique.while_retained(),
      unique.AllRetained,
    )
  let assert Ok(submission.Inserted(reopen_first_handle)) =
    submit_keep_existing(
      database,
      "prune-reopen",
      "prune-reopen-first",
      reopen_worker,
      99,
      reopen_policy,
    )
  let assert Ok(submission.Existing(_)) =
    submit_keep_existing(
      database,
      "prune-reopen",
      "prune-reopen-second",
      reopen_worker,
      99,
      reopen_policy,
    )
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'succeeded', finished_at = clock_timestamp() - interval '1 hour' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(reopen_first_handle)))
    |> pog.execute(on: connection)
  let assert Ok(reopen_prune) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  reopen_prune.jobs |> should.equal(1)
  let assert Ok(submission.Inserted(reopen_third_handle)) =
    submit_keep_existing(
      database,
      "prune-reopen",
      "prune-reopen-third",
      reopen_worker,
      99,
      reopen_policy,
    )
  { job.id_value(reopen_third_handle) != job.id_value(reopen_first_handle) }
  |> should.equal(True)

  // `young_terminal_ids` were deliberately left behind (proving they
  // survive *this* test's own 1000ms retention window) — but `prune_url()`
  // is a database every test in this section shares, and a terminal row
  // seeded "100ms old" keeps aging in real wall-clock time long after this
  // test itself returns. Removed directly (not via `prune_finished`, which
  // would also sweep up anything else already old enough by now) so a
  // later test's own broad `limit` can never mistake it for a row that
  // test itself is supposed to control.
  let assert Ok(_) =
    pog.query("DELETE FROM grind_jobs WHERE queue = 'prune-young'")
    |> pog.execute(on: connection)

  mark_database_test_executed("prune-finished-deletes-old-terminal-rows")
}

/// Installs a `BEFORE INSERT` trigger on `grind_unique_submissions`, scoped
/// to one exact `submission_id`, that blocks on `pg_advisory_xact_lock`
/// before letting that one receipt insert proceed — the same shape
/// `install_unique_insert_barrier` uses against `grind_jobs`, aimed instead
/// at the point in `submit_unique`'s own admission transaction that comes
/// strictly *after* its candidate `SELECT ... FOR KEY SHARE`/`FOR UPDATE`
/// has already run and decided `KeepExisting`, so a concurrent
/// `prune_finished` racing the same candidate row is forced to land in
/// exactly that window.
fn install_admission_receipt_barrier(
  connection: pog.Connection,
  name: String,
  submission_id: String,
  lock_key: Int,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.submission_id = '"
      <> submission_id
      <> "' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> name
      <> " BEFORE INSERT ON grind_unique_submissions FOR EACH ROW EXECUTE FUNCTION "
      <> name
      <> "()",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS " <> name <> " ON grind_unique_submissions",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
}

/// Polls (bounded) until some other backend is genuinely blocked acquiring
/// the barrier's own advisory lock from inside its `INSERT INTO
/// grind_unique_submissions` — proof the admission transaction's own
/// candidate `SELECT` has already run (and, with the fix in place, already
/// holds its `FOR KEY SHARE` lock) and is now paused strictly before its
/// receipt commits, rather than inferring this from timing alone.
fn await_admission_blocked_on_receipt_insert(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Bool {
  let waiting =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND query LIKE 'INSERT INTO grind_unique_submissions%')",
    )
    |> pog.returning({
      use waiting <- decode.field(0, decode.bool)
      decode.success(waiting)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [waiting] -> Ok(waiting)
        _ -> Error(Nil)
      }
    })
  case waiting {
    Ok(True) -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_admission_blocked_on_receipt_insert(
            connection,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

/// The race `candidate_sql`'s `FOR KEY SHARE` (non-reschedule candidates)
/// exists to close: a `submit_unique` admission deciding `KeepExisting`
/// against an old, terminal candidate, forced to overlap with a concurrent
/// `prune_finished` call racing the exact same row. The barrier above pins
/// admission strictly after its own candidate lock is taken (with the fix)
/// and before its own receipt commits, which is exactly the window
/// `prune_finished` is called from — proving `FOR UPDATE SKIP LOCKED` skips
/// this row (rather than deleting out from under a still-open admission)
/// and the row and its receipt both survive together. See
/// `docs/RECOVERY-EVIDENCE.md` for the mutation (`FOR KEY SHARE` removed)
/// that reproduces the opposite: the row deleted while admission's own
/// still-open transaction goes on to commit a receipt that names it,
/// leaving `grind_unique_submissions` an orphaned row pointing at nothing.
pub fn postgres_prune_finished_admission_race_keeps_candidate_and_receipt_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_prune_admission_race_test(database_url)
  }
}

fn run_prune_admission_race_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  let worker_def = unique_test_worker("prune.race-worker")
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.AllRetained,
    )
  let assert Ok(submission.Inserted(original_handle)) =
    submit_keep_existing(
      database,
      "prune-race",
      "prune-race-original",
      worker_def,
      7,
      policy,
    )
  let original_id = job.id_value(original_handle)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'succeeded', finished_at = clock_timestamp() - interval '1 hour' WHERE id = $1",
    )
    |> pog.parameter(pog.int(original_id))
    |> pog.execute(on: connection)

  let lock_key = unique_test_lock_key(100)
  let retry_submission_id = "prune-race-retry"
  let cleanup =
    install_admission_receipt_barrier(
      connection,
      "grind_test_prune_race_barrier",
      retry_submission_id,
      lock_key,
    )
  use <- exception.defer(cleanup)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      connection,
      pog.query(
        "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_prune_race_lock",
      )
        |> pog.parameter(pog.int(lock_key)),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let admission_result = process.new_subject()
  spawn_submit(admission_result, fn() {
    submit_keep_existing(
      database,
      "prune-race",
      retry_submission_id,
      worker_def,
      7,
      policy,
    )
  })

  await_admission_blocked_on_receipt_insert(connection, 250)
  |> should.equal(True)

  // Admission's own candidate lock is already held (with the fix); the
  // concurrent prune below must skip this exact row rather than delete it.
  let assert Ok(race_prune) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  race_prune.jobs |> should.equal(0)
  job_row_exists(connection, original_id) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  let assert Ok(ClaimGateReleased(True)) =
    process.receive(lock_finished, within: 5000)
  let assert Ok(Ok(submission.Existing(conflict))) =
    process.receive(admission_result, within: 5000)
  submission.conflict_job_id(conflict) |> should.equal(original_id)

  // The candidate and its own admission receipt both survived together —
  // no orphan.
  job_row_exists(connection, original_id) |> should.equal(True)
  let assert Ok(receipt_target) =
    pog.query(
      "SELECT job_id FROM grind_unique_submissions WHERE submission_id = $1",
    )
    |> pog.parameter(pog.text(retry_submission_id))
    |> pog.returning({
      use job_id <- decode.field(0, decode.int)
      decode.success(job_id)
    })
    |> pog.execute(on: connection)
  receipt_target.rows |> should.equal([original_id])

  // `original_id` deliberately survived this test (that was the point) —
  // but it is now a terminal, hour-old row left behind in a database this
  // whole section shares. Pruned directly, via the real public API, so a
  // later test's own tightly `LIMIT`ed prune call can never mistake it for
  // that test's own intended candidate.
  let assert Ok(_) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)

  mark_database_test_executed("prune-finished-admission-race-no-orphan")
}

/// Installs a PL/pgSQL function called from inside a `WHERE` clause, one
/// candidate row at a time, that blocks on `pg_advisory_xact_lock` only
/// when it is evaluating `target_id` — used to pause a prune-shaped query
/// mid-scan, after PostgreSQL has already fixed that statement's own
/// snapshot (`READ COMMITTED` takes one snapshot per statement, not per
/// row), but before it reaches and locks one specific candidate. Returns a
/// cleanup thunk for `exception.defer`.
fn install_snapshot_barrier(
  connection: pog.Connection,
  name: String,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "(target_id bigint, candidate_id bigint, lock_key bigint) RETURNS boolean LANGUAGE plpgsql AS $body$ BEGIN IF candidate_id = target_id THEN PERFORM pg_advisory_xact_lock(lock_key); END IF; RETURN true; END $body$",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ =
      pog.query(
        "DROP FUNCTION IF EXISTS " <> name <> "(bigint, bigint, bigint)",
      )
      |> pog.execute(on: connection)
    Nil
  }
}

/// The exact shape `sql/prune_finished.sql` itself selects candidates with,
/// plus one extra `AND` clause calling the snapshot barrier above for one
/// exact row — proving the underlying mechanism (`ON DELETE CASCADE`) reads
/// its own fresh state when a job is deleted, not the deleting statement's
/// own snapshot, since `postgres.prune_finished`'s real, fixed SQL text has
/// no injection point of its own to pause mid-scan from a test.
fn snapshot_barrier_prune_sql(barrier_name: String) -> String {
  "WITH doomed AS (SELECT id FROM grind_jobs WHERE finished_at IS NOT NULL AND state IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled') AND finished_at < statement_timestamp() - ($1::bigint::double precision * interval '1 millisecond') AND "
  <> barrier_name
  <> "($3, id, $4) ORDER BY finished_at, id LIMIT $2 FOR UPDATE SKIP LOCKED) DELETE FROM grind_jobs x USING doomed d WHERE x.id = d.id RETURNING x.id"
}

/// Polls (bounded) until some other backend is genuinely blocked acquiring
/// the snapshot barrier's own advisory lock from inside the `WITH doomed
/// AS (...)` query above, the same `pg_stat_activity` discipline
/// `await_admission_blocked_on_receipt_insert` uses.
fn await_prune_blocked_on_snapshot_barrier(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Bool {
  let waiting =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND query LIKE 'WITH doomed AS (%')",
    )
    |> pog.returning({
      use waiting <- decode.field(0, decode.bool)
      decode.success(waiting)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [waiting] -> Ok(waiting)
        _ -> Error(Nil)
      }
    })
  case waiting {
    Ok(True) -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_prune_blocked_on_snapshot_barrier(
            connection,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

/// The race `grind_v12`'s `ON DELETE CASCADE` foreign keys close: under
/// `READ COMMITTED`, every CTE of one `prune_finished`-shaped statement
/// shares that one statement's own start-of-statement snapshot. A receipt
/// committed by some other writer *after* that snapshot was taken, but
/// *before* the scan actually reaches and locks the row it names, is
/// invisible to that snapshot — an explicit, snapshot-scoped `DELETE ...
/// WHERE job_id = d.id` against the receipt table (the shape this module
/// used before this fix) could never see or delete it, leaving it an
/// orphan once the job itself is deleted. `ON DELETE CASCADE` fires its own
/// fresh query when the row is actually deleted, immune to the deleting
/// statement's own snapshot, so it still finds and removes a receipt
/// committed in exactly that window.
pub fn postgres_prune_finished_cascade_survives_late_committed_receipt_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_prune_cascade_snapshot_race_test(database_url)
  }
}

fn run_prune_cascade_snapshot_race_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  let target_id =
    seed_terminal_job(
      connection,
      "prune-snapshot-race",
      "prune.worker",
      "succeeded",
      3_600_000,
    )

  let barrier_name = "grind_test_prune_snapshot_barrier"
  let cleanup = install_snapshot_barrier(connection, barrier_name)
  use <- exception.defer(cleanup)
  let lock_key = unique_test_lock_key(200)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      connection,
      pog.query(
        "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_prune_snapshot_lock",
      )
        |> pog.parameter(pog.int(lock_key)),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let prune_result = process.new_subject()
  spawn_submit(prune_result, fn() {
    pog.query(snapshot_barrier_prune_sql(barrier_name))
    |> pog.parameter(pog.int(1000))
    // `LIMIT 1`, not a generous batch size: `prune_url()` is a database
    // this whole test module shares, and another test's own deliberately
    // "too young to prune" row can have aged well past 1000ms by the time
    // this one runs. `target_id` is seeded a full hour old
    // (`ORDER BY finished_at, id` sorts it first regardless), so `LIMIT 1`
    // selects only it, never a leftover row that merely aged into
    // eligibility in the meantime.
    |> pog.parameter(pog.int(1))
    |> pog.parameter(pog.int(target_id))
    |> pog.parameter(pog.int(lock_key))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  })

  await_prune_blocked_on_snapshot_barrier(connection, 250)
  |> should.equal(True)

  // Committed strictly after the blocked prune statement's own snapshot,
  // strictly before it reaches and locks `target_id`.
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('prune-snapshot-race-command', 'prune-snapshot-race', $1, 'prune.worker', 'v1', 1, 1, 'prune-test-owner', 'succeeded', sha256(convert_to('prune-snapshot-race-proposal', 'UTF8')))",
    )
    |> pog.parameter(pog.int(target_id))
    |> pog.execute(on: connection)

  process.send(release_lock, ReleaseAttempt)
  let assert Ok(ClaimGateReleased(True)) =
    process.receive(lock_finished, within: 5000)
  let assert Ok(Ok(pruned)) = process.receive(prune_result, within: 5000)
  pruned.rows |> should.equal([target_id])

  job_row_exists(connection, target_id) |> should.equal(False)
  let assert Ok(orphan_check) =
    pog.query(
      "SELECT count(*) = 0 FROM grind_job_acknowledgements WHERE command_id = 'prune-snapshot-race-command'",
    )
    |> pog.returning({
      use no_orphan <- decode.field(0, decode.bool)
      decode.success(no_orphan)
    })
    |> pog.execute(on: connection)
  orphan_check.rows |> should.equal([True])

  mark_database_test_executed("prune-finished-cascade-survives-snapshot-race")
}
