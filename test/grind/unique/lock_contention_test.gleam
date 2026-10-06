import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/list
import gleeunit/should
import grind/internal/job
import grind/internal/postgres
import grind/internal/submission
import grind/internal/unique
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt,
  install_unique_insert_barrier, spawn_lock_holder, spawn_submit,
  unique_test_lock_key,
}
import grind/support/env.{
  database_url, mark_database_test_executed, repeatable_read_url,
}
import grind/support/job_queries.{count_jobs_in_queue}
import grind/support/lock_wait.{
  await_overlap_shape, unique_domain_lock_query, unique_domain_lock_query_like,
  unique_insert_query_like,
}
import grind/support/submissions.{
  submit_keep_existing, unique_receipt_exists, unique_test_worker,
}
import grind/support/unique_fixture.{
  unique_test_suffix, with_unique_database, with_unique_databases,
}
import grind/support/unique_rows.{
  future_available_at, job_available_at_ms, submit_reschedule,
}
import pog

// -- Increment 9: contention -------------------------------------------------
//
// See `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/RECOVERY-EVIDENCE.md`, Increment 9, for this section's evidence.

/// The test itself holds the real domain lock (via `unique_domain_lock_query`,
/// built from the same `@internal lock_key_sql` production code uses) in its
/// own open transaction; a concurrent `submit_unique` with a 200ms
/// `unique_lock_wait` for the same key contends and reports
/// `AdmissionContended`, with no job row and no receipt. Once the lock is
/// released, the same `SubmissionId` succeeds.
pub fn postgres_submit_unique_contended_lock_wait_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_contended_lock_wait_test(database_url)
  }
}

fn run_unique_contended_lock_wait_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.contended-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-contended-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  let assert Ok(holder_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(holder_database) = postgres.start(holder_settings)
  use <- exception.defer(fn() { postgres.close(holder_database) })
  let assert Ok(Nil) = postgres.migrate(holder_database)
  let holder_connection = postgres.connection(holder_database)
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      holder_connection,
      unique_domain_lock_query(holder_database, worker_def, 1),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(submitter_settings) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(200)
    |> postgres.validate
  let assert Ok(submitter_database) = postgres.start(submitter_settings)
  use <- exception.defer(fn() { postgres.close(submitter_database) })

  let submission_text = "unique-contended-1-" <> suffix
  submit_keep_existing(
    submitter_database,
    test_queue,
    submission_text,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.AdmissionContended))
  count_jobs_in_queue(holder_connection, test_queue) |> should.equal(0)
  unique_receipt_exists(holder_connection, submission_text)
  |> should.equal(False)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      submitter_database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  count_jobs_in_queue(holder_connection, test_queue) |> should.equal(1)

  mark_database_test_executed("unique-contended-lock-wait-passed")
}

/// DEFECT 1 (https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/RELEASE-READINESS.md, "Defects found while designing the
/// deadline"): the same contention as above, but with *default*
/// `postgres.settings` on the submitter — no `unique_lock_wait` override.
/// Before the fix, the default `unique_lock_wait_ms` (5000) was equal to
/// pgo's own hardcoded pool checkout deadline (also ~5000 ms — see
/// `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/RECOVERY-EVIDENCE.md`, "Acknowledgement deadline"), so contention
/// could surface as the checkout being force-closed
/// (`NotCommitted(pog.QueryTimeout)`/`CommitUnknown`) instead of the
/// clean, typed `AdmissionContended` a caller can actually branch on — a
/// race, not deterministically wrong every time, which is exactly why it
/// went unnoticed: `postgres_submit_unique_contended_lock_wait_test` above
/// always overrode `unique_lock_wait` to 200 ms and never exercised the
/// default at all. `postgres.validate` now rejects any `Settings` where
/// `unique_lock_wait_ms` is within `unique_lock_wait_margin_ms` (1000 ms) of
/// `statement_deadline_ms` (`UniqueLockWaitTooCloseToDeadline`), and the
/// shipped defaults (`unique_lock_wait_ms` 2000, `statement_deadline_ms`
/// 4000) clear that margin — so this is deterministic today, not a race.
pub fn postgres_submit_unique_contended_lock_wait_default_settings_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_unique_contended_lock_wait_default_settings_test(database_url)
  }
}

fn run_unique_contended_lock_wait_default_settings_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.contended-default-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-contended-default-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  let assert Ok(holder_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(holder_database) = postgres.start(holder_settings)
  use <- exception.defer(fn() { postgres.close(holder_database) })
  let assert Ok(Nil) = postgres.migrate(holder_database)
  let holder_connection = postgres.connection(holder_database)
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      holder_connection,
      unique_domain_lock_query(holder_database, worker_def, 1),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  // No `unique_lock_wait` override: exercises the shipped default exactly
  // as any caller who never touches this setting would experience it.
  let assert Ok(submitter_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(submitter_database) = postgres.start(submitter_settings)
  use <- exception.defer(fn() { postgres.close(submitter_database) })

  let submission_text = "unique-contended-default-1-" <> suffix
  submit_keep_existing(
    submitter_database,
    test_queue,
    submission_text,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.AdmissionContended))
  count_jobs_in_queue(holder_connection, test_queue) |> should.equal(0)
  unique_receipt_exists(holder_connection, submission_text)
  |> should.equal(False)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      submitter_database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  count_jobs_in_queue(holder_connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "unique-contended-lock-wait-default-settings-passed",
  )
}

/// The row-lock variant of contention: a scheduled row's own row lock (held
/// by the test through an open `SELECT ... FOR UPDATE` transaction, not the
/// domain lock) blocks a `RescheduleScheduledTo` submission's candidate
/// selection (which takes that same row lock, per
/// `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md`'s admission transaction step 6) until its
/// 200ms `unique_lock_wait` elapses; the row is left completely unchanged.
/// Once released, the same reschedule request succeeds.
pub fn postgres_submit_unique_reschedule_row_lock_contention_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_row_lock_test(database_url)
  }
}

fn run_unique_reschedule_row_lock_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_lock_" <> suffix,
  )
  let worker_id = "unique.reschedule-lock-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-reschedule-lock-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.ScheduledOnly,
    )

  let far_future = future_available_at(connection, 3_600_000)
  let assert Ok(seed_submission) =
    submission.submission_id("unique-reschedule-lock-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      1,
      submission.At(far_future),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let original_available_at_ms =
    job_available_at_ms(connection, job.id_value(handle))

  let row_lock_query =
    pog.query("SELECT 1 FROM grind_jobs WHERE id = $1 FOR UPDATE")
    |> pog.parameter(pog.int(job.id_value(handle)))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, row_lock_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(contended_settings) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(200)
    |> postgres.validate
  let assert Ok(contended_database) = postgres.start(contended_settings)
  use <- exception.defer(fn() { postgres.close(contended_database) })

  let later_target = future_available_at(connection, 7_200_000)
  let reschedule_submission = "unique-reschedule-lock-retry-" <> suffix
  submit_reschedule(
    contended_database,
    test_queue,
    reschedule_submission,
    worker_def,
    1,
    policy,
    later_target,
  )
  |> should.equal(Error(submission.AdmissionContended))
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  job_available_at_ms(connection, job.id_value(handle))
  |> should.equal(original_available_at_ms)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(submission.Rescheduled(conflict)) =
    submit_reschedule(
      contended_database,
      test_queue,
      reschedule_submission,
      worker_def,
      1,
      policy,
      later_target,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))
  job_available_at_ms(connection, job.id_value(handle))
  |> should.equal(job.available_at_unix_milliseconds(later_target))

  mark_database_test_executed("unique-reschedule-row-lock-contention-passed")
}

/// `set_config('lock_timeout', ..., true)` (step 1 of the admission
/// transaction) is transaction-local: it must not leak into a later
/// statement that reuses the same pooled physical connection. A
/// single-connection pool guarantees the reuse; after a contended attempt
/// on it, `SHOW lock_timeout` on that same pool must read back the
/// cluster's own default, not `200ms`.
pub fn postgres_unique_lock_timeout_does_not_leak_to_later_statements_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_lock_timeout_no_leak_test(database_url)
  }
}

fn run_unique_lock_timeout_no_leak_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.timeout-leak-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-timeout-leak-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  let assert Ok(holder_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(holder_database) = postgres.start(holder_settings)
  use <- exception.defer(fn() { postgres.close(holder_database) })
  let assert Ok(Nil) = postgres.migrate(holder_database)
  let holder_connection = postgres.connection(holder_database)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      holder_connection,
      unique_domain_lock_query(holder_database, worker_def, 1),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(probe_settings) =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
    |> postgres.with_unique_lock_wait(200)
    |> postgres.validate
  let assert Ok(probe_database) = postgres.start(probe_settings)
  use <- exception.defer(fn() { postgres.close(probe_database) })

  submit_keep_existing(
    probe_database,
    test_queue,
    "unique-timeout-leak-1-" <> suffix,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.AdmissionContended))

  let probe_connection = postgres.connection(probe_database)
  let show_lock_timeout = fn() {
    let assert Ok(returned) =
      pog.query("SHOW lock_timeout")
      |> pog.returning({
        use value <- decode.field(0, decode.string)
        decode.success(value)
      })
      |> pog.execute(on: probe_connection)
    let assert [value] = returned.rows
    value
  }

  // A *rolled-back* transaction's `SET`/`set_config` change is undone
  // regardless of `is_local` — PostgreSQL reverts GUC changes made inside
  // an aborted transaction either way, so a contended (and hence
  // rolled-back) attempt alone cannot distinguish `is_local: true` from
  // `false`. Checked anyway, for completeness, but the assertion below
  // (after a *committed* attempt on this same connection) is the one that
  // actually exercises `is_local`'s documented difference.
  show_lock_timeout() |> should.equal("0")

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  // With the domain lock now free, this same pooled connection commits a
  // fresh submission. `is_local: true` (`set_config`'s third argument)
  // means `SET LOCAL`-style transaction-local scope: the setting reverts at
  // COMMIT, not only at ROLLBACK. `is_local: false` would instead behave
  // like a plain session-level `SET`, which survives the COMMIT and would
  // leave `lock_timeout` at `200` for every later statement on this pool.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      probe_database,
      test_queue,
      "unique-timeout-leak-2-" <> suffix,
      worker_def,
      1,
      policy,
    )
  show_lock_timeout() |> should.equal("0")

  mark_database_test_executed("unique-lock-timeout-no-leak-passed")
}

// -- Isolation-level pinning (R1) --------------------------------------------
//
// See `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/RECOVERY-EVIDENCE.md`, "Isolation-level pinning", for the red
// evidence this test was checked against.

/// The admission transaction's correctness (a waiter's plain reads after the
/// domain lock must see whatever committed while it waited) depends on
/// `READ COMMITTED` semantics, not merely the cluster's *default* being
/// `READ COMMITTED` — a role or database configured with
/// `default_transaction_isolation = 'repeatable read'` would otherwise
/// silently break admission with no code-visible signal. This test runs the
/// same forced-overlap barrier as Increment 8's main test, but against a
/// dedicated disposable database whose own configured default really is
/// `repeatable read` (`GRIND_TEST_REPEATABLE_READ_URL`,
/// `scripts/test-postgres.sh`), and proves the admission transaction still
/// produces exactly one `Inserted` and one `Existing` against that same job
/// id — not two rows.
pub fn postgres_submit_unique_admission_safe_under_repeatable_read_test() {
  case repeatable_read_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_repeatable_read_test(database_url)
  }
}

fn run_unique_repeatable_read_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.repeatable-read-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-repeatable-read-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  use entries <- with_unique_databases(database_url, [
    "grind_unique_rr_a_" <> suffix,
    "grind_unique_rr_b_" <> suffix,
  ])
  let assert [#(database_a, barrier_connection), #(database_b, _)] = entries

  // Confirm this database's own *persisted, configured* default directly,
  // rather than trust `scripts/test-postgres.sh`'s setup silently — and
  // read it from `pg_db_role_setting`/`pg_database`, not `SHOW
  // default_transaction_isolation` on a Grind-managed connection: Grind's
  // own pool now pins `default_transaction_isolation` to `read committed`
  // as a startup connection parameter (see `postgres.validate`), so a
  // Grind connection's *active* session setting reads `read committed`
  // regardless of what this database is configured to default to. The
  // catalog query below reads the database-level configuration itself,
  // which this connection's own override does not change.
  let assert Ok(isolation_returned) =
    pog.query(
      "SELECT EXISTS (SELECT 1 FROM pg_db_role_setting JOIN pg_database ON pg_database.oid = pg_db_role_setting.setdatabase WHERE pg_database.datname = current_database() AND pg_db_role_setting.setrole = 0 AND EXISTS (SELECT 1 FROM unnest(pg_db_role_setting.setconfig) AS cfg WHERE cfg = 'default_transaction_isolation=repeatable read'))",
    )
    |> pog.returning({
      use configured <- decode.field(0, decode.bool)
      decode.success(configured)
    })
    |> pog.execute(on: barrier_connection)
  let assert [True] = isolation_returned.rows

  let lock_key = unique_test_lock_key(3)
  let cleanup_trigger =
    install_unique_insert_barrier(
      barrier_connection,
      "grind_test_unique_rr_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_unique_rr_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(barrier_connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let submission_a = "unique-rr-a-" <> suffix
  let submission_b = "unique-rr-b-" <> suffix
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_keep_existing(
      database_a,
      test_queue,
      submission_a,
      worker_def,
      1,
      policy,
    )
  })
  spawn_submit(result_b, fn() {
    submit_keep_existing(
      database_b,
      test_queue,
      submission_b,
      worker_def,
      1,
      policy,
    )
  })

  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    1,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  let outcomes = [outcome_a, outcome_b]

  let inserted_ids =
    list.filter_map(outcomes, fn(outcome) {
      case outcome {
        Ok(submission.Inserted(handle)) -> Ok(job.id_value(handle))
        _ -> Error(Nil)
      }
    })
  let existing_conflicts =
    list.filter_map(outcomes, fn(outcome) {
      case outcome {
        Ok(submission.Existing(conflict)) -> Ok(conflict)
        _ -> Error(Nil)
      }
    })
  list.length(inserted_ids) |> should.equal(1)
  list.length(existing_conflicts) |> should.equal(1)
  let assert [inserted_id] = inserted_ids
  let assert [existing_conflict] = existing_conflicts
  submission.conflict_job_id(existing_conflict) |> should.equal(inserted_id)
  count_jobs_in_queue(barrier_connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "unique-admission-safe-under-repeatable-read-passed",
  )
}
