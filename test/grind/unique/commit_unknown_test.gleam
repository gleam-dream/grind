import exception
import gleam/erlang/process
import gleeunit/should
import grind/internal/job
import grind/internal/postgres
import grind/internal/submission
import grind/internal/unique
import grind/support/ack_queries.{wait_for_commit_trigger_backend}
import grind/support/concurrency.{spawn_submit}
import grind/support/env.{database_url, mark_database_test_executed}
import grind/support/job_queries.{count_jobs_in_queue}
import grind/support/job_state.{retry_transient_query}
import grind/support/submissions.{
  submit_keep_existing, unique_receipt_exists, unique_test_worker,
}
import grind/support/syncrep.{
  backend_pid_is_alive, install_syncrep_reply_trigger,
  require_syncrep_cluster_configured, terminate_backend, wait_for_backend_gone,
  wait_for_syncrep_trigger_backend,
}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_database}
import grind/support/unique_rows.{
  future_available_at, job_available_at_ms, submit_reschedule,
}
import pog

// -- Increment 11: uncertain admission commits -------------------------------
//
// Full contract: `docs/UNIQUENESS-CONTRACT.md`, "Admission transaction" (the
// `pog.TransactionQueryError` classification and `CommitUnknown`/
// `reconcile_unique`). `install_syncrep_reply_trigger` above is generalized
// (table + predicate) so the same mechanism Increment 2 proved for the
// acknowledgement path proves the same claims here, scoped by
// `submission_id` on `grind_unique_submissions` rather than a
// server-generated `job_id` — the submission id is chosen by the caller and
// known before the admission transaction that would create a job id even
// starts, which the acknowledgement path's job-id scoping could not offer.

/// (a) A pool closed *before* `submit_unique` ever sends anything: `run`
/// (`src/grind/internal/unique_admission.gleam`) calls
/// `transaction_or_checkout_failure`, whose checkout-failure branch (the
/// pool could not hand out a connection at all, so `BEGIN` never ran) is
/// reported directly as `NotCommitted(ConnectionUnavailable)`, with no
/// `PendingSubmission` constructed and no receipt lookup attempted — this is
/// knowably not-committed, not merely uncertain. See
/// `docs/RECOVERY-EVIDENCE.md`, Increment 11, for why this needed its own
/// FFI wrapper distinguishing a checkout failure from `run`'s other,
/// genuinely uncertain `pog.TransactionQueryError` case (case (d) below).
/// Reopening the same pool name and retrying the identical `SubmissionId` —
/// a plain `submit_unique`, not `reconcile_unique` (there is no
/// `PendingSubmission` to reconcile from) — then succeeds normally:
/// `Inserted`, exactly one row.
pub fn postgres_submit_unique_closed_pool_before_send_is_admission_failed_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_closed_before_send_test(database_url)
  }
}

fn run_unique_closed_before_send_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)
  let worker_def = unique_test_worker("unique.closed-before-send-" <> suffix)
  let test_queue = "unique-closed-before-send-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "unique-closed-before-send-" <> suffix

  let _ = postgres.close(database)

  submit_keep_existing(
    database,
    test_queue,
    submission_text,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.NotCommitted(pog.ConnectionUnavailable)))

  let assert Ok(reopened_validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(reopened) = postgres.start(reopened_validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      reopened,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  count_jobs_in_queue(postgres.connection(reopened), test_queue)
  |> should.equal(1)
  postgres.arguments(reopened, handle) |> should.equal(Ok(1))

  mark_database_test_executed("unique-closed-before-send-recovers-passed")
}

/// (b) An aborted commit: a deferred constraint trigger's `pg_sleep(30)`
/// fires during the admission transaction's own COMMIT (the same mechanism
/// the pre-existing `ack-commit-connection-loss-unknown` test uses for the
/// acknowledgement path), scoped by `submission_id` on
/// `grind_unique_submissions`. Terminating the backend while it sleeps
/// aborts the whole transaction before it is ever marked committed — unlike
/// (c)/(d) below, nothing is visible to any other connection, not even
/// briefly. `submit_unique`'s own reply is `CommitUnknown(pending)`, exactly
/// as (c)/(d) also report, because the follow-up receipt lookup — run on a
/// fresh connection after the connection loss — genuinely cannot tell an
/// aborted commit from a lost reply after a real one; that ambiguity is
/// exactly what `CommitUnknown` documents. An independent read confirms zero
/// jobs and zero receipts for this key. Because nothing was ever durably
/// recorded, `reconcile_unique` alone can never resolve this (it would find
/// nothing again, forever) — the only correct recovery is a plain retry of
/// the same `SubmissionId`, which this test proves converges to `Inserted`,
/// exactly one row.
pub fn postgres_submit_unique_aborted_commit_is_commit_unknown_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_aborted_commit_test(database_url)
  }
}

fn run_unique_aborted_commit_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_aborted_commit_" <> suffix,
  )
  let worker_def = unique_test_worker("unique.aborted-commit-" <> suffix)
  let test_queue = "unique-aborted-commit-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "unique-aborted-commit-" <> suffix

  let trigger_name = "grind_test_unique_aborted_commit_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT (NEW.submission_id = '"
      <> submission_text
      <> "') THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> trigger_name
      <> " AFTER INSERT ON grind_unique_submissions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection)
  let drop_trigger = fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS "
        <> trigger_name
        <> " ON grind_unique_submissions",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
  use <- exception.defer(drop_trigger)

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  })

  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Error(submission.CommitUnknown(pending))) =
    process.receive(reply, within: 10_000)

  count_jobs_in_queue(connection, test_queue) |> should.equal(0)
  unique_receipt_exists(connection, submission_text)
  |> should.equal(False)

  // `reconcile_unique` alone can never recover this: nothing was ever
  // committed, so the receipt lookup finds nothing, forever.
  let assert Error(submission.CommitUnknown(_)) =
    postgres.reconcile_unique(database, pending)

  // Drop the trigger *before* retrying: it is still scoped by this exact
  // `submission_id`, so a retry reusing the same `SubmissionId` (the whole
  // point of this claim) would otherwise fire it again and hang the retry's
  // own commit in another 30-second `pg_sleep`, with nobody left to
  // terminate that backend — exactly the trap this early cleanup avoids.
  drop_trigger()

  // The pool just had a connection deliberately terminated
  // (`terminate_backend`, above): `pgo_connection`'s own supervised restart
  // of that connection is a real, transient recovery window (not a
  // steady-state failure), matching every other terminate-then-retry test
  // in this file — `run_ack_commit_connection_loss_test`'s own
  // `retry_transient_query` (documented `docs/RECOVERY-EVIDENCE.md`,
  // "Acknowledgement deadline") is the pattern this test was previously
  // missing, which is exactly why it flaked under load: a plain,
  // unretried call here could observe `QueryTimeout`/`ConnectionUnavailable`
  // during that same window instead of the deterministic recovered state.
  let assert Ok(submission.Inserted(handle)) =
    retry_transient_query(
      fn() {
        submit_keep_existing(
          database,
          test_queue,
          submission_text,
          worker_def,
          1,
          policy,
        )
      },
      20,
    )
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  retry_transient_query(fn() { postgres.arguments(database, handle) }, 20)
  |> should.equal(Ok(1))

  mark_database_test_executed("unique-aborted-commit-is-commit-unknown-passed")
}

/// (c) A genuinely committed admission whose reply is lost after PostgreSQL
/// has already committed locally (the same SyncRep-park-then-terminate
/// mechanism Increment 2 uses for the acknowledgement path).
/// `submit_unique` itself still returns `Ok(Inserted(handle))` — resolved by
/// the follow-up receipt lookup `run` performs on a fresh connection after
/// the connection loss, not a `CommitUnknown` the caller must separately
/// reconcile. `bind_handle` agrees on the same job id and input.
pub fn postgres_submit_unique_committed_reply_lost_returns_inserted_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_committed_reply_lost_test(database_url)
  }
}

fn run_unique_committed_reply_lost_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)

  let worker_def = unique_test_worker("unique.reply-lost-" <> suffix)
  let test_queue = "unique-reply-lost-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "unique-reply-lost-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_unique_reply_lost_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> submission_text <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      5,
      policy,
    )
  })

  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Ok(submission.Inserted(handle))) =
    process.receive(reply, within: 10_000)
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)

  postgres.arguments(database, handle) |> should.equal(Ok(5))
  let assert Ok(rebound) =
    postgres.bind_handle(database, worker_def, job.id_value(handle))
  postgres.arguments(database, rebound) |> should.equal(Ok(5))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  unique_receipt_exists(connection, submission_text)
  |> should.equal(True)

  mark_database_test_executed("unique-committed-reply-lost-inserted-passed")
}

/// (d) Committed, reply lost, *and* Grind's own pool closed while the commit
/// is still parked in `SyncRep`.
///
/// **This was found to be a correctness bug in `run`
/// (`src/grind/internal/unique_admission.gleam`), not a classification
/// difference to document and move on from.** The admission transaction's
/// own "commit" call correctly unblocks with `pog.TransactionQueryError`
/// (checked out fine, then lost the connection — genuinely uncertain, might
/// have committed), and `run` correctly routes it to
/// `reconcile_from_receipt` to check. But that follow-up `find_receipt`
/// query then *also* fails — the pool is now fully closed — and the
/// unfixed code's `Error(error) -> Error(error)` branch returned that
/// *lookup's own* connectivity failure, `NotCommitted(ConnectionUnavailable)`,
/// as if it were the *admission's* outcome, silently discarding the
/// `pending: PendingSubmission` that was already in hand. A caller told
/// `NotCommitted` reasonably treats that as "did not happen, safe to
/// retry independently" — but the zombie transaction can still commit
/// later. **Fixed** by two changes: (R1) `reconcile_from_receipt` now maps
/// a failed lookup to `Error(submission.CommitUnknown(pending))`, the same as
/// finding no receipt yet — mirroring `reconcile_unknown_ack`'s `Ok(None) |
/// Error(_) -> QueueAckUnknown` in `grind/postgres`, so a transient failure
/// while *checking* is never confused with a definite answer; (R2) `run`
/// now calls a new FFI wrapper, `transaction_or_checkout_failure`
/// (`grind_postgres_ffi.erl`), that distinguishes a checkout failure
/// (nothing was ever attempted — genuinely `NotCommitted`, no
/// `PendingSubmission`, no receipt lookup even tried) from pog's own
/// transaction outcome, so a checkout failure is no longer disguised as the
/// same `TransactionQueryError` shape a genuinely uncertain mid-transaction
/// loss produces — R1 alone would have made every checkout failure
/// (including (a) above) report `CommitUnknown` too, imprecisely; R2
/// restores (a)'s precise `NotCommitted`. See
/// `docs/RECOVERY-EVIDENCE.md` for the red-before-fix output and the R1
/// mutation that reverts to the bug.
///
/// With the fix: `submit_unique` reports `CommitUnknown(pending)`.
/// `reconcile_unique(reopened, pending)` while the zombie is still parked is
/// a pure receipt lookup with no lock of its own — the zombie's receipt
/// insert is not yet visible to any other session, so it still reports
/// `CommitUnknown` (not a persisted-conflict inference). A second,
/// independent recovery path — a *plain* `submit_unique` retry of the same
/// `SubmissionId`, attempted while the zombie is still parked — genuinely
/// needs the domain lock the zombie's still-open transaction holds, and
/// reports `AdmissionContended`; no second row either way. Only after the
/// zombie backend is terminated and confirmed gone (`wait_for_backend_gone`
/// — an independent observer connection is what later reads visibility
/// here, not Grind's own closed-then-reopened socket) does
/// `reconcile_unique(reopened, pending)` resolve from the now-visible
/// receipt: `Inserted`, with the original job id, exactly one row — and the
/// plain-retry path, tried again, converges on that same job id.
pub fn postgres_submit_unique_committed_reply_lost_store_unavailable_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_unique_committed_reply_lost_store_unavailable_test(database_url)
  }
}

fn run_unique_committed_reply_lost_store_unavailable_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_settings =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = postgres.connection(observer)
  require_syncrep_cluster_configured(observer_connection)

  let worker_def = unique_test_worker("unique.reply-lost-unavail-" <> suffix)
  let test_queue = "unique-reply-lost-unavail-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "unique-reply-lost-unavail-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    observer_connection,
    "grind_test_unique_reply_lost_unavail_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> submission_text <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      13,
      policy,
    )
  })

  let assert Ok(backend_pid) =
    wait_for_syncrep_trigger_backend(observer_connection, 300)

  let _ = postgres.close(database)

  // The admission transaction reached the database (its own "commit" call
  // was genuinely mid-flight when the pool closed) — genuinely uncertain,
  // not knowably absent: `CommitUnknown`, carrying a `PendingSubmission` to
  // reconcile from.
  let assert Ok(Error(submission.CommitUnknown(pending))) =
    process.receive(reply, within: 10_000)

  let assert Ok(reopened_validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(reopened) = postgres.start(reopened_validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  // While the zombie is still parked, its receipt insert is not yet visible
  // to any other session — a pure receipt lookup still finds nothing and
  // reports `CommitUnknown` again (not a persisted-conflict inference).
  let assert Error(submission.CommitUnknown(_)) =
    postgres.reconcile_unique(reopened, pending)

  // A fresh admission retry, unlike `reconcile_unique`, genuinely needs the
  // domain lock the zombie's still-open transaction holds — the same
  // safety `reconcile_unique` alone already provided above, confirmed here
  // as a second, independent recovery path.
  let assert Ok(contended_settings) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(200)
    |> postgres.validate
  let assert Ok(contended) = postgres.start(contended_settings)
  use <- exception.defer(fn() { postgres.close(contended) })
  submit_keep_existing(
    contended,
    test_queue,
    submission_text,
    worker_def,
    13,
    policy,
  )
  |> should.equal(Error(submission.AdmissionContended))
  count_jobs_in_queue(observer_connection, test_queue) |> should.equal(0)

  terminate_backend(observer_connection, backend_pid) |> should.equal(True)
  let assert Ok(Nil) =
    wait_for_backend_gone(observer_connection, backend_pid, 300)

  // `reconcile_unique`, once the zombie is gone, resolves from the now-
  // visible receipt — not by candidate selection reinterpreting the row as
  // a fresh conflict.
  let assert Ok(submission.Inserted(handle)) =
    postgres.reconcile_unique(reopened, pending)
  let original_job_id = job.id_value(handle)
  count_jobs_in_queue(observer_connection, test_queue) |> should.equal(1)
  postgres.arguments(reopened, handle) |> should.equal(Ok(13))

  // Second recovery path: a plain retry of the same `SubmissionId` (no
  // retained `PendingSubmission` needed) converges on the identical job id
  // through `admission_transaction`'s own receipt lookup — still one row.
  let assert Ok(submission.Inserted(retried_handle)) =
    submit_keep_existing(
      reopened,
      test_queue,
      submission_text,
      worker_def,
      13,
      policy,
    )
  job.id_value(retried_handle) |> should.equal(original_job_id)
  count_jobs_in_queue(observer_connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "unique-committed-reply-lost-store-unavailable-passed",
  )
}

/// (e) A reschedule whose commit reply is lost: replay via the same internal
/// receipt lookup returns `Rescheduled`, not `Existing`, even though the
/// row's current state (`scheduled`, at its new `available_at`) looks
/// exactly like an ordinary scheduled conflict either way — the receipt's
/// own recorded *decision* column, not the row's current state, is what
/// `find_receipt`/`outcome_of_receipt` decodes.
pub fn postgres_submit_unique_reschedule_reply_lost_returns_rescheduled_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_reply_lost_test(database_url)
  }
}

fn run_unique_reschedule_reply_lost_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)

  let worker_def = unique_test_worker("unique.reschedule-reply-lost-" <> suffix)
  let test_queue = "unique-reschedule-reply-lost-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.ScheduledOnly,
    )

  let original_target = future_available_at(connection, 3_600_000)
  let assert Ok(seed_submission) =
    submission.submission_id("unique-reschedule-reply-lost-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      21,
      submission.At(original_target),
      policy,
      unique.KeepExisting,
    )
  let job_id = job.id_value(handle)

  let new_target = future_available_at(connection, 7_200_000)
  let reschedule_submission = "unique-reschedule-reply-lost-retry-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_unique_reschedule_reply_lost_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> reschedule_submission <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_reschedule(
      database,
      test_queue,
      reschedule_submission,
      worker_def,
      21,
      policy,
      new_target,
    )
  })

  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Ok(submission.Rescheduled(conflict))) =
    process.receive(reply, within: 10_000)
  submission.conflict_job_id(conflict) |> should.equal(job_id)
  job_available_at_ms(connection, job_id)
  |> should.equal(job.available_at_unix_milliseconds(new_target))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  mark_database_test_executed("unique-reschedule-reply-lost-rescheduled-passed")
}
