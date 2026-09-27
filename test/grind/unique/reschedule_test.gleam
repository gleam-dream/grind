import exception
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/submission
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt, spawn_lock_holder,
  spawn_submit, unique_test_lock_key,
}
import grind/support/consumer.{manual_policy}
import grind/support/env.{database_url, mark_database_test_executed}
import grind/support/job_queries.{count_jobs_in_queue}
import grind/support/lock_wait.{
  await_claim_waiting_on_advisory, await_lock_wait_counts,
}
import grind/support/queue_signals.{FirstAttemptStarted}
import grind/support/submissions.{submit_keep_existing, unique_test_worker}
import grind/support/unique_fixture.{
  unique_test_blocking_worker, unique_test_suffix, with_unique_database,
}
import grind/support/unique_rows.{
  force_available_at_due, force_job_state, future_available_at,
  job_available_at_ms, submit_reschedule, unique_receipt_reschedule_fields,
}
import grind/unique
import pog

// -- Increment 10: rescheduling ---------------------------------------------
//
// Full contract: `docs/UNIQUENESS-CONTRACT.md`, `ConflictAction`,
// `RescheduleScheduledTo`, and admission transaction steps 5-6.
// `postgres_submit_unique_reschedule_row_lock_contention_test` above already
// proves lock contention on the reschedule candidate's row; the tests below
// prove the reschedule decision itself (moving `available_at`, leaving other
// states alone, making a rescheduled row genuinely claimable, and the live
// race against a real claim).

/// A scheduled conflict rescheduled to `t2` settles `Rescheduled`;
/// `available_at` in the database equals `t2` exactly; the job id, worker,
/// and input are unchanged (rebinding the same id under the same worker
/// still reads the originally submitted input); the receipt records both the
/// previous and the new `available_at`.
pub fn postgres_submit_unique_reschedule_moves_available_at_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_basic_test(database_url)
  }
}

fn run_unique_reschedule_basic_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_basic_" <> suffix,
  )
  let worker_id = "unique.reschedule-basic-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-reschedule-basic-" <> suffix
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
    submission.submission_id("unique-reschedule-basic-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      7,
      submission.At(original_target),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let job_id = job.id_value(handle)

  let new_target = future_available_at(connection, 7_200_000)
  let reschedule_submission = "unique-reschedule-basic-retry-" <> suffix
  let assert Ok(submission.Rescheduled(conflict)) =
    submit_reschedule(
      database,
      test_queue,
      reschedule_submission,
      worker_def,
      7,
      policy,
      new_target,
    )

  submission.conflict_job_id(conflict) |> should.equal(job_id)
  submission.conflict_queue(conflict) |> should.equal(test_queue)
  submission.conflict_state(conflict) |> should.equal(job.Scheduled)
  job_available_at_ms(connection, job_id)
  |> should.equal(job.available_at_unix_milliseconds(new_target))

  // The job id, worker, and input are unchanged: the same id under the same
  // worker still reads the originally submitted input, and only one row for
  // this key exists.
  postgres.arguments(database, handle) |> should.equal(Ok(7))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  let #(from_ms, to_ms) =
    unique_receipt_reschedule_fields(connection, reschedule_submission)
  from_ms
  |> should.equal(Some(job.available_at_unix_milliseconds(original_target)))
  to_ms |> should.equal(Some(job.available_at_unix_milliseconds(new_target)))

  mark_database_test_executed("unique-reschedule-moves-available-at-passed")
}

/// A `RescheduleScheduledTo` submission against a conflict in `queued`,
/// `retryable`, `executing`, or `uncertain` — every non-`scheduled` state
/// `Incomplete` admits — settles `Existing` with the row completely
/// unchanged (`available_at` untouched), never `Rescheduled`. Oban's own
/// "replacing fields based on job state" is inspired-by-upstream here,
/// limited to `available_at` on `scheduled` rows specifically.
pub fn postgres_submit_unique_reschedule_leaves_non_scheduled_states_unchanged_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_non_scheduled_test(database_url)
  }
}

fn run_unique_reschedule_non_scheduled_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_states_" <> suffix,
  )
  let worker_def = unique_test_worker("unique.reschedule-states-" <> suffix)
  let test_queue = "unique-reschedule-states-" <> suffix
  let period = unique.while_retained()
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  let cases = [
    #("queued", job.Queued, 501),
    #("retryable", job.Retryable, 502),
    #("executing", job.Executing, 503),
    #("uncertain", job.Uncertain, 504),
  ]

  cases
  |> list.each(fn(entry) {
    let #(stored, expected_state, input_value) = entry
    let assert Ok(submission.Inserted(handle)) =
      submit_keep_existing(
        database,
        test_queue,
        "unique-reschedule-states-seed-" <> suffix <> "-" <> stored,
        worker_def,
        input_value,
        policy,
      )
    let job_id = job.id_value(handle)
    force_job_state(connection, job_id, stored)
    let before_ms = job_available_at_ms(connection, job_id)

    let target = future_available_at(connection, 3_600_000)
    let assert Ok(submission.Existing(conflict)) =
      submit_reschedule(
        database,
        test_queue,
        "unique-reschedule-states-check-" <> suffix <> "-" <> stored,
        worker_def,
        input_value,
        policy,
        target,
      )
    submission.conflict_job_id(conflict) |> should.equal(job_id)
    submission.conflict_state(conflict) |> should.equal(expected_state)
    job_available_at_ms(connection, job_id) |> should.equal(before_ms)
  })

  mark_database_test_executed(
    "unique-reschedule-non-scheduled-unchanged-passed",
  )
}

/// Rescheduling a `scheduled` row to a due (past) database time makes it
/// genuinely claimable by a real manually-driven consumer on its very next
/// `process_one` call.
pub fn postgres_submit_unique_reschedule_to_due_time_makes_row_claimable_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_claimable_test(database_url)
  }
}

fn run_unique_reschedule_claimable_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_claimable_" <> suffix,
  )
  let worker_id = "unique.reschedule-claimable-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-reschedule-claimable-" <> suffix
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
    submission.submission_id("unique-reschedule-claimable-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      9,
      submission.At(far_future),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  let due_target = future_available_at(connection, -2000)
  let assert Ok(submission.Rescheduled(_)) =
    submit_reschedule(
      database,
      test_queue,
      "unique-reschedule-claimable-retry-" <> suffix,
      worker_def,
      9,
      policy,
      due_target,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  let assert Ok(registry_workers) = registry.new(test_queue)
  let assert Ok(registry_workers) =
    registry.register(registry_workers, worker_def)
  let assert Ok(consumer) =
    queue.start(database, registry_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  mark_database_test_executed("unique-reschedule-due-time-claimable-passed")
}

/// The live race between a manual consumer's claim (which locks the
/// scheduled row as part of its own claim `UPDATE`) and a concurrent
/// `RescheduleScheduledTo` submission for the same key (whose candidate
/// selection also locks that row, per `docs/UNIQUENESS-CONTRACT.md`'s
/// admission transaction step 5). The claim is blocked mid-`UPDATE` — after
/// it has already locked the row via its own `FOR UPDATE SKIP LOCKED`
/// candidate CTE, before it commits — by a `BEFORE UPDATE` trigger scoped to
/// this job id (the same `grind_test_claim_overlap` shape
/// `run_overlapping_claim_test` uses), holding a fresh test-only advisory
/// lock via `spawn_lock_holder`; the reschedule submission's own candidate
/// `SELECT ... FOR UPDATE` then genuinely waits on the row lock the claim's
/// still-open transaction holds (`pg_stat_activity`'s `transactionid` wait
/// event — a real tuple-lock wait, not a second advisory wait, confirmed
/// alongside the claim's own advisory wait via `await_lock_wait_counts`).
/// Releasing the barrier lets the claim finish (`state -> executing`,
/// commit), which releases the row lock. PostgreSQL's own `EvalPlanQual`
/// re-check for a `SELECT ... FOR UPDATE` whose target row was concurrently
/// updated then re-evaluates the reschedule submission's eligible-states
/// filter against the row's *fresh* post-commit state, not the stale
/// `scheduled` value the row had when the wait began:
///
/// - Under `Incomplete` (which admits `executing`), the fresh state still
///   matches the filter, so the row is returned with `state = "executing"`;
///   the Gleam-level `RescheduleScheduledTo(_), "scheduled"` pattern match
///   does not fire, and the call settles `Existing` with the observed
///   `Executing` state, `available_at` completely untouched.
/// - Under `ScheduledOnly` (which does not admit `executing`), the fresh
///   state no longer matches the filter at all, so `find_candidate` returns
///   no row and the call settles `Inserted` — a fresh row.
///
/// The claimed job's handler (`unique_test_blocking_worker`) deliberately
/// blocks rather than returning immediately, and is released only *after*
/// this test has already observed the reschedule submission's own result: a
/// handler that returns immediately would let the coordinator's own
/// subsequent acknowledgement race ahead to `succeeded` (which `Incomplete`
/// does not admit either) before the reschedule's blocked row lock is even
/// granted — an environment-dependent race, not a deterministic proof. The
/// handler starting (`FirstAttemptStarted`) is itself proof the claim's row
/// lock has already been released (the claim's `UPDATE` commits, as a
/// single-statement transaction, strictly before the coordinator invokes
/// the handler), so waiting for it before checking the reschedule's result
/// is a real synchronization point, not a sleep.
pub fn postgres_submit_unique_reschedule_race_incomplete_returns_existing_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_unique_reschedule_claim_race_test(
        database_url,
        unique.Incomplete,
        6,
        "unique-reschedule-race-incomplete-existing-passed",
      )
  }
}

pub fn postgres_submit_unique_reschedule_race_scheduled_only_returns_inserted_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_unique_reschedule_claim_race_test(
        database_url,
        unique.ScheduledOnly,
        7,
        "unique-reschedule-race-scheduled-only-inserted-passed",
      )
  }
}

fn run_unique_reschedule_claim_race_test(
  database_url: String,
  states: unique.States,
  salt: Int,
  marker: String,
) -> Nil {
  let run_id = unique_test_suffix()
  let suffix = run_id <> "-" <> int.to_string(salt)
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_race_" <> suffix,
  )
  let worker_id = "unique.reschedule-race-" <> suffix
  let handler_started = process.new_subject()
  let worker_def = unique_test_blocking_worker(worker_id, handler_started)
  let test_queue = "unique-reschedule-race-" <> suffix
  let assert Ok(registry_workers) = registry.new(test_queue)
  let assert Ok(registry_workers) =
    registry.register(registry_workers, worker_def)
  let assert Ok(consumer) =
    queue.start(database, registry_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let period = unique.while_retained()
  let policy =
    unique.policy(unique.full_input(), unique.WithinQueue, period, states)

  let far_future = future_available_at(connection, 3_600_000)
  let assert Ok(seed_submission) =
    submission.submission_id("unique-reschedule-race-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      11,
      submission.At(far_future),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let job_id = job.id_value(handle)
  force_available_at_due(connection, job_id)
  let original_available_at_ms = job_available_at_ms(connection, job_id)

  let lock_key = unique_test_lock_key(salt)
  let trigger_name =
    "grind_test_reschedule_race_" <> run_id <> "_" <> int.to_string(salt)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND NEW.state = 'executing' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> trigger_name
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection)
    Nil
  })

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_reschedule_race_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net (see the uniqueness barrier tests above for why this is
  // registered right after obtaining `release_lock`).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let claim_reply = process.new_subject()
  spawn_submit(claim_reply, fn() { queue.process_one(consumer) })
  await_claim_waiting_on_advisory(connection, 250) |> should.equal(True)

  let reschedule_target = future_available_at(connection, 7_200_000)
  let reschedule_submission = "unique-reschedule-race-retry-" <> suffix
  let reschedule_reply = process.new_subject()
  spawn_submit(reschedule_reply, fn() {
    submit_reschedule(
      database,
      test_queue,
      reschedule_submission,
      worker_def,
      11,
      policy,
      reschedule_target,
    )
  })

  // The claim is blocked in its trigger (advisory wait) and the reschedule
  // submission is genuinely waiting on the row lock the claim's still-open
  // transaction holds (a real tuple-lock wait) — both from one snapshot.
  await_lock_wait_counts(connection, 1, 1, 500) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  // The claim's own `UPDATE` has now committed (`state = 'executing'`, its
  // row lock released) and the coordinator has invoked the handler, which
  // blocks immediately — the row stays genuinely `executing` (no
  // acknowledgement has run yet) for as long as this barrier is held, which
  // is deterministically until this test releases it below, *after*
  // confirming the reschedule submission's own result. Without this
  // barrier, a handler that returns immediately would let the
  // acknowledgement race ahead to `succeeded` before the reschedule's
  // blocked row lock is even granted — an environment-dependent race, not
  // proof.
  let assert Ok(FirstAttemptStarted(handler_release)) =
    process.receive(handler_started, within: 5000)

  let assert Ok(reschedule_result) =
    process.receive(reschedule_reply, within: 5000)

  process.send(handler_release, ReleaseAttempt)
  process.receive(claim_reply, within: 5000) |> should.equal(Ok(Ok(True)))

  case states {
    unique.Incomplete -> {
      let assert Ok(submission.Existing(conflict)) = reschedule_result
      submission.conflict_job_id(conflict) |> should.equal(job_id)
      submission.conflict_state(conflict) |> should.equal(job.Executing)
      job_available_at_ms(connection, job_id)
      |> should.equal(original_available_at_ms)
      count_jobs_in_queue(connection, test_queue) |> should.equal(1)
    }
    unique.ScheduledOnly -> {
      let assert Ok(submission.Inserted(new_handle)) = reschedule_result
      job.id_value(new_handle) |> should.not_equal(job_id)
      count_jobs_in_queue(connection, test_queue) |> should.equal(2)
    }
    _ -> should.fail()
  }

  mark_database_test_executed(marker)
}
