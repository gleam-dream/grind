import exception
import gleam/erlang/process
import gleam/list
import gleeunit/should
import grind/job
import grind/submission
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt,
  install_unique_insert_barrier, spawn_lock_holder, spawn_submit,
  unique_test_lock_key,
}
import grind/support/env.{database_url, mark_database_test_executed}
import grind/support/job_queries.{count_jobs_in_queue}
import grind/support/lock_wait.{
  await_overlap_shape, unique_domain_lock_query_like, unique_insert_query_like,
}
import grind/support/submissions.{submit_keep_existing, unique_test_worker}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_databases}
import grind/unique
import pog

// -- Increment 8: concurrent admission under a forced barrier --------------
//
// See `docs/RECOVERY-EVIDENCE.md`, Increment 8, for the mutation evidence
// (a genuine red run with the domain lock skipped, and one with the lock
// key widened to include the queue) these tests were checked against.

/// Three `submit_unique` calls, same key, `KeepExisting`, distinct
/// `SubmissionId`s, from three separate pools, forced to actually overlap by
/// a test-only `BEFORE INSERT` barrier trigger: exactly one settles as
/// `Inserted` and the other two settle as `Existing` referencing that same
/// job id, and exactly one row is ever persisted.
pub fn postgres_submit_unique_concurrent_admission_forced_overlap_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_concurrent_overlap_test(database_url)
  }
}

fn run_unique_concurrent_overlap_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.overlap-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-overlap-" <> suffix
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
    "grind_unique_overlap_a_" <> suffix,
    "grind_unique_overlap_b_" <> suffix,
    "grind_unique_overlap_c_" <> suffix,
  ])
  let assert [
    #(database_a, barrier_connection),
    #(database_b, _),
    #(database_c, _),
  ] = entries
  let lock_key = unique_test_lock_key(0)
  let cleanup_trigger =
    install_unique_insert_barrier(
      barrier_connection,
      "grind_test_unique_overlap_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_unique_overlap_barrier",
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

  let submission_a = "unique-overlap-a-" <> suffix
  let submission_b = "unique-overlap-b-" <> suffix
  let submission_c = "unique-overlap-c-" <> suffix
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  let result_c = process.new_subject()
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
  spawn_submit(result_c, fn() {
    submit_keep_existing(
      database_c,
      test_queue,
      submission_c,
      worker_def,
      1,
      policy,
    )
  })

  // Exactly one submitter has won the domain lock and blocked inserting
  // behind the test's own held trigger lock; the other two are blocked
  // acquiring that same domain lock.
  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    2,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  let assert Ok(outcome_c) = process.receive(result_c, within: 5000)
  let outcomes = [outcome_a, outcome_b, outcome_c]

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
  list.length(existing_conflicts) |> should.equal(2)
  let assert [inserted_id] = inserted_ids
  list.each(existing_conflicts, fn(conflict) {
    submission.conflict_job_id(conflict) |> should.equal(inserted_id)
  })
  count_jobs_in_queue(barrier_connection, test_queue) |> should.equal(1)

  mark_database_test_executed("unique-concurrent-forced-overlap-passed")
}

/// The uniqueness domain lock key deliberately excludes queue
/// (`docs/UNIQUENESS-CONTRACT.md`, admission transaction step 3), so a
/// `WithinQueue` submission in one queue and an `AcrossQueues` submission in
/// another, on the same key, still serialize against each other: one row,
/// not two.
pub fn postgres_submit_unique_concurrent_admission_mixed_scope_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_concurrent_mixed_scope_test(database_url)
  }
}

fn run_unique_concurrent_mixed_scope_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.mixed-scope-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let queue_a = "unique-mixed-scope-q1-" <> suffix
  let queue_b = "unique-mixed-scope-q2-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy_within =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let policy_across =
    unique.policy(
      unique.full_input(),
      unique.AcrossQueues,
      period,
      unique.Incomplete,
    )

  use entries <- with_unique_databases(database_url, [
    "grind_unique_mixed_a_" <> suffix,
    "grind_unique_mixed_b_" <> suffix,
  ])
  let assert [#(database_a, barrier_connection), #(database_b, _)] = entries
  let lock_key = unique_test_lock_key(1)
  let cleanup_trigger =
    install_unique_insert_barrier(
      barrier_connection,
      "grind_test_unique_mixed_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_unique_mixed_barrier",
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

  let submission_a = "unique-mixed-scope-a-" <> suffix
  let submission_b = "unique-mixed-scope-b-" <> suffix
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_keep_existing(
      database_a,
      queue_a,
      submission_a,
      worker_def,
      1,
      policy_within,
    )
  })

  // A (`WithinQueue`, `queue_a`) must actually be blocked inserting behind
  // the barrier before B starts. A `WithinQueue` candidate query only ever
  // looks inside its own queue, so which of A/B reaches the domain lock
  // first is not incidental here the way it was for the same-queue overlap
  // test above: if B (`AcrossQueues`, `queue_b`) inserted *first*, A's own
  // `queue_a`-scoped candidate query would never see B's `queue_b` row and
  // would legitimately insert its own — two rows, correctly, by the
  // documented per-queue semantics `WithinQueue` already promises (Increment
  // 4). Starting A alone first and waiting for it to reach the barrier
  // fixes the order without weakening the concurrency being proved: B still
  // arrives while A's insert transaction is genuinely open and still needs
  // the same domain lock A holds, which is exactly what the lock key
  // excluding queue (`docs/UNIQUENESS-CONTRACT.md`, admission transaction
  // step 2) is being proved to guarantee.
  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    0,
    500,
  )
  |> should.equal(True)

  spawn_submit(result_b, fn() {
    submit_keep_existing(
      database_b,
      queue_b,
      submission_b,
      worker_def,
      1,
      policy_across,
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
  let assert Ok(submission.Inserted(handle_a)) = outcome_a
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  let assert Ok(submission.Existing(conflict_b)) = outcome_b
  submission.conflict_job_id(conflict_b) |> should.equal(job.id_value(handle_a))
  submission.conflict_queue(conflict_b) |> should.equal(queue_a)
  count_jobs_in_queue(barrier_connection, queue_a) |> should.equal(1)
  count_jobs_in_queue(barrier_connection, queue_b) |> should.equal(0)

  mark_database_test_executed("unique-concurrent-mixed-scope-passed")
}

/// Increment 8's deferred receipt-ordering evidence: submitter A commits its
/// `Inserted` decision (and its receipt) while submitter B — the *same*
/// `SubmissionId` and the same request — is still waiting on the domain
/// lock A holds. Once A releases, B must return A's recorded `Inserted`
/// decision (same job id, same `JobHandle`-carrying variant), not a fresh
/// `Existing` conflict against the row A just committed — proving the
/// receipt lookup genuinely runs, and matches, before B ever performs its
/// own candidate selection.
pub fn postgres_submit_unique_receipt_ordering_returns_committed_decision_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_receipt_ordering_test(database_url)
  }
}

fn run_unique_receipt_ordering_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.receipt-order-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-receipt-order-" <> suffix
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
    "grind_unique_receipt_order_a_" <> suffix,
    "grind_unique_receipt_order_b_" <> suffix,
  ])
  let assert [#(database_a, barrier_connection), #(database_b, _)] = entries
  let lock_key = unique_test_lock_key(2)
  let cleanup_trigger =
    install_unique_insert_barrier(
      barrier_connection,
      "grind_test_unique_receipt_order_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_unique_receipt_order_barrier",
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

  let shared_submission = "unique-receipt-order-shared-" <> suffix
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_keep_existing(
      database_a,
      test_queue,
      shared_submission,
      worker_def,
      1,
      policy,
    )
  })

  // A must actually be blocked inserting behind the barrier before B
  // starts, or B could race A for the domain lock instead of waiting
  // behind it.
  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    0,
    500,
  )
  |> should.equal(True)

  spawn_submit(result_b, fn() {
    submit_keep_existing(
      database_b,
      test_queue,
      shared_submission,
      worker_def,
      1,
      policy,
    )
  })

  // B must be waiting on the domain lock A still holds before we release A
  // — otherwise this run proves nothing about ordering.
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
  let assert Ok(submission.Inserted(handle_a)) = outcome_a
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  let assert Ok(submission.Inserted(handle_b)) = outcome_b
  job.id_value(handle_b) |> should.equal(job.id_value(handle_a))
  count_jobs_in_queue(barrier_connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "unique-receipt-ordering-b-returns-a-decision-passed",
  )
}
