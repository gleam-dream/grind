import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/submission
import grind/internal/unique
import grind/internal/worker
import grind/support/consumer.{manual_policy}
import grind/support/env.{database_url, mark_database_test_executed}
import grind/support/job_queries.{count_jobs_in_queue}
import grind/support/submissions.{submit_keep_existing, unique_test_worker}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_database}
import grind/support/unique_rows.{force_job_state, force_job_timestamp}

/// Increment 7(a): the same `SubmissionId` with the same request, retried
/// after the original row genuinely succeeded and its short period has
/// elapsed, returns the original `Inserted` handle (same job id) from the
/// receipt — not a second row (which a fresh, receipt-blind candidate
/// lookup would create, since the succeeded row is by then outside its own
/// period).
pub fn postgres_submit_unique_receipt_replay_is_idempotent_after_period_elapses_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_receipt_idempotent_replay_test(database_url)
  }
}

fn run_receipt_idempotent_replay_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_receipt_idempotent",
  )
  let worker_def = unique_test_worker("unique.receipt-idempotent-" <> suffix)
  let test_queue = "receipt-idempotent-" <> suffix
  let assert Ok(registry_workers) = registry.new(test_queue)
  let assert Ok(registry_workers) =
    registry.register(registry_workers, worker_def)
  let assert Ok(consumer) =
    queue.start(database, registry_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let assert Ok(period) = unique.within_milliseconds(5000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.IncompleteOrSucceeded,
    )
  let submission_text = "unique-receipt-idempotent-1-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  // The 5-second period has elapsed: without the receipt, a fresh candidate
  // lookup for this key would now find nothing eligible.
  force_job_timestamp(
    connection,
    job.id_value(handle),
    "inserted_at",
    "clock_timestamp() - interval '6 seconds'",
  )

  let assert Ok(submission.Inserted(replayed_handle)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  job.id_value(replayed_handle) |> should.equal(job.id_value(handle))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  mark_database_test_executed("unique-receipt-idempotent-replay-passed")
}

/// Increment 7(b): the same `SubmissionId` with a different input conflicts.
pub fn postgres_submit_unique_receipt_replay_with_different_input_conflicts_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_receipt_different_input_conflict_test(database_url)
  }
}

fn run_receipt_different_input_conflict_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_receipt_conflict",
  )
  let worker_def = unique_test_worker("unique.receipt-conflict-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "receipt-conflict-" <> suffix
  let submission_text = "unique-receipt-conflict-1-" <> suffix

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  submit_keep_existing(
    database,
    test_queue,
    submission_text,
    worker_def,
    2,
    policy,
  )
  |> should.equal(Error(submission.SubmissionConflict))

  mark_database_test_executed("unique-receipt-different-input-conflict-passed")
}

/// `reconcile_unique` carrying a `PendingSubmission` whose fingerprint does
/// not match the receipt actually committed under this `SubmissionId` (as if
/// it were reconciling a different request B's `CommitUnknown` against an id
/// request A already committed under) must report `SubmissionConflict`, the
/// same as `submit_unique`'s own in-transaction receipt check — never
/// `CommitUnknown`, which would tell request B's caller to keep retrying an
/// admission that, correctly, already belongs to someone else forever.
pub fn postgres_reconcile_unique_mismatched_pending_reports_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_reconcile_unique_mismatched_pending_test(database_url)
  }
}

fn run_reconcile_unique_mismatched_pending_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_reconcile_mismatch_" <> suffix,
  )
  let worker_def = unique_test_worker("unique.reconcile-mismatch-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "reconcile-mismatch-" <> suffix
  let assert Ok(submission_id) =
    submission.submission_id("unique-reconcile-mismatch-" <> suffix)

  // Request A commits under `submission_id`.
  let assert Ok(submission.Inserted(_)) =
    postgres.submit_unique(
      database,
      test_queue,
      submission_id,
      worker_def,
      1,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )

  // A `PendingSubmission` standing in for a *different* request B, reusing
  // the same `submission_id` but carrying a fingerprint that does not match
  // what request A actually committed.
  let mismatched_pending =
    submission.new_pending_submission(
      postgres.installation(database),
      submission_id,
      worker_def,
      <<9, 9, 9>>,
    )

  postgres.reconcile_unique(database, mismatched_pending)
  |> should.equal(Error(submission.SubmissionConflict))

  mark_database_test_executed(
    "reconcile-unique-mismatched-pending-conflict-passed",
  )
}

/// Increment 7(c): a replayed `Existing` decision returns the observed state
/// recorded in the receipt at decision time, not the row's current
/// (possibly since-progressed) state.
pub fn postgres_submit_unique_receipt_replay_returns_originally_observed_state_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_receipt_replay_observed_state_test(database_url)
  }
}

fn run_receipt_replay_observed_state_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_receipt_observed_state",
  )
  let worker_def = unique_test_worker("unique.receipt-observed-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "receipt-observed-" <> suffix
  let submission_replay = "unique-receipt-observed-2-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-receipt-observed-1-" <> suffix,
      worker_def,
      1,
      policy,
    )
  let assert Ok(submission.Existing(conflict_first)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_replay,
      worker_def,
      1,
      policy,
    )
  submission.conflict_state(conflict_first) |> should.equal(job.Queued)

  // The row genuinely progresses after the receipt was recorded.
  force_job_state(connection, job.id_value(handle), "succeeded")

  let assert Ok(submission.Existing(conflict_replayed)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_replay,
      worker_def,
      1,
      policy,
    )
  submission.conflict_job_id(conflict_replayed)
  |> should.equal(job.id_value(handle))
  submission.conflict_state(conflict_replayed) |> should.equal(job.Queued)

  mark_database_test_executed(
    "unique-receipt-replay-returns-observed-state-passed",
  )
}

/// Increment 7(d), proving R2 (Decision 9): replaying the same
/// `SubmissionId` and input against a worker whose output codec version has
/// changed conflicts, rather than returning a handle bound to a different
/// codec than the one it was originally admitted under.
pub fn postgres_submit_unique_receipt_replay_with_changed_output_codec_conflicts_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_receipt_output_codec_change_conflict_test(database_url)
  }
}

fn run_receipt_output_codec_change_conflict_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_receipt_codec_change",
  )
  let assert Ok(input_codec) =
    worker.codec(
      "unique-receipt-codec-input-" <> suffix <> "-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec_v1) =
    worker.codec(
      "unique-receipt-codec-output-" <> suffix <> "-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let worker_id = "unique.receipt-codec-change-" <> suffix
  let assert Ok(worker_v1) =
    worker.define(worker_id, "v1", input_codec, output_codec_v1, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(output_codec_v2) =
    worker.codec(
      "unique-receipt-codec-output-" <> suffix <> "-v2",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker_v1_recoded) =
    worker.define(worker_id, "v1", input_codec, output_codec_v2, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "receipt-codec-change-" <> suffix
  let submission_text = "unique-receipt-codec-1-" <> suffix

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_v1,
      1,
      policy,
    )
  submit_keep_existing(
    database,
    test_queue,
    submission_text,
    worker_v1_recoded,
    1,
    policy,
  )
  |> should.equal(Error(submission.SubmissionConflict))

  mark_database_test_executed(
    "unique-receipt-output-codec-change-conflict-passed",
  )
}
