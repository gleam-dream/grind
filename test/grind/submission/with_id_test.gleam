import exception
import gleam/erlang/process
import gleam/list
import gleeunit/should
import grind/internal/job
import grind/internal/postgres
import grind/internal/submission
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt,
  install_unique_insert_barrier, spawn_lock_holder, spawn_submit,
  unique_test_lock_key,
}
import grind/support/env.{database_url, mark_database_test_executed}
import grind/support/job_queries.{count_jobs_in_queue}
import grind/support/lock_wait.{await_overlap_shape}
import grind/support/submission_helpers.{submit_with_id_immediately}
import grind/support/submissions.{
  unique_receipt_exists, unique_test_worker, unique_test_worker_versioned,
}
import grind/support/syncrep.{
  backend_pid_is_alive, install_syncrep_reply_trigger,
  require_syncrep_cluster_configured, terminate_backend,
  wait_for_syncrep_trigger_backend,
}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_database}
import pog

pub fn postgres_submit_with_id_first_submit_inserted_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_with_id_first_submit_test(database_url)
  }
}

fn run_submit_with_id_first_submit_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_first_" <> suffix,
  )
  let worker_def = unique_test_worker("plain-first.echo-" <> suffix)
  let test_queue = "plain-first-" <> suffix
  let submission_text = "plain-first-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      7,
    )
  postgres.arguments(database, handle) |> should.equal(Ok(7))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  mark_database_test_executed("submit-with-id-first-submit-inserted-passed")
}

pub fn postgres_submit_with_id_same_request_retry_returns_original_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_with_id_retry_test(database_url)
  }
}

fn run_submit_with_id_retry_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_retry_" <> suffix,
  )
  let worker_def = unique_test_worker("plain-retry.echo-" <> suffix)
  let test_queue = "plain-retry-" <> suffix
  let submission_text = "plain-retry-" <> suffix

  let assert Ok(submission.Inserted(first_handle)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      11,
    )
  let assert Ok(submission.Inserted(retry_handle)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      11,
    )
  job.id_value(first_handle) |> should.equal(job.id_value(retry_handle))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  mark_database_test_executed("submit-with-id-retry-returns-original-passed")
}

pub fn postgres_submit_with_id_different_input_same_id_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_with_id_different_input_conflict_test(database_url)
  }
}

fn run_submit_with_id_different_input_conflict_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_conflict_" <> suffix,
  )
  let worker_def = unique_test_worker("plain-conflict.echo-" <> suffix)
  let test_queue = "plain-conflict-" <> suffix
  let submission_text = "plain-conflict-" <> suffix

  let assert Ok(submission.Inserted(_)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
    )
  submit_with_id_immediately(
    database,
    test_queue,
    submission_text,
    worker_def,
    2,
  )
  |> should.equal(Error(submission.SubmissionConflict))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  mark_database_test_executed("submit-with-id-different-input-conflict-passed")
}

/// A genuinely committed plain admission whose reply is lost after
/// PostgreSQL has already committed locally (the same SyncRep-park-then-
/// terminate mechanism Increment 2/11 use), scoped on `grind_unique_submissions`
/// by `submission_id` — the same receipt table `submit_unique` uses, since
/// `submit_with_id` reuses it directly. `submit_with_id` itself still
/// returns `Ok(Inserted(handle))`, resolved by the shared `run`'s own
/// follow-up receipt lookup (the identical code `submit_unique` runs
/// through), not a `CommitUnknown` the caller must separately reconcile.
pub fn postgres_submit_with_id_committed_reply_lost_returns_inserted_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_with_id_committed_reply_lost_test(database_url)
  }
}

fn run_submit_with_id_committed_reply_lost_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)

  let worker_def = unique_test_worker("plain-reply-lost-" <> suffix)
  let test_queue = "plain-reply-lost-" <> suffix
  let submission_text = "plain-reply-lost-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_plain_reply_lost_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> submission_text <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      5,
    )
  })

  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Ok(submission.Inserted(handle))) =
    process.receive(reply, within: 10_000)
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)

  postgres.arguments(database, handle) |> should.equal(Ok(5))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  unique_receipt_exists(connection, submission_text)
  |> should.equal(True)

  mark_database_test_executed(
    "submit-with-id-committed-reply-lost-inserted-passed",
  )
}

/// The exact query text `insert_job` (`grind/internal/unique_admission`)
/// issues for a `policy: None` request, as a `LIKE` prefix for
/// `await_overlap_shape`/`pg_stat_activity` — the "no policy" counterpart to
/// `unique_insert_query_like` above (that one has the trailing
/// `unique_key_contract, unique_key_sha256` columns this one omits).
const plain_insert_query_like = "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, error_version, max_attempts, state, available_at, inserted_at) VALUES%"

/// Two concurrent `submit_with_id` callers, same `SubmissionId` and
/// identical request, forced to actually overlap: a `BEFORE INSERT` barrier
/// trigger (`install_unique_insert_barrier`, the same mechanism the
/// uniqueness-admission overlap tests use) blocks both callers' own
/// `grind_jobs` insert behind one held advisory lock until both are
/// genuinely waiting, proven by `pg_stat_activity` rather than timing. There
/// is no domain-wide advisory lock on this "no policy" path (see
/// `admission_transaction`'s doc comment in
/// `grind/internal/unique_admission.gleam`), so releasing the barrier lets
/// whichever caller happens to proceed first commit its row and receipt;
/// the other's own `record_receipt` then hits a real `23505` against that
/// just-committed receipt, which the shared `run` resolves by re-reading
/// the exact same receipt rather than surfacing a bare conflict for what
/// is, from that caller's perspective, an ordinary successful retry. Both
/// callers converge on `Inserted` with the same job id, and exactly one row
/// is ever persisted.
pub fn postgres_submit_with_id_concurrent_same_id_one_row_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_with_id_concurrent_test(database_url)
  }
}

fn run_submit_with_id_concurrent_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_concurrent_" <> suffix,
  )
  let worker_id = "plain-concurrent.echo-" <> suffix
  let worker_def = unique_test_worker_versioned(worker_id, "v1")
  let test_queue = "plain-concurrent-" <> suffix
  let submission_text = "plain-concurrent-" <> suffix

  let lock_key = unique_test_lock_key(0)
  let cleanup_trigger =
    install_unique_insert_barrier(
      connection,
      "grind_test_plain_concurrent_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_plain_concurrent_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      42,
    )
  })
  spawn_submit(result_b, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      42,
    )
  })

  await_overlap_shape(
    connection,
    plain_insert_query_like,
    plain_insert_query_like,
    2,
    2,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 10_000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 10_000)

  case outcome_a, outcome_b {
    Ok(submission.Inserted(handle_a)), Ok(submission.Inserted(handle_b)) ->
      job.id_value(handle_a) |> should.equal(job.id_value(handle_b))
    _, _ -> should.fail()
  }
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  mark_database_test_executed("submit-with-id-concurrent-one-row-passed")
}

/// The same barrier-forced overlap as
/// `postgres_submit_with_id_concurrent_same_id_one_row_test` above, but the
/// two concurrent callers submit *different* inputs under the identical
/// `SubmissionId`. Whichever the barrier releases first commits and is
/// `Inserted`; the other's own `record_receipt` still hits the same real
/// `23505` against that just-committed receipt, but this time
/// `reconcile_from_receipt`'s fingerprint check does not match (different
/// `encoded_input`, hence a different `request_sha256`) — it stays
/// `SubmissionConflict` rather than converging, exactly like a sequential
/// different-input-same-id retry, and exactly one row is ever persisted.
pub fn postgres_submit_with_id_concurrent_different_input_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_with_id_concurrent_different_input_test(database_url)
  }
}

fn run_submit_with_id_concurrent_different_input_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_concurrent_diff_" <> suffix,
  )
  let worker_id = "plain-concurrent-diff.echo-" <> suffix
  let worker_def = unique_test_worker_versioned(worker_id, "v1")
  let test_queue = "plain-concurrent-diff-" <> suffix
  let submission_text = "plain-concurrent-diff-" <> suffix

  let lock_key = unique_test_lock_key(1)
  let cleanup_trigger =
    install_unique_insert_barrier(
      connection,
      "grind_test_plain_concurrent_diff_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_plain_concurrent_diff_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      42,
    )
  })
  spawn_submit(result_b, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      99,
    )
  })

  await_overlap_shape(
    connection,
    plain_insert_query_like,
    plain_insert_query_like,
    2,
    2,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 10_000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 10_000)

  let inserted =
    list.filter_map([outcome_a, outcome_b], fn(outcome) {
      case outcome {
        Ok(submission.Inserted(handle)) -> Ok(handle)
        _ -> Error(Nil)
      }
    })
  let conflicts =
    list.filter([outcome_a, outcome_b], fn(outcome) {
      outcome == Error(submission.SubmissionConflict)
    })
  list.length(inserted) |> should.equal(1)
  list.length(conflicts) |> should.equal(1)
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "submit-with-id-concurrent-different-input-conflict-passed",
  )
}
