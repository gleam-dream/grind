import exception
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/submission
import grind/unique
import grind_consumer/support/env
import grind_consumer/support/wait
import grind_consumer/support/workers

// -- Uniqueness admission through the public API -----------------------------
//
// `grind/unique`/`postgres.submit_unique`/`postgres.reconcile_unique` are
// exercised here through public imports only, mirroring the rest of this
// file's discipline: no `@internal` function, no `grind/postgres.Database`
// internals, no raw `pog` connection. See `docs/UNIQUENESS-CONTRACT.md` for
// the full contract these calls implement.

pub fn public_consumer_unique_admission_existing_conflict_and_retry_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_unique_admission_existing_conflict_and_retry_test(url)
  }
}

fn run_unique_admission_existing_conflict_and_retry_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_def = workers.unique_echo_worker("consumer.unique_echo")
  let assert Ok(workers) = registry.new("consumer-unique")
  let assert Ok(workers) = registry.register(workers, worker_def)

  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "consumer-unique"

  let assert Ok(first_submission) =
    submission.submission_id("consumer-unique-first")
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      first_submission,
      worker_def,
      42,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )

  // A second, independently identified admission with the same key hits the
  // still-queued conflict instead of inserting a new row.
  let assert Ok(second_submission) =
    submission.submission_id("consumer-unique-second")
  let assert Ok(submission.Existing(conflict)) =
    postgres.submit_unique(
      database,
      test_queue,
      second_submission,
      worker_def,
      42,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))

  // `Conflict` is not a handle: the caller rebinds it through the same
  // durable-id path used after a restart before reading typed state.
  let assert Ok(bound) =
    postgres.bind_handle(
      database,
      worker_def,
      submission.conflict_job_id(conflict),
    )
  postgres.state(database, bound) |> should.equal(Ok(job.Queued))

  // Replaying the *original* SubmissionId returns the receipt's own recorded
  // decision -- the original job id, as `Inserted` again -- rather than
  // treating the still-present row as a fresh conflict.
  let assert Ok(submission.Inserted(replayed)) =
    postgres.submit_unique(
      database,
      test_queue,
      first_submission,
      worker_def,
      42,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  job.id_value(replayed) |> should.equal(job.id_value(handle))

  let assert Ok(consumer) = queue.start(database, workers, env.manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  wait.await_state(database, bound, job.Succeeded, 250) |> should.equal(True)
  postgres.outcome(database, bound) |> should.equal(Ok(job.SucceededWith("42")))

  env.mark("consumer-unique-admission-existing-conflict-retry-passed")
}

/// `postgres.submit_with_id` — the retry-safe plain submit path, exercised
/// from a consumer of only Grind's public API. A same-id, same-input retry
/// after a genuine first commit converges on the original job (`Inserted`,
/// the same id, no second row), unlike a plain `submit`/`submit_at` retry.
pub fn public_consumer_submit_with_id_retry_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_submit_with_id_retry_test(url)
  }
}

fn run_submit_with_id_retry_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_def = workers.unique_echo_worker("consumer.submit_with_id_echo")
  let assert Ok(workers) = registry.new("consumer-submit-with-id")
  let assert Ok(workers) = registry.register(workers, worker_def)
  let test_queue = "consumer-submit-with-id"

  let assert Ok(submission_id) =
    submission.submission_id("consumer-with-id-once")
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_with_id(
      database,
      test_queue,
      submission_id,
      worker_def,
      42,
      submission.Immediately,
    )

  // A retry with the identical `SubmissionId` and request converges on the
  // exact same job -- no second row -- rather than risking a duplicate the
  // way a plain `submit` retry could.
  let assert Ok(submission.Inserted(retried)) =
    postgres.submit_with_id(
      database,
      test_queue,
      submission_id,
      worker_def,
      42,
      submission.Immediately,
    )
  job.id_value(retried) |> should.equal(job.id_value(handle))

  // A different request under the same id is a genuine conflict, not a
  // silent replay.
  postgres.submit_with_id(
    database,
    test_queue,
    submission_id,
    worker_def,
    43,
    submission.Immediately,
  )
  |> should.equal(Error(submission.SubmissionConflict))

  let assert Ok(consumer) = queue.start(database, workers, env.manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  wait.await_state(database, handle, job.Succeeded, 250) |> should.equal(True)
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("42")))

  env.mark("consumer-submit-with-id-retry-passed")
}

pub fn public_consumer_unique_reschedule_across_queues_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_unique_reschedule_across_queues_test(url)
  }
}

fn run_unique_reschedule_across_queues_test(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_def = workers.unique_echo_worker("consumer.unique_reschedule_echo")
  let assert Ok(workers) = registry.new("consumer-unique-across-a")
  let assert Ok(workers) = registry.register(workers, worker_def)

  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.AcrossQueues,
      period,
      unique.ScheduledOnly,
    )

  // Seeded far enough in the future that it is genuinely `scheduled`, not
  // already due.
  let far_future_ms = env.now_unix_ms() + 3_600_000
  let assert Ok(far_future_at) = job.available_at(far_future_ms)
  let assert Ok(seed_submission) =
    submission.submission_id("consumer-unique-across-seed")
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      "consumer-unique-across-a",
      seed_submission,
      worker_def,
      7,
      submission.At(far_future_at),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  // A second submission through a *different* queue, under `AcrossQueues`,
  // reschedules the same key's still-scheduled row to a near-future time --
  // it never inserts a second row in its own submitting queue.
  let soon_ms = env.now_unix_ms() + 50
  let assert Ok(soon_at) = job.available_at(soon_ms)
  let assert Ok(reschedule_submission) =
    submission.submission_id("consumer-unique-across-reschedule")
  let assert Ok(submission.Rescheduled(conflict)) =
    postgres.submit_unique(
      database,
      "consumer-unique-across-b",
      reschedule_submission,
      worker_def,
      7,
      submission.Immediately,
      policy,
      unique.RescheduleScheduledTo(soon_at),
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))
  // The row's actual queue is the one it was originally inserted under, not
  // the rescheduling submission's own queue.
  submission.conflict_queue(conflict)
  |> should.equal("consumer-unique-across-a")

  let assert Ok(bound) =
    postgres.bind_handle(
      database,
      worker_def,
      submission.conflict_job_id(conflict),
    )

  let assert Ok(consumer) = queue.start(database, workers, env.manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  wait.await_claim(consumer, 250) |> should.equal(Ok(True))

  wait.await_state(database, bound, job.Succeeded, 250) |> should.equal(True)
  postgres.outcome(database, bound) |> should.equal(Ok(job.SucceededWith("7")))

  env.mark("consumer-unique-reschedule-across-queues-passed")
}
