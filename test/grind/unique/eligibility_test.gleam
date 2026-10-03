import exception
import gleam/dynamic/decode
import gleam/json
import gleam/list
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
import grind/support/submissions.{submit_keep_existing, unique_test_worker}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_database}
import grind/support/unique_rows.{force_job_state}

/// Increment 4: `WithinQueue` admits the same key independently in two
/// queues; `AcrossQueues` then conflicts with the earlier of the two rows
/// (lowest id) regardless of which queue it lives in.
pub fn postgres_submit_unique_respects_queue_scope_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_queue_scope_test(database_url)
  }
}

fn run_submit_unique_queue_scope_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_queue_scope",
  )
  let worker_def = unique_test_worker("unique.queue-scope-" <> suffix)
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
  let queue_1 = "q1-" <> suffix
  let queue_2 = "q2-" <> suffix

  let assert Ok(submission.Inserted(handle_q1)) =
    submit_keep_existing(
      database,
      queue_1,
      "unique-queue-scope-q1-" <> suffix,
      worker_def,
      1,
      policy_within,
    )

  // WithinQueue: the same key admits independently in a second queue.
  let assert Ok(submission.Inserted(handle_q2)) =
    submit_keep_existing(
      database,
      queue_2,
      "unique-queue-scope-q2-" <> suffix,
      worker_def,
      1,
      policy_within,
    )
  job.id_value(handle_q2) |> should.not_equal(job.id_value(handle_q1))

  // AcrossQueues submitted against q2 conflicts with the earlier q1 row.
  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      queue_2,
      "unique-queue-scope-across-" <> suffix,
      worker_def,
      1,
      policy_across,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle_q1))
  submission.conflict_queue(conflict) |> should.equal(queue_1)

  mark_database_test_executed("unique-queue-scope-passed")
}

/// Increment 5(a): force each of the 11 persisted states by raw SQL and
/// assert each of the four named `States` groups' eligibility for it.
type StateEligibility {
  StateEligibility(
    stored: String,
    incomplete: Bool,
    scheduled_only: Bool,
    incomplete_or_succeeded: Bool,
    all_retained: Bool,
  )
}

fn unique_state_eligibility_matrix() -> List(StateEligibility) {
  [
    StateEligibility("queued", True, False, True, True),
    StateEligibility("scheduled", True, True, True, True),
    StateEligibility("retryable", True, False, True, True),
    StateEligibility("executing", True, False, True, True),
    StateEligibility("succeeded", False, False, True, True),
    StateEligibility("business_failed", False, False, False, True),
    StateEligibility("runtime_failed", False, False, False, True),
    StateEligibility("contract_mismatch", False, False, False, True),
    StateEligibility("uncertain", True, False, True, True),
    StateEligibility("discarded", False, False, False, True),
    StateEligibility("cancelled", False, False, False, True),
  ]
}

pub fn postgres_submit_unique_state_eligibility_matrix_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_state_eligibility_matrix_test(database_url)
  }
}

fn run_state_eligibility_matrix_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_states_pool",
  )
  let assert Ok(input_codec) =
    worker.codec(
      "unique-states-input-" <> suffix <> "-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "unique-states-output-" <> suffix <> "-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(
      "unique.states-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value) },
    )
  let period = unique.while_retained()
  let test_queue = "states-" <> suffix

  let groups = [
    #(unique.Incomplete, "incomplete", fn(row: StateEligibility) {
      row.incomplete
    }),
    #(unique.ScheduledOnly, "scheduled-only", fn(row: StateEligibility) {
      row.scheduled_only
    }),
    #(
      unique.IncompleteOrSucceeded,
      "incomplete-or-succeeded",
      fn(row: StateEligibility) { row.incomplete_or_succeeded },
    ),
    #(unique.AllRetained, "all-retained", fn(row: StateEligibility) {
      row.all_retained
    }),
  ]

  unique_state_eligibility_matrix()
  |> list.each(fn(row) {
    groups
    |> list.each(fn(group) {
      let #(states, label, eligible) = group
      let key_input = suffix <> "/" <> row.stored <> "/" <> label
      let policy =
        unique.policy(unique.full_input(), unique.WithinQueue, period, states)

      let assert Ok(submission.Inserted(handle)) =
        submit_keep_existing(
          database,
          test_queue,
          "unique-states-insert-" <> key_input,
          worker_def,
          key_input,
          policy,
        )
      let job_id = job.id_value(handle)
      force_job_state(connection, job_id, row.stored)

      let result =
        submit_keep_existing(
          database,
          test_queue,
          "unique-states-check-" <> key_input,
          worker_def,
          key_input,
          policy,
        )
      case eligible(row) {
        True -> {
          let assert Ok(submission.Existing(conflict)) = result
          submission.conflict_job_id(conflict) |> should.equal(job_id)
        }
        False -> {
          let assert Ok(submission.Inserted(_)) = result
          Nil
        }
      }
    })
  })

  mark_database_test_executed("unique-state-eligibility-matrix-passed")
}

/// Increment 5(b): a live transition through a real, manually-driven
/// consumer — `Incomplete` sees the row while it is genuinely `queued`, then
/// stops seeing it once the job has genuinely succeeded, while
/// `IncompleteOrSucceeded` still matches the original succeeded row.
pub fn postgres_submit_unique_state_live_transition_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_state_live_transition_test(database_url)
  }
}

fn run_state_live_transition_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_live_transition",
  )
  let worker_def = unique_test_worker("unique.live-" <> suffix)
  let test_queue = "unique-live-" <> suffix
  let assert Ok(registry_workers) = registry.new(test_queue)
  let assert Ok(registry_workers) =
    registry.register(registry_workers, worker_def)
  let assert Ok(consumer) =
    queue.start(database, registry_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let period = unique.while_retained()
  let policy_incomplete =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let policy_incomplete_or_succeeded =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.IncompleteOrSucceeded,
    )

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-live-1-" <> suffix,
      worker_def,
      99,
      policy_incomplete,
    )

  // While the row is genuinely still queued, `Incomplete` sees it.
  let assert Ok(submission.Existing(conflict_while_queued)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-live-2-" <> suffix,
      worker_def,
      99,
      policy_incomplete,
    )
  submission.conflict_job_id(conflict_while_queued)
  |> should.equal(job.id_value(handle))

  // Run it to a real, committed success.
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  // After a genuine success, `Incomplete` no longer counts it: a fresh row
  // is admitted.
  let assert Ok(submission.Inserted(handle_after_success)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-live-3-" <> suffix,
      worker_def,
      99,
      policy_incomplete,
    )
  job.id_value(handle_after_success)
  |> should.not_equal(job.id_value(handle))

  // `IncompleteOrSucceeded` still matches the original succeeded row (lowest
  // id), not the fresh one `Incomplete` just admitted.
  let assert Ok(submission.Existing(conflict_after_success)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-live-4-" <> suffix,
      worker_def,
      99,
      policy_incomplete_or_succeeded,
    )
  submission.conflict_job_id(conflict_after_success)
  |> should.equal(job.id_value(handle))
  submission.conflict_state(conflict_after_success)
  |> should.equal(job.Succeeded)

  mark_database_test_executed("unique-state-live-transition-passed")
}
