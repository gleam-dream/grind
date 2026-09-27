import gleam/dynamic/decode
import gleeunit/should
import grind/internal/unique_admission
import grind/job
import grind/submission
import grind/support/env.{database_url, mark_database_test_executed}
import grind/support/submissions.{submit_keep_existing, unique_test_worker}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_database}
import grind/support/unique_rows.{force_job_timestamp}
import grind/unique
import pog

/// Increment 6(a): the shared `@internal` period predicate at an exact
/// database-time instant, against literal timestamps only (no table
/// involved) — `now = ts + period` is inclusive (`true`); one microsecond
/// later is not.
pub fn postgres_unique_period_predicate_matches_the_exact_instant_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_period_predicate_exact_instant_test(database_url)
  }
}

fn run_period_predicate_exact_instant_test(database_url: String) -> Nil {
  use _database, connection <- with_unique_database(
    database_url,
    "grind_unique_period_predicate",
  )
  let ts = "timestamptz '2024-01-01 00:00:00+00'"
  let now_at_boundary = "timestamptz '2024-01-01 00:00:05+00'"
  let now_past_boundary = "timestamptz '2024-01-01 00:00:05.000001+00'"
  let period_ms = "5000"

  let assert Ok(returned) =
    pog.query(
      "SELECT "
      <> unique_admission.period_predicate(ts, now_at_boundary, period_ms)
      <> ", "
      <> unique_admission.period_predicate(ts, now_past_boundary, period_ms),
    )
    |> pog.returning({
      use at_boundary <- decode.field(0, decode.bool)
      use past_boundary <- decode.field(1, decode.bool)
      decode.success(#(at_boundary, past_boundary))
    })
    |> pog.execute(on: connection)
  let assert [#(at_boundary, past_boundary)] = returned.rows
  at_boundary |> should.equal(True)
  past_boundary |> should.equal(False)

  mark_database_test_executed("unique-period-predicate-exact-instant-passed")
}

/// Increment 6(b): `FromInsertion` at the database clock, live through
/// `submit_unique` (not the predicate alone) — 58 seconds inside a 60-second
/// window still conflicts; 62 seconds outside it does not.
pub fn postgres_submit_unique_from_insertion_period_matches_database_time_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_period_from_insertion_boundary_test(database_url)
  }
}

fn run_period_from_insertion_boundary_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_from_insertion",
  )
  let worker_def = unique_test_worker("unique.from-insertion-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(60_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "from-insertion-" <> suffix

  // Inserted 58 seconds ago: still inside the 60-second window.
  let assert Ok(submission.Inserted(handle_within)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-insertion-within-1-" <> suffix,
      worker_def,
      1,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle_within),
    "inserted_at",
    "clock_timestamp() - interval '58 seconds'",
  )
  let assert Ok(submission.Existing(conflict_within)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-insertion-within-2-" <> suffix,
      worker_def,
      1,
      policy,
    )
  submission.conflict_job_id(conflict_within)
  |> should.equal(job.id_value(handle_within))

  // Inserted 62 seconds ago: outside the 60-second window.
  let assert Ok(submission.Inserted(handle_outside)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-insertion-outside-1-" <> suffix,
      worker_def,
      2,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle_outside),
    "inserted_at",
    "clock_timestamp() - interval '62 seconds'",
  )
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-insertion-outside-2-" <> suffix,
      worker_def,
      2,
      policy,
    )

  mark_database_test_executed("unique-period-from-insertion-boundary-passed")
}

/// Increment 6(c): `FromSchedule`, "compared to the scheduled time" (Oban's
/// own framing) — a row whose `available_at` is 121 seconds in the past no
/// longer conflicts under a 120-second period; 119 seconds still does.
pub fn postgres_submit_unique_from_schedule_period_past_boundary_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_period_from_schedule_past_boundary_test(database_url)
  }
}

fn run_period_from_schedule_past_boundary_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_from_schedule_past",
  )
  let worker_def = unique_test_worker("unique.from-schedule-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(120_000, unique.FromSchedule)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "from-schedule-past-" <> suffix

  // Scheduled 121 seconds in the past: outside the 120-second window.
  let assert Ok(submission.Inserted(handle_outside)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-outside-1-" <> suffix,
      worker_def,
      1,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle_outside),
    "available_at",
    "clock_timestamp() - interval '121 seconds'",
  )
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-outside-2-" <> suffix,
      worker_def,
      1,
      policy,
    )

  // Scheduled 119 seconds in the past: still inside the 120-second window.
  let assert Ok(submission.Inserted(handle_inside)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-inside-1-" <> suffix,
      worker_def,
      2,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle_inside),
    "available_at",
    "clock_timestamp() - interval '119 seconds'",
  )
  let assert Ok(submission.Existing(conflict_inside)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-inside-2-" <> suffix,
      worker_def,
      2,
      policy,
    )
  submission.conflict_job_id(conflict_inside)
  |> should.equal(job.id_value(handle_inside))

  mark_database_test_executed(
    "unique-period-from-schedule-past-boundary-passed",
  )
}

/// Increment 6(d): a future `FromSchedule` deadline extends the occupancy
/// window well beyond what the same period length would already have let
/// expire under `FromInsertion` — the same row, the same 60-second period,
/// inserted 5 minutes ago (long past a `FromInsertion` window) but scheduled
/// 5 minutes from now (comfortably inside a `FromSchedule` window).
pub fn postgres_submit_unique_from_schedule_future_extends_window_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_period_from_schedule_future_extends_window_test(database_url)
  }
}

fn run_period_from_schedule_future_extends_window_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_from_schedule_future",
  )
  let worker_def = unique_test_worker("unique.from-schedule-future-" <> suffix)
  let assert Ok(period_from_schedule) =
    unique.within_milliseconds(60_000, unique.FromSchedule)
  let policy_from_schedule =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period_from_schedule,
      unique.Incomplete,
    )
  let assert Ok(period_from_insertion) =
    unique.within_milliseconds(60_000, unique.FromInsertion)
  let policy_from_insertion =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period_from_insertion,
      unique.Incomplete,
    )
  let test_queue = "from-schedule-future-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-future-1-" <> suffix,
      worker_def,
      9,
      policy_from_schedule,
    )
  let job_id = job.id_value(handle)
  force_job_timestamp(
    connection,
    job_id,
    "inserted_at",
    "clock_timestamp() - interval '300 seconds'",
  )
  force_job_timestamp(
    connection,
    job_id,
    "available_at",
    "clock_timestamp() + interval '300 seconds'",
  )

  // `FromInsertion`, same key: the row's insertion is long past the
  // 60-second period, so it no longer conflicts.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-future-2-" <> suffix,
      worker_def,
      9,
      policy_from_insertion,
    )

  // `FromSchedule`, same key, same 60-second period: measured from a
  // schedule 5 minutes in the future, it still covers the original row.
  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-future-3-" <> suffix,
      worker_def,
      9,
      policy_from_schedule,
    )
  submission.conflict_job_id(conflict) |> should.equal(job_id)

  mark_database_test_executed(
    "unique-period-from-schedule-future-extends-window-passed",
  )
}

/// Increment 6(e): `while_retained()` has no time boundary at all — a row
/// inserted a year ago still conflicts.
pub fn postgres_submit_unique_while_retained_matches_a_year_old_row_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_period_while_retained_old_row_test(database_url)
  }
}

fn run_period_while_retained_old_row_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_while_retained",
  )
  let worker_def = unique_test_worker("unique.while-retained-" <> suffix)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      unique.while_retained(),
      unique.Incomplete,
    )
  let test_queue = "while-retained-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-while-retained-1-" <> suffix,
      worker_def,
      3,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle),
    "inserted_at",
    "clock_timestamp() - interval '1 year'",
  )

  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-while-retained-2-" <> suffix,
      worker_def,
      3,
      policy,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))

  mark_database_test_executed("unique-period-while-retained-old-row-passed")
}
