import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleeunit/should
import grind/internal/job
import grind/internal/postgres
import grind/internal/submission
import grind/internal/unique
import grind/internal/worker
import grind/support/env.{database_url, mark_database_test_executed}
import grind/support/submissions.{submit_keep_existing, unique_test_worker}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_database}
import grind/support/unique_rows.{RawInput, encode_raw_input, raw_input_decoder}

pub fn unique_period_validation_test() {
  unique.within_milliseconds(0, unique.FromInsertion)
  |> should.equal(Error(unique.NonPositivePeriod))
  unique.within_milliseconds(-1, unique.FromSchedule)
  |> should.equal(Error(unique.NonPositivePeriod))
  let too_large = worker.retry_delay_maximum_milliseconds() + 1
  unique.within_milliseconds(too_large, unique.FromInsertion)
  |> should.equal(Error(unique.PeriodAbovePrecisionBound))
  let assert Ok(_) = unique.within_milliseconds(1000, unique.FromInsertion)
  let assert Ok(_) =
    unique.within_milliseconds(
      worker.retry_delay_maximum_milliseconds(),
      unique.FromSchedule,
    )
  Nil
}

pub fn unique_key_and_submission_id_validation_test() {
  let assert Ok(codec) =
    worker.codec("unique-key-v1", worker.infallible(json.int), decode.int)
  unique.selected("", fn(input: Int) { input }, codec)
  |> should.equal(Error(unique.EmptyKeyName))
  let assert Ok(_) = unique.selected("account", fn(input: Int) { input }, codec)

  submission.submission_id("")
  |> should.equal(Error(submission.EmptySubmissionId))
  let assert Ok(id) = submission.submission_id("abc-123")
  submission.submission_id_value(id) |> should.equal("abc-123")
}

pub fn postgres_unique_lock_wait_must_be_positive_test() {
  let base = postgres.settings("postgres://grind@127.0.0.1:5432/unused")

  base
  |> postgres.with_unique_lock_wait(0)
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidUniqueLockWait))

  base
  |> postgres.with_unique_lock_wait(-5)
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidUniqueLockWait))

  case base |> postgres.with_unique_lock_wait(200) |> postgres.validate {
    Ok(_) -> Nil
    Error(_) -> should.fail()
  }
}

pub fn postgres_submit_unique_rejects_before_touching_storage_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_unique_pre_storage_rejection_test(database_url)
  }
}

fn run_submit_unique_pre_storage_rejection_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  // Closed immediately: any query attempt beyond this point would fail at
  // the storage boundary, so a passing test here proves the empty-queue
  // rejection is a pure check that runs before `submit_unique` ever reaches
  // the database.
  let _ = postgres.close(database)

  let worker_def = unique_test_worker("unique.closed-" <> suffix)
  let assert Ok(period) = unique.within_milliseconds(1000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  submit_keep_existing(
    database,
    "",
    "unique-closed-" <> suffix,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.EmptyQueueName))

  mark_database_test_executed("unique-pre-storage-rejections-passed")
}

pub fn postgres_submit_unique_admits_and_detects_existing_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_existing_conflict_test(database_url)
  }
}

fn run_submit_unique_existing_conflict_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_pool",
  )
  let worker_def = unique_test_worker("unique.existing-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "default-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-existing-1-" <> suffix,
      worker_def,
      7,
      policy,
    )

  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-existing-2-" <> suffix,
      worker_def,
      7,
      policy,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))
  submission.conflict_queue(conflict) |> should.equal(test_queue)
  submission.conflict_state(conflict) |> should.equal(job.Queued)

  let assert Ok(bound) =
    postgres.bind_handle(
      database,
      worker_def,
      submission.conflict_job_id(conflict),
    )
  postgres.arguments(database, bound) |> should.equal(Ok(7))

  let assert Ok(submission.Inserted(other_handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-existing-3-" <> suffix,
      worker_def,
      8,
      policy,
    )
  job.id_value(other_handle) |> should.not_equal(job.id_value(handle))

  mark_database_test_executed("unique-admission-existing-conflict-passed")
}

pub fn postgres_submit_unique_json_equality_matches_postgres_jsonb_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_json_equality_test(database_url)
  }
}

fn run_submit_unique_json_equality_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_json_pool",
  )
  let assert Ok(input_codec) =
    worker.codec(
      "unique-json-input-" <> suffix <> "-v1",
      worker.infallible(encode_raw_input),
      raw_input_decoder(),
    )
  let assert Ok(output_codec) =
    worker.codec(
      "unique-json-output-" <> suffix <> "-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(
      "unique.json-equality-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(_) { Ok("done") },
    )
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "json-equality-" <> suffix

  let admit = fn(tag: String, value: json.Json) {
    submit_keep_existing(
      database,
      test_queue,
      tag <> "-" <> suffix,
      worker_def,
      RawInput(value),
      policy,
    )
  }

  // Field order is irrelevant: {"a":1,"b":2} conflicts with {"b":2,"a":1}.
  let assert Ok(submission.Inserted(_)) =
    admit(
      "field-order-1",
      json.object([#("a", json.int(1)), #("b", json.int(2))]),
    )
  let assert Ok(submission.Existing(_)) =
    admit(
      "field-order-2",
      json.object([#("b", json.int(2)), #("a", json.int(1))]),
    )

  // 1 and 1.0 are distinct scalars under PostgreSQL's own jsonb::text
  // rendering: a deliberate departure from a cross-language canonical JSON
  // equality (see https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md).
  let assert Ok(submission.Inserted(_)) = admit("numeric-int", json.int(1))
  let assert Ok(submission.Inserted(_)) =
    admit("numeric-float", json.float(1.0))

  // Array order is significant.
  let assert Ok(submission.Inserted(_)) =
    admit("array-order-1", json.array([1, 2], of: json.int))
  let assert Ok(submission.Inserted(_)) =
    admit("array-order-2", json.array([2, 1], of: json.int))

  // {id:1} does not conflict with {id:1, extra:2}: exact equality, not
  // Oban's containment semantics.
  let assert Ok(submission.Inserted(_)) =
    admit("subset-1", json.object([#("id", json.int(1))]))
  let assert Ok(submission.Inserted(_)) =
    admit(
      "subset-2",
      json.object([#("id", json.int(1)), #("extra", json.int(2))]),
    )

  // An empty object does not conflict with a non-empty one.
  let assert Ok(submission.Inserted(_)) = admit("empty", json.object([]))
  let assert Ok(submission.Inserted(_)) =
    admit("non-empty", json.object([#("a", json.int(1))]))

  mark_database_test_executed("unique-json-equality-cases-passed")
}

pub fn postgres_submit_unique_scopes_key_to_worker_identity_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_worker_identity_test(database_url)
  }
}

fn run_submit_unique_worker_identity_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_worker_pool",
  )
  let assert Ok(input_codec) =
    worker.codec(
      "unique-identity-input-" <> suffix <> "-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "unique-identity-output-" <> suffix <> "-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let worker_id = "unique.identity-" <> suffix
  let assert Ok(worker_v1) =
    worker.define(worker_id, "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(worker_v2) =
    worker.define(worker_id, "v2", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(other_worker) =
    worker.define(
      "unique.identity-other-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "identity-" <> suffix

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-identity-v1-" <> suffix,
      worker_v1,
      5,
      policy,
    )

  // Same worker id, different worker version: proven-by-mutation isolation
  // (dropping worker_version from the candidate match makes this line see
  // the v1 row above as a false conflict instead of `Inserted`).
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-identity-v2-" <> suffix,
      worker_v2,
      5,
      policy,
    )

  // A different worker id entirely: does not conflict either.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-identity-other-" <> suffix,
      other_worker,
      5,
      policy,
    )

  // The same worker and version, same input, does still conflict.
  let assert Ok(submission.Existing(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-identity-repeat-" <> suffix,
      worker_v1,
      5,
      policy,
    )

  mark_database_test_executed("unique-worker-identity-isolation-passed")
}

pub fn postgres_submit_unique_ignores_plain_submitted_rows_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_plain_submit_test(database_url)
  }
}

fn run_submit_unique_plain_submit_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_plain_pool",
  )
  let worker_def = unique_test_worker("unique.plain-" <> suffix)
  let test_queue = "plain-" <> suffix
  let assert Ok(_plain_handle) =
    postgres.submit(database, test_queue, worker_def, 99)

  // AllRetained deliberately widens eligibility to every persisted state, so
  // a false match here could only come from the plain row's NULL
  // unique_key_contract/unique_key_sha256 being mishandled, not from a
  // states filter accidentally excluding it.
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.AllRetained,
    )
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-plain-1-" <> suffix,
      worker_def,
      99,
      policy,
    )

  mark_database_test_executed("unique-plain-submit-non-participation-passed")
}
