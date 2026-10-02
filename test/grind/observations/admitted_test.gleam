import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{None, Some}
import gleeunit/should
import grind/job
import grind/observation
import grind/postgres
import grind/submission
import grind/support/concurrency.{spawn_submit}
import grind/support/env.{
  database_url, mark_database_test_executed, queue_database_url,
}
import grind/support/observers.{detach}
import grind/support/submission_helpers.{submit_with_id_immediately}
import grind/support/submissions.{submit_keep_existing, unique_test_worker}
import grind/support/syncrep.{
  backend_pid_is_alive, install_syncrep_reply_trigger,
  require_syncrep_cluster_configured, terminate_backend, wait_for_backend_gone,
  wait_for_syncrep_trigger_backend,
}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_database}
import grind/unique
import grind/worker
import pog
import sinal

// -- Round 2 `[grind, job, *]` observations --------------------------------
//
// Every descriptor below is emitted through the same `sinal/forwarder` as
// `[grind, job, acknowledged]` above; see `grind/observation`'s module
// documentation for the shared delivery semantics. Each descriptor gets at
// least one test proving emission (with the exact metadata a consumer would
// read) and one proving a read-only/negative outcome emits nothing, using
// the same "assert the very next observation on this exact channel is a
// known sentinel" technique `assert_next_observation_is_sentinel` documents
// above (per-producer FIFO through one Forwarder makes this deterministic,
// not racy).

pub fn postgres_admitted_observation_plain_submit_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_plain_submit_test(database_url)
  }
}

/// Plain `submit`/`submit_at`: `committed_state` and `available_at_unix_ms`
/// come from the insert's own `RETURNING`, `submission_id` is `None`, and
/// `confirmation` is always `Replied` (a plain submission has no receipt
/// concept to reconcile from).
fn run_admitted_plain_submit_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "admitted-plain-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "admitted-plain-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define("admitted.plain", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(handle) =
    postgres.submit(database, "admitted-plain", definition, 5)
  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.AdmittedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.ref.queue |> should.equal("admitted-plain")
  metadata.ref.worker_id |> should.equal("admitted.plain")
  metadata.ref.worker_version |> should.equal("v1")
  metadata.committed_state |> should.equal(job.Queued)
  metadata.submission_id |> should.equal(None)
  metadata.confirmation |> should.equal(observation.Replied)
  let assert Some(immediate_available_at) = metadata.available_at_unix_ms
  immediate_available_at |> should.not_equal(0)

  let assert Ok(future) = job.available_at(immediate_available_at + 3_600_000)
  let assert Ok(scheduled_handle) =
    postgres.submit_at(database, "admitted-plain", definition, 6, future)
  let assert Ok(#(_, scheduled_metadata)) =
    process.receive(signal, within: 5000)
  scheduled_metadata.ref.job_id
  |> should.equal(job.id_value(scheduled_handle))
  scheduled_metadata.committed_state |> should.equal(job.Scheduled)
  scheduled_metadata.available_at_unix_ms
  |> should.equal(Some(immediate_available_at + 3_600_000))
  scheduled_metadata.confirmation |> should.equal(observation.Replied)

  // A `submit_at` whose target is already in the past by the *database's*
  // clock still commits `queued` (`grind_jobs`'s own `CASE ... <=
  // clock_timestamp()`), not `scheduled` — proving `committed_state` is read
  // back from that same `RETURNING`, never inferred client-side from the
  // request's own "immediate vs. future" intent.
  let assert Ok(past) = job.available_at(immediate_available_at - 3_600_000)
  let assert Ok(past_handle) =
    postgres.submit_at(database, "admitted-plain", definition, 7, past)
  let assert Ok(#(_, past_metadata)) = process.receive(signal, within: 5000)
  past_metadata.ref.job_id |> should.equal(job.id_value(past_handle))
  past_metadata.committed_state |> should.equal(job.Queued)
  mark_database_test_executed("admitted-observation-plain-submit-passed")
}

pub fn postgres_admitted_observation_unique_inserted_and_reconciled_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_admitted_unique_inserted_reconciled_test(database_url)
  }
}

/// A fresh unique admission (`Inserted`) is `Replied`, with a known
/// `available_at_unix_ms`. Replaying the exact same `submission_id` and
/// request hits the receipt inside `admission_transaction` itself before any
/// candidate row is even looked up — proven committed by that receipt read,
/// not a fresh write — so the second observation is `Reconciled` with
/// `available_at_unix_ms: None` (the receipt-matched path never re-derives
/// it, the same limitation `acknowledged` documents for its own
/// receipt-matched commits).
fn run_admitted_unique_inserted_reconciled_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_admitted_unique_inserted",
  )
  let worker_def = unique_test_worker("admitted.unique.inserted-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "admitted-unique-" <> suffix
  let submission_text = "admitted-unique-1-" <> suffix

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
      policy,
    )
  let assert Ok(#(_, first_metadata)) = process.receive(signal, within: 5000)
  first_metadata.ref.job_id |> should.equal(job.id_value(handle))
  first_metadata.submission_id |> should.equal(Some(submission_text))
  first_metadata.confirmation |> should.equal(observation.Replied)
  first_metadata.committed_state |> should.equal(job.Queued)
  let assert Some(_) = first_metadata.available_at_unix_ms

  let assert Ok(submission.Inserted(replayed)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
      policy,
    )
  job.id_value(replayed) |> should.equal(job.id_value(handle))
  let assert Ok(#(_, second_metadata)) = process.receive(signal, within: 5000)
  second_metadata.ref.job_id |> should.equal(job.id_value(handle))
  second_metadata.submission_id |> should.equal(Some(submission_text))
  second_metadata.confirmation |> should.equal(observation.Reconciled)
  second_metadata.available_at_unix_ms |> should.equal(None)
  mark_database_test_executed(
    "admitted-observation-unique-inserted-reconciled-passed",
  )
}

pub fn postgres_admitted_observation_unique_existing_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_unique_existing_test(database_url)
  }
}

/// A distinct `submission_id` that lands on an already-occupied uniqueness
/// key (`Existing`) is its own fresh commit (`Replied`) — this exact
/// submission's own receipt row is what got written, even though the job row
/// itself is untouched.
fn run_admitted_unique_existing_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_admitted_unique_existing",
  )
  let worker_def = unique_test_worker("admitted.unique.existing-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "admitted-unique-existing-" <> suffix

  // Attached before any submission: `forwarder.emit` only sends the event to
  // the forwarder process and returns — it does not wait for that process to
  // actually run the attached handler — so a handler attached only *after* a
  // call returns can still race that call's own not-yet-processed emission.
  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-existing-1-" <> suffix,
      worker_def,
      3,
      policy,
    )
  let assert Ok(#(_, first_metadata)) = process.receive(signal, within: 5000)
  first_metadata.submission_id
  |> should.equal(Some("admitted-existing-1-" <> suffix))
  first_metadata.confirmation |> should.equal(observation.Replied)

  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-existing-2-" <> suffix,
      worker_def,
      3,
      policy,
    )
  let assert Ok(#(_, metadata)) = process.receive(signal, within: 5000)
  metadata.ref.job_id |> should.equal(submission.conflict_job_id(conflict))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.submission_id |> should.equal(Some("admitted-existing-2-" <> suffix))
  metadata.confirmation |> should.equal(observation.Replied)
  metadata.committed_state |> should.equal(job.Queued)
  mark_database_test_executed(
    "admitted-observation-unique-existing-conflict-passed",
  )
}

pub fn postgres_admitted_observation_existing_over_executing_available_at_none_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_existing_over_executing_test(database_url)
  }
}

/// `available_at_unix_ms` is `Some` only when the committed/observed state
/// is `Queued`, `Scheduled`, or `Retryable` — an `Existing` conflict can
/// land on any other policy-eligible state too (`Incomplete` reaches as far
/// as `Executing`), where the row's raw `available_at` is not a real
/// next-run time and must be reported `None`.
fn run_admitted_existing_over_executing_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_admitted_existing_executing_" <> suffix,
  )
  let worker_def = unique_test_worker("admitted.existing.executing-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "admitted-existing-executing-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-existing-executing-1-" <> suffix,
      worker_def,
      9,
      policy,
    )
  // Force the row into `executing` directly (no queue actor needed) so the
  // later `Existing` conflict below lands on a non-eligibility state.
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 1, attempt_epoch = 1, attempt_owner = 'x', lease_expires_at = clock_timestamp() + interval '1 hour' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-existing-executing-2-" <> suffix,
      worker_def,
      9,
      policy,
    )
  submission.conflict_state(conflict) |> should.equal(job.Executing)
  let assert Ok(#(_, metadata)) = process.receive(signal, within: 5000)
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.committed_state |> should.equal(job.Executing)
  metadata.available_at_unix_ms |> should.equal(None)
  mark_database_test_executed(
    "admitted-observation-existing-over-executing-available-at-none-passed",
  )
}

pub fn postgres_admitted_observation_absent_on_submission_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_absent_on_conflict_test(database_url)
  }
}

/// `SubmissionConflict` (the same `submission_id` reused for a materially
/// different request) never reaches the database's own commit, so it must
/// never emit — proven here by a subsequent, distinct submission through the
/// exact same producer/forwarder arriving as the very next `admitted`
/// observation.
fn run_admitted_absent_on_conflict_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_admitted_conflict",
  )
  let worker_def = unique_test_worker("admitted.conflict-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "admitted-conflict-" <> suffix
  let submission_text = "admitted-conflict-1-" <> suffix

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
      policy,
    )
  let assert Ok(#(_, _)) = process.receive(signal, within: 5000)

  submit_keep_existing(
    database,
    test_queue,
    submission_text,
    worker_def,
    4,
    policy,
  )
  |> should.equal(Error(submission.SubmissionConflict))

  let assert Ok(submission.Inserted(sentinel_handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-conflict-sentinel-" <> suffix,
      worker_def,
      5,
      policy,
    )
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  mark_database_test_executed(
    "admitted-observation-absent-on-submission-conflict-passed",
  )
}

pub fn postgres_admitted_observation_absent_from_reconcile_unique_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_admitted_absent_from_reconcile_unique_test(database_url)
  }
}

/// Public `reconcile_unique` follows round 1's decision for
/// `reconcile_acknowledgement`: it never emits, even though it can recover a
/// genuine committed outcome (`Inserted`) from a `CommitUnknown` command —
/// reusing the exact "(d) committed, reply lost, pool closed" scenario from
/// `run_unique_committed_reply_lost_store_unavailable_test` above. Both are
/// pure receipt reads offered for a caller to recover its own return value
/// after a lost reply, not a fresh proof of commit tied to a call this
/// module owns end to end — `submit_unique` itself already emits
/// `Reconciled` for the equivalent in-call recovery (see the
/// inserted-and-reconciled test above); a separate, possibly much later
/// `reconcile_unique` call must not double-report the same commit.
fn run_admitted_absent_from_reconcile_unique_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

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

  let worker_def = unique_test_worker("admitted.reconcile-" <> suffix)
  let test_queue = "admitted-reconcile-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "admitted-reconcile-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    observer_connection,
    "grind_test_admitted_reconcile_" <> suffix,
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
      1,
      policy,
    )
  })
  let assert Ok(backend_pid) =
    wait_for_syncrep_trigger_backend(observer_connection, 300)

  let _ = postgres.close(database)
  let assert Ok(Error(submission.CommitUnknown(pending))) =
    process.receive(reply, within: 10_000)
  // No `admitted` observation for the `CommitUnknown` outcome itself.
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  let assert Ok(reopened_validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(reopened) = postgres.start(reopened_validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  // Zombie still parked: a pure receipt lookup still finds nothing.
  let assert Error(submission.CommitUnknown(_)) =
    postgres.reconcile_unique(reopened, pending)
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  terminate_backend(observer_connection, backend_pid) |> should.equal(True)
  let assert Ok(Nil) =
    wait_for_backend_gone(observer_connection, backend_pid, 300)

  let assert Ok(submission.Inserted(handle)) =
    postgres.reconcile_unique(reopened, pending)
  postgres.arguments(reopened, handle) |> should.equal(Ok(1))
  // Still nothing on the `admitted` channel from `reconcile_unique` itself,
  // even though it just recovered a genuinely committed `Inserted` outcome.
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  // Sentinel: the producer/forwarder itself is still alive and correctly
  // wired for a genuine fresh admission afterward.
  let assert Ok(submission.Inserted(sentinel_handle)) =
    submit_keep_existing(
      reopened,
      test_queue,
      "admitted-reconcile-sentinel-" <> suffix,
      worker_def,
      2,
      policy,
    )
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  mark_database_test_executed(
    "admitted-observation-absent-from-reconcile-unique-passed",
  )
}

pub fn postgres_admitted_observation_in_call_post_commit_unknown_reconciled_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_in_call_reconciled_test(database_url)
  }
}

/// The other `Reconciled` path, distinct from the in-transaction receipt hit
/// the inserted-and-reconciled test above proves: `submit_unique`'s own
/// commit reply is lost (the `SyncRep`-park-then-terminate harness), so its
/// transaction result comes back as `pog.TransactionQueryError` — but
/// `run`'s own follow-up `reconcile_from_receipt` call, made within this
/// exact same `submit_unique` call before it ever returns, finds the
/// now-visible receipt and resolves `Ok(Inserted(handle))` transparently
/// (`run_unique_committed_reply_lost_test`'s own scenario). This is proven
/// committed by a receipt read, not a fresh write, so exactly one `admitted`
/// event is emitted, `Reconciled`, with `available_at_unix_ms: None`.
fn run_admitted_in_call_reconciled_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)

  let worker_def = unique_test_worker("admitted.in-call-reconciled-" <> suffix)
  let test_queue = "admitted-in-call-reconciled-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "admitted-in-call-reconciled-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_admitted_in_call_reconciled_" <> suffix,
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
      7,
      policy,
    )
  })

  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Ok(submission.Inserted(handle))) =
    process.receive(reply, within: 10_000)
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)

  let assert Ok(#(_, metadata)) = process.receive(signal, within: 5000)
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.submission_id |> should.equal(Some(submission_text))
  metadata.confirmation |> should.equal(observation.Reconciled)
  metadata.available_at_unix_ms |> should.equal(None)

  // Exactly one: the sentinel through the exact same producer is the very
  // next observation on this channel.
  let assert Ok(submission.Inserted(sentinel_handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-in-call-reconciled-sentinel-" <> suffix,
      worker_def,
      8,
      policy,
    )
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  mark_database_test_executed(
    "admitted-observation-in-call-post-commit-unknown-reconciled-passed",
  )
}

/// `submit_with_id`'s own `[grind, job, admitted]` observation: `Some`
/// `submission_id`, `Replied` on the first genuine commit, `Reconciled` on a
/// same-id/same-request replay resolved from the receipt without a fresh
/// write — the same contract `submit_unique`'s admitted observation already
/// has (`postgres_admitted_observation_unique_inserted_and_reconciled_test`),
/// proven here for the "no policy" path.
pub fn postgres_admitted_observation_submit_with_id_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_admitted_observation_submit_with_id_test(database_url)
  }
}

fn run_admitted_observation_submit_with_id_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_admitted_submit_with_id_" <> suffix,
  )
  let worker_def = unique_test_worker("admitted.submit-with-id-" <> suffix)
  let test_queue = "admitted-submit-with-id-" <> suffix
  let submission_text = "admitted-submit-with-id-" <> suffix

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Inserted(handle)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
    )
  let assert Ok(#(_, first_metadata)) = process.receive(signal, within: 5000)
  first_metadata.ref.job_id |> should.equal(job.id_value(handle))
  first_metadata.submission_id |> should.equal(Some(submission_text))
  first_metadata.confirmation |> should.equal(observation.Replied)
  first_metadata.committed_state |> should.equal(job.Queued)
  let assert Some(_) = first_metadata.available_at_unix_ms

  let assert Ok(submission.Inserted(replayed)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
    )
  job.id_value(replayed) |> should.equal(job.id_value(handle))
  let assert Ok(#(_, second_metadata)) = process.receive(signal, within: 5000)
  second_metadata.ref.job_id |> should.equal(job.id_value(handle))
  second_metadata.submission_id |> should.equal(Some(submission_text))
  second_metadata.confirmation |> should.equal(observation.Reconciled)
  second_metadata.available_at_unix_ms |> should.equal(None)

  mark_database_test_executed("admitted-observation-submit-with-id-passed")
}
