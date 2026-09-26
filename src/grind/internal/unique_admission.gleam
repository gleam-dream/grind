//// The admission transaction behind `postgres.submit_unique`/
//// `reconcile_unique`/`submit_with_id` — one `Request` and transaction
//// serve both, keyed on whether `Request.policy` is `Some` or `None`. See
//// `docs/UNIQUENESS-CONTRACT.md` for the full contract.

import gleam/bit_array
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import grind/internal/sql
import grind/internal/store
import grind/job
import grind/submission
import grind/unique
import grind/worker.{type Worker}
import pog

/// The request fingerprint's hash; see `docs/UNIQUENESS-CONTRACT.md`, Decision 9.
@external(erlang, "grind_unique_ffi", "sha256")
fn sha256(data: BitArray) -> BitArray

/// The uniqueness-policy fields a `submit_unique` request carries and a
/// `submit_with_id` request does not. Held as its own type (rather than
/// flattened into `Request`) so `Request.policy: Option(PolicyPart)` alone
/// expresses "does this request have a uniqueness policy" — no separate
/// sentinel key/scope/period/states/action values are ever constructed for
/// the "no policy" case.
type PolicyPart {
  PolicyPart(
    key_contract: String,
    encoded_key: String,
    scope: unique.QueueScope,
    period: unique.Period,
    states: unique.States,
    on_conflict: unique.ConflictAction,
  )
}

/// Every value the admission transaction needs, gathered once by `submit`/
/// `submit_plain`. `request_sha256` starts empty and is filled in by
/// `fingerprint`, which takes this whole record as its input — one field
/// list, built once. `policy: None` is `submit_with_id`'s "no policy" case:
/// no uniqueness key, no candidate selection, no domain-wide advisory lock,
/// always an insert (or a replayed receipt) — see the module doc comment.
type Request(input, output, error) {
  Request(
    storage_owner: String,
    submission_id: submission.SubmissionId,
    queue: String,
    worker: Worker(input, output, error),
    worker_id: String,
    worker_version: String,
    input_version: String,
    encoded_input: String,
    output_version: String,
    error_version: Option(String),
    max_attempts: Int,
    availability: submission.Availability,
    policy: Option(PolicyPart),
    request_sha256: BitArray,
  )
}

/// The proven-committed outcome of one `submit` call. Mirrors
/// `grind/internal/attempt`'s own internal `AckCommit`, including
/// `via_receipt_match`'s and `available_at_unix_ms`'s meaning — see that
/// type's doc comment.
pub type Commit(input, output, error) {
  Commit(
    outcome: submission.Admission(input, output, error),
    committed_state: job.State,
    available_at_unix_ms: Option(Int),
    via_receipt_match: Bool,
  )
}

/// Admits one job under a uniqueness policy, reached through
/// `postgres.submit_unique`. Rejects an empty queue name before touching
/// any resource.
pub fn submit(
  connection: pog.Connection,
  storage_owner: String,
  lock_wait_ms: Int,
  queue: String,
  submission_id: submission.SubmissionId,
  worker_def: Worker(input, output, error),
  input: input,
  availability: submission.Availability,
  policy: unique.Policy(input),
  on_conflict: unique.ConflictAction,
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  case queue {
    "" -> Error(submission.EmptyQueueName)
    _ -> {
      let unique.PolicyFields(key:, scope:, period:, states:) =
        unique.policy_fields(policy)
      run(
        connection,
        build_request(
          storage_owner,
          submission_id,
          queue,
          worker_def,
          input,
          availability,
          fn(input_version, encoded_input) {
            let #(key_contract, encoded_key) =
              unique.key_material(key, input, input_version, encoded_input)
            Some(PolicyPart(
              key_contract:,
              encoded_key:,
              scope:,
              period:,
              states:,
              on_conflict:,
            ))
          },
        ),
        lock_wait_ms,
      )
    }
  }
}

/// Admits one job with a caller-supplied `SubmissionId` and no uniqueness
/// policy, reached through `postgres.submit_with_id`. Rejects an empty
/// queue name before touching any resource, exactly like `submit` above.
/// No domain-wide advisory lock is ever acquired for this request — see
/// `admission_transaction`'s own doc comment and
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts", for the full
/// justification.
pub fn submit_plain(
  connection: pog.Connection,
  storage_owner: String,
  lock_wait_ms: Int,
  queue: String,
  submission_id: submission.SubmissionId,
  worker_def: Worker(input, output, error),
  input: input,
  availability: submission.Availability,
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  case queue {
    "" -> Error(submission.EmptyQueueName)
    _ ->
      run(
        connection,
        build_request(
          storage_owner,
          submission_id,
          queue,
          worker_def,
          input,
          availability,
          fn(_input_version, _encoded_input) { None },
        ),
        lock_wait_ms,
      )
  }
}

/// Re-reads the receipt a `CommitUnknown` command would have written. A
/// failed lookup (the store is unavailable right now, mid-check) reports
/// `CommitUnknown` again, the same as finding no receipt yet — mirroring
/// `reconcile_unknown_ack`'s `Ok(None) | Error(_) -> QueueAckUnknown`
/// pattern in `grind/postgres`. This is load-bearing, not decorative: the
/// lookup's own transient connectivity failure must never be surfaced as if
/// it answered "did the original admission commit or not" — it did not
/// answer that, and a caller must retry (either `reconcile_unique` again,
/// or a plain `submit_unique` retry) once the store is reachable.
pub fn reconcile(
  connection: pog.Connection,
  pending: submission.PendingSubmission(input, output, error),
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  case reconcile_from_receipt(connection, pending) {
    Ok(#(outcome, _committed_state)) -> Ok(outcome)
    Error(error) -> Error(error)
  }
}

/// Gathers every value the admission transaction needs. `build_policy`
/// receives the submitting worker's own input codec version and encoded
/// input text (so a `Some` policy's key material is derived from exactly
/// the same encoding the request itself carries, never re-encoded) and
/// returns `Some(PolicyPart)` for `submit`'s uniqueness case or `None` for
/// `submit_plain`'s "no policy" case.
fn build_request(
  storage_owner: String,
  submission_id: submission.SubmissionId,
  queue: String,
  worker_def: Worker(input, output, error),
  input: input,
  availability: submission.Availability,
  build_policy: fn(String, String) -> Option(PolicyPart),
) -> Request(input, output, error) {
  let worker.Metadata(
    id: worker_id,
    worker_version:,
    input_version:,
    output_version:,
    error_version:,
    max_attempts:,
  ) = worker.metadata(worker_def)
  let encoded_input = worker.encode_input(worker_def, input)
  let policy = build_policy(input_version, encoded_input)
  let request =
    Request(
      storage_owner:,
      submission_id:,
      queue:,
      worker: worker_def,
      worker_id:,
      worker_version:,
      input_version:,
      encoded_input:,
      output_version:,
      error_version:,
      max_attempts:,
      availability:,
      policy:,
      request_sha256: <<>>,
    )
  Request(..request, request_sha256: fingerprint(request))
}

/// The request fingerprint envelope; see `docs/UNIQUENESS-CONTRACT.md`,
/// Decision 9, and "Admission receipts". Tagged `"grind-unique-request-v1"`
/// when `policy` is `Some` and `"grind-plain-request-v1"` when it is
/// `None` — deliberately different magic strings, so a `SubmissionId`
/// reused between `submit_unique` and `submit_with_id` (or between two
/// calls whose only difference is the presence of a policy) always
/// fingerprint-mismatches and reports `SubmissionConflict`, never a
/// silently-replayed decision from the wrong kind of admission. The
/// policy-specific fields (key, scope, period, states, action) are present
/// in the envelope only when `policy` is `Some`, in the same field order
/// this envelope has always used.
fn fingerprint(request: Request(input, output, error)) -> BitArray {
  let tag = case request.policy {
    Some(_) -> "grind-unique-request-v1"
    None -> "grind-plain-request-v1"
  }
  let policy_fields = case request.policy {
    None -> []
    Some(PolicyPart(
      key_contract:,
      encoded_key:,
      scope:,
      period:,
      states:,
      on_conflict:,
    )) -> {
      let #(period_ms, period_origin) = case unique.period_spec(period) {
        unique.Unbounded -> #(None, None)
        unique.FinitePeriod(ms, from) -> #(
          Some(ms),
          Some(unique.period_origin_label(from)),
        )
      }
      let reschedule_ms = unique.reschedule_target_ms(on_conflict)
      [
        json.string(key_contract),
        json.string(encoded_key),
        json.string(unique.scope_label(scope)),
        json.bool(option.is_some(period_ms)),
        json.nullable(period_ms, json.int),
        json.bool(option.is_some(period_origin)),
        json.nullable(period_origin, json.string),
        json.string(unique.states_label(states)),
        json.string(unique.action_label(on_conflict)),
        json.bool(option.is_some(reschedule_ms)),
        json.nullable(reschedule_ms, json.int),
      ]
    }
  }
  let availability_ms = submission.availability_ms(request.availability)
  let envelope =
    list.flatten([
      [
        json.string(tag),
        json.string(request.queue),
        json.string(request.worker_id),
        json.string(request.worker_version),
        json.string(request.input_version),
        json.string(request.encoded_input),
      ],
      policy_fields,
      [
        json.bool(option.is_some(availability_ms)),
        json.nullable(availability_ms, json.int),
        json.string(request.output_version),
        json.bool(option.is_some(request.error_version)),
        json.nullable(request.error_version, json.string),
        json.int(request.max_attempts),
      ],
    ])
  json.preprocessed_array(envelope)
  |> json.to_string
  |> bit_array.from_string
  |> sha256
}

fn pending_submission(
  request: Request(input, output, error),
) -> submission.PendingSubmission(input, output, error) {
  submission.new_pending_submission(
    request.storage_owner,
    request.submission_id,
    request.worker,
    request.request_sha256,
  )
}

/// Runs the admission transaction and classifies its result. Shared by
/// `submit` (`policy: Some`) and `submit_plain` (`policy: None`); see
/// "Out of scope" in `docs/UNIQUENESS-CONTRACT.md` for why the
/// `TransactionRolledBack(SubmissionConflict)` arm below matters for
/// `Some` too.
fn run(
  connection: pog.Connection,
  request: Request(input, output, error),
  lock_wait_ms: Int,
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  case
    store.transaction_or_checkout_failure(connection, fn(transaction) {
      admission_transaction(transaction, lock_wait_ms, request)
    })
  {
    // Checkout itself failed: no connection was ever handed out, so `BEGIN`
    // never ran. This is knowably not committed — no `PendingSubmission` is
    // constructed, and no receipt lookup is attempted (there is nothing a
    // lookup on this same unreachable store could tell us that we don't
    // already know).
    Error(Nil) -> Error(submission.NotCommitted(pog.ConnectionUnavailable))
    Ok(Ok(commit)) -> Ok(commit)
    // Both leave a receipt that may now be visible; re-reading it resolves
    // the genuine outcome. See "Admission transaction" in
    // `docs/UNIQUENESS-CONTRACT.md`.
    Ok(Error(pog.TransactionRolledBack(submission.SubmissionConflict)))
    | Ok(Error(pog.TransactionQueryError(_))) ->
      case reconcile_from_receipt(connection, pending_submission(request)) {
        Ok(#(outcome, committed_state)) ->
          Ok(Commit(
            outcome:,
            committed_state:,
            available_at_unix_ms: None,
            via_receipt_match: True,
          ))
        Error(error) -> Error(error)
      }
    Ok(Error(pog.TransactionRolledBack(error))) -> Error(error)
  }
}

fn reconcile_from_receipt(
  connection: pog.Connection,
  pending: submission.PendingSubmission(input, output, error),
) -> Result(
  #(submission.Admission(input, output, error), job.State),
  submission.SubmitError(input, output, error),
) {
  let storage_owner = submission.pending_submission_storage_owner(pending)
  let worker_def = submission.pending_submission_worker(pending)
  let request_sha256 = submission.pending_submission_request_sha256(pending)
  case
    find_receipt(
      connection,
      storage_owner,
      submission.submission_id_value(submission.pending_submission_id(pending)),
      worker_def,
      request_sha256,
    )
  {
    Ok(Some(outcome_with_state)) -> Ok(outcome_with_state)
    // Conservative by construction: a failed lookup (the store could not be
    // reached to check) is indistinguishable from "no receipt yet" here, so
    // both report `CommitUnknown` rather than guessing — see `find_receipt`
    // and `classify_query_error` below for how a query failure reaches this
    // same `Ok(None)`-shaped uncertainty rather than its own `Error`.
    Ok(None) -> Error(submission.CommitUnknown(pending))
    // A fingerprint mismatch (or an unrecognized stored `decision`/
    // `observed_state`) is knowable, not uncertain: this exact
    // `SubmissionId` was already durably decided for a *different* request,
    // so a caller retrying `CommitUnknown` forever would never converge.
    // Pass it through unchanged, mirroring `reconcile_unknown_ack`'s
    // `QueueAckCommandConflict` passthrough for the acknowledgement path.
    // Every other error here (a lock-timeout-shaped or otherwise failed
    // lookup) means the check itself could not run, which is exactly what
    // `CommitUnknown` documents.
    Error(submission.SubmissionConflict) -> Error(submission.SubmissionConflict)
    Error(submission.AdmissionContended)
    | Error(submission.NotCommitted(_))
    | Error(submission.EmptyQueueName)
    | Error(submission.CommitUnknown(_))
    | Error(submission.CommitUnknownWithoutId(_)) ->
      Error(submission.CommitUnknown(pending))
  }
}

/// Only `55P03` (the bounded `lock_timeout` elapsing) is `AdmissionContended`.
/// A `job_id` foreign-key violation (`grind_v12`'s `ON DELETE CASCADE`
/// constraints on `grind_job_acknowledgements`/`grind_unique_submissions`/
/// `grind_job_resolutions`) should be unreachable from here — `FOR KEY
/// SHARE` on every non-reschedule candidate (`FOR UPDATE` on a reschedule
/// one) already guarantees a concurrent `prune_finished` can only ever skip
/// a row this transaction still holds, never delete it out from under a
/// receipt insert about to name it — and needs no dedicated branch here
/// either way: it already falls through to the same `NotCommitted(error)`
/// the general case below gives any other constraint violation, which is
/// exactly the "definitely did not commit" shape it should get.
fn classify_query_error(
  error: pog.QueryError,
) -> submission.SubmitError(input, output, error) {
  case error {
    pog.PostgresqlError("55P03", _, _) -> submission.AdmissionContended
    _ -> submission.NotCommitted(error)
  }
}

fn unique_execute(
  query: pog.Query(a),
  connection: pog.Connection,
) -> Result(pog.Returned(a), submission.SubmitError(input, output, error)) {
  store.execute_safely(query, on: connection)
  |> result.map_error(classify_query_error)
}

/// A cardinality guarantee of the calling SQL's shape, not a stored-data
/// trust decision (contrast this module's fail-closed handling of stored
/// `decision`/`observed_state` text, which never uses `assert`).
fn single_row(rows: List(a)) -> a {
  let assert [row] = rows
  row
}

/// The domain-wide advisory lock (`acquire_lock`) and candidate selection
/// only ever run when `request.policy` is `Some`. See
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts", for why the `None`
/// case needs no other lock.
fn admission_transaction(
  connection: pog.Connection,
  lock_wait_ms: Int,
  request: Request(input, output, error),
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  use _ <- result.try(pin_read_committed(connection))
  use _ <- result.try(set_lock_timeout(connection, lock_wait_ms))
  use _ <- result.try(case request.policy {
    Some(policy_part) ->
      acquire_lock(
        connection,
        request.storage_owner,
        request.worker_id,
        request.worker_version,
        policy_part,
      )
    None -> Ok(Nil)
  })
  use existing <- result.try(find_receipt(
    connection,
    request.storage_owner,
    submission.submission_id_value(request.submission_id),
    request.worker,
    request.request_sha256,
  ))
  case existing {
    Some(#(outcome, committed_state)) ->
      Ok(Commit(
        outcome:,
        committed_state:,
        available_at_unix_ms: None,
        via_receipt_match: True,
      ))
    None -> admit_candidate(connection, request)
  }
}

/// Pins this transaction to `READ COMMITTED`, as the **literal first
/// statement** — must run before any other query, or PostgreSQL rejects it.
/// See `docs/UNIQUENESS-CONTRACT.md`, "Admission transaction", step 1.
fn pin_read_committed(
  connection: pog.Connection,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  use _ <- result.try(
    store.call_safely(connection, fn(connection) {
      sql.pin_read_committed(connection)
    })
    |> result.map_error(classify_query_error),
  )
  Ok(Nil)
}

fn set_lock_timeout(
  connection: pog.Connection,
  lock_wait_ms: Int,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  use _ <- result.try(
    store.call_safely(connection, fn(connection) {
      sql.set_lock_timeout(connection, int.to_string(lock_wait_ms))
    })
    |> result.map_error(classify_query_error),
  )
  Ok(Nil)
}

/// The domain-wide advisory lock key; see `docs/UNIQUENESS-CONTRACT.md`,
/// admission transaction step 3. `first_parameter` is the storage owner's
/// position; worker id, version, key contract, and key JSON follow it.
pub fn lock_key_sql(first_parameter: Int) -> String {
  "hashtextextended(jsonb_build_array('grind-unique-v1', current_schema(), "
  <> sql_parameter(first_parameter, "text")
  <> ", "
  <> sql_parameter(first_parameter + 1, "text")
  <> ", "
  <> sql_parameter(first_parameter + 2, "text")
  <> ", "
  <> sql_parameter(first_parameter + 3, "text")
  <> ", encode("
  <> key_digest_sql(first_parameter + 4)
  <> ", 'hex'))::text, 0)"
}

/// The domain-wide advisory lock query itself — SQL text, parameter
/// binding, and the `Bool` decoder together — built once here so
/// `acquire_lock` and any test that needs to hold this exact same lock (the
/// forced-overlap and contention tests in `test/grind_test.gleam`) never
/// re-encode it by hand; `pg_advisory_xact_lock` itself returns `void`,
/// which `pg_types` cannot decode (see `docs/UNIQUENESS-CONTRACT.md`'s
/// PostgreSQL driver note), hence the `SELECT true FROM (...)` wrapping.
pub fn lock_query(
  storage_owner: String,
  worker_id: String,
  worker_version: String,
  key_contract: String,
  encoded_key: String,
) -> pog.Query(Bool) {
  pog.query(
    "SELECT true FROM (SELECT pg_advisory_xact_lock("
    <> lock_key_sql(1)
    <> ")) AS grind_unique_lock",
  )
  |> pog.parameter(pog.text(storage_owner))
  |> pog.parameter(pog.text(worker_id))
  |> pog.parameter(pog.text(worker_version))
  |> pog.parameter(pog.text(key_contract))
  |> pog.parameter(pog.text(encoded_key))
  |> pog.returning({
    use acquired <- decode.field(0, decode.bool)
    decode.success(acquired)
  })
}

fn acquire_lock(
  connection: pog.Connection,
  storage_owner: String,
  worker_id: String,
  worker_version: String,
  policy_part: PolicyPart,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  let query =
    lock_query(
      storage_owner,
      worker_id,
      worker_version,
      policy_part.key_contract,
      policy_part.encoded_key,
    )
  use _ <- result.try(unique_execute(query, connection))
  Ok(Nil)
}

fn sql_parameter(index: Int, cast: String) -> String {
  "$" <> int.to_string(index) <> "::" <> cast
}

/// The uniqueness key digest; see `docs/UNIQUENESS-CONTRACT.md`, Decision 1.
fn key_digest_sql(key_json_parameter: Int) -> String {
  "sha256(convert_to(("
  <> sql_parameter(key_json_parameter, "jsonb")
  <> ")::text, 'UTF8'))"
}

/// Converts a bound millisecond (`divisor` `1000.0`) or microsecond
/// (`1000000.0`) integer into `timestamptz` (also correct for a bound
/// `NULL`). See `docs/UNIQUENESS-CONTRACT.md`'s PostgreSQL driver note for
/// why time round-trips through a bound integer instead of a decoded value.
fn to_timestamptz_sql(param_index: Int, divisor: String) -> String {
  "to_timestamp("
  <> sql_parameter(param_index, "double precision")
  <> " / "
  <> divisor
  <> ")"
}

/// The uniqueness period predicate; see `docs/UNIQUENESS-CONTRACT.md`,
/// admission transaction step 6. `column` and `now_expression` are trusted
/// SQL fragments spliced verbatim, never caller input.
pub fn period_predicate(
  column: String,
  now_expression: String,
  period_ms_expression: String,
) -> String {
  column
  <> " >= "
  <> now_expression
  <> " - ("
  <> period_ms_expression
  <> "::double precision * interval '1 millisecond')"
}

/// `Ok(None)` means no receipt yet; a fingerprint mismatch or an
/// unrecognized `decision`/`observed_state` both fail closed as
/// `SubmissionConflict`.
fn find_receipt(
  connection: pog.Connection,
  storage_owner: String,
  submission_id_value: String,
  worker_def: Worker(input, output, error),
  request_sha256: BitArray,
) -> Result(
  Option(#(submission.Admission(input, output, error), job.State)),
  submission.SubmitError(input, output, error),
) {
  use returned <- result.try(
    store.call_safely(connection, fn(connection) {
      sql.find_receipt(connection, storage_owner, submission_id_value)
    })
    |> result.map_error(classify_query_error),
  )
  case returned.rows {
    [] -> Ok(None)
    [row] ->
      case row.request_sha256 == request_sha256 {
        False -> Error(submission.SubmissionConflict)
        True ->
          case outcome_of_receipt(worker_def, storage_owner, row) {
            Ok(outcome_with_state) -> Ok(Some(outcome_with_state))
            Error(Nil) -> Error(submission.SubmissionConflict)
          }
      }
    _ -> Error(submission.SubmissionConflict)
  }
}

fn outcome_of_receipt(
  worker_def: Worker(input, output, error),
  storage_owner: String,
  row: sql.FindReceiptRow,
) -> Result(#(submission.Admission(input, output, error), job.State), Nil) {
  use state <- result.try(job.state_of_stored(row.observed_state))
  let conflict =
    submission.new_conflict(
      row.job_id,
      storage_owner,
      row.job_queue,
      row.worker_id,
      row.worker_version,
      state,
    )
  case row.decision {
    "inserted" ->
      Ok(#(
        submission.Inserted(job.new_handle(
          row.job_id,
          storage_owner,
          row.job_queue,
          worker_def,
        )),
        state,
      ))
    "existing" -> Ok(#(submission.Existing(conflict), state))
    "rescheduled" -> Ok(#(submission.Rescheduled(conflict), state))
    _ -> Error(Nil)
  }
}

/// Candidate selection only ever runs for a `Some` policy; a `None`
/// ("no policy") request always inserts, since there is no candidate for it
/// to compare against.
fn admit_candidate(
  connection: pog.Connection,
  request: Request(input, output, error),
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  use now_us <- result.try(sample_now(connection))
  case request.policy {
    None -> insert_job(connection, request, None, now_us)
    Some(policy_part) -> {
      use candidate <- result.try(find_candidate(
        connection,
        request,
        policy_part,
        now_us,
      ))
      case candidate {
        None -> insert_job(connection, request, Some(policy_part), now_us)
        Some(row) -> decide_conflict(connection, request, policy_part, row)
      }
    }
  }
}

fn sample_now(
  connection: pog.Connection,
) -> Result(Int, submission.SubmitError(input, output, error)) {
  use returned <- result.try(
    store.call_safely(connection, fn(connection) { sql.sample_now(connection) })
    |> result.map_error(classify_query_error),
  )
  let sql.SampleNowRow(int8: now) = single_row(returned.rows)
  Ok(now)
}

type Candidate {
  Candidate(id: Int, queue: String, state: String, available_at_us: Int)
}

fn is_reschedule(action: unique.ConflictAction) -> Bool {
  case action {
    unique.RescheduleScheduledTo(_) -> True
    unique.KeepExisting -> False
  }
}

/// Every candidate this admission transaction reads is locked, never merely
/// read: a `RescheduleScheduledTo` action needs `FOR UPDATE` (it is about to
/// write `available_at`), and every other action still needs `FOR KEY
/// SHARE` — the weakest lock mode that still conflicts with a `DELETE`
/// (`postgres.prune_finished` locks its own candidates at `FOR UPDATE`
/// strength). `prune_finished` itself never blocks on this: its own scan is
/// `FOR UPDATE SKIP LOCKED`, so a row this transaction already holds is
/// simply skipped, never waited on. The direction that *can* block is this
/// transaction's own read, when `prune_finished` instead reaches and locks
/// this row first — held for as long as that one `DELETE` statement, batch
/// and all, takes to run — this read then waits behind it, and reports
/// `AdmissionContended` if that wait exceeds this transaction's own
/// `lock_timeout`: a correct outcome, bounded to however long that single
/// prune batch holds the row, not a bug. `FOR KEY SHARE` deliberately does
/// *not* conflict with `FOR NO KEY UPDATE`: `attempt.claim_registered_job`,
/// `postgres.cancel_lock`, `lease`'s own quarantine scan, and
/// `postgres.apply_uncertain_resolution`'s own row lock all lock this same
/// table at that weaker strength precisely so an unrelated claim, cancel,
/// quarantine sweep, or resolution racing a `KeepExisting` read of the
/// identical row never spuriously contends (`AdmissionContended`) for a
/// reason that was never actually a write conflict — see
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission transaction" step 6, for the
/// full contention picture.
///
/// The window this lock actually closes: this transaction committing (its
/// own `INSERT` and receipt) *after* `prune_finished`'s `DELETE` statement
/// already took its snapshot but *before* that statement's own scan reaches
/// and locks this exact row — without this lock, `prune_finished`'s `SKIP
/// LOCKED` search would find the row still unlocked at that point and
/// delete it out from under the read this transaction just performed.
/// `grind_v12`'s own `ON DELETE CASCADE` foreign keys are the second,
/// independent backstop for the one narrower window this lock alone cannot
/// close (a prune statement that already locked this row, under its own
/// fixed snapshot, strictly before this admission's own commit becomes
/// visible to it) — see `docs/RECOVERY-EVIDENCE.md`, Increment 24, for why
/// a receipt referencing an already-deleted job can still never become a
/// permanent orphan either way.
fn candidate_sql(
  scope: unique.QueueScope,
  period: unique.PeriodSpec,
  is_reschedule: Bool,
) -> String {
  let base =
    "SELECT id, queue, state, (extract(epoch FROM available_at) * 1000000)::bigint FROM grind_jobs WHERE storage_owner = $1 AND worker_id = $2 AND worker_version = $3 AND unique_key_contract = $4 AND unique_key_sha256 = "
    <> key_digest_sql(5)
    <> " AND state = ANY($6::text[])"
  let #(scoped, next) = case scope {
    unique.WithinQueue -> #(base <> " AND queue = $7", 8)
    unique.AcrossQueues -> #(base, 7)
  }
  let with_period = case period {
    unique.Unbounded -> scoped
    unique.FinitePeriod(_, from) ->
      scoped
      <> " AND "
      <> period_predicate(
        unique.period_column(from),
        to_timestamptz_sql(next, "1000000.0"),
        sql_parameter(next + 1, "bigint"),
      )
  }
  with_period
  <> " ORDER BY id LIMIT 1"
  <> case is_reschedule {
    True -> " FOR UPDATE"
    False -> " FOR KEY SHARE"
  }
}

fn bind_candidate_params(
  query: pog.Query(a),
  storage_owner: String,
  worker_id: String,
  worker_version: String,
  queue: String,
  policy_part: PolicyPart,
  eligible_states: List(String),
  now_us: Int,
  period: unique.PeriodSpec,
) -> pog.Query(a) {
  let base =
    query
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.text(policy_part.key_contract))
    |> pog.parameter(pog.text(policy_part.encoded_key))
    |> pog.parameter(pog.array(pog.text, eligible_states))
  let scoped = case policy_part.scope {
    unique.WithinQueue -> base |> pog.parameter(pog.text(queue))
    unique.AcrossQueues -> base
  }
  case period {
    unique.Unbounded -> scoped
    unique.FinitePeriod(ms, _) ->
      scoped |> pog.parameter(pog.int(now_us)) |> pog.parameter(pog.int(ms))
  }
}

fn find_candidate(
  connection: pog.Connection,
  request: Request(input, output, error),
  policy_part: PolicyPart,
  now_us: Int,
) -> Result(Option(Candidate), submission.SubmitError(input, output, error)) {
  let period_spec = unique.period_spec(policy_part.period)
  let eligible_states = unique.eligible_states(policy_part.states)
  let sql =
    candidate_sql(
      policy_part.scope,
      period_spec,
      is_reschedule(policy_part.on_conflict),
    )
  let query =
    pog.query(sql)
    |> bind_candidate_params(
      request.storage_owner,
      request.worker_id,
      request.worker_version,
      request.queue,
      policy_part,
      eligible_states,
      now_us,
      period_spec,
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      use row_queue <- decode.field(1, decode.string)
      use state <- decode.field(2, decode.string)
      use available_at_us <- decode.field(3, decode.int)
      decode.success(Candidate(id:, queue: row_queue, state:, available_at_us:))
    })
  use returned <- result.try(unique_execute(query, connection))
  case returned.rows {
    [row] -> Ok(Some(row))
    _ -> Ok(None)
  }
}

fn initial_state(
  availability: submission.Availability,
  now_ms: Int,
) -> #(job.State, Int) {
  case availability {
    submission.Immediately -> #(job.Queued, now_ms)
    submission.At(at) -> {
      let target_ms = job.available_at_unix_milliseconds(at)
      case target_ms <= now_ms {
        True -> #(job.Queued, target_ms)
        False -> #(job.Scheduled, target_ms)
      }
    }
  }
}

/// `available_at_unix_ms` is only a meaningful "next eligibility" signal for
/// a committed/observed state where that even applies — `Queued`,
/// `Scheduled`, or `Retryable`. An `Existing` conflict can land on any
/// policy-eligible state (`Incomplete`/`AllRetained` reach as far as
/// `Executing`, `Succeeded`, or beyond); reporting that row's raw
/// `available_at` for those would misrepresent it as a real next-run time.
/// Mirrors `postgres`'s own `available_at_for_observation` for
/// `acknowledged`.
fn admitted_available_at(state: job.State, ms: Int) -> Option(Int) {
  case state {
    job.Queued | job.Scheduled | job.Retryable -> Some(ms)
    _ -> None
  }
}

/// Every value one `grind_unique_submissions` write needs, gathered by
/// `insert_job`/`decide_conflict` rather than passed as a long positional
/// argument list. `submitted_queue` is the request's own submitted queue
/// (column `queue`); `job_queue` is the job's actual queue, which can differ
/// from `submitted_queue` under `AcrossQueues` (column `job_queue`).
type ReceiptWrite {
  ReceiptWrite(
    storage_owner: String,
    submission_id: submission.SubmissionId,
    submitted_queue: String,
    worker_id: String,
    worker_version: String,
    request_sha256: BitArray,
    decision: String,
    job_id: Int,
    job_queue: String,
    observed_state: String,
    rescheduled_from_ms: Option(Int),
    rescheduled_to_ms: Option(Int),
  )
}

/// Records one admission decision in `grind_unique_submissions`, shared by
/// both `submit`'s policy-bearing requests and `submit_plain`'s "no policy"
/// requests — the receipt table itself carries no key material either way,
/// only the fields every request shape already has: the identity needed to
/// look this receipt back up (`storage_owner`/`submission_id`), enough to
/// detect a mismatched retry (`request_sha256`), and the outcome to record.
fn record_receipt(
  connection: pog.Connection,
  write: ReceiptWrite,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  let ReceiptWrite(
    storage_owner:,
    submission_id:,
    submitted_queue:,
    worker_id:,
    worker_version:,
    request_sha256:,
    decision:,
    job_id:,
    job_queue:,
    observed_state:,
    rescheduled_from_ms:,
    rescheduled_to_ms:,
  ) = write
  let query =
    pog.query(
      "INSERT INTO grind_unique_submissions (storage_owner, submission_id, queue, worker_id, worker_version, request_sha256, decision, job_id, job_queue, observed_state, rescheduled_from, rescheduled_to) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "
      <> to_timestamptz_sql(11, "1000.0")
      <> ", "
      <> to_timestamptz_sql(12, "1000.0")
      <> ")",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(submission.submission_id_value(submission_id)))
    |> pog.parameter(pog.text(submitted_queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.bytea(request_sha256))
    |> pog.parameter(pog.text(decision))
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.text(job_queue))
    |> pog.parameter(pog.text(observed_state))
    |> pog.parameter(pog.nullable(pog.int, rescheduled_from_ms))
    |> pog.parameter(pog.nullable(pog.int, rescheduled_to_ms))
  case store.execute_safely(query, on: connection) {
    Ok(_) -> Ok(Nil)
    Error(pog.ConstraintViolated(_, "grind_unique_submissions_pkey", _)) ->
      Error(submission.SubmissionConflict)
    Error(other) -> Error(classify_query_error(other))
  }
}

/// Inserts a fresh job row. `policy_part` here is `None` only for a
/// `submit_plain` request; `admit_candidate` always passes `Some` for a
/// policy-bearing `submit` request, even on a fresh insert (candidate
/// selection found nothing to conflict with), so `submit_unique`'s rows
/// always get real key columns. `None` binds `NULL` for both
/// `unique_key_contract` and `unique_key_sha256`; `Some` binds the real key
/// material.
fn insert_job(
  connection: pog.Connection,
  request: Request(input, output, error),
  policy_part: Option(PolicyPart),
  now_us: Int,
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let now_ms = now_us / 1000
  let #(state, available_at_ms) = initial_state(request.availability, now_ms)
  let error_version_param = case request.error_version {
    Some(version) -> pog.text(version)
    None -> pog.null()
  }
  let base_columns =
    "storage_owner, queue, worker_id, worker_version, input_version, input, output_version, error_version, max_attempts, state, available_at, inserted_at"
  let base_values =
    "$1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, "
    <> to_timestamptz_sql(11, "1000.0")
    <> ", "
    <> to_timestamptz_sql(12, "1000000.0")
  let #(columns, values, key_params) = case policy_part {
    None -> #(base_columns, base_values, [])
    Some(PolicyPart(key_contract:, encoded_key:, ..)) -> #(
      base_columns <> ", unique_key_contract, unique_key_sha256",
      base_values <> ", $13, " <> key_digest_sql(14),
      [pog.text(key_contract), pog.text(encoded_key)],
    )
  }
  let query =
    pog.query(
      "INSERT INTO grind_jobs ("
      <> columns
      <> ") VALUES ("
      <> values
      <> ") RETURNING id",
    )
    |> pog.parameter(pog.text(request.storage_owner))
    |> pog.parameter(pog.text(request.queue))
    |> pog.parameter(pog.text(request.worker_id))
    |> pog.parameter(pog.text(request.worker_version))
    |> pog.parameter(pog.text(request.input_version))
    |> pog.parameter(pog.text(request.encoded_input))
    |> pog.parameter(pog.text(request.output_version))
    |> pog.parameter(error_version_param)
    |> pog.parameter(pog.int(request.max_attempts))
    |> pog.parameter(pog.text(job.state_to_stored(state)))
    |> pog.parameter(pog.int(available_at_ms))
    |> pog.parameter(pog.int(now_us))
  let query = list.fold(key_params, query, pog.parameter)
  let query =
    query
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  use returned <- result.try(unique_execute(query, connection))
  let job_id = single_row(returned.rows)
  use _ <- result.try(record_receipt(
    connection,
    ReceiptWrite(
      storage_owner: request.storage_owner,
      submission_id: request.submission_id,
      submitted_queue: request.queue,
      worker_id: request.worker_id,
      worker_version: request.worker_version,
      request_sha256: request.request_sha256,
      decision: "inserted",
      job_id:,
      job_queue: request.queue,
      observed_state: job.state_to_stored(state),
      rescheduled_from_ms: None,
      rescheduled_to_ms: None,
    ),
  ))
  Ok(Commit(
    outcome: submission.Inserted(job.new_handle(
      job_id,
      request.storage_owner,
      request.queue,
      request.worker,
    )),
    committed_state: state,
    available_at_unix_ms: admitted_available_at(state, available_at_ms),
    via_receipt_match: False,
  ))
}

/// Only ever called for a policy-bearing (`Some`) request — a `None`
/// request never performs candidate selection, so it never has a candidate
/// to decide against.
fn decide_conflict(
  connection: pog.Connection,
  request: Request(input, output, error),
  policy_part: PolicyPart,
  candidate: Candidate,
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  case policy_part.on_conflict, candidate.state {
    unique.RescheduleScheduledTo(at), "scheduled" -> {
      let new_ms = job.available_at_unix_milliseconds(at)
      use _ <- result.try(reschedule_job(
        connection,
        request.storage_owner,
        candidate.id,
        new_ms,
      ))
      use _ <- result.try(record_receipt(
        connection,
        ReceiptWrite(
          storage_owner: request.storage_owner,
          submission_id: request.submission_id,
          submitted_queue: request.queue,
          worker_id: request.worker_id,
          worker_version: request.worker_version,
          request_sha256: request.request_sha256,
          decision: "rescheduled",
          job_id: candidate.id,
          job_queue: candidate.queue,
          observed_state: "scheduled",
          rescheduled_from_ms: Some(candidate.available_at_us / 1000),
          rescheduled_to_ms: Some(new_ms),
        ),
      ))
      Ok(Commit(
        outcome: submission.Rescheduled(submission.new_conflict(
          candidate.id,
          request.storage_owner,
          candidate.queue,
          request.worker_id,
          request.worker_version,
          job.Scheduled,
        )),
        committed_state: job.Scheduled,
        available_at_unix_ms: admitted_available_at(job.Scheduled, new_ms),
        via_receipt_match: False,
      ))
    }
    _, _ -> {
      use state <- result.try(
        job.state_of_stored(candidate.state)
        |> result.replace_error(submission.SubmissionConflict),
      )
      use _ <- result.try(record_receipt(
        connection,
        ReceiptWrite(
          storage_owner: request.storage_owner,
          submission_id: request.submission_id,
          submitted_queue: request.queue,
          worker_id: request.worker_id,
          worker_version: request.worker_version,
          request_sha256: request.request_sha256,
          decision: "existing",
          job_id: candidate.id,
          job_queue: candidate.queue,
          observed_state: candidate.state,
          rescheduled_from_ms: None,
          rescheduled_to_ms: None,
        ),
      ))
      Ok(Commit(
        outcome: submission.Existing(submission.new_conflict(
          candidate.id,
          request.storage_owner,
          candidate.queue,
          request.worker_id,
          request.worker_version,
          state,
        )),
        committed_state: state,
        available_at_unix_ms: admitted_available_at(
          state,
          candidate.available_at_us / 1000,
        ),
        via_receipt_match: False,
      ))
    }
  }
}

fn reschedule_job(
  connection: pog.Connection,
  storage_owner: String,
  job_id: Int,
  new_ms: Int,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  use _ <- result.try(
    store.call_safely(connection, fn(connection) {
      sql.reschedule_job(connection, new_ms, job_id, storage_owner)
    })
    |> result.map_error(classify_query_error),
  )
  Ok(Nil)
}
