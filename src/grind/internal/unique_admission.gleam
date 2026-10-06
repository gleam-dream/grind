//// The admission transaction behind `postgres.submit_unique`/
//// `reconcile_unique`/`submit_with_id` — one `Request` and transaction
//// serve both, keyed on whether `Request.policy` is `Some` or `None`.
//// Request fingerprints and dynamic query construction live in the
//// `grind/internal/unique_admission` submodules. See
//// `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md` for the full contract.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import grind/internal/job
import grind/internal/sql
import grind/internal/store
import grind/internal/submission
import grind/internal/unique
import grind/internal/unique_admission/query as unique_admission_query
import grind/internal/unique_admission/request.{
  type PolicyPart, type Request, PolicyPart,
} as unique_admission_request
import grind/internal/worker.{type Worker}
import pog

/// The proven-committed outcome of one `submit` call. Mirrors
/// `grind/internal/attempt/acknowledgement`'s `AckCommit`, including
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

/// One admission command: everything `admit` needs besides the connection.
/// `policy: None` admits with a receipt and no uniqueness key.
pub type Spec(input, output, error) {
  Spec(
    queue: String,
    submission_id: submission.SubmissionId,
    worker: Worker(input, output, error),
    input: input,
    availability: submission.Availability,
    policy: Option(#(unique.Policy(input), unique.ConflictAction)),
    correlation: Option(String),
  )
}

/// Builds the request for `spec`, rejecting an empty queue, a rejected or
/// over-large input and a rejected key before any storage call.
fn request_for(
  installation: job.Installation,
  max_payload_bytes: Int,
  spec: Spec(input, output, error),
) -> Result(
  Request(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let Spec(
    queue:,
    submission_id:,
    worker: worker_def,
    input:,
    availability:,
    policy:,
    correlation:,
  ) = spec
  case queue {
    "" -> Error(submission.EmptyQueueName)
    _ ->
      unique_admission_request.build_request(
        installation,
        submission_id,
        queue,
        worker_def,
        input,
        availability,
        correlation,
        max_payload_bytes,
        fn(input_version, encoded_input) {
          case policy {
            None -> Ok(None)
            Some(#(policy, on_conflict)) -> {
              let unique.PolicyFields(key:, scope:, period:, states:) =
                unique.policy_fields(policy)
              use #(key_contract, encoded_key) <- result.map(
                unique.key_material(key, input, input_version, encoded_input),
              )
              Some(PolicyPart(
                key_contract:,
                encoded_key:,
                scope:,
                period:,
                states:,
                on_conflict:,
              ))
            }
          }
        },
      )
  }
}

/// Admits one job in its own transaction on `connection`'s pool. Every
/// admission records a receipt under its `SubmissionId`, so an outcome lost
/// with its reply is reconcilable.
pub fn admit(
  connection: pog.Connection,
  installation: job.Installation,
  lock_wait_ms: Int,
  max_payload_bytes: Int,
  spec: Spec(input, output, error),
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  use request <- result.try(request_for(installation, max_payload_bytes, spec))
  run(connection, request, lock_wait_ms)
}

/// Admits one job inside the caller's open transaction `tx`, without
/// `BEGIN` or `COMMIT`: the job, its receipt and any uniqueness decision
/// commit or roll back with the caller's own writes.
///
/// The caller's transaction must be `READ COMMITTED` on this installation's
/// database. For the duration of the admission, `search_path` and
/// `lock_timeout` are set transaction-locally to Grind's schema and lock
/// wait, then restored to the caller's values. A statement that fails aborts
/// the caller's transaction, as any failed statement does.
pub fn admit_in_transaction(
  tx: pog.Connection,
  installation: job.Installation,
  lock_wait_ms: Int,
  max_payload_bytes: Int,
  quoted_schema: String,
  spec: Spec(input, output, error),
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  use request <- result.try(request_for(installation, max_payload_bytes, spec))
  case is_single_connection(tx) {
    False -> Error(submission.NotInTransaction)
    True -> {
      use #(isolation, database_oid, search_path, lock_timeout) <- result.try(
        transaction_settings(tx),
      )
      case
        isolation,
        database_oid == job.installation_database_oid(installation)
      {
        "read committed", True -> {
          use _ <- result.try(set_local(
            tx,
            quoted_schema,
            int.to_string(lock_wait_ms) <> "ms",
          ))
          let outcome = transaction_body(tx, request)
          case set_local(tx, search_path, lock_timeout), outcome {
            _, Error(error) -> Error(error)
            Error(error), Ok(_) -> Error(error)
            Ok(Nil), Ok(commit) -> Ok(commit)
          }
        }
        "read committed", False ->
          Error(submission.HandleFromAnotherInstallation)
        other, _ -> Error(submission.TransactionIsolationUnsupported(other))
      }
    }
  }
}

@external(erlang, "grind_postgres_ffi", "is_single_connection")
fn is_single_connection(connection: pog.Connection) -> Bool

fn transaction_settings(
  tx: pog.Connection,
) -> Result(
  #(String, Int, String, String),
  submission.SubmitError(input, output, error),
) {
  let query =
    pog.query(
      "SELECT current_setting('transaction_isolation'), (SELECT oid::int4 FROM pg_database WHERE datname = current_database()), current_setting('search_path'), current_setting('lock_timeout')",
    )
    |> pog.returning({
      use isolation <- decode.field(0, decode.string)
      use oid <- decode.field(1, decode.int)
      use search_path <- decode.field(2, decode.string)
      use lock_timeout <- decode.field(3, decode.string)
      decode.success(#(isolation, oid, search_path, lock_timeout))
    })
  use returned <- result.try(unique_execute(query, tx))
  Ok(single_row(returned.rows))
}

fn set_local(
  tx: pog.Connection,
  search_path: String,
  lock_timeout: String,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  let query =
    pog.query(
      "SELECT set_config('search_path', $1, true), set_config('lock_timeout', $2, true)",
    )
    |> pog.parameter(pog.text(search_path))
    |> pog.parameter(pog.text(lock_timeout))
  use _ <- result.try(unique_execute(query, tx))
  Ok(Nil)
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

/// Runs the admission transaction and classifies its result. Shared by
/// `submit` (`policy: Some`) and `submit_plain` (`policy: None`); see
/// "Out of scope" in `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md` for why the
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
    // `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md`.
    Ok(Error(pog.TransactionRolledBack(submission.SubmissionConflict)))
    | Ok(Error(pog.TransactionQueryError(_))) ->
      case
        reconcile_from_receipt(
          connection,
          unique_admission_request.pending_submission(request),
        )
      {
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
  let worker_def = submission.pending_submission_worker(pending)
  let request_sha256 = submission.pending_submission_request_sha256(pending)
  let installation = submission.pending_submission_installation(pending)
  case
    find_receipt(
      connection,
      installation,
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
    // Never actually produced by `find_receipt`/`classify_query_error` —
    // `postgres.reconcile_unique` already gates on this before ever calling
    // into this module (see that function's own doc comment) — but
    // `submission.SubmitError` is a shared type, so this match must still be
    // exhaustive. Passed through unchanged rather than folded into
    // `CommitUnknown`: were this ever reached some other way, misreporting a
    // wrong-installation handle as merely uncertain would be actively
    // misleading.
    Error(submission.HandleFromAnotherInstallation) ->
      Error(submission.HandleFromAnotherInstallation)
    Error(submission.AdmissionContended)
    | Error(submission.NotCommitted(_))
    | Error(submission.EmptyQueueName)
    | Error(submission.InvalidInput(_))
    | Error(submission.CommitUnknown(_))
    | Error(submission.PayloadTooLarge(_, _))
    | Error(submission.NotInTransaction)
    | Error(submission.TransactionIsolationUnsupported(_)) ->
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
/// `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md`, "Admission receipts", for why the `None`
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
  transaction_body(connection, request)
}

/// The admission after its isolation and lock wait are set: the uniqueness
/// lock (with a policy), the receipt lookup, then the insert or conflict
/// decision.
fn transaction_body(
  connection: pog.Connection,
  request: Request(input, output, error),
) -> Result(
  Commit(input, output, error),
  submission.SubmitError(input, output, error),
) {
  use _ <- result.try(case request.policy {
    Some(policy_part) ->
      acquire_lock(
        connection,
        job.installation_schema(request.installation),
        request.worker_id,
        request.worker_version,
        policy_part,
      )
    None -> Ok(Nil)
  })
  use existing <- result.try(find_receipt(
    connection,
    request.installation,
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
/// See `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md`, "Admission transaction", step 1.
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

/// The domain-wide advisory lock key; see `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md`,
/// admission transaction step 3. The configured schema (`postgres.Settings.schema`,
/// see `postgres.with_schema`) is bound as an ordinary parameter — never
/// `current_schema()` spliced into the SQL text — so two schemas, now the
/// whole unit of isolation (see "Isolation" in `README.md`), never share a
/// lock even though nothing else in this key is schema-specific. Binding the
/// configured schema deliberately, rather than asking PostgreSQL to resolve
/// `current_schema()` itself, is what makes this key immune to the `$user`
/// `search_path`-fallback hazard: `current_schema()` reports the *first*
/// schema in `search_path` that exists, which need not be the schema
/// `grind_jobs` actually lives in whenever `search_path` names more than
/// one schema and the one this pool is meant to use is not first (or was
/// created after that first entry). `postgres.validate` pins `search_path`
/// to exactly this one configured schema for every pooled connection, so in
/// ordinary operation the two already agree by construction — this bound
/// parameter is the belt to that suspenders, correct even if a future change
/// ever widened `search_path` again. `first_parameter` is the worker id's
/// own position, with worker version, key contract, and key JSON following
/// it, and the schema bound one position before it.
fn acquire_lock(
  connection: pog.Connection,
  schema: String,
  worker_id: String,
  worker_version: String,
  policy_part: PolicyPart,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  let query =
    unique_admission_query.lock_query(
      schema,
      worker_id,
      worker_version,
      policy_part.key_contract,
      policy_part.encoded_key,
    )
  use _ <- result.try(unique_execute(query, connection))
  Ok(Nil)
}

/// `Ok(None)` means no receipt yet; a fingerprint mismatch or an
/// unrecognized `decision`/`observed_state` both fail closed as
/// `SubmissionConflict`.
fn find_receipt(
  connection: pog.Connection,
  installation: job.Installation,
  submission_id_value: String,
  worker_def: Worker(input, output, error),
  request_sha256: BitArray,
) -> Result(
  Option(#(submission.Admission(input, output, error), job.State)),
  submission.SubmitError(input, output, error),
) {
  use returned <- result.try(
    store.call_safely(connection, fn(connection) {
      sql.find_receipt(connection, submission_id_value)
    })
    |> result.map_error(classify_query_error),
  )
  case returned.rows {
    [] -> Ok(None)
    [row] ->
      case row.request_sha256 == request_sha256 {
        False -> Error(submission.SubmissionConflict)
        True ->
          case outcome_of_receipt(installation, worker_def, row) {
            Ok(outcome_with_state) -> Ok(Some(outcome_with_state))
            Error(Nil) -> Error(submission.SubmissionConflict)
          }
      }
    _ -> Error(submission.SubmissionConflict)
  }
}

fn outcome_of_receipt(
  installation: job.Installation,
  worker_def: Worker(input, output, error),
  row: sql.FindReceiptRow,
) -> Result(#(submission.Admission(input, output, error), job.State), Nil) {
  use state <- result.try(job.state_of_stored(row.observed_state))
  let conflict =
    submission.new_conflict(
      row.job_id,
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
          installation,
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

fn find_candidate(
  connection: pog.Connection,
  request: Request(input, output, error),
  policy_part: PolicyPart,
  now_us: Int,
) -> Result(Option(Candidate), submission.SubmitError(input, output, error)) {
  let period_spec = unique.period_spec(policy_part.period)
  let eligible_states = unique.eligible_states(policy_part.states)
  let sql =
    unique_admission_query.candidate_sql(
      policy_part.scope,
      period_spec,
      unique_admission_query.is_reschedule(policy_part.on_conflict),
    )
  let query =
    pog.query(sql)
    |> unique_admission_query.bind_candidate_params(
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
    submission.Delayed(milliseconds) ->
      case milliseconds <= 0 {
        True -> #(job.Queued, now_ms)
        False -> #(job.Scheduled, now_ms + milliseconds)
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
/// look this receipt back up (`submission_id`, unique on its own now that
/// one schema is one installation — see "Isolation" in `README.md`), enough
/// to detect a mismatched retry (`request_sha256`), and the outcome to
/// record.
fn record_receipt(
  connection: pog.Connection,
  write: ReceiptWrite,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  let ReceiptWrite(
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
      "INSERT INTO grind_unique_submissions (submission_id, queue, worker_id, worker_version, request_sha256, decision, job_id, job_queue, observed_state, rescheduled_from, rescheduled_to) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, "
      <> unique_admission_query.to_timestamptz_sql(10, "1000.0")
      <> ", "
      <> unique_admission_query.to_timestamptz_sql(11, "1000.0")
      <> ")",
    )
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
    "queue, worker_id, worker_version, input_version, input, output_version, error_version, max_attempts, state, available_at, inserted_at"
  let base_values =
    "$1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, "
    <> unique_admission_query.to_timestamptz_sql(10, "1000.0")
    <> ", "
    <> unique_admission_query.to_timestamptz_sql(11, "1000000.0")
  // The v13 columns are written only when set, so the statement text of a
  // request without them is unchanged.
  let optional =
    [
      #("correlation", option.map(request.correlation, pog.text)),
      #("max_replays", option.map(request.max_replays, pog.int)),
    ]
    |> list.filter_map(fn(column) {
      case column.1 {
        Some(value) -> Ok(#(column.0, value))
        None -> Error(Nil)
      }
    })
  let #(base_columns, base_values, _) =
    list.fold(optional, #(base_columns, base_values, 12), fn(acc, column) {
      let #(columns, values, next) = acc
      #(
        columns <> ", " <> column.0,
        values <> ", $" <> int.to_string(next),
        next + 1,
      )
    })
  let first_key_parameter = 12 + list.length(optional)
  let #(columns, values, key_params) = case policy_part {
    None -> #(base_columns, base_values, [])
    Some(PolicyPart(key_contract:, encoded_key:, ..)) -> #(
      base_columns <> ", unique_key_contract, unique_key_sha256",
      base_values
        <> ", $"
        <> int.to_string(first_key_parameter)
        <> ", "
        <> unique_admission_query.key_digest_sql(first_key_parameter + 1),
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
  let query =
    list.fold(optional, query, fn(query, column) {
      pog.parameter(query, column.1)
    })
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
      request.installation,
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
      use _ <- result.try(reschedule_job(connection, candidate.id, new_ms))
      use _ <- result.try(record_receipt(
        connection,
        ReceiptWrite(
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
  job_id: Int,
  new_ms: Int,
) -> Result(Nil, submission.SubmitError(input, output, error)) {
  use _ <- result.try(
    store.call_safely(connection, fn(connection) {
      sql.reschedule_job(connection, new_ms, job_id)
    })
    |> result.map_error(classify_query_error),
  )
  Ok(Nil)
}

/// The exact lock key used by a unique admission transaction.
pub fn lock_key_sql(first_parameter: Int) -> String {
  unique_admission_query.lock_key_sql(first_parameter)
}

/// The domain-wide advisory lock query used by admission and overlap tests.
pub fn lock_query(
  schema: String,
  worker_id: String,
  worker_version: String,
  key_contract: String,
  encoded_key: String,
) -> pog.Query(Bool) {
  unique_admission_query.lock_query(
    schema,
    worker_id,
    worker_version,
    key_contract,
    encoded_key,
  )
}

/// The predicate shared by period-bounded candidate selection.
pub fn period_predicate(
  column: String,
  now_expression: String,
  period_ms_expression: String,
) -> String {
  unique_admission_query.period_predicate(
    column,
    now_expression,
    period_ms_expression,
  )
}
