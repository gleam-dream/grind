//// The uniqueness admission transaction behind `postgres.submit_unique`/
//// `reconcile_unique`. No dependency on `grind/postgres` (which depends on
//// this module, not the reverse); this module returns `grind/unique`'s
//// public `Admission`/`SubmitError`/`PendingSubmission` types
//// directly, so `postgres.submit_unique`/`reconcile_unique` are thin entry
//// points, not a second translating layer. See `docs/UNIQUENESS-CONTRACT.md`
//// for the full contract.

import gleam/bit_array
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/result
import grind/internal/sql
import grind/job
import grind/unique
import grind/worker.{type Worker}
import pog

@external(erlang, "grind_postgres_ffi", "execute_safely")
fn execute_safely(
  query: pog.Query(a),
  on connection: pog.Connection,
) -> Result(pog.Returned(a), pog.QueryError)

/// Generic form of `execute_safely`, for calling a Squirrel-generated query
/// function (`grind/internal/sql`) that invokes `pog.execute` itself rather
/// than going through `execute_safely`. See `grind/postgres`'s identical
/// binding for the full rationale.
@external(erlang, "grind_postgres_ffi", "call_safely")
fn call_safely(
  run: fn() -> Result(pog.Returned(a), pog.QueryError),
) -> Result(pog.Returned(a), pog.QueryError)

/// Distinguishes a checkout failure (the pool could not hand out a
/// connection at all, before `BEGIN` ever runs — definitely not committed)
/// from pog's own transaction outcome (which may itself be a genuinely
/// uncertain `TransactionQueryError`, checked out fine then lost mid-way).
/// `transaction_safely`'s ordinary catch-all would disguise both as the same
/// `TransactionQueryError` shape; `run` below needs the distinction to avoid
/// reporting `AdmissionFailed` for a fault that might actually have
/// committed, and to avoid reporting `CommitUnknown` (with a
/// `PendingSubmission` a caller might spend a lock-timeout worth of
/// `AdmissionContended` retries chasing) for one that provably never
/// touched the database at all.
@external(erlang, "grind_postgres_ffi", "transaction_or_checkout_failure")
fn transaction_or_checkout_failure(
  connection: pog.Connection,
  callback: fn(pog.Connection) -> Result(a, b),
) -> Result(Result(a, pog.TransactionError(b)), Nil)

/// The request fingerprint's hash; see `docs/UNIQUENESS-CONTRACT.md`, Decision 9.
@external(erlang, "grind_unique_ffi", "sha256")
fn sha256(data: BitArray) -> BitArray

/// Every value the admission transaction needs, gathered once by `submit`.
/// `request_sha256` starts empty and is filled in by `fingerprint`, which
/// takes this whole record as its input — one field list, built once.
type Request(input, output, error) {
  Request(
    storage_owner: String,
    submission_id: unique.SubmissionId,
    queue: String,
    worker: Worker(input, output, error),
    worker_id: String,
    worker_version: String,
    input_version: String,
    encoded_input: String,
    output_version: String,
    error_version: Option(String),
    max_attempts: Int,
    key_contract: String,
    encoded_key: String,
    scope: unique.QueueScope,
    period: unique.Period,
    states: unique.States,
    on_conflict: unique.ConflictAction,
    availability: unique.Availability,
    request_sha256: BitArray,
  )
}

/// The proven-committed outcome of one `submit` call, carried from wherever
/// it was proven (a fresh write, or a durable receipt read back matching
/// this exact submission) up to `grind/postgres`'s `submit_unique`, which is
/// the only place that emits `[grind, job, admitted]` for it — never from
/// inside a transaction callback. Mirrors `grind/postgres`'s own internal
/// `AckCommit`. `via_receipt_match: True` means this call's own transaction
/// (or its post-`CommitUnknown` reconciliation) did not write anything new —
/// the exact same submission was already durably decided, so the
/// observation's `confirmation` is `Reconciled` rather than `Replied`, and
/// `available_at_unix_ms` is `None` (a receipt read cannot re-derive it, the
/// same limitation `AckCommit` documents for a receipt-matched
/// acknowledgement).
pub type Commit(input, output, error) {
  Commit(
    outcome: unique.Admission(input, output, error),
    committed_state: job.State,
    available_at_unix_ms: Option(Int),
    via_receipt_match: Bool,
  )
}

pub fn submit(
  connection: pog.Connection,
  storage_owner: String,
  lock_wait_ms: Int,
  queue: String,
  submission_id: unique.SubmissionId,
  worker_def: Worker(input, output, error),
  input: input,
  availability: unique.Availability,
  policy: unique.Policy(input),
  on_conflict: unique.ConflictAction,
) -> Result(
  Commit(input, output, error),
  unique.SubmitError(input, output, error),
) {
  case queue {
    "" -> Error(unique.EmptyQueueName)
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
          policy,
          on_conflict,
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
  pending: unique.PendingSubmission(input, output, error),
) -> Result(
  unique.Admission(input, output, error),
  unique.SubmitError(input, output, error),
) {
  case reconcile_from_receipt(connection, pending) {
    Ok(#(outcome, _committed_state)) -> Ok(outcome)
    Error(error) -> Error(error)
  }
}

fn build_request(
  storage_owner: String,
  submission_id: unique.SubmissionId,
  queue: String,
  worker_def: Worker(input, output, error),
  input: input,
  availability: unique.Availability,
  policy: unique.Policy(input),
  on_conflict: unique.ConflictAction,
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
  let unique.PolicyFields(key:, scope:, period:, states:) =
    unique.policy_fields(policy)
  let #(key_contract, encoded_key) =
    unique.key_material(key, input, input_version, encoded_input)
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
      key_contract:,
      encoded_key:,
      scope:,
      period:,
      states:,
      on_conflict:,
      availability:,
      request_sha256: <<>>,
    )
  Request(..request, request_sha256: fingerprint(request))
}

/// The request fingerprint envelope; see `docs/UNIQUENESS-CONTRACT.md`, Decision 9.
fn fingerprint(request: Request(input, output, error)) -> BitArray {
  let #(period_ms, period_origin) = case unique.period_spec(request.period) {
    unique.Unbounded -> #(None, None)
    unique.FinitePeriod(ms, from) -> #(
      Some(ms),
      Some(unique.period_origin_label(from)),
    )
  }
  let reschedule_ms = unique.reschedule_target_ms(request.on_conflict)
  let availability_ms = unique.availability_ms(request.availability)
  json.preprocessed_array([
    json.string("grind-unique-request-v1"),
    json.string(request.queue),
    json.string(request.worker_id),
    json.string(request.worker_version),
    json.string(request.input_version),
    json.string(request.encoded_input),
    json.string(request.key_contract),
    json.string(request.encoded_key),
    json.string(unique.scope_label(request.scope)),
    json.bool(option.is_some(period_ms)),
    json.nullable(period_ms, json.int),
    json.bool(option.is_some(period_origin)),
    json.nullable(period_origin, json.string),
    json.string(unique.states_label(request.states)),
    json.string(unique.action_label(request.on_conflict)),
    json.bool(option.is_some(reschedule_ms)),
    json.nullable(reschedule_ms, json.int),
    json.bool(option.is_some(availability_ms)),
    json.nullable(availability_ms, json.int),
    json.string(request.output_version),
    json.bool(option.is_some(request.error_version)),
    json.nullable(request.error_version, json.string),
    json.int(request.max_attempts),
  ])
  |> json.to_string
  |> bit_array.from_string
  |> sha256
}

fn run(
  connection: pog.Connection,
  request: Request(input, output, error),
  lock_wait_ms: Int,
) -> Result(
  Commit(input, output, error),
  unique.SubmitError(input, output, error),
) {
  case
    transaction_or_checkout_failure(connection, fn(transaction) {
      admission_transaction(transaction, lock_wait_ms, request)
    })
  {
    // Checkout itself failed: no connection was ever handed out, so `BEGIN`
    // never ran. This is knowably not committed — no `PendingSubmission` is
    // constructed, and no receipt lookup is attempted (there is nothing a
    // lookup on this same unreachable store could tell us that we don't
    // already know).
    Error(Nil) -> Error(unique.AdmissionFailed(pog.ConnectionUnavailable))
    Ok(Ok(commit)) -> Ok(commit)
    Ok(Error(pog.TransactionRolledBack(error))) -> Error(error)
    Ok(Error(pog.TransactionQueryError(_))) ->
      case
        reconcile_from_receipt(
          connection,
          unique.new_pending_submission(
            request.storage_owner,
            request.submission_id,
            request.worker,
            request.request_sha256,
          ),
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
  }
}

fn reconcile_from_receipt(
  connection: pog.Connection,
  pending: unique.PendingSubmission(input, output, error),
) -> Result(
  #(unique.Admission(input, output, error), job.State),
  unique.SubmitError(input, output, error),
) {
  let storage_owner = unique.pending_submission_storage_owner(pending)
  let worker_def = unique.pending_submission_worker(pending)
  let request_sha256 = unique.pending_submission_request_sha256(pending)
  case
    find_receipt(
      connection,
      storage_owner,
      unique.submission_id_value(unique.pending_submission_id(pending)),
      worker_def,
      request_sha256,
    )
  {
    Ok(Some(outcome_with_state)) -> Ok(outcome_with_state)
    Ok(None) -> Error(unique.CommitUnknown(pending))
    // A fingerprint mismatch (or an unrecognized stored `decision`/
    // `observed_state`) is knowable, not uncertain: this exact
    // `SubmissionId` was already durably decided for a *different* request,
    // so a caller retrying `CommitUnknown` forever would never converge.
    // Pass it through unchanged, mirroring `reconcile_unknown_ack`'s
    // `QueueAckCommandConflict` passthrough for the acknowledgement path.
    // Every other error here (a lock-timeout-shaped or otherwise failed
    // lookup) means the check itself could not run, which is exactly what
    // `CommitUnknown` documents.
    Error(unique.SubmissionConflict) -> Error(unique.SubmissionConflict)
    Error(unique.AdmissionContended)
    | Error(unique.AdmissionFailed(_))
    | Error(unique.EmptyQueueName)
    | Error(unique.CommitUnknown(_)) -> Error(unique.CommitUnknown(pending))
  }
}

/// Only `55P03` (the bounded `lock_timeout` elapsing) is `AdmissionContended`.
fn classify_query_error(
  error: pog.QueryError,
) -> unique.SubmitError(input, output, error) {
  case error {
    pog.PostgresqlError("55P03", _, _) -> unique.AdmissionContended
    _ -> unique.AdmissionFailed(error)
  }
}

fn unique_execute(
  query: pog.Query(a),
  connection: pog.Connection,
) -> Result(pog.Returned(a), unique.SubmitError(input, output, error)) {
  execute_safely(query, on: connection)
  |> result.map_error(classify_query_error)
}

/// A cardinality guarantee of the calling SQL's shape, not a stored-data
/// trust decision (contrast this module's fail-closed handling of stored
/// `decision`/`observed_state` text, which never uses `assert`).
fn single_row(rows: List(a)) -> a {
  let assert [row] = rows
  row
}

fn admission_transaction(
  connection: pog.Connection,
  lock_wait_ms: Int,
  request: Request(input, output, error),
) -> Result(
  Commit(input, output, error),
  unique.SubmitError(input, output, error),
) {
  use _ <- result.try(pin_read_committed(connection))
  use _ <- result.try(set_lock_timeout(connection, lock_wait_ms))
  use _ <- result.try(acquire_lock(connection, request))
  use existing <- result.try(find_receipt(
    connection,
    request.storage_owner,
    unique.submission_id_value(request.submission_id),
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
/// statement** — `SET TRANSACTION ISOLATION LEVEL` must run before any other
/// query in the transaction or PostgreSQL rejects it. This transaction's
/// correctness depends on it: every plain read after the domain lock
/// (`find_receipt`, `find_candidate`) must see whatever another submitter
/// committed while this one was waiting for the lock, and only `READ
/// COMMITTED` takes a fresh snapshot per statement — `REPEATABLE READ`/
/// `SERIALIZABLE` freeze the snapshot at the transaction's first statement,
/// which here would be *before* the lock wait even starts. Relying on the
/// connecting role or database's own `default_transaction_isolation`
/// happening to already be `READ COMMITTED` would make this transaction
/// silently produce a duplicate row under a differently configured role or
/// database, with no error and no other code-visible signal — proven by
/// `postgres_submit_unique_admission_safe_under_repeatable_read_test`
/// against a real database configured with
/// `default_transaction_isolation = 'repeatable read'`. See
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission transaction", step 1.
fn pin_read_committed(
  connection: pog.Connection,
) -> Result(Nil, unique.SubmitError(input, output, error)) {
  use _ <- result.try(
    call_safely(fn() { sql.pin_read_committed(connection) })
    |> result.map_error(classify_query_error),
  )
  Ok(Nil)
}

fn set_lock_timeout(
  connection: pog.Connection,
  lock_wait_ms: Int,
) -> Result(Nil, unique.SubmitError(input, output, error)) {
  use _ <- result.try(
    call_safely(fn() {
      sql.set_lock_timeout(connection, int.to_string(lock_wait_ms))
    })
    |> result.map_error(classify_query_error),
  )
  Ok(Nil)
}

/// The domain-wide advisory lock key; see `docs/UNIQUENESS-CONTRACT.md`,
/// admission transaction step 3. `first_parameter` is the storage owner's
/// position; worker id, version, key contract, and key JSON follow it.
@internal
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
@internal
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
  request: Request(input, output, error),
) -> Result(Nil, unique.SubmitError(input, output, error)) {
  let query =
    lock_query(
      request.storage_owner,
      request.worker_id,
      request.worker_version,
      request.key_contract,
      request.encoded_key,
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
@internal
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
  Option(#(unique.Admission(input, output, error), job.State)),
  unique.SubmitError(input, output, error),
) {
  use returned <- result.try(
    call_safely(fn() {
      sql.find_receipt(connection, storage_owner, submission_id_value)
    })
    |> result.map_error(classify_query_error),
  )
  case returned.rows {
    [] -> Ok(None)
    [row] ->
      case row.request_sha256 == request_sha256 {
        False -> Error(unique.SubmissionConflict)
        True ->
          case outcome_of_receipt(worker_def, storage_owner, row) {
            Ok(outcome_with_state) -> Ok(Some(outcome_with_state))
            Error(Nil) -> Error(unique.SubmissionConflict)
          }
      }
    _ -> Error(unique.SubmissionConflict)
  }
}

fn outcome_of_receipt(
  worker_def: Worker(input, output, error),
  storage_owner: String,
  row: sql.FindReceiptRow,
) -> Result(#(unique.Admission(input, output, error), job.State), Nil) {
  use state <- result.try(job.state_of_stored(row.observed_state))
  let conflict =
    unique.new_conflict(
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
        unique.Inserted(job.new_handle(
          row.job_id,
          storage_owner,
          row.job_queue,
          worker_def,
        )),
        state,
      ))
    "existing" -> Ok(#(unique.Existing(conflict), state))
    "rescheduled" -> Ok(#(unique.Rescheduled(conflict), state))
    _ -> Error(Nil)
  }
}

fn admit_candidate(
  connection: pog.Connection,
  request: Request(input, output, error),
) -> Result(
  Commit(input, output, error),
  unique.SubmitError(input, output, error),
) {
  use now_us <- result.try(sample_now(connection))
  use candidate <- result.try(find_candidate(connection, request, now_us))
  case candidate {
    None -> insert_job(connection, request, now_us)
    Some(row) -> decide_conflict(connection, request, row)
  }
}

fn sample_now(
  connection: pog.Connection,
) -> Result(Int, unique.SubmitError(input, output, error)) {
  use returned <- result.try(
    call_safely(fn() { sql.sample_now(connection) })
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

fn candidate_sql(
  scope: unique.QueueScope,
  period: unique.PeriodSpec,
  lock_for_reschedule: Bool,
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
  <> case lock_for_reschedule {
    True -> " FOR UPDATE"
    False -> ""
  }
}

fn bind_candidate_params(
  query: pog.Query(a),
  request: Request(input, output, error),
  eligible_states: List(String),
  now_us: Int,
  period: unique.PeriodSpec,
) -> pog.Query(a) {
  let base =
    query
    |> pog.parameter(pog.text(request.storage_owner))
    |> pog.parameter(pog.text(request.worker_id))
    |> pog.parameter(pog.text(request.worker_version))
    |> pog.parameter(pog.text(request.key_contract))
    |> pog.parameter(pog.text(request.encoded_key))
    |> pog.parameter(pog.array(pog.text, eligible_states))
  let scoped = case request.scope {
    unique.WithinQueue -> base |> pog.parameter(pog.text(request.queue))
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
  now_us: Int,
) -> Result(Option(Candidate), unique.SubmitError(input, output, error)) {
  let period_spec = unique.period_spec(request.period)
  let eligible_states = unique.eligible_states(request.states)
  let sql =
    candidate_sql(
      request.scope,
      period_spec,
      is_reschedule(request.on_conflict),
    )
  let query =
    pog.query(sql)
    |> bind_candidate_params(request, eligible_states, now_us, period_spec)
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
  availability: unique.Availability,
  now_ms: Int,
) -> #(job.State, Int) {
  case availability {
    unique.Immediately -> #(job.Queued, now_ms)
    unique.At(at) -> {
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

fn insert_job(
  connection: pog.Connection,
  request: Request(input, output, error),
  now_us: Int,
) -> Result(
  Commit(input, output, error),
  unique.SubmitError(input, output, error),
) {
  let now_ms = now_us / 1000
  let #(state, available_at_ms) = initial_state(request.availability, now_ms)
  let error_version_param = case request.error_version {
    Some(version) -> pog.text(version)
    None -> pog.null()
  }
  let query =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, error_version, max_attempts, state, available_at, inserted_at, unique_key_contract, unique_key_sha256) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, "
      <> to_timestamptz_sql(11, "1000.0")
      <> ", "
      <> to_timestamptz_sql(12, "1000000.0")
      <> ", $13, "
      <> key_digest_sql(14)
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
    |> pog.parameter(pog.text(request.key_contract))
    |> pog.parameter(pog.text(request.encoded_key))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  use returned <- result.try(unique_execute(query, connection))
  let job_id = single_row(returned.rows)
  use _ <- result.try(record_receipt(
    connection,
    request,
    "inserted",
    job_id,
    request.queue,
    job.state_to_stored(state),
    None,
    None,
  ))
  Ok(Commit(
    outcome: unique.Inserted(job.new_handle(
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

fn decide_conflict(
  connection: pog.Connection,
  request: Request(input, output, error),
  candidate: Candidate,
) -> Result(
  Commit(input, output, error),
  unique.SubmitError(input, output, error),
) {
  case request.on_conflict, candidate.state {
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
        request,
        "rescheduled",
        candidate.id,
        candidate.queue,
        "scheduled",
        Some(candidate.available_at_us / 1000),
        Some(new_ms),
      ))
      Ok(Commit(
        outcome: unique.Rescheduled(unique.new_conflict(
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
        |> result.replace_error(unique.SubmissionConflict),
      )
      use _ <- result.try(record_receipt(
        connection,
        request,
        "existing",
        candidate.id,
        candidate.queue,
        candidate.state,
        None,
        None,
      ))
      Ok(Commit(
        outcome: unique.Existing(unique.new_conflict(
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
) -> Result(Nil, unique.SubmitError(input, output, error)) {
  use _ <- result.try(
    call_safely(fn() {
      sql.reschedule_job(connection, new_ms, job_id, storage_owner)
    })
    |> result.map_error(classify_query_error),
  )
  Ok(Nil)
}

fn record_receipt(
  connection: pog.Connection,
  request: Request(input, output, error),
  decision: String,
  job_id: Int,
  job_queue: String,
  observed_state: String,
  rescheduled_from_ms: Option(Int),
  rescheduled_to_ms: Option(Int),
) -> Result(Nil, unique.SubmitError(input, output, error)) {
  let query =
    pog.query(
      "INSERT INTO grind_unique_submissions (storage_owner, submission_id, queue, worker_id, worker_version, request_sha256, decision, job_id, job_queue, observed_state, rescheduled_from, rescheduled_to) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "
      <> to_timestamptz_sql(11, "1000.0")
      <> ", "
      <> to_timestamptz_sql(12, "1000.0")
      <> ")",
    )
    |> pog.parameter(pog.text(request.storage_owner))
    |> pog.parameter(
      pog.text(unique.submission_id_value(request.submission_id)),
    )
    |> pog.parameter(pog.text(request.queue))
    |> pog.parameter(pog.text(request.worker_id))
    |> pog.parameter(pog.text(request.worker_version))
    |> pog.parameter(pog.bytea(request.request_sha256))
    |> pog.parameter(pog.text(decision))
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.text(job_queue))
    |> pog.parameter(pog.text(observed_state))
    |> pog.parameter(pog.nullable(pog.int, rescheduled_from_ms))
    |> pog.parameter(pog.nullable(pog.int, rescheduled_to_ms))
  case execute_safely(query, on: connection) {
    Ok(_) -> Ok(Nil)
    Error(pog.ConstraintViolated(_, "grind_unique_submissions_pkey", _)) ->
      Error(unique.SubmissionConflict)
    Error(other) -> Error(classify_query_error(other))
  }
}
