//// This module contains the code to run the sql queries defined in
//// `./src/grind/internal/sql`.
//// > 🐿️ This module was generated automatically using v4.7.0 of
//// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
////
//// This file is regenerated wholesale by `scripts/generate-sql.sh` (which
//// wraps `gleam run -m squirrel` against a disposable database) from the
//// static `.sql` files under `./src/grind/internal/sql/`; do not hand-edit
//// it, and re-add this note if it is ever lost to a regeneration. This is
//// one half of a permanent split, not a migration in progress: a query
//// belongs here only when its SQL text is fixed at compile time. Dynamic
//// SQL — shared lease/period/lock predicate fragments spliced into more
//// than one query, per-disposition acknowledgement SQL (the proposed state
//// selects which columns/branches apply), nullable-parameter queries whose
//// bound value shape varies by call, and candidate selection (its `WHERE`/
//// `ORDER BY`/locking clause depends on scope, period, and conflict
//// action) — stays hand-written inline in `grind/postgres` and
//// `grind/internal/unique_admission`, where squirrel cannot generate it
//// from a single static string. See `AGENTS.md` for the same rule.

import gleam/dynamic/decode
import gleam/option.{type Option}
import pog

/// A row you get from running the `arguments` query
/// defined in `./src/grind/internal/sql/arguments.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ArgumentsRow {
  ArgumentsRow(
    input: String,
    input_version: String,
    queue: String,
    worker_id: String,
    worker_version: String,
  )
}

/// Runs the `arguments` query
/// defined in `./src/grind/internal/sql/arguments.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn arguments(
  db: pog.Connection,
  id: Int,
) -> Result(pog.Returned(ArgumentsRow), pog.QueryError) {
  let decoder = {
    use input <- decode.field(0, decode.string)
    use input_version <- decode.field(1, decode.string)
    use queue <- decode.field(2, decode.string)
    use worker_id <- decode.field(3, decode.string)
    use worker_version <- decode.field(4, decode.string)
    decode.success(ArgumentsRow(
      input:,
      input_version:,
      queue:,
      worker_id:,
      worker_version:,
    ))
  }

  "SELECT input::text, input_version, queue, worker_id, worker_version FROM grind_jobs WHERE id = $1
"
  |> pog.query
  |> pog.parameter(pog.int(id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `bind_handle` query
/// defined in `./src/grind/internal/sql/bind_handle.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type BindHandleRow {
  BindHandleRow(
    queue: String,
    worker_id: String,
    worker_version: String,
    input_version: String,
    output_version: String,
    error_version: Option(String),
  )
}

/// Runs the `bind_handle` query
/// defined in `./src/grind/internal/sql/bind_handle.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn bind_handle(
  db: pog.Connection,
  id: Int,
) -> Result(pog.Returned(BindHandleRow), pog.QueryError) {
  let decoder = {
    use queue <- decode.field(0, decode.string)
    use worker_id <- decode.field(1, decode.string)
    use worker_version <- decode.field(2, decode.string)
    use input_version <- decode.field(3, decode.string)
    use output_version <- decode.field(4, decode.string)
    use error_version <- decode.field(5, decode.optional(decode.string))
    decode.success(BindHandleRow(
      queue:,
      worker_id:,
      worker_version:,
      input_version:,
      output_version:,
      error_version:,
    ))
  }

  "SELECT queue, worker_id, worker_version, input_version, output_version, error_version FROM grind_jobs WHERE id = $1
"
  |> pog.query
  |> pog.parameter(pog.int(id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `cancel_before_run` query
/// defined in `./src/grind/internal/sql/cancel_before_run.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type CancelBeforeRunRow {
  CancelBeforeRunRow(id: Int)
}

/// Runs the `cancel_before_run` query
/// defined in `./src/grind/internal/sql/cancel_before_run.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn cancel_before_run(
  db: pog.Connection,
  id: Int,
) -> Result(pog.Returned(CancelBeforeRunRow), pog.QueryError) {
  let decoder = {
    use id <- decode.field(0, decode.int)
    decode.success(CancelBeforeRunRow(id:))
  }

  "UPDATE grind_jobs SET state = 'cancelled', output = NULL, error = NULL, failure_description = 'cancelled by caller', failure_cause = NULL, uncertain_at = NULL, cancel_requested_at = NULL, finished_at = clock_timestamp() WHERE id = $1 AND state IN ('queued', 'scheduled', 'retryable') RETURNING id
"
  |> pog.query
  |> pog.parameter(pog.int(id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `cancel_executing` query
/// defined in `./src/grind/internal/sql/cancel_executing.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type CancelExecutingRow {
  CancelExecutingRow(id: Int)
}

/// Runs the `cancel_executing` query
/// defined in `./src/grind/internal/sql/cancel_executing.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn cancel_executing(
  db: pog.Connection,
  id: Int,
) -> Result(pog.Returned(CancelExecutingRow), pog.QueryError) {
  let decoder = {
    use id <- decode.field(0, decode.int)
    decode.success(CancelExecutingRow(id:))
  }

  "UPDATE grind_jobs SET cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp()) WHERE id = $1 AND state IN ('executing', 'uncertain') RETURNING id
"
  |> pog.query
  |> pog.parameter(pog.int(id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `cancel_lock` query
/// defined in `./src/grind/internal/sql/cancel_lock.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type CancelLockRow {
  CancelLockRow(
    queue: String,
    worker_id: String,
    worker_version: String,
    state: String,
  )
}

/// Runs the `cancel_lock` query
/// defined in `./src/grind/internal/sql/cancel_lock.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn cancel_lock(
  db: pog.Connection,
  id: Int,
) -> Result(pog.Returned(CancelLockRow), pog.QueryError) {
  let decoder = {
    use queue <- decode.field(0, decode.string)
    use worker_id <- decode.field(1, decode.string)
    use worker_version <- decode.field(2, decode.string)
    use state <- decode.field(3, decode.string)
    decode.success(CancelLockRow(queue:, worker_id:, worker_version:, state:))
  }

  "SELECT queue, worker_id, worker_version, state FROM grind_jobs WHERE id = $1 FOR NO KEY UPDATE
"
  |> pog.query
  |> pog.parameter(pog.int(id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `find_receipt` query
/// defined in `./src/grind/internal/sql/find_receipt.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type FindReceiptRow {
  FindReceiptRow(
    decision: String,
    job_id: Int,
    job_queue: String,
    observed_state: String,
    worker_id: String,
    worker_version: String,
    request_sha256: BitArray,
  )
}

/// Runs the `find_receipt` query
/// defined in `./src/grind/internal/sql/find_receipt.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn find_receipt(
  db: pog.Connection,
  submission_id: String,
) -> Result(pog.Returned(FindReceiptRow), pog.QueryError) {
  let decoder = {
    use decision <- decode.field(0, decode.string)
    use job_id <- decode.field(1, decode.int)
    use job_queue <- decode.field(2, decode.string)
    use observed_state <- decode.field(3, decode.string)
    use worker_id <- decode.field(4, decode.string)
    use worker_version <- decode.field(5, decode.string)
    use request_sha256 <- decode.field(6, decode.bit_array)
    decode.success(FindReceiptRow(
      decision:,
      job_id:,
      job_queue:,
      observed_state:,
      worker_id:,
      worker_version:,
      request_sha256:,
    ))
  }

  "SELECT decision, job_id, job_queue, observed_state, worker_id, worker_version, request_sha256 FROM grind_unique_submissions WHERE submission_id = $1
"
  |> pog.query
  |> pog.parameter(pog.text(submission_id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `outcome` query
/// defined in `./src/grind/internal/sql/outcome.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type OutcomeRow {
  OutcomeRow(
    queue: String,
    worker_id: String,
    worker_version: String,
    state: String,
    output: Option(String),
    output_version: String,
    error: Option(String),
    error_version: Option(String),
    failure_description: Option(String),
    failure_cause: Option(String),
  )
}

/// Runs the `outcome` query
/// defined in `./src/grind/internal/sql/outcome.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn outcome(
  db: pog.Connection,
  id: Int,
) -> Result(pog.Returned(OutcomeRow), pog.QueryError) {
  let decoder = {
    use queue <- decode.field(0, decode.string)
    use worker_id <- decode.field(1, decode.string)
    use worker_version <- decode.field(2, decode.string)
    use state <- decode.field(3, decode.string)
    use output <- decode.field(4, decode.optional(decode.string))
    use output_version <- decode.field(5, decode.string)
    use error <- decode.field(6, decode.optional(decode.string))
    use error_version <- decode.field(7, decode.optional(decode.string))
    use failure_description <- decode.field(8, decode.optional(decode.string))
    use failure_cause <- decode.field(9, decode.optional(decode.string))
    decode.success(OutcomeRow(
      queue:,
      worker_id:,
      worker_version:,
      state:,
      output:,
      output_version:,
      error:,
      error_version:,
      failure_description:,
      failure_cause:,
    ))
  }

  "SELECT queue, worker_id, worker_version, state, output, output_version, error, error_version, failure_description, failure_cause FROM grind_jobs WHERE id = $1
"
  |> pog.query
  |> pog.parameter(pog.int(id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `pin_read_committed` query
/// defined in `./src/grind/internal/sql/pin_read_committed.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn pin_read_committed(
  db: pog.Connection,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "SET TRANSACTION ISOLATION LEVEL READ COMMITTED
"
  |> pog.query
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `prune_finished` query
/// defined in `./src/grind/internal/sql/prune_finished.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type PruneFinishedRow {
  PruneFinishedRow(count: Int)
}

/// Runs the `prune_finished` query
/// defined in `./src/grind/internal/sql/prune_finished.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn prune_finished(
  db: pog.Connection,
  arg_1: Int,
  arg_2: Int,
) -> Result(pog.Returned(PruneFinishedRow), pog.QueryError) {
  let decoder = {
    use count <- decode.field(0, decode.int)
    decode.success(PruneFinishedRow(count:))
  }

  "WITH doomed AS (SELECT id FROM grind_jobs WHERE finished_at IS NOT NULL AND state IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled') AND finished_at < statement_timestamp() - ($1::bigint::double precision * interval '1 millisecond') ORDER BY finished_at, id LIMIT $2 FOR UPDATE SKIP LOCKED), deleted AS (DELETE FROM grind_jobs x USING doomed d WHERE x.id = d.id RETURNING 1) SELECT count(*) FROM deleted
"
  |> pog.query
  |> pog.parameter(pog.int(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `queue_statistics` query
/// defined in `./src/grind/internal/sql/queue_statistics.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type QueueStatisticsRow {
  QueueStatisticsRow(
    sampled_at_ms: Int,
    state: String,
    count: Int,
    oldest_job_age_ms: Int,
    due_count: Int,
    oldest_due_age_ms: Int,
  )
}

/// One queue, one MVCC snapshot and one database clock sample.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn queue_statistics(
  db: pog.Connection,
  queue: String,
) -> Result(pog.Returned(QueueStatisticsRow), pog.QueryError) {
  let decoder = {
    use sampled_at_ms <- decode.field(0, decode.int)
    use state <- decode.field(1, decode.string)
    use count <- decode.field(2, decode.int)
    use oldest_job_age_ms <- decode.field(3, decode.int)
    use due_count <- decode.field(4, decode.int)
    use oldest_due_age_ms <- decode.field(5, decode.int)
    decode.success(QueueStatisticsRow(
      sampled_at_ms:,
      state:,
      count:,
      oldest_job_age_ms:,
      due_count:,
      oldest_due_age_ms:,
    ))
  }

  "-- One queue, one MVCC snapshot and one database clock sample.
WITH sample AS MATERIALIZED (
  SELECT clock_timestamp() AS at
), counts AS MATERIALIZED (
  SELECT
    state,
    count(*)::bigint AS count,
    max(greatest(0, floor(extract(epoch FROM (at - inserted_at)) * 1000)))::bigint AS oldest_job_age_ms,
    count(*) FILTER (
      WHERE state IN ('queued', 'scheduled', 'retryable') AND available_at <= at
    )::bigint AS due_count,
    max(greatest(0, floor(extract(epoch FROM (at - available_at)) * 1000))) FILTER (
      WHERE state IN ('queued', 'scheduled', 'retryable') AND available_at <= at
    )::bigint AS oldest_due_age_ms
  FROM grind_jobs CROSS JOIN sample
  WHERE queue = $1
  GROUP BY state
), state_names AS (
  SELECT state FROM counts
  UNION
  SELECT unnest(ARRAY[
    'queued', 'scheduled', 'retryable', 'executing', 'succeeded',
    'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain',
    'discarded', 'cancelled'
  ]::text[])
)
SELECT
  (SELECT floor(extract(epoch FROM at) * 1000)::bigint FROM sample) AS sampled_at_ms,
  state_names.state,
  coalesce(counts.count, 0)::bigint AS count,
  coalesce(counts.oldest_job_age_ms, 0)::bigint AS oldest_job_age_ms,
  coalesce(counts.due_count, 0)::bigint AS due_count,
  coalesce(counts.oldest_due_age_ms, 0)::bigint AS oldest_due_age_ms
FROM state_names LEFT JOIN counts USING (state)
ORDER BY state_names.state;
"
  |> pog.query
  |> pog.parameter(pog.text(queue))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `reconcile_acknowledgement` query
/// defined in `./src/grind/internal/sql/reconcile_acknowledgement.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ReconcileAcknowledgementRow {
  ReconcileAcknowledgementRow(
    queue: String,
    job_id: Int,
    worker_id: String,
    worker_version: String,
    attempt_id: Int,
    attempt_epoch: Int,
    committed_state: String,
    failure_cause: Option(String),
    committed_at_unix_ms: Int,
  )
}

/// Runs the `reconcile_acknowledgement` query
/// defined in `./src/grind/internal/sql/reconcile_acknowledgement.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn reconcile_acknowledgement(
  db: pog.Connection,
  command_id: String,
) -> Result(pog.Returned(ReconcileAcknowledgementRow), pog.QueryError) {
  let decoder = {
    use queue <- decode.field(0, decode.string)
    use job_id <- decode.field(1, decode.int)
    use worker_id <- decode.field(2, decode.string)
    use worker_version <- decode.field(3, decode.string)
    use attempt_id <- decode.field(4, decode.int)
    use attempt_epoch <- decode.field(5, decode.int)
    use committed_state <- decode.field(6, decode.string)
    use failure_cause <- decode.field(7, decode.optional(decode.string))
    use committed_at_unix_ms <- decode.field(8, decode.int)
    decode.success(ReconcileAcknowledgementRow(
      queue:,
      job_id:,
      worker_id:,
      worker_version:,
      attempt_id:,
      attempt_epoch:,
      committed_state:,
      failure_cause:,
      committed_at_unix_ms:,
    ))
  }

  "SELECT queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, committed_state, failure_cause, (extract(epoch FROM committed_at) * 1000)::bigint AS committed_at_unix_ms FROM grind_job_acknowledgements WHERE command_id = $1
"
  |> pog.query
  |> pog.parameter(pog.text(command_id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `release_unstarted_claim` query
/// defined in `./src/grind/internal/sql/release_unstarted_claim.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ReleaseUnstartedClaimRow {
  ReleaseUnstartedClaimRow(id: Int)
}

/// Runs the `release_unstarted_claim` query
/// defined in `./src/grind/internal/sql/release_unstarted_claim.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn release_unstarted_claim(
  db: pog.Connection,
  id: Int,
  queue: String,
  attempt_id: Int,
  attempt_epoch: Int,
  attempt_owner: String,
  arg_6: String,
) -> Result(pog.Returned(ReleaseUnstartedClaimRow), pog.QueryError) {
  let decoder = {
    use id <- decode.field(0, decode.int)
    decode.success(ReleaseUnstartedClaimRow(id:))
  }

  "UPDATE grind_jobs SET state = $6, attempt_epoch = attempt_epoch + 1, attempt_owner = NULL, lease_expires_at = NULL, attempt_count = GREATEST(attempt_count - 1, 0) WHERE id = $1 AND queue = $2 AND state = 'executing' AND attempt_id = $3 AND attempt_epoch = $4 AND attempt_owner = $5 RETURNING id
"
  |> pog.query
  |> pog.parameter(pog.int(id))
  |> pog.parameter(pog.text(queue))
  |> pog.parameter(pog.int(attempt_id))
  |> pog.parameter(pog.int(attempt_epoch))
  |> pog.parameter(pog.text(attempt_owner))
  |> pog.parameter(pog.text(arg_6))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `reschedule_job` query
/// defined in `./src/grind/internal/sql/reschedule_job.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn reschedule_job(
  db: pog.Connection,
  arg_1: Int,
  id: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "UPDATE grind_jobs SET available_at = to_timestamp($1::bigint::double precision / 1000.0) WHERE id = $2 AND state = 'scheduled'
"
  |> pog.query
  |> pog.parameter(pog.int(arg_1))
  |> pog.parameter(pog.int(id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `resolution_transaction_settings` query
/// defined in `./src/grind/internal/sql/resolution_transaction_settings.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ResolutionTransactionSettingsRow {
  ResolutionTransactionSettingsRow(
    isolation: String,
    database_oid: Int,
    search_path: String,
    lock_timeout: String,
    statement_timeout: String,
  )
}

/// Runs the `resolution_transaction_settings` query
/// defined in `./src/grind/internal/sql/resolution_transaction_settings.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn resolution_transaction_settings(
  db: pog.Connection,
) -> Result(pog.Returned(ResolutionTransactionSettingsRow), pog.QueryError) {
  let decoder = {
    use isolation <- decode.field(0, decode.string)
    use database_oid <- decode.field(1, decode.int)
    use search_path <- decode.field(2, decode.string)
    use lock_timeout <- decode.field(3, decode.string)
    use statement_timeout <- decode.field(4, decode.string)
    decode.success(ResolutionTransactionSettingsRow(
      isolation:,
      database_oid:,
      search_path:,
      lock_timeout:,
      statement_timeout:,
    ))
  }

  "SELECT
  current_setting('transaction_isolation') AS isolation,
  (SELECT oid::int4 FROM pg_database WHERE datname = current_database()) AS database_oid,
  current_setting('search_path') AS search_path,
  current_setting('lock_timeout') AS lock_timeout,
  current_setting('statement_timeout') AS statement_timeout;
"
  |> pog.query
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `restore_resolution_transaction` query
/// defined in `./src/grind/internal/sql/restore_resolution_transaction.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type RestoreResolutionTransactionRow {
  RestoreResolutionTransactionRow(
    search_path: String,
    lock_timeout: String,
    statement_timeout: String,
  )
}

/// Runs the `restore_resolution_transaction` query
/// defined in `./src/grind/internal/sql/restore_resolution_transaction.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn restore_resolution_transaction(
  db: pog.Connection,
  arg_1: String,
  arg_2: String,
  arg_3: String,
) -> Result(pog.Returned(RestoreResolutionTransactionRow), pog.QueryError) {
  let decoder = {
    use search_path <- decode.field(0, decode.string)
    use lock_timeout <- decode.field(1, decode.string)
    use statement_timeout <- decode.field(2, decode.string)
    decode.success(RestoreResolutionTransactionRow(
      search_path:,
      lock_timeout:,
      statement_timeout:,
    ))
  }

  "SELECT
  set_config('search_path', $1, true) AS search_path,
  set_config('lock_timeout', $2, true) AS lock_timeout,
  set_config('statement_timeout', $3, true) AS statement_timeout;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.text(arg_2))
  |> pog.parameter(pog.text(arg_3))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `sample_now` query
/// defined in `./src/grind/internal/sql/sample_now.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type SampleNowRow {
  SampleNowRow(int8: Int)
}

/// Runs the `sample_now` query
/// defined in `./src/grind/internal/sql/sample_now.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn sample_now(
  db: pog.Connection,
) -> Result(pog.Returned(SampleNowRow), pog.QueryError) {
  let decoder = {
    use int8 <- decode.field(0, decode.int)
    decode.success(SampleNowRow(int8:))
  }

  "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint
"
  |> pog.query
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `scope_resolution_transaction` query
/// defined in `./src/grind/internal/sql/scope_resolution_transaction.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ScopeResolutionTransactionRow {
  ScopeResolutionTransactionRow(
    search_path: String,
    lock_timeout: String,
    statement_timeout: String,
  )
}

/// Runs the `scope_resolution_transaction` query
/// defined in `./src/grind/internal/sql/scope_resolution_transaction.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn scope_resolution_transaction(
  db: pog.Connection,
  arg_1: String,
  arg_2: Int,
) -> Result(pog.Returned(ScopeResolutionTransactionRow), pog.QueryError) {
  let decoder = {
    use search_path <- decode.field(0, decode.string)
    use lock_timeout <- decode.field(1, decode.string)
    use statement_timeout <- decode.field(2, decode.string)
    decode.success(ScopeResolutionTransactionRow(
      search_path:,
      lock_timeout:,
      statement_timeout:,
    ))
  }

  "SELECT
  set_config('search_path', $1, true) AS search_path,
  set_config('lock_timeout', LEAST(
    COALESCE(NULLIF(extract(epoch FROM current_setting('lock_timeout')::interval) * 1000, 0), $2::int),
    $2::int)::int::text, true) AS lock_timeout,
  set_config('statement_timeout', LEAST(
    COALESCE(NULLIF(extract(epoch FROM current_setting('statement_timeout')::interval) * 1000, 0), $2::int),
    $2::int)::int::text, true) AS statement_timeout;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `set_lock_timeout` query
/// defined in `./src/grind/internal/sql/set_lock_timeout.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type SetLockTimeoutRow {
  SetLockTimeoutRow(set_config: String)
}

/// Runs the `set_lock_timeout` query
/// defined in `./src/grind/internal/sql/set_lock_timeout.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn set_lock_timeout(
  db: pog.Connection,
  arg_1: String,
) -> Result(pog.Returned(SetLockTimeoutRow), pog.QueryError) {
  let decoder = {
    use set_config <- decode.field(0, decode.string)
    decode.success(SetLockTimeoutRow(set_config:))
  }

  "SELECT set_config('lock_timeout', $1, true)
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `state` query
/// defined in `./src/grind/internal/sql/state.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.7.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type StateRow {
  StateRow(
    queue: String,
    worker_id: String,
    worker_version: String,
    state: String,
  )
}

/// Runs the `state` query
/// defined in `./src/grind/internal/sql/state.sql`.
///
/// > 🐿️ This function was generated automatically using v4.7.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn state(
  db: pog.Connection,
  id: Int,
) -> Result(pog.Returned(StateRow), pog.QueryError) {
  let decoder = {
    use queue <- decode.field(0, decode.string)
    use worker_id <- decode.field(1, decode.string)
    use worker_version <- decode.field(2, decode.string)
    use state <- decode.field(3, decode.string)
    decode.success(StateRow(queue:, worker_id:, worker_version:, state:))
  }

  "SELECT queue, worker_id, worker_version, state FROM grind_jobs WHERE id = $1
"
  |> pog.query
  |> pog.parameter(pog.int(id))
  |> pog.returning(decoder)
  |> pog.execute(db)
}
