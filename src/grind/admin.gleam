//// Operator work: listing jobs, resolving uncertain ones, sweeping expired
//// attempts, pruning, and reading acknowledgement receipts.
////
//// ```gleam
//// import grind/admin
//// import grind/job
////
//// // Find the jobs that wait for an operator.
//// let assert Ok(jobs) =
////   admin.list(grind, admin.query(limit: 100) |> admin.in_state(job.Uncertain))
//// ```
////
//// An uncertain job's attempt may or may not have had its effect: its node
//// died, its handler exceeded its timeout, or it reported `Uncertain`. Grind
//// never runs it again on its own unless its worker opted into
//// `worker.ReplayAfterLeaseExpiry`. An operator resolves it with
//// `resolve_uncertain`: confirm the success or failure that happened, or
//// authorize a replay. Every resolution is recorded with its author and is
//// idempotent under its id.

import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import grind.{type Grind}
import grind/internal/convert
import grind/internal/postgres
import grind/internal/runtime
import grind/job.{type JobHandle, type State}
import pog
import sinal/correlation.{type Correlation}

/// A filter for `list`. Build it with `query` and the `in_*` and `after`
/// setters.
pub opaque type Query {
  Query(limit: Int, queue: Option(String), state: Option(State), after_id: Int)
}

/// Up to `limit` jobs (at most 10,000), oldest id first.
pub fn query(limit limit: Int) -> Query {
  Query(limit:, queue: None, state: None, after_id: 0)
}

/// Only jobs in `queue`.
pub fn in_queue(query: Query, queue: String) -> Query {
  Query(..query, queue: Some(queue))
}

/// Only jobs in `state`.
pub fn in_state(query: Query, state: State) -> Query {
  Query(..query, state: Some(state))
}

/// Only jobs with an id above `id`: pass the last id of one page to read
/// the next.
pub fn after(query: Query, id: Int) -> Query {
  Query(..query, after_id: id)
}

/// One stored job, without its typed payloads.
pub type JobSummary {
  JobSummary(
    id: Int,
    queue: String,
    worker_id: String,
    worker_version: String,
    state: State,
    attempt: Int,
    max_attempts: Int,
    snooze_count: Int,
    replay_count: Int,
    inserted_at: Timestamp,
    available_at: Timestamp,
    finished_at: Option(Timestamp),
    description: Option(String),
    correlation: Option(Correlation),
  )
}

pub type Error {
  /// A limit is below 1 or above `maximum`.
  InvalidLimit(limit: Int, maximum: Int)
  /// A retention age is not positive.
  InvalidRetention(milliseconds: Int)
  /// A resolution's `field` is empty.
  EmptyResolutionField(field: String)
  /// The job is not uncertain, so there is nothing to resolve.
  NotUncertain
  /// The resolution id was already used for another job or a different command
  /// in this installation (the same PostgreSQL database and Grind schema).
  ResolutionConflict
  /// The stored job does not match the handle's queue, worker or codecs.
  RecordMismatch
  /// A `ConfirmFailure` needs a worker with an error codec.
  ResolutionNeedsErrorCodec
  /// The confirmed value was rejected by its codec.
  ResolutionValueRejected(reason: String)
  /// Cancellation intent forbids replay. Investigate the effect and use an
  /// attributed terminal confirmation to settle the job.
  CancellationPending
  /// The resolution may or may not have committed; repeat it with the same
  /// id.
  ResolutionCommitUnknown(resolution_id: String)
  /// Borrowed resolution requires the connection passed to a pog.transaction callback.
  NotInTransaction
  /// Borrowed resolution requires READ COMMITTED; isolation is never changed.
  TransactionIsolationUnsupported(String)
  JobNotFound
  ReceiptNotFound
  /// The handle belongs to another installation, or the transaction uses another database.
  WrongDatabase
  Unavailable(pog.QueryError)
  /// No runtime is running under this name on this node.
  NotRunning
}

pub fn describe_error(error: Error) -> String {
  case error {
    InvalidLimit(limit:, maximum:) ->
      "grind/admin: the limit "
      <> int.to_string(limit)
      <> " is not between 1 and "
      <> int.to_string(maximum)
    InvalidRetention(milliseconds:) ->
      "grind/admin: the retention of "
      <> int.to_string(milliseconds)
      <> " ms is not positive"
    EmptyResolutionField(field:) ->
      "grind/admin: the resolution's " <> field <> " is empty"
    NotUncertain -> "grind/admin: the job is not uncertain"
    ResolutionConflict ->
      "grind/admin: the resolution id was used for a different decision"
    RecordMismatch -> "grind/admin: the stored job does not match the handle"
    ResolutionNeedsErrorCodec ->
      "grind/admin: confirming a failure needs a worker with an error codec"
    ResolutionValueRejected(reason:) ->
      "grind/admin: the confirmed value was rejected: " <> reason
    CancellationPending -> "grind/admin: a cancellation of the job is pending"
    ResolutionCommitUnknown(resolution_id:) ->
      "grind/admin: resolution "
      <> resolution_id
      <> " may have committed; repeat it with the same id"
    NotInTransaction ->
      "grind/admin: resolution requires a caller-owned transaction"
    TransactionIsolationUnsupported(level) ->
      "grind/admin: unsupported transaction isolation: " <> level
    JobNotFound -> "grind/admin: no such job"
    ReceiptNotFound -> "grind/admin: no such acknowledgement receipt"
    WrongDatabase ->
      "grind/admin: the handle or transaction belongs to another database or schema"
    Unavailable(reason) ->
      "grind/admin: the database is unavailable: " <> string.inspect(reason)
    NotRunning -> "grind/admin: no runtime is running under this name"
  }
}

pub fn error_kind(error: Error) -> grind.ErrorKind {
  case error {
    InvalidLimit(..)
    | InvalidRetention(_)
    | EmptyResolutionField(_)
    | ResolutionNeedsErrorCodec
    | ResolutionValueRejected(_)
    | NotInTransaction
    | TransactionIsolationUnsupported(_) -> grind.Invalid
    JobNotFound | ReceiptNotFound | NotUncertain -> grind.NotFound
    ResolutionConflict | RecordMismatch | WrongDatabase | CancellationPending ->
      grind.Mismatch
    ResolutionCommitUnknown(_) -> grind.Unknown
    Unavailable(_) | NotRunning -> grind.Unreachable
  }
}

fn database(grind: Grind) -> Result(postgres.Database, Error) {
  runtime.database(grind) |> result.replace_error(NotRunning)
}

/// Lists stored jobs that match `query`. Uncertain jobs are served by an
/// index; other states are read in id order.
pub fn list(grind: Grind, query: Query) -> Result(List(JobSummary), Error) {
  use database <- result.try(database(grind))
  let Query(limit:, queue:, state:, after_id:) = query
  postgres.list_jobs(
    database,
    queue,
    option.map(state, convert.internal_state),
    after_id,
    limit,
  )
  |> result.map(fn(rows) {
    rows
    |> list.map(fn(row) {
      let postgres.JobRow(
        id:,
        queue:,
        worker_id:,
        worker_version:,
        state:,
        attempt:,
        max_attempts:,
        snooze_count:,
        replay_count:,
        inserted_at_us:,
        available_at_us:,
        finished_at_us:,
        failure_description:,
        correlation:,
      ) = row
      JobSummary(
        id:,
        queue:,
        worker_id:,
        worker_version:,
        state: convert.state(state),
        attempt:,
        max_attempts:,
        snooze_count:,
        replay_count:,
        inserted_at: from_unix_us(inserted_at_us),
        available_at: from_unix_us(available_at_us),
        finished_at: option.map(finished_at_us, from_unix_us),
        description: failure_description,
        correlation: option.map(correlation, correlation.from_key),
      )
    })
  })
  |> result.map_error(fn(error) {
    case error {
      postgres.NonPositiveListLimit | postgres.ListLimitTooLarge ->
        InvalidLimit(limit:, maximum: postgres.list_limit_maximum)
      postgres.ListQueryFailed(reason) -> Unavailable(reason)
      postgres.ListInvalidStoredState(_) -> RecordMismatch
    }
  })
}

fn from_unix_us(microseconds: Int) -> Timestamp {
  timestamp.from_unix_seconds_and_nanoseconds(
    microseconds / 1_000_000,
    { microseconds % 1_000_000 } * 1000,
  )
}

/// Moves up to `limit` (at most 10,000) expired `executing` attempts, in every queue, to
/// their abandonment outcome: `uncertain`, or queued again for a worker
/// that replays. Consumers already do this for the queues they poll; run it
/// for queues no node polls. Returns how many rows moved.
pub fn quarantine_expired(
  grind: Grind,
  limit limit: Int,
) -> Result(Int, Error) {
  use database <- result.try(database(grind))
  case limit > postgres.list_limit_maximum {
    True -> Error(InvalidLimit(limit:, maximum: postgres.list_limit_maximum))
    False ->
      postgres.quarantine_expired(database, limit:)
      |> result.map_error(fn(error) {
        case error {
          postgres.NonPositiveLimit ->
            InvalidLimit(limit:, maximum: postgres.list_limit_maximum)
          postgres.QuarantineQueryFailed(reason) -> Unavailable(reason)
        }
      })
  }
}

/// Deletes up to `limit` (at most 10,000) jobs that finished more than
/// `older_than` ago, with their receipts, and returns how many. The
/// configured pruner does this on a timer.
pub fn prune_finished(
  grind: Grind,
  older_than older_than: Duration,
  limit limit: Int,
) -> Result(Int, Error) {
  use database <- result.try(database(grind))
  let older_than_ms = duration.to_milliseconds(older_than)
  postgres.prune_finished(database, older_than_ms:, limit:)
  |> result.map(fn(report) { report.jobs })
  |> result.map_error(fn(error) {
    case error {
      postgres.NonPositiveRetention | postgres.RetentionAbovePrecisionBound ->
        InvalidRetention(older_than_ms)
      postgres.NonPositivePruneLimit | postgres.PruneLimitTooLarge ->
        InvalidLimit(limit:, maximum: postgres.prune_limit_maximum())
      postgres.PruneQueryFailed(reason) -> Unavailable(reason)
    }
  })
}

/// An operator's decision about an uncertain job.
pub type Decision(output, error) {
  /// The attempt succeeded with this output.
  ConfirmSuccess(output)
  /// The attempt failed with this error. Needs an error codec.
  ConfirmFailure(error)
  /// Run the job again.
  AuthorizeReplay
}

/// An attributed decision. Build it with `resolution`.
pub opaque type Resolution(output, error) {
  Resolution(
    decision: Decision(output, error),
    id: String,
    by: String,
    details: String,
  )
}

/// A decision, identified by `id` so a repeated call after a lost reply
/// applies it once, and attributed to `by` with `details`.
///
/// IDs are unique across one installation (the same PostgreSQL database and
/// Grind schema), not just within a job, queue or worker. Retain one ID for
/// one exact command. Retry it with the same job, decision, value, author and
/// details; another job or changed command returns `ResolutionConflict`.
/// A later decision needs a new ID. A durable application decision ID, qualified
/// by the job ID when necessary, can supply this identity. Do not generate a
/// new ID merely because the original reply was lost.
///
/// The receipt is removed when its job is pruned. These IDs and attribution
/// fields neither authenticate the operator nor replace business idempotency.
pub fn resolution(
  decision: Decision(output, error),
  id id: String,
  by by: String,
  details details: String,
) -> Resolution(output, error) {
  Resolution(decision:, id:, by:, details:)
}

/// What a resolution did.
pub type Resolved {
  /// The decision was applied; the job is now in this state.
  Applied(State)
  /// The same decision was already applied under this id.
  AlreadyApplied(State)
}

/// Applies an audited decision to an uncertain job.
pub fn resolve_uncertain(
  grind: Grind,
  handle: JobHandle(input, output, error),
  resolution: Resolution(output, error),
) -> Result(Resolved, Error) {
  use database <- result.try(database(grind))
  postgres.resolve_uncertain(database, handle, resolution_request(resolution))
  |> resolution_result
}

/// The result of resolution statements inside a caller-owned transaction.
/// Even AlreadyApplied does not prove that the surrounding application writes committed.
pub type StagedResolution {
  Staged(Resolved)
}

/// Resolves an uncertain job together with application writes in the caller's
/// open READ COMMITTED transaction on the same PostgreSQL database.
/// Pass the connection received by a pog.transaction callback, not a pool.
/// The caller owns commit, rollback, checkout and the outer transaction lifetime.
///
/// Grind scopes search_path to its schema and bounds lock_timeout and
/// statement_timeout by with_statement_deadline, preserving stricter caller
/// limits. Successful statements restore these settings before returning.
/// A database statement error may abort the transaction; propagate every error
/// to its owner so all staged writes roll back. There is no nested transaction.
///
/// Keep investigation and external calls outside the transaction: row locks
/// remain held until the caller finishes it. No committed resolved event is
/// emitted. If the outer commit reply is lost, read the application's durable
/// acknowledgment; a staged result is not commit evidence. Exact-command retry
/// remains valid while the job and its receipt are retained.
pub fn resolve_uncertain_in(
  grind: Grind,
  transaction: pog.Connection,
  handle: JobHandle(input, output, error),
  resolution: Resolution(output, error),
) -> Result(StagedResolution, Error) {
  use database <- result.try(database(grind))
  postgres.resolve_uncertain_in(
    database,
    transaction,
    handle,
    resolution_request(resolution),
  )
  |> resolution_result
  |> result.map(Staged)
}

fn resolution_request(
  resolution: Resolution(output, error),
) -> postgres.ResolutionRequest(output, error) {
  let Resolution(decision:, id:, by:, details:) = resolution
  let decision = case decision {
    ConfirmSuccess(output) -> postgres.ConfirmSuccess(output)
    ConfirmFailure(error) -> postgres.ConfirmBusinessFailure(error)
    AuthorizeReplay -> postgres.AuthorizeReplay
  }
  postgres.ResolutionRequest(
    resolution_id: id,
    resolved_by: by,
    details:,
    decision:,
  )
}

fn resolution_result(
  outcome: Result(postgres.ResolutionResult, postgres.ResolutionError),
) -> Result(Resolved, Error) {
  outcome
  |> result.map(fn(result) {
    case result {
      postgres.ResolutionApplied(state) -> Applied(convert.state(state))
      postgres.ResolutionAlreadyApplied(state) ->
        AlreadyApplied(convert.state(state))
    }
  })
  |> result.map_error(fn(error) {
    case error {
      postgres.ResolutionNotInTransaction -> NotInTransaction
      postgres.ResolutionIsolationUnsupported(level) ->
        TransactionIsolationUnsupported(level)
      postgres.EmptyResolutionId -> EmptyResolutionField("id")
      postgres.EmptyResolver -> EmptyResolutionField("by")
      postgres.EmptyResolutionDetails -> EmptyResolutionField("details")
      postgres.ReconciliationQueryFailed(reason) -> Unavailable(reason)
      postgres.ReconciliationNotRequired -> NotUncertain
      postgres.ResolutionCommandConflict -> ResolutionConflict
      postgres.ResolutionRouteMismatch
      | postgres.ResolutionWorkerContractMismatch
      | postgres.ResolutionCodecMismatch
      | postgres.ResolutionAttemptMetadataMissing
      | postgres.ResolutionWriteRejected -> RecordMismatch
      postgres.ResolutionRequiresErrorCodec -> ResolutionNeedsErrorCodec
      postgres.ResolutionInvalidValue(reason) -> ResolutionValueRejected(reason)
      postgres.ResolutionCancellationPending -> CancellationPending
      postgres.ResolutionCommitUnknown(resolution_id) ->
        ResolutionCommitUnknown(resolution_id)
      postgres.ResolutionFromAnotherInstallation -> WrongDatabase
    }
  })
}

/// The durable record of one acknowledged attempt.
pub type AcknowledgementReceipt {
  AcknowledgementReceipt(
    command_id: String,
    attempt_id: Int,
    attempt_epoch: Int,
    committed_state: State,
    cause: Option(job.TerminalCause),
    committed_at: Timestamp,
  )
}

/// Reads the receipt of the acknowledgement `command_id`, from a
/// `[grind, job, acknowledged]` event or a diagnostic, to settle whether it
/// committed.
pub fn reconcile_acknowledgement(
  grind: Grind,
  handle: JobHandle(input, output, error),
  command_id: String,
) -> Result(AcknowledgementReceipt, Error) {
  use database <- result.try(database(grind))
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> result.map(fn(receipt) {
    let postgres.AcknowledgementReceipt(
      command_id:,
      attempt_id:,
      attempt_epoch:,
      committed_state:,
      business_failure_cause:,
      committed_at_unix_ms:,
    ) = receipt
    AcknowledgementReceipt(
      command_id:,
      attempt_id:,
      attempt_epoch:,
      committed_state: convert.state(committed_state),
      cause: option.map(business_failure_cause, convert.terminal_cause),
      committed_at: from_unix_us(committed_at_unix_ms * 1000),
    )
  })
  |> result.map_error(fn(error) {
    case error {
      postgres.ReceiptNotFound -> ReceiptNotFound
      postgres.JobNotFound -> JobNotFound
      postgres.JobReadQueryFailed(reason) -> Unavailable(reason)
      postgres.HandleFromAnotherInstallation -> WrongDatabase
      _ -> RecordMismatch
    }
  })
}
