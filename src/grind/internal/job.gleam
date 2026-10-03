//// Defines the typed handle of a persisted job, its states and its outcomes.
////
//// `grind/postgres`'s submit functions return a `JobHandle`, which keeps the
//// worker's codecs so that `postgres.state` and `postgres.outcome` return typed
//// values. `State` lists the persisted lifecycle states, and `Outcome` is the
//// last committed result: pending, succeeded, failed, discarded, cancelled, or
//// waiting for an audited resolution. `available_at` checks an absolute
//// Unix-millisecond time for `postgres.submit_at`.

import gleam/option.{type Option, None, Some}
import grind/internal/worker.{type BusinessFailureCause, type Codec, type Worker}

/// A cheap, in-memory-only, never-persisted identity for one `Database`
/// value's own installation: the physical database (by OID, not by
/// whatever host/port/DNS name reached it — see `postgres.start`'s own doc
/// comment), the configured schema (`postgres.with_schema`), and — when
/// readable — the enclosing PostgreSQL *cluster*'s own
/// `pg_control_system().system_identifier` (a value generated once at
/// `initdb` time, effectively unique per cluster). The cluster identifier
/// exists to disambiguate two *different* clusters that happen to assign
/// the identical low OID to their own first user database (a very common
/// case: a fresh cluster's first created database is conventionally OID
/// 16384) while both also default to schema `"public"` — without it, two
/// such clusters' tokens would be indistinguishable from one shared
/// installation. Stock, unmodified PostgreSQL does not actually restrict
/// `pg_control_system()` at all — any connecting role can call it by
/// default; some managed or deliberately hardened deployments do revoke
/// `EXECUTE` on it from `PUBLIC`, and a role without permission to call it
/// there makes this read fail, silently, at `postgres.start` — never
/// surfaced as a `StartError`, since this disambiguation is a best-effort
/// improvement, not something `start` itself depends on
/// (`postgres.read_cluster_identifier` checks the privilege itself first,
/// as a separate query, specifically so that failure never reaches
/// PostgreSQL's own server log either — see its own doc comment). In that
/// case `same_installation` below falls back to
/// comparing only the database OID and schema, exactly as before this
/// field existed: two different physical clusters that collide on OID and
/// schema can still be indistinguishable to this client-side check when
/// the cluster identifier could not be read on either side — a residual,
/// documented gap, not a regression (see `docs/RISKS.md` risk 7). Stamped
/// onto a `JobHandle`/`PendingSubmission` at mint/bind time so a value
/// minted against one `Database` can be caught, with a typed error, if it
/// is later used against a different one — see
/// `postgres.HandleFromAnotherInstallation` and friends. This is a
/// client-side sanity check only: the real isolation boundary is the
/// PostgreSQL schema itself (see README, "Isolation"), which this token
/// never influences and nothing here is ever written to a row or compared
/// against one.
pub opaque type Installation {
  Installation(
    database_oid: Int,
    schema: String,
    cluster_identifier: Option(Int),
  )
}

pub fn new_installation(
  database_oid: Int,
  schema: String,
  cluster_identifier: Option(Int),
) -> Installation {
  Installation(database_oid:, schema:, cluster_identifier:)
}

pub fn installation_schema(installation: Installation) -> String {
  let Installation(schema:, ..) = installation
  schema
}

/// Whether two `Installation` tokens name the same physical database and
/// configured schema. When both sides successfully read a cluster
/// identifier, it must also match — this is what disambiguates two
/// different clusters that happen to collide on database OID and schema
/// (see `Installation`'s own doc comment); when either side could not read
/// one, this falls back to comparing only the database OID and schema,
/// exactly as it always has. A thin, named wrapper around this fallback
/// logic (never a plain `==`) so every call site reads as a deliberate
/// installation check. Not transitive: an installation whose cluster
/// identifier could not be read can compare equal (via the OID+schema
/// fallback) to two installations that would themselves compare unequal to
/// each other once their own cluster identifiers are actually compared —
/// safe because a `JobHandle`/`PendingSubmission` only ever carries the
/// token of the single `Database` that minted it, so this function is only
/// ever called pairwise against that one minting `Database`, never chained
/// across independently-minted tokens.
pub fn same_installation(a: Installation, b: Installation) -> Bool {
  let Installation(
    database_oid: oid_a,
    schema: schema_a,
    cluster_identifier: cluster_a,
  ) = a
  let Installation(
    database_oid: oid_b,
    schema: schema_b,
    cluster_identifier: cluster_b,
  ) = b
  case cluster_a, cluster_b {
    Some(cluster_a), Some(cluster_b) ->
      cluster_a == cluster_b && oid_a == oid_b && schema_a == schema_b
    _, _ -> oid_a == oid_b && schema_a == schema_b
  }
}

/// A typed reference to a persisted job. Codecs are retained from its definition.
pub opaque type JobHandle(input, output, error) {
  JobHandle(
    id: Int,
    installation: Installation,
    queue: String,
    worker_id: String,
    worker_version: String,
    input: Codec(input),
    output: Codec(output),
    error: Option(Codec(error)),
  )
}

pub opaque type JobId {
  JobId(Int)
}

/// A checked absolute Unix-millisecond time at which a job may run.
pub opaque type AvailableAt {
  AvailableAt(Int)
}

pub type AvailableAtError {
  NegativeUnixMilliseconds
}

pub fn available_at(
  unix_milliseconds: Int,
) -> Result(AvailableAt, AvailableAtError) {
  case unix_milliseconds < 0 {
    True -> Error(NegativeUnixMilliseconds)
    False -> Ok(AvailableAt(unix_milliseconds))
  }
}

pub fn available_at_unix_milliseconds(available_at: AvailableAt) -> Int {
  let AvailableAt(unix_milliseconds) = available_at
  unix_milliseconds
}

pub type State {
  Queued
  Scheduled
  Retryable
  Executing
  Succeeded
  BusinessFailed
  RuntimeFailed
  ContractMismatch
  Uncertain
  Discarded
  Cancelled
}

/// Maps a persisted state column's text (`grind_jobs.state`,
/// `grind_unique_submissions.observed_state`) to its typed `State`. Shared
/// by `postgres.state` and the uniqueness admission path so the mapping is
/// defined once.
pub fn state_of_stored(text: String) -> Result(State, Nil) {
  case text {
    "queued" -> Ok(Queued)
    "scheduled" -> Ok(Scheduled)
    "retryable" -> Ok(Retryable)
    "executing" -> Ok(Executing)
    "succeeded" -> Ok(Succeeded)
    "business_failed" -> Ok(BusinessFailed)
    "runtime_failed" -> Ok(RuntimeFailed)
    "contract_mismatch" -> Ok(ContractMismatch)
    "uncertain" -> Ok(Uncertain)
    "discarded" -> Ok(Discarded)
    "cancelled" -> Ok(Cancelled)
    _ -> Error(Nil)
  }
}

/// The inverse of `state_of_stored`: a typed `State`'s persisted column
/// text. Total (every `State` variant has a stored representation), unlike
/// `state_of_stored`'s partial direction (stored text can be tampered with
/// or, for a closed enum read back from an `Option`/receipt column, absent).
/// Shared the same way, so `grind/observation` and `grind/postgres` encode a
/// `State` through this one definition rather than each keeping its own
/// copy.
pub fn state_to_stored(state: State) -> String {
  case state {
    Queued -> "queued"
    Scheduled -> "scheduled"
    Retryable -> "retryable"
    Executing -> "executing"
    Succeeded -> "succeeded"
    BusinessFailed -> "business_failed"
    RuntimeFailed -> "runtime_failed"
    ContractMismatch -> "contract_mismatch"
    Uncertain -> "uncertain"
    Discarded -> "discarded"
    Cancelled -> "cancelled"
  }
}

pub type Outcome(output, error) {
  Pending(State)
  SucceededWith(output)
  BusinessFailedWith(error)
  BusinessFailedWithCause(error, BusinessFailureCause)
  DiscardedWithReason(String)
  CancelledWithReason(String)
  /// A terminal failure that carries no typed error: invalid stored input, a
  /// handler output or error that its codec rejected (both `RuntimeFailed`),
  /// a codec contract mismatch, or a business failure without an error codec.
  FailedOperationally(String)
  FailedOperationallyWithCause(String, BusinessFailureCause)
  /// A worker declared or recovery detected an uncertain outcome needing an
  /// explicit audited resolution.
  ReconciliationRequired(String)
}

pub fn id(handle: JobHandle(input, output, error)) -> JobId {
  let JobHandle(id:, ..) = handle
  JobId(id)
}

/// Returns the durable PostgreSQL integer so applications can persist and rebind it.
pub fn id_value(handle: JobHandle(input, output, error)) -> Int {
  let JobHandle(id:, ..) = handle
  id
}

pub fn queue(handle: JobHandle(input, output, error)) -> String {
  let JobHandle(queue:, ..) = handle
  queue
}

/// Internal: bind the submitting worker's typed codecs to a returned row.
pub fn new_handle(
  id: Int,
  installation: Installation,
  queue: String,
  worker: Worker(input, output, error),
) -> JobHandle(input, output, error) {
  let #(metadata, input, output, error) = worker.handle_data(worker)
  let worker.Metadata(id: worker_id, worker_version: worker_version, ..) =
    metadata
  JobHandle(
    id:,
    installation:,
    queue:,
    worker_id:,
    worker_version:,
    input:,
    output:,
    error:,
  )
}

/// Internal persistence fields. These do not form a caller-managed workflow.
pub fn storage_fields(
  handle: JobHandle(input, output, error),
) -> #(Int, Installation, String, String, String, Codec(input)) {
  let JobHandle(
    id:,
    installation:,
    queue:,
    worker_id:,
    worker_version:,
    input:,
    ..,
  ) = handle
  #(id, installation, queue, worker_id, worker_version, input)
}

pub fn result_fields(
  handle: JobHandle(input, output, error),
) -> #(
  Int,
  Installation,
  String,
  String,
  String,
  Codec(output),
  Option(Codec(error)),
) {
  let JobHandle(
    id:,
    installation:,
    queue:,
    worker_id:,
    worker_version:,
    output:,
    error:,
    ..,
  ) = handle
  #(id, installation, queue, worker_id, worker_version, output, error)
}

/// Internal identity and persisted codec contract used to resolve an
/// uncertain attempt without repeating worker metadata at the call site.
pub fn reconciliation_fields(
  handle: JobHandle(input, output, error),
) -> #(Int, Installation, String, String, String, String, Option(String)) {
  let JobHandle(
    id:,
    installation:,
    queue:,
    worker_id:,
    worker_version:,
    output:,
    error:,
    ..,
  ) = handle
  let error_version = case error {
    Some(codec) -> Some(worker.codec_version(codec))
    None -> None
  }
  #(
    id,
    installation,
    queue,
    worker_id,
    worker_version,
    worker.codec_version(output),
    error_version,
  )
}

/// Encodes a caller-confirmed output using the admitted job's bound codec.
/// `Error` carries the codec's rejection reason.
pub fn encode_reconciled_success(
  handle: JobHandle(input, output, error),
  value: output,
) -> Result(#(String, String), String) {
  let JobHandle(output:, ..) = handle
  worker.encode_value(output, value)
}

/// Encodes a caller-confirmed business error when the worker retained a
/// codec. `Error` carries the codec's rejection reason.
pub fn encode_reconciled_error(
  handle: JobHandle(input, output, error),
  value: error,
) -> Option(Result(#(String, String), String)) {
  let JobHandle(error:, ..) = handle
  case error {
    Some(codec) -> Some(worker.encode_value(codec, value))
    None -> None
  }
}
