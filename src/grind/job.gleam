import gleam/option.{type Option, None, Some}
import grind/worker.{type BusinessFailureCause, type Codec, type Worker}

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
/// installation. `pg_control_system()` is a restricted, superuser-adjacent
/// function in stock PostgreSQL; a role without permission to call it
/// (an ordinary non-superuser connecting role, the common case for a
/// least-privilege installation) makes this read fail, silently, at
/// `postgres.start` — never surfaced as a `StartError`, since this
/// disambiguation is a best-effort improvement, not something `start`
/// itself depends on. In that case `same_installation` below falls back to
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

@internal
pub fn new_installation(
  database_oid: Int,
  schema: String,
  cluster_identifier: Option(Int),
) -> Installation {
  Installation(database_oid:, schema:, cluster_identifier:)
}

@internal
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
/// installation check.
@internal
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

@internal
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
@internal
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
@internal
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
@internal
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
@internal
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

@internal
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
@internal
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
@internal
pub fn encode_reconciled_success(
  handle: JobHandle(input, output, error),
  value: output,
) -> #(String, String) {
  let JobHandle(output:, ..) = handle
  worker.encode_value(output, value)
}

/// Encodes a caller-confirmed business error when the worker retained a codec.
@internal
pub fn encode_reconciled_error(
  handle: JobHandle(input, output, error),
  value: error,
) -> Option(#(String, String)) {
  let JobHandle(error:, ..) = handle
  case error {
    Some(codec) -> Some(worker.encode_value(codec, value))
    None -> None
  }
}
