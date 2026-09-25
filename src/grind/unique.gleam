//// Pure uniqueness policy values for `grind/postgres`'s admission transaction,
//// and the public result types that admission transaction produces.
////
//// The policy/key/period values below never touch PostgreSQL: they only
//// build and validate the typed description of a uniqueness policy — which
//// key two submissions must share to conflict, which persisted states count
//// as "still occupying that key", how long a key stays occupied, and a
//// stable submission identity used to make a retried admission command
//// idempotent. `grind/internal/unique_admission` reads these values through
//// `@internal` accessors to build the actual SQL, and returns the
//// `Admission`/`Conflict`/`SubmitError`/`PendingSubmission` types
//// below directly — `grind/postgres`'s `submit_unique`/`reconcile_unique` are
//// thin entry points over that module, not a second, translating layer.

import gleam/option.{type Option, None, Some}
import grind/job
import grind/worker.{type Codec, type Worker}
import pog

/// Whether a uniqueness key is scoped to the submitting queue or shared by
/// every queue in the same storage owner.
pub type QueueScope {
  WithinQueue
  AcrossQueues
}

/// Which persisted timestamp a finite period is measured from.
pub type UniqueTimestamp {
  FromInsertion
  FromSchedule
}

/// How long an admitted job continues to occupy its uniqueness key.
pub opaque type Period {
  For(milliseconds: Int, from: UniqueTimestamp)
  WhileRetained
}

pub type PolicyError {
  NonPositivePeriod
  PeriodAbovePrecisionBound
  EmptyKeyName
  EmptySubmissionId
}

/// A finite occupancy window. Rejects a non-positive duration and a duration
/// above Grind's existing millisecond-to-microsecond precision bound (the
/// same bound `worker.retry_delay` enforces), so the same conservative limit
/// applies everywhere Grind converts a millisecond duration into a PostgreSQL
/// interval.
pub fn within_milliseconds(
  milliseconds: Int,
  from: UniqueTimestamp,
) -> Result(Period, PolicyError) {
  case milliseconds <= 0 {
    True -> Error(NonPositivePeriod)
    False ->
      case milliseconds > worker.retry_delay_maximum_milliseconds() {
        True -> Error(PeriodAbovePrecisionBound)
        False -> Ok(For(milliseconds, from))
      }
  }
}

/// The key stays occupied for as long as a matching row is retained, with no
/// time boundary.
pub fn while_retained() -> Period {
  WhileRetained
}

/// Which persisted job states count as still occupying a uniqueness key.
/// `contract_mismatch` is terminal and is never included except by
/// `AllRetained`.
pub type States {
  /// queued, scheduled, retryable, executing, uncertain.
  Incomplete
  /// scheduled only.
  ScheduledOnly
  /// `Incomplete` plus succeeded.
  IncompleteOrSucceeded
  /// Every persisted state.
  AllRetained
}

/// When a uniquely-submitted job may first run. Governs only a fresh
/// insertion: a `RescheduleScheduledTo` conflict action carries its own
/// target time independently of this value.
pub type Availability {
  Immediately
  At(job.AvailableAt)
}

/// What happens to a persisted conflict when a new submission matches it.
pub type ConflictAction {
  /// The persisted row is left exactly as it is.
  KeepExisting
  /// If the persisted conflict is `scheduled`, its `available_at` moves to
  /// this target; any other state is left unchanged. The target lives on
  /// the action itself, so there is no separate "reschedule with no target"
  /// state to reject before storage.
  RescheduleScheduledTo(job.AvailableAt)
}

/// A typed description of which part of a worker's input forms its
/// uniqueness key. The projected type is never exposed here: `selected`
/// closes over the caller's projection and codec, so `Key(input)` never
/// carries a second type parameter for it.
pub opaque type Key(input) {
  FullInput
  Selected(name: String, project: fn(input) -> #(String, String))
}

/// The whole encoded input already admitted forms the key.
pub fn full_input() -> Key(input) {
  FullInput
}

/// A typed projection of the input forms the key, encoded with its own
/// codec. `name` and the codec's version together identify this key's
/// contract, so a changed projection or codec never collides with a
/// differently-meant key that happens to encode to the same JSON.
pub fn selected(
  name: String,
  select: fn(input) -> key,
  codec: Codec(key),
) -> Result(Key(input), PolicyError) {
  case name {
    "" -> Error(EmptyKeyName)
    _ ->
      Ok(
        Selected(name, fn(input) { worker.encode_value(codec, select(input)) }),
      )
  }
}

/// A complete uniqueness policy: the key, its scope, its occupancy window,
/// and which states count as occupying it.
pub opaque type Policy(input) {
  Policy(key: Key(input), scope: QueueScope, period: Period, states: States)
}

pub fn policy(
  key: Key(input),
  scope: QueueScope,
  period: Period,
  states: States,
) -> Policy(input) {
  Policy(key:, scope:, period:, states:)
}

/// A stable identity for one admission command, independent of the
/// uniqueness key. Reconciling a retried command uses this identity because
/// a retry may arrive after the key's period or eligible states no longer
/// match.
pub opaque type SubmissionId {
  SubmissionId(String)
}

pub fn submission_id(value: String) -> Result(SubmissionId, PolicyError) {
  case value {
    "" -> Error(EmptySubmissionId)
    _ -> Ok(SubmissionId(value))
  }
}

pub fn submission_id_value(id: SubmissionId) -> String {
  let SubmissionId(value) = id
  value
}

// -- Admission results -------------------------------------------------------
//
// These are produced by `grind/internal/unique_admission`'s admission
// transaction and returned unchanged by `grind/postgres`'s
// `submit_unique`/`reconcile_unique`.

/// A persisted row that already occupies a uniqueness key. Not a
/// `JobHandle`: its worker contract is known, but its codecs are not, so a
/// caller must rebind it with `postgres.bind_handle` before reading typed
/// state.
pub opaque type Conflict {
  Conflict(
    job_id: Int,
    storage_owner: String,
    queue: String,
    worker_id: String,
    worker_version: String,
    /// The state observed at decision time, not necessarily the row's
    /// current state (it may have progressed since).
    state: job.State,
  )
}

pub fn conflict_job_id(conflict: Conflict) -> Int {
  let Conflict(job_id:, ..) = conflict
  job_id
}

pub fn conflict_queue(conflict: Conflict) -> String {
  let Conflict(queue:, ..) = conflict
  queue
}

pub fn conflict_state(conflict: Conflict) -> job.State {
  let Conflict(state:, ..) = conflict
  state
}

@internal
pub fn new_conflict(
  job_id: Int,
  storage_owner: String,
  queue: String,
  worker_id: String,
  worker_version: String,
  state: job.State,
) -> Conflict {
  Conflict(job_id:, storage_owner:, queue:, worker_id:, worker_version:, state:)
}

/// The result of one `submit_unique` or `reconcile_unique` call.
pub type Admission(input, output, error) {
  Inserted(job.JobHandle(input, output, error))
  Existing(Conflict)
  Rescheduled(Conflict)
}

/// A retained admission command whose outcome could not be established from
/// the transaction result alone. `reconcile_unique` re-reads the receipt
/// this same request would have written.
pub opaque type PendingSubmission(input, output, error) {
  PendingSubmission(
    storage_owner: String,
    submission_id: SubmissionId,
    worker: Worker(input, output, error),
    request_sha256: BitArray,
  )
}

pub fn pending_submission_id(
  pending: PendingSubmission(input, output, error),
) -> SubmissionId {
  let PendingSubmission(submission_id:, ..) = pending
  submission_id
}

@internal
pub fn new_pending_submission(
  storage_owner: String,
  submission_id: SubmissionId,
  worker: Worker(input, output, error),
  request_sha256: BitArray,
) -> PendingSubmission(input, output, error) {
  PendingSubmission(storage_owner:, submission_id:, worker:, request_sha256:)
}

@internal
pub fn pending_submission_storage_owner(
  pending: PendingSubmission(input, output, error),
) -> String {
  let PendingSubmission(storage_owner:, ..) = pending
  storage_owner
}

@internal
pub fn pending_submission_worker(
  pending: PendingSubmission(input, output, error),
) -> Worker(input, output, error) {
  let PendingSubmission(worker:, ..) = pending
  worker
}

@internal
pub fn pending_submission_request_sha256(
  pending: PendingSubmission(input, output, error),
) -> BitArray {
  let PendingSubmission(request_sha256:, ..) = pending
  request_sha256
}

pub type SubmitError(input, output, error) {
  EmptyQueueName
  /// The bounded wait for the admission lock (`postgres.unique_lock_wait`,
  /// default 5000ms) elapsed (PostgreSQL `55P03`). No conflicting job is
  /// implied.
  AdmissionContended
  /// This `SubmissionId` was already used for a request that does not match
  /// this one (from either `submit_unique`'s own in-transaction receipt
  /// check or a later `reconcile_unique` call). Also returned for a stored
  /// receipt whose `decision`/`observed_state` text is not one this code
  /// recognizes — unreachable without direct tampering, since both columns
  /// carry a `CHECK` constraint against the same closed vocabulary this code
  /// decodes, but handled the same fail-closed way rather than trusted.
  SubmissionConflict
  /// The admission did not commit. Reported only when that is knowable
  /// directly: either the store could not even hand out a connection to
  /// attempt the admission at all (`pog.ConnectionUnavailable`, before its
  /// transaction ever began), or any query inside the admission transaction
  /// itself failed (any `pog.QueryError` other than the `55P03` lock-timeout
  /// code, which is `AdmissionContended` instead) — the transaction callback
  /// failed, so `COMMIT` was never sent, and therefore it cannot have
  /// committed. This is not the same claim as "PostgreSQL rolled it back and
  /// confirmed that": the resulting `ROLLBACK` may itself never reach the
  /// server if the connection was already lost, but a `COMMIT` that this
  /// code never sent still cannot have made anything durable either way.
  /// Never reported for a fault that left the true outcome genuinely
  /// uncertain (a lost connection mid-transaction with no such
  /// never-sent-`COMMIT` guarantee; see `CommitUnknown`). Safe to retry the
  /// same `SubmissionId` once the store is reachable; no `PendingSubmission`
  /// is retained because there is nothing to reconcile from.
  AdmissionFailed(pog.QueryError)
  /// The admission transaction reached the database (its own connection was
  /// checked out and `BEGIN` ran) and its outcome could not be established
  /// afterward — a lost connection mid-commit, or a later receipt lookup
  /// that itself could not reach the store while checking. It may or may
  /// not have committed. Retry with `reconcile_unique`, or with a plain
  /// `submit_unique` retry of the same `SubmissionId` (safe either way: the
  /// admission transaction's own receipt lookup, not candidate selection,
  /// resolves a genuinely committed prior attempt once it becomes visible).
  CommitUnknown(PendingSubmission(input, output, error))
}

// -- Internal accessors used only by grind/postgres ------------------------

/// The key's contract string and its encoded JSON text, given the input
/// already admitted and the submitting worker's own input codec version and
/// encoded text (reused for `FullInput` rather than re-encoding).
@internal
pub fn key_material(
  key: Key(input),
  input: input,
  input_codec_version: String,
  encoded_input_json: String,
) -> #(String, String) {
  case key {
    FullInput -> #("full-input:" <> input_codec_version, encoded_input_json)
    Selected(name:, project:) -> {
      let #(codec_version, encoded_json) = project(input)
      #("selected:" <> name <> ":" <> codec_version, encoded_json)
    }
  }
}

@internal
pub type PeriodSpec {
  FinitePeriod(milliseconds: Int, from: UniqueTimestamp)
  Unbounded
}

@internal
pub fn period_spec(period: Period) -> PeriodSpec {
  case period {
    For(milliseconds, from) -> FinitePeriod(milliseconds, from)
    WhileRetained -> Unbounded
  }
}

@internal
pub type PolicyFields(input) {
  PolicyFields(
    key: Key(input),
    scope: QueueScope,
    period: Period,
    states: States,
  )
}

@internal
pub fn policy_fields(policy: Policy(input)) -> PolicyFields(input) {
  let Policy(key:, scope:, period:, states:) = policy
  PolicyFields(key:, scope:, period:, states:)
}

/// The persisted state strings a policy's `States` group admits, in Grind's
/// own vocabulary (`grind/job.State`, lower-cased).
@internal
pub fn eligible_states(states: States) -> List(String) {
  case states {
    Incomplete -> ["queued", "scheduled", "retryable", "executing", "uncertain"]
    ScheduledOnly -> ["scheduled"]
    IncompleteOrSucceeded -> [
      "queued", "scheduled", "retryable", "executing", "uncertain", "succeeded",
    ]
    AllRetained -> [
      "queued", "scheduled", "retryable", "executing", "succeeded",
      "business_failed", "runtime_failed", "contract_mismatch", "uncertain",
      "discarded", "cancelled",
    ]
  }
}

// -- Stable string labels for the admission request fingerprint ------------
//
// These have no SQL meaning; they are only embedded in the request
// fingerprint (`grind/internal/unique_admission`) so a retried
// `SubmissionId` with a changed scope/period/states/action is detected as a
// conflicting request rather than silently replayed.

@internal
pub fn scope_label(scope: QueueScope) -> String {
  case scope {
    WithinQueue -> "within_queue"
    AcrossQueues -> "across_queues"
  }
}

@internal
pub fn states_label(states: States) -> String {
  case states {
    Incomplete -> "incomplete"
    ScheduledOnly -> "scheduled_only"
    IncompleteOrSucceeded -> "incomplete_or_succeeded"
    AllRetained -> "all_retained"
  }
}

@internal
pub fn action_label(action: ConflictAction) -> String {
  case action {
    KeepExisting -> "keep_existing"
    RescheduleScheduledTo(_) -> "reschedule_scheduled_to"
  }
}

@internal
pub fn period_origin_label(from: UniqueTimestamp) -> String {
  case from {
    FromInsertion -> "from_insertion"
    FromSchedule -> "from_schedule"
  }
}

/// The persisted-column name a finite period's origin measures from.
@internal
pub fn period_column(from: UniqueTimestamp) -> String {
  case from {
    FromInsertion -> "inserted_at"
    FromSchedule -> "available_at"
  }
}

/// The reschedule target's millisecond value, or `None` for `KeepExisting`.
@internal
pub fn reschedule_target_ms(action: ConflictAction) -> Option(Int) {
  case action {
    KeepExisting -> None
    RescheduleScheduledTo(at) -> Some(job.available_at_unix_milliseconds(at))
  }
}

/// The submission's own availability, as a millisecond value (`None` for
/// `Immediately`).
@internal
pub fn availability_ms(availability: Availability) -> Option(Int) {
  case availability {
    Immediately -> None
    At(at) -> Some(job.available_at_unix_milliseconds(at))
  }
}
