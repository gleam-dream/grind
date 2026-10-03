//// The admission vocabulary every submit path (`grind/postgres`'s `submit`,
//// `submit_at`, `submit_with_id`, `submit_unique`, `reconcile_unique`)
//// returns: a stable request identity (`SubmissionId`), when a freshly
//// inserted job may first run (`Availability`), and what an admission
//// transaction produced or could not establish (`Admission`, `Conflict`,
//// `PendingSubmission`, `SubmitError`). `grind/unique` keeps only the
//// uniqueness *policy* vocabulary (which key, which states, how long it
//// stays occupied) — this module has no dependency on it, and is not
//// specific to a uniqueness policy being present at all: plain `submit`/
//// `submit_at` produce `Admission`/`SubmitError` values too.
////
//// `grind/internal/unique_admission` builds these values directly;
//// `grind/postgres`'s submit functions are thin entry points over that
//// module, not a second, translating layer.

import gleam/option.{type Option, None, Some}
import grind/internal/job
import grind/internal/worker.{type Worker}
import pog

/// A stable identity for one admission command, independent of any
/// uniqueness key. Reconciling a retried command uses this identity because
/// a retry may arrive after a uniqueness policy's own period or eligible
/// states no longer match.
pub opaque type SubmissionId {
  SubmissionId(String)
}

pub type SubmissionIdError {
  EmptySubmissionId
}

pub fn submission_id(value: String) -> Result(SubmissionId, SubmissionIdError) {
  case value {
    "" -> Error(EmptySubmissionId)
    _ -> Ok(SubmissionId(value))
  }
}

pub fn submission_id_value(id: SubmissionId) -> String {
  let SubmissionId(value) = id
  value
}

/// When a uniquely-submitted job may first run. Governs only a fresh
/// insertion: a `unique.RescheduleScheduledTo` conflict action carries its
/// own target time independently of this value.
pub type Availability {
  Immediately
  At(job.AvailableAt)
}

/// The submission's own availability, as a millisecond value (`None` for
/// `Immediately`).
pub fn availability_ms(availability: Availability) -> Option(Int) {
  case availability {
    Immediately -> None
    At(at) -> Some(job.available_at_unix_milliseconds(at))
  }
}

// -- Admission results -------------------------------------------------------
//
// These are produced by `grind/internal/unique_admission`'s admission
// transaction and returned unchanged by `grind/postgres`'s submit functions.

/// A persisted row that already occupies a uniqueness key. Not a
/// `JobHandle`: its worker contract is known, but its codecs are not, so a
/// caller must rebind it with `postgres.bind_handle` before reading typed
/// state.
pub opaque type Conflict {
  Conflict(
    job_id: Int,
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

pub fn new_conflict(
  job_id: Int,
  queue: String,
  worker_id: String,
  worker_version: String,
  state: job.State,
) -> Conflict {
  Conflict(job_id:, queue:, worker_id:, worker_version:, state:)
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
    installation: job.Installation,
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

pub fn new_pending_submission(
  installation: job.Installation,
  submission_id: SubmissionId,
  worker: Worker(input, output, error),
  request_sha256: BitArray,
) -> PendingSubmission(input, output, error) {
  PendingSubmission(installation:, submission_id:, worker:, request_sha256:)
}

pub fn pending_submission_installation(
  pending: PendingSubmission(input, output, error),
) -> job.Installation {
  let PendingSubmission(installation:, ..) = pending
  installation
}

pub fn pending_submission_worker(
  pending: PendingSubmission(input, output, error),
) -> Worker(input, output, error) {
  let PendingSubmission(worker:, ..) = pending
  worker
}

pub fn pending_submission_request_sha256(
  pending: PendingSubmission(input, output, error),
) -> BitArray {
  let PendingSubmission(request_sha256:, ..) = pending
  request_sha256
}

pub type SubmitError(input, output, error) {
  EmptyQueueName
  /// The worker's input codec, or a `unique.selected` key's codec, rejected
  /// the value. `reason` is the codec's own text. Checked before any
  /// connection is checked out, so nothing was written and no
  /// `PendingSubmission` exists. Retrying the same value fails the same way.
  InvalidInput(reason: String)
  /// The bounded wait for the admission lock (`postgres.with_unique_lock_wait`,
  /// default 2000ms) elapsed (PostgreSQL `55P03`). No conflicting job is
  /// implied. For `submit_unique` this is the domain-wide advisory lock;
  /// `submit_with_id` has no such lock, but the same bound also caps its
  /// internal wait on the `grind_unique_submissions` primary key when a
  /// concurrent same-id writer's insert is still uncommitted (see
  /// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts").
  AdmissionContended
  /// This `SubmissionId` was already used for a request that does not match
  /// this one (from `submit_unique`'s or `submit_with_id`'s own
  /// in-transaction receipt check, a later `reconcile_unique` call, or a
  /// genuinely concurrent same-id writer that committed first — see
  /// `submit_with_id`'s own doc comment for that last case). Also returned
  /// for a stored receipt whose `decision`/`observed_state` text is not one
  /// this code recognizes — unreachable without direct tampering, since both
  /// columns carry a `CHECK` constraint against the same closed vocabulary
  /// this code decodes, but handled the same fail-closed way rather than
  /// trusted.
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
  NotCommitted(pog.QueryError)
  /// The admission transaction reached the database (its own connection was
  /// checked out and `BEGIN` ran) and its outcome could not be established
  /// afterward — a lost connection mid-commit, or a later receipt lookup
  /// that itself could not reach the store while checking. It may or may
  /// not have committed. Retry with `reconcile_unique`, or with a plain
  /// `submit_unique` retry of the same `SubmissionId` (safe either way: the
  /// admission transaction's own receipt lookup, not candidate selection,
  /// resolves a genuinely committed prior attempt once it becomes visible).
  CommitUnknown(PendingSubmission(input, output, error))
  /// The same genuinely-uncertain outcome `CommitUnknown` describes, for a
  /// plain `submit`/`submit_at` call instead: its insert query failed or its
  /// reply was lost, but a `pog.QueryError` here can also mean the
  /// connection was lost after PostgreSQL already committed the row. Also
  /// reported, conservatively, for a pool-checkout failure that never sent
  /// anything at all (knowably not committed, the same case `NotCommitted`
  /// distinguishes for `submit_unique`/`submit_with_id`) — plain
  /// `submit`/`submit_at` makes no checkout-vs-mid-transaction distinction
  /// of its own, so both shapes are folded into this one variant. Unlike
  /// `submit_unique`/`submit_with_id`, plain `submit`/`submit_at` has no
  /// request identity of its own to retain, so there is no `PendingSubmission`
  /// to reconcile from — retrying can create a duplicate job. A caller that
  /// must retry safely should use `submit_unique`/`submit_with_id` with a
  /// caller-chosen `SubmissionId` instead: its admission transaction records
  /// that identity durably, so a retried request converges on the original
  /// outcome rather than inserting again.
  CommitUnknownWithoutId(pog.QueryError)
  /// `reconcile_unique` only: this `PendingSubmission` was minted against a
  /// different `postgres.Database` (a different physical database, or the
  /// same database under a different configured schema — see
  /// `postgres.with_schema`) than the one it was just used against. Checked
  /// before any storage call is made, purely from the two in-memory
  /// installation tokens — see `grind/job`'s `Installation` type doc comment
  /// for what this client-side check does and, more importantly, does not
  /// guarantee (the real isolation boundary is the PostgreSQL schema itself,
  /// see `README.md`, "Isolation").
  HandleFromAnotherInstallation
}
