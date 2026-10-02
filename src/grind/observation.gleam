//// Grind-owned Sinal event descriptors for its durable job lifecycle.
////
//// Grind does not own a telemetry event sum type or a subscription API —
//// applications attach their own Sinal handlers
//// (`sinal.observe`/`sinal.attach`) to the descriptors this module exposes,
//// the same way they would attach to any other Sinal event. This module
//// only owns the events themselves: their native names and their typed
//// measurement/metadata field contracts. Shared wire primitives and field
//// codecs live in `grind/internal/observation/wire`.
////
//// Every event here is emitted through `grind/postgres`'s own
//// `sinal/forwarder.Forwarder` (one per `Database`), never through a plain
//// `sinal.emit`, so a slow or blocked attached handler stalls only the
//// forwarder process — never the coordinator that is proving the commit or
//// the worker that produced it. See `sinal/forwarder`'s module documentation
//// for the resulting delivery semantics: best-effort, per-producer FIFO, and
//// silently dropped past the configured capacity (reported separately via
//// `sinal/forwarder.dropped_event`).
////
//// Delivery is best-effort: an event can be delivered more than once (a
//// `Reconciled` observation after an earlier `Replied` one for the exact
//// same `command_id`), and it can be lost entirely (forwarder capacity
//// exceeded, or the forwarder process down between a crash and its next
//// supervised restart). A persistently crashing or exiting handler can also
//// exhaust the forwarder's own nested supervisor's restart budget; once that
//// happens the forwarder is never restarted again for that `Database`'s
//// lifetime — a permanent degraded state, not a transient gap — and every
//// later `forwarder.emit` for it reports `ForwarderUnavailable`, which Grind
//// discards. Admission, claiming, and acknowledgement all continue
//// unaffected either way; only observations are lost. The durable truth is
//// always Grind's own tables and receipts, never an observation — a handler
//// attached here must not be the only place an outcome is recorded.
////
//// Every descriptor is emitted only once its commit is proven: either this
//// call's own transaction reply confirmed the write (`Replied`), or a
//// later read of a durable receipt confirmed a commit whose own reply was
//// lost (`Reconciled`). Metadata carries a dedupe key so a consumer that
//// cares can collapse a `Replied`/`Reconciled` pair for the same command.
////
//// The events: `acknowledged`, `admitted`, `claimed`, `quarantined`,
//// `resolved`, `cancellation_decided`, `released`,
//// `contract_mismatch_recorded`, `prune_completed`, and `prune_failed` —
//// the last two are the exception to nearly everything above: each is one
//// aggregate per `postgres.prune_finished` call (`prune_completed` from
//// `prune_finished` itself; `prune_failed` from `grind/pruner`, which has
//// no caller to return a failed call's `PruneError` to) rather than one
//// event per job, so neither carries a `JobRef`/`AttemptRef` at all.
//// `JobRef`/`AttemptRef` are the shared
//// identity/attempt-fencing shapes every event's metadata embeds rather
//// than redeclaring per event; embedding them in `acknowledged`'s own
//// metadata does not change its wire keys (a nested `sinal/fields` record
//// flattens into the same top-level fields either way), so this is unreleased-only
//// housekeeping, not a compatibility break.

import gleam/option.{type Option}
import grind/internal/observation/wire as observation_wire
import grind/job
import grind/worker
import sinal.{type Event}
import sinal/fields

/// How a `Replied`/`Reconciled` observation is emitted here: `Replied` is
/// this call's own commit reply, received directly, causing a fresh durable
/// write. `Reconciled` is a commit proven by reading back an existing
/// durable receipt instead — the exact same command was already durably
/// applied, either by an earlier attempt of this same caller whose reply was
/// lost, or by a concurrent retry that raced it. Both are genuine proof of a
/// commit; `Reconciled` never means "assumed" or "probably".
pub type Confirmation {
  Replied
  Reconciled
}

/// The closed set of dispositions a worker can propose for one attempt,
/// mirroring `grind/worker.Execution`'s shape without depending on it —
/// this module only needs the disposition kind, not the worker's typed
/// payload.
pub type Proposed {
  ProposedSuccess
  ProposedBusinessFailure
  ProposedRetryable
  ProposedRuntimeFailed
  ProposedSnoozed
  ProposedDiscarded
  ProposedCancelled
  ProposedUncertain
}

/// Shared identity carried by every `[grind, job, *]` event: which job,
/// queue, and worker contract the event is about. Reused as an embedded
/// field rather than re-declared per event, so the underlying `sinal/fields`
/// codec for it (`job_ref_fields`, defined once further down, above every
/// metadata field builder that embeds it) is also built exactly once.
pub type JobRef {
  JobRef(job_id: Int, queue: String, worker_id: String, worker_version: String)
}

/// Shared attempt fencing fields carried by every event that is about one
/// claimed attempt rather than the job as a whole (`acknowledged`, `claimed`,
/// `quarantined`, `released`, `contract_mismatch_recorded`). `attempt` is the attempt
/// number (`grind_jobs.attempt_count` at the time of the event), distinct
/// from `attempt_id` (the durable per-attempt identity) and `epoch` (this
/// job's own attempt-fencing counter).
pub type AttemptRef {
  AttemptRef(attempt_id: Int, epoch: Int, attempt: Int)
}

/// `[grind, job, acknowledged]` measurements. `count` is always `1`; no
/// attempt-duration timing is available at the acknowledgement boundary
/// without new machinery, so none is claimed here.
pub type AcknowledgedMeasurements {
  AcknowledgedMeasurements(count: Int)
}

/// `[grind, job, acknowledged]` metadata.
///
/// `proposed` is what the worker's execution proposed; `committed_state` is
/// what was actually durably committed, read from the acknowledgement's own
/// `RETURNING`/receipt — never from the proposal. They can differ: a
/// concurrent cancellation overrides a proposed success (or any other
/// proposal) with a committed `Cancelled`. Retry-budget exhaustion is
/// decided by the worker's own business retry policy before the
/// acknowledgement ever runs — an exhausted retry is proposed as a business
/// failure (`worker.ExecutedBusinessFailure(.., BudgetExhausted)`, so
/// `proposed` is already `ProposedBusinessFailure`), not as a `Retryable`
/// proposal the acknowledgement later reinterprets. The acknowledgement's
/// own `retryable` commit path re-checks `attempt_count < max_attempts` as a
/// defensive consistency guard, not a policy decision: if that guard fails
/// for a genuinely proposed retry, the whole acknowledgement is rejected as
/// stale (no commit, no observation) rather than silently committing some
/// other outcome. `available_at_unix_ms`
/// is only meaningful for a committed `Retryable`/`Scheduled` outcome (the
/// job's next eligibility time) and is `None` otherwise, including whenever
/// the commit was proven by `Reconciled` receipt-read rather than by a fresh
/// write — the acknowledgement receipt itself does not retain a
/// re-derivable `available_at`. `command_id` is the stable acknowledgement
/// fence for this attempt (`grind/internal/attempt.acknowledgement_command_id`),
/// making it this event's dedupe key across a `Replied`/`Reconciled` pair.
pub type AcknowledgedMetadata {
  AcknowledgedMetadata(
    ref: JobRef,
    attempt: AttemptRef,
    proposed: Proposed,
    committed_state: job.State,
    failure_cause: Option(worker.BusinessFailureCause),
    available_at_unix_ms: Option(Int),
    confirmation: Confirmation,
    command_id: String,
  )
}

/// `[grind, job, admitted]` measurements. `count` is always `1`.
pub type AdmittedMeasurements {
  AdmittedMeasurements(count: Int)
}

/// `[grind, job, admitted]` metadata, shared by a plain `submit`/`submit_at`
/// admission and a `submit_unique` admission decision (`Inserted`, `Existing`,
/// or `Rescheduled`).
///
/// `committed_state` always comes from this call's own committed
/// transaction, never the request — the same discipline `acknowledged`'s
/// metadata documents — but the exact source differs by admission shape: a
/// plain submit reads it back from the insert's own `RETURNING`; a unique
/// `Inserted` decision is the computed initial state (`queued`/`scheduled`)
/// bound as that same insert's own parameter; a `Rescheduled` decision is the
/// value that decision's own `UPDATE` just wrote (always `scheduled`); an
/// `Existing` decision is read from the locked candidate row inside the same
/// admission transaction. `available_at_unix_ms` is `Some` only when
/// `committed_state` is one where "next eligibility time" is a meaningful
/// idea — `Queued`, `Scheduled`, or `Retryable` — and `None` otherwise,
/// including for an `Existing` conflict whose candidate row is in some other
/// eligible-for-the-policy state (`Executing`, `Succeeded`, and beyond,
/// depending on the policy's `States` group) where the column's raw value is
/// not a real next-run time. It is also `None` for a `Reconciled` decision
/// (proven by a receipt read rather than a fresh write): the receipt does
/// not retain a re-derivable `available_at`, the same limitation
/// `acknowledged` documents for its own receipt-proven commits.
/// `submission_id` is `None` for a plain submission and `Some` for a unique
/// one — Grind's own uniqueness identity, not a caller-assigned job id.
pub type AdmittedMetadata {
  AdmittedMetadata(
    ref: JobRef,
    committed_state: job.State,
    available_at_unix_ms: Option(Int),
    submission_id: Option(String),
    confirmation: Confirmation,
  )
}

/// `[grind, job, claimed]` measurements. `count` is always `1`.
pub type ClaimedMeasurements {
  ClaimedMeasurements(count: Int)
}

/// `[grind, job, claimed]` metadata. The claim itself autocommits as a single
/// fenced `UPDATE ... RETURNING`, so a returned row is already the proof of
/// commit — there is no separate transaction reply to distinguish a
/// `Replied`/`Reconciled` pair for this event. `previous_state` is the state
/// the row held immediately before this claim (`queued`, `scheduled`, or
/// `retryable`), read from the same `RETURNING`.
pub type ClaimedMetadata {
  ClaimedMetadata(ref: JobRef, attempt: AttemptRef, previous_state: job.State)
}

/// `[grind, job, quarantined]` measurements. `count` is always `1`.
pub type QuarantinedMeasurements {
  QuarantinedMeasurements(count: Int)
}

/// `[grind, job, quarantined]` metadata: one abandoned attempt (an expired
/// lease found by the claim-time quarantine scan) moved to `uncertain`.
/// `cancellation_was_requested` distinguishes an abandoned attempt that also
/// had a pending cancellation request from an ordinary one — both still
/// require audited reconciliation, but the failure description differs (see
/// `postgres`'s quarantine scan).
pub type QuarantinedMetadata {
  QuarantinedMetadata(
    ref: JobRef,
    attempt: AttemptRef,
    cancellation_was_requested: Bool,
  )
}

/// `[grind, job, resolved]` measurements. `count` is always `1`.
pub type ResolvedMeasurements {
  ResolvedMeasurements(count: Int)
}

/// The closed set of decisions an operator can apply to an `uncertain` job,
/// mirroring `grind/postgres.Resolution`'s shape without depending on it.
pub type ResolutionDecision {
  DecisionConfirmSuccess
  DecisionConfirmBusinessFailure
  DecisionAuthorizeReplay
}

/// `[grind, job, resolved]` metadata: an audited operator decision applied to
/// an `uncertain` job. `confirmation` is `Replied` when this call's own
/// transaction committed the decision, `Reconciled` when an identical
/// decision was already durably applied by an earlier attempt of this same
/// `resolution_id` (`postgres.ResolutionAlreadyApplied`) — the resolution
/// analogue of `acknowledged`'s duplicate-command reconciliation.
/// `committed_state` comes from that same proven outcome, never the request.
pub type ResolvedMetadata {
  ResolvedMetadata(
    ref: JobRef,
    decision: ResolutionDecision,
    committed_state: job.State,
    resolution_id: String,
    resolved_by: String,
    confirmation: Confirmation,
  )
}

/// `[grind, job, cancellation_decided]` measurements. `count` is always `1`.
pub type CancellationMeasurements {
  CancellationMeasurements(count: Int)
}

/// The two `postgres.CancellationResult` variants that are genuine writes;
/// the read-only variants (`AlreadyCancelled`, `AlreadyUncertain`,
/// `AlreadyFinished`) never reach this type because they never emit.
pub type CancellationOutcome {
  CancellationDecidedBeforeRun
  CancellationDecidedWhileRunning
}

/// `[grind, job, cancellation_decided]` metadata. `outcome: CancellationDecidedWhileRunning`
/// can be delivered more than once for the same job: cancelling an already
/// `executing` job whose cancellation is already requested is idempotent at
/// the storage layer (`postgres`'s `cancel_executing` re-affirms the existing
/// request rather than rejecting the call), so a caller that retries a
/// cancellation request observes this event again rather than a distinct
/// no-op outcome.
pub type CancellationMetadata {
  CancellationMetadata(
    ref: JobRef,
    previous_state: job.State,
    outcome: CancellationOutcome,
  )
}

/// `[grind, job, released]` measurements. `count` is always `1`.
pub type ReleasedMeasurements {
  ReleasedMeasurements(count: Int)
}

/// `[grind, job, released]` metadata: a claimed attempt refunded before its
/// worker ever ran (the temporary worker child failed to start). `restored_state`
/// is the state the row is returned to — the same state it held before this
/// claim.
pub type ReleasedMetadata {
  ReleasedMetadata(ref: JobRef, attempt: AttemptRef, restored_state: job.State)
}

/// `[grind, job, contract_mismatch_recorded]` measurements. `count` is always `1`.
pub type ContractMismatchMeasurements {
  ContractMismatchMeasurements(count: Int)
}

/// `[grind, job, contract_mismatch_recorded]` metadata: a claimed attempt parked in
/// the terminal, nonclaimable `contract_mismatch` state (not returned to
/// `queued` — contrast `released` above) because the registered worker's
/// codec contract no longer matches what was persisted when the job was
/// admitted (a deploy changed a codec version without a matching
/// worker/version bump). `expected_version` is what was stored;
/// `actual_version` is what the currently registered worker declares.
pub type ContractMismatchMetadata {
  ContractMismatchMetadata(
    ref: JobRef,
    attempt: AttemptRef,
    kind: worker.CodecKind,
    expected_version: String,
    actual_version: String,
  )
}

/// `[grind, prune, completed]` measurements: how many rows one
/// `postgres.prune_finished` call actually deleted from `grind_jobs` —
/// mirroring `postgres.PruneReport`'s own shape (each deleted job's own
/// acknowledgement/uniqueness-submission/resolution receipts cascade with
/// it, via `grind_v12`'s own `ON DELETE CASCADE` foreign keys, and are not
/// separately counted). Unlike every other event in this module, this one
/// is not about a single job: it is one aggregate per call, the same shape
/// Oban's own `[:oban, :plugin]` pruner span reports a single
/// `pruned_count` for.
pub type PruneCompletedMeasurements {
  PruneCompletedMeasurements(jobs: Int)
}

/// `[grind, prune, completed]` metadata: the retention window and batch
/// size this exact call was given, so a handler can tell a small
/// exhausted-limit batch (`measurements.jobs == limit`, worth looping again
/// immediately) from a genuinely empty one apart from the count alone.
pub type PruneCompletedMetadata {
  PruneCompletedMetadata(older_than_ms: Int, limit: Int)
}

/// `[grind, prune, failed]` measurements: always `count: 1`, the same
/// "this happened once" convention every `[grind, job, *]` event's own
/// `count` field already uses — there is no meaningful row count for a call
/// that never committed cleanly.
pub type PruneFailedMeasurements {
  PruneFailedMeasurements(count: Int)
}

/// Coarsely classifies why a `grind/pruner` tick's own `prune_finished`
/// call failed, from the `pog.QueryError` it returned — narrow enough to be
/// useful in a handler without exposing `pog`'s own type as part of Grind's
/// observation surface.
pub type PruneFailureKind {
  /// The reply was lost after the statement may have already reached
  /// PostgreSQL and committed (`pog.QueryTimeout`) — rows may have been
  /// deleted despite this being reported as a failure.
  PruneReplyLost
  /// The statement ran, returned rows, and committed, but they could not be
  /// decoded (`pog.UnexpectedResultType`) — the delete itself already
  /// happened; only reading its own `RETURNING` back failed.
  PruneResultUndecodable
  /// A real PostgreSQL-side rejection before anything could commit
  /// (`pog.ConstraintViolated`/`pog.PostgresqlError`) — nothing was
  /// deleted.
  PruneRejected
  /// The call never reached PostgreSQL at all (`pog.ConnectionUnavailable`/
  /// `pog.UnexpectedArgumentCount`/`pog.UnexpectedArgumentType`) — nothing
  /// was attempted.
  PruneNotAttempted
}

/// `[grind, prune, failed]` metadata: the retention window and batch size
/// the failed call was given, plus its own coarse failure kind.
pub type PruneFailedMetadata {
  PruneFailedMetadata(older_than_ms: Int, limit: Int, kind: PruneFailureKind)
}

fn proposed_to_string(proposed: Proposed) -> String {
  case proposed {
    ProposedSuccess -> "succeeded"
    ProposedBusinessFailure -> "business_failed"
    ProposedRetryable -> "retryable"
    ProposedRuntimeFailed -> "runtime_failed"
    ProposedSnoozed -> "snoozed"
    ProposedDiscarded -> "discarded"
    ProposedCancelled -> "cancelled"
    ProposedUncertain -> "uncertain"
  }
}

fn proposed_field() -> fields.Fields(Proposed) {
  fields.enum(
    "proposed",
    [
      ProposedSuccess,
      ProposedBusinessFailure,
      ProposedRetryable,
      ProposedRuntimeFailed,
      ProposedSnoozed,
      ProposedDiscarded,
      ProposedCancelled,
      ProposedUncertain,
    ],
    proposed_to_string,
  )
}

fn confirmation_field() -> fields.Fields(Confirmation) {
  fields.enum("confirmation", [Replied, Reconciled], fn(confirmation) {
    case confirmation {
      Replied -> "replied"
      Reconciled -> "reconciled"
    }
  })
}

fn resolution_decision_field() -> fields.Fields(ResolutionDecision) {
  fields.enum(
    "decision",
    [
      DecisionConfirmSuccess,
      DecisionConfirmBusinessFailure,
      DecisionAuthorizeReplay,
    ],
    fn(decision) {
      case decision {
        DecisionConfirmSuccess -> "confirm_success"
        DecisionConfirmBusinessFailure -> "confirm_business_failure"
        DecisionAuthorizeReplay -> "authorize_replay"
      }
    },
  )
}

fn cancellation_outcome_field() -> fields.Fields(CancellationOutcome) {
  fields.enum(
    "outcome",
    [CancellationDecidedBeforeRun, CancellationDecidedWhileRunning],
    fn(outcome) {
      case outcome {
        CancellationDecidedBeforeRun -> "cancelled_before_run"
        CancellationDecidedWhileRunning -> "cancellation_requested"
      }
    },
  )
}

fn codec_kind_field() -> fields.Fields(worker.CodecKind) {
  fields.enum(
    "kind",
    [worker.InputCodec, worker.OutputCodec, worker.ErrorCodec],
    fn(kind) {
      case kind {
        worker.InputCodec -> "input"
        worker.OutputCodec -> "output"
        worker.ErrorCodec -> "error"
      }
    },
  )
}

fn failure_cause_field() -> fields.Fields(worker.BusinessFailureCause) {
  fields.enum(
    "failure_cause",
    [worker.BudgetExhausted, worker.RetryDeclined],
    worker.business_failure_cause_to_string,
  )
}

fn prune_failure_kind_field() -> fields.Fields(PruneFailureKind) {
  fields.enum(
    "kind",
    [PruneReplyLost, PruneResultUndecodable, PruneRejected, PruneNotAttempted],
    fn(kind) {
      case kind {
        PruneReplyLost -> "reply_lost"
        PruneResultUndecodable -> "result_undecodable"
        PruneRejected -> "rejected"
        PruneNotAttempted -> "not_attempted"
      }
    },
  )
}

fn job_ref_fields() -> fields.Fields(JobRef) {
  observation_wire.job_ref_fields(
    JobRef,
    fn(ref: JobRef) { ref.job_id },
    fn(ref) { ref.queue },
    fn(ref) { ref.worker_id },
    fn(ref) { ref.worker_version },
  )
}

fn attempt_ref_fields() -> fields.Fields(AttemptRef) {
  observation_wire.attempt_ref_fields(
    AttemptRef,
    fn(ref: AttemptRef) { ref.attempt_id },
    fn(ref) { ref.epoch },
    fn(ref) { ref.attempt },
  )
}

/// A one-key `count` measurement record, shared by every event whose only
/// measurement is `count`.
fn count_record(make: fn(Int) -> m, count: fn(m) -> Int) -> fields.Fields(m) {
  use count <- fields.include(observation_wire.count_fields(), get: count)
  fields.success(make(count))
}

fn admitted_metadata_fields() -> fields.Fields(AdmittedMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use committed_state <- fields.include(
    observation_wire.job_state_field("committed_state"),
    get: fn(m) { m.committed_state },
  )
  use available_at_unix_ms <- fields.include(
    fields.optional(fields.int("available_at_unix_ms")),
    get: fn(m) { m.available_at_unix_ms },
  )
  use submission_id <- fields.include(
    fields.optional(fields.string("submission_id")),
    get: fn(m) { m.submission_id },
  )
  use confirmation <- fields.include(confirmation_field(), get: fn(m) {
    m.confirmation
  })
  fields.success(AdmittedMetadata(
    ref:,
    committed_state:,
    available_at_unix_ms:,
    submission_id:,
    confirmation:,
  ))
}

fn claimed_metadata_fields() -> fields.Fields(ClaimedMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use attempt <- fields.include(attempt_ref_fields(), get: fn(m) { m.attempt })
  use previous_state <- fields.include(
    observation_wire.job_state_field("previous_state"),
    get: fn(m) { m.previous_state },
  )
  fields.success(ClaimedMetadata(ref:, attempt:, previous_state:))
}

fn quarantined_metadata_fields() -> fields.Fields(QuarantinedMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use attempt <- fields.include(attempt_ref_fields(), get: fn(m) { m.attempt })
  use cancellation_was_requested <- fields.include(
    fields.bool("cancellation_was_requested"),
    get: fn(m) { m.cancellation_was_requested },
  )
  fields.success(QuarantinedMetadata(
    ref:,
    attempt:,
    cancellation_was_requested:,
  ))
}

fn resolved_metadata_fields() -> fields.Fields(ResolvedMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use decision <- fields.include(resolution_decision_field(), get: fn(m) {
    m.decision
  })
  use committed_state <- fields.include(
    observation_wire.job_state_field("committed_state"),
    get: fn(m) { m.committed_state },
  )
  use resolution_id <- fields.include(
    fields.string("resolution_id"),
    get: fn(m) { m.resolution_id },
  )
  use resolved_by <- fields.include(fields.string("resolved_by"), get: fn(m) {
    m.resolved_by
  })
  use confirmation <- fields.include(confirmation_field(), get: fn(m) {
    m.confirmation
  })
  fields.success(ResolvedMetadata(
    ref:,
    decision:,
    committed_state:,
    resolution_id:,
    resolved_by:,
    confirmation:,
  ))
}

fn cancellation_metadata_fields() -> fields.Fields(CancellationMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use previous_state <- fields.include(
    observation_wire.job_state_field("previous_state"),
    get: fn(m) { m.previous_state },
  )
  use outcome <- fields.include(cancellation_outcome_field(), get: fn(m) {
    m.outcome
  })
  fields.success(CancellationMetadata(ref:, previous_state:, outcome:))
}

fn released_metadata_fields() -> fields.Fields(ReleasedMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use attempt <- fields.include(attempt_ref_fields(), get: fn(m) { m.attempt })
  use restored_state <- fields.include(
    observation_wire.job_state_field("restored_state"),
    get: fn(m) { m.restored_state },
  )
  fields.success(ReleasedMetadata(ref:, attempt:, restored_state:))
}

fn contract_mismatch_metadata_fields() -> fields.Fields(
  ContractMismatchMetadata,
) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use attempt <- fields.include(attempt_ref_fields(), get: fn(m) { m.attempt })
  use kind <- fields.include(codec_kind_field(), get: fn(m) { m.kind })
  use expected_version <- fields.include(
    fields.string("expected_version"),
    get: fn(m) { m.expected_version },
  )
  use actual_version <- fields.include(
    fields.string("actual_version"),
    get: fn(m) { m.actual_version },
  )
  fields.success(ContractMismatchMetadata(
    ref:,
    attempt:,
    kind:,
    expected_version:,
    actual_version:,
  ))
}

fn prune_completed_measurements_fields() -> fields.Fields(
  PruneCompletedMeasurements,
) {
  use jobs <- fields.include(fields.int("jobs"), get: fn(m) { m.jobs })
  fields.success(PruneCompletedMeasurements(jobs:))
}

fn prune_completed_metadata_fields() -> fields.Fields(PruneCompletedMetadata) {
  use older_than_ms <- fields.include(fields.int("older_than_ms"), get: fn(m) {
    m.older_than_ms
  })
  use limit <- fields.include(fields.int("limit"), get: fn(m) { m.limit })
  fields.success(PruneCompletedMetadata(older_than_ms:, limit:))
}

fn prune_failed_metadata_fields() -> fields.Fields(PruneFailedMetadata) {
  use older_than_ms <- fields.include(fields.int("older_than_ms"), get: fn(m) {
    m.older_than_ms
  })
  use limit <- fields.include(fields.int("limit"), get: fn(m) { m.limit })
  use kind <- fields.include(prune_failure_kind_field(), get: fn(m) { m.kind })
  fields.success(PruneFailedMetadata(older_than_ms:, limit:, kind:))
}

fn metadata_fields() -> fields.Fields(AcknowledgedMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use attempt <- fields.include(attempt_ref_fields(), get: fn(m) { m.attempt })
  use proposed <- fields.include(proposed_field(), get: fn(m) { m.proposed })
  use committed_state <- fields.include(
    observation_wire.job_state_field("committed_state"),
    get: fn(m) { m.committed_state },
  )
  use failure_cause <- fields.include(
    fields.optional(failure_cause_field()),
    get: fn(m) { m.failure_cause },
  )
  use available_at_unix_ms <- fields.include(
    fields.optional(fields.int("available_at_unix_ms")),
    get: fn(m) { m.available_at_unix_ms },
  )
  use confirmation <- fields.include(confirmation_field(), get: fn(m) {
    m.confirmation
  })
  use command_id <- fields.include(fields.string("command_id"), get: fn(m) {
    m.command_id
  })
  fields.success(AcknowledgedMetadata(
    ref:,
    attempt:,
    proposed:,
    committed_state:,
    failure_cause:,
    available_at_unix_ms:,
    confirmation:,
    command_id:,
  ))
}

/// The `[grind, job, acknowledged]` event descriptor: one committed
/// disposition for one claimed attempt. Emitted by `grind/postgres` from its
/// own `sinal/forwarder.Forwarder`, strictly after that disposition is
/// proven committed (see the module documentation above).
pub fn acknowledged() -> Event(AcknowledgedMeasurements, AcknowledgedMetadata) {
  sinal.event(
    observation_wire.job_event_name("acknowledged"),
    count_record(AcknowledgedMeasurements, fn(m) { m.count }),
    metadata_fields(),
  )
}

/// The `[grind, job, admitted]` event descriptor: one committed admission
/// decision, either a plain `submit`/`submit_at` or a `submit_unique` outcome
/// (`Inserted`, `Existing`, or `Rescheduled`). Emitted by `grind/postgres`
/// strictly after that decision is proven committed — see `AdmittedMetadata`.
pub fn admitted() -> Event(AdmittedMeasurements, AdmittedMetadata) {
  sinal.event(
    observation_wire.job_event_name("admitted"),
    count_record(AdmittedMeasurements, fn(m) { m.count }),
    admitted_metadata_fields(),
  )
}

/// The `[grind, job, claimed]` event descriptor: one job atomically claimed
/// for execution. Emitted strictly after the claim's own fenced `UPDATE ...
/// RETURNING` returns a row — see `ClaimedMetadata`.
pub fn claimed() -> Event(ClaimedMeasurements, ClaimedMetadata) {
  sinal.event(
    observation_wire.job_event_name("claimed"),
    count_record(ClaimedMeasurements, fn(m) { m.count }),
    claimed_metadata_fields(),
  )
}

/// The `[grind, job, quarantined]` event descriptor: one abandoned attempt
/// (an expired lease) moved to `uncertain` by the claim-time quarantine scan.
/// Emitted once per row the scan's `RETURNING` reports as quarantined.
pub fn quarantined() -> Event(QuarantinedMeasurements, QuarantinedMetadata) {
  sinal.event(
    observation_wire.job_event_name("quarantined"),
    count_record(QuarantinedMeasurements, fn(m) { m.count }),
    quarantined_metadata_fields(),
  )
}

/// The `[grind, job, resolved]` event descriptor: one audited operator
/// decision committed against an `uncertain` job. Emitted strictly after that
/// decision is proven committed — see `ResolvedMetadata`.
pub fn resolved() -> Event(ResolvedMeasurements, ResolvedMetadata) {
  sinal.event(
    observation_wire.job_event_name("resolved"),
    count_record(ResolvedMeasurements, fn(m) { m.count }),
    resolved_metadata_fields(),
  )
}

/// The `[grind, job, cancellation_decided]` event descriptor: a cancellation request
/// that changed something durable (`CancellationDecidedBeforeRun` or
/// `CancellationDecidedWhileRunning`). The three read-only outcomes
/// (`AlreadyCancelled`, `AlreadyUncertain`, `AlreadyFinished`) never emit —
/// see `CancellationMetadata`.
pub fn cancellation_decided() -> Event(
  CancellationMeasurements,
  CancellationMetadata,
) {
  sinal.event(
    observation_wire.job_event_name("cancellation_decided"),
    count_record(CancellationMeasurements, fn(m) { m.count }),
    cancellation_metadata_fields(),
  )
}

/// The `[grind, job, released]` event descriptor: a claimed attempt refunded
/// before its worker ever ran. Emitted strictly after the release's own
/// fenced `UPDATE ... RETURNING` returns a row.
pub fn released() -> Event(ReleasedMeasurements, ReleasedMetadata) {
  sinal.event(
    observation_wire.job_event_name("released"),
    count_record(ReleasedMeasurements, fn(m) { m.count }),
    released_metadata_fields(),
  )
}

/// The `[grind, job, contract_mismatch_recorded]` event descriptor: a claimed attempt
/// parked in the terminal, nonclaimable `contract_mismatch` state because a
/// registered worker's codec contract no longer matches what was persisted
/// at admission. Emitted strictly after the mismatch's own fenced
/// `UPDATE ... RETURNING` returns a row.
pub fn contract_mismatch_recorded() -> Event(
  ContractMismatchMeasurements,
  ContractMismatchMetadata,
) {
  sinal.event(
    observation_wire.job_event_name("contract_mismatch_recorded"),
    count_record(ContractMismatchMeasurements, fn(m) { m.count }),
    contract_mismatch_metadata_fields(),
  )
}

/// The `[grind, prune, completed]` event descriptor: one aggregate per
/// `postgres.prune_finished` call, emitted strictly after that call's own
/// autocommitted delete has already returned its counts — never one event
/// per deleted job. Unlike every `[grind, job, *]` event above, this is not
/// scoped to a single job at all, so it lives under its own `[grind, prune,
/// ...]` prefix rather than reusing `job_event_name`.
pub fn prune_completed() -> Event(
  PruneCompletedMeasurements,
  PruneCompletedMetadata,
) {
  sinal.event(
    ["grind", "prune", "completed"],
    prune_completed_measurements_fields(),
    prune_completed_metadata_fields(),
  )
}

/// The `[grind, prune, failed]` event descriptor: `grind/pruner`'s own tick
/// emits this when the `prune_finished` call it drives returns an `Error`,
/// since a supervised background pruner has no caller to return the
/// `PruneError` to directly. `postgres.prune_finished` itself never emits
/// this — see its own doc comment.
pub fn prune_failed() -> Event(PruneFailedMeasurements, PruneFailedMetadata) {
  sinal.event(
    ["grind", "prune", "failed"],
    count_record(PruneFailedMeasurements, fn(m) { m.count }),
    prune_failed_metadata_fields(),
  )
}
