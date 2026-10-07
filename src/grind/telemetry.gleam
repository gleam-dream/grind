//// The Sinal events Grind emits. Attach your handlers to these
//// descriptors with `sinal.observe` or `sinal.attach`, as to any Sinal
//// event.
////
//// ```gleam
//// sinal.observe(telemetry.acknowledged(), fn(measurements, metadata) {
////   log(metadata.ref.correlation, job.state_name(metadata.committed_state))
//// })
//// ```
////
//// Lifecycle events, named `[grind, job, <event>]`, are emitted once a
//// commit is proven: `admitted`, `claimed`, `acknowledged`, `quarantined`,
//// `resolved`, `cancellation_decided`, `released` and
//// `contract_mismatch_recorded`. Each carries a `JobRef` with the job's
//// correlation (the one given with `job.with_correlation`, or one Grind
//// generated), and `JobMeasurements` with the node's monotonic time.
//// `[grind, prune, completed]` and `[grind, prune, failed]` report one
//// pruner batch. Diagnostic events, named `[grind, diagnostic, <event>]`,
//// report renewal, acknowledgement, retry, checkout, claim-failure and
//// capacity details; they do not imply a committed state.
////
//// Events go through the runtime's bounded forwarder (1,024 in flight by
//// default), so a slow handler never stalls a consumer. Delivery is
//// best-effort: an event can arrive twice (a `Reconciled` after a
//// `Replied` for the same command) or be dropped when the forwarder is
//// full. Grind's tables and receipts are the record; an event is a signal.
//// The unions here may gain variants and the records may gain fields; read
//// records by label.

import gleam/option.{type Option}
import grind/job
import sinal.{type Event}
import sinal/correlation.{type Correlation}
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
/// The measurements of every `[grind, job, *]` event: `count` is always 1,
/// and `monotonic_ms` is the emitting node's monotonic clock when the event
/// was emitted, so events from one node order causally even when they are
/// delivered through different forwarders.
pub type JobMeasurements {
  JobMeasurements(count: Int, monotonic_ms: Int)
}

pub type JobRef {
  JobRef(
    job_id: Int,
    queue: String,
    worker_id: String,
    worker_version: String,
    correlation: Correlation,
  )
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

/// `[grind, job, acknowledged]` metadata.
///
/// `proposed` is what the worker's execution proposed; `committed_state` is
/// what was actually durably committed, read from the acknowledgement's own
/// `RETURNING`/receipt — never from the proposal. They can differ: a
/// concurrent cancellation overrides a proposed success or another non-uncertain
/// proposal with a committed `Cancelled`. An explicit `Uncertain` proposal
/// preserves its evidence and cancellation intent. Retry-budget exhaustion is
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
    failure_cause: Option(job.TerminalCause),
    available_at_unix_ms: Option(Int),
    confirmation: Confirmation,
    command_id: String,
  )
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

/// `[grind, job, claimed]` metadata. The claim itself autocommits as a single
/// fenced `UPDATE ... RETURNING`, so a returned row is already the proof of
/// commit — there is no separate transaction reply to distinguish a
/// `Replied`/`Reconciled` pair for this event. `previous_state` is the state
/// the row held immediately before this claim (`queued`, `scheduled`, or
/// `retryable`), read from the same `RETURNING`.
pub type ClaimedMetadata {
  ClaimedMetadata(ref: JobRef, attempt: AttemptRef, previous_state: job.State)
}

/// `[grind, job, quarantined]` metadata: one abandoned attempt (an expired
/// lease found by the claim-time quarantine scan) moved to `uncertain`.
/// `cancellation_was_requested` distinguishes an abandoned attempt that also
/// had a pending cancellation request from an ordinary one — both still
/// require audited reconciliation, but the failure description differs (see
/// `postgres`'s quarantine scan). `attempt` names the attempt that expired,
/// as the row held it before the scan. A replay (`replayed: True`) refunds
/// that attempt's number, like a snooze, so the redelivery's `claimed`
/// event reports the same `attempt` under a new `attempt_id` and `epoch`.
pub type QuarantinedMetadata {
  QuarantinedMetadata(
    ref: JobRef,
    attempt: AttemptRef,
    cancellation_was_requested: Bool,
    /// The worker's `ReplayAfterLeaseExpiry` policy queued the job again
    /// instead of holding it `uncertain`.
    replayed: Bool,
  )
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

/// Cancellation of queued or executing work. AlreadyUncertain records intent
/// without changing the disposition and emits no event. AlreadyCancelled and
/// AlreadyFinished are read-only and also emit no event.
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

/// `[grind, job, released]` metadata: a claimed attempt refunded before its
/// worker ever ran (the temporary worker child failed to start). `restored_state`
/// is the state the row is returned to — the same state it held before this
/// claim.
pub type ReleasedMetadata {
  ReleasedMetadata(ref: JobRef, attempt: AttemptRef, restored_state: job.State)
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
    kind: job.CodecKind,
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

fn codec_kind_field() -> fields.Fields(job.CodecKind) {
  fields.enum(
    "kind",
    [job.InputCodec, job.OutputCodec, job.ErrorCodec],
    fn(kind) {
      case kind {
        job.InputCodec -> "input"
        job.OutputCodec -> "output"
        job.ErrorCodec -> "error"
      }
    },
  )
}

fn failure_cause_field() -> fields.Fields(job.TerminalCause) {
  fields.enum(
    "failure_cause",
    [job.BudgetExhausted, job.RetryDeclined, job.SnoozeLimitReached],
    job.terminal_cause_name,
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
  use job_id <- fields.include(fields.int("job_id"), get: fn(ref: JobRef) {
    ref.job_id
  })
  use queue <- fields.include(fields.string("queue"), get: fn(ref) { ref.queue })
  use worker_id <- fields.include(fields.string("worker_id"), get: fn(ref) {
    ref.worker_id
  })
  use worker_version <- fields.include(
    fields.string("worker_version"),
    get: fn(ref) { ref.worker_version },
  )
  use correlation <- fields.include(correlation.required_field(), get: fn(ref) {
    ref.correlation
  })
  fields.success(JobRef(
    job_id:,
    queue:,
    worker_id:,
    worker_version:,
    correlation:,
  ))
}

fn attempt_ref_fields() -> fields.Fields(AttemptRef) {
  use attempt_id <- fields.include(
    fields.int("attempt_id"),
    get: fn(ref: AttemptRef) { ref.attempt_id },
  )
  use epoch <- fields.include(fields.int("epoch"), get: fn(ref) { ref.epoch })
  use attempt <- fields.include(fields.int("attempt"), get: fn(ref) {
    ref.attempt
  })
  fields.success(AttemptRef(attempt_id:, epoch:, attempt:))
}

/// A one-key `count` measurement record, shared by every event whose only
/// measurement is `count`.
fn count_record(make: fn(Int) -> m, count: fn(m) -> Int) -> fields.Fields(m) {
  use count <- fields.include(fields.int("count"), get: count)
  fields.success(make(count))
}

fn job_measurement_fields() -> fields.Fields(JobMeasurements) {
  use count <- fields.include(fields.int("count"), get: fn(m: JobMeasurements) {
    m.count
  })
  use monotonic_ms <- fields.include(fields.int("monotonic_ms"), get: fn(m) {
    m.monotonic_ms
  })
  fields.success(JobMeasurements(count:, monotonic_ms:))
}

/// Every `job.State`, in declaration order.
const job_states = [
  job.Queued,
  job.Scheduled,
  job.Retryable,
  job.Executing,
  job.Succeeded,
  job.BusinessFailed,
  job.RuntimeFailed,
  job.ContractMismatch,
  job.Uncertain,
  job.Discarded,
  job.Cancelled,
]

/// A `job.State` written as its stored name.
fn state_field(key: String) -> fields.Fields(job.State) {
  fields.enum(key, job_states, job.state_name)
}

fn job_event_name(part: String) -> List(String) {
  ["grind", "job", part]
}

fn admitted_metadata_fields() -> fields.Fields(AdmittedMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use committed_state <- fields.include(
    state_field("committed_state"),
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
    state_field("previous_state"),
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
  use replayed <- fields.include(fields.bool("replayed"), get: fn(m) {
    m.replayed
  })
  fields.success(QuarantinedMetadata(
    ref:,
    attempt:,
    cancellation_was_requested:,
    replayed:,
  ))
}

fn resolved_metadata_fields() -> fields.Fields(ResolvedMetadata) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use decision <- fields.include(resolution_decision_field(), get: fn(m) {
    m.decision
  })
  use committed_state <- fields.include(
    state_field("committed_state"),
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
    state_field("previous_state"),
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
    state_field("restored_state"),
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
    state_field("committed_state"),
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
pub fn acknowledged() -> Event(JobMeasurements, AcknowledgedMetadata) {
  sinal.event(
    job_event_name("acknowledged"),
    job_measurement_fields(),
    metadata_fields(),
  )
}

/// The `[grind, job, admitted]` event descriptor: one committed admission
/// decision, either a plain `submit`/`submit_at` or a `submit_unique` outcome
/// (`Inserted`, `Existing`, or `Rescheduled`). Emitted by `grind/postgres`
/// strictly after that decision is proven committed — see `AdmittedMetadata`.
pub fn admitted() -> Event(JobMeasurements, AdmittedMetadata) {
  sinal.event(
    job_event_name("admitted"),
    job_measurement_fields(),
    admitted_metadata_fields(),
  )
}

/// The `[grind, job, claimed]` event descriptor: one job atomically claimed
/// for execution. Emitted strictly after the claim's own fenced `UPDATE ...
/// RETURNING` returns a row — see `ClaimedMetadata`.
pub fn claimed() -> Event(JobMeasurements, ClaimedMetadata) {
  sinal.event(
    job_event_name("claimed"),
    job_measurement_fields(),
    claimed_metadata_fields(),
  )
}

/// The `[grind, job, quarantined]` event descriptor: one abandoned attempt
/// (an expired lease) moved to `uncertain` by the claim-time quarantine scan.
/// Emitted once per row the scan's `RETURNING` reports as quarantined.
pub fn quarantined() -> Event(JobMeasurements, QuarantinedMetadata) {
  sinal.event(
    job_event_name("quarantined"),
    job_measurement_fields(),
    quarantined_metadata_fields(),
  )
}

/// The `[grind, job, resolved]` event descriptor: one audited operator
/// decision committed against an `uncertain` job. Emitted strictly after that
/// decision is proven committed — see `ResolvedMetadata`.
pub fn resolved() -> Event(JobMeasurements, ResolvedMetadata) {
  sinal.event(
    job_event_name("resolved"),
    job_measurement_fields(),
    resolved_metadata_fields(),
  )
}

/// The `[grind, job, cancellation_decided]` event descriptor: a cancellation request
/// that changed something durable (`CancellationDecidedBeforeRun` or
/// `CancellationDecidedWhileRunning`). AlreadyUncertain records intent without
/// a disposition change and emits no event. AlreadyCancelled and AlreadyFinished
/// are read-only and also emit no event —
/// see `CancellationMetadata`.
pub fn cancellation_decided() -> Event(JobMeasurements, CancellationMetadata) {
  sinal.event(
    job_event_name("cancellation_decided"),
    job_measurement_fields(),
    cancellation_metadata_fields(),
  )
}

/// The `[grind, job, released]` event descriptor: a claimed attempt refunded
/// before its worker ever ran. Emitted strictly after the release's own
/// fenced `UPDATE ... RETURNING` returns a row.
pub fn released() -> Event(JobMeasurements, ReleasedMetadata) {
  sinal.event(
    job_event_name("released"),
    job_measurement_fields(),
    released_metadata_fields(),
  )
}

/// The `[grind, job, contract_mismatch_recorded]` event descriptor: a claimed attempt
/// parked in the terminal, nonclaimable `contract_mismatch` state because a
/// registered worker's codec contract no longer matches what was persisted
/// at admission. Emitted strictly after the mismatch's own fenced
/// `UPDATE ... RETURNING` returns a row.
pub fn contract_mismatch_recorded() -> Event(
  JobMeasurements,
  ContractMismatchMetadata,
) {
  sinal.event(
    job_event_name("contract_mismatch_recorded"),
    job_measurement_fields(),
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

/// Native strings: `handler_running`, `acknowledgement_pending`.
pub type AttemptPhase {
  HandlerRunning
  AcknowledgementPending
}

/// Native strings: `renewed`, `skipped_locked`, `live_fence_unavailable`, `storage_failed`, `completion_budget_exhausted`.
pub type RenewalOutcome {
  Renewed
  SkippedLocked
  LiveFenceUnavailable
  StorageFailed
  CompletionBudgetExhausted
}

/// Native strings: `replied`, `reconciled`, `rolled_back`, `unknown`, `fence_rejected`, `command_conflict`, `failed`.
pub type AckOutcome {
  AckReplied
  AckReconciled
  AckRolledBack
  AckUnknown
  AckFenceRejected
  AckCommandConflict
  AckFailed
}

/// Native strings: `after_failure`, `after_unknown`.
pub type RetryReason {
  RetryAfterFailure
  RetryAfterUnknown
}

/// Native strings: `quarantine_scan`, `claim_candidate`, `lease_renewal`, `acknowledge`, `reconcile_acknowledgement`.
pub type Operation {
  QuarantineScan
  ClaimCandidate
  LeaseRenewal
  Acknowledge
  ReconcileAcknowledgement
}

/// Native strings: `main`, `reserved`.
pub type PoolRole {
  MainPool
  ReservedPool
}

/// Native strings: `acquired`, `unavailable`.
pub type CheckoutOutcome {
  CheckoutAcquired
  CheckoutUnavailable
}

/// Native strings: `succeeded`, `failed`.
pub type CallOutcome {
  CallSucceeded
  CallFailed
}

/// Native strings: `timed_out`, `connection_unavailable`, `rejected`, `unexpected_result`.
pub type FailureKind {
  TimedOut
  ConnectionUnavailable
  Rejected
  UnexpectedResult
}

/// Node and consumer-incarnation identity; both are native strings, never dynamic atoms.
pub type ConsumerRef {
  ConsumerRef(node: String, consumer: String)
}

/// One local consumer of a queue, not a global queue-depth identity.
pub type QueueRef {
  QueueRef(queue: String, consumer: ConsumerRef)
}

/// The job and attempt fence observed by one consumer incarnation.
pub type AttemptContext {
  AttemptContext(ref: JobRef, attempt: AttemptRef, consumer: ConsumerRef)
}

/// One renewal outcome. Duration is the shared batch storage-call duration in
/// local microseconds, repeated per attempt; summing it overcounts database time.
/// Optional signed lease headroom is measured during database evaluation, not delivery.
pub type RenewalMeasurements {
  RenewalMeasurements(
    count: Int,
    duration_us: Int,
    remaining_lease_ms: Option(Int),
  )
}

/// A skipped lock does not identify its owner. A missing live fence may mean
/// completion, cancellation, expiry or changed ownership. Budget exhaustion is
/// local and proves neither expiry nor quarantine.
pub type RenewalMetadata {
  RenewalMetadata(
    context: AttemptContext,
    phase: AttemptPhase,
    outcome: RenewalOutcome,
  )
}

/// One returned ACK transaction/reconciliation path; duration includes reconciliation.
/// Proposal validation before storage and contract-mismatch parking are excluded.
pub type AcknowledgementMeasurements {
  AcknowledgementMeasurements(count: Int, duration_us: Int)
}

/// Rolled back means a transaction result proved rollback. Unknown is not proof
/// of either commit or rollback. Reconciled means a durable matching receipt.
pub type AcknowledgementMetadata {
  AcknowledgementMetadata(
    context: AttemptContext,
    command_id: String,
    outcome: AckOutcome,
  )
}

/// One scheduled ACK retry. The retry number is local to acknowledgement, not
/// the job attempt number. Pending duration is local monotonic microseconds.
pub type RetryMeasurements {
  RetryMeasurements(
    count: Int,
    retry_number: Int,
    delay_ms: Int,
    pending_duration_us: Int,
  )
}

/// Scheduling repeats acknowledgement/reconciliation, never the handler.
pub type RetryMetadata {
  RetryMetadata(
    context: AttemptContext,
    command_id: String,
    reason: RetryReason,
  )
}

/// Actual checkout wait and full wrapper duration in microseconds. Native wait
/// key is `checkout_wait_us`. Candidate count includes stale checkout candidates;
/// probes, SQL and cleanup are outside wait but inside total duration.
pub type CheckoutMeasurements {
  CheckoutMeasurements(
    count: Int,
    wait_us: Int,
    call_duration_us: Int,
    candidates: Int,
  )
}

/// A completed selected queue-storage call, after cleanup. Nested calls on an
/// already checked-out connection do not emit another checkout sample.
pub type CheckoutMetadata {
  CheckoutMetadata(
    queue: QueueRef,
    operation: Operation,
    pool: PoolRole,
    checkout: CheckoutOutcome,
    returned: CallOutcome,
  )
}

/// One failed quarantine scan or candidate claim, timed in local microseconds.
pub type ClaimFailedMeasurements {
  ClaimFailedMeasurements(count: Int, duration_us: Int)
}

/// Sanitized failure classification before a job was obtained; no job identity
/// or underlying exception message is invented.
pub type ClaimFailedMetadata {
  ClaimFailedMetadata(queue: QueueRef, stage: Operation, failure: FailureKind)
}

/// One local ledger snapshot. Active = running + ack_pending; available =
/// maximum - active. Pending acknowledgements continue to occupy capacity.
pub type CapacityMeasurements {
  CapacityMeasurements(
    maximum: Int,
    active: Int,
    running: Int,
    ack_pending: Int,
    available: Int,
  )
}

/// Observed consumer draining state; this is not a global queue-depth sample.
pub type CapacityMetadata {
  CapacityMetadata(queue: QueueRef, draining: Bool)
}

/// `[grind, diagnostic, renewal]`.
pub fn renewal() -> Event(RenewalMeasurements, RenewalMetadata) {
  sinal.event(
    ["grind", "diagnostic", "renewal"],
    renewal_measurements_fields(),
    renewal_metadata_fields(),
  )
}

/// `[grind, diagnostic, acknowledgement]`.
pub fn acknowledgement() -> Event(
  AcknowledgementMeasurements,
  AcknowledgementMetadata,
) {
  sinal.event(
    ["grind", "diagnostic", "acknowledgement"],
    acknowledgement_measurements_fields(),
    acknowledgement_metadata_fields(),
  )
}

/// `[grind, diagnostic, acknowledgement_retry]`.
pub fn acknowledgement_retry() -> Event(RetryMeasurements, RetryMetadata) {
  sinal.event(
    ["grind", "diagnostic", "acknowledgement_retry"],
    retry_measurements_fields(),
    retry_metadata_fields(),
  )
}

/// `[grind, diagnostic, checkout]`.
pub fn checkout() -> Event(CheckoutMeasurements, CheckoutMetadata) {
  sinal.event(
    ["grind", "diagnostic", "checkout"],
    checkout_measurements_fields(),
    checkout_metadata_fields(),
  )
}

/// `[grind, diagnostic, claim_failed]`.
pub fn claim_failed() -> Event(ClaimFailedMeasurements, ClaimFailedMetadata) {
  sinal.event(
    ["grind", "diagnostic", "claim_failed"],
    claim_failed_measurements_fields(),
    claim_failed_metadata_fields(),
  )
}

/// `[grind, diagnostic, capacity]`.
pub fn capacity() -> Event(CapacityMeasurements, CapacityMetadata) {
  sinal.event(
    ["grind", "diagnostic", "capacity"],
    capacity_measurements_fields(),
    capacity_metadata_fields(),
  )
}

fn consumer_ref_fields() -> fields.Fields(ConsumerRef) {
  use node <- fields.include(fields.string("node"), get: fn(m) { m.node })
  use consumer <- fields.include(fields.string("consumer"), get: fn(m) {
    m.consumer
  })
  fields.success(ConsumerRef(node:, consumer:))
}

fn queue_ref_fields() -> fields.Fields(QueueRef) {
  use queue <- fields.include(fields.string("queue"), get: fn(m) { m.queue })
  use consumer <- fields.include(consumer_ref_fields(), get: fn(m) {
    m.consumer
  })
  fields.success(QueueRef(queue:, consumer:))
}

fn attempt_context_fields() -> fields.Fields(AttemptContext) {
  use ref <- fields.include(job_ref_fields(), get: fn(m) { m.ref })
  use attempt <- fields.include(attempt_ref_fields(), get: fn(m) { m.attempt })
  use consumer <- fields.include(consumer_ref_fields(), get: fn(m) {
    m.consumer
  })
  fields.success(AttemptContext(ref:, attempt:, consumer:))
}

fn renewal_measurements_fields() -> fields.Fields(RenewalMeasurements) {
  use count <- fields.include(fields.int("count"), get: fn(m) { m.count })
  use duration_us <- fields.include(fields.int("duration_us"), get: fn(m) {
    m.duration_us
  })
  use remaining_lease_ms <- fields.include(
    fields.optional(fields.int("remaining_lease_ms")),
    get: fn(m) { m.remaining_lease_ms },
  )
  fields.success(RenewalMeasurements(count:, duration_us:, remaining_lease_ms:))
}

fn renewal_metadata_fields() -> fields.Fields(RenewalMetadata) {
  use context <- fields.include(attempt_context_fields(), get: fn(m) {
    m.context
  })
  use phase <- fields.include(
    fields.enum(
      "phase",
      [HandlerRunning, AcknowledgementPending],
      attempt_phase_to_string,
    ),
    get: fn(m) { m.phase },
  )
  use outcome <- fields.include(
    fields.enum(
      "outcome",
      [
        Renewed,
        SkippedLocked,
        LiveFenceUnavailable,
        StorageFailed,
        CompletionBudgetExhausted,
      ],
      renewal_outcome_to_string,
    ),
    get: fn(m) { m.outcome },
  )
  fields.success(RenewalMetadata(context:, phase:, outcome:))
}

fn acknowledgement_measurements_fields() -> fields.Fields(
  AcknowledgementMeasurements,
) {
  use count <- fields.include(fields.int("count"), get: fn(m) { m.count })
  use duration_us <- fields.include(fields.int("duration_us"), get: fn(m) {
    m.duration_us
  })
  fields.success(AcknowledgementMeasurements(count:, duration_us:))
}

fn acknowledgement_metadata_fields() -> fields.Fields(AcknowledgementMetadata) {
  use context <- fields.include(attempt_context_fields(), get: fn(m) {
    m.context
  })
  use command_id <- fields.include(fields.string("command_id"), get: fn(m) {
    m.command_id
  })
  use outcome <- fields.include(
    fields.enum(
      "outcome",
      [
        AckReplied,
        AckReconciled,
        AckRolledBack,
        AckUnknown,
        AckFenceRejected,
        AckCommandConflict,
        AckFailed,
      ],
      ack_outcome_to_string,
    ),
    get: fn(m) { m.outcome },
  )
  fields.success(AcknowledgementMetadata(context:, command_id:, outcome:))
}

fn retry_measurements_fields() -> fields.Fields(RetryMeasurements) {
  use count <- fields.include(fields.int("count"), get: fn(m) { m.count })
  use retry_number <- fields.include(fields.int("retry_number"), get: fn(m) {
    m.retry_number
  })
  use delay_ms <- fields.include(fields.int("delay_ms"), get: fn(m) {
    m.delay_ms
  })
  use pending_duration_us <- fields.include(
    fields.int("pending_duration_us"),
    get: fn(m) { m.pending_duration_us },
  )
  fields.success(RetryMeasurements(
    count:,
    retry_number:,
    delay_ms:,
    pending_duration_us:,
  ))
}

fn retry_metadata_fields() -> fields.Fields(RetryMetadata) {
  use context <- fields.include(attempt_context_fields(), get: fn(m) {
    m.context
  })
  use command_id <- fields.include(fields.string("command_id"), get: fn(m) {
    m.command_id
  })
  use reason <- fields.include(
    fields.enum(
      "reason",
      [RetryAfterFailure, RetryAfterUnknown],
      retry_reason_to_string,
    ),
    get: fn(m) { m.reason },
  )
  fields.success(RetryMetadata(context:, command_id:, reason:))
}

fn checkout_measurements_fields() -> fields.Fields(CheckoutMeasurements) {
  use count <- fields.include(fields.int("count"), get: fn(m) { m.count })
  use wait_us <- fields.include(fields.int("checkout_wait_us"), get: fn(m) {
    m.wait_us
  })
  use call_duration_us <- fields.include(
    fields.int("call_duration_us"),
    get: fn(m) { m.call_duration_us },
  )
  use candidates <- fields.include(fields.int("candidates"), get: fn(m) {
    m.candidates
  })
  fields.success(CheckoutMeasurements(
    count:,
    wait_us:,
    call_duration_us:,
    candidates:,
  ))
}

fn checkout_metadata_fields() -> fields.Fields(CheckoutMetadata) {
  use queue <- fields.include(queue_ref_fields(), get: fn(m) { m.queue })
  use operation <- fields.include(operation_field("operation"), get: fn(m) {
    m.operation
  })
  use pool <- fields.include(
    fields.enum("pool", [MainPool, ReservedPool], pool_role_to_string),
    get: fn(m) { m.pool },
  )
  use checkout <- fields.include(
    fields.enum(
      "checkout",
      [CheckoutAcquired, CheckoutUnavailable],
      checkout_outcome_to_string,
    ),
    get: fn(m) { m.checkout },
  )
  use returned <- fields.include(
    fields.enum("returned", [CallSucceeded, CallFailed], call_outcome_to_string),
    get: fn(m) { m.returned },
  )
  fields.success(CheckoutMetadata(
    queue:,
    operation:,
    pool:,
    checkout:,
    returned:,
  ))
}

fn claim_failed_measurements_fields() -> fields.Fields(ClaimFailedMeasurements) {
  use count <- fields.include(fields.int("count"), get: fn(m) { m.count })
  use duration_us <- fields.include(fields.int("duration_us"), get: fn(m) {
    m.duration_us
  })
  fields.success(ClaimFailedMeasurements(count:, duration_us:))
}

fn claim_failed_metadata_fields() -> fields.Fields(ClaimFailedMetadata) {
  use queue <- fields.include(queue_ref_fields(), get: fn(m) { m.queue })
  use stage <- fields.include(operation_field("stage"), get: fn(m) { m.stage })
  use failure <- fields.include(
    fields.enum(
      "failure",
      [TimedOut, ConnectionUnavailable, Rejected, UnexpectedResult],
      failure_kind_to_string,
    ),
    get: fn(m) { m.failure },
  )
  fields.success(ClaimFailedMetadata(queue:, stage:, failure:))
}

fn capacity_measurements_fields() -> fields.Fields(CapacityMeasurements) {
  use maximum <- fields.include(fields.int("maximum"), get: fn(m) { m.maximum })
  use active <- fields.include(fields.int("active"), get: fn(m) { m.active })
  use running <- fields.include(fields.int("running"), get: fn(m) { m.running })
  use ack_pending <- fields.include(fields.int("ack_pending"), get: fn(m) {
    m.ack_pending
  })
  use available <- fields.include(fields.int("available"), get: fn(m) {
    m.available
  })
  fields.success(CapacityMeasurements(
    maximum:,
    active:,
    running:,
    ack_pending:,
    available:,
  ))
}

fn capacity_metadata_fields() -> fields.Fields(CapacityMetadata) {
  use queue <- fields.include(queue_ref_fields(), get: fn(m) { m.queue })
  use draining <- fields.include(fields.bool("draining"), get: fn(m) {
    m.draining
  })
  fields.success(CapacityMetadata(queue:, draining:))
}

fn operation_field(key: String) -> fields.Fields(Operation) {
  fields.enum(
    key,
    [
      QuarantineScan,
      ClaimCandidate,
      LeaseRenewal,
      Acknowledge,
      ReconcileAcknowledgement,
    ],
    operation_to_string,
  )
}

fn attempt_phase_to_string(value: AttemptPhase) -> String {
  case value {
    HandlerRunning -> "handler_running"
    AcknowledgementPending -> "acknowledgement_pending"
  }
}

fn renewal_outcome_to_string(value: RenewalOutcome) -> String {
  case value {
    Renewed -> "renewed"
    SkippedLocked -> "skipped_locked"
    LiveFenceUnavailable -> "live_fence_unavailable"
    StorageFailed -> "storage_failed"
    CompletionBudgetExhausted -> "completion_budget_exhausted"
  }
}

fn ack_outcome_to_string(value: AckOutcome) -> String {
  case value {
    AckReplied -> "replied"
    AckReconciled -> "reconciled"
    AckRolledBack -> "rolled_back"
    AckUnknown -> "unknown"
    AckFenceRejected -> "fence_rejected"
    AckCommandConflict -> "command_conflict"
    AckFailed -> "failed"
  }
}

fn retry_reason_to_string(value: RetryReason) -> String {
  case value {
    RetryAfterFailure -> "after_failure"
    RetryAfterUnknown -> "after_unknown"
  }
}

fn operation_to_string(value: Operation) -> String {
  case value {
    QuarantineScan -> "quarantine_scan"
    ClaimCandidate -> "claim_candidate"
    LeaseRenewal -> "lease_renewal"
    Acknowledge -> "acknowledge"
    ReconcileAcknowledgement -> "reconcile_acknowledgement"
  }
}

fn pool_role_to_string(value: PoolRole) -> String {
  case value {
    MainPool -> "main"
    ReservedPool -> "reserved"
  }
}

fn checkout_outcome_to_string(value: CheckoutOutcome) -> String {
  case value {
    CheckoutAcquired -> "acquired"
    CheckoutUnavailable -> "unavailable"
  }
}

fn call_outcome_to_string(value: CallOutcome) -> String {
  case value {
    CallSucceeded -> "succeeded"
    CallFailed -> "failed"
  }
}

fn failure_kind_to_string(value: FailureKind) -> String {
  case value {
    TimedOut -> "timed_out"
    ConnectionUnavailable -> "connection_unavailable"
    Rejected -> "rejected"
    UnexpectedResult -> "unexpected_result"
  }
}
