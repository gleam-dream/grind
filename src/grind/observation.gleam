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
//// metadata does not change its wire keys (`sinal/fields.pair` flattens
//// into the same top-level fields either way), so this is unreleased-only
//// housekeeping, not a compatibility break.

import gleam/erlang/atom
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

fn proposed_from_string(raw: String) -> Result(Proposed, Nil) {
  case raw {
    "succeeded" -> Ok(ProposedSuccess)
    "business_failed" -> Ok(ProposedBusinessFailure)
    "retryable" -> Ok(ProposedRetryable)
    "runtime_failed" -> Ok(ProposedRuntimeFailed)
    "snoozed" -> Ok(ProposedSnoozed)
    "discarded" -> Ok(ProposedDiscarded)
    "cancelled" -> Ok(ProposedCancelled)
    "uncertain" -> Ok(ProposedUncertain)
    _ -> Error(Nil)
  }
}

fn confirmation_to_string(confirmation: Confirmation) -> String {
  case confirmation {
    Replied -> "replied"
    Reconciled -> "reconciled"
  }
}

fn confirmation_from_string(raw: String) -> Result(Confirmation, Nil) {
  case raw {
    "replied" -> Ok(Replied)
    "reconciled" -> Ok(Reconciled)
    _ -> Error(Nil)
  }
}

fn resolution_decision_to_string(decision: ResolutionDecision) -> String {
  case decision {
    DecisionConfirmSuccess -> "confirm_success"
    DecisionConfirmBusinessFailure -> "confirm_business_failure"
    DecisionAuthorizeReplay -> "authorize_replay"
  }
}

fn resolution_decision_from_string(
  raw: String,
) -> Result(ResolutionDecision, Nil) {
  case raw {
    "confirm_success" -> Ok(DecisionConfirmSuccess)
    "confirm_business_failure" -> Ok(DecisionConfirmBusinessFailure)
    "authorize_replay" -> Ok(DecisionAuthorizeReplay)
    _ -> Error(Nil)
  }
}

fn cancellation_outcome_to_string(outcome: CancellationOutcome) -> String {
  case outcome {
    CancellationDecidedBeforeRun -> "cancelled_before_run"
    CancellationDecidedWhileRunning -> "cancellation_requested"
  }
}

fn cancellation_outcome_from_string(
  raw: String,
) -> Result(CancellationOutcome, Nil) {
  case raw {
    "cancelled_before_run" -> Ok(CancellationDecidedBeforeRun)
    "cancellation_requested" -> Ok(CancellationDecidedWhileRunning)
    _ -> Error(Nil)
  }
}

fn codec_kind_to_string(kind: worker.CodecKind) -> String {
  case kind {
    worker.InputCodec -> "input"
    worker.OutputCodec -> "output"
    worker.ErrorCodec -> "error"
  }
}

fn codec_kind_from_string(raw: String) -> Result(worker.CodecKind, Nil) {
  case raw {
    "input" -> Ok(worker.InputCodec)
    "output" -> Ok(worker.OutputCodec)
    "error" -> Ok(worker.ErrorCodec)
    _ -> Error(Nil)
  }
}

fn job_ref_from_tuple(t: #(#(#(Int, String), String), String)) -> JobRef {
  let #(p2, worker_version) = t
  let #(p1, worker_id) = p2
  let #(job_id, queue) = p1
  JobRef(job_id:, queue:, worker_id:, worker_version:)
}

fn job_ref_to_tuple(ref: JobRef) -> #(#(#(Int, String), String), String) {
  let JobRef(job_id:, queue:, worker_id:, worker_version:) = ref
  #(#(#(job_id, queue), worker_id), worker_version)
}

fn attempt_ref_from_tuple(t: #(#(Int, Int), Int)) -> AttemptRef {
  let #(p1, attempt) = t
  let #(attempt_id, epoch) = p1
  AttemptRef(attempt_id:, epoch:, attempt:)
}

fn attempt_ref_to_tuple(ref: AttemptRef) -> #(#(Int, Int), Int) {
  let AttemptRef(attempt_id:, epoch:, attempt:) = ref
  #(#(attempt_id, epoch), attempt)
}

fn admitted_measurements_fields() -> fields.Fields(AdmittedMeasurements) {
  fields.imap(observation_wire.count_fields(), AdmittedMeasurements, fn(m) {
    m.count
  })
}

fn admitted_metadata_fields() -> fields.Fields(AdmittedMetadata) {
  let ref =
    observation_wire.job_ref_fields(job_ref_from_tuple, job_ref_to_tuple)
  let state =
    observation_wire.closed_string_field(
      "committed_state",
      job.state_to_stored,
      job.state_of_stored,
    )
  let confirmation =
    observation_wire.closed_string_field(
      "confirmation",
      confirmation_to_string,
      confirmation_from_string,
    )
  let assert Ok(available_at_field) =
    fields.optional(fields.int(atom.create("available_at_unix_ms")))
  let assert Ok(submission_id_field) =
    fields.optional(fields.string(atom.create("submission_id")))
  let assert Ok(p1) = fields.pair(ref, state)
  let assert Ok(p2) = fields.pair(p1, available_at_field)
  let assert Ok(p3) = fields.pair(p2, submission_id_field)
  let assert Ok(p4) = fields.pair(p3, confirmation)
  fields.imap(
    p4,
    fn(t) {
      let #(p3, confirmation) = t
      let #(p2, submission_id) = p3
      let #(p1, available_at_unix_ms) = p2
      let #(ref, committed_state) = p1
      AdmittedMetadata(
        ref:,
        committed_state:,
        available_at_unix_ms:,
        submission_id:,
        confirmation:,
      )
    },
    fn(m: AdmittedMetadata) {
      let AdmittedMetadata(
        ref:,
        committed_state:,
        available_at_unix_ms:,
        submission_id:,
        confirmation:,
      ) = m
      let p1 = #(ref, committed_state)
      let p2 = #(p1, available_at_unix_ms)
      let p3 = #(p2, submission_id)
      #(p3, confirmation)
    },
  )
}

fn claimed_measurements_fields() -> fields.Fields(ClaimedMeasurements) {
  fields.imap(observation_wire.count_fields(), ClaimedMeasurements, fn(m) {
    m.count
  })
}

fn claimed_metadata_fields() -> fields.Fields(ClaimedMetadata) {
  let assert Ok(p1) =
    fields.pair(
      observation_wire.job_ref_fields(job_ref_from_tuple, job_ref_to_tuple),
      observation_wire.attempt_ref_fields(
        attempt_ref_from_tuple,
        attempt_ref_to_tuple,
      ),
    )
  let assert Ok(p2) =
    fields.pair(
      p1,
      observation_wire.closed_string_field(
        "previous_state",
        job.state_to_stored,
        job.state_of_stored,
      ),
    )
  fields.imap(
    p2,
    fn(t) {
      let #(p1, previous_state) = t
      let #(ref, attempt) = p1
      ClaimedMetadata(ref:, attempt:, previous_state:)
    },
    fn(m: ClaimedMetadata) {
      let ClaimedMetadata(ref:, attempt:, previous_state:) = m
      #(#(ref, attempt), previous_state)
    },
  )
}

fn quarantined_measurements_fields() -> fields.Fields(QuarantinedMeasurements) {
  fields.imap(observation_wire.count_fields(), QuarantinedMeasurements, fn(m) {
    m.count
  })
}

fn quarantined_metadata_fields() -> fields.Fields(QuarantinedMetadata) {
  let assert Ok(p1) =
    fields.pair(
      observation_wire.job_ref_fields(job_ref_from_tuple, job_ref_to_tuple),
      observation_wire.attempt_ref_fields(
        attempt_ref_from_tuple,
        attempt_ref_to_tuple,
      ),
    )
  let assert Ok(p2) =
    fields.pair(p1, fields.bool(atom.create("cancellation_was_requested")))
  fields.imap(
    p2,
    fn(t) {
      let #(p1, cancellation_was_requested) = t
      let #(ref, attempt) = p1
      QuarantinedMetadata(ref:, attempt:, cancellation_was_requested:)
    },
    fn(m: QuarantinedMetadata) {
      let QuarantinedMetadata(ref:, attempt:, cancellation_was_requested:) = m
      #(#(ref, attempt), cancellation_was_requested)
    },
  )
}

fn resolved_measurements_fields() -> fields.Fields(ResolvedMeasurements) {
  fields.imap(observation_wire.count_fields(), ResolvedMeasurements, fn(m) {
    m.count
  })
}

fn resolved_metadata_fields() -> fields.Fields(ResolvedMetadata) {
  let ref =
    observation_wire.job_ref_fields(job_ref_from_tuple, job_ref_to_tuple)
  let decision =
    observation_wire.closed_string_field(
      "decision",
      resolution_decision_to_string,
      resolution_decision_from_string,
    )
  let state =
    observation_wire.closed_string_field(
      "committed_state",
      job.state_to_stored,
      job.state_of_stored,
    )
  let confirmation =
    observation_wire.closed_string_field(
      "confirmation",
      confirmation_to_string,
      confirmation_from_string,
    )
  let assert Ok(p1) = fields.pair(ref, decision)
  let assert Ok(p2) = fields.pair(p1, state)
  let assert Ok(p3) =
    fields.pair(p2, fields.string(atom.create("resolution_id")))
  let assert Ok(p4) = fields.pair(p3, fields.string(atom.create("resolved_by")))
  let assert Ok(p5) = fields.pair(p4, confirmation)
  fields.imap(
    p5,
    fn(t) {
      let #(p4, confirmation) = t
      let #(p3, resolved_by) = p4
      let #(p2, resolution_id) = p3
      let #(p1, committed_state) = p2
      let #(ref, decision) = p1
      ResolvedMetadata(
        ref:,
        decision:,
        committed_state:,
        resolution_id:,
        resolved_by:,
        confirmation:,
      )
    },
    fn(m: ResolvedMetadata) {
      let ResolvedMetadata(
        ref:,
        decision:,
        committed_state:,
        resolution_id:,
        resolved_by:,
        confirmation:,
      ) = m
      let p1 = #(ref, decision)
      let p2 = #(p1, committed_state)
      let p3 = #(p2, resolution_id)
      let p4 = #(p3, resolved_by)
      #(p4, confirmation)
    },
  )
}

fn cancellation_measurements_fields() -> fields.Fields(CancellationMeasurements) {
  fields.imap(observation_wire.count_fields(), CancellationMeasurements, fn(m) {
    m.count
  })
}

fn cancellation_metadata_fields() -> fields.Fields(CancellationMetadata) {
  let assert Ok(p1) =
    fields.pair(
      observation_wire.job_ref_fields(job_ref_from_tuple, job_ref_to_tuple),
      observation_wire.closed_string_field(
        "previous_state",
        job.state_to_stored,
        job.state_of_stored,
      ),
    )
  let assert Ok(p2) =
    fields.pair(
      p1,
      observation_wire.closed_string_field(
        "outcome",
        cancellation_outcome_to_string,
        cancellation_outcome_from_string,
      ),
    )
  fields.imap(
    p2,
    fn(t) {
      let #(p1, outcome) = t
      let #(ref, previous_state) = p1
      CancellationMetadata(ref:, previous_state:, outcome:)
    },
    fn(m: CancellationMetadata) {
      let CancellationMetadata(ref:, previous_state:, outcome:) = m
      #(#(ref, previous_state), outcome)
    },
  )
}

fn released_measurements_fields() -> fields.Fields(ReleasedMeasurements) {
  fields.imap(observation_wire.count_fields(), ReleasedMeasurements, fn(m) {
    m.count
  })
}

fn released_metadata_fields() -> fields.Fields(ReleasedMetadata) {
  let assert Ok(p1) =
    fields.pair(
      observation_wire.job_ref_fields(job_ref_from_tuple, job_ref_to_tuple),
      observation_wire.attempt_ref_fields(
        attempt_ref_from_tuple,
        attempt_ref_to_tuple,
      ),
    )
  let assert Ok(p2) =
    fields.pair(
      p1,
      observation_wire.closed_string_field(
        "restored_state",
        job.state_to_stored,
        job.state_of_stored,
      ),
    )
  fields.imap(
    p2,
    fn(t) {
      let #(p1, restored_state) = t
      let #(ref, attempt) = p1
      ReleasedMetadata(ref:, attempt:, restored_state:)
    },
    fn(m: ReleasedMetadata) {
      let ReleasedMetadata(ref:, attempt:, restored_state:) = m
      #(#(ref, attempt), restored_state)
    },
  )
}

fn contract_mismatch_measurements_fields() -> fields.Fields(
  ContractMismatchMeasurements,
) {
  fields.imap(
    observation_wire.count_fields(),
    ContractMismatchMeasurements,
    fn(m) { m.count },
  )
}

fn contract_mismatch_metadata_fields() -> fields.Fields(
  ContractMismatchMetadata,
) {
  let ref =
    observation_wire.job_ref_fields(job_ref_from_tuple, job_ref_to_tuple)
  let attempt =
    observation_wire.attempt_ref_fields(
      attempt_ref_from_tuple,
      attempt_ref_to_tuple,
    )
  let kind =
    observation_wire.closed_string_field(
      "kind",
      codec_kind_to_string,
      codec_kind_from_string,
    )
  let assert Ok(p1) = fields.pair(ref, attempt)
  let assert Ok(p2) = fields.pair(p1, kind)
  let assert Ok(p3) =
    fields.pair(p2, fields.string(atom.create("expected_version")))
  let assert Ok(p4) =
    fields.pair(p3, fields.string(atom.create("actual_version")))
  fields.imap(
    p4,
    fn(t) {
      let #(p3, actual_version) = t
      let #(p2, expected_version) = p3
      let #(p1, kind) = p2
      let #(ref, attempt) = p1
      ContractMismatchMetadata(
        ref:,
        attempt:,
        kind:,
        expected_version:,
        actual_version:,
      )
    },
    fn(m: ContractMismatchMetadata) {
      let ContractMismatchMetadata(
        ref:,
        attempt:,
        kind:,
        expected_version:,
        actual_version:,
      ) = m
      let p1 = #(ref, attempt)
      let p2 = #(p1, kind)
      let p3 = #(p2, expected_version)
      #(p3, actual_version)
    },
  )
}

fn prune_completed_measurements_fields() -> fields.Fields(
  PruneCompletedMeasurements,
) {
  fields.imap(
    fields.int(atom.create("jobs")),
    PruneCompletedMeasurements,
    fn(m) { m.jobs },
  )
}

fn prune_completed_metadata_fields() -> fields.Fields(PruneCompletedMetadata) {
  let assert Ok(p1) =
    fields.pair(
      fields.int(atom.create("older_than_ms")),
      fields.int(atom.create("limit")),
    )
  fields.imap(
    p1,
    fn(t) {
      let #(older_than_ms, limit) = t
      PruneCompletedMetadata(older_than_ms:, limit:)
    },
    fn(m: PruneCompletedMetadata) {
      let PruneCompletedMetadata(older_than_ms:, limit:) = m
      #(older_than_ms, limit)
    },
  )
}

fn prune_failed_measurements_fields() -> fields.Fields(PruneFailedMeasurements) {
  fields.imap(observation_wire.count_fields(), PruneFailedMeasurements, fn(m) {
    m.count
  })
}

fn prune_failure_kind_to_string(kind: PruneFailureKind) -> String {
  case kind {
    PruneReplyLost -> "reply_lost"
    PruneResultUndecodable -> "result_undecodable"
    PruneRejected -> "rejected"
    PruneNotAttempted -> "not_attempted"
  }
}

fn prune_failure_kind_from_string(
  raw: String,
) -> Result(PruneFailureKind, Nil) {
  case raw {
    "reply_lost" -> Ok(PruneReplyLost)
    "result_undecodable" -> Ok(PruneResultUndecodable)
    "rejected" -> Ok(PruneRejected)
    "not_attempted" -> Ok(PruneNotAttempted)
    _ -> Error(Nil)
  }
}

fn prune_failed_metadata_fields() -> fields.Fields(PruneFailedMetadata) {
  let assert Ok(p1) =
    fields.pair(
      fields.int(atom.create("older_than_ms")),
      fields.int(atom.create("limit")),
    )
  let assert Ok(p2) =
    fields.pair(
      p1,
      observation_wire.closed_string_field(
        "kind",
        prune_failure_kind_to_string,
        prune_failure_kind_from_string,
      ),
    )
  fields.imap(
    p2,
    fn(t) {
      let #(p1, kind) = t
      let #(older_than_ms, limit) = p1
      PruneFailedMetadata(older_than_ms:, limit:, kind:)
    },
    fn(m: PruneFailedMetadata) {
      let PruneFailedMetadata(older_than_ms:, limit:, kind:) = m
      #(#(older_than_ms, limit), kind)
    },
  )
}

fn measurements_fields() -> fields.Fields(AcknowledgedMeasurements) {
  fields.imap(fields.int(atom.create("count")), AcknowledgedMeasurements, fn(m) {
    m.count
  })
}

fn metadata_fields() -> fields.Fields(AcknowledgedMetadata) {
  let ref =
    observation_wire.job_ref_fields(job_ref_from_tuple, job_ref_to_tuple)
  let attempt =
    observation_wire.attempt_ref_fields(
      attempt_ref_from_tuple,
      attempt_ref_to_tuple,
    )
  let proposed =
    observation_wire.closed_string_field(
      "proposed",
      proposed_to_string,
      proposed_from_string,
    )
  let state =
    observation_wire.closed_string_field(
      "committed_state",
      job.state_to_stored,
      job.state_of_stored,
    )
  let failure_cause =
    observation_wire.closed_string_field(
      "failure_cause",
      worker.business_failure_cause_to_string,
      worker.business_failure_cause_from_string,
    )
  let confirmation =
    observation_wire.closed_string_field(
      "confirmation",
      confirmation_to_string,
      confirmation_from_string,
    )
  let assert Ok(failure_cause_field) = fields.optional(failure_cause)
  let assert Ok(available_at_field) =
    fields.optional(fields.int(atom.create("available_at_unix_ms")))
  let assert Ok(p1) = fields.pair(ref, attempt)
  let assert Ok(p2) = fields.pair(p1, proposed)
  let assert Ok(p3) = fields.pair(p2, state)
  let assert Ok(p4) = fields.pair(p3, failure_cause_field)
  let assert Ok(p5) = fields.pair(p4, available_at_field)
  let assert Ok(p6) = fields.pair(p5, confirmation)
  let assert Ok(p7) = fields.pair(p6, fields.string(atom.create("command_id")))
  fields.imap(
    p7,
    fn(t) {
      let #(p6, command_id) = t
      let #(p5, confirmation) = p6
      let #(p4, available_at_unix_ms) = p5
      let #(p3, failure_cause) = p4
      let #(p2, committed_state) = p3
      let #(p1, proposed) = p2
      let #(ref, attempt) = p1
      AcknowledgedMetadata(
        ref:,
        attempt:,
        proposed:,
        committed_state:,
        failure_cause:,
        available_at_unix_ms:,
        confirmation:,
        command_id:,
      )
    },
    fn(m: AcknowledgedMetadata) {
      let AcknowledgedMetadata(
        ref:,
        attempt:,
        proposed:,
        committed_state:,
        failure_cause:,
        available_at_unix_ms:,
        confirmation:,
        command_id:,
      ) = m
      let p1 = #(ref, attempt)
      let p2 = #(p1, proposed)
      let p3 = #(p2, committed_state)
      let p4 = #(p3, failure_cause)
      let p5 = #(p4, available_at_unix_ms)
      let p6 = #(p5, confirmation)
      #(p6, command_id)
    },
  )
}

/// The `[grind, job, acknowledged]` event descriptor: one committed
/// disposition for one claimed attempt. Emitted by `grind/postgres` from its
/// own `sinal/forwarder.Forwarder`, strictly after that disposition is
/// proven committed (see the module documentation above).
pub fn acknowledged() -> Event(AcknowledgedMeasurements, AcknowledgedMetadata) {
  let name = [
    atom.create("grind"),
    atom.create("job"),
    atom.create("acknowledged"),
  ]
  observation_wire.event(name, measurements_fields(), metadata_fields())
}

/// The `[grind, job, admitted]` event descriptor: one committed admission
/// decision, either a plain `submit`/`submit_at` or a `submit_unique` outcome
/// (`Inserted`, `Existing`, or `Rescheduled`). Emitted by `grind/postgres`
/// strictly after that decision is proven committed — see `AdmittedMetadata`.
pub fn admitted() -> Event(AdmittedMeasurements, AdmittedMetadata) {
  observation_wire.event(
    observation_wire.job_event_name("admitted"),
    admitted_measurements_fields(),
    admitted_metadata_fields(),
  )
}

/// The `[grind, job, claimed]` event descriptor: one job atomically claimed
/// for execution. Emitted strictly after the claim's own fenced `UPDATE ...
/// RETURNING` returns a row — see `ClaimedMetadata`.
pub fn claimed() -> Event(ClaimedMeasurements, ClaimedMetadata) {
  observation_wire.event(
    observation_wire.job_event_name("claimed"),
    claimed_measurements_fields(),
    claimed_metadata_fields(),
  )
}

/// The `[grind, job, quarantined]` event descriptor: one abandoned attempt
/// (an expired lease) moved to `uncertain` by the claim-time quarantine scan.
/// Emitted once per row the scan's `RETURNING` reports as quarantined.
pub fn quarantined() -> Event(QuarantinedMeasurements, QuarantinedMetadata) {
  observation_wire.event(
    observation_wire.job_event_name("quarantined"),
    quarantined_measurements_fields(),
    quarantined_metadata_fields(),
  )
}

/// The `[grind, job, resolved]` event descriptor: one audited operator
/// decision committed against an `uncertain` job. Emitted strictly after that
/// decision is proven committed — see `ResolvedMetadata`.
pub fn resolved() -> Event(ResolvedMeasurements, ResolvedMetadata) {
  observation_wire.event(
    observation_wire.job_event_name("resolved"),
    resolved_measurements_fields(),
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
  observation_wire.event(
    observation_wire.job_event_name("cancellation_decided"),
    cancellation_measurements_fields(),
    cancellation_metadata_fields(),
  )
}

/// The `[grind, job, released]` event descriptor: a claimed attempt refunded
/// before its worker ever ran. Emitted strictly after the release's own
/// fenced `UPDATE ... RETURNING` returns a row.
pub fn released() -> Event(ReleasedMeasurements, ReleasedMetadata) {
  observation_wire.event(
    observation_wire.job_event_name("released"),
    released_measurements_fields(),
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
  observation_wire.event(
    observation_wire.job_event_name("contract_mismatch_recorded"),
    contract_mismatch_measurements_fields(),
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
  observation_wire.event(
    [atom.create("grind"), atom.create("prune"), atom.create("completed")],
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
  observation_wire.event(
    [atom.create("grind"), atom.create("prune"), atom.create("failed")],
    prune_failed_measurements_fields(),
    prune_failed_metadata_fields(),
  )
}
