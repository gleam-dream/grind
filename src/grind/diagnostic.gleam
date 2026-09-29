//// Payload-free operational Sinal descriptors, separate from durable lifecycle events.
////
//// Native names are `[grind, diagnostic, <descriptor>]`. All field names are
//// fixed atom literals; node, consumer, queue and worker identities are strings.
//// `AttemptContext`, `QueueRef` and `ConsumerRef` flatten into their named fields;
//// no nested context map, job input/output/error, SQL or connection setting is sent.
////
//// Runtime producers use the same Database-owned bounded `sinal/forwarder` as
//// `grind/observation`, including reserved renewal. Handlers never run in a queue,
//// renewer, attempt or checkout. Delivery is best-effort, FIFO only per producer;
//// overflow/unavailability drops observations and cannot change work. Missing
//// events never prove missing activity. Unlike durable lifecycle observations,
//// failure, retry, pressure and timing diagnostics do not imply a committed state.
////
//// Measurements use native integers: durations/waits in microseconds, lease
//// headroom and retry delays in milliseconds. Absent `remaining_lease_ms` omits
//// its native key; present values are signed database-time samples. All other
//// fields use their record's snake_case name except `wait_us`, whose native key
//// is `checkout_wait_us`. Closed enum wire strings are listed on each type.

import gleam/erlang/atom
import gleam/option.{type Option}
import grind/internal/observation/wire
import grind/observation
import sinal.{type Event}
import sinal/fields

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
  AttemptContext(
    ref: observation.JobRef,
    attempt: observation.AttemptRef,
    consumer: ConsumerRef,
  )
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
  wire.event(
    [atom.create("grind"), atom.create("diagnostic"), atom.create("renewal")],
    renewal_measurements_fields(),
    renewal_metadata_fields(),
  )
}

/// `[grind, diagnostic, acknowledgement]`.
pub fn acknowledgement() -> Event(
  AcknowledgementMeasurements,
  AcknowledgementMetadata,
) {
  wire.event(
    [
      atom.create("grind"),
      atom.create("diagnostic"),
      atom.create("acknowledgement"),
    ],
    acknowledgement_measurements_fields(),
    acknowledgement_metadata_fields(),
  )
}

/// `[grind, diagnostic, acknowledgement_retry]`.
pub fn acknowledgement_retry() -> Event(RetryMeasurements, RetryMetadata) {
  wire.event(
    [
      atom.create("grind"),
      atom.create("diagnostic"),
      atom.create("acknowledgement_retry"),
    ],
    retry_measurements_fields(),
    retry_metadata_fields(),
  )
}

/// `[grind, diagnostic, checkout]`.
pub fn checkout() -> Event(CheckoutMeasurements, CheckoutMetadata) {
  wire.event(
    [atom.create("grind"), atom.create("diagnostic"), atom.create("checkout")],
    checkout_measurements_fields(),
    checkout_metadata_fields(),
  )
}

/// `[grind, diagnostic, claim_failed]`.
pub fn claim_failed() -> Event(ClaimFailedMeasurements, ClaimFailedMetadata) {
  wire.event(
    [
      atom.create("grind"),
      atom.create("diagnostic"),
      atom.create("claim_failed"),
    ],
    claim_failed_measurements_fields(),
    claim_failed_metadata_fields(),
  )
}

/// `[grind, diagnostic, capacity]`.
pub fn capacity() -> Event(CapacityMeasurements, CapacityMetadata) {
  wire.event(
    [atom.create("grind"), atom.create("diagnostic"), atom.create("capacity")],
    capacity_measurements_fields(),
    capacity_metadata_fields(),
  )
}

fn pair(a: fields.Fields(a), b: fields.Fields(b)) -> fields.Fields(#(a, b)) {
  let assert Ok(paired) = fields.pair(a, b)
  paired
}

fn triple(
  a: fields.Fields(a),
  b: fields.Fields(b),
  c: fields.Fields(c),
) -> fields.Fields(#(a, b, c)) {
  fields.imap(pair(pair(a, b), c), fn(t) { #(t.0.0, t.0.1, t.1) }, fn(t) {
    #(#(t.0, t.1), t.2)
  })
}

fn quadruple(
  a: fields.Fields(a),
  b: fields.Fields(b),
  c: fields.Fields(c),
  d: fields.Fields(d),
) -> fields.Fields(#(a, b, c, d)) {
  fields.imap(
    pair(triple(a, b, c), d),
    fn(t) { #(t.0.0, t.0.1, t.0.2, t.1) },
    fn(t) { #(#(t.0, t.1, t.2), t.3) },
  )
}

fn quintuple(
  a: fields.Fields(a),
  b: fields.Fields(b),
  c: fields.Fields(c),
  d: fields.Fields(d),
  e: fields.Fields(e),
) -> fields.Fields(#(a, b, c, d, e)) {
  fields.imap(
    pair(quadruple(a, b, c, d), e),
    fn(t) { #(t.0.0, t.0.1, t.0.2, t.0.3, t.1) },
    fn(t) { #(#(t.0, t.1, t.2, t.3), t.4) },
  )
}

fn job_ref_fields() -> fields.Fields(observation.JobRef) {
  wire.job_ref_fields(
    fn(t) { observation.JobRef(t.0.0.0, t.0.0.1, t.0.1, t.1) },
    fn(r) { #(#(#(r.job_id, r.queue), r.worker_id), r.worker_version) },
  )
}

fn attempt_ref_fields() -> fields.Fields(observation.AttemptRef) {
  wire.attempt_ref_fields(
    fn(t) { observation.AttemptRef(t.0.0, t.0.1, t.1) },
    fn(r) { #(#(r.attempt_id, r.epoch), r.attempt) },
  )
}

fn consumer_ref_fields() -> fields.Fields(ConsumerRef) {
  fields.imap(
    pair(
      fields.string(atom.create("node")),
      fields.string(atom.create("consumer")),
    ),
    fn(t) { ConsumerRef(node: t.0, consumer: t.1) },
    fn(m) { #(m.node, m.consumer) },
  )
}

fn queue_ref_fields() -> fields.Fields(QueueRef) {
  fields.imap(
    pair(fields.string(atom.create("queue")), consumer_ref_fields()),
    fn(t) { QueueRef(queue: t.0, consumer: t.1) },
    fn(m) { #(m.queue, m.consumer) },
  )
}

fn attempt_context_fields() -> fields.Fields(AttemptContext) {
  fields.imap(
    triple(job_ref_fields(), attempt_ref_fields(), consumer_ref_fields()),
    fn(t) { AttemptContext(ref: t.0, attempt: t.1, consumer: t.2) },
    fn(m) { #(m.ref, m.attempt, m.consumer) },
  )
}

fn renewal_measurements_fields() -> fields.Fields(RenewalMeasurements) {
  let assert Ok(optional_remaining_lease_ms) =
    fields.optional(fields.int(atom.create("remaining_lease_ms")))
  fields.imap(
    triple(
      fields.int(atom.create("count")),
      fields.int(atom.create("duration_us")),
      optional_remaining_lease_ms,
    ),
    fn(t) {
      RenewalMeasurements(count: t.0, duration_us: t.1, remaining_lease_ms: t.2)
    },
    fn(m) { #(m.count, m.duration_us, m.remaining_lease_ms) },
  )
}

fn renewal_metadata_fields() -> fields.Fields(RenewalMetadata) {
  fields.imap(
    triple(
      attempt_context_fields(),
      wire.closed_string_field(
        "phase",
        attempt_phase_to_string,
        attempt_phase_from_string,
      ),
      wire.closed_string_field(
        "outcome",
        renewal_outcome_to_string,
        renewal_outcome_from_string,
      ),
    ),
    fn(t) { RenewalMetadata(context: t.0, phase: t.1, outcome: t.2) },
    fn(m) { #(m.context, m.phase, m.outcome) },
  )
}

fn acknowledgement_measurements_fields() -> fields.Fields(
  AcknowledgementMeasurements,
) {
  fields.imap(
    pair(
      fields.int(atom.create("count")),
      fields.int(atom.create("duration_us")),
    ),
    fn(t) { AcknowledgementMeasurements(count: t.0, duration_us: t.1) },
    fn(m) { #(m.count, m.duration_us) },
  )
}

fn acknowledgement_metadata_fields() -> fields.Fields(AcknowledgementMetadata) {
  fields.imap(
    triple(
      attempt_context_fields(),
      fields.string(atom.create("command_id")),
      wire.closed_string_field(
        "outcome",
        ack_outcome_to_string,
        ack_outcome_from_string,
      ),
    ),
    fn(t) {
      AcknowledgementMetadata(context: t.0, command_id: t.1, outcome: t.2)
    },
    fn(m) { #(m.context, m.command_id, m.outcome) },
  )
}

fn retry_measurements_fields() -> fields.Fields(RetryMeasurements) {
  fields.imap(
    quadruple(
      fields.int(atom.create("count")),
      fields.int(atom.create("retry_number")),
      fields.int(atom.create("delay_ms")),
      fields.int(atom.create("pending_duration_us")),
    ),
    fn(t) {
      RetryMeasurements(
        count: t.0,
        retry_number: t.1,
        delay_ms: t.2,
        pending_duration_us: t.3,
      )
    },
    fn(m) { #(m.count, m.retry_number, m.delay_ms, m.pending_duration_us) },
  )
}

fn retry_metadata_fields() -> fields.Fields(RetryMetadata) {
  fields.imap(
    triple(
      attempt_context_fields(),
      fields.string(atom.create("command_id")),
      wire.closed_string_field(
        "reason",
        retry_reason_to_string,
        retry_reason_from_string,
      ),
    ),
    fn(t) { RetryMetadata(context: t.0, command_id: t.1, reason: t.2) },
    fn(m) { #(m.context, m.command_id, m.reason) },
  )
}

fn checkout_measurements_fields() -> fields.Fields(CheckoutMeasurements) {
  fields.imap(
    quadruple(
      fields.int(atom.create("count")),
      fields.int(atom.create("checkout_wait_us")),
      fields.int(atom.create("call_duration_us")),
      fields.int(atom.create("candidates")),
    ),
    fn(t) {
      CheckoutMeasurements(
        count: t.0,
        wait_us: t.1,
        call_duration_us: t.2,
        candidates: t.3,
      )
    },
    fn(m) { #(m.count, m.wait_us, m.call_duration_us, m.candidates) },
  )
}

fn checkout_metadata_fields() -> fields.Fields(CheckoutMetadata) {
  fields.imap(
    quintuple(
      queue_ref_fields(),
      wire.closed_string_field(
        "operation",
        operation_to_string,
        operation_from_string,
      ),
      wire.closed_string_field(
        "pool",
        pool_role_to_string,
        pool_role_from_string,
      ),
      wire.closed_string_field(
        "checkout",
        checkout_outcome_to_string,
        checkout_outcome_from_string,
      ),
      wire.closed_string_field(
        "returned",
        call_outcome_to_string,
        call_outcome_from_string,
      ),
    ),
    fn(t) {
      CheckoutMetadata(
        queue: t.0,
        operation: t.1,
        pool: t.2,
        checkout: t.3,
        returned: t.4,
      )
    },
    fn(m) { #(m.queue, m.operation, m.pool, m.checkout, m.returned) },
  )
}

fn claim_failed_measurements_fields() -> fields.Fields(ClaimFailedMeasurements) {
  fields.imap(
    pair(
      fields.int(atom.create("count")),
      fields.int(atom.create("duration_us")),
    ),
    fn(t) { ClaimFailedMeasurements(count: t.0, duration_us: t.1) },
    fn(m) { #(m.count, m.duration_us) },
  )
}

fn claim_failed_metadata_fields() -> fields.Fields(ClaimFailedMetadata) {
  fields.imap(
    triple(
      queue_ref_fields(),
      wire.closed_string_field(
        "stage",
        operation_to_string,
        operation_from_string,
      ),
      wire.closed_string_field(
        "failure",
        failure_kind_to_string,
        failure_kind_from_string,
      ),
    ),
    fn(t) { ClaimFailedMetadata(queue: t.0, stage: t.1, failure: t.2) },
    fn(m) { #(m.queue, m.stage, m.failure) },
  )
}

fn capacity_measurements_fields() -> fields.Fields(CapacityMeasurements) {
  fields.imap(
    quintuple(
      fields.int(atom.create("maximum")),
      fields.int(atom.create("active")),
      fields.int(atom.create("running")),
      fields.int(atom.create("ack_pending")),
      fields.int(atom.create("available")),
    ),
    fn(t) {
      CapacityMeasurements(
        maximum: t.0,
        active: t.1,
        running: t.2,
        ack_pending: t.3,
        available: t.4,
      )
    },
    fn(m) { #(m.maximum, m.active, m.running, m.ack_pending, m.available) },
  )
}

fn capacity_metadata_fields() -> fields.Fields(CapacityMetadata) {
  fields.imap(
    pair(queue_ref_fields(), fields.bool(atom.create("draining"))),
    fn(t) { CapacityMetadata(queue: t.0, draining: t.1) },
    fn(m) { #(m.queue, m.draining) },
  )
}

fn attempt_phase_to_string(value: AttemptPhase) -> String {
  case value {
    HandlerRunning -> "handler_running"
    AcknowledgementPending -> "acknowledgement_pending"
  }
}

fn attempt_phase_from_string(value: String) -> Result(AttemptPhase, Nil) {
  case value {
    "handler_running" -> Ok(HandlerRunning)
    "acknowledgement_pending" -> Ok(AcknowledgementPending)
    _ -> Error(Nil)
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

fn renewal_outcome_from_string(value: String) -> Result(RenewalOutcome, Nil) {
  case value {
    "renewed" -> Ok(Renewed)
    "skipped_locked" -> Ok(SkippedLocked)
    "live_fence_unavailable" -> Ok(LiveFenceUnavailable)
    "storage_failed" -> Ok(StorageFailed)
    "completion_budget_exhausted" -> Ok(CompletionBudgetExhausted)
    _ -> Error(Nil)
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

fn ack_outcome_from_string(value: String) -> Result(AckOutcome, Nil) {
  case value {
    "replied" -> Ok(AckReplied)
    "reconciled" -> Ok(AckReconciled)
    "rolled_back" -> Ok(AckRolledBack)
    "unknown" -> Ok(AckUnknown)
    "fence_rejected" -> Ok(AckFenceRejected)
    "command_conflict" -> Ok(AckCommandConflict)
    "failed" -> Ok(AckFailed)
    _ -> Error(Nil)
  }
}

fn retry_reason_to_string(value: RetryReason) -> String {
  case value {
    RetryAfterFailure -> "after_failure"
    RetryAfterUnknown -> "after_unknown"
  }
}

fn retry_reason_from_string(value: String) -> Result(RetryReason, Nil) {
  case value {
    "after_failure" -> Ok(RetryAfterFailure)
    "after_unknown" -> Ok(RetryAfterUnknown)
    _ -> Error(Nil)
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

fn operation_from_string(value: String) -> Result(Operation, Nil) {
  case value {
    "quarantine_scan" -> Ok(QuarantineScan)
    "claim_candidate" -> Ok(ClaimCandidate)
    "lease_renewal" -> Ok(LeaseRenewal)
    "acknowledge" -> Ok(Acknowledge)
    "reconcile_acknowledgement" -> Ok(ReconcileAcknowledgement)
    _ -> Error(Nil)
  }
}

fn pool_role_to_string(value: PoolRole) -> String {
  case value {
    MainPool -> "main"
    ReservedPool -> "reserved"
  }
}

fn pool_role_from_string(value: String) -> Result(PoolRole, Nil) {
  case value {
    "main" -> Ok(MainPool)
    "reserved" -> Ok(ReservedPool)
    _ -> Error(Nil)
  }
}

fn checkout_outcome_to_string(value: CheckoutOutcome) -> String {
  case value {
    CheckoutAcquired -> "acquired"
    CheckoutUnavailable -> "unavailable"
  }
}

fn checkout_outcome_from_string(value: String) -> Result(CheckoutOutcome, Nil) {
  case value {
    "acquired" -> Ok(CheckoutAcquired)
    "unavailable" -> Ok(CheckoutUnavailable)
    _ -> Error(Nil)
  }
}

fn call_outcome_to_string(value: CallOutcome) -> String {
  case value {
    CallSucceeded -> "succeeded"
    CallFailed -> "failed"
  }
}

fn call_outcome_from_string(value: String) -> Result(CallOutcome, Nil) {
  case value {
    "succeeded" -> Ok(CallSucceeded)
    "failed" -> Ok(CallFailed)
    _ -> Error(Nil)
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

fn failure_kind_from_string(value: String) -> Result(FailureKind, Nil) {
  case value {
    "timed_out" -> Ok(TimedOut)
    "connection_unavailable" -> Ok(ConnectionUnavailable)
    "rejected" -> Ok(Rejected)
    "unexpected_result" -> Ok(UnexpectedResult)
    _ -> Error(Nil)
  }
}
