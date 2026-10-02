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

fn job_ref_fields() -> fields.Fields(observation.JobRef) {
  wire.job_ref_fields(
    observation.JobRef,
    fn(ref: observation.JobRef) { ref.job_id },
    fn(ref) { ref.queue },
    fn(ref) { ref.worker_id },
    fn(ref) { ref.worker_version },
  )
}

fn attempt_ref_fields() -> fields.Fields(observation.AttemptRef) {
  wire.attempt_ref_fields(
    observation.AttemptRef,
    fn(ref: observation.AttemptRef) { ref.attempt_id },
    fn(ref) { ref.epoch },
    fn(ref) { ref.attempt },
  )
}

fn consumer_ref_fields() -> fields.Fields(ConsumerRef) {
  fields.record({
    use node <- fields.parameter
    use consumer <- fields.parameter
    ConsumerRef(node:, consumer:)
  })
  |> fields.and(fields.string("node"), fn(m: ConsumerRef) { m.node })
  |> fields.and(fields.string("consumer"), fn(m) { m.consumer })
  |> fields.build
}

fn queue_ref_fields() -> fields.Fields(QueueRef) {
  fields.record({
    use queue <- fields.parameter
    use consumer <- fields.parameter
    QueueRef(queue:, consumer:)
  })
  |> fields.and(fields.string("queue"), fn(m: QueueRef) { m.queue })
  |> fields.and(consumer_ref_fields(), fn(m) { m.consumer })
  |> fields.build
}

fn attempt_context_fields() -> fields.Fields(AttemptContext) {
  fields.record({
    use ref <- fields.parameter
    use attempt <- fields.parameter
    use consumer <- fields.parameter
    AttemptContext(ref:, attempt:, consumer:)
  })
  |> fields.and(job_ref_fields(), fn(m: AttemptContext) { m.ref })
  |> fields.and(attempt_ref_fields(), fn(m) { m.attempt })
  |> fields.and(consumer_ref_fields(), fn(m) { m.consumer })
  |> fields.build
}

fn renewal_measurements_fields() -> fields.Fields(RenewalMeasurements) {
  fields.record({
    use count <- fields.parameter
    use duration_us <- fields.parameter
    use remaining_lease_ms <- fields.parameter
    RenewalMeasurements(count:, duration_us:, remaining_lease_ms:)
  })
  |> fields.and(fields.int("count"), fn(m: RenewalMeasurements) { m.count })
  |> fields.and(fields.int("duration_us"), fn(m) { m.duration_us })
  |> fields.and(fields.optional(fields.int("remaining_lease_ms")), fn(m) {
    m.remaining_lease_ms
  })
  |> fields.build
}

fn renewal_metadata_fields() -> fields.Fields(RenewalMetadata) {
  fields.record({
    use context <- fields.parameter
    use phase <- fields.parameter
    use outcome <- fields.parameter
    RenewalMetadata(context:, phase:, outcome:)
  })
  |> fields.and(attempt_context_fields(), fn(m: RenewalMetadata) { m.context })
  |> fields.and(
    fields.enum(
      "phase",
      [HandlerRunning, AcknowledgementPending],
      attempt_phase_to_string,
    ),
    fn(m) { m.phase },
  )
  |> fields.and(
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
    fn(m) { m.outcome },
  )
  |> fields.build
}

fn acknowledgement_measurements_fields() -> fields.Fields(
  AcknowledgementMeasurements,
) {
  fields.record({
    use count <- fields.parameter
    use duration_us <- fields.parameter
    AcknowledgementMeasurements(count:, duration_us:)
  })
  |> fields.and(fields.int("count"), fn(m: AcknowledgementMeasurements) {
    m.count
  })
  |> fields.and(fields.int("duration_us"), fn(m) { m.duration_us })
  |> fields.build
}

fn acknowledgement_metadata_fields() -> fields.Fields(AcknowledgementMetadata) {
  fields.record({
    use context <- fields.parameter
    use command_id <- fields.parameter
    use outcome <- fields.parameter
    AcknowledgementMetadata(context:, command_id:, outcome:)
  })
  |> fields.and(attempt_context_fields(), fn(m: AcknowledgementMetadata) {
    m.context
  })
  |> fields.and(fields.string("command_id"), fn(m) { m.command_id })
  |> fields.and(
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
    fn(m) { m.outcome },
  )
  |> fields.build
}

fn retry_measurements_fields() -> fields.Fields(RetryMeasurements) {
  fields.record({
    use count <- fields.parameter
    use retry_number <- fields.parameter
    use delay_ms <- fields.parameter
    use pending_duration_us <- fields.parameter
    RetryMeasurements(count:, retry_number:, delay_ms:, pending_duration_us:)
  })
  |> fields.and(fields.int("count"), fn(m: RetryMeasurements) { m.count })
  |> fields.and(fields.int("retry_number"), fn(m) { m.retry_number })
  |> fields.and(fields.int("delay_ms"), fn(m) { m.delay_ms })
  |> fields.and(fields.int("pending_duration_us"), fn(m) {
    m.pending_duration_us
  })
  |> fields.build
}

fn retry_metadata_fields() -> fields.Fields(RetryMetadata) {
  fields.record({
    use context <- fields.parameter
    use command_id <- fields.parameter
    use reason <- fields.parameter
    RetryMetadata(context:, command_id:, reason:)
  })
  |> fields.and(attempt_context_fields(), fn(m: RetryMetadata) { m.context })
  |> fields.and(fields.string("command_id"), fn(m) { m.command_id })
  |> fields.and(
    fields.enum(
      "reason",
      [RetryAfterFailure, RetryAfterUnknown],
      retry_reason_to_string,
    ),
    fn(m) { m.reason },
  )
  |> fields.build
}

fn checkout_measurements_fields() -> fields.Fields(CheckoutMeasurements) {
  fields.record({
    use count <- fields.parameter
    use wait_us <- fields.parameter
    use call_duration_us <- fields.parameter
    use candidates <- fields.parameter
    CheckoutMeasurements(count:, wait_us:, call_duration_us:, candidates:)
  })
  |> fields.and(fields.int("count"), fn(m: CheckoutMeasurements) { m.count })
  |> fields.and(fields.int("checkout_wait_us"), fn(m) { m.wait_us })
  |> fields.and(fields.int("call_duration_us"), fn(m) { m.call_duration_us })
  |> fields.and(fields.int("candidates"), fn(m) { m.candidates })
  |> fields.build
}

fn checkout_metadata_fields() -> fields.Fields(CheckoutMetadata) {
  fields.record({
    use queue <- fields.parameter
    use operation <- fields.parameter
    use pool <- fields.parameter
    use checkout <- fields.parameter
    use returned <- fields.parameter
    CheckoutMetadata(queue:, operation:, pool:, checkout:, returned:)
  })
  |> fields.and(queue_ref_fields(), fn(m: CheckoutMetadata) { m.queue })
  |> fields.and(operation_field("operation"), fn(m) { m.operation })
  |> fields.and(
    fields.enum("pool", [MainPool, ReservedPool], pool_role_to_string),
    fn(m) { m.pool },
  )
  |> fields.and(
    fields.enum(
      "checkout",
      [CheckoutAcquired, CheckoutUnavailable],
      checkout_outcome_to_string,
    ),
    fn(m) { m.checkout },
  )
  |> fields.and(
    fields.enum("returned", [CallSucceeded, CallFailed], call_outcome_to_string),
    fn(m) { m.returned },
  )
  |> fields.build
}

fn claim_failed_measurements_fields() -> fields.Fields(ClaimFailedMeasurements) {
  fields.record({
    use count <- fields.parameter
    use duration_us <- fields.parameter
    ClaimFailedMeasurements(count:, duration_us:)
  })
  |> fields.and(fields.int("count"), fn(m: ClaimFailedMeasurements) { m.count })
  |> fields.and(fields.int("duration_us"), fn(m) { m.duration_us })
  |> fields.build
}

fn claim_failed_metadata_fields() -> fields.Fields(ClaimFailedMetadata) {
  fields.record({
    use queue <- fields.parameter
    use stage <- fields.parameter
    use failure <- fields.parameter
    ClaimFailedMetadata(queue:, stage:, failure:)
  })
  |> fields.and(queue_ref_fields(), fn(m: ClaimFailedMetadata) { m.queue })
  |> fields.and(operation_field("stage"), fn(m) { m.stage })
  |> fields.and(
    fields.enum(
      "failure",
      [TimedOut, ConnectionUnavailable, Rejected, UnexpectedResult],
      failure_kind_to_string,
    ),
    fn(m) { m.failure },
  )
  |> fields.build
}

fn capacity_measurements_fields() -> fields.Fields(CapacityMeasurements) {
  fields.record({
    use maximum <- fields.parameter
    use active <- fields.parameter
    use running <- fields.parameter
    use ack_pending <- fields.parameter
    use available <- fields.parameter
    CapacityMeasurements(maximum:, active:, running:, ack_pending:, available:)
  })
  |> fields.and(fields.int("maximum"), fn(m: CapacityMeasurements) { m.maximum })
  |> fields.and(fields.int("active"), fn(m) { m.active })
  |> fields.and(fields.int("running"), fn(m) { m.running })
  |> fields.and(fields.int("ack_pending"), fn(m) { m.ack_pending })
  |> fields.and(fields.int("available"), fn(m) { m.available })
  |> fields.build
}

fn capacity_metadata_fields() -> fields.Fields(CapacityMetadata) {
  fields.record({
    use queue <- fields.parameter
    use draining <- fields.parameter
    CapacityMetadata(queue:, draining:)
  })
  |> fields.and(queue_ref_fields(), fn(m: CapacityMetadata) { m.queue })
  |> fields.and(fields.bool("draining"), fn(m) { m.draining })
  |> fields.build
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
