//// Pure uniqueness policy values for `grind/postgres`'s admission transaction.
////
//// The policy/key/period values below never touch PostgreSQL: they only
//// build and validate the typed description of a uniqueness policy — which
//// key two submissions must share to conflict, which persisted states count
//// as "still occupying that key", and how long a key stays occupied.
//// `grind/internal/unique_admission` reads these values through `@internal`
//// accessors to build the actual SQL. The admission *results* every submit
//// path returns (`Admission`, `Conflict`, `SubmitError`,
//// `PendingSubmission`) and the stable submission identity
//// (`SubmissionId`) used to make a retried admission command idempotent
//// live in `grind/submission` instead — they apply to plain `submit`/
//// `submit_at` too, which never touch a uniqueness policy at all.

import gleam/option.{type Option, None, Some}
import grind/job
import grind/worker.{type Codec}

/// Whether a uniqueness key is scoped to the submitting queue or shared by
/// every queue in the same schema.
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
