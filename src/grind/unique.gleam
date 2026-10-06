//// Uniqueness policies: which jobs count as duplicates of each other, for
//// how long, and what a duplicate submission does.
////
//// ```gleam
//// import gleam/time/duration
//// import grind/job
//// import grind/unique
////
//// let policy =
////   unique.policy(unique.full_input(), unique.within(duration.hours(1), unique.FromInsertion))
//// grind.submit(jobs, job.new(mailer(), email) |> job.unique(policy))
//// ```
////
//// A job submitted with a policy is admitted only when no job of the same
//// worker and version with the same key occupies it. Otherwise the submit
//// returns `grind.Existing` (or `grind.Rescheduled` with `reschedule_to`)
//// with the occupying job. The policy is checked under an advisory lock in
//// the admission transaction, so two concurrent submissions never both
//// insert. See `https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/UNIQUENESS-CONTRACT.md`.
////
//// The defaults are: scope `WithinQueue`, states `Incomplete`, and a
//// duplicate leaves the existing job unchanged.

import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import grind/internal/job as internal_job
import grind/internal/unique as internal
import grind/worker.{type Codec}

/// A complete uniqueness policy. Build it with `policy`.
pub type Policy(input) =
  internal.Uniqueness(input)

/// Which part of the input forms the key. Build it with `full_input` or
/// `selected`.
pub type Key(input) =
  internal.Key(input)

/// How long a job occupies its key. Build it with `within` or
/// `while_retained`.
pub type Period =
  internal.Period

/// Whether a key is shared by every queue in the schema.
pub type QueueScope {
  WithinQueue
  AcrossQueues
}

/// Which stored states count as still occupying a key.
pub type States {
  /// queued, scheduled, retryable, executing and uncertain.
  Incomplete
  /// scheduled only.
  ScheduledOnly
  /// `Incomplete` plus succeeded.
  IncompleteOrSucceeded
  /// Every stored state.
  AllRetained
}

/// What a finite period is measured from.
pub type Origin {
  /// The job's insertion time.
  FromInsertion
  /// The job's scheduled time.
  FromSchedule
}

/// A policy over `key` for `period`, scoped to the submitting queue, over
/// the `Incomplete` states, that leaves a duplicate's existing job
/// unchanged.
pub fn policy(key: Key(input), period: Period) -> Policy(input) {
  internal.Uniqueness(
    policy: internal.policy(
      key,
      internal.WithinQueue,
      period,
      internal.Incomplete,
    ),
    on_conflict: internal.KeepExisting,
  )
}

/// Sets whether the key is shared across queues.
pub fn with_scope(policy: Policy(input), scope: QueueScope) -> Policy(input) {
  let internal.PolicyFields(key:, period:, states:, ..) =
    internal.policy_fields(policy.policy)
  let scope = case scope {
    WithinQueue -> internal.WithinQueue
    AcrossQueues -> internal.AcrossQueues
  }
  internal.Uniqueness(
    ..policy,
    policy: internal.policy(key, scope, period, states),
  )
}

/// Sets which states occupy the key.
pub fn with_states(policy: Policy(input), states: States) -> Policy(input) {
  let internal.PolicyFields(key:, scope:, period:, ..) =
    internal.policy_fields(policy.policy)
  let states = case states {
    Incomplete -> internal.Incomplete
    ScheduledOnly -> internal.ScheduledOnly
    IncompleteOrSucceeded -> internal.IncompleteOrSucceeded
    AllRetained -> internal.AllRetained
  }
  internal.Uniqueness(
    ..policy,
    policy: internal.policy(key, scope, period, states),
  )
}

/// A duplicate moves a `scheduled` existing job to `at`; an existing job in
/// any other state is left unchanged. The submit then returns
/// `grind.Rescheduled`.
pub fn reschedule_to(policy: Policy(input), at: Timestamp) -> Policy(input) {
  let at = internal_job.AvailableAt(int_max(unix_ms(at), 0))
  internal.Uniqueness(..policy, on_conflict: internal.RescheduleScheduledTo(at))
}

/// The whole encoded input is the key.
pub fn full_input() -> Key(input) {
  internal.full_input()
}

/// A projection of the input, encoded with its own codec, is the key. The
/// `name` and the codec's version identify the key, so a changed projection
/// never matches keys of the old one. If the codec rejects the projected
/// value, the submit returns `InvalidInput`. Panics on an empty name.
pub fn selected(
  name: String,
  select: fn(input) -> key,
  codec: Codec(key),
) -> Key(input) {
  case internal.selected(name, select, codec) {
    Ok(key) -> key
    Error(_) -> panic as "grind/unique: a selected key needs a name"
  }
}

/// A job occupies its key for `period`, measured from `from`. Panics on a
/// period that is not positive or above about 285 years.
pub fn within(period: Duration, from from: Origin) -> Period {
  let from = case from {
    FromInsertion -> internal.FromInsertion
    FromSchedule -> internal.FromSchedule
  }
  case internal.within_milliseconds(duration.to_milliseconds(period), from) {
    Ok(period) -> period
    Error(_) ->
      panic as "grind/unique: a uniqueness period must be positive and at most 9,007,199,254,740 ms"
  }
}

/// A job occupies its key for as long as it is stored.
pub fn while_retained() -> Period {
  internal.while_retained()
}

fn unix_ms(at: Timestamp) -> Int {
  let #(seconds, nanoseconds) = timestamp.to_unix_seconds_and_nanoseconds(at)
  seconds * 1000 + nanoseconds / 1_000_000
}

fn int_max(a: Int, b: Int) -> Int {
  case a > b {
    True -> a
    False -> b
  }
}
