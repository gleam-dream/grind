//// A job that an abandoned attempt left `uncertain`, as a worker's declared
//// settlement receives it (see `grind/worker.with_settlement`). Read it with
//// the functions here; `grind/testing.abandoned` builds one for a test.
////
//// ```gleam
//// worker.with_settlement(agent, fn(_transaction, job) {
////   case abandoned.cause(job), abandoned.try_number(job) {
////     abandoned.TimedOut, _ -> Ok(worker.ConfirmFailure("timed out"))
////     abandoned.HandlerCrashed, _ -> Ok(worker.LeaveUncertain("a bug"))
////     abandoned.LeaseExpired, try if try < 5 -> Error("ask again later")
////     abandoned.LeaseExpired, _ -> Ok(worker.LeaveUncertain("needs a person"))
////   }
//// })
//// ```

import gleam/time/timestamp.{type Timestamp}
import grind/internal/worker as definition
import sinal/correlation.{type Correlation}

/// An abandoned job and its abandoned attempt. Opaque: read it with the
/// functions below, and build one for a test with `grind/testing.abandoned`.
pub opaque type Abandoned(input) {
  Abandoned(definition.Abandoned(input))
}

/// Wraps Grind's own record. Not part of the public API.
@internal
pub fn from_definition(
  abandoned: definition.Abandoned(input),
) -> Abandoned(input) {
  Abandoned(abandoned)
}

/// Unwraps Grind's own record. Not part of the public API.
@internal
pub fn to_definition(
  abandoned: Abandoned(input),
) -> definition.Abandoned(input) {
  let Abandoned(inner) = abandoned
  inner
}

/// Why the attempt was abandoned.
pub type Cause {
  /// Its lease expired without an acknowledgement: its node died or lost the
  /// database, a renewal found the lease lost while its handler ran (Grind
  /// then stops the handler and leaves the job to lease-expiry recovery), or
  /// its handler ran past a replay-safe timeout and the lease ran out.
  LeaseExpired
  /// Grind stopped its handler at the worker's timeout under `HoldUncertain`.
  TimedOut
  /// Its handler crashed under `HoldUncertain`: it panicked, failed a
  /// `let assert`, or its process exited in any other way without returning.
  /// `description` names the kind of failure and where it happened.
  HandlerCrashed
}

/// The job's id.
pub fn job_id(abandoned: Abandoned(input)) -> Int {
  to_definition(abandoned).job_id
}

/// The job's input, decoded with the worker's input codec.
pub fn input(abandoned: Abandoned(input)) -> input {
  to_definition(abandoned).input
}

/// The abandoned attempt, whose effect the settlement decides.
pub fn attempt_id(abandoned: Abandoned(input)) -> Int {
  to_definition(abandoned).attempt_id
}

/// The abandoned attempt's business attempt number, from 1.
pub fn attempt(abandoned: Abandoned(input)) -> Int {
  to_definition(abandoned).attempt
}

/// Why the attempt was abandoned.
pub fn cause(abandoned: Abandoned(input)) -> Cause {
  case to_definition(abandoned).cause {
    definition.LeaseExpired -> LeaseExpired
    definition.TimedOut -> TimedOut
    definition.HandlerCrashed -> HandlerCrashed
  }
}

/// How many lease-expiry replays the job used under its abandonment policy.
pub fn replays(abandoned: Abandoned(input)) -> Int {
  to_definition(abandoned).replays
}

/// How many replays a settlement or an operator authorized for this job
/// after earlier abandonments. `AuthorizeReplay` does not reset the job's
/// replays, so a settlement that authorizes replay bounds it with this.
pub fn authorized_replays(abandoned: Abandoned(input)) -> Int {
  to_definition(abandoned).authorized_replays
}

/// Which settlement try this is for the abandoned attempt, from 1. A
/// settlement may stop trying with `LeaveUncertain` after its own limit.
pub fn try_number(abandoned: Abandoned(input)) -> Int {
  to_definition(abandoned).try_number
}

/// Whether a cancellation of the job is pending. A settlement that then
/// returns `AuthorizeReplay` declines the job instead, for an operator.
pub fn cancellation_requested(abandoned: Abandoned(input)) -> Bool {
  to_definition(abandoned).cancellation_requested
}

/// When Grind held the job uncertain. Expired ownership does not prove the
/// abandoned handler stopped: a node cut off from the database may still run
/// it until its own timeout.
pub fn held_at(abandoned: Abandoned(input)) -> Timestamp {
  to_definition(abandoned).held_at
}

/// The job's description, for logs and notices: why it is uncertain, in
/// words. Branch on `cause`, not on this text.
pub fn description(abandoned: Abandoned(input)) -> String {
  to_definition(abandoned).description
}

/// The job's correlation.
pub fn correlation(abandoned: Abandoned(input)) -> Correlation {
  to_definition(abandoned).correlation
}
