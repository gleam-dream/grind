//// A worker's declared settlement with its types erased, so a registry can
//// hold the settlements of workers of any input, output and error type
//// (docs/design/design.typ, "Declared settlement of abandoned jobs").
////
//// The erased function decodes the stored input with the worker's codec,
//// calls the declared function with the open transaction, and turns its
//// decision into the resolution command the operator resolution protocol
//// applies. It never touches storage on its own.

import exception
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/timestamp
import grind/internal/job
import grind/internal/postgres/resolution
import grind/internal/postgres/resolution_queries.{type ResolutionCommand}
import grind/internal/worker.{type Worker}
import pog
import sinal/correlation

/// The stored facts about one due job a try hands to the declared function.
pub type Request {
  Request(
    installation: job.Installation,
    job_id: Int,
    queue: String,
    input_version: String,
    encoded_input: String,
    attempt_id: Int,
    attempt: Int,
    cause: worker.AbandonmentCause,
    replays: Int,
    authorized_replays: Int,
    try_number: Int,
    cancellation_requested: Bool,
    held_at_us: Int,
    description: String,
    correlation: Option(String),
  )
}

/// What the declared function decided.
pub type Decided {
  /// Apply this resolution command.
  Resolve(ResolutionCommand)
  /// Keep the job uncertain for an operator, with this reason.
  Leave(reason: String)
}

/// Why a try cannot commit a decision.
pub type Failure {
  /// The function returned an error.
  FunctionFailed(reason: String)
  /// The function crashed.
  FunctionCrashed(reason: String)
  /// The stored input does not decode under the deployed codec.
  Undecodable(reason: String)
  /// The resolution protocol refuses the decision before any write; a later
  /// try or deploy may change that.
  Refused(reason: String)
  /// A refusal no later try can change: `AuthorizeReplay` under a pending
  /// cancellation, which cannot be withdrawn. The try ends the mark as
  /// declined, with this reason, without the function's writes.
  RefusedForGood(reason: String)
}

/// An erased declared settlement.
pub type Settle =
  fn(pog.Connection, Request) -> Result(Decided, Failure)

/// The resolution id prefix Grind reserves for declared settlements.
pub const resolution_prefix = "grind-settlement:"

/// The attribution of every settlement's resolution.
pub const resolved_by = "grind settlement"

/// The worker's declared settlement, erased, or `None` when it declares none.
pub fn erase(definition: Worker(input, output, error)) -> Option(Settle) {
  case definition.settlement {
    None -> None
    Some(settle) ->
      Some(fn(transaction, request) {
        settle_erased(definition, settle, transaction, request)
      })
  }
}

fn settle_erased(
  definition: Worker(input, output, error),
  settle: fn(pog.Connection, worker.Abandoned(input)) ->
    Result(worker.Settlement(output, error), String),
  transaction: pog.Connection,
  request: Request,
) -> Result(Decided, Failure) {
  let Request(
    installation:,
    job_id:,
    queue:,
    input_version:,
    encoded_input:,
    attempt_id:,
    attempt:,
    cause:,
    replays:,
    authorized_replays:,
    try_number:,
    cancellation_requested:,
    held_at_us:,
    description:,
    correlation: stored_correlation,
  ) = request
  use input <- result.try(
    worker.decode_codec(definition.input, input_version, encoded_input)
    |> result.map_error(fn(error) { Undecodable(string.inspect(error)) }),
  )
  let abandoned =
    worker.Abandoned(
      job_id:,
      input:,
      attempt_id:,
      attempt:,
      cause:,
      replays:,
      authorized_replays:,
      try_number:,
      cancellation_requested:,
      held_at: timestamp.from_unix_seconds_and_nanoseconds(
        held_at_us / 1_000_000,
        { held_at_us % 1_000_000 } * 1000,
      ),
      description:,
      correlation: case stored_correlation {
        Some(key) -> correlation.from_key(key)
        None -> correlation.from_key("grind-job-" <> int.to_string(job_id))
      },
    )
  use decided <- result.try(
    case exception.rescue(fn() { settle(transaction, abandoned) }) {
      Error(crash) -> Error(FunctionCrashed(string.inspect(crash)))
      Ok(Error(reason)) -> Error(FunctionFailed(reason))
      Ok(Ok(decision)) -> Ok(decision)
    },
  )
  let handle = job.new_handle(job_id, installation, queue, definition)
  let command = fn(decision) {
    resolution.prepare(
      handle,
      resolution.Request(
        resolution_id: resolution_id(job_id, attempt_id),
        resolved_by:,
        details: "declared settlement of abandoned attempt "
          <> int.to_string(attempt_id),
        decision:,
      ),
    )
    |> result.map(Resolve)
    |> result.map_error(fn(error) { Refused(string.inspect(error)) })
  }
  case decided {
    worker.SettleSuccess(output) -> command(resolution.ConfirmSuccess(output))
    // Without an error codec, the failure commits with no stored error, as a
    // handler's `Failed` does on the same worker.
    worker.SettleFailure(error) ->
      case definition.error {
        Some(_) -> command(resolution.ConfirmBusinessFailure(error))
        None -> command(resolution.ConfirmUnrecordedFailure)
      }
    worker.SettleReplay ->
      case cancellation_requested {
        True ->
          Error(RefusedForGood(
            "replay refused: a cancellation of the job is pending",
          ))
        False -> command(resolution.AuthorizeReplay)
      }
    worker.SettleLeaveUncertain(reason:) ->
      worker.storable_text("the settlement reason", reason)
      |> result.replace(Leave(reason))
      |> result.map_error(Refused)
  }
}

/// The resolution id of the settlement of `attempt_id` of job `job_id`.
pub fn resolution_id(job_id: Int, attempt_id: Int) -> String {
  resolution_prefix <> int.to_string(job_id) <> ":" <> int.to_string(attempt_id)
}
