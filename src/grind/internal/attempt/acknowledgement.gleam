//// The durable acknowledgement transaction and receipt protocol for a claimed attempt.
//// The claim and worker execution remain in `grind/internal/attempt`.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import grind/diagnostic
import grind/internal/diagnostics
import grind/internal/lease
import grind/internal/store
import grind/job
import grind/postgres
import grind/worker
import pog
import sinal/forwarder.{type Forwarder}

pub type Claim {
  Claim(
    id: Int,
    attempt_id: Int,
    epoch: Int,
    input_version: String,
    encoded_input: String,
    worker_id: String,
    worker_version: String,
    output_version: String,
    error_version: Option(String),
    current_attempt: Int,
    max_attempts: Int,
    snooze_count: Int,
    delivery_count: Int,
    previous_state: String,
  )
}

pub type AckProposal {
  AckProposal(
    proposed_state: String,
    failure_cause: Option(String),
    requested_delay_ms: Option(Int),
    output_version: Option(String),
    output: Option(String),
    error_version: Option(String),
    error: Option(String),
    failure_description: Option(String),
  )
}

/// The proven-committed disposition of one acknowledgement command, carried
/// from wherever it was proven (a fresh write, or a durable receipt read
/// back matching this exact command) up to `acknowledge`, which is the only
/// place that emits `[grind, job, acknowledged]` — never from inside a
/// transaction callback. `via_receipt_match: True` means this call's own
/// transaction did not write anything new; the exact same command was
/// already durably applied, so the observation's `confirmation` is
/// `Reconciled` rather than `Replied`. `available_at_unix_ms` is only ever
/// known for a fresh write (read from that write's own `RETURNING`); a
/// receipt match cannot recover it, since `grind_job_acknowledgements` does
/// not retain `available_at`.
pub type AckCommit {
  AckCommit(
    committed_state: String,
    failure_cause: Option(String),
    available_at_unix_ms: Option(Int),
    via_receipt_match: Bool,
  )
}

pub fn resolve_ack_transaction_result(
  connection: pog.Connection,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
  transaction_result: Result(
    AckCommit,
    pog.TransactionError(postgres.QueueRunError),
  ),
  forwarder: Forwarder,
  reference: diagnostic.QueueRef,
) -> Result(AckCommit, postgres.QueueRunError) {
  case transaction_result {
    Ok(commit) -> Ok(commit)
    Error(pog.TransactionRolledBack(error)) -> Error(error)
    Error(pog.TransactionQueryError(_)) ->
      reconcile_unknown_ack(
        connection,
        queue,
        attempt_owner,
        claim,
        command_id,
        proposal,
        execution,
        forwarder,
        reference,
      )
  }
}

fn reconcile_unknown_ack(
  connection: pog.Connection,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
  forwarder: Forwarder,
  reference: diagnostic.QueueRef,
) -> Result(AckCommit, postgres.QueueRunError) {
  case
    matching_acknowledgement_observed(
      connection,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
      Some(#(forwarder, reference)),
    )
  {
    Ok(Some(#(True, committed_state, failure_cause))) ->
      Ok(AckCommit(
        committed_state:,
        failure_cause:,
        available_at_unix_ms: None,
        via_receipt_match: True,
      ))
    Ok(Some(#(False, _, _))) -> Error(postgres.QueueAckCommandConflict)
    Ok(None) | Error(_) ->
      Error(postgres.QueueAckUnknown(command_id, execution))
  }
}

pub fn acknowledge_transaction(
  connection: pog.Connection,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
  expected_output_version: String,
) -> Result(AckCommit, postgres.QueueRunError) {
  case
    matching_acknowledgement(
      connection,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
    )
  {
    Error(error) -> Error(error)
    Ok(Some(#(True, committed_state, failure_cause))) ->
      Ok(AckCommit(
        committed_state:,
        failure_cause:,
        available_at_unix_ms: None,
        via_receipt_match: True,
      ))
    Ok(Some(#(False, _, _))) -> Error(postgres.QueueAckCommandConflict)
    Ok(None) -> {
      let Claim(id:, attempt_id:, epoch:, error_version:, ..) = claim
      let AckProposal(
        proposed_state:,
        failure_cause:,
        output_version: _,
        requested_delay_ms:,
        output:,
        error_version: _,
        error:,
        failure_description:,
      ) = proposal
      // `output_version` and `error_version` hold the contract versions the
      // job was admitted under. The claim and `bind_handle` compare them with
      // the registered worker, so no acknowledgement writes them: a write
      // stores or clears only the `output` and `error` payloads.
      let #(sql, parameters) = case proposed_state {
        "succeeded" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'succeeded' END, output = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $1::jsonb END, error = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE NULL END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL, finished_at = clock_timestamp() WHERE id = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND output_version = $7 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, output),
            pog.int(id),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.text(expected_output_version),
          ],
        )
        "business_failed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'business_failed' END, output = NULL, error = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $1::jsonb END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $2 END, failure_cause = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $3 END, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL, finished_at = clock_timestamp() WHERE id = $4 AND queue = $5 AND state = 'executing' AND attempt_id = $6 AND attempt_epoch = $7 AND attempt_owner = $8 AND error_version IS NOT DISTINCT FROM $9 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " AND (cancel_requested_at IS NOT NULL OR (($3 = 'budget_exhausted' AND attempt_count >= max_attempts) OR ($3 = 'retry_declined' AND attempt_count < max_attempts))) RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, error),
            pog.nullable(pog.text, failure_description),
            pog.nullable(pog.text, failure_cause),
            pog.int(id),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.nullable(pog.text, error_version),
          ],
        )
        "retryable" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'retryable' END, available_at = CASE WHEN cancel_requested_at IS NOT NULL THEN available_at ELSE clock_timestamp() + ($1::double precision * interval '1 millisecond') END, output = NULL, error = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $2::jsonb END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $3 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL, finished_at = CASE WHEN cancel_requested_at IS NOT NULL THEN clock_timestamp() END WHERE id = $4 AND queue = $5 AND state = 'executing' AND attempt_id = $6 AND attempt_epoch = $7 AND attempt_owner = $8 AND error_version IS NOT DISTINCT FROM $9 AND (attempt_count < max_attempts OR cancel_requested_at IS NOT NULL) AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.int, requested_delay_ms),
            pog.nullable(pog.text, error),
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.nullable(pog.text, error_version),
          ],
        )
        "snoozed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'scheduled' END, available_at = CASE WHEN cancel_requested_at IS NOT NULL THEN available_at ELSE clock_timestamp() + ($1::double precision * interval '1 millisecond') END, snooze_count = CASE WHEN cancel_requested_at IS NOT NULL THEN snooze_count ELSE snooze_count + 1 END, attempt_count = CASE WHEN cancel_requested_at IS NOT NULL THEN attempt_count ELSE GREATEST(attempt_count - 1, 0) END, output = NULL, error = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $2 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL, finished_at = CASE WHEN cancel_requested_at IS NOT NULL THEN clock_timestamp() END WHERE id = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.int, requested_delay_ms),
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "discarded" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'discarded' END, output = NULL, error = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL, finished_at = clock_timestamp() WHERE id = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "cancelled" -> #(
          "UPDATE grind_jobs SET state = 'cancelled', output = NULL, error = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL, finished_at = clock_timestamp() WHERE id = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "uncertain" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'uncertain' END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, uncertain_at = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE clock_timestamp() END, attempt_owner = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE attempt_owner END, lease_expires_at = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE lease_expires_at END, cancel_requested_at = NULL, finished_at = CASE WHEN cancel_requested_at IS NOT NULL THEN clock_timestamp() END WHERE id = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "runtime_failed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'runtime_failed' END, output = NULL, error = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL, finished_at = clock_timestamp() WHERE id = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND "
            <> lease.live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        _ -> #("SELECT 1 WHERE FALSE", [])
      }
      let query =
        list.fold(parameters, pog.query(sql), fn(query, parameter) {
          pog.parameter(query, parameter)
        })
        |> pog.returning({
          use updated_id <- decode.field(0, decode.int)
          use committed_state <- decode.field(1, decode.string)
          use committed_description <- decode.field(
            2,
            decode.optional(decode.string),
          )
          use committed_available_at_ms <- decode.field(3, decode.int)
          decode.success(#(
            updated_id,
            committed_state,
            committed_description,
            committed_available_at_ms,
          ))
        })
      case store.execute_safely(query, on: connection) {
        Error(error) -> Error(postgres.QueueAckFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] ->
              // A duplicate ACK can wait behind the first writer's row lock.
              // Re-read its receipt after the UPDATE observes the committed
              // state instead of misreporting that exact retry as stale.
              case
                matching_acknowledgement(
                  connection,
                  queue,
                  attempt_owner,
                  claim,
                  command_id,
                  proposal,
                )
              {
                Ok(Some(#(True, committed_state, failure_cause))) ->
                  Ok(AckCommit(
                    committed_state:,
                    failure_cause:,
                    available_at_unix_ms: None,
                    via_receipt_match: True,
                  ))
                Ok(Some(#(False, _, _))) ->
                  Error(postgres.QueueAckCommandConflict)
                Ok(None) ->
                  current_ack_rejection(
                    connection,
                    queue,
                    attempt_owner,
                    claim,
                    execution,
                  )
                Error(error) -> Error(error)
              }
            [#(_, committed_state, _committed_description, available_at_ms)] ->
              insert_acknowledgement(
                connection,
                queue,
                attempt_owner,
                claim,
                command_id,
                proposal,
                committed_state,
                available_at_for_observation(committed_state, available_at_ms),
              )
            _ -> Error(postgres.QueueStorageInvariantViolated)
          }
      }
    }
  }
}

/// `available_at` is only a meaningful "next eligibility" signal for a
/// *committed* `retryable`/`scheduled` outcome — read from the acknowledge
/// UPDATE's own `RETURNING`, never from the proposal. A proposed
/// retry/snooze whose `available_at` write was itself overridden (a
/// concurrent cancellation commits `cancelled` instead, leaving
/// `available_at` at its unrelated pre-ack value) must not report that
/// stale value as if it were a real next-eligibility time; gating on the
/// committed state, not the proposed one, is what keeps this correct. For
/// every other outcome the column still holds a value (it is `NOT NULL`),
/// but that value is not what an observer means by "when does this job
/// become eligible again", so it is not surfaced there.
fn available_at_for_observation(
  committed_state: String,
  available_at_ms: Int,
) -> Option(Int) {
  case committed_state {
    "retryable" | "scheduled" -> Some(available_at_ms)
    _ -> None
  }
}

fn current_ack_rejection(
  connection: pog.Connection,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  execution: worker.Execution,
) -> Result(AckCommit, postgres.QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "SELECT state, attempt_id, attempt_epoch, attempt_owner, "
      <> lease.live_lease_predicate("clock_timestamp()")
      <> " FROM grind_jobs WHERE id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(queue))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use current_attempt <- decode.field(1, decode.optional(decode.int))
      use current_epoch <- decode.field(2, decode.optional(decode.int))
      use current_owner <- decode.field(3, decode.optional(decode.string))
      use lease_live <- decode.field(4, decode.bool)
      decode.success(#(
        state,
        current_attempt,
        current_epoch,
        current_owner,
        lease_live,
      ))
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(postgres.QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] ->
          Error(postgres.QueueAckStale(execution, postgres.AckRecordMissing))
        [#(state, current_attempt, current_epoch, current_owner, lease_live)] ->
          case state {
            "executing" ->
              case
                current_attempt == Some(attempt_id)
                && current_epoch == Some(epoch)
                && current_owner == Some(attempt_owner)
              {
                False ->
                  Error(postgres.QueueAckStale(
                    execution,
                    postgres.AckOwnershipChanged(
                      attempt_id: current_attempt,
                      epoch: current_epoch,
                      owner: current_owner,
                    ),
                  ))
                True ->
                  case lease_live {
                    False ->
                      Error(postgres.QueueAckStale(
                        execution,
                        postgres.AckLeaseExpired(
                          attempt_id,
                          epoch,
                          attempt_owner,
                        ),
                      ))
                    True ->
                      Error(postgres.QueueAckStale(
                        execution,
                        postgres.AckRecordChanged,
                      ))
                  }
              }
            _ ->
              case job.state_of_stored(state) {
                Ok(decoded_state) ->
                  Error(postgres.QueueAckStale(
                    execution,
                    postgres.AckStateChanged(decoded_state),
                  ))
                Error(Nil) -> Error(postgres.QueueStorageInvariantViolated)
              }
          }
        _ -> Error(postgres.QueueStorageInvariantViolated)
      }
  }
}

/// The stable acknowledgement command ID for one attempt fence. Exposed only
/// so tests can construct the same ID an internal reconciliation path would,
/// without re-encoding this format themselves.
pub fn acknowledgement_command_id(
  job_id: Int,
  attempt_id: Int,
  epoch: Int,
) -> String {
  "grind-ack:"
  <> int.to_string(job_id)
  <> ":"
  <> int.to_string(attempt_id)
  <> ":"
  <> int.to_string(epoch)
}

/// The fixed-order envelope deliberately hashes PostgreSQL's JSONB rendering,
/// not a cross-runtime canonical JSON representation. Presence flags keep SQL
/// NULL distinct from JSON null. Large numeric textual variants may conflict.
fn acknowledgement_fingerprint_sql(first_parameter: Int) -> String {
  let proposed_state = sql_parameter(first_parameter, "text")
  let output_version = sql_parameter(first_parameter + 1, "text")
  let output = sql_parameter(first_parameter + 2, "text")
  let error_version = sql_parameter(first_parameter + 3, "text")
  let error = sql_parameter(first_parameter + 4, "text")
  let reason = sql_parameter(first_parameter + 5, "text")
  let delay = sql_parameter(first_parameter + 6, "bigint")
  let cause = sql_parameter(first_parameter + 7, "text")
  "sha256(convert_to(jsonb_build_array('grind-ack-proposal-v1', "
  <> proposed_state
  <> ", "
  <> output_version
  <> " IS NOT NULL, "
  <> output_version
  <> ", "
  <> output
  <> " IS NOT NULL, CASE WHEN "
  <> output
  <> " IS NULL THEN NULL::jsonb ELSE "
  <> output
  <> "::jsonb END, "
  <> error_version
  <> " IS NOT NULL, "
  <> error_version
  <> ", "
  <> error
  <> " IS NOT NULL, CASE WHEN "
  <> error
  <> " IS NULL THEN NULL::jsonb ELSE "
  <> error
  <> "::jsonb END, "
  <> reason
  <> " IS NOT NULL, "
  <> reason
  <> ", "
  <> delay
  <> " IS NOT NULL, "
  <> delay
  <> ", "
  <> cause
  <> " IS NOT NULL, "
  <> cause
  <> ")::text, 'UTF8'))"
}

fn sql_parameter(index: Int, cast: String) -> String {
  "$" <> int.to_string(index) <> "::" <> cast
}

/// Looks up the exact acknowledgement row for `command_id`, if one exists,
/// and reports both whether it matches this exact proposal and the
/// disposition it actually committed — so a caller that finds a match can
/// build the `Reconciled` observation from a real receipt read rather than
/// re-deriving it from this call's own (possibly stale) proposal.
fn matching_acknowledgement(
  connection: pog.Connection,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
) -> Result(Option(#(Bool, String, Option(String))), postgres.QueueRunError) {
  matching_acknowledgement_observed(
    connection,
    queue,
    attempt_owner,
    claim,
    command_id,
    proposal,
    None,
  )
}

fn matching_acknowledgement_observed(
  connection: pog.Connection,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  observer: Option(#(Forwarder, diagnostic.QueueRef)),
) -> Result(Option(#(Bool, String, Option(String))), postgres.QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  let AckProposal(
    proposed_state:,
    failure_cause:,
    requested_delay_ms:,
    output_version:,
    output:,
    error_version:,
    error:,
    failure_description:,
  ) = proposal
  let query =
    pog.query(
      "SELECT command_id = $1 AND queue = $2 AND job_id = $3 AND worker_id = $4 AND worker_version = $5 AND attempt_id = $6 AND attempt_epoch = $7 AND attempt_owner = $8 AND proposal_sha256 = "
      <> acknowledgement_fingerprint_sql(9)
      <> ", committed_state, failure_cause FROM grind_job_acknowledgements WHERE command_id = $1",
    )
    |> pog.parameter(pog.text(command_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(proposed_state))
    |> pog.parameter(pog.nullable(pog.text, output_version))
    |> pog.parameter(pog.nullable(pog.text, output))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.nullable(pog.int, requested_delay_ms))
    |> pog.parameter(pog.nullable(pog.text, failure_cause))
    |> pog.returning({
      use matches <- decode.field(0, decode.bool)
      use committed_state <- decode.field(1, decode.string)
      use committed_failure_cause <- decode.field(
        2,
        decode.optional(decode.string),
      )
      decode.success(#(matches, committed_state, committed_failure_cause))
    })
  let queried = case observer {
    None -> store.execute_safely(query, on: connection)
    Some(#(forwarder, reference)) ->
      diagnostics.checkout(
        forwarder,
        reference,
        diagnostic.ReconcileAcknowledgement,
        diagnostic.MainPool,
        store.execute_measured(query, on: connection),
      )
  }
  case queried {
    Error(error) -> Error(postgres.QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Ok(None)
        [row] -> Ok(Some(row))
        _ -> Error(postgres.QueueStorageInvariantViolated)
      }
  }
}

fn insert_acknowledgement(
  connection: pog.Connection,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  actual_committed_state: String,
  available_at_unix_ms: Option(Int),
) -> Result(AckCommit, postgres.QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  let AckProposal(
    proposed_state:,
    failure_cause:,
    requested_delay_ms:,
    output_version:,
    output:,
    error_version:,
    error:,
    failure_description:,
  ) = proposal
  let committed_failure_cause = case actual_committed_state {
    "cancelled" -> None
    _ -> failure_cause
  }
  let query =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, failure_cause, proposal_sha256) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $17, $18, "
      <> acknowledgement_fingerprint_sql(9)
      <> ") RETURNING command_id",
    )
    |> pog.parameter(pog.text(command_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(proposed_state))
    |> pog.parameter(pog.nullable(pog.text, output_version))
    |> pog.parameter(pog.nullable(pog.text, output))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.nullable(pog.int, requested_delay_ms))
    |> pog.parameter(pog.nullable(pog.text, failure_cause))
    |> pog.parameter(pog.text(actual_committed_state))
    |> pog.parameter(pog.nullable(pog.text, committed_failure_cause))
    |> pog.returning({
      use inserted_command_id <- decode.field(0, decode.string)
      decode.success(inserted_command_id)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(postgres.QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] ->
          Ok(AckCommit(
            committed_state: actual_committed_state,
            failure_cause: committed_failure_cause,
            available_at_unix_ms:,
            via_receipt_match: False,
          ))
        _ -> Error(postgres.QueueStorageInvariantViolated)
      }
  }
}
