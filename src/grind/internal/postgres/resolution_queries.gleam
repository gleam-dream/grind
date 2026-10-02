//// Constructs audited resolution SQL and row decoders.
//// Transaction ordering lives in `grind/internal/postgres/resolution`;
//// public result classification stays in `grind/postgres`.

import gleam/dynamic/decode
import gleam/option.{type Option, None, Some}
import grind/internal/terminal
import pog

pub type ResolutionCommand {
  ResolutionCommand(
    id: Int,
    queue: String,
    worker_id: String,
    worker_version: String,
    expected_output_version: String,
    expected_error_version: Option(String),
    resolution_id: String,
    resolved_by: String,
    details: String,
    decision: String,
    target_state: String,
    output_version: String,
    encoded_output: Option(String),
    error_version: Option(String),
    encoded_error: Option(String),
    failure_description: Option(String),
  )
}

pub fn find_resolution_query(resolution_id: String, payload: Option(String)) {
  let query =
    pog.query(
      "SELECT job_id, queue, worker_id, worker_version, decision, resolved_by, details, target_state, payload_version, CASE WHEN payload IS NOT DISTINCT FROM $2::jsonb THEN 'same' ELSE 'different' END FROM grind_job_resolutions WHERE resolution_id = $1",
    )
    |> pog.parameter(pog.text(resolution_id))
    |> pog.parameter(pog.nullable(pog.text, payload))
    |> pog.returning({
      use job_id <- decode.field(0, decode.int)
      use queue <- decode.field(1, decode.string)
      use worker_id <- decode.field(2, decode.optional(decode.string))
      use worker_version <- decode.field(3, decode.optional(decode.string))
      use decision <- decode.field(4, decode.string)
      use resolved_by <- decode.field(5, decode.string)
      use details <- decode.field(6, decode.string)
      use target_state <- decode.field(7, decode.string)
      use payload_version <- decode.field(8, decode.optional(decode.string))
      use payload_match <- decode.field(9, decode.string)
      decode.success(#(
        job_id,
        queue,
        worker_id,
        worker_version,
        decision,
        resolved_by,
        details,
        target_state,
        payload_version,
        payload_match,
      ))
    })
  query
}

pub fn lock_uncertain_query(id: Int) {
  let select =
    pog.query(
      "SELECT queue, worker_id, worker_version, state, attempt_id, attempt_epoch, attempt_owner, lease_expires_at::text, output_version, error_version, cancel_requested_at IS NOT NULL FROM grind_jobs WHERE id = $1 FOR NO KEY UPDATE",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use stored_queue <- decode.field(0, decode.string)
      use stored_worker <- decode.field(1, decode.string)
      use stored_worker_version <- decode.field(2, decode.string)
      use state <- decode.field(3, decode.string)
      use attempt_id <- decode.field(4, decode.optional(decode.int))
      use attempt_epoch <- decode.field(5, decode.int)
      use attempt_owner <- decode.field(6, decode.optional(decode.string))
      use lease_expires_at <- decode.field(7, decode.optional(decode.string))
      use stored_output_version <- decode.field(8, decode.string)
      use stored_error_version <- decode.field(
        9,
        decode.optional(decode.string),
      )
      use cancel_requested <- decode.field(10, decode.bool)
      decode.success(#(
        stored_queue,
        stored_worker,
        stored_worker_version,
        state,
        attempt_id,
        attempt_epoch,
        attempt_owner,
        lease_expires_at,
        stored_output_version,
        stored_error_version,
        cancel_requested,
      ))
    })
  select
}

pub fn insert_resolution_query(
  command: ResolutionCommand,
  attempt_id: Int,
  attempt_epoch: Int,
  attempt_owner: String,
) {
  let ResolutionCommand(
    id:,
    queue:,
    worker_id:,
    worker_version:,
    resolution_id:,
    resolved_by:,
    details:,
    decision:,
    target_state:,
    output_version:,
    encoded_output:,
    error_version:,
    encoded_error:,
    ..,
  ) = command
  let insert =
    pog.query(
      "INSERT INTO grind_job_resolutions (queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, resolved_by, details, target_state, payload_version, payload) SELECT job.queue, job.id, $13, $14, $3, job.attempt_id, job.attempt_epoch, job.attempt_owner, job.lease_expires_at, $7, $8, $9, $10, $11, $12::jsonb FROM grind_jobs AS job WHERE job.id = $2 AND job.queue = $1 AND job.worker_id = $15 AND job.worker_version = $16 AND job.state = 'uncertain' AND job.attempt_id = $4 AND job.attempt_epoch = $5 AND job.attempt_owner = $6 AND job.lease_expires_at IS NOT NULL RETURNING resolution_id",
    )
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(resolution_id))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(attempt_epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(decision))
    |> pog.parameter(pog.text(resolved_by))
    |> pog.parameter(pog.text(details))
    |> pog.parameter(pog.text(target_state))
    |> pog.parameter(
      pog.nullable(pog.text, case decision {
        "confirm_success" -> Some(output_version)
        "confirm_business_failure" -> error_version
        _ -> None
      }),
    )
    |> pog.parameter(
      pog.nullable(pog.text, case decision {
        "confirm_success" -> encoded_output
        "confirm_business_failure" -> encoded_error
        _ -> None
      }),
    )
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.returning({
      use resolution_id <- decode.field(0, decode.string)
      decode.success(resolution_id)
    })
  insert
}

pub fn update_resolution_query(
  command: ResolutionCommand,
  attempt_id: Int,
  attempt_epoch: Int,
  attempt_owner: String,
) {
  let ResolutionCommand(
    id:,
    queue:,
    worker_id:,
    worker_version:,
    target_state:,
    encoded_output:,
    encoded_error:,
    failure_description:,
    ..,
  ) = command
  // The job's output and error contract versions are fixed at admission and
  // never written here: an audited resolution only writes the payloads. The
  // codec check in `reconcile_transaction` already proved a confirmed payload
  // matches them, and a replayed job must be re-claimed under the same
  // contract.
  let update =
    pog.query(
      "UPDATE grind_jobs SET state = $1, output = $2::jsonb, error = $3::jsonb, failure_description = $4, attempt_id = CASE WHEN $1 = 'queued' THEN NULL ELSE attempt_id END, attempt_owner = NULL, lease_expires_at = NULL, available_at = CASE WHEN $1 = 'queued' THEN clock_timestamp() ELSE available_at END, finished_at = CASE WHEN $1 IN ("
      <> terminal.states_sql()
      <> ") THEN clock_timestamp() ELSE NULL END WHERE id = $5 AND queue = $6 AND worker_id = $7 AND worker_version = $8 AND state = 'uncertain' AND attempt_id = $9 AND attempt_epoch = $10 AND attempt_owner = $11 RETURNING state",
    )
    |> pog.parameter(pog.text(target_state))
    |> pog.parameter(pog.nullable(pog.text, encoded_output))
    |> pog.parameter(pog.nullable(pog.text, encoded_error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(attempt_epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      decode.success(state)
    })
  update
}
