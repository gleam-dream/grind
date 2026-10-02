//// Constructs admission requests and their stable request fingerprints.

import gleam/bit_array
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import grind/job
import grind/submission
import grind/unique
import grind/worker.{type Worker}

/// The request fingerprint's hash; see `docs/UNIQUENESS-CONTRACT.md`, Decision 9.
@external(erlang, "grind_unique_ffi", "sha256")
fn sha256(data: BitArray) -> BitArray

/// The uniqueness-policy fields a `submit_unique` request carries and a
/// `submit_with_id` request does not. Held as its own type (rather than
/// flattened into `Request`) so `Request.policy: Option(PolicyPart)` alone
/// expresses "does this request have a uniqueness policy" — no separate
/// sentinel key/scope/period/states/action values are ever constructed for
/// the "no policy" case.
pub type PolicyPart {
  PolicyPart(
    key_contract: String,
    encoded_key: String,
    scope: unique.QueueScope,
    period: unique.Period,
    states: unique.States,
    on_conflict: unique.ConflictAction,
  )
}

/// Every value the admission transaction needs, gathered once by `submit`/
/// `submit_plain`. `request_sha256` starts empty and is filled in by
/// `fingerprint`, which takes this whole record as its input — one field
/// list, built once. `policy: None` is `submit_with_id`'s "no policy" case:
/// no uniqueness key, no candidate selection, no domain-wide advisory lock,
/// always an insert (or a replayed receipt) — see the module doc comment.
pub type Request(input, output, error) {
  Request(
    installation: job.Installation,
    submission_id: submission.SubmissionId,
    queue: String,
    worker: Worker(input, output, error),
    worker_id: String,
    worker_version: String,
    input_version: String,
    encoded_input: String,
    output_version: String,
    error_version: Option(String),
    max_attempts: Int,
    availability: submission.Availability,
    policy: Option(PolicyPart),
    request_sha256: BitArray,
  )
}

/// Gathers every value the admission transaction needs. `build_policy`
/// receives the submitting worker's input codec version and encoded input
/// so a policy's key uses the exact same encoding as the request. Returns
/// `submission.InvalidInput` when the input codec or `build_policy` rejects
/// the value, before any storage call.
pub fn build_request(
  installation: job.Installation,
  submission_id: submission.SubmissionId,
  queue: String,
  worker_def: Worker(input, output, error),
  input: input,
  availability: submission.Availability,
  build_policy: fn(String, String) -> Result(Option(PolicyPart), String),
) -> Result(
  Request(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let worker.Metadata(
    id: worker_id,
    worker_version:,
    input_version:,
    output_version:,
    error_version:,
    max_attempts:,
  ) = worker.metadata(worker_def)
  use encoded_input <- result.try(
    worker.encode_input(worker_def, input)
    |> result.map_error(submission.InvalidInput),
  )
  use policy <- result.try(
    build_policy(input_version, encoded_input)
    |> result.map_error(submission.InvalidInput),
  )
  let request =
    Request(
      installation:,
      submission_id:,
      queue:,
      worker: worker_def,
      worker_id:,
      worker_version:,
      input_version:,
      encoded_input:,
      output_version:,
      error_version:,
      max_attempts:,
      availability:,
      policy:,
      request_sha256: <<>>,
    )
  Ok(Request(..request, request_sha256: fingerprint(request)))
}

/// The request fingerprint envelope; see `docs/UNIQUENESS-CONTRACT.md`,
/// Decision 9, and "Admission receipts". Tagged `"grind-unique-request-v1"`
/// when `policy` is `Some` and `"grind-plain-request-v1"` when it is
/// `None` — deliberately different magic strings, so a `SubmissionId`
/// reused between `submit_unique` and `submit_with_id` (or between two
/// calls whose only difference is the presence of a policy) always
/// fingerprint-mismatches and reports `SubmissionConflict`, never a
/// silently-replayed decision from the wrong kind of admission. The
/// policy-specific fields (key, scope, period, states, action) are present
/// in the envelope only when `policy` is `Some`, in the same field order
/// this envelope has always used.
fn fingerprint(request: Request(input, output, error)) -> BitArray {
  let tag = case request.policy {
    Some(_) -> "grind-unique-request-v1"
    None -> "grind-plain-request-v1"
  }
  let policy_fields = case request.policy {
    None -> []
    Some(PolicyPart(
      key_contract:,
      encoded_key:,
      scope:,
      period:,
      states:,
      on_conflict:,
    )) -> {
      let #(period_ms, period_origin) = case unique.period_spec(period) {
        unique.Unbounded -> #(None, None)
        unique.FinitePeriod(ms, from) -> #(
          Some(ms),
          Some(unique.period_origin_label(from)),
        )
      }
      let reschedule_ms = unique.reschedule_target_ms(on_conflict)
      [
        json.string(key_contract),
        json.string(encoded_key),
        json.string(unique.scope_label(scope)),
        json.bool(option.is_some(period_ms)),
        json.nullable(period_ms, json.int),
        json.bool(option.is_some(period_origin)),
        json.nullable(period_origin, json.string),
        json.string(unique.states_label(states)),
        json.string(unique.action_label(on_conflict)),
        json.bool(option.is_some(reschedule_ms)),
        json.nullable(reschedule_ms, json.int),
      ]
    }
  }
  let availability_ms = submission.availability_ms(request.availability)
  let envelope =
    list.flatten([
      [
        json.string(tag),
        json.string(request.queue),
        json.string(request.worker_id),
        json.string(request.worker_version),
        json.string(request.input_version),
        json.string(request.encoded_input),
      ],
      policy_fields,
      [
        json.bool(option.is_some(availability_ms)),
        json.nullable(availability_ms, json.int),
        json.string(request.output_version),
        json.bool(option.is_some(request.error_version)),
        json.nullable(request.error_version, json.string),
        json.int(request.max_attempts),
      ],
    ])
  json.preprocessed_array(envelope)
  |> json.to_string
  |> bit_array.from_string
  |> sha256
}

pub fn pending_submission(
  request: Request(input, output, error),
) -> submission.PendingSubmission(input, output, error) {
  submission.new_pending_submission(
    request.installation,
    request.submission_id,
    request.worker,
    request.request_sha256,
  )
}
