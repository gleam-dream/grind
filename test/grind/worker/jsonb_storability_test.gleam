//// PostgreSQL `jsonb` refuses the `\u0000` escape (SQLSTATE `22P05`), and
//// `gleam_json` encodes a NUL character in any string as exactly that
//// escape. These tests pin that Grind refuses such a payload itself, before
//// it reaches storage: at admission as `InvalidInput`, and after the
//// handler ran as a terminal `ExecutedUnencodable` proposal.

import gleam/dynamic/decode
import gleam/json
import gleam/option.{None, Some}
import gleeunit/should
import grind/internal/job
import grind/internal/submission
import grind/internal/unique
import grind/internal/unique_admission/request
import grind/internal/worker

fn text_codec(version: String) -> worker.Codec(String) {
  let assert Ok(codec) =
    worker.codec(version, worker.infallible(json.string), decode.string)
  codec
}

fn definition(
  handler: fn(String) -> Result(String, String),
) -> worker.Worker(String, String, String) {
  let assert Ok(definition) =
    worker.define_with_error_codec(
      "jsonb.probe",
      "v1",
      text_codec("jsonb-input-v1"),
      text_codec("jsonb-output-v1"),
      text_codec("jsonb-error-v1"),
      handler,
    )
  definition
}

fn context() -> worker.Context {
  worker.synthetic_context(
    job_id: 1,
    attempt: 1,
    max_attempts: 3,
    snooze_count: 0,
    queue: "default",
  )
}

fn build(input: String) {
  build_with(input, 1_048_576, fn(_, _) { Ok(None) })
}

fn build_with(
  input: String,
  max_payload_bytes: Int,
  build_policy: fn(String, String) ->
    Result(option.Option(request.PolicyPart), String),
) {
  let assert Ok(id) = submission.submission_id("jsonb-probe")
  request.build_request(
    job.Installation(
      database_oid: 1,
      schema: "public",
      cluster_identifier: None,
    ),
    id,
    "default",
    definition(fn(text) { Ok(text) }),
    input,
    submission.Immediately,
    None,
    max_payload_bytes,
    build_policy,
  )
}

fn selected_key(encoded_key: String) {
  fn(_, _) {
    Ok(
      Some(request.PolicyPart(
        key_contract: "selected:probe:jsonb-key-v1",
        encoded_key:,
        scope: unique.WithinQueue,
        period: unique.while_retained(),
        states: unique.Incomplete,
        on_conflict: unique.KeepExisting,
      )),
    )
  }
}

pub fn an_oversized_input_with_a_nul_is_invalid_not_too_large_test() {
  let assert Error(submission.InvalidInput(reason)) =
    build_with("a\u{0}bcdefgh", 4, fn(_, _) { Ok(None) })
  reason |> should.equal(worker.jsonb_unstorable_reason)
}

pub fn an_oversized_input_without_a_nul_is_too_large_test() {
  build_with("abcdefgh", 4, fn(_, _) { Ok(None) })
  |> should.equal(Error(submission.PayloadTooLarge(bytes: 10, limit: 4)))
}

pub fn a_nul_in_a_selected_unique_key_is_refused_test() {
  let assert Error(submission.InvalidInput(reason)) =
    build_with("plain", 1_048_576, selected_key("\"a\\u0000b\""))
  reason
  |> should.equal("the unique key: " <> worker.jsonb_unstorable_reason)
}

pub fn a_storable_selected_unique_key_is_admitted_test() {
  let assert Ok(_) =
    build_with("plain", 1_048_576, selected_key("\"a\\\\u0000b\""))
}

pub fn admission_refuses_a_nul_in_the_input_test() {
  let assert Error(submission.InvalidInput(reason)) = build("a\u{0}b")
  reason
  |> should.equal(worker.jsonb_unstorable_reason)
}

pub fn admission_accepts_an_escaped_backslash_before_u0000_test() {
  // The text `\u0000` (backslash, `u`, four zeros) encodes as `\\u0000`,
  // which jsonb stores; only a NUL character is refused.
  let assert Ok(_) = build("a\\u0000b")
}

pub fn nul_in_handler_output_is_proposed_as_unencodable_test() {
  definition(fn(_) { Ok("a\u{0}b") })
  |> worker.execute_encoded("jsonb-input-v1", "\"x\"", context(), 1_048_576)
  |> should.equal(worker.ExecutedUnencodable(
    worker.OutputCodec,
    worker.jsonb_unstorable_reason,
  ))
}

pub fn nul_in_handler_error_is_proposed_as_unencodable_test() {
  definition(fn(_) { Error("a\u{0}b") })
  |> worker.execute_encoded("jsonb-input-v1", "\"x\"", context(), 1_048_576)
  |> should.equal(worker.ExecutedUnencodable(
    worker.ErrorCodec,
    worker.jsonb_unstorable_reason,
  ))
}

pub fn jsonb_storable_distinguishes_escapes_test() {
  worker.jsonb_storable("\"plain\"") |> should.equal(Ok(Nil))
  worker.jsonb_storable("\"a\\\\u0000b\"") |> should.equal(Ok(Nil))
  worker.jsonb_storable("\"a\\\\\\u0000b\"")
  |> should.equal(Error(worker.jsonb_unstorable_reason))
  worker.jsonb_storable("\"a\\u0000b\"")
  |> should.equal(Error(worker.jsonb_unstorable_reason))
  worker.jsonb_storable("\"a\u{0}b\"")
  |> should.equal(Error(worker.jsonb_unstorable_reason))
}
