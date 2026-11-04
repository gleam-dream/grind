//// PostgreSQL `jsonb` refuses the `\u0000` escape (SQLSTATE `22P05`), and
//// `gleam_json` encodes a NUL character in any string as exactly that
//// escape. These tests pin that Grind refuses such a payload itself, before
//// it reaches storage: at admission as `InvalidInput`, and after the
//// handler ran as a terminal `ExecutedUnencodable` proposal.

import gleam/dynamic/decode
import gleam/json
import gleam/option.{None}
import gleeunit/should
import grind/internal/job
import grind/internal/submission
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
    1_048_576,
    fn(_, _) { Ok(None) },
  )
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
