//// A codec's encoder may reject a value. These tests pin what the erased
//// worker proposes when the handler's output or error is rejected after
//// the handler ran: a terminal `ExecutedUnencodable`, never a retry and
//// never a crash.

import gleam/dynamic/decode
import gleam/json
import gleeunit/should
import grind/internal/worker
import grind/support/worker_failure.{
  type LookupFailure, AccountMissing, decode_lookup_failure,
  encode_lookup_failure,
}

/// Accepts only non-negative integers, like a json_blueprint codec with an
/// `integer_between` refinement.
fn non_negative(value: Int) -> Result(json.Json, String) {
  case value >= 0 {
    True -> Ok(json.int(value))
    False -> Error("must be at least 0")
  }
}

fn short_text(value: String) -> Result(json.Json, String) {
  case value {
    "too long" -> Error("longer than 4 bytes")
    _ -> Ok(json.string(value))
  }
}

fn rejecting_error(error: LookupFailure) -> Result(json.Json, String) {
  case error {
    AccountMissing(id) if id < 0 -> Error("account id must be at least 0")
    _ -> Ok(encode_lookup_failure(error))
  }
}

fn definition(
  handler: fn(Int) -> Result(String, LookupFailure),
) -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input) =
    worker.codec("encoder-input-v1", non_negative, decode.int)
  let assert Ok(output) =
    worker.codec("encoder-output-v1", short_text, decode.string)
  let assert Ok(error) =
    worker.codec("encoder-error-v1", rejecting_error, decode_lookup_failure())
  let assert Ok(definition) =
    worker.define_with_error_codec(
      "encoder.probe",
      "v1",
      input,
      output,
      error,
      handler,
    )
  definition
}

fn context(current_attempt: Int, max_attempts: Int) -> worker.Context {
  worker.synthetic_context(
    job_id: 1,
    attempt: current_attempt,
    max_attempts:,
    snooze_count: 0,
    queue: "default",
  )
}

pub fn infallible_adapts_a_total_encoder_test() {
  let encode = worker.infallible(json.int)
  encode(7) |> should.equal(Ok(json.int(7)))
}

pub fn encode_input_reports_the_codec_reason_test() {
  let probe = definition(fn(_) { Ok("ok") })
  worker.encode_input(probe, 3) |> should.equal(Ok("3"))
  worker.encode_input(probe, -1) |> should.equal(Error("must be at least 0"))
}

pub fn accepted_output_is_proposed_as_success_test() {
  definition(fn(_) { Ok("fine") })
  |> worker.execute_encoded("encoder-input-v1", "1", context(1, 3), 1_048_576)
  |> should.equal(worker.ExecutedSuccess("encoder-output-v1", "\"fine\""))
}

pub fn rejected_output_is_proposed_as_unencodable_test() {
  definition(fn(_) { Ok("too long") })
  |> worker.execute_encoded("encoder-input-v1", "1", context(1, 3), 1_048_576)
  |> should.equal(worker.ExecutedUnencodable(
    worker.OutputCodec,
    "longer than 4 bytes",
  ))
}

pub fn rejected_error_with_retries_left_is_not_retried_test() {
  definition(fn(_) { Error(AccountMissing(-5)) })
  |> worker.execute_encoded("encoder-input-v1", "1", context(1, 3), 1_048_576)
  |> should.equal(worker.ExecutedUnencodable(
    worker.ErrorCodec,
    "account id must be at least 0",
  ))
}

pub fn rejected_error_on_the_last_attempt_is_unencodable_test() {
  definition(fn(_) { Error(AccountMissing(-5)) })
  |> worker.execute_encoded("encoder-input-v1", "1", context(3, 3), 1_048_576)
  |> should.equal(worker.ExecutedUnencodable(
    worker.ErrorCodec,
    "account id must be at least 0",
  ))
}

pub fn accepted_error_keeps_the_retry_disposition_test() {
  let assert worker.ExecutedRetryable(
    error_version,
    encoded_error,
    _description,
    _delay,
  ) =
    definition(fn(_) { Error(AccountMissing(5)) })
    |> worker.execute_encoded("encoder-input-v1", "1", context(1, 3), 1_048_576)
  error_version |> should.be_some
  encoded_error |> should.be_some
}

pub fn unencodable_descriptions_name_the_codec_test() {
  worker.unencodable_description(worker.OutputCodec, "bad")
  |> should.equal("output codec rejected the handler's output: bad")
  worker.unencodable_description(worker.ErrorCodec, "bad")
  |> should.equal("error codec rejected the handler's error: bad")
}
