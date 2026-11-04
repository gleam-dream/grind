//// Caller-chosen text that Grind stores in `text` columns: a job id, a
//// per-job queue, a correlation, and an operator resolution's id, `by`
//// and details. PostgreSQL `text` refuses a NUL (SQLSTATE `22021`), which
//// inside a borrowed transaction would abort the caller's work. These tests
//// pin that Grind refuses such text itself, before any SQL runs.

import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option, None, Some}
import gleeunit/should
import grind/internal/job
import grind/internal/postgres/resolution
import grind/internal/submission
import grind/internal/unique_admission/request
import grind/internal/worker
import grind/testing
import grind/worker as public_worker

fn text_codec(version: String) -> worker.Codec(String) {
  let assert Ok(codec) =
    worker.codec(version, worker.infallible(json.string), decode.string)
  codec
}

fn definition() -> worker.Worker(String, String, String) {
  let assert Ok(definition) =
    worker.define_with_error_codec(
      "text.probe",
      "v1",
      text_codec("text-input-v1"),
      text_codec("text-output-v1"),
      text_codec("text-error-v1"),
      fn(text) { Ok(text) },
    )
  definition
}

fn installation() -> job.Installation {
  job.Installation(database_oid: 1, schema: "public", cluster_identifier: None)
}

fn build(id: String, queue: String, correlation: Option(String)) {
  let assert Ok(id) = submission.submission_id(id)
  request.build_request(
    installation(),
    id,
    queue,
    definition(),
    "input",
    submission.Immediately,
    correlation,
    1_048_576,
    fn(_, _) { Ok(None) },
  )
}

fn refused(reason: String) {
  Error(submission.InvalidInput(reason))
}

pub fn ordinary_text_is_admitted_test() {
  let assert Ok(_) = build("order:42", "default", Some("trace-1"))
}

pub fn a_nul_in_the_job_id_is_refused_test() {
  build("order\u{0}42", "default", None)
  |> should.equal(refused(worker.control_character_reason("the job id")))
}

pub fn a_control_character_in_the_job_id_is_refused_test() {
  build("order\t42", "default", None)
  |> should.equal(refused(worker.control_character_reason("the job id")))
  build("order\u{7f}42", "default", None)
  |> should.equal(refused(worker.control_character_reason("the job id")))
}

pub fn a_control_character_in_the_queue_is_refused_test() {
  build("order:42", "de\u{0}fault", None)
  |> should.equal(refused(worker.control_character_reason("the job's queue")))
  build("order:42", "de\nfault", None)
  |> should.equal(refused(worker.control_character_reason("the job's queue")))
}

pub fn a_control_character_in_the_correlation_is_refused_test() {
  build("order:42", "default", Some("trace\u{0}1"))
  |> should.equal(refused(worker.control_character_reason("the correlation")))
}

fn handle() -> job.JobHandle(String, String, String) {
  job.JobHandle(
    id: 1,
    installation: installation(),
    queue: "default",
    worker_id: "text.probe",
    worker_version: "v1",
    input: text_codec("text-input-v1"),
    output: text_codec("text-output-v1"),
    error: Some(text_codec("text-error-v1")),
  )
}

fn prepare(id: String, by: String, details: String) {
  resolution.prepare(
    handle(),
    resolution.Request(
      resolution_id: id,
      resolved_by: by,
      details:,
      decision: resolution.AuthorizeReplay,
    ),
  )
}

pub fn ordinary_resolution_text_is_accepted_test() {
  let assert Ok(_) =
    prepare("resolution-1", "on-call", "checked upstream\nline two")
}

pub fn a_control_character_in_the_resolution_id_is_refused_test() {
  let assert Error(resolution.ResolutionInvalidValue(reason)) =
    prepare("resolution\u{0}1", "on-call", "checked")
  reason |> should.equal(worker.control_character_reason("the resolution id"))
}

pub fn a_control_character_in_the_resolver_is_refused_test() {
  let assert Error(resolution.ResolutionInvalidValue(reason)) =
    prepare("resolution-1", "on\u{1}call", "checked")
  reason |> should.equal(worker.control_character_reason("the resolver"))
}

pub fn a_nul_in_the_resolution_details_is_refused_test() {
  let assert Error(resolution.ResolutionInvalidValue(reason)) =
    prepare("resolution-1", "on-call", "checked\u{0}upstream")
  reason |> should.equal(worker.nul_text_reason("the resolution details"))
}

fn echo_worker() -> public_worker.Worker(String, String, Nil) {
  public_worker.new(
    "text.testing",
    input: text_codec("text-input-v1"),
    output: text_codec("text-output-v1"),
    perform: fn(text) { Ok(text) },
  )
}

pub fn testing_perform_refuses_a_nul_input_test() {
  testing.perform(echo_worker(), "a\u{0}b")
  |> should.equal(Error(testing.InputRejected(worker.jsonb_unstorable_reason)))
}

pub fn testing_perform_refuses_a_nul_output_test() {
  let appending =
    public_worker.new(
      "text.testing.appending",
      input: text_codec("text-input-v1"),
      output: text_codec("text-output-v1"),
      perform: fn(text) { Ok(text <> "\u{0}") },
    )
  testing.perform(appending, "a")
  |> should.equal(Error(testing.OutputRejected(worker.jsonb_unstorable_reason)))
}
