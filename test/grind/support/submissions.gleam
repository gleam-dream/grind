import gleam/dynamic/decode
import gleam/int
import gleam/json
import grind/postgres
import grind/submission
import grind/unique
import grind/worker
import pog

/// The `Int` input / `String` output (`int.to_string`) worker shape most
/// uniqueness tests in this suite use. `id` should already carry a
/// `unique_test_suffix()`.
pub fn unique_test_worker(id: String) -> worker.Worker(Int, String, e) {
  unique_test_worker_versioned(id, "v1")
}

/// Like `unique_test_worker`, but with a caller-chosen worker version — used
/// by the cross-version quarantine test in `grind/observations/claim_quarantine_test`, which registers two different
/// versions of the same worker id against two different consumers.
pub fn unique_test_worker_versioned(
  id: String,
  version: String,
) -> worker.Worker(Int, String, e) {
  let assert Ok(input_codec) =
    worker.codec(id <> "-input-" <> version, json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(id <> "-output-" <> version, json.string, decode.string)
  let assert Ok(worker_def) =
    worker.define(id, version, input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  worker_def
}

/// `submit_unique` under the `Immediately`/`KeepExisting` case the uniqueness admission tests need (none of these increments exercise rescheduling).
pub fn submit_keep_existing(
  database: postgres.Database,
  queue: String,
  id_text: String,
  worker_def: worker.Worker(input, output, error),
  input: input,
  policy: unique.Policy(input),
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let assert Ok(submission) = submission.submission_id(id_text)
  postgres.submit_unique(
    database,
    queue,
    submission,
    worker_def,
    input,
    submission.Immediately,
    policy,
    unique.KeepExisting,
  )
}

pub fn unique_receipt_exists(
  connection: pog.Connection,
  submission_id_text: String,
) -> Bool {
  let assert Ok(returned) =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM grind_unique_submissions WHERE submission_id = $1)",
    )
    |> pog.parameter(pog.text(submission_id_text))
    |> pog.returning({
      use exists <- decode.field(0, decode.bool)
      decode.success(exists)
    })
    |> pog.execute(on: connection)
  let assert [exists] = returned.rows
  exists
}
