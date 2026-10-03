import grind/internal/postgres
import grind/internal/submission
import grind/internal/worker

// -- Decision B (2026-09-25): retry-safe plain submit (`submit_with_id`) ----
//
// `docs/RELEASE-READINESS.md`, "Retry-safe plain submit", and
// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts". Plain `submit`/
// `submit_at` have no request identity, so a caller that retries after
// `SubmitQueryFailed` (which may itself have committed) risks a duplicate
// row. `submit_with_id` reuses `submit_unique`'s own admission receipt,
// request fingerprint, and reconciliation machinery through a "no policy"
// path in `grind/internal/unique_admission` — no uniqueness key, no
// candidate selection, no conflict decision, always `Inserted` once
// resolved.

/// `submit_with_id` under `Immediately`, the shape the submit-with-id and admitted-observation tests need.
pub fn submit_with_id_immediately(
  database: postgres.Database,
  queue: String,
  id_text: String,
  worker_def: worker.Worker(input, output, error),
  input: input,
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let assert Ok(submission) = submission.submission_id(id_text)
  postgres.submit_with_id(
    database,
    queue,
    submission,
    worker_def,
    input,
    submission.Immediately,
  )
}
