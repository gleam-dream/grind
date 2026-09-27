import gleam/dynamic/decode
import pog

// -- Retention (postgres.prune_finished) ------------------------------------

/// Inserts one already-terminal `grind_jobs` row directly (bypassing the
/// typed acknowledgement path entirely, the same way the upgrade harness
/// in `grind/migrations/upgrade_test` seeds legacy rows) with `finished_at` backdated by
/// `finished_ago_ms` — old enough to prune, or not, entirely under the
/// caller's control rather than depending on real wall-clock timing.
pub fn seed_terminal_job(
  connection: pog.Connection,
  queue: String,
  worker_id: String,
  state: String,
  finished_ago_ms: Int,
) -> Int {
  let assert Ok(returned) =
    pog.query(
      "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, state, available_at, finished_at) VALUES ($1, $2, 'v1', 'v1', '1'::jsonb, 'v1', $3, clock_timestamp(), clock_timestamp() - ($4::bigint::double precision * interval '1 millisecond')) RETURNING id",
    )
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(state))
    |> pog.parameter(pog.int(finished_ago_ms))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  let assert [id] = returned.rows
  id
}

/// Inserts one non-terminal `grind_jobs` row directly, `finished_at` left at
/// its natural `NULL` (never backdated — a non-terminal row's `finished_at`
/// is never anything else, by `grind_jobs_finished_at_check`).
pub fn seed_nonterminal_job(
  connection: pog.Connection,
  queue: String,
  worker_id: String,
  state: String,
) -> Int {
  let assert Ok(returned) =
    pog.query(
      "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, state, available_at) VALUES ($1, $2, 'v1', 'v1', '1'::jsonb, 'v1', $3, clock_timestamp()) RETURNING id",
    )
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(state))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  let assert [id] = returned.rows
  id
}

pub fn seed_acknowledgement_receipt(
  connection: pog.Connection,
  queue: String,
  job_id: Int,
  worker_id: String,
  command_id: String,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ($1, $2, $3, $4, 'v1', 1, 1, 'prune-test-owner', 'succeeded', sha256(convert_to('prune-test-proposal', 'UTF8')))",
    )
    |> pog.parameter(pog.text(command_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.execute(on: connection)
  Nil
}

pub fn seed_unique_submission_receipt(
  connection: pog.Connection,
  queue: String,
  job_id: Int,
  worker_id: String,
  submission_id: String,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_unique_submissions (submission_id, queue, worker_id, worker_version, request_sha256, decision, job_id, job_queue, observed_state) VALUES ($1, $2, $3, 'v1', sha256(convert_to('prune-test-request', 'UTF8')), 'inserted', $4, $2, 'succeeded')",
    )
    |> pog.parameter(pog.text(submission_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  Nil
}

pub fn seed_resolution_receipt(
  connection: pog.Connection,
  queue: String,
  job_id: Int,
  worker_id: String,
  resolution_id: String,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ($1, $2, $3, 'v1', $4, 1, 1, 'prune-test-owner', clock_timestamp(), 'confirm_success', 'succeeded', 'prune-test', 'prune cascade probe')",
    )
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(resolution_id))
    |> pog.execute(on: connection)
  Nil
}

pub fn job_row_exists(connection: pog.Connection, id: Int) -> Bool {
  let assert Ok(returned) =
    pog.query("SELECT count(*) = 1 FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use exists <- decode.field(0, decode.bool)
      decode.success(exists)
    })
    |> pog.execute(on: connection)
  let assert [exists] = returned.rows
  exists
}
