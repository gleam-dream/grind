import gleam/dynamic/decode
import gleam/erlang/process
import gleam/result
import grind/internal/unique_admission
import grind/job
import grind/postgres
import grind/unique
import grind/worker
import pog

pub fn await_claim_waiting_on_advisory(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Bool {
  let waiting =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND query LIKE 'WITH candidate AS (%')",
    )
    |> pog.returning({
      use waiting <- decode.field(0, decode.bool)
      decode.success(waiting)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [waiting] -> Ok(waiting)
        _ -> Error(Nil)
      }
    })
  case waiting {
    Ok(True) -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_claim_waiting_on_advisory(connection, checks_remaining - 1)
        }
        False -> False
      }
  }
}

/// Polls (bounded) until the number of other active backends waiting on an
/// **advisory** lock equals `advisory_target` and the number waiting on a
/// **row** lock (`transactionid`, PostgreSQL's wait event for a tuple lock
/// held by another transaction) equals `transactionid_target` — both from
/// one query over one snapshot, the same discipline `await_overlap_shape`
/// uses. Unlike `await_overlap_shape`, this does not key off query
/// text: the two waiters here run byte-identical SQL (the same
/// acknowledgement command retried), so only `wait_event` tells them apart.
pub fn await_lock_wait_counts(
  connection: pog.Connection,
  advisory_target: Int,
  transactionid_target: Int,
  checks_remaining: Int,
) -> Bool {
  let counts =
    pog.query(
      "SELECT count(*) FILTER (WHERE wait_event = 'advisory'), count(*) FILTER (WHERE wait_event = 'transactionid') FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'Lock'",
    )
    |> pog.returning({
      use advisory <- decode.field(0, decode.int)
      use transactionid <- decode.field(1, decode.int)
      decode.success(#(advisory, transactionid))
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [pair] -> Ok(pair)
        _ -> Error(Nil)
      }
    })
  case counts {
    Ok(#(advisory, transactionid))
      if advisory == advisory_target && transactionid == transactionid_target
    -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_lock_wait_counts(
            connection,
            advisory_target,
            transactionid_target,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

/// Polls (bounded) until the number of other active backends whose query
/// text matches `insert_like` equals `insert_target`, and the number
/// matching `lock_like` equals `lock_target`, **at the same instant** — both
/// counted from one query so the two figures are never read from two
/// different moments in time. Used to prove the forced-overlap barrier's
/// exact expected shape (one backend blocked inserting behind the test's
/// own held trigger lock, N others blocked acquiring the real uniqueness
/// domain lock) rather than inferring it from timing alone, the same
/// discipline `await_claim_waiting_on_advisory` above uses for the
/// claim-overlap barrier.
pub fn await_overlap_shape(
  connection: pog.Connection,
  insert_like: String,
  lock_like: String,
  insert_target: Int,
  lock_target: Int,
  checks_remaining: Int,
) -> Bool {
  let counts =
    pog.query(
      "SELECT count(*) FILTER (WHERE query LIKE $1), count(*) FILTER (WHERE query LIKE $2) FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory'",
    )
    |> pog.parameter(pog.text(insert_like))
    |> pog.parameter(pog.text(lock_like))
    |> pog.returning({
      use inserting <- decode.field(0, decode.int)
      use locking <- decode.field(1, decode.int)
      decode.success(#(inserting, locking))
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [pair] -> Ok(pair)
        _ -> Error(Nil)
      }
    })
  case counts {
    Ok(#(inserting, locking))
      if inserting == insert_target && locking == lock_target
    -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_overlap_shape(
            connection,
            insert_like,
            lock_like,
            insert_target,
            lock_target,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

/// The exact query text `grind/internal/unique_admission`'s `insert_job`
/// issues, as a `LIKE` prefix for `await_overlap_shape`/`pg_stat_activity`.
pub const unique_insert_query_like = "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version%"

/// The exact query text `grind/internal/unique_admission`'s `acquire_lock`
/// issues, as a `LIKE` prefix for `await_overlap_shape`/`pg_stat_activity`.
pub const unique_domain_lock_query_like = "SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended%"

/// The domain-wide uniqueness advisory lock's own SQL and parameters (`@internal
/// unique_admission.lock_key_sql`), built from a worker/input pair exactly the
/// way `grind/internal/unique_admission`'s `acquire_lock` would for a real
/// `submit_unique` call against that worker and input under `full_input()`
/// — used by the contention tests in `grind/unique/lock_contention_test` to hold, from the test
/// itself, the *same* lock a concurrent `submit_unique` call would need.
pub fn unique_domain_lock_query(
  database: postgres.Database,
  worker_def: worker.Worker(input, output, error),
  input: input,
) -> pog.Query(Bool) {
  let worker_meta = worker.metadata(worker_def)
  let encoded_input = worker.encode_input(worker_def, input)
  let #(key_contract, encoded_key) =
    unique.key_material(
      unique.full_input(),
      input,
      worker_meta.input_version,
      encoded_input,
    )
  unique_admission.lock_query(
    job.installation_schema(postgres.installation(database)),
    worker_meta.id,
    worker_meta.worker_version,
    key_contract,
    encoded_key,
  )
}
