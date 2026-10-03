import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{type Option}
import gleam/result
import grind/internal/job
import grind/internal/postgres
import pog

pub fn wait_for_succeeded(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  checks_remaining: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(job.Succeeded) -> True
    _ ->
      case checks_remaining > 0 {
        False -> False
        True -> {
          process.sleep(25)
          wait_for_succeeded(database, handle, checks_remaining - 1)
        }
      }
  }
}

pub fn attempt_snapshot(
  connection: pog.Connection,
  id: Int,
) -> Result(#(Int, Int, Int, Option(String)), Nil) {
  pog.query(
    "SELECT attempt_id, attempt_epoch, (extract(epoch FROM lease_expires_at) * 1000)::bigint, attempt_owner FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use attempt_id <- decode.field(0, decode.int)
    use attempt_epoch <- decode.field(1, decode.int)
    use lease_expires_at <- decode.field(2, decode.int)
    use attempt_owner <- decode.field(3, decode.optional(decode.string))
    decode.success(#(attempt_id, attempt_epoch, lease_expires_at, attempt_owner))
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [snapshot] -> Ok(snapshot)
      _ -> Error(Nil)
    }
  })
}

pub fn job_finished_at_ms(
  connection: pog.Connection,
  id: Int,
) -> Result(Int, Nil) {
  pog.query(
    "SELECT (extract(epoch FROM finished_at) * 1000)::bigint FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use finished_at <- decode.field(0, decode.int)
    decode.success(finished_at)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [finished_at] -> Ok(finished_at)
      _ -> Error(Nil)
    }
  })
}

pub fn wait_for_job_state(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  expected: job.State,
  remaining_checks: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(state) ->
      case state == expected, remaining_checks > 0 {
        True, _ -> True
        False, True -> {
          process.sleep(20)
          wait_for_job_state(database, handle, expected, remaining_checks - 1)
        }
        False, False -> False
      }
    Error(_) -> False
  }
}

/// Like `wait_for_job_state` above, but a transient `postgres.state` error
/// counts as "not yet" and keeps retrying instead of failing the wait
/// outright. Used where the test itself just closed or killed a connection
/// on this same pool moments earlier, so an immediate read can genuinely
/// error while the pool recovers — that is not evidence the state will never
/// reach `expected`, only that this one read failed.
pub fn wait_for_job_state_tolerating_errors(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  expected: job.State,
  remaining_checks: Int,
) -> Bool {
  let matches = case postgres.state(database, handle) {
    Ok(state) -> state == expected
    Error(_) -> False
  }
  case matches, remaining_checks > 0 {
    True, _ -> True
    False, True -> {
      process.sleep(20)
      wait_for_job_state_tolerating_errors(
        database,
        handle,
        expected,
        remaining_checks - 1,
      )
    }
    False, False -> False
  }
}

/// Retries a query on any `Error`, tolerating the same kind of transient
/// pool-recovery failure `wait_for_job_state_tolerating_errors` above
/// tolerates for a state read — for a plain one-shot read (`arguments`,
/// `outcome`) taken moments after this test's own connection kill, where
/// there is no polling loop already absorbing that latency.
pub fn retry_transient_query(
  attempt: fn() -> Result(a, b),
  remaining: Int,
) -> Result(a, b) {
  case attempt() {
    Ok(value) -> Ok(value)
    Error(error) ->
      case remaining > 0 {
        True -> {
          process.sleep(50)
          retry_transient_query(attempt, remaining - 1)
        }
        False -> Error(error)
      }
  }
}
