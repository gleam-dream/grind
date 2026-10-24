import gleam/dynamic/decode
import gleam/erlang/process
import gleam/result
import pog

pub fn wait_for_commit_trigger_backend(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND state = 'active' AND wait_event = 'PgSleep' ORDER BY query_start DESC LIMIT 1",
    )
    |> pog.returning({
      use pid <- decode.field(0, decode.int)
      decode.success(pid)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [pid] -> Ok(pid)
        [] ->
          case checks_remaining > 0 {
            False -> Error(Nil)
            True -> {
              process.sleep(10)
              wait_for_commit_trigger_backend(connection, checks_remaining - 1)
            }
          }
        _ -> Error(Nil)
      }
  }
}

pub fn stored_attempt_identity(
  connection: pog.Connection,
  job_id: Int,
) -> Result(#(Int, Int), Nil) {
  pog.query("SELECT attempt_id, attempt_epoch FROM grind_jobs WHERE id = $1")
  |> pog.parameter(pog.int(job_id))
  |> pog.returning({
    use attempt_id <- decode.field(0, decode.int)
    use epoch <- decode.field(1, decode.int)
    decode.success(#(attempt_id, epoch))
  })
  |> pog.execute(on: connection)
  |> result.replace_error(Nil)
  |> result.try(fn(returned) {
    case returned.rows {
      [row] -> Ok(row)
      _ -> Error(Nil)
    }
  })
}

pub fn count_acknowledgements_for_job(
  connection: pog.Connection,
  job_id: Int,
) -> Int {
  let assert Ok(returned) =
    pog.query(
      "SELECT count(*)::bigint FROM grind_job_acknowledgements WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  let assert [count] = returned.rows
  count
}
