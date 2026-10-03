import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{Some}
import gleam/result
import grind/internal/consumer as queue
import pog

pub fn attempt_count_for(
  connection: pog.Connection,
  id: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query("SELECT attempt_count FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [count] -> Ok(count)
        _ -> Error(Nil)
      }
  }
}

pub fn await_renewal_status(
  consumer: queue.Consumer,
  expected: queue.RenewalStatus,
  checks_remaining: Int,
) -> Bool {
  case queue.renewal_status(consumer) {
    Ok(Some(actual)) ->
      case actual == expected {
        True -> True
        False -> retry_renewal_status(consumer, expected, checks_remaining)
      }
    _ -> retry_renewal_status(consumer, expected, checks_remaining)
  }
}

pub fn retry_renewal_status(
  consumer: queue.Consumer,
  expected: queue.RenewalStatus,
  checks_remaining: Int,
) -> Bool {
  case checks_remaining > 0 {
    False -> False
    True -> {
      process.sleep(10)
      await_renewal_status(consumer, expected, checks_remaining - 1)
    }
  }
}

pub fn lease_expiration(
  connection: pog.Connection,
  id: Int,
) -> Result(Int, Nil) {
  pog.query(
    "SELECT (extract(epoch FROM lease_expires_at) * 1000)::bigint FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use expiry <- decode.field(0, decode.int)
    decode.success(expiry)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [expiry] -> Ok(expiry)
      _ -> Error(Nil)
    }
  })
}

pub fn await_later_lease_expiry(
  connection: pog.Connection,
  id: Int,
  threshold: Int,
  remaining_checks: Int,
) -> Bool {
  case lease_expiration(connection, id) {
    Ok(expiry) ->
      case expiry > threshold, remaining_checks > 0 {
        True, _ -> True
        False, True -> {
          process.sleep(20)
          await_later_lease_expiry(
            connection,
            id,
            threshold,
            remaining_checks - 1,
          )
        }
        False, False -> False
      }
    Error(Nil) -> False
  }
}
