import gleam/dynamic/decode
import gleam/erlang/process
import gleam/result
import grind/internal/consumer as queue
import grind/support/concurrency.{type LeaseCommand, ReleaseAttempt}
import pog

pub fn wait_for_shutdown_state(
  consumer: queue.Consumer,
  checks_remaining: Int,
) -> Bool {
  case queue.shutdown_state(consumer) {
    Ok(True) -> True
    Ok(False) ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          wait_for_shutdown_state(consumer, checks_remaining - 1)
        }
        False -> False
      }
    Error(_) -> False
  }
}

pub fn database_time_ms(connection: pog.Connection) -> Result(Int, Nil) {
  pog.query("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
  |> pog.returning({
    use now <- decode.field(0, decode.int)
    decode.success(now)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [now] -> Ok(now)
      _ -> Error(Nil)
    }
  })
}

pub fn await_new_coordinator_pid(
  consumer: queue.Consumer,
  previous: process.Pid,
  checks_remaining: Int,
) -> Result(process.Pid, Nil) {
  case queue.coordinator_pid(consumer) {
    Ok(pid) if pid != previous -> Ok(pid)
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          await_new_coordinator_pid(consumer, previous, checks_remaining - 1)
        }
        False -> Error(Nil)
      }
  }
}

pub fn settle_attempt(
  finished: process.Subject(Nil),
  release: process.Subject(LeaseCommand),
  reply: process.Subject(Result(Bool, queue.ProcessError)),
) -> Nil {
  case process.receive(finished, within: 0) {
    Ok(Nil) -> Nil
    Error(Nil) -> {
      process.send(release, ReleaseAttempt)
      let _ = process.receive(reply, within: 5000)
      Nil
    }
  }
}

pub fn database_time_milliseconds(connection: pog.Connection) -> Int {
  let assert Ok(sample) =
    pog.query(
      "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
    )
    |> pog.returning({
      use milliseconds <- decode.field(0, decode.int)
      decode.success(milliseconds)
    })
    |> pog.execute(on: connection)
  let assert [milliseconds] = sample.rows
  milliseconds
}
