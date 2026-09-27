import gleam/erlang/process
import grind/job
import grind/postgres
import grind/queue

pub fn await_state(
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
          await_state(database, handle, expected, remaining_checks - 1)
        }
        False, False -> False
      }
    Error(_) -> False
  }
}

/// Bounded polling for a manual consumer's next claimable job. Used to wait
/// for a real retry delay to become due without sleeping as the assertion
/// itself.
pub fn await_claim(
  consumer: queue.Consumer,
  remaining_checks: Int,
) -> Result(Bool, queue.ProcessError) {
  case queue.process_one(consumer) {
    Ok(True) -> Ok(True)
    Ok(False) ->
      case remaining_checks > 0 {
        True -> {
          process.sleep(20)
          await_claim(consumer, remaining_checks - 1)
        }
        False -> Ok(False)
      }
    Error(error) -> Error(error)
  }
}

pub fn await_pruned(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  remaining_checks: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Error(postgres.JobNotFound) -> True
    _ ->
      case remaining_checks > 0 {
        True -> {
          process.sleep(20)
          await_pruned(database, handle, remaining_checks - 1)
        }
        False -> False
      }
  }
}
