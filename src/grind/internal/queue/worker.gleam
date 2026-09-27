//// Runs one claimed worker in its supervised temporary actor.
//// The coordinator supplies the typed completion message constructor.

import gleam/erlang/process
import gleam/otp/actor
import grind/internal/attempt
import grind/worker

pub type WorkerMessage {
  StartAttempt
  StopWorker
}

pub type WorkerRequest(message) {
  WorkerRequest(
    queue_subject: process.Subject(message),
    claimed: attempt.ClaimedJob,
    on_return: fn(Int, Int, Int, worker.Execution) -> message,
  )
}

pub opaque type WorkerState(message) {
  WorkerState(
    queue_subject: process.Subject(message),
    claimed: attempt.ClaimedJob,
    on_return: fn(Int, Int, Int, worker.Execution) -> message,
  )
}

pub fn worker_actor(
  request: WorkerRequest(message),
) -> actor.Builder(
  WorkerState(message),
  WorkerMessage,
  process.Subject(WorkerMessage),
) {
  let WorkerRequest(queue_subject:, claimed:, on_return:) = request
  actor.new(WorkerState(queue_subject:, claimed:, on_return:))
  |> actor.on_message(fn(state, message) {
    case message {
      StartAttempt -> {
        let #(id, attempt_id, epoch) = attempt.claim_identity(state.claimed)
        let execution = attempt.execute_claim(state.claimed)
        process.send(
          state.queue_subject,
          state.on_return(id, attempt_id, epoch, execution),
        )
        actor.continue(state)
      }
      StopWorker -> actor.stop()
    }
  })
}
