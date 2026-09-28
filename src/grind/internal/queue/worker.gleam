//// Executes a handler once and owns its acknowledgement/reconciliation.
//// A finished proposal remains here until settled; it never invokes the handler again.

import gleam/erlang/process
import gleam/otp/actor
import grind/internal/attempt
import grind/internal/queue/renewer
import grind/postgres
import grind/worker

pub type WorkerMessage {
  StartAttempt
  RetryAcknowledgement
  StopWorker
}

pub type WorkerRequest(message) {
  WorkerRequest(
    queue_subject: process.Subject(message),
    claimed: attempt.ClaimedJob,
    on_complete: fn(Int, Int, Int, Result(Bool, postgres.QueueRunError)) ->
      message,
    database: postgres.Database,
    queue: String,
    owner: String,
    renewer: process.Subject(renewer.Message),
    retry_interval_ms: Int,
    automatic: Bool,
  )
}

type Phase {
  Ready
  WaitingForAcknowledgement(worker.Execution)
  Finished
}

pub opaque type WorkerState(message) {
  WorkerState(
    request: WorkerRequest(message),
    subject: process.Subject(WorkerMessage),
    phase: Phase,
  )
}

pub fn worker_actor(
  request: WorkerRequest(message),
) -> actor.Builder(
  WorkerState(message),
  WorkerMessage,
  process.Subject(WorkerMessage),
) {
  actor.new_with_initialiser(1000, fn(subject) {
    Ok(
      actor.initialised(WorkerState(request:, subject:, phase: Ready))
      |> actor.returning(subject),
    )
  })
  |> actor.on_message(fn(state, message) {
    case message, state.phase {
      StartAttempt, Ready -> {
        process.send(
          state.request.renewer,
          renewer.Track(state.request.claimed, process.self()),
        )
        let execution = attempt.execute_claim(state.request.claimed)
        let #(_, attempt_id, epoch) =
          attempt.claim_identity(state.request.claimed)
        process.send(
          state.request.renewer,
          renewer.AwaitAcknowledgement(attempt_id, epoch),
        )
        acknowledge(state, execution)
      }
      RetryAcknowledgement, WaitingForAcknowledgement(execution) ->
        acknowledge(state, execution)
      StopWorker, _ -> actor.stop()
      _, _ -> actor.continue(state)
    }
  })
}

fn acknowledge(
  state: WorkerState(message),
  execution: worker.Execution,
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let request = state.request
  let result =
    attempt.acknowledge(
      request.database,
      request.queue,
      request.owner,
      request.claimed,
      execution,
    )
  case request.automatic, result {
    True, Error(postgres.QueueAckUnknown(_, proposed)) -> retry(state, proposed)
    True, Error(postgres.QueueAckFailed(_)) -> retry(state, execution)
    _, _ -> {
      let #(id, attempt_id, epoch) = attempt.claim_identity(request.claimed)
      process.send(request.renewer, renewer.Untrack(attempt_id, epoch))
      process.send(
        request.queue_subject,
        request.on_complete(id, attempt_id, epoch, result),
      )
      // The coordinator stops and demonitors us after consuming completion.
      // This keeps normal completion distinguishable from unexpected death.
      actor.continue(WorkerState(..state, phase: Finished))
    }
  }
}

fn retry(
  state: WorkerState(message),
  execution: worker.Execution,
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let _ =
    process.send_after(
      state.subject,
      state.request.retry_interval_ms,
      RetryAcknowledgement,
    )
  actor.continue(
    WorkerState(..state, phase: WaitingForAcknowledgement(execution)),
  )
}
