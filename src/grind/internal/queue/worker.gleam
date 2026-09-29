//// Executes a handler once and owns its acknowledgement/reconciliation.
//// A finished proposal remains here until settled; it never invokes the handler again.

import gleam/erlang/process
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import grind/diagnostic
import grind/internal/attempt
import grind/internal/diagnostics
import grind/internal/queue/renewer
import grind/postgres
import grind/worker
import sinal/forwarder

pub type WorkerMessage {
  StartAttempt
  RetryAcknowledgement
  StopWorker
}

pub type WorkerRequest(message) {
  WorkerRequest(
    queue_subject: process.Subject(message),
    claimed: attempt.ClaimedJob,
    on_acknowledging: fn(Int, Int, Int) -> message,
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
    pending_since_us: Option(Int),
    retries: Int,
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
      actor.initialised(WorkerState(
        request:,
        subject:,
        phase: Ready,
        pending_since_us: None,
        retries: 0,
      ))
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
        let #(id, attempt_id, epoch) =
          attempt.claim_identity(state.request.claimed)
        let state =
          WorkerState(
            ..state,
            pending_since_us: Some(diagnostics.monotonic_us()),
          )
        process.send(
          state.request.queue_subject,
          state.request.on_acknowledging(id, attempt_id, epoch),
        )
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
    True, Error(postgres.QueueAckUnknown(_, proposed)) ->
      retry(state, proposed, diagnostic.RetryAfterUnknown)
    True, Error(postgres.QueueAckFailed(_)) ->
      retry(state, execution, diagnostic.RetryAfterFailure)
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
  reason: diagnostic.RetryReason,
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let _ =
    process.send_after(
      state.subject,
      state.request.retry_interval_ms,
      RetryAcknowledgement,
    )
  let request = state.request
  let retry_number = state.retries + 1
  let pending_duration_us = case state.pending_since_us {
    Some(started) -> diagnostics.monotonic_us() - started
    None -> 0
  }
  let #(id, attempt_id, epoch) = attempt.claim_identity(request.claimed)
  let _ =
    forwarder.emit(
      postgres.forwarder(request.database),
      diagnostic.acknowledgement_retry(),
      diagnostic.RetryMeasurements(
        count: 1,
        retry_number:,
        delay_ms: request.retry_interval_ms,
        pending_duration_us:,
      ),
      diagnostic.RetryMetadata(
        context: attempt.diagnostic_context(
          request.claimed,
          request.queue,
          request.owner,
        ),
        command_id: attempt.acknowledgement_command_id(id, attempt_id, epoch),
        reason:,
      ),
    )
  actor.continue(
    WorkerState(
      ..state,
      phase: WaitingForAcknowledgement(execution),
      retries: retry_number,
    ),
  )
}
