//// Executes a handler once and owns its acknowledgement/reconciliation.
//// A finished proposal remains here until settled; it never invokes the handler again.
////
//// The handler runs in its own linked process, so that the attempt process
//// can bound it with the worker's timeout and deliver a committed
//// cancellation to it. A handler that crashes takes the attempt process
//// with it, exactly as an inline handler would: the claim is then recovered
//// through lease expiry. A handler that exceeds its timeout is unlinked and
//// killed. Under `HoldUncertain` the attempt acknowledges `uncertain`; under
//// `ReplayAfterLeaseExpiry` it stops renewing and lets lease-expiry recovery
//// decide.

import gleam/erlang/process
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/time/duration
import gleam/time/timestamp
import grind/internal/attempt
import grind/internal/diagnostics
import grind/internal/postgres
import grind/internal/queue/renewer
import grind/internal/worker
import grind/telemetry
import sinal/forwarder

pub type WorkerMessage {
  StartAttempt
  RetryAcknowledgement
  StopWorker
  /// The renewer saw a committed cancellation for this attempt.
  CancelRequested
  HandlerReady(cancel: process.Subject(Nil))
  HandlerFinished(worker.Execution)
  HandlerTimedOut
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
  Running(
    handler: process.Pid,
    cancel: Option(process.Subject(Nil)),
    cancel_requested: Bool,
    timer: Option(process.Timer),
  )
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
  |> actor.on_message(handle)
}

fn handle(
  state: WorkerState(message),
  message: WorkerMessage,
) -> actor.Next(WorkerState(message), WorkerMessage) {
  case message, state.phase {
    StartAttempt, Ready -> start_handler(state)
    HandlerReady(cancel), Running(handler:, cancel_requested:, timer:, ..) -> {
      case cancel_requested {
        True -> process.send(cancel, Nil)
        False -> Nil
      }
      actor.continue(
        WorkerState(
          ..state,
          phase: Running(
            handler:,
            cancel: Some(cancel),
            cancel_requested:,
            timer:,
          ),
        ),
      )
    }
    CancelRequested, Running(handler:, cancel:, cancel_requested: False, timer:)
    -> {
      case cancel {
        Some(cancel) -> process.send(cancel, Nil)
        None -> Nil
      }
      actor.continue(
        WorkerState(
          ..state,
          phase: Running(handler:, cancel:, cancel_requested: True, timer:),
        ),
      )
    }
    HandlerFinished(execution), Running(timer:, ..) -> {
      cancel_timer(timer)
      begin_acknowledgement(state, execution)
    }
    HandlerTimedOut, Running(handler:, ..) -> {
      process.unlink(handler)
      process.kill(handler)
      timed_out(state)
    }
    RetryAcknowledgement, WaitingForAcknowledgement(execution) ->
      acknowledge(state, execution)
    StopWorker, _ -> actor.stop()
    _, _ -> actor.continue(state)
  }
}

fn start_handler(
  state: WorkerState(message),
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let request = state.request
  let self = state.subject
  process.send(
    request.renewer,
    renewer.Track(request.claimed, process.self(), fn() {
      process.send(self, CancelRequested)
    }),
  )
  let timeout_ms = attempt.claim_timeout_ms(request.claimed)
  let deadline = case timeout_ms {
    Some(ms) ->
      Some(timestamp.add(timestamp.system_time(), duration.milliseconds(ms)))
    None -> None
  }
  let claimed = request.claimed
  let handler =
    process.spawn(fn() {
      let cancel = process.new_subject()
      process.send(self, HandlerReady(cancel))
      let cancellation = process.new_selector() |> process.select(cancel)
      let context = attempt.claim_context(claimed, cancellation, deadline)
      let execution = attempt.execute_claim(claimed, context)
      process.send(self, HandlerFinished(execution))
    })
  let timer = case timeout_ms {
    Some(ms) -> Some(process.send_after(self, ms, HandlerTimedOut))
    None -> None
  }
  actor.continue(
    WorkerState(
      ..state,
      phase: Running(handler:, cancel: None, cancel_requested: False, timer:),
    ),
  )
}

fn cancel_timer(timer: Option(process.Timer)) -> Nil {
  case timer {
    Some(timer) -> {
      let _ = process.cancel_timer(timer)
      Nil
    }
    None -> Nil
  }
}

/// The handler outlived its timeout and was killed. `HoldUncertain`
/// acknowledges `uncertain` now; `ReplayAfterLeaseExpiry` leaves the claim
/// unacknowledged, so lease-expiry recovery replays it.
fn timed_out(
  state: WorkerState(message),
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let request = state.request
  let timeout = case attempt.claim_timeout_ms(request.claimed) {
    Some(ms) -> int.to_string(ms)
    None -> "?"
  }
  case attempt.claim_abandonment(request.claimed) {
    worker.HoldUncertain ->
      begin_acknowledgement(
        state,
        worker.ExecutedUncertain(
          "handler exceeded its timeout of "
          <> timeout
          <> " ms and was stopped; its effect is unknown",
        ),
      )
    worker.ReplayAfterLeaseExpiry(_) -> {
      let #(id, attempt_id, epoch) = attempt.claim_identity(request.claimed)
      process.send(request.renewer, renewer.Untrack(attempt_id, epoch))
      process.send(
        request.queue_subject,
        request.on_complete(id, attempt_id, epoch, Ok(True)),
      )
      actor.continue(WorkerState(..state, phase: Finished))
    }
  }
}

fn begin_acknowledgement(
  state: WorkerState(message),
  execution: worker.Execution,
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let request = state.request
  let #(id, attempt_id, epoch) = attempt.claim_identity(request.claimed)
  let state =
    WorkerState(..state, pending_since_us: Some(diagnostics.monotonic_us()))
  process.send(
    request.queue_subject,
    request.on_acknowledging(id, attempt_id, epoch),
  )
  process.send(request.renewer, renewer.AwaitAcknowledgement(attempt_id, epoch))
  acknowledge(state, execution)
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
      retry(state, proposed, telemetry.RetryAfterUnknown)
    True, Error(postgres.QueueAckFailed(_)) ->
      retry(state, execution, telemetry.RetryAfterFailure)
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
  reason: telemetry.RetryReason,
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
      telemetry.acknowledgement_retry(),
      telemetry.RetryMeasurements(
        count: 1,
        retry_number:,
        delay_ms: request.retry_interval_ms,
        pending_duration_us:,
      ),
      telemetry.RetryMetadata(
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
