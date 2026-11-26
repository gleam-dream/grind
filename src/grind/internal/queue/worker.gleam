//// Executes a handler once and owns its acknowledgement/reconciliation.
//// A finished proposal remains here until settled; it never invokes the handler again.
////
//// The handler runs in its own linked process, so that the attempt process
//// can bound it with the worker's timeout and deliver a committed
//// cancellation to it. The attempt process traps exits, so a handler crash
//// arrives as a message. When any other linked process exits, which is its
//// supervisor stopping it, it exits with `shutdown` and its handler ends
//// with it. A handler that exceeds its timeout is unlinked and killed.
//// Either way the attempt is abandoned, because neither proves that the
//// handler had no effect: under `HoldUncertain` the attempt acknowledges
//// `uncertain` at once, with the cause and a bounded description; under
//// `ReplayAfterLeaseExpiry` it stops renewing and lets lease-expiry
//// recovery decide. A handler whose attempt lost its lease is unlinked and
//// killed too, and the attempt finishes here without acknowledging: no
//// write of that attempt can commit any more, so lease-expiry recovery owns
//// the job's outcome and may already have started another attempt of the
//// same job. Because it is unlinked first, its stop is never read as a
//// crash.

import gleam/dynamic.{type Dynamic}
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
  /// A process linked to this attempt process exited: its handler, or the
  /// supervisor that is stopping it.
  LinkedExited(process.ExitMessage)
  /// The renewer found no live fence for this attempt.
  LeaseLost
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
    /// The supervisor that started this attempt process.
    parent: process.Pid,
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
    // A handler crash arrives as a message instead of ending this process,
    // so the reason can be stored before the attempt ends.
    process.trap_exits(True)
    let selector =
      process.new_selector()
      |> process.select(subject)
      |> process.select_trapped_exits(LinkedExited)
    Ok(
      actor.initialised(WorkerState(
        request:,
        subject:,
        parent: parent_pid(),
        phase: Ready,
        pending_since_us: None,
        retries: 0,
      ))
      |> actor.selecting(selector)
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
    // Only a running handler is stopped. Once the handler has returned, the
    // acknowledgement decides: a renewal can also miss the fence because
    // that acknowledgement has just committed.
    LeaseLost, Running(handler:, timer:, ..) -> {
      cancel_timer(timer)
      process.unlink(handler)
      process.kill(handler)
      leave_to_lease_recovery(state)
    }
    RetryAcknowledgement, WaitingForAcknowledgement(execution) ->
      acknowledge(state, execution)
    LinkedExited(process.ExitMessage(pid:, reason:)),
      Running(handler:, timer:, ..)
      if pid == handler
    -> {
      // A handler that returns sends `HandlerFinished` before it exits, and
      // that message leaves this phase. An exit seen here is a crash.
      cancel_timer(timer)
      handler_crashed(state, reason)
    }
    // The supervisor is stopping this attempt. Any other exit is one this
    // process no longer acts on: a handler that returned and ended, or one
    // that crashed in the instant its timeout was handled.
    LinkedExited(process.ExitMessage(pid:, ..)), _ if pid == state.parent ->
      exit_shutdown()
    LinkedExited(_), _ -> actor.continue(state)
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
    renewer.Track(
      attempt.lease_fence(request.claimed, request.owner),
      process.self(),
      on_cancel: fn() { process.send(self, CancelRequested) },
      on_lost: fn() { process.send(self, LeaseLost) },
    ),
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
  // The handler process has its own copy of the worker; this actor keeps
  // only the fence, policies and connection it acknowledges with.
  actor.continue(
    WorkerState(
      ..state,
      request: WorkerRequest(..request, claimed: attempt.handed_off(claimed)),
      phase: Running(handler:, cancel: None, cancel_requested: False, timer:),
    ),
  )
}

/// The handler process exited without returning: an abandonment, with a
/// bounded description of its exit reason.
fn handler_crashed(
  state: WorkerState(message),
  reason: process.ExitReason,
) -> actor.Next(WorkerState(message), WorkerMessage) {
  abandon(state, worker.StoppedByCrash, crash_description(reason))
}

/// The description stored on a crashed handler's job, prefix included, cut
/// to at most 500 characters and 2000 bytes.
pub fn crash_description(reason: process.ExitReason) -> String {
  bounded(
    "handler exited: "
    <> case reason {
      process.Normal -> "normal"
      process.Killed -> "killed"
      process.Abnormal(reason) -> describe_exit(reason)
    },
  )
}

@external(erlang, "grind_queue_ffi", "bounded")
fn bounded(text: String) -> String

@external(erlang, "grind_queue_ffi", "describe_exit")
fn describe_exit(reason: Dynamic) -> String

@external(erlang, "grind_runtime_ffi", "parent_pid")
fn parent_pid() -> process.Pid

/// Ends this process with reason `shutdown`, the reason a supervisor expects
/// from a child it is stopping. The linked handler ends with it.
@external(erlang, "grind_runtime_ffi", "exit_shutdown")
fn exit_shutdown() -> a

fn cancel_timer(timer: Option(process.Timer)) -> Nil {
  case timer {
    Some(timer) -> {
      let _ = process.cancel_timer(timer)
      Nil
    }
    None -> Nil
  }
}

/// The handler outlived its timeout and was killed.
fn timed_out(
  state: WorkerState(message),
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let timeout = case attempt.claim_timeout_ms(state.request.claimed) {
    Some(ms) -> int.to_string(ms)
    None -> "?"
  }
  abandon(
    state,
    worker.StoppedAtTimeout,
    "handler exceeded its timeout of "
      <> timeout
      <> " ms and was stopped; its effect is unknown",
  )
}

/// The handler stopped without a result, at its timeout or by crashing.
/// `HoldUncertain` acknowledges `uncertain` now, with the cause;
/// `ReplayAfterLeaseExpiry` leaves the claim unacknowledged, so lease-expiry
/// recovery replays it.
fn abandon(
  state: WorkerState(message),
  stopped: worker.Stopped,
  description: String,
) -> actor.Next(WorkerState(message), WorkerMessage) {
  case attempt.claim_abandonment(state.request.claimed) {
    worker.HoldUncertain ->
      begin_acknowledgement(
        state,
        worker.ExecutedAbandoned(stopped, description),
      )
    worker.ReplayAfterLeaseExpiry(_) -> leave_to_lease_recovery(state)
  }
}

/// Finishes the attempt on this node without acknowledging: it stops
/// renewing and frees its slot, and lease-expiry recovery decides the job.
fn leave_to_lease_recovery(
  state: WorkerState(message),
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let request = state.request
  let #(id, attempt_id, epoch) = attempt.claim_identity(request.claimed)
  process.send(request.renewer, renewer.Untrack(attempt_id, epoch))
  process.send(
    request.queue_subject,
    request.on_complete(id, attempt_id, epoch, Ok(True)),
  )
  actor.continue(WorkerState(..state, phase: Finished))
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
    True, Error(postgres.QueueAckFailed(error)) ->
      case postgres.data_exception_code(error) {
        // `attempt.acknowledge` already replaced a proposal refused with a
        // data exception; a replacement refused the same way cannot commit
        // on retry either, so the attempt completes with the error and
        // lease recovery applies.
        Some(_) -> complete(state, result)
        None -> retry(state, execution, telemetry.RetryAfterFailure)
      }
    _, _ -> complete(state, result)
  }
}

fn complete(
  state: WorkerState(message),
  result: Result(Bool, postgres.QueueRunError),
) -> actor.Next(WorkerState(message), WorkerMessage) {
  let request = state.request
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
