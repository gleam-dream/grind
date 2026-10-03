//// Independently renews a consumer's leases through its reserved pool.
//// One checkout covers all live attempts; locked ACK rows never block siblings.

import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import grind/internal/attempt
import grind/internal/diagnostic
import grind/internal/diagnostics
import pog
import sinal/forwarder.{type Forwarder}

pub type Status {
  Confirmed
  Unknown
  Lost
}

pub type Message {
  Track(attempt.ClaimedJob, process.Pid)
  AwaitAcknowledgement(attempt_id: Int, epoch: Int)
  Untrack(attempt_id: Int, epoch: Int)
  Tick
  Down(process.Down)
}

type Phase {
  Running
  Acknowledging(renew_until: Int)
  OwnershipLost
  CompletionBudgetFinished
}

type Entry {
  Entry(
    claim: attempt.ClaimedJob,
    attempt_id: Int,
    epoch: Int,
    monitor: process.Monitor,
    phase: Phase,
  )
}

type State {
  State(
    connection: pog.Connection,
    forwarder: Option(Forwarder),
    queue: String,
    owner: String,
    lease_ms: Int,
    interval_ms: Int,
    subject: process.Subject(Message),
    on_status: fn(Int, Int, Status) -> Nil,
    parent_monitor: process.Monitor,
    entries: List(Entry),
  )
}

@external(erlang, "grind_queue_ffi", "monotonic_ms")
fn monotonic_ms() -> Int

pub fn start(
  connection: pog.Connection,
  queue: String,
  owner: String,
  lease_ms: Int,
  interval_ms: Int,
  on_status: fn(Int, Int, Status) -> Nil,
) -> Result(actor.Started(process.Subject(Message)), actor.StartError) {
  start_with_forwarder(
    connection,
    queue,
    owner,
    lease_ms,
    interval_ms,
    on_status,
    None,
  )
}

pub fn start_observed(
  connection: pog.Connection,
  queue: String,
  owner: String,
  lease_ms: Int,
  interval_ms: Int,
  on_status: fn(Int, Int, Status) -> Nil,
  forwarder: Forwarder,
) -> Result(actor.Started(process.Subject(Message)), actor.StartError) {
  start_with_forwarder(
    connection,
    queue,
    owner,
    lease_ms,
    interval_ms,
    on_status,
    Some(forwarder),
  )
}

fn start_with_forwarder(
  connection: pog.Connection,
  queue: String,
  owner: String,
  lease_ms: Int,
  interval_ms: Int,
  on_status: fn(Int, Int, Status) -> Nil,
  forwarder: Option(Forwarder),
) -> Result(actor.Started(process.Subject(Message)), actor.StartError) {
  let parent = process.self()
  actor.new_with_initialiser(1000, fn(subject) {
    let _ = process.send_after(subject, interval_ms, Tick)
    Ok(
      actor.initialised(
        State(
          connection:,
          forwarder:,
          queue:,
          owner:,
          lease_ms:,
          interval_ms:,
          subject:,
          on_status:,
          parent_monitor: process.monitor(parent),
          entries: [],
        ),
      )
      |> actor.selecting(
        process.new_selector()
        |> process.select(subject)
        |> process.select_monitors(Down),
      )
      |> actor.returning(subject),
    )
  })
  |> actor.on_message(handle)
  |> actor.start
}

fn handle(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    Track(claim, pid) -> {
      let #(_, attempt_id, epoch) = attempt.claim_identity(claim)
      let entry =
        Entry(
          claim:,
          attempt_id:,
          epoch:,
          monitor: process.monitor(pid),
          phase: Running,
        )
      actor.continue(State(..state, entries: [entry, ..state.entries]))
    }
    AwaitAcknowledgement(id, epoch) -> {
      let until = monotonic_ms() + state.lease_ms
      let entries =
        list.map(state.entries, fn(entry) {
          case entry.attempt_id == id && entry.epoch == epoch, entry.phase {
            True, Running -> Entry(..entry, phase: Acknowledging(until))
            _, _ -> entry
          }
        })
      actor.continue(State(..state, entries:))
    }
    Untrack(id, epoch) -> {
      let entries =
        list.filter(state.entries, fn(entry) {
          case entry.attempt_id == id && entry.epoch == epoch {
            True -> {
              process.demonitor_process(entry.monitor)
              False
            }
            False -> True
          }
        })
      actor.continue(State(..state, entries:))
    }
    Down(process.ProcessDown(monitor:, ..)) -> {
      case monitor == state.parent_monitor {
        True -> actor.stop()
        False -> {
          let entries =
            list.filter(state.entries, fn(entry) { entry.monitor != monitor })
          actor.continue(State(..state, entries:))
        }
      }
    }
    Down(process.PortDown(..)) -> actor.continue(state)
    Tick -> {
      // Keep one next tick armed while storage runs. Scheduling it afterward
      // would add the query duration to every interval, consuming lease slack.
      let _ = process.send_after(state.subject, state.interval_ms, Tick)
      actor.continue(renew(state))
    }
  }
}

fn renew(state: State) -> State {
  let now = monotonic_ms()
  let entries =
    list.map(state.entries, fn(entry) {
      case entry.phase {
        Acknowledging(until) if now >= until -> {
          emit_renewal(
            state,
            entry,
            diagnostic.CompletionBudgetExhausted,
            0,
            None,
          )
          Entry(..entry, phase: CompletionBudgetFinished)
        }
        _ -> entry
      }
    })
  let state = State(..state, entries:)
  let eligible =
    list.filter(state.entries, fn(entry) {
      case entry.phase {
        Running | Acknowledging(_) -> True
        OwnershipLost | CompletionBudgetFinished -> False
      }
    })
  case eligible {
    [] -> state
    _ -> {
      let measured =
        attempt.renew_many_observed(
          state.connection,
          state.queue,
          state.owner,
          list.map(eligible, fn(entry) { entry.claim }),
          state.lease_ms,
        )
      let result = case state.forwarder {
        None -> measured.value
        Some(fwd) ->
          diagnostics.checkout(
            fwd,
            diagnostics.queue_ref(state.queue, state.owner),
            diagnostic.LeaseRenewal,
            diagnostic.ReservedPool,
            measured,
          )
      }
      case result {
        Error(_) -> {
          list.each(eligible, fn(entry) {
            emit_renewal(
              state,
              entry,
              diagnostic.StorageFailed,
              measured.call_duration_us,
              None,
            )
            state.on_status(entry.attempt_id, entry.epoch, Unknown)
          })
          state
        }
        Ok(results) -> {
          let entries =
            list.map(state.entries, fn(entry) {
              case
                list.find(results, fn(result) {
                  result.attempt_id == entry.attempt_id
                  && result.epoch == entry.epoch
                })
              {
                Error(Nil) -> entry
                Ok(result) -> {
                  let outcome = case result.status {
                    attempt.BatchLocked -> diagnostic.SkippedLocked
                    attempt.BatchRenewed -> diagnostic.Renewed
                    attempt.BatchLeaseLost -> diagnostic.LiveFenceUnavailable
                  }
                  emit_renewal(
                    state,
                    entry,
                    outcome,
                    measured.call_duration_us,
                    result.remaining_lease_ms,
                  )
                  case result.status {
                    attempt.BatchLocked -> entry
                    attempt.BatchRenewed -> {
                      state.on_status(entry.attempt_id, entry.epoch, Confirmed)
                      entry
                    }
                    attempt.BatchLeaseLost -> {
                      state.on_status(entry.attempt_id, entry.epoch, Lost)
                      Entry(..entry, phase: OwnershipLost)
                    }
                  }
                }
              }
            })
          State(..state, entries:)
        }
      }
    }
  }
}

fn emit_renewal(
  state: State,
  entry: Entry,
  outcome: diagnostic.RenewalOutcome,
  duration_us: Int,
  remaining_lease_ms: Option(Int),
) -> Nil {
  case state.forwarder {
    None -> Nil
    Some(fwd) -> {
      let phase = case entry.phase {
        Running -> diagnostic.HandlerRunning
        Acknowledging(_) | CompletionBudgetFinished | OwnershipLost ->
          diagnostic.AcknowledgementPending
      }
      let _ =
        forwarder.emit(
          fwd,
          diagnostic.renewal(),
          diagnostic.RenewalMeasurements(
            count: 1,
            duration_us:,
            remaining_lease_ms:,
          ),
          diagnostic.RenewalMetadata(
            context: attempt.diagnostic_context(
              entry.claim,
              state.queue,
              state.owner,
            ),
            phase:,
            outcome:,
          ),
        )
      Nil
    }
  }
}
