//// The settler of one queue's consumer (docs/design/design.typ, "Settler").
////
//// It starts only when the consumer polls on its own and its registry
//// declares a settlement. When it starts, it marks the jobs of its queue
//// that a node of the previous release held uncertain without a mark, then
//// takes due jobs once per poll interval, one try at a time, while one is
//// due. Each try runs in a process linked to the settler
//// (`grind/internal/settlement`). The settler runs under a supervisor of
//// its own, so its crash restarts only it and never the consumer's
//// coordinator or renewal pool.

import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import grind/internal/postgres.{type Database}
import grind/internal/registry.{type Registry}
import grind/internal/settlement

/// What the settler is sent: look for due jobs now.
pub opaque type Message {
  Look
}

/// The most tries one look runs before yielding to its mailbox, so a
/// backlog cannot hold the settler past a shutdown request for long.
const tries_per_look = 100

/// The settler under its own one-for-one supervisor, as a transient child
/// of the consumer's supervisor: a settler whose restarts are exhausted
/// stops without restarting the consumer, and a consumer restart starts it
/// again.
pub fn supervised(
  database: Database,
  workers: Registry,
  lease_ms: Int,
  interval_ms: Int,
  observe: fn(settlement.Outcome) -> Nil,
) -> supervision.ChildSpecification(static_supervisor.Supervisor) {
  static_supervisor.new(static_supervisor.OneForOne)
  |> static_supervisor.add(
    supervision.worker(fn() {
      start(database, workers, lease_ms, interval_ms, observe)
    }),
  )
  |> static_supervisor.supervised()
  |> supervision.restart(supervision.Transient)
}

fn start(
  database: Database,
  workers: Registry,
  lease_ms: Int,
  interval_ms: Int,
  observe: fn(settlement.Outcome) -> Nil,
) -> Result(actor.Started(process.Subject(Message)), actor.StartError) {
  actor.new_with_initialiser(1000, fn(self) {
    // Jobs of the previous release first, so they join this look.
    let _ = settlement.recognise_unmarked(database, workers)
    process.send(self, Look)
    actor.initialised(self)
    |> actor.returning(self)
    |> Ok
  })
  |> actor.on_message(fn(self, message) {
    case message {
      Look -> {
        look(database, workers, lease_ms, observe, tries_per_look)
        process.send_after(self, interval_ms, Look)
        actor.continue(self)
      }
    }
  })
  |> actor.start
}

/// Takes and tries due jobs until none is due, a take fails, or the look
/// has run `remaining` tries. A failed take is tried again at the next
/// look.
fn look(
  database: Database,
  workers: Registry,
  lease_ms: Int,
  observe: fn(settlement.Outcome) -> Nil,
  remaining: Int,
) -> Nil {
  case remaining > 0 {
    False -> Nil
    True ->
      case settlement.settle_next(database, workers, lease_ms) {
        Error(_) | Ok(settlement.NothingDue) -> Nil
        Ok(outcome) -> {
          observe(outcome)
          look(database, workers, lease_ms, observe, remaining - 1)
        }
      }
  }
}
