//// Bridges a supervised queue actor's ready subject back to its starter.
//// The handoff process stops after the actor starts or its owner exits.

import gleam/erlang/process

pub type HandoffMessage(message) {
  HandoffSubjects(
    actor_ready: process.Subject(process.Subject(message)),
    stop: process.Subject(Nil),
  )
  HandoffStarted(process.Subject(message))
}

type QueueActorHandoffEvent(message) {
  QueueActorStarted(process.Subject(message))
  QueueActorHandoffStopped
  QueueActorHandoffOwnerDown(process.Down)
}

pub fn start(
  reply: process.Subject(HandoffMessage(message)),
  owner: process.Pid,
) -> process.Pid {
  process.spawn_unlinked(fn() {
    let actor_ready = process.new_subject()
    let stop = process.new_subject()
    process.send(reply, HandoffSubjects(actor_ready, stop))
    let monitor = process.monitor(owner)
    let selector =
      process.new_selector()
      |> process.select_map(actor_ready, fn(subject) {
        QueueActorStarted(subject)
      })
      |> process.select_map(stop, fn(_) { QueueActorHandoffStopped })
      |> process.select_specific_monitor(monitor, fn(down) {
        QueueActorHandoffOwnerDown(down)
      })
    case process.selector_receive_forever(selector) {
      QueueActorStarted(subject) -> {
        process.send(reply, HandoffStarted(subject))
        let _ = process.demonitor_process(monitor)
        Nil
      }
      QueueActorHandoffStopped | QueueActorHandoffOwnerDown(_) -> {
        let _ = process.demonitor_process(monitor)
        Nil
      }
    }
  })
}
