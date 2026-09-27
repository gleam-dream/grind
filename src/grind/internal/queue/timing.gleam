//// Pure queue lease/shutdown timing and timer sends.

import gleam/erlang/process

pub fn renewal_is_current(
  active_attempt_id: Int,
  active_epoch: Int,
  tick_attempt_id: Int,
  tick_epoch: Int,
) -> Bool {
  active_attempt_id == tick_attempt_id && active_epoch == tick_epoch
}

pub fn next_shutdown_generation(
  current_generation: Int,
  shutdown_already_pending: Bool,
) -> Int {
  case shutdown_already_pending {
    True -> current_generation
    False -> current_generation + 1
  }
}

pub fn minimum_lease_for_deadline(
  maximum_concurrency: Int,
  statement_deadline_ms: Int,
) -> Int {
  case maximum_concurrency > 1 {
    True -> 6 * statement_deadline_ms
    False -> 3 * statement_deadline_ms / 2
  }
}

pub fn start_polling(
  subject: process.Subject(message),
  auto_poll: Bool,
  poll: message,
) -> Nil {
  case auto_poll {
    True -> {
      let _ = process.send(subject, poll)
      Nil
    }
    False -> Nil
  }
}

pub fn schedule_poll(
  subject: process.Subject(message),
  auto_poll: Bool,
  poll_interval_ms: Int,
  poll: message,
) -> Nil {
  case auto_poll {
    True -> {
      let _ = process.send_after(subject, poll_interval_ms, poll)
      Nil
    }
    False -> Nil
  }
}
