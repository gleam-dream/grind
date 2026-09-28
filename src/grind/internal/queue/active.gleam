//// Tracks the private active attempt ledger for one queue coordinator.
//// Completion and renewal status retain their owning queue module's types.

import gleam/erlang/process
import gleam/list
import grind/internal/attempt
import grind/internal/queue/worker as queue_worker

pub type ActiveAttempt(completion, status) {
  ActiveAttempt(
    id: Int,
    attempt_id: Int,
    epoch: Int,
    claimed: attempt.ClaimedJob,
    worker_subject: process.Subject(queue_worker.WorkerMessage),
    monitor: process.Monitor,
    completion: completion,
    renewal_status: status,
  )
}

pub fn find_active(
  active_attempts: List(ActiveAttempt(completion, status)),
  id: Int,
  attempt_id: Int,
  epoch: Int,
) -> Result(ActiveAttempt(completion, status), Nil) {
  list.find(active_attempts, fn(active) {
    let ActiveAttempt(
      id: active_id,
      attempt_id: active_attempt_id,
      epoch: active_epoch,
      ..,
    ) = active
    active_id == id && active_attempt_id == attempt_id && active_epoch == epoch
  })
}

pub fn remove_active(
  active_attempts: List(ActiveAttempt(completion, status)),
  id: Int,
  attempt_id: Int,
  epoch: Int,
) -> List(ActiveAttempt(completion, status)) {
  list.filter(active_attempts, fn(active) {
    let ActiveAttempt(
      id: active_id,
      attempt_id: active_attempt_id,
      epoch: active_epoch,
      ..,
    ) = active
    case
      active_id == id
      && active_attempt_id == attempt_id
      && active_epoch == epoch
    {
      True -> False
      False -> True
    }
  })
}

pub fn replace_active(
  active_attempts: List(ActiveAttempt(completion, status)),
  id: Int,
  attempt_id: Int,
  epoch: Int,
  replacement: ActiveAttempt(completion, status),
) -> List(ActiveAttempt(completion, status)) {
  list.map(active_attempts, fn(active) {
    let ActiveAttempt(
      id: active_id,
      attempt_id: active_attempt_id,
      epoch: active_epoch,
      ..,
    ) = active
    case
      active_id == id
      && active_attempt_id == attempt_id
      && active_epoch == epoch
    {
      True -> replacement
      False -> active
    }
  })
}

pub fn set_renewal_status(
  active_attempts: List(ActiveAttempt(completion, status)),
  attempt_id: Int,
  epoch: Int,
  renewal_status: status,
) -> List(ActiveAttempt(completion, status)) {
  list.map(active_attempts, fn(active) {
    let ActiveAttempt(attempt_id: active_id, epoch: active_epoch, ..) = active
    case active_id == attempt_id && active_epoch == epoch {
      True -> ActiveAttempt(..active, renewal_status:)
      False -> active
    }
  })
}

pub fn find_active_by_attempt(
  active_attempts: List(ActiveAttempt(completion, status)),
  attempt_id: Int,
  epoch: Int,
) -> Result(ActiveAttempt(completion, status), Nil) {
  list.find(active_attempts, fn(active) {
    let ActiveAttempt(attempt_id: active_id, epoch: active_epoch, ..) = active
    active_id == attempt_id && active_epoch == epoch
  })
}
