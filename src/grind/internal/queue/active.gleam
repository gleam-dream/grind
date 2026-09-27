//// Tracks the private active attempt ledger for one queue coordinator.
//// Completion and renewal status retain their owning queue module's types.

import gleam/erlang/process
import gleam/list
import gleam/option.{type Option}
import grind/internal/attempt
import grind/internal/queue/worker as queue_worker
import grind/worker

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
    /// `Some(execution)` once this `Automatic`-completion attempt's worker
    /// has already returned and its acknowledgement came back
    /// `QueueAckUnknown` (the ack transaction reached the database but its
    /// reply was lost) — the attempt is kept in `active` rather than
    /// dropped, so it still counts against `maximum_concurrency` and blocks
    /// a clean shutdown drain, but its `worker_subject`/`monitor` are
    /// already stale (the worker was stopped and demonitored before the ack
    /// was ever attempted). This incarnation's own renewal timer retries the
    /// exact same `acknowledge` call (`attempt.acknowledgement_command_id`
    /// is deterministic in job id, attempt id, and epoch, so the retry is
    /// idempotent), renewing the lease first on each tick while within
    /// `ConsumerState.pending_ack_retry_budget` — see `retry_pending_ack`'s
    /// and `retry_ack_until_known`'s doc comments for the full mechanism and
    /// why renewing (bounded) is the safe choice, not the lease-independence
    /// an earlier version of this fix wrongly assumed. Always `None` for a
    /// `ManualCompletion` — a `process_one` caller already gets
    /// `QueueAckUnknown` back synchronously and can retry itself — and for
    /// an attempt whose worker is still running normally.
    pending_ack: Option(worker.Execution),
    /// Number of retry attempts already made while `pending_ack` has been
    /// `Some` (0 for the first one, about to be made). Compared against
    /// `ConsumerState.pending_ack_retry_budget` to decide whether this
    /// tick still renews the lease before retrying the acknowledgement.
    /// Meaningless (left at its last value) while `pending_ack` is `None`.
    pending_ack_ticks: Int,
    /// Invalidates a still-outstanding timer from an earlier chain: bumped
    /// only when `retry_ack_until_known` first moves this attempt from
    /// `pending_ack: None` to `Some` (there is always exactly one leftover
    /// ordinary-renewal timer already scheduled at that point, from the
    /// chain `start_attempt`/`renew_lease` maintains), and carried unchanged
    /// by every later tick of the same chain (ordinary renewal or pending
    /// retry alike). `renew_active_attempt` ignores a `Renew` whose own
    /// `generation` does not match this field — see its doc comment for why
    /// this, not just the existing `attempt_id`/`epoch` match, is needed to
    /// guarantee exactly one outstanding timer per attempt.
    renewal_generation: Int,
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
