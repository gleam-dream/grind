//// Public queue policy and coordinator. Internal queue modules own the
//// active-attempt ledger, temporary worker, startup handoff, and timers.

import exception
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/factory_supervisor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import grind/internal/attempt
import grind/internal/consumer_hooks.{type Hooks}
import grind/internal/queue/active.{type ActiveAttempt, ActiveAttempt} as queue_active
import grind/internal/queue/handoff as queue_handoff
import grind/internal/queue/timing as queue_timing
import grind/internal/queue/worker as queue_worker
import grind/postgres.{type Database}
import grind/registry.{type Registry}
import grind/worker

/// An independently supervised consumer for one configured queue. The process
/// that starts the consumer owns its supervisor and must also stop it.
pub opaque type Consumer {
  Consumer(
    subject: process.Subject(Message),
    supervisor_pid: process.Pid,
    maximum_batch_jobs: Int,
    shutdown_grace_ms: Int,
    owner_pid: process.Pid,
  )
}

/// How a consumer discovers newly-due work: `PollEvery` schedules its own
/// recurring `Poll` message on this timer; `Manual` schedules none, and a
/// caller drives each attempt through `process_one`/`process_batch` instead.
pub type Polling {
  PollEvery(interval_ms: Int)
  Manual
}

/// Queue-local polling, lease, and execution-capacity settings.
pub type QueuePolicy {
  QueuePolicy(
    polling: Polling,
    /// Upper bound on how many jobs one `process_available` call processes
    /// before returning control to the caller — a manual-mode batch size,
    /// nothing else. It has no effect on automatic (`PollEvery`) polling,
    /// which always tries to claim into every free slot up to
    /// `maximum_concurrency` and backs off to `polling`'s interval only once
    /// a claim finds nothing (see `grind/queue`'s module-level automatic
    /// polling behavior); before this field was narrowed to this single
    /// meaning it also throttled automatic polling to at most this many
    /// claims per `Poll` timer tick, which is what produced the low default
    /// throughput ceiling documented as risk 6 in `docs/RISKS.md`. **Default:
    /// 1** (see `default_policy`) — plainly, this means `process_available`
    /// handles exactly one job per call unless raised with
    /// `with_maximum_batch_jobs`; a caller relying on `process_available` to
    /// drain a manual-mode backlog rather than calling `process_one` in its
    /// own loop needs to raise this explicitly. The default is kept at 1,
    /// not raised, so that `default_policy`'s manual-mode behavior stays the
    /// conservative, explicit-opt-in shape it always has been (one call, one
    /// job, one typed result) rather than a surprising multi-job batch a
    /// caller did not ask for; `default_policy`'s own `maximum_concurrency`
    /// is 1 for the identical reason.
    maximum_batch_jobs: Int,
    maximum_concurrency: Int,
    lease_duration_ms: Int,
    shutdown_grace_ms: Int,
  )
}

pub fn default_policy() -> QueuePolicy {
  QueuePolicy(
    polling: PollEvery(250),
    maximum_batch_jobs: 1,
    maximum_concurrency: 1,
    lease_duration_ms: 30_000,
    shutdown_grace_ms: 5000,
  )
}

/// `default_policy() |> validate_policy`, already unwrapped: the shipped
/// defaults are always valid, so this never fails. Lets `queue.start`'s
/// ordinary, no-customization path skip both the intermediate `QueuePolicy`
/// and the `let assert` its own `validate_policy` call would otherwise need.
pub fn default_policy_validated() -> ValidatedPolicy {
  let assert Ok(policy) = validate_policy(default_policy())
  policy
}

/// Reports whether a renewal timer still belongs to the active attempt.
@internal
pub fn renewal_is_current(
  active_attempt_id: Int,
  active_epoch: Int,
  tick_attempt_id: Int,
  tick_epoch: Int,
) -> Bool {
  queue_timing.renewal_is_current(
    active_attempt_id,
    active_epoch,
    tick_attempt_id,
    tick_epoch,
  )
}

/// Starts one shutdown grace period, or keeps its generation when another
/// caller joins the already pending drain.
@internal
pub fn next_shutdown_generation(
  current_generation: Int,
  shutdown_already_pending: Bool,
) -> Int {
  queue_timing.next_shutdown_generation(
    current_generation,
    shutdown_already_pending,
  )
}

pub fn with_poll_interval(
  policy: QueuePolicy,
  poll_interval_ms: Int,
) -> QueuePolicy {
  QueuePolicy(..policy, polling: PollEvery(poll_interval_ms))
}

/// Disables automatic polling: the started consumer schedules no `Poll`
/// timer of its own, and a caller must drive each attempt through
/// `process_one`/`process_batch` instead.
pub fn with_manual_polling(policy: QueuePolicy) -> QueuePolicy {
  QueuePolicy(..policy, polling: Manual)
}

/// Sets the manual-mode batch size `process_available` processes per call.
/// Has no effect on automatic (`PollEvery`) polling — see the field's doc
/// comment on `QueuePolicy`.
pub fn with_maximum_batch_jobs(
  policy: QueuePolicy,
  maximum_batch_jobs: Int,
) -> QueuePolicy {
  QueuePolicy(..policy, maximum_batch_jobs:)
}

/// Sets the maximum number of live worker children owned by this consumer.
/// This limit is local to one consumer process, not shared across consumers.
pub fn with_maximum_concurrency(
  policy: QueuePolicy,
  maximum_concurrency: Int,
) -> QueuePolicy {
  QueuePolicy(..policy, maximum_concurrency:)
}

/// Sets the database lease duration for a claimed attempt. The consumer renews
/// this lease while the worker is running.
pub fn with_lease_duration(
  policy: QueuePolicy,
  lease_duration_ms: Int,
) -> QueuePolicy {
  QueuePolicy(..policy, lease_duration_ms:)
}

/// Sets how long stop waits for active workers before shutting down the queue.
/// A zero grace stops immediately and leaves active claims for reconciliation.
pub fn with_shutdown_grace(
  policy: QueuePolicy,
  shutdown_grace_ms: Int,
) -> QueuePolicy {
  QueuePolicy(..policy, shutdown_grace_ms:)
}

pub type PolicyError {
  PollIntervalMustBePositive
  MaximumBatchJobsMustBePositive
  MaximumConcurrencyMustBePositive
  LeaseDurationMustBePositive
  ShutdownGraceMustBeNonNegative
}

pub opaque type ValidatedPolicy {
  ValidatedPolicy(
    polling: Polling,
    maximum_batch_jobs: Int,
    maximum_concurrency: Int,
    lease_duration_ms: Int,
    shutdown_grace_ms: Int,
  )
}

/// Validates the policy before a queue actor is started.
pub fn validate_policy(
  policy: QueuePolicy,
) -> Result(ValidatedPolicy, PolicyError) {
  let QueuePolicy(
    polling:,
    maximum_batch_jobs:,
    maximum_concurrency:,
    lease_duration_ms:,
    shutdown_grace_ms:,
  ) = policy
  let polling_ok = case polling {
    PollEvery(interval_ms) -> interval_ms > 0
    Manual -> True
  }
  case polling_ok {
    False -> Error(PollIntervalMustBePositive)
    True ->
      case maximum_batch_jobs > 0 {
        False -> Error(MaximumBatchJobsMustBePositive)
        True ->
          case maximum_concurrency > 0 {
            False -> Error(MaximumConcurrencyMustBePositive)
            True ->
              case lease_duration_ms > 0 {
                False -> Error(LeaseDurationMustBePositive)
                True ->
                  case shutdown_grace_ms >= 0 {
                    False -> Error(ShutdownGraceMustBeNonNegative)
                    True ->
                      Ok(ValidatedPolicy(
                        polling:,
                        maximum_batch_jobs:,
                        maximum_concurrency:,
                        lease_duration_ms:,
                        shutdown_grace_ms:,
                      ))
                  }
              }
          }
      }
  }
}

pub type StartError {
  NoRegisteredWorkers
  QueueActorStartFailed(actor.StartError)
  QueueSupervisorStartFailed(actor.StartError)
  QueueActorHandoffFailed
  QueueSupervisorStopTimedOut
  /// `lease_duration_ms` is too short relative to `database`'s own
  /// `postgres.Settings.statement_deadline_ms` (`D`) for the coordinator's
  /// own recovery machinery to have a real chance to act before the lease
  /// lapses. Derivation: a live attempt's own renewal timer fires every
  /// `L / 3` (`renewal_interval_ms`, computed once when the consumer
  /// starts), so after a renewal succeeds there is `L - L / 3 = (2 / 3) * L`
  /// of slack before that same lease would otherwise expire. The
  /// coordinator's single message loop can block for up to roughly `3 * D`
  /// retrying one pending
  /// acknowledgement (`ConsumerState.pending_ack_retry_budget`, ~3 ticks),
  /// during which a *sibling* attempt's own renewal tick sits queued behind
  /// it before it can even start; that renewal call is itself now bounded
  /// by `D`. At `maximum_concurrency > 1`, the slack must cover both: `(2 /
  /// 3) * L >= 3 * D + D` gives `L >= 6 * D`, with *zero* margin left at
  /// that exact minimum (the queued renewal starts the instant the stall
  /// clears and takes the full `D` to finish, landing exactly at the
  /// lease's own expiry). At `maximum_concurrency` of exactly 1 there is no
  /// sibling to queue behind anything, so only the renewal's own `D` needs
  /// to fit: `(2 / 3) * L >= D` gives `L >= 1.5 * D`, again with zero margin
  /// at that minimum. `attempted_ms` is the lease this call was given;
  /// `minimum_ms` is the smallest lease that would have passed. Neither
  /// bound carries any margin beyond exact algebraic sufficiency — a real
  /// deployment should clear it with real headroom (the shipped
  /// `default_policy` lease of 30000 clears the `maximum_concurrency > 1`
  /// minimum of `6 * 4000 = 24000` by 6000ms, `1.5 * D`, not by design
  /// margin baked into the rule itself).
  /// Known limit even once this rule passes, assumed away by the derivation
  /// above ("at most one stalled message ahead"): at `maximum_concurrency >
  /// 2`, more than one sibling's renewal can queue up behind the same
  /// stalled acknowledgement, each then also waiting out however many
  /// siblings' own `D`-bounded renewal calls are queued ahead of it —
  /// `3 * D + (N - 1) * D` for the `N`-th sibling in that queue, not the
  /// `3 * D + D` this rule accounts for. The real fix is moving renewals off
  /// the coordinator's own loop entirely (tracked in
  /// `docs/RELEASE-READINESS.md`, "Decide on per-attempt storage calls").
  LeaseTooShortForDeadline(attempted_ms: Int, minimum_ms: Int)
}

/// The lease-rule minimum for `maximum_concurrency` and a storage deadline
/// `D` (`postgres.statement_deadline_ms`) — see `LeaseTooShortForDeadline`.
@internal
pub fn minimum_lease_for_deadline(
  maximum_concurrency: Int,
  statement_deadline_ms: Int,
) -> Int {
  queue_timing.minimum_lease_for_deadline(
    maximum_concurrency,
    statement_deadline_ms,
  )
}

pub type StopError {
  ConsumerOwnedByAnotherProcess
  ConsumerStopTimedOut
}

pub type StopOutcome {
  /// The coordinator this call reached drained cleanly with no active work.
  /// This describes only the incarnation `stop` actually talked to: it does
  /// not mean no attempt was ever abandoned by an earlier incarnation that
  /// crashed before this call — an abandoned attempt is recovered only
  /// through lease expiry moving it to `Uncertain` and an audited
  /// resolution, independent of what any later `stop` call reports.
  StoppedCleanly
  StoppedWithActiveWork(active_attempts: Int)
  /// The supervisor teardown itself succeeded (this consumer's whole
  /// process tree is gone), but the coordinator never confirmed whether its
  /// own drain finished cleanly or was forced with active work still
  /// outstanding before that teardown — the shutdown-confirmation request
  /// this call also made timed out first. Not an error: the consumer is
  /// stopped either way. Any attempt this incarnation still owned when its
  /// supervisor came down is recovered the same way an abandoned attempt
  /// always is — lease expiry to `Uncertain`, then an audited resolution —
  /// regardless of which of `StoppedCleanly`/`StoppedWithActiveWork` it
  /// would otherwise have been.
  StoppedDrainUnconfirmed
  /// No coordinator was reachable under this consumer's name at all — for
  /// example a repeated `stop` on an already-stopped consumer, or a
  /// coordinator that crashed and was not, or not yet, restarted. The
  /// consumer's supervisor tree is stopped regardless. Any work an earlier
  /// incarnation had claimed is not drained or observed by this call; it is
  /// recovered only through lease expiry and an audited resolution, exactly
  /// as if this `stop` had never been called. This can also happen when the
  /// top-level supervisor restarts a coordinator during this very call: a
  /// freshly restarted incarnation auto-polls immediately and may claim a
  /// job before this call's own supervisor teardown kills it moments later.
  /// That job is left `Executing` with nothing left to renew it, and is
  /// recovered the same way — lease expiry to `Uncertain`, then an audited
  /// resolution — as any other abandoned attempt.
  StoppedWithoutDrain
}

pub type ProcessError {
  QueueProcessFailed(postgres.QueueRunError)
  QueueWorkerStartFailed(actor.StartError)
  QueueWorkerStartClaimLost(actor.StartError)
  QueueWorkerStartReleaseFailed(actor.StartError, postgres.QueueRunError)
  QueueWorkerExitedBeforeActivation
  QueueWorkerActivationClaimLost
  QueueWorkerActivationReleaseFailed(postgres.QueueRunError)
  QueueBusy
  QueueShuttingDown
  QueueWorkerExited
  QueueActorExited
}

@internal
pub type RenewalStatus {
  LeaseRenewalConfirmed
  LeaseRenewalUnknown
  LeaseRenewalLost
}

type Message {
  Poll
  FillSlots
  BeginShutdown(process.Subject(ShutdownReply))
  ShutdownGraceExpired(Int)
  ReadShutdownState(process.Subject(Bool))
  Renew(attempt_id: Int, epoch: Int, generation: Int)
  ReadRenewalStatus(reply: process.Subject(Option(RenewalStatus)))
  ProcessOne(reply: process.Subject(Result(Bool, ProcessError)))
  AttemptReturned(
    id: Int,
    attempt_id: Int,
    epoch: Int,
    execution: worker.Execution,
  )
  WorkerDown(process.Down)
}

/// Exposed `@internal` only so `begin_shutdown_for_test` can hand its reply
/// subject's type to a caller outside this module; not part of the stable
/// public API.
@internal
pub type ShutdownReply {
  ShutdownDrained
  ShutdownForced(Int)
}

type Completion {
  ManualCompletion(reply: process.Subject(Result(Bool, ProcessError)))
  Automatic
}

type ProcessOneResponse {
  ProcessOneResult(Result(Bool, ProcessError))
  ProcessOneActorDown(process.Down)
}

/// Reports acknowledged outcomes before a later attempt in the same poll fails.
/// The count excludes the failed call; its error can still describe an
/// operational disposition that was committed for that job.
pub type BatchOutcome {
  BatchCompleted(processed: Int)
  BatchStopped(acknowledged_before_error: Int, error: ProcessError)
}

type ConsumerState {
  ConsumerState(
    database: Database,
    workers: Registry,
    worker_factory: factory_supervisor.Supervisor(
      queue_worker.WorkerRequest(Message),
      process.Subject(queue_worker.WorkerMessage),
    ),
    queue: String,
    attempt_owner: String,
    subject: process.Subject(Message),
    /// A plain (unregistered, pid-bound) subject created fresh by this
    /// incarnation's own initialiser, used for every message that is
    /// internal to this incarnation: the timers it schedules for its own
    /// future self (`Poll`, `Renew`, `ShutdownGraceExpired`) and the
    /// `AttemptReturned` reply a worker this incarnation started sends back
    /// once it finishes. Unlike `subject`, which is named and therefore
    /// reaches whichever incarnation is *currently* registered,
    /// `erlang:send_after` targeting a plain subject's pid keeps targeting
    /// that exact pid even after it dies, so a timer an incarnation
    /// scheduled and never got to cancel becomes an inert send to a dead
    /// pid instead of landing in a later incarnation's mailbox. A worker
    /// cannot actually outlive its own incarnation's coordinator (its
    /// factory_supervisor is linked to, and dies with, that coordinator),
    /// so `AttemptReturned` could not have leaked across incarnations
    /// either way; routing it through this same incarnation-scoped subject
    /// keeps every internal channel consistent rather than relying on that
    /// cascade as the only reason it would have been safe.
    incarnation_subject: process.Subject(Message),
    auto_poll: Bool,
    policy: ValidatedPolicy,
    lease_duration_ms: Int,
    renewal_interval_ms: Int,
    /// How many `pending_ack` retry ticks (see `ActiveAttempt.pending_ack_ticks`)
    /// keep renewing the lease before giving up on renewal and letting it
    /// lapse — approximately one lease duration's worth of ticks
    /// (`lease_duration_ms / renewal_interval_ms`, at least 1), computed once
    /// alongside `renewal_interval_ms`. A persistently failing commit still
    /// keeps retrying the acknowledgement after this budget is spent (that
    /// stays safe and cheap), it just stops extending the lease — so the
    /// lease eventually expires, a claim-time quarantine scan (this
    /// consumer's own next poll, or another consumer's) can pick the row up,
    /// and the retry's next attempt observes that as an ordinary known
    /// `QueueAckStale(_, AckLeaseExpired(..))` rather than retrying forever.
    pending_ack_retry_budget: Int,
    active: List(ActiveAttempt(Completion, RenewalStatus)),
    /// True while a `Poll` timer is already scheduled against
    /// `incarnation_subject` and has not yet fired. `continue_if_idle` is the
    /// sole scheduler and checks this before arming another one, so free
    /// capacity (`maximum_concurrency` above `active`'s length) while other
    /// attempts are still running never accumulates more than one pending
    /// timer — see its doc comment.
    poll_scheduled: Bool,
    /// True while a `FillSlots` message is already outstanding in this
    /// incarnation's own mailbox and has not yet been handled. `request_fill`
    /// is the sole sender and checks this before sending another, for the
    /// same single-outstanding-message reason `poll_scheduled` guards `Poll`.
    /// Unlike `poll_scheduled` (a timer, armed only once idle), `request_fill`
    /// sends `FillSlots` unconditionally after every successful automatic
    /// claim — it never itself checks whether free capacity remains; that
    /// check happens only once the message is actually handled, inside
    /// `fill_automatic_slots` (`list.length(state.active) < maximum_concurrency`),
    /// which simply does nothing more (`continue_if_idle`) if none is left.
    /// Sending unconditionally rather than pre-checking capacity at the send
    /// site is deliberate, not an oversight: it is what makes a burst that
    /// fills many slots proceed one claim per message dispatch, returning to
    /// the mailbox after each one — see `fill_automatic_slots`'s doc comment
    /// for why this is what keeps `Renew`/`AttemptReturned`/`BeginShutdown`
    /// from starving behind a whole burst of claims. A harmless extra
    /// `FillSlots` dispatch once capacity is already full costs one mailbox
    /// round-trip, never a wasted claim attempt.
    fill_pending: Bool,
    hooks: Hooks,
    shutting_down: Bool,
    shutdown_generation: Int,
    shutdown_replies: List(process.Subject(ShutdownReply)),
  )
}

/// Starts a supervised, serial consumer under `policy`. Serial execution is
/// its concurrency bound, and an OTP child owns the process until `stop` is
/// called. `policy.polling` decides whether the consumer schedules its own
/// `Poll` timer (`PollEvery`) or waits for a caller to drive each attempt
/// through `process_one`/`process_batch` (`Manual`). Allocates one
/// atom-backed coordinator name (see `process.new_name`); bounded per call,
/// reused rather than recreated across any internal restart.
pub fn start(
  database: Database,
  workers: Registry,
  policy: ValidatedPolicy,
) -> Result(Consumer, StartError) {
  start_with_hooks(database, workers, policy, consumer_hooks.none())
}

/// `start`'s own implementation, plus test-only `Hooks` into one consumer's
/// worker-start sequence — see `consumer_hooks.Hooks`'s doc comment. Not
/// part of the public API: every publicly-started consumer runs under
/// `consumer_hooks.none()`.
@internal
pub fn start_with_hooks(
  database: Database,
  workers: Registry,
  policy: ValidatedPolicy,
  hooks: Hooks,
) -> Result(Consumer, StartError) {
  let queue_name = registry.queue(workers)
  let ValidatedPolicy(maximum_concurrency:, lease_duration_ms:, ..) = policy
  let minimum_lease_ms =
    minimum_lease_for_deadline(
      maximum_concurrency,
      postgres.statement_deadline_ms(database),
    )
  case registry.identities(workers), lease_duration_ms < minimum_lease_ms {
    [], _ -> Error(NoRegisteredWorkers)
    _, True ->
      Error(LeaseTooShortForDeadline(lease_duration_ms, minimum_lease_ms))
    _, False ->
      start_configured_consumer(database, workers, queue_name, policy, hooks)
  }
}

fn start_configured_consumer(
  database: Database,
  workers: Registry,
  queue_name: String,
  policy: ValidatedPolicy,
  hooks: Hooks,
) -> Result(Consumer, StartError) {
  // Created once per consumer, never inside the child start function: a
  // restarted coordinator incarnation re-registers this same name (the prior
  // registration is cleared by OTP when its owner dies), so `Consumer.subject`
  // keeps routing to whichever incarnation is currently alive. This bounds
  // atom creation to one name per `Consumer` value, not one per restart.
  let coordinator_name = process.new_name("grind_queue_coordinator")
  let handoff_reply = process.new_subject()
  let handoff_pid = queue_handoff.start(handoff_reply, process.self())
  case process.receive(handoff_reply, within: 5000) {
    Error(Nil) -> {
      process.kill(handoff_pid)
      Error(QueueActorHandoffFailed)
    }
    Ok(queue_handoff.HandoffSubjects(actor_ready, stop_handoff)) ->
      start_configured_consumer_with_handoff(
        database,
        workers,
        queue_name,
        policy,
        hooks,
        coordinator_name,
        handoff_reply,
        handoff_pid,
        actor_ready,
        stop_handoff,
      )
    Ok(queue_handoff.HandoffStarted(_)) -> {
      process.kill(handoff_pid)
      Error(QueueActorHandoffFailed)
    }
  }
}

fn start_configured_consumer_with_handoff(
  database: Database,
  workers: Registry,
  queue_name: String,
  policy: ValidatedPolicy,
  hooks: Hooks,
  coordinator_name: process.Name(Message),
  handoff_reply: process.Subject(queue_handoff.HandoffMessage(Message)),
  handoff_pid: process.Pid,
  actor_ready: process.Subject(process.Subject(Message)),
  stop_handoff: process.Subject(Nil),
) -> Result(Consumer, StartError) {
  let ValidatedPolicy(
    polling:,
    maximum_batch_jobs:,
    maximum_concurrency: _,
    lease_duration_ms:,
    shutdown_grace_ms:,
  ) = policy
  let auto_poll = case polling {
    PollEvery(_) -> True
    Manual -> False
  }
  let renewal_interval_ms = case lease_duration_ms / 3 > 0 {
    True -> lease_duration_ms / 3
    False -> 1
  }
  // See `ConsumerState.pending_ack_retry_budget`'s doc comment.
  let pending_ack_retry_budget = case lease_duration_ms / renewal_interval_ms {
    budget if budget > 0 -> budget
    _ -> 1
  }
  let worker_factory_builder =
    factory_supervisor.worker_child(fn(request) {
      actor.start(queue_worker.worker_actor(request))
    })
    |> factory_supervisor.restart_strategy(supervision.Temporary)
  let builder =
    actor.new_with_initialiser(1000, fn(subject) {
      // Computed fresh on every incarnation (initial start and every
      // supervised restart both run this closure), so each incarnation has
      // its own distinct owner label. attempt_owner is itself part of the
      // SQL fence checked alongside attempt_id and epoch on renewal,
      // release, and resolution (grind/internal/attempt's `renew`,
      // `release_unstarted`, `acknowledge`, and the resolution
      // route); attempt_id is already globally unique on its own (a
      // noncycling sequence), so this label does not change which row a
      // correct claim can match, but a distinct label per incarnation means
      // a coordinator that somehow still held stale in-memory claim state
      // from a previous incarnation could never accidentally satisfy a
      // fence it does not legitimately own.
      let attempt_owner = "grind-consumer-" <> int.to_string(unique_integer())
      // Fresh per incarnation, unlike `subject`: see the field's doc
      // comment on `ConsumerState` for why every self-scheduled timer uses
      // this instead of the named `subject`.
      let incarnation_subject = process.new_subject()
      queue_timing.start_polling(incarnation_subject, auto_poll, Poll)
      case factory_supervisor.start(worker_factory_builder) {
        Error(error) -> Error(string.inspect(error))
        Ok(started_factory) -> {
          let selector =
            process.new_selector()
            |> process.select(for: subject)
            |> process.select(for: incarnation_subject)
            |> process.select_monitors(fn(down) { WorkerDown(down) })
          Ok(
            actor.initialised(
              ConsumerState(
                database:,
                workers:,
                worker_factory: started_factory.data,
                queue: queue_name,
                attempt_owner:,
                subject:,
                incarnation_subject:,
                auto_poll:,
                policy:,
                lease_duration_ms:,
                renewal_interval_ms:,
                pending_ack_retry_budget:,
                active: [],
                poll_scheduled: auto_poll,
                fill_pending: False,
                hooks:,
                shutting_down: False,
                shutdown_generation: 0,
                shutdown_replies: [],
              ),
            )
            |> actor.selecting(selector)
            |> actor.returning(subject),
          )
        }
      }
    })
    |> actor.on_message(handle_message)
    |> actor.named(coordinator_name)
  let child =
    supervision.worker(fn() {
      case actor.start(builder) {
        Error(error) -> Error(error)
        Ok(started) -> {
          process.send(actor_ready, started.data)
          Ok(started)
        }
      }
    })
  let supervisor =
    static_supervisor.new(static_supervisor.OneForAll)
    |> static_supervisor.add(child)
  case static_supervisor.start(supervisor) {
    Error(error) -> {
      process.send(stop_handoff, Nil)
      process.kill(handoff_pid)
      Error(QueueSupervisorStartFailed(error))
    }
    Ok(started) ->
      case process.receive(handoff_reply, within: 5000) {
        Error(Nil) -> {
          process.send(stop_handoff, Nil)
          process.kill(handoff_pid)
          case stop_consumer_supervisor(started.pid) {
            Ok(Nil) -> Error(QueueActorHandoffFailed)
            Error(Nil) -> Error(QueueSupervisorStopTimedOut)
          }
        }
        Ok(queue_handoff.HandoffStarted(subject)) ->
          Ok(Consumer(
            subject,
            started.pid,
            maximum_batch_jobs,
            shutdown_grace_ms,
            process.self(),
          ))
        Ok(queue_handoff.HandoffSubjects(_, _)) -> {
          process.send(stop_handoff, Nil)
          process.kill(handoff_pid)
          case stop_consumer_supervisor(started.pid) {
            Ok(Nil) -> Error(QueueActorHandoffFailed)
            Error(Nil) -> Error(QueueSupervisorStopTimedOut)
          }
        }
      }
  }
}

/// Sends through a consumer's named coordinator subject. That name is a
/// fixed identity, not one incarnation's pid, so a send normally reaches
/// whichever incarnation the top-level supervisor currently has registered
/// under that name, including one restarted after the original crashed.
/// `process.send` panics if it resolves a named subject with nobody
/// currently registered (this consumer's coordinator has fully exited, for
/// example a repeated `stop` on an already-stopped consumer, or the rare
/// window where a crash lands between a caller's own liveness check and this
/// send); that panic is caught here and reported as an ordinary delivery
/// failure instead of propagating into the caller.
fn send_to_coordinator(
  consumer: Consumer,
  message: Message,
) -> Result(Nil, Nil) {
  let Consumer(subject:, ..) = consumer
  exception.rescue(fn() { process.send(subject, message) })
  |> result.map_error(fn(_) { Nil })
}

/// Sends a message built around a fresh reply subject and waits up to one
/// second for the answer, sharing the send/timeout/error shape used by every
/// `@internal` coordinator-introspection hook.
fn call_coordinator(
  consumer: Consumer,
  build_message: fn(process.Subject(reply)) -> Message,
) -> Result(reply, ProcessError) {
  let reply = process.new_subject()
  case send_to_coordinator(consumer, build_message(reply)) {
    Error(Nil) -> Error(QueueActorExited)
    Ok(Nil) ->
      process.receive(reply, within: 1000)
      |> result.map_error(fn(_) { QueueActorExited })
  }
}

/// Deterministically asks the queue actor to claim one due job now and waits
/// without an implicit deadline. A valid worker can run longer than the
/// polling interval; callers needing a deadline should run this operation in
/// their own process and define their timeout and reconciliation behavior.
///
/// The liveness check below resolves and monitors a specific coordinator
/// pid; the later send resolves the (possibly different, if a restart lands
/// in between) pid currently registered under this consumer's name. If a
/// restart wins that narrow race, this call reports `QueueActorExited` for
/// the pid it was actually watching even though a new incarnation may have
/// gone on to claim and run a job — there is no lost or duplicated delivery
/// from Grind's perspective (nothing was acknowledged twice), only a result
/// that undercounts what the fresh incarnation did. A caller that needs to
/// know whether work actually happened after such a result should re-check
/// job state rather than treat `QueueActorExited` as proof nothing ran.
pub fn process_one(consumer: Consumer) -> Result(Bool, ProcessError) {
  let Consumer(subject:, ..) = consumer
  case process.subject_owner(subject) {
    Error(Nil) -> Error(QueueActorExited)
    Ok(actor_pid) -> {
      case process.is_alive(actor_pid) {
        False -> Error(QueueActorExited)
        True -> {
          let monitor = process.monitor(actor_pid)
          case process.is_alive(actor_pid) {
            // OTP may omit a DOWN message when the target died just before
            // monitor/2 was installed, so check liveness on both sides.
            False -> {
              let _ = process.demonitor_process(monitor)
              Error(QueueActorExited)
            }
            True -> {
              let reply = process.new_subject()
              case send_to_coordinator(consumer, ProcessOne(reply)) {
                Error(Nil) -> {
                  let _ = process.demonitor_process(monitor)
                  Error(QueueActorExited)
                }
                Ok(Nil) -> {
                  let selector =
                    process.new_selector()
                    |> process.select_map(reply, fn(result) {
                      ProcessOneResult(result)
                    })
                    |> process.select_specific_monitor(monitor, fn(down) {
                      ProcessOneActorDown(down)
                    })
                  wait_for_process_one(actor_pid, monitor, selector)
                }
              }
            }
          }
        }
      }
    }
  }
}

fn wait_for_process_one(
  actor_pid: process.Pid,
  monitor: process.Monitor,
  selector: process.Selector(ProcessOneResponse),
) -> Result(Bool, ProcessError) {
  case process.selector_receive(selector, within: 1000) {
    Ok(ProcessOneResult(result)) -> {
      let _ = process.demonitor_process(monitor)
      result
    }
    Ok(ProcessOneActorDown(_)) -> Error(QueueActorExited)
    Error(Nil) ->
      case process.is_alive(actor_pid) {
        False -> {
          let _ = process.demonitor_process(monitor)
          Error(QueueActorExited)
        }
        True -> wait_for_process_one(actor_pid, monitor, selector)
      }
  }
}

/// Processes up to the configured batch size (`maximum_batch_jobs`),
/// stopping when no due job remains.
pub fn process_available(consumer: Consumer) -> BatchOutcome {
  let Consumer(maximum_batch_jobs:, ..) = consumer
  run_batch_from(consumer, maximum_batch_jobs, 0)
}

fn run_batch_from(
  consumer: Consumer,
  remaining_jobs: Int,
  acknowledged: Int,
) -> BatchOutcome {
  case remaining_jobs > 0 {
    False -> BatchCompleted(acknowledged)
    True ->
      case process_one(consumer) {
        Error(error) -> BatchStopped(acknowledged, error)
        Ok(False) -> BatchCompleted(acknowledged)
        Ok(True) ->
          run_batch_from(consumer, remaining_jobs - 1, acknowledged + 1)
      }
  }
}

type ShutdownOutcome {
  ShutdownReceived(ShutdownReply)
  /// Nobody is registered under this consumer's coordinator name at all, so
  /// there is nothing to ask to drain and no reply will ever come; reported
  /// to the caller as `StoppedWithoutDrain`, not as a clean stop.
  ShutdownAbsent
  ShutdownTimedOut
}

/// Stops the queue supervisor from the process that started it. The queue
/// pauses new claims and waits up to its configured grace for active workers.
pub fn stop(consumer: Consumer) -> Result(StopOutcome, StopError) {
  let Consumer(supervisor_pid:, shutdown_grace_ms:, owner_pid:, ..) = consumer
  case process.self() == owner_pid {
    False -> Error(ConsumerOwnedByAnotherProcess)
    True -> {
      let shutdown = request_shutdown(consumer, shutdown_grace_ms)
      case stop_consumer_supervisor(supervisor_pid) {
        Error(Nil) -> Error(ConsumerStopTimedOut)
        Ok(Nil) ->
          case shutdown {
            ShutdownReceived(ShutdownDrained) -> Ok(StoppedCleanly)
            ShutdownReceived(ShutdownForced(active)) ->
              Ok(StoppedWithActiveWork(active))
            ShutdownAbsent -> Ok(StoppedWithoutDrain)
            ShutdownTimedOut -> Ok(StoppedDrainUnconfirmed)
          }
      }
    }
  }
}

fn request_shutdown(
  consumer: Consumer,
  shutdown_grace_ms: Int,
) -> ShutdownOutcome {
  let reply = process.new_subject()
  // Reaches whichever coordinator incarnation is currently registered under
  // this consumer's name, even if the original one has since crashed and
  // been restarted by the top-level supervisor.
  case send_to_coordinator(consumer, BeginShutdown(reply)) {
    Error(Nil) -> ShutdownAbsent
    Ok(Nil) ->
      case process.receive(reply, within: shutdown_grace_ms + 1000) {
        Ok(shutdown_reply) -> ShutdownReceived(shutdown_reply)
        Error(Nil) -> ShutdownTimedOut
      }
  }
}

/// Test hook that begins the coordinator's shutdown transition the same way
/// `stop` does, but without `stop`'s single-owner check or its subsequent
/// supervisor teardown, and with the reply subject supplied by the caller
/// instead of consumed internally. This lets a test drive a coordinator
/// through "draining with active work" (and therefore through scheduling its
/// grace timer) from any process, independent of the one owner process that
/// may legitimately call the public, blocking `stop`.
@internal
pub fn begin_shutdown_for_test(
  consumer: Consumer,
  reply: process.Subject(ShutdownReply),
) -> Result(Nil, ProcessError) {
  case send_to_coordinator(consumer, BeginShutdown(reply)) {
    Error(Nil) -> Error(QueueActorExited)
    Ok(Nil) -> Ok(Nil)
  }
}

/// Reports whether the coordinator has begun its shutdown transition.
/// This internal observation supports deterministic lifecycle synchronization.
@internal
pub fn shutdown_state(consumer: Consumer) -> Result(Bool, ProcessError) {
  call_coordinator(consumer, ReadShutdownState)
}

@internal
pub fn renewal_status(
  consumer: Consumer,
) -> Result(Option(RenewalStatus), ProcessError) {
  call_coordinator(consumer, ReadRenewalStatus)
}

/// Returns the coordinator incarnation owned by this consumer handle.
@internal
pub fn coordinator_pid(consumer: Consumer) -> Result(process.Pid, Nil) {
  let Consumer(subject:, ..) = consumer
  process.subject_owner(subject)
}

/// Returns the OTP supervisor process owned by this consumer handle.
@internal
pub fn supervisor_pid(consumer: Consumer) -> process.Pid {
  let Consumer(supervisor_pid:, ..) = consumer
  supervisor_pid
}

@external(erlang, "grind_postgres_ffi", "stop_consumer_supervisor")
fn stop_consumer_supervisor(pid: process.Pid) -> Result(Nil, Nil)

@external(erlang, "erlang", "unique_integer")
fn unique_integer() -> Int

fn handle_message(
  state: ConsumerState,
  message: Message,
) -> actor.Next(ConsumerState, Message) {
  case message {
    Poll -> {
      // This message is the one outstanding timer `poll_scheduled` was
      // tracking (or the initial kick `start_polling` sent) — clear it
      // before anything else so `continue_if_idle` is free to arm the next
      // one, on this round or a later one, regardless of which branch below
      // is taken. Filling itself is unconditional (Oban-like): try to claim
      // into every free slot right away, backing off to the next `Poll`
      // timer only once a claim actually finds nothing (see
      // `fill_automatic_slots`/`continue_if_idle`) — there is no separate
      // per-poll claim budget to reset here any more.
      let state = ConsumerState(..state, poll_scheduled: False)
      case state.shutting_down {
        True -> actor.continue(state)
        False -> fill_automatic_slots(state)
      }
    }
    FillSlots -> {
      // The one outstanding `FillSlots` message `fill_pending` was tracking:
      // clear it before anything else, exactly like `Poll` above clears
      // `poll_scheduled`. A `BeginShutdown` that landed ahead of this message
      // in the mailbox already set `shutting_down`, so this step correctly
      // stops filling rather than starting another claim — see
      // `fill_automatic_slots`'s doc comment.
      let state = ConsumerState(..state, fill_pending: False)
      case state.shutting_down {
        True -> actor.continue(state)
        False -> fill_automatic_slots(state)
      }
    }
    BeginShutdown(reply) -> begin_shutdown(state, reply)
    ShutdownGraceExpired(generation) ->
      case generation == state.shutdown_generation {
        False -> actor.continue(state)
        True -> {
          list.each(state.shutdown_replies, fn(waiter) {
            process.send(waiter, ShutdownForced(list.length(state.active)))
          })
          actor.continue(ConsumerState(..state, shutdown_replies: []))
        }
      }
    ReadShutdownState(reply) -> {
      process.send(reply, state.shutting_down)
      actor.continue(state)
    }
    Renew(attempt_id, epoch, generation) ->
      renew_active_attempt(state, attempt_id, epoch, generation)
    ReadRenewalStatus(reply) -> {
      let status = case state.active {
        [] -> None
        [active, ..] -> Some(active.renewal_status)
      }
      process.send(reply, status)
      actor.continue(state)
    }
    ProcessOne(reply) -> {
      let ValidatedPolicy(maximum_concurrency:, ..) = state.policy
      case state.shutting_down {
        True -> {
          process.send(reply, Error(QueueShuttingDown))
          actor.continue(state)
        }
        False ->
          case list.length(state.active) >= maximum_concurrency {
            True -> {
              process.send(reply, Error(QueueBusy))
              actor.continue(state)
            }
            False -> start_attempt(state, ManualCompletion(reply))
          }
      }
    }
    AttemptReturned(id, attempt_id, epoch, execution) ->
      finish_attempt(state, id, attempt_id, epoch, execution)
    WorkerDown(down) -> handle_worker_down(state, down)
  }
}

fn begin_shutdown(
  state: ConsumerState,
  reply: process.Subject(ShutdownReply),
) -> actor.Next(ConsumerState, Message) {
  case list.is_empty(state.active) {
    True -> {
      process.send(reply, ShutdownDrained)
      list.each(state.shutdown_replies, fn(waiter) {
        process.send(waiter, ShutdownDrained)
      })
      actor.continue(
        ConsumerState(..state, shutting_down: True, shutdown_replies: []),
      )
    }
    False ->
      case list.is_empty(state.shutdown_replies) {
        False ->
          actor.continue(
            ConsumerState(
              ..state,
              shutting_down: True,
              shutdown_replies: list.prepend(state.shutdown_replies, reply),
            ),
          )
        True -> {
          let ValidatedPolicy(shutdown_grace_ms:, ..) = state.policy
          let generation =
            next_shutdown_generation(state.shutdown_generation, False)
          let _ =
            process.send_after(
              state.incarnation_subject,
              shutdown_grace_ms,
              ShutdownGraceExpired(generation),
            )
          actor.continue(
            ConsumerState(
              ..state,
              shutting_down: True,
              shutdown_generation: generation,
              shutdown_replies: [reply],
            ),
          )
        }
      }
  }
}

fn start_attempt(
  state: ConsumerState,
  completion: Completion,
) -> actor.Next(ConsumerState, Message) {
  let ConsumerState(
    database:,
    workers:,
    worker_factory:,
    queue:,
    attempt_owner:,
    incarnation_subject:,
    lease_duration_ms:,
    renewal_interval_ms:,
    hooks:,
    ..,
  ) = state
  case
    attempt.claim_one(
      database,
      queue,
      workers,
      attempt_owner,
      lease_duration_ms,
    )
  {
    Error(error) ->
      finish_without_claim(state, completion, Error(QueueProcessFailed(error)))
    Ok(None) -> finish_without_claim(state, completion, Ok(False))
    Ok(Some(claimed)) -> {
      let #(id, attempt_id, epoch) = attempt.claim_identity(claimed)
      let request =
        queue_worker.WorkerRequest(
          incarnation_subject,
          claimed,
          fn(id, attempt_id, epoch, execution) {
            AttemptReturned(id, attempt_id, epoch, execution)
          },
        )
      let start_result = case hooks.before_worker_start() {
        Error(reason) -> Error(actor.InitFailed(reason))
        Ok(Nil) -> factory_supervisor.start_child(worker_factory, request)
      }
      case start_result {
        Error(error) ->
          release_failed_worker_start(state, completion, claimed, error)
        Ok(started) -> {
          hooks.after_worker_start(started.pid)
          let monitor = process.monitor(started.pid)
          case process.is_alive(started.pid) {
            False -> {
              let _ = process.demonitor_process(monitor)
              release_dead_worker_before_activation(state, completion, claimed)
            }
            True -> {
              process.send(started.data, queue_worker.StartAttempt)
              let active =
                ActiveAttempt(
                  id:,
                  attempt_id:,
                  epoch:,
                  claimed:,
                  worker_subject: started.data,
                  monitor:,
                  completion:,
                  renewal_status: LeaseRenewalConfirmed,
                  pending_ack: None,
                  pending_ack_ticks: 0,
                  renewal_generation: 0,
                )
              let _ =
                process.send_after(
                  incarnation_subject,
                  renewal_interval_ms,
                  Renew(attempt_id, epoch, 0),
                )
              let state =
                ConsumerState(
                  ..state,
                  active: list.prepend(state.active, active),
                )
              continue_after_start(state, completion)
            }
          }
        }
      }
    }
  }
}

fn release_dead_worker_before_activation(
  state: ConsumerState,
  completion: Completion,
  claimed: attempt.ClaimedJob,
) -> actor.Next(ConsumerState, Message) {
  case
    attempt.release_unstarted(
      state.database,
      state.queue,
      state.attempt_owner,
      claimed,
    )
  {
    Ok(True) ->
      finish_without_claim(
        state,
        completion,
        Error(QueueWorkerExitedBeforeActivation),
      )
    Ok(False) ->
      finish_without_claim(
        state,
        completion,
        Error(QueueWorkerActivationClaimLost),
      )
    Error(error) ->
      finish_without_claim(
        state,
        completion,
        Error(QueueWorkerActivationReleaseFailed(error)),
      )
  }
}

/// Polls (1ms apiece) until `pid` is no longer alive or `checks_remaining`
/// is spent. Exposed only so a test's own `Hooks.after_worker_start` can
/// wait out a worker it just killed before this coordinator's own
/// `process.monitor` call — see `consumer_hooks.Hooks`.
@internal
pub fn wait_for_worker_exit(pid: process.Pid, checks_remaining: Int) -> Nil {
  case process.is_alive(pid), checks_remaining > 0 {
    False, _ -> Nil
    True, False -> Nil
    True, True -> {
      process.sleep(1)
      wait_for_worker_exit(pid, checks_remaining - 1)
    }
  }
}

fn release_failed_worker_start(
  state: ConsumerState,
  completion: Completion,
  claimed: attempt.ClaimedJob,
  start_error: actor.StartError,
) -> actor.Next(ConsumerState, Message) {
  case
    attempt.release_unstarted(
      state.database,
      state.queue,
      state.attempt_owner,
      claimed,
    )
  {
    Ok(True) ->
      finish_without_claim(
        state,
        completion,
        Error(QueueWorkerStartFailed(start_error)),
      )
    Ok(False) ->
      finish_without_claim(
        state,
        completion,
        Error(QueueWorkerStartClaimLost(start_error)),
      )
    Error(release_error) ->
      finish_without_claim(
        state,
        completion,
        Error(QueueWorkerStartReleaseFailed(start_error, release_error)),
      )
  }
}

fn finish_without_claim(
  state: ConsumerState,
  completion: Completion,
  result: Result(Bool, ProcessError),
) -> actor.Next(ConsumerState, Message) {
  case completion {
    ManualCompletion(reply) -> process.send(reply, result)
    Automatic -> Nil
  }
  // A claim that found nothing (or failed outright) is exactly "idle": stop
  // trying to fill more slots this round and let `continue_if_idle` arm the
  // next `Poll` timer at the full interval, rather than looping straight
  // back into `fill_automatic_slots`.
  continue_if_idle(state)
}

fn finish_attempt(
  state: ConsumerState,
  id: Int,
  attempt_id: Int,
  epoch: Int,
  execution: worker.Execution,
) -> actor.Next(ConsumerState, Message) {
  case queue_active.find_active(state.active, id, attempt_id, epoch) {
    Error(Nil) -> actor.continue(state)
    Ok(active) -> {
      let ActiveAttempt(claimed:, worker_subject:, monitor:, ..) = active
      process.demonitor_process(monitor)
      process.send(worker_subject, queue_worker.StopWorker)
      let result =
        attempt.acknowledge(
          state.database,
          state.queue,
          state.attempt_owner,
          claimed,
          execution,
        )
      finalize_ack_result(state, active, result)
    }
  }
}

/// Shared by a first acknowledgement attempt (`finish_attempt`) and every
/// retry of one still `pending_ack` (`retry_pending_ack`): in `Automatic`
/// mode only, `QueueAckUnknown` keeps the attempt `active` and schedules
/// another retry. A retry already in flight (`pending_ack: Some`) also
/// retries on a plain `QueueAckFailed` — the transaction callback failed so
/// `COMMIT` was never sent (the same "genuinely did not commit" reasoning
/// `submission.NotCommitted`'s doc comment gives), but surfacing that here
/// would silently drop the claim in `Automatic` mode exactly like an
/// unhandled `QueueAckUnknown` would, and retrying costs nothing extra since
/// `command_id` already makes it idempotent; a *first* attempt's own
/// `QueueAckFailed` is unaffected and still resolves immediately, unchanged.
/// Anything else — success, one of the ack's own known-outcome errors
/// (`QueueAckStale`, `QueueAckCommandConflict`, ...), or any result at all
/// under `ManualCompletion` — resolves it exactly like an ordinary
/// first-attempt result always has, which for `Automatic` completion means
/// `finish_completion` calling straight through to `fill_automatic_slots`/
/// `continue_if_idle`, so a slot a resolved retry frees is reused promptly.
/// `ManualCompletion` deliberately never retries: a `process_one` caller already gets
/// `QueueAckUnknown` back synchronously today and can already call
/// `reconcile_acknowledgement` itself; only automatic mode had no caller
/// left to hand an unknown ack to, which is the gap this fixes.
fn finalize_ack_result(
  state: ConsumerState,
  active: ActiveAttempt(Completion, RenewalStatus),
  result: Result(Bool, postgres.QueueRunError),
) -> actor.Next(ConsumerState, Message) {
  case result, active.completion, active.pending_ack {
    Error(postgres.QueueAckUnknown(_, proposed)), Automatic, _ ->
      retry_ack_until_known(state, active, proposed)
    Error(postgres.QueueAckFailed(_)), Automatic, Some(proposed) ->
      retry_ack_until_known(state, active, proposed)
    _, _, _ -> {
      let ActiveAttempt(id:, attempt_id:, epoch:, completion:, ..) = active
      let state =
        ConsumerState(
          ..state,
          active: queue_active.remove_active(
            state.active,
            id,
            attempt_id,
            epoch,
          ),
        )
      finish_completion(
        state,
        completion,
        result |> result.map_error(QueueProcessFailed),
      )
    }
  }
}

/// Marks `active` as `pending_ack` (rather than removing it) and arms one
/// more retry on this incarnation's renewal timer, reusing the renewal
/// interval as its cadence per the `ActiveAttempt.pending_ack` doc comment.
/// `renewal_generation` is bumped only the first time this fires for a given
/// attempt (`pending_ack` still `None` on entry): exactly one ordinary
/// renewal timer is always already outstanding at that point (the chain
/// `start_attempt`/`renew_lease` maintains), and bumping the generation makes
/// `renew_active_attempt` ignore that leftover tick as stale rather than
/// running a second, overlapping timer chain — see `renew_active_attempt`'s
/// doc comment. A later call for the same still-`Some` attempt (another
/// `QueueAckUnknown`/`QueueAckFailed` on a retry already in flight) keeps the
/// same generation: by then the leftover ordinary timer has already fired
/// and been consumed by this exact chain, so there is nothing left to
/// invalidate.
fn retry_ack_until_known(
  state: ConsumerState,
  active: ActiveAttempt(Completion, RenewalStatus),
  execution: worker.Execution,
) -> actor.Next(ConsumerState, Message) {
  let ActiveAttempt(
    id:,
    attempt_id:,
    epoch:,
    pending_ack:,
    renewal_generation:,
    ..,
  ) = active
  let generation = case pending_ack {
    None -> renewal_generation + 1
    Some(_) -> renewal_generation
  }
  let _ =
    process.send_after(
      state.incarnation_subject,
      state.renewal_interval_ms,
      Renew(attempt_id, epoch, generation),
    )
  actor.continue(
    ConsumerState(
      ..state,
      active: queue_active.replace_active(
        state.active,
        id,
        attempt_id,
        epoch,
        ActiveAttempt(
          ..active,
          pending_ack: Some(execution),
          renewal_generation: generation,
        ),
      ),
    ),
  )
}

/// Performs one `pending_ack` retry tick. Renews the lease first — fenced to
/// `executing`, this exact `attempt_id`/`epoch`/`attempt_owner`, and a still
/// live lease, exactly like an ordinary in-progress attempt's renewal — for
/// as long as `pending_ack_ticks` stays under `ConsumerState.
/// pending_ack_retry_budget`; a not-yet-committed retry's own acknowledgement
/// UPDATE is *itself* fenced the same way (`lease.live_lease_predicate`),
/// so renewing is what keeps that path retriable rather than merely
/// "possible in principle": once the lease is gone, only a receipt-matched
/// commit can still resolve it. If the original attempt's transaction
/// actually committed already (a lost reply, not an abort), `renew`'s
/// own fence no longer matches (the row is no longer `executing` under this
/// attempt) and it harmlessly reports `Ok(False)`; the acknowledgement retry
/// right after it reconciles from the now-visible receipt regardless — that
/// path never depended on the lease. Once the budget is spent, this stops
/// renewing (and lets the lease lapse) but keeps retrying the
/// acknowledgement itself, which stays cheap and safe; a persistently
/// failing commit then converges on a known `QueueAckStale(_,
/// AckLeaseExpired(..))` once the lease is truly gone, ending the retry
/// chain — not a new failure mode, the same lease-expiry-to-`uncertain`
/// recovery this codebase already relies on elsewhere, just reached instead
/// of retried forever.
fn retry_pending_ack(
  state: ConsumerState,
  active: ActiveAttempt(Completion, RenewalStatus),
  execution: worker.Execution,
) -> actor.Next(ConsumerState, Message) {
  let ActiveAttempt(claimed:, pending_ack_ticks:, ..) = active
  case pending_ack_ticks < state.pending_ack_retry_budget {
    True -> {
      let _ =
        attempt.renew(
          state.database,
          state.queue,
          state.attempt_owner,
          claimed,
          state.lease_duration_ms,
        )
      Nil
    }
    False -> Nil
  }
  let result =
    attempt.acknowledge(
      state.database,
      state.queue,
      state.attempt_owner,
      claimed,
      execution,
    )
  let active = ActiveAttempt(..active, pending_ack_ticks: pending_ack_ticks + 1)
  finalize_ack_result(state, active, result)
}

fn finish_completion(
  state: ConsumerState,
  completion: Completion,
  result: Result(Bool, ProcessError),
) -> actor.Next(ConsumerState, Message) {
  case completion {
    ManualCompletion(reply) -> {
      process.send(reply, result)
      continue_if_idle(state)
    }
    Automatic ->
      case result {
        Ok(True) -> fill_automatic_slots(state)
        Ok(False) | Error(_) -> continue_if_idle(state)
      }
  }
}

/// Claims into one free slot, if any is free and this consumer is not
/// shutting down — Oban-like: there is no separate per-poll claim budget, so
/// a backlog drains as fast as capacity allows instead of at most one claim
/// (or `maximum_jobs_per_poll`, before this change) per `Poll` timer tick.
/// Filling *more* than one slot in a row is not done by recursing here
/// directly: a successful automatic claim that leaves free capacity behind
/// asks `continue_after_start` to send this coordinator's own incarnation a
/// `FillSlots` message instead (`request_fill`), so this function itself
/// only ever performs at most one claim per call. That message goes to the
/// back of the same mailbox `Renew`, `AttemptReturned`, and `BeginShutdown`
/// also arrive on, so a burst that fills many slots interleaves with those
/// messages one claim at a time — each one gets to run between successive
/// claims, rather than waiting out an entire burst of up to
/// `maximum_concurrency` claims before this coordinator's message loop comes
/// up for air again. (An earlier version of this function recursed directly
/// into `start_attempt`/`continue_after_start` from inside one message
/// handler, so a burst that filled every free slot could stall a pending
/// lease renewal or a shutdown request behind all of it — see `docs/RISKS.md`
/// risks 4 and 5.) `attempt.claim_one`'s own claim-time quarantine scan (see
/// its doc comment) still runs on every one of these calls; that stays
/// bounded by real progress, not by wall-clock time, because every call here
/// either starts a genuine attempt (shrinking the free-slot count by one) or
/// finds no job and stops immediately via `continue_if_idle` — never
/// spinning and finding nothing on the same call, so this can never become a
/// hot loop (see `docs/RISKS.md` risk 6 for the fixed per-claim cost that
/// remains).
fn fill_automatic_slots(
  state: ConsumerState,
) -> actor.Next(ConsumerState, Message) {
  let ValidatedPolicy(maximum_concurrency:, ..) = state.policy
  case !state.shutting_down && list.length(state.active) < maximum_concurrency {
    True -> start_attempt(state, Automatic)
    False -> continue_if_idle(state)
  }
}

/// Asks this coordinator's own incarnation to run `fill_automatic_slots`
/// again, via a `FillSlots` message rather than a direct call, so any
/// message already ahead of it in the mailbox (a lease renewal, a worker's
/// completion, a shutdown request) is handled first. `fill_pending` is the
/// single-outstanding-message guard: a completion that arrives while a
/// `FillSlots` is already queued does not send a second one, exactly like
/// `continue_if_idle`'s `poll_scheduled` guard for `Poll`.
fn request_fill(state: ConsumerState) -> actor.Next(ConsumerState, Message) {
  case state.fill_pending {
    True -> actor.continue(state)
    False -> {
      process.send(state.incarnation_subject, FillSlots)
      actor.continue(ConsumerState(..state, fill_pending: True))
    }
  }
}

fn continue_after_start(
  state: ConsumerState,
  completion: Completion,
) -> actor.Next(ConsumerState, Message) {
  case completion {
    ManualCompletion(_) -> actor.continue(state)
    Automatic -> request_fill(state)
  }
}

/// Called at the end of every poll round (a claim just found nothing or
/// failed, or an active attempt just finished). Arms the next `Poll` timer
/// whenever this consumer is not shutting down, `auto_poll` is on, no timer
/// is already outstanding, and free capacity remains (`active` below
/// `maximum_concurrency`) — not only when `active` is fully empty. A
/// `maximum_concurrency` above 1 otherwise leaves spare slots idle for as
/// long as one attempt keeps running: with an empty-only check, a newly due
/// job (or the claim-time expired-lease quarantine scan, which piggybacks on
/// the same claim query) had to wait for every currently active attempt to
/// finish before the next poll was even scheduled, no matter how much
/// capacity was actually free in the meantime. Reaching this function at all
/// already means the immediately preceding claim (if any) found nothing to
/// claim right now — a slot freeing up later calls `fill_automatic_slots`
/// directly instead, so a fresh claim attempt is never delayed behind this
/// timer while capacity is genuinely free. `poll_scheduled` is the
/// single-outstanding-timer guard this relies on: an active attempt
/// finishing while a timer from an earlier round is already pending must
/// not arm a second, overlapping one (see the field's doc comment).
/// Shutdown draining is unaffected — a consumer already shutting down never
/// re-arms a poll regardless of capacity.
fn continue_if_idle(
  state: ConsumerState,
) -> actor.Next(ConsumerState, Message) {
  case state.shutting_down {
    True ->
      case list.is_empty(state.active) {
        True -> {
          list.each(state.shutdown_replies, fn(reply) {
            process.send(reply, ShutdownDrained)
          })
          actor.continue(ConsumerState(..state, shutdown_replies: []))
        }
        False -> actor.continue(state)
      }
    False -> {
      let ValidatedPolicy(maximum_concurrency:, ..) = state.policy
      case
        state.auto_poll
        && !state.poll_scheduled
        && list.length(state.active) < maximum_concurrency
      {
        True -> {
          schedule_next_poll(state)
          actor.continue(ConsumerState(..state, poll_scheduled: True))
        }
        False -> actor.continue(state)
      }
    }
  }
}

/// This incarnation's renewal timer fires once per active attempt every
/// `renewal_interval_ms`. A `pending_ack` attempt (its worker has already
/// returned; see `ActiveAttempt`'s doc comment) repurposes this same tick to
/// retry its acknowledgement instead of renewing a lease nothing is running
/// against.
/// This incarnation's renewal timer fires once per active attempt every
/// `renewal_interval_ms`. A `pending_ack` attempt (its worker has already
/// returned; see `ActiveAttempt`'s doc comment) repurposes this same tick to
/// renew-then-retry its acknowledgement instead of only renewing a lease.
/// `tick_generation` must match the attempt's own current
/// `renewal_generation`, not just its `attempt_id`/`epoch`
/// (`renewal_is_current`): the latter alone guards against a stale tick from
/// a *different* attempt that happens to reuse this row, but not against two
/// overlapping timer chains for the *same* still-active attempt — exactly
/// what would otherwise happen the moment `retry_ack_until_known` arms a
/// pending-retry timer while an ordinary renewal timer from before is still
/// outstanding. A generation mismatch means this exact tick belongs to an
/// invalidated chain; it is dropped with no further scheduling, because the
/// chain that replaced it already has its own outstanding timer.
fn renew_active_attempt(
  state: ConsumerState,
  tick_attempt_id: Int,
  tick_epoch: Int,
  tick_generation: Int,
) -> actor.Next(ConsumerState, Message) {
  case
    queue_active.find_active_by_attempt(
      state.active,
      tick_attempt_id,
      tick_epoch,
    )
  {
    Error(Nil) -> actor.continue(state)
    Ok(active) -> {
      let ActiveAttempt(
        attempt_id:,
        epoch:,
        pending_ack:,
        renewal_generation:,
        ..,
      ) = active
      case
        renewal_is_current(attempt_id, epoch, tick_attempt_id, tick_epoch)
        && tick_generation == renewal_generation
      {
        False -> actor.continue(state)
        True ->
          case pending_ack {
            Some(execution) -> retry_pending_ack(state, active, execution)
            None -> renew_lease(state, active)
          }
      }
    }
  }
}

fn renew_lease(
  state: ConsumerState,
  active: ActiveAttempt(Completion, RenewalStatus),
) -> actor.Next(ConsumerState, Message) {
  let ActiveAttempt(attempt_id:, epoch:, renewal_generation:, ..) = active
  let result =
    attempt.renew(
      state.database,
      state.queue,
      state.attempt_owner,
      active.claimed,
      state.lease_duration_ms,
    )
  case result {
    Ok(attempt.Renewed) -> {
      let state =
        set_renewal_status(state, attempt_id, epoch, LeaseRenewalConfirmed)
      let _ =
        process.send_after(
          state.incarnation_subject,
          state.renewal_interval_ms,
          Renew(attempt_id, epoch, renewal_generation),
        )
      actor.continue(state)
    }
    Ok(attempt.LeaseLost) ->
      actor.continue(set_renewal_status(
        state,
        attempt_id,
        epoch,
        LeaseRenewalLost,
      ))
    Error(_) -> {
      let state =
        set_renewal_status(state, attempt_id, epoch, LeaseRenewalUnknown)
      let _ =
        process.send_after(
          state.incarnation_subject,
          state.renewal_interval_ms,
          Renew(attempt_id, epoch, renewal_generation),
        )
      actor.continue(state)
    }
  }
}

fn set_renewal_status(
  state: ConsumerState,
  attempt_id: Int,
  epoch: Int,
  renewal_status: RenewalStatus,
) -> ConsumerState {
  let active =
    queue_active.set_renewal_status(
      state.active,
      attempt_id,
      epoch,
      renewal_status,
    )
  ConsumerState(..state, active:)
}

fn handle_worker_down(
  state: ConsumerState,
  down: process.Down,
) -> actor.Next(ConsumerState, Message) {
  case down {
    process.ProcessDown(monitor: down_monitor, ..) ->
      case
        list.find(state.active, fn(active) {
          let ActiveAttempt(monitor:, ..) = active
          monitor == down_monitor
        })
      {
        Error(Nil) -> actor.continue(state)
        Ok(active) -> {
          let ActiveAttempt(id:, attempt_id:, epoch:, monitor:, completion:, ..) =
            active
          let _ = process.demonitor_process(monitor)
          let state =
            ConsumerState(
              ..state,
              active: queue_active.remove_active(
                state.active,
                id,
                attempt_id,
                epoch,
              ),
            )
          case completion {
            ManualCompletion(reply) ->
              process.send(reply, Error(QueueWorkerExited))
            Automatic -> Nil
          }
          // An unexpected worker exit backs off to the next scheduled poll
          // the same way an empty/failed claim does, rather than
          // immediately trying to fill the slot it just freed — see
          // `continue_if_idle`.
          continue_if_idle(state)
        }
      }
    process.PortDown(..) -> actor.continue(state)
  }
}

fn schedule_next_poll(state: ConsumerState) -> Nil {
  let ValidatedPolicy(polling:, ..) = state.policy
  let poll_interval_ms = case polling {
    PollEvery(interval_ms) -> interval_ms
    Manual -> 0
  }
  queue_timing.schedule_poll(
    state.incarnation_subject,
    state.auto_poll,
    poll_interval_ms,
    Poll,
  )
}
