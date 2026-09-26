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
import grind/postgres.{type Database}
import grind/registry.{type Registry}
import grind/worker

/// An independently supervised consumer for one configured queue. The process
/// that starts the consumer owns its supervisor and must also stop it.
pub opaque type Consumer {
  Consumer(
    subject: process.Subject(Message),
    supervisor_pid: process.Pid,
    maximum_jobs_per_poll: Int,
    shutdown_grace_ms: Int,
    owner_pid: process.Pid,
  )
}

/// Queue-local polling, lease, and execution-capacity settings.
pub type QueuePolicy {
  QueuePolicy(
    poll_interval_ms: Int,
    maximum_jobs_per_poll: Int,
    maximum_concurrency: Int,
    lease_duration_ms: Int,
    shutdown_grace_ms: Int,
  )
}

pub fn default_policy() -> QueuePolicy {
  QueuePolicy(
    poll_interval_ms: 250,
    maximum_jobs_per_poll: 1,
    maximum_concurrency: 1,
    lease_duration_ms: 30_000,
    shutdown_grace_ms: 5000,
  )
}

/// Reports whether a renewal timer still belongs to the active attempt.
@internal
pub fn renewal_is_current(
  active_attempt_id: Int,
  active_epoch: Int,
  tick_attempt_id: Int,
  tick_epoch: Int,
) -> Bool {
  active_attempt_id == tick_attempt_id && active_epoch == tick_epoch
}

/// Starts one shutdown grace period, or keeps its generation when another
/// caller joins the already pending drain.
@internal
pub fn next_shutdown_generation(
  current_generation: Int,
  shutdown_already_pending: Bool,
) -> Int {
  case shutdown_already_pending {
    True -> current_generation
    False -> current_generation + 1
  }
}

pub fn with_poll_interval(
  policy: QueuePolicy,
  poll_interval_ms: Int,
) -> QueuePolicy {
  QueuePolicy(..policy, poll_interval_ms:)
}

pub fn with_maximum_jobs_per_poll(
  policy: QueuePolicy,
  maximum_jobs_per_poll: Int,
) -> QueuePolicy {
  QueuePolicy(..policy, maximum_jobs_per_poll:)
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
  MaximumJobsPerPollMustBePositive
  MaximumConcurrencyMustBePositive
  LeaseDurationMustBePositive
  ShutdownGraceMustBeNonNegative
}

pub opaque type ValidatedPolicy {
  ValidatedPolicy(
    poll_interval_ms: Int,
    maximum_jobs_per_poll: Int,
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
    poll_interval_ms:,
    maximum_jobs_per_poll:,
    maximum_concurrency:,
    lease_duration_ms:,
    shutdown_grace_ms:,
  ) = policy
  case poll_interval_ms > 0 {
    False -> Error(PollIntervalMustBePositive)
    True ->
      case maximum_jobs_per_poll > 0 {
        False -> Error(MaximumJobsPerPollMustBePositive)
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
                        poll_interval_ms:,
                        maximum_jobs_per_poll:,
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
  RegistryQueueMismatch
  NoRegisteredWorkers
  QueueActorStartFailed(actor.StartError)
  QueueSupervisorStartFailed(actor.StartError)
  QueueActorHandoffFailed
  QueueSupervisorStopTimedOut
  /// `lease_duration_ms` is too short relative to `database`'s own
  /// `postgres.Settings.statement_deadline_ms` (`D`) for the coordinator's
  /// own recovery machinery to have a real chance to act before the lease
  /// lapses. Derivation: a live attempt's own renewal timer fires every
  /// `L / 3` (`renewal_interval_ms`, `start_with_policy`), so after a
  /// renewal succeeds there is `L - L / 3 = (2 / 3) * L` of slack before
  /// that same lease would otherwise expire. The coordinator's single
  /// message loop can block for up to roughly `3 * D` retrying one pending
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
  /// at that minimum. `attempted` is the lease this call was given;
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
  LeaseTooShortForDeadline(attempted: Int, minimum_ms: Int)
}

/// The lease-rule minimum for `maximum_concurrency` and a storage deadline
/// `D` (`postgres.statement_deadline_ms`) — see `LeaseTooShortForDeadline`.
@internal
pub fn minimum_lease_for_deadline(
  maximum_concurrency: Int,
  statement_deadline_ms: Int,
) -> Int {
  case maximum_concurrency > 1 {
    True -> 6 * statement_deadline_ms
    False -> 3 * statement_deadline_ms / 2
  }
}

pub type StopError {
  ConsumerOwnedByAnotherProcess
  ConsumerStopTimedOut
  ConsumerDrainTimedOut
}

pub type StopOutcome {
  /// The coordinator this call reached drained cleanly with no active work.
  /// This describes only the incarnation `stop` actually talked to: it does
  /// not mean no attempt was ever abandoned by an earlier incarnation that
  /// crashed before this call — an abandoned attempt is recovered only
  /// through lease expiry moving it to `Uncertain` and an audited
  /// resolution, independent of what any later `stop` call reports.
  StoppedCleanly
  StoppedWithActiveWork(Int)
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
  BeginShutdown(process.Subject(ShutdownReply))
  ShutdownGraceExpired(Int)
  ReadShutdownState(process.Subject(Bool))
  Renew(attempt_id: Int, epoch: Int, generation: Int)
  ReadRenewalStatus(reply: process.Subject(Option(RenewalStatus)))
  InjectWorkerStartFailure(reply: process.Subject(Nil))
  KillNextWorkerBeforeMonitor(reply: process.Subject(Nil))
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

type WorkerMessage {
  StartAttempt
  StopWorker
}

type WorkerRequest {
  WorkerRequest(
    queue_subject: process.Subject(Message),
    claimed: postgres.ClaimedJob,
  )
}

type Completion {
  Manual(reply: process.Subject(Result(Bool, ProcessError)))
  Automatic
}

type ProcessOneResponse {
  ProcessOneResult(Result(Bool, ProcessError))
  ProcessOneActorDown(process.Down)
}

type ActiveAttempt {
  ActiveAttempt(
    id: Int,
    attempt_id: Int,
    epoch: Int,
    claimed: postgres.ClaimedJob,
    worker_subject: process.Subject(WorkerMessage),
    monitor: process.Monitor,
    completion: Completion,
    renewal_status: RenewalStatus,
    /// `Some(execution)` once this `Automatic`-completion attempt's worker
    /// has already returned and its acknowledgement came back
    /// `QueueAckUnknown` (the ack transaction reached the database but its
    /// reply was lost) — the attempt is kept in `active` rather than
    /// dropped, so it still counts against `maximum_concurrency` and blocks
    /// a clean shutdown drain, but its `worker_subject`/`monitor` are
    /// already stale (the worker was stopped and demonitored before the ack
    /// was ever attempted). This incarnation's own renewal timer retries the
    /// exact same `acknowledge_claim` call (`postgres.acknowledgement_command_id`
    /// is deterministic in job id, attempt id, and epoch, so the retry is
    /// idempotent), renewing the lease first on each tick while within
    /// `ConsumerState.pending_ack_retry_budget` — see `retry_pending_ack`'s
    /// and `retry_ack_until_known`'s doc comments for the full mechanism and
    /// why renewing (bounded) is the safe choice, not the lease-independence
    /// an earlier version of this fix wrongly assumed. Always `None` for a
    /// `Manual` completion — a `process_one` caller already gets
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
      WorkerRequest,
      process.Subject(WorkerMessage),
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
    active: List(ActiveAttempt),
    poll_remaining_jobs: Int,
    /// True while a `Poll` timer is already scheduled against
    /// `incarnation_subject` and has not yet fired. `continue_if_idle` is the
    /// sole scheduler and checks this before arming another one, so free
    /// capacity (`maximum_concurrency` above `active`'s length) while other
    /// attempts are still running never accumulates more than one pending
    /// timer — see its doc comment.
    poll_scheduled: Bool,
    fail_next_worker_start: Bool,
    kill_next_worker_before_monitor: Bool,
    shutting_down: Bool,
    shutdown_generation: Int,
    shutdown_replies: List(process.Subject(ShutdownReply)),
  )
}

/// Starts a supervised, serial consumer. Serial execution is its concurrency
/// bound, and an OTP child owns the process until `stop` is called.
/// Allocates one atom-backed coordinator name (see `process.new_name`);
/// bounded per call, reused rather than recreated across any internal
/// restart.
pub fn start(
  database: Database,
  workers: Registry,
) -> Result(Consumer, StartError) {
  let assert Ok(policy) = validate_policy(default_policy())
  start_consumer(database, workers, policy, True)
}

/// Starts an automatically polling queue with a validated local policy.
/// Allocates one atom-backed coordinator name (see `process.new_name`);
/// bounded per call, reused rather than recreated across any internal
/// restart.
pub fn start_with_policy(
  database: Database,
  workers: Registry,
  policy: ValidatedPolicy,
) -> Result(Consumer, StartError) {
  start_consumer(database, workers, policy, True)
}

/// Starts a supervised consumer without a timer; callers drive each attempt
/// through `process_one`. This is useful for deterministic operations and
/// tests. Allocates one atom-backed coordinator name (see
/// `process.new_name`); bounded per call, reused rather than recreated
/// across any internal restart.
pub fn start_manual(
  database: Database,
  workers: Registry,
) -> Result(Consumer, StartError) {
  let assert Ok(policy) = validate_policy(default_policy())
  start_consumer(database, workers, policy, False)
}

/// Starts a manually polled queue with a validated local policy. Allocates
/// one atom-backed coordinator name (see `process.new_name`); bounded per
/// call, reused rather than recreated across any internal restart.
pub fn start_manual_with_policy(
  database: Database,
  workers: Registry,
  policy: ValidatedPolicy,
) -> Result(Consumer, StartError) {
  start_consumer(database, workers, policy, False)
}

fn start_consumer(
  database: Database,
  workers: Registry,
  policy: ValidatedPolicy,
  auto_poll: Bool,
) -> Result(Consumer, StartError) {
  let queue_name = registry.queue(workers)
  let ValidatedPolicy(maximum_concurrency:, lease_duration_ms:, ..) = policy
  let minimum_lease_ms =
    minimum_lease_for_deadline(
      maximum_concurrency,
      postgres.statement_deadline_ms(database),
    )
  case
    queue_name == "",
    registry.identities(workers),
    lease_duration_ms < minimum_lease_ms
  {
    True, _, _ -> Error(RegistryQueueMismatch)
    False, [], _ -> Error(NoRegisteredWorkers)
    False, _, True ->
      Error(LeaseTooShortForDeadline(lease_duration_ms, minimum_lease_ms))
    False, _, False ->
      start_configured_consumer(
        database,
        workers,
        queue_name,
        policy,
        auto_poll,
      )
  }
}

fn start_configured_consumer(
  database: Database,
  workers: Registry,
  queue_name: String,
  policy: ValidatedPolicy,
  auto_poll: Bool,
) -> Result(Consumer, StartError) {
  // Created once per consumer, never inside the child start function: a
  // restarted coordinator incarnation re-registers this same name (the prior
  // registration is cleared by OTP when its owner dies), so `Consumer.subject`
  // keeps routing to whichever incarnation is currently alive. This bounds
  // atom creation to one name per `Consumer` value, not one per restart.
  let coordinator_name = process.new_name("grind_queue_coordinator")
  let handoff_reply = process.new_subject()
  let handoff_pid = start_queue_actor_handoff(handoff_reply, process.self())
  case process.receive(handoff_reply, within: 5000) {
    Error(Nil) -> {
      process.kill(handoff_pid)
      Error(QueueActorHandoffFailed)
    }
    Ok(QueueActorHandoffSubjects(actor_ready, stop_handoff)) ->
      start_configured_consumer_with_handoff(
        database,
        workers,
        queue_name,
        policy,
        auto_poll,
        coordinator_name,
        handoff_reply,
        handoff_pid,
        actor_ready,
        stop_handoff,
      )
    Ok(QueueActorHandoffStarted(_)) -> {
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
  auto_poll: Bool,
  coordinator_name: process.Name(Message),
  handoff_reply: process.Subject(QueueActorHandoffMessage),
  handoff_pid: process.Pid,
  actor_ready: process.Subject(process.Subject(Message)),
  stop_handoff: process.Subject(Nil),
) -> Result(Consumer, StartError) {
  let ValidatedPolicy(
    poll_interval_ms: _,
    maximum_jobs_per_poll:,
    maximum_concurrency: _,
    lease_duration_ms:,
    shutdown_grace_ms:,
  ) = policy
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
      actor.start(worker_actor(request))
    })
    |> factory_supervisor.restart_strategy(supervision.Temporary)
  let builder =
    actor.new_with_initialiser(1000, fn(subject) {
      // Computed fresh on every incarnation (initial start and every
      // supervised restart both run this closure), so each incarnation has
      // its own distinct owner label. attempt_owner is itself part of the
      // SQL fence checked alongside attempt_id and epoch on renewal,
      // release, and resolution (postgres.gleam's `renew_claim`,
      // `release_unstarted_claim`, `acknowledge`, and the resolution
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
      start_polling(incarnation_subject, auto_poll)
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
                poll_remaining_jobs: 0,
                poll_scheduled: auto_poll,
                fail_next_worker_start: False,
                kill_next_worker_before_monitor: False,
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
        Ok(QueueActorHandoffStarted(subject)) ->
          Ok(Consumer(
            subject,
            started.pid,
            maximum_jobs_per_poll,
            shutdown_grace_ms,
            process.self(),
          ))
        Ok(QueueActorHandoffSubjects(_, _)) -> {
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

type QueueActorHandoffMessage {
  QueueActorHandoffSubjects(
    actor_ready: process.Subject(process.Subject(Message)),
    stop: process.Subject(Nil),
  )
  QueueActorHandoffStarted(process.Subject(Message))
}

type QueueActorHandoffEvent {
  QueueActorStarted(process.Subject(Message))
  QueueActorHandoffStopped
  QueueActorHandoffOwnerDown(process.Down)
}

fn start_queue_actor_handoff(
  reply: process.Subject(QueueActorHandoffMessage),
  owner: process.Pid,
) -> process.Pid {
  process.spawn_unlinked(fn() {
    let actor_ready = process.new_subject()
    let stop = process.new_subject()
    process.send(reply, QueueActorHandoffSubjects(actor_ready, stop))
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
        process.send(reply, QueueActorHandoffStarted(subject))
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

fn start_polling(subject: process.Subject(Message), auto_poll: Bool) -> Nil {
  case auto_poll {
    True -> {
      let _ = process.send(subject, Poll)
      Nil
    }
    False -> Nil
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

/// Processes up to the configured batch size, stopping when no due job remains.
pub fn process_available(consumer: Consumer) -> BatchOutcome {
  let Consumer(maximum_jobs_per_poll:, ..) = consumer
  run_batch_from(consumer, maximum_jobs_per_poll, 0)
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
            ShutdownTimedOut -> Error(ConsumerDrainTimedOut)
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

@internal
pub fn fail_next_worker_start(consumer: Consumer) -> Result(Nil, ProcessError) {
  call_coordinator(consumer, InjectWorkerStartFailure)
}

/// Test hook that kills the next idle worker after start_child but before its
/// owner installs a monitor. It exercises OTP's already-dead monitor edge.
@internal
pub fn kill_next_worker_before_monitor(
  consumer: Consumer,
) -> Result(Nil, ProcessError) {
  call_coordinator(consumer, KillNextWorkerBeforeMonitor)
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
      // is taken.
      let state = ConsumerState(..state, poll_scheduled: False)
      let ValidatedPolicy(maximum_jobs_per_poll:, ..) = state.policy
      case state.shutting_down {
        True -> actor.continue(state)
        False ->
          case state.poll_remaining_jobs > 0 {
            True -> actor.continue(state)
            False ->
              fill_automatic_slots(
                ConsumerState(
                  ..state,
                  poll_remaining_jobs: maximum_jobs_per_poll,
                ),
              )
          }
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
    InjectWorkerStartFailure(reply) -> {
      process.send(reply, Nil)
      actor.continue(ConsumerState(..state, fail_next_worker_start: True))
    }
    KillNextWorkerBeforeMonitor(reply) -> {
      process.send(reply, Nil)
      actor.continue(
        ConsumerState(..state, kill_next_worker_before_monitor: True),
      )
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
            False -> start_attempt(state, Manual(reply))
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

fn schedule_poll(
  subject: process.Subject(Message),
  auto_poll: Bool,
  poll_interval_ms: Int,
) -> Nil {
  case auto_poll {
    True -> {
      let _ = process.send_after(subject, poll_interval_ms, Poll)
      Nil
    }
    False -> Nil
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
    fail_next_worker_start:,
    kill_next_worker_before_monitor:,
    ..,
  ) = state
  case
    postgres.claim_one(
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
      let #(id, attempt_id, epoch) = postgres.claim_identity(claimed)
      let request = WorkerRequest(incarnation_subject, claimed)
      let state =
        ConsumerState(
          ..state,
          fail_next_worker_start: False,
          kill_next_worker_before_monitor: False,
        )
      let start_result = case fail_next_worker_start {
        True -> Error(actor.InitFailed("injected start failure"))
        False -> factory_supervisor.start_child(worker_factory, request)
      }
      case start_result {
        Error(error) ->
          release_failed_worker_start(state, completion, claimed, error)
        Ok(started) -> {
          case kill_next_worker_before_monitor {
            True -> {
              process.kill(started.pid)
              wait_for_worker_exit(started.pid, 1000)
              Nil
            }
            False -> Nil
          }
          let monitor = process.monitor(started.pid)
          case process.is_alive(started.pid) {
            False -> {
              let _ = process.demonitor_process(monitor)
              release_dead_worker_before_activation(state, completion, claimed)
            }
            True -> {
              process.send(started.data, StartAttempt)
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
              let state = case completion {
                Manual(_) -> state
                Automatic ->
                  ConsumerState(
                    ..state,
                    poll_remaining_jobs: state.poll_remaining_jobs - 1,
                  )
              }
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
  claimed: postgres.ClaimedJob,
) -> actor.Next(ConsumerState, Message) {
  case
    postgres.release_unstarted_claim(
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

fn wait_for_worker_exit(pid: process.Pid, checks_remaining: Int) -> Nil {
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
  claimed: postgres.ClaimedJob,
  start_error: actor.StartError,
) -> actor.Next(ConsumerState, Message) {
  case
    postgres.release_unstarted_claim(
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
  let state = case completion {
    Manual(reply) -> {
      process.send(reply, result)
      state
    }
    Automatic -> ConsumerState(..state, poll_remaining_jobs: 0)
  }
  continue_after_completion(state)
}

fn finish_attempt(
  state: ConsumerState,
  id: Int,
  attempt_id: Int,
  epoch: Int,
  execution: worker.Execution,
) -> actor.Next(ConsumerState, Message) {
  case find_active(state.active, id, attempt_id, epoch) {
    Error(Nil) -> actor.continue(state)
    Ok(active) -> {
      let ActiveAttempt(claimed:, worker_subject:, monitor:, ..) = active
      process.demonitor_process(monitor)
      process.send(worker_subject, StopWorker)
      let result =
        postgres.acknowledge_claim(
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
/// `unique.AdmissionFailed`'s doc comment gives), but surfacing that here
/// would silently drop the claim in `Automatic` mode exactly like an
/// unhandled `QueueAckUnknown` would, and retrying costs nothing extra since
/// `command_id` already makes it idempotent; a *first* attempt's own
/// `QueueAckFailed` is unaffected and still resolves immediately, unchanged.
/// Anything else — success, one of the ack's own known-outcome errors
/// (`QueueAckStale`, `QueueAckCommandConflict`, ...), or any result at all
/// under `Manual` completion — resolves it exactly like an ordinary
/// first-attempt result always has, which for `Automatic` completion means
/// `finish_completion` calling straight through to `fill_automatic_slots`/
/// `continue_if_idle`, so a slot a resolved retry frees is reused promptly.
/// `Manual` deliberately never retries: a `process_one` caller already gets
/// `QueueAckUnknown` back synchronously today and can already call
/// `reconcile_acknowledgement` itself; only automatic mode had no caller
/// left to hand an unknown ack to, which is the gap this fixes.
fn finalize_ack_result(
  state: ConsumerState,
  active: ActiveAttempt,
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
          active: remove_active(state.active, id, attempt_id, epoch),
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
  active: ActiveAttempt,
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
      active: replace_active(
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
/// UPDATE is *itself* fenced the same way (`postgres.live_lease_predicate`),
/// so renewing is what keeps that path retriable rather than merely
/// "possible in principle": once the lease is gone, only a receipt-matched
/// commit can still resolve it. If the original attempt's transaction
/// actually committed already (a lost reply, not an abort), `renew_claim`'s
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
  active: ActiveAttempt,
  execution: worker.Execution,
) -> actor.Next(ConsumerState, Message) {
  let ActiveAttempt(claimed:, pending_ack_ticks:, ..) = active
  case pending_ack_ticks < state.pending_ack_retry_budget {
    True -> {
      let _ =
        postgres.renew_claim(
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
    postgres.acknowledge_claim(
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
    Manual(reply) -> {
      process.send(reply, result)
      continue_after_completion(state)
    }
    Automatic ->
      case result {
        Ok(True) -> fill_automatic_slots(state)
        Ok(False) | Error(_) ->
          continue_if_idle(ConsumerState(..state, poll_remaining_jobs: 0))
      }
  }
}

fn fill_automatic_slots(
  state: ConsumerState,
) -> actor.Next(ConsumerState, Message) {
  let ValidatedPolicy(maximum_concurrency:, ..) = state.policy
  case
    !state.shutting_down
    && state.poll_remaining_jobs > 0
    && list.length(state.active) < maximum_concurrency
  {
    True -> start_attempt(state, Automatic)
    False -> continue_if_idle(state)
  }
}

fn continue_after_start(
  state: ConsumerState,
  completion: Completion,
) -> actor.Next(ConsumerState, Message) {
  case completion {
    Manual(_) -> actor.continue(state)
    Automatic -> fill_automatic_slots(state)
  }
}

fn continue_after_completion(
  state: ConsumerState,
) -> actor.Next(ConsumerState, Message) {
  case state.poll_remaining_jobs > 0 {
    True -> fill_automatic_slots(state)
    False -> continue_if_idle(state)
  }
}

/// Called at the end of every poll round (this poll's claim batch drained,
/// or an active attempt just finished). Arms the next `Poll` timer whenever
/// this consumer is not shutting down, this round is done claiming
/// (`poll_remaining_jobs == 0`), and free capacity remains
/// (`active` below `maximum_concurrency`) — not only when `active` is fully
/// empty. A `maximum_concurrency` above 1 otherwise leaves spare slots idle
/// for as long as one attempt keeps running: with the old empty-only check,
/// a newly due job (or the claim-time expired-lease quarantine scan, which
/// piggybacks on the same claim query) had to wait for every currently
/// active attempt to finish before the next poll was even scheduled, no
/// matter how much capacity was actually free in the meantime.
/// `poll_scheduled` is the single-outstanding-timer guard this relies on: an
/// active attempt finishing while a timer from an earlier round is already
/// pending must not arm a second, overlapping one (see the field's doc
/// comment). Shutdown draining is unaffected — a consumer already shutting
/// down never re-arms a poll regardless of capacity.
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
        && state.poll_remaining_jobs == 0
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

fn find_active(
  active_attempts: List(ActiveAttempt),
  id: Int,
  attempt_id: Int,
  epoch: Int,
) -> Result(ActiveAttempt, Nil) {
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

fn remove_active(
  active_attempts: List(ActiveAttempt),
  id: Int,
  attempt_id: Int,
  epoch: Int,
) -> List(ActiveAttempt) {
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

/// This incarnation's renewal timer fires once per active attempt every
/// `renewal_interval_ms`. A `pending_ack` attempt (its worker has already
/// returned; see `ActiveAttempt`'s doc comment) repurposes this same tick to
/// retry its acknowledgement instead of renewing a lease nothing is running
/// against.
fn replace_active(
  active_attempts: List(ActiveAttempt),
  id: Int,
  attempt_id: Int,
  epoch: Int,
  replacement: ActiveAttempt,
) -> List(ActiveAttempt) {
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
  case find_active_by_attempt(state.active, tick_attempt_id, tick_epoch) {
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
  active: ActiveAttempt,
) -> actor.Next(ConsumerState, Message) {
  let ActiveAttempt(attempt_id:, epoch:, renewal_generation:, ..) = active
  let result =
    postgres.renew_claim(
      state.database,
      state.queue,
      state.attempt_owner,
      active.claimed,
      state.lease_duration_ms,
    )
  case result {
    Ok(True) -> {
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
    Ok(False) ->
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
    list.map(state.active, fn(active) {
      let ActiveAttempt(attempt_id: active_id, epoch: active_epoch, ..) = active
      case active_id == attempt_id && active_epoch == epoch {
        True -> ActiveAttempt(..active, renewal_status:)
        False -> active
      }
    })
  ConsumerState(..state, active:)
}

fn find_active_by_attempt(
  active_attempts: List(ActiveAttempt),
  attempt_id: Int,
  epoch: Int,
) -> Result(ActiveAttempt, Nil) {
  list.find(active_attempts, fn(active) {
    let ActiveAttempt(attempt_id: active_id, epoch: active_epoch, ..) = active
    active_id == attempt_id && active_epoch == epoch
  })
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
              active: remove_active(state.active, id, attempt_id, epoch),
            )
          case completion {
            Manual(reply) -> process.send(reply, Error(QueueWorkerExited))
            Automatic -> Nil
          }
          let state = case completion {
            Manual(_) -> state
            Automatic -> ConsumerState(..state, poll_remaining_jobs: 0)
          }
          continue_after_completion(state)
        }
      }
    process.PortDown(..) -> actor.continue(state)
  }
}

fn schedule_next_poll(state: ConsumerState) -> Nil {
  let ValidatedPolicy(poll_interval_ms:, ..) = state.policy
  schedule_poll(state.incarnation_subject, state.auto_poll, poll_interval_ms)
}

type WorkerState {
  WorkerState(
    queue_subject: process.Subject(Message),
    claimed: postgres.ClaimedJob,
  )
}

fn worker_actor(
  request: WorkerRequest,
) -> actor.Builder(WorkerState, WorkerMessage, process.Subject(WorkerMessage)) {
  let WorkerRequest(queue_subject:, claimed:) = request
  actor.new(WorkerState(queue_subject:, claimed:))
  |> actor.on_message(fn(state, message) {
    case message {
      StartAttempt -> {
        let #(id, attempt_id, epoch) = postgres.claim_identity(state.claimed)
        let execution = postgres.execute_claim(state.claimed)
        process.send(
          state.queue_subject,
          AttemptReturned(id, attempt_id, epoch, execution),
        )
        actor.continue(state)
      }
      StopWorker -> actor.stop()
    }
  })
}
