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
    expired_attempt_policy: ExpiredAttemptPolicy,
  )
}

/// Controls whether an expired execution can invoke its handler again.
pub type ExpiredAttemptPolicy {
  RequireReconciliation
  ReplayAtLeastOnce
}

pub fn default_policy() -> QueuePolicy {
  QueuePolicy(
    poll_interval_ms: 250,
    maximum_jobs_per_poll: 1,
    maximum_concurrency: 1,
    lease_duration_ms: 30_000,
    shutdown_grace_ms: 5000,
    expired_attempt_policy: RequireReconciliation,
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

pub fn with_expired_attempt_policy(
  policy: QueuePolicy,
  expired_attempt_policy: ExpiredAttemptPolicy,
) -> QueuePolicy {
  QueuePolicy(..policy, expired_attempt_policy:)
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
    expired_attempt_policy: ExpiredAttemptPolicy,
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
    expired_attempt_policy:,
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
                        expired_attempt_policy:,
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
  QueueConfigurationFailed(postgres.QueueConfigurationError)
  QueueActorStartFailed(actor.StartError)
  QueueSupervisorStartFailed(actor.StartError)
  QueueActorHandoffFailed
  QueueSupervisorStopTimedOut
}

pub type StopError {
  ConsumerOwnedByAnotherProcess
  ConsumerStopTimedOut
  ConsumerDrainTimedOut
}

pub type StopOutcome {
  StoppedCleanly
  StoppedWithActiveWork(Int)
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
  Renew(attempt_id: Int, epoch: Int)
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

type ShutdownReply {
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
    auto_poll: Bool,
    policy: ValidatedPolicy,
    lease_duration_ms: Int,
    renewal_interval_ms: Int,
    active: List(ActiveAttempt),
    poll_remaining_jobs: Int,
    fail_next_worker_start: Bool,
    kill_next_worker_before_monitor: Bool,
    shutting_down: Bool,
    shutdown_generation: Int,
    shutdown_replies: List(process.Subject(ShutdownReply)),
  )
}

/// Starts a supervised, serial consumer. Serial execution is its concurrency
/// bound, and an OTP child owns the process until `stop` is called.
pub fn start(
  database: Database,
  workers: Registry,
) -> Result(Consumer, StartError) {
  let assert Ok(policy) = validate_policy(default_policy())
  start_consumer(database, workers, policy, True)
}

/// Starts an automatically polling queue with a validated local policy.
pub fn start_with_policy(
  database: Database,
  workers: Registry,
  policy: ValidatedPolicy,
) -> Result(Consumer, StartError) {
  start_consumer(database, workers, policy, True)
}

/// Starts a supervised consumer without a timer; callers drive each attempt
/// through `process_one`. This is useful for deterministic operations and tests.
pub fn start_manual(
  database: Database,
  workers: Registry,
) -> Result(Consumer, StartError) {
  let assert Ok(policy) = validate_policy(default_policy())
  start_consumer(database, workers, policy, False)
}

/// Starts a manually polled queue with a validated local policy.
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
  case queue_name == "", registry.identities(workers) {
    True, _ -> Error(RegistryQueueMismatch)
    False, [] -> Error(NoRegisteredWorkers)
    False, _ -> {
      let ValidatedPolicy(expired_attempt_policy:, ..) = policy
      let replay_expired = case expired_attempt_policy {
        RequireReconciliation -> False
        ReplayAtLeastOnce -> True
      }
      case postgres.configure_queue(database, queue_name, replay_expired) {
        Error(error) -> Error(QueueConfigurationFailed(error))
        Ok(Nil) ->
          start_configured_consumer(
            database,
            workers,
            queue_name,
            policy,
            auto_poll,
          )
      }
    }
  }
}

fn start_configured_consumer(
  database: Database,
  workers: Registry,
  queue_name: String,
  policy: ValidatedPolicy,
  auto_poll: Bool,
) -> Result(Consumer, StartError) {
  let attempt_owner = "grind-consumer-" <> int.to_string(unique_integer())
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
        attempt_owner,
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
  attempt_owner: String,
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
    ..,
  ) = policy
  let renewal_interval_ms = case lease_duration_ms / 3 > 0 {
    True -> lease_duration_ms / 3
    False -> 1
  }
  let worker_factory_builder =
    factory_supervisor.worker_child(fn(request) {
      actor.start(worker_actor(request))
    })
    |> factory_supervisor.restart_strategy(supervision.Temporary)
  let builder =
    actor.new_with_initialiser(1000, fn(subject) {
      start_polling(subject, auto_poll)
      case factory_supervisor.start(worker_factory_builder) {
        Error(error) -> Error(string.inspect(error))
        Ok(started_factory) -> {
          let selector =
            process.new_selector()
            |> process.select(for: subject)
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
                auto_poll:,
                policy:,
                lease_duration_ms:,
                renewal_interval_ms:,
                active: [],
                poll_remaining_jobs: 0,
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

/// Deterministically asks the queue actor to claim one due job now and waits
/// without an implicit deadline. A valid worker can run longer than the
/// polling interval; callers needing a deadline should run this operation in
/// their own process and define their timeout and reconciliation behavior.
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
              // `subject` is the unregistered subject from this exact actor
              // incarnation. It cannot retarget a restarted coordinator.
              process.send(subject, ProcessOne(reply))
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

/// Stops the queue supervisor from the process that started it. The queue
/// pauses new claims and waits up to its configured grace for active workers.
pub fn stop(consumer: Consumer) -> Result(StopOutcome, StopError) {
  let Consumer(subject:, supervisor_pid:, shutdown_grace_ms:, owner_pid:, ..) =
    consumer
  case process.self() == owner_pid {
    False -> Error(ConsumerOwnedByAnotherProcess)
    True -> {
      let reply = process.new_subject()
      process.send(subject, BeginShutdown(reply))
      let shutdown = process.receive(reply, within: shutdown_grace_ms + 1000)
      case stop_consumer_supervisor(supervisor_pid) {
        Error(Nil) -> Error(ConsumerStopTimedOut)
        Ok(Nil) ->
          case shutdown {
            Ok(ShutdownDrained) -> Ok(StoppedCleanly)
            Ok(ShutdownForced(active)) -> Ok(StoppedWithActiveWork(active))
            Error(Nil) -> Error(ConsumerDrainTimedOut)
          }
      }
    }
  }
}

/// Reports whether the coordinator has begun its shutdown transition.
/// This internal observation supports deterministic lifecycle synchronization.
@internal
pub fn shutdown_state(consumer: Consumer) -> Result(Bool, ProcessError) {
  let Consumer(subject:, ..) = consumer
  let reply = process.new_subject()
  process.send(subject, ReadShutdownState(reply))
  process.receive(reply, within: 1000)
  |> result.map_error(fn(_) { QueueActorExited })
}

@internal
pub fn renewal_status(
  consumer: Consumer,
) -> Result(Option(RenewalStatus), ProcessError) {
  let Consumer(subject:, ..) = consumer
  let reply = process.new_subject()
  process.send(subject, ReadRenewalStatus(reply))
  process.receive(reply, within: 1000)
  |> result.map_error(fn(_) { QueueActorExited })
}

@internal
pub fn fail_next_worker_start(consumer: Consumer) -> Result(Nil, ProcessError) {
  let Consumer(subject:, ..) = consumer
  let reply = process.new_subject()
  process.send(subject, InjectWorkerStartFailure(reply))
  process.receive(reply, within: 1000)
  |> result.map_error(fn(_) { QueueActorExited })
}

/// Test hook that kills the next idle worker after start_child but before its
/// owner installs a monitor. It exercises OTP's already-dead monitor edge.
@internal
pub fn kill_next_worker_before_monitor(
  consumer: Consumer,
) -> Result(Nil, ProcessError) {
  let Consumer(subject:, ..) = consumer
  let reply = process.new_subject()
  process.send(subject, KillNextWorkerBeforeMonitor(reply))
  process.receive(reply, within: 1000)
  |> result.map_error(fn(_) { QueueActorExited })
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
    Renew(attempt_id, epoch) -> renew_active_attempt(state, attempt_id, epoch)
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
              state.subject,
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
    subject:,
    lease_duration_ms:,
    renewal_interval_ms:,
    policy:,
    fail_next_worker_start:,
    kill_next_worker_before_monitor:,
    ..,
  ) = state
  let ValidatedPolicy(expired_attempt_policy:, ..) = policy
  let replay_expired = case expired_attempt_policy {
    RequireReconciliation -> False
    ReplayAtLeastOnce -> True
  }
  case
    postgres.claim_one(
      database,
      queue,
      workers,
      attempt_owner,
      replay_expired,
      lease_duration_ms,
    )
  {
    Error(error) ->
      finish_without_claim(state, completion, Error(QueueProcessFailed(error)))
    Ok(None) -> finish_without_claim(state, completion, Ok(False))
    Ok(Some(claimed)) -> {
      let #(id, attempt_id, epoch) = postgres.claim_identity(claimed)
      let request = WorkerRequest(subject, claimed)
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
                )
              let _ =
                process.send_after(
                  subject,
                  renewal_interval_ms,
                  Renew(attempt_id, epoch),
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
      let ActiveAttempt(claimed:, worker_subject:, monitor:, completion:, ..) =
        active
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
    False ->
      case list.is_empty(state.active) && state.poll_remaining_jobs == 0 {
        True -> {
          schedule_next_poll(state)
          actor.continue(state)
        }
        False -> actor.continue(state)
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

fn renew_active_attempt(
  state: ConsumerState,
  tick_attempt_id: Int,
  tick_epoch: Int,
) -> actor.Next(ConsumerState, Message) {
  case find_active_by_attempt(state.active, tick_attempt_id, tick_epoch) {
    Error(Nil) -> actor.continue(state)
    Ok(active) -> {
      let ActiveAttempt(attempt_id:, epoch:, ..) = active
      case renewal_is_current(attempt_id, epoch, tick_attempt_id, tick_epoch) {
        False -> actor.continue(state)
        True -> {
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
                set_renewal_status(
                  state,
                  tick_attempt_id,
                  tick_epoch,
                  LeaseRenewalConfirmed,
                )
              let _ =
                process.send_after(
                  state.subject,
                  state.renewal_interval_ms,
                  Renew(attempt_id, epoch),
                )
              actor.continue(state)
            }
            Ok(False) ->
              actor.continue(set_renewal_status(
                state,
                tick_attempt_id,
                tick_epoch,
                LeaseRenewalLost,
              ))
            Error(_) -> {
              let state =
                set_renewal_status(
                  state,
                  tick_attempt_id,
                  tick_epoch,
                  LeaseRenewalUnknown,
                )
              let _ =
                process.send_after(
                  state.subject,
                  state.renewal_interval_ms,
                  Renew(attempt_id, epoch),
                )
              actor.continue(state)
            }
          }
        }
      }
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
  schedule_poll(state.subject, state.auto_poll, poll_interval_ms)
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
