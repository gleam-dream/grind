import gleam/erlang/process
import gleam/int
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import grind/postgres.{type Database}
import grind/registry.{type Registry}

/// An independently supervised consumer for one configured queue.
pub opaque type Consumer {
  Consumer(subject: process.Subject(Message), supervisor_pid: process.Pid)
}

/// Queue-local polling settings. Jobs execute serially within this consumer;
/// `maximum_jobs_per_poll` controls how many it drains in one poll tick.
pub type QueuePolicy {
  QueuePolicy(poll_interval_ms: Int, maximum_jobs_per_poll: Int)
}

pub fn default_policy() -> QueuePolicy {
  QueuePolicy(poll_interval_ms: 250, maximum_jobs_per_poll: 1)
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

pub type PolicyError {
  PollIntervalMustBePositive
  MaximumJobsPerPollMustBePositive
}

pub opaque type ValidatedPolicy {
  ValidatedPolicy(poll_interval_ms: Int, maximum_jobs_per_poll: Int)
}

/// Validates the policy before a queue actor is started.
pub fn validate_policy(
  policy: QueuePolicy,
) -> Result(ValidatedPolicy, PolicyError) {
  let QueuePolicy(poll_interval_ms:, maximum_jobs_per_poll:) = policy
  case poll_interval_ms > 0 {
    False -> Error(PollIntervalMustBePositive)
    True ->
      case maximum_jobs_per_poll > 0 {
        False -> Error(MaximumJobsPerPollMustBePositive)
        True -> Ok(ValidatedPolicy(poll_interval_ms:, maximum_jobs_per_poll:))
      }
  }
}

pub type StartError {
  RegistryQueueMismatch
  NoRegisteredWorkers
  QueueActorStartFailed(actor.StartError)
  QueueSupervisorStartFailed(actor.StartError)
}

pub type ProcessError {
  QueueProcessFailed(postgres.QueueRunError)
}

type Message {
  Poll
  ProcessOne(reply: process.Subject(Result(Bool, ProcessError)))
  ProcessBatch(reply: process.Subject(BatchOutcome))
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
    queue: String,
    attempt_owner: String,
    subject: process.Subject(Message),
    processed: Int,
    auto_poll: Bool,
    policy: ValidatedPolicy,
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
      let attempt_owner = "grind-consumer-" <> int.to_string(unique_integer())
      let ValidatedPolicy(poll_interval_ms:, maximum_jobs_per_poll:) = policy
      let actor_name = process.new_name("grind_queue_consumer")
      let builder =
        actor.new_with_initialiser(1000, fn(subject) {
          start_polling(subject, auto_poll)
          Ok(
            actor.initialised(ConsumerState(
              database:,
              workers:,
              queue: queue_name,
              attempt_owner:,
              subject:,
              processed: 0,
              auto_poll:,
              policy: ValidatedPolicy(poll_interval_ms:, maximum_jobs_per_poll:),
            ))
            |> actor.returning(subject),
          )
        })
        |> actor.on_message(handle_message)
        |> actor.named(actor_name)
      let child = supervision.worker(fn() { actor.start(builder) })
      let supervisor =
        static_supervisor.new(static_supervisor.OneForOne)
        |> static_supervisor.add(child)
      case static_supervisor.start(supervisor) {
        Error(error) -> Error(QueueSupervisorStartFailed(error))
        Ok(started) -> {
          process.unlink(started.pid)
          Ok(Consumer(process.named_subject(actor_name), started.pid))
        }
      }
    }
  }
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

/// Deterministically asks the queue actor to claim one due job now.
pub fn process_one(consumer: Consumer) -> Result(Bool, ProcessError) {
  let Consumer(subject:, ..) = consumer
  process.call(subject, 30_000, ProcessOne)
}

/// Processes up to the configured batch size, stopping when no due job remains.
pub fn process_available(consumer: Consumer) -> BatchOutcome {
  let Consumer(subject:, ..) = consumer
  process.call(subject, 30_000, ProcessBatch)
}

/// Immediately stops the queue's OTP supervisor and worker child. It does not
/// drain or wait for active work, which may be interrupted.
pub fn stop(consumer: Consumer) -> Nil {
  let Consumer(supervisor_pid:, ..) = consumer
  stop_supervisor(supervisor_pid)
}

@external(erlang, "grind_postgres_ffi", "stop_supervisor")
fn stop_supervisor(pid: process.Pid) -> Nil

@external(erlang, "erlang", "unique_integer")
fn unique_integer() -> Int

fn handle_message(
  state: ConsumerState,
  message: Message,
) -> actor.Next(ConsumerState, Message) {
  case message {
    Poll -> {
      let ValidatedPolicy(poll_interval_ms:, maximum_jobs_per_poll:) =
        state.policy
      let _ = run_batch(state, maximum_jobs_per_poll)
      schedule_poll(state.subject, state.auto_poll, poll_interval_ms)
      actor.continue(state)
    }
    ProcessOne(reply) -> {
      let result = run_one(state)
      process.send(reply, result)
      actor.continue(state)
    }
    ProcessBatch(reply) -> {
      let ValidatedPolicy(maximum_jobs_per_poll:, ..) = state.policy
      let result = run_batch(state, maximum_jobs_per_poll)
      process.send(reply, result)
      actor.continue(state)
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

fn run_batch(state: ConsumerState, maximum_jobs: Int) -> BatchOutcome {
  run_batch_from(state, maximum_jobs, 0)
}

fn run_batch_from(
  state: ConsumerState,
  remaining_jobs: Int,
  processed_jobs: Int,
) -> BatchOutcome {
  case remaining_jobs > 0 {
    False -> BatchCompleted(processed_jobs)
    True ->
      case run_one(state) {
        Error(error) -> BatchStopped(processed_jobs, error)
        Ok(False) -> BatchCompleted(processed_jobs)
        Ok(True) ->
          run_batch_from(state, remaining_jobs - 1, processed_jobs + 1)
      }
  }
}

fn run_one(state: ConsumerState) -> Result(Bool, ProcessError) {
  let ConsumerState(database:, workers:, queue:, attempt_owner:, ..) = state
  postgres.process_one(database, queue, workers, attempt_owner)
  |> result.map_error(QueueProcessFailed)
}
