//// Periodically prunes finished jobs from one Database. Each tick runs one
//// bounded batch; a full batch waits until the next scheduled tick.
//// Concurrent callers are safe because candidate deletion uses SKIP LOCKED.
////
//// The internal policy defaults to a thirty-second interval, 10,000 jobs and
//// a sixty-second age. The public Grind runtime overrides the age to seven
//// days. A large batch can exceed the storage deadline; tune against the
//// deployed workload. Failure emits [grind, prune, failed] through telemetry
//// and retries on the next tick. See docs/USAGE.md, "Retention".

import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import grind/internal/postgres.{type Database}
import grind/telemetry
import pog
import sinal/forwarder.{type Forwarder}

/// How often, how much, and how far back one supervised pruner prunes.
pub type PrunerPolicy {
  PrunerPolicy(interval_ms: Int, limit: Int, max_age_ms: Int)
}

/// Oban's own pruner defaults: `interval_ms` 30_000, `limit` 10_000,
/// `max_age_ms` 60_000.
pub fn default_policy() -> PrunerPolicy {
  PrunerPolicy(interval_ms: 30_000, limit: 10_000, max_age_ms: 60_000)
}

pub fn with_interval(policy: PrunerPolicy, interval_ms: Int) -> PrunerPolicy {
  PrunerPolicy(..policy, interval_ms:)
}

pub fn with_limit(policy: PrunerPolicy, limit: Int) -> PrunerPolicy {
  PrunerPolicy(..policy, limit:)
}

pub fn with_max_age(policy: PrunerPolicy, max_age_ms: Int) -> PrunerPolicy {
  PrunerPolicy(..policy, max_age_ms:)
}

pub type PrunerPolicyError {
  NonPositiveInterval
  /// `limit` was not positive.
  NonPositivePruneLimit
  /// `limit` exceeds `postgres.prune_limit_maximum()`.
  PruneLimitTooLarge
  /// `max_age_ms` was not positive. There is no minimum floor beyond this —
  /// see `postgres.prune_finished`'s own `NonPositiveRetention`, which this
  /// mirrors exactly, for why.
  NonPositiveMaxAge
  /// `max_age_ms` exceeds `worker.retry_delay_maximum_milliseconds()` — see
  /// `postgres.prune_finished`'s own `RetentionAbovePrecisionBound`, which
  /// this mirrors exactly.
  MaxAgeAbovePrecisionBound
}

pub opaque type ValidatedPrunerPolicy {
  ValidatedPrunerPolicy(interval_ms: Int, limit: Int, max_age_ms: Int)
}

/// Validates a `PrunerPolicy` against exactly the same checks
/// `postgres.prune_finished` itself runs on `older_than_ms`/`limit`
/// (`postgres.validate_retention_ms`/`postgres.validate_prune_limit`, the
/// same `@internal` functions `prune_finished` calls, so the two can never
/// silently drift apart), plus a positive `interval_ms` of its own — pure,
/// and required before `start`/`supervised`.
pub fn validate_policy(
  policy: PrunerPolicy,
) -> Result(ValidatedPrunerPolicy, PrunerPolicyError) {
  let PrunerPolicy(interval_ms:, limit:, max_age_ms:) = policy
  case interval_ms <= 0 {
    True -> Error(NonPositiveInterval)
    False ->
      case postgres.validate_prune_limit(limit) {
        Error(postgres.NonPositivePruneLimit) -> Error(NonPositivePruneLimit)
        Error(postgres.PruneLimitTooLarge) -> Error(PruneLimitTooLarge)
        Error(_) ->
          panic as "postgres.validate_prune_limit only ever returns NonPositivePruneLimit or PruneLimitTooLarge"
        Ok(Nil) ->
          case postgres.validate_retention_ms(max_age_ms) {
            Error(postgres.NonPositiveRetention) -> Error(NonPositiveMaxAge)
            Error(postgres.RetentionAbovePrecisionBound) ->
              Error(MaxAgeAbovePrecisionBound)
            Error(_) ->
              panic as "postgres.validate_retention_ms only ever returns NonPositiveRetention or RetentionAbovePrecisionBound"
            Ok(Nil) ->
              Ok(ValidatedPrunerPolicy(interval_ms:, limit:, max_age_ms:))
          }
      }
  }
}

/// `default_policy() |> validate_policy`, already unwrapped: the shipped
/// defaults are always valid.
pub fn default_policy_validated() -> ValidatedPrunerPolicy {
  let assert Ok(policy) = validate_policy(default_policy())
  policy
}

/// A running, independently supervised pruner. The process that starts it
/// owns its supervisor and must also stop it.
pub opaque type Pruner {
  Pruner(subject: process.Subject(Message), supervisor_pid: process.Pid)
}

pub fn supervisor_pid(pruner: Pruner) -> process.Pid {
  let Pruner(supervisor_pid:, ..) = pruner
  supervisor_pid
}

/// The current incarnation's own actor pid, resolved fresh through the
/// named subject every call (never cached) — the same
/// `process.subject_owner` lookup `grind/queue.coordinator_pid` uses.
/// `@internal`: exposed only so the test suite can kill a live incarnation
/// to prove the supervisor restarts it cleanly, with no duplicated or
/// leaked tick.
pub fn actor_pid(pruner: Pruner) -> Result(process.Pid, Nil) {
  let Pruner(subject:, ..) = pruner
  process.subject_owner(subject)
}

/// A message this actor's own message loop handles. Public (but opaque —
/// no constructor is exposed) purely so `supervised`'s own return type can
/// name it; nothing outside this module ever constructs or matches one.
pub opaque type Message {
  Tick
}

type PrunerState {
  PrunerState(
    database: Database,
    policy: ValidatedPrunerPolicy,
    forwarder: Forwarder,
    // The per-incarnation subject self-scheduled `Tick` timers target —
    // never the named subject `Pruner.subject` exposes. `process.send_after`
    // targeting a *named* subject resolves to whichever process currently
    // holds that name at delivery time, not at scheduling time: a timer an
    // old, now-dead incarnation scheduled for itself would otherwise still
    // fire and land on whatever later incarnation happens to be running
    // when it does, accumulating extra, undead ticks across restarts
    // instead of dying with the incarnation that scheduled them. Tied to
    // one specific incarnation instead, exactly like `grind/queue`'s own
    // `ConsumerState.incarnation_subject`, it simply has no live owner left
    // to deliver to once that incarnation is gone.
    incarnation_subject: process.Subject(Message),
  )
}

pub type StartError {
  PrunerSupervisorStartFailed(actor.StartError)
}

/// Builds the actor for one pruner, named so `Pruner.subject`/`actor_pid`
/// keep resolving to whichever incarnation is currently alive across a
/// supervised restart — the same reason `grind/queue.Consumer` names its
/// own coordinator. Shared by `start` (which wraps this in its own
/// dedicated one-child supervisor) and `supervised` (which hands the same
/// child specification to a caller's own supervision tree instead).
fn builder(
  database: Database,
  policy: ValidatedPrunerPolicy,
  pruner_name: process.Name(Message),
) -> actor.Builder(PrunerState, Message, process.Subject(Message)) {
  let ValidatedPrunerPolicy(interval_ms:, ..) = policy
  let forwarder = postgres.forwarder(database)
  actor.new_with_initialiser(1000, fn(subject) {
    let incarnation_subject = process.new_subject()
    process.send_after(incarnation_subject, interval_ms, Tick)
    let selector =
      process.new_selector()
      |> process.select(for: subject)
      |> process.select(for: incarnation_subject)
    Ok(
      actor.initialised(PrunerState(
        database:,
        policy:,
        forwarder:,
        incarnation_subject:,
      ))
      |> actor.selecting(selector)
      |> actor.returning(subject),
    )
  })
  |> actor.on_message(handle_message)
  |> actor.named(pruner_name)
}

/// Starts one supervised pruner against `database`, under its own dedicated
/// one-child supervisor. Its first tick fires `policy.interval_ms` after
/// this call returns, not immediately (every later tick reschedules itself
/// the same `policy.interval_ms` after the one before it — see the module
/// doc comment above for what one tick does). This call links that
/// dedicated supervisor to the caller: if the pruner actor crashes
/// repeatedly past the supervisor's own restart budget, the supervisor
/// gives up and exits, and — because it is linked, not merely spawned —
/// the caller's own process receives that exit too, and dies with it unless
/// it traps exits. Use `supervised` instead to embed the pruner into an
/// application's own supervision tree, where a crash loop past budget
/// escalates into that tree the normal OTP way instead of exiting whichever
/// unrelated process happened to call `start`.
pub fn start(
  database: Database,
  policy: ValidatedPrunerPolicy,
) -> Result(Pruner, StartError) {
  let pruner_name = process.new_name("grind_pruner")
  let child =
    supervision.worker(fn() {
      actor.start(builder(database, policy, pruner_name))
    })
  let supervisor =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(child)
  case static_supervisor.start(supervisor) {
    Error(error) -> Error(PrunerSupervisorStartFailed(error))
    Ok(started) -> Ok(Pruner(process.named_subject(pruner_name), started.pid))
  }
}

/// A child specification for embedding a pruner directly into an
/// application's own supervision tree (`static_supervisor.add`), rather
/// than tracking the separate, dedicated supervisor `start` creates and
/// returns as part of its own `Pruner` value. Its first tick fires
/// `policy.interval_ms` after the caller's own supervisor starts this
/// child, not immediately, the same as `start`. The caller's own supervisor
/// owns restart policy and shutdown for this child like any other; there is
/// no `Pruner` value to `stop` here — stopping this pruner means stopping
/// (or reconfiguring) the caller's own supervisor child, the same as for
/// any other supervised worker in that tree. A crash loop past this
/// child's own local restart budget escalates into the caller's tree the
/// normal OTP way (its immediate supervisor gives up and is itself
/// restarted or terminated by its own parent, and so on upward) rather than
/// exiting an unrelated caller process the way `start`'s linked supervisor
/// does. Isolation is otherwise the same either way: a pruner crash never
/// reaches `Database`'s own pool supervisor (a sibling, not a parent, in
/// both `start` and here).
pub fn supervised(
  database: Database,
  policy: ValidatedPrunerPolicy,
) -> supervision.ChildSpecification(process.Subject(Message)) {
  let pruner_name = process.new_name("grind_pruner")
  supervision.worker(fn() {
    actor.start(builder(database, policy, pruner_name))
  })
}

/// A pruner child that reads its `Database` each time it starts, so a
/// restarted pruner uses its runtime's current one.
pub fn supervised_from(
  database: fn() -> Result(Database, String),
  policy: ValidatedPrunerPolicy,
  name: process.Name(Message),
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() {
    case database() {
      Error(reason) -> Error(actor.InitFailed(reason))
      Ok(database) ->
        actor.start(builder(database, policy, name))
        |> result.map(fn(started) { actor.Started(started.pid, Nil) })
    }
  })
}

fn handle_message(
  state: PrunerState,
  message: Message,
) -> actor.Next(PrunerState, Message) {
  case message {
    Tick -> {
      let PrunerState(database:, policy:, forwarder:, incarnation_subject:) =
        state
      let ValidatedPrunerPolicy(interval_ms:, limit:, max_age_ms:) = policy
      case
        postgres.prune_finished(database, older_than_ms: max_age_ms, limit:)
      {
        Ok(_) -> Nil
        Error(error) -> emit_prune_failed(forwarder, error, max_age_ms, limit)
      }
      process.send_after(incarnation_subject, interval_ms, Tick)
      actor.continue(state)
    }
  }
}

/// Classifies a `postgres.PruneError` into the coarse
/// `telemetry.PruneFailureKind` `[grind, prune, failed]` reports — see
/// that type's own doc comment for what each variant means and, in
/// particular, which ones leave "did this actually delete anything"
/// genuinely unknown rather than "no".
fn prune_failure_kind(
  error: postgres.PruneError,
) -> telemetry.PruneFailureKind {
  case error {
    postgres.PruneQueryFailed(pog_error) -> query_failure_kind(pog_error)
    postgres.NonPositiveRetention
    | postgres.RetentionAbovePrecisionBound
    | postgres.NonPositivePruneLimit
    | postgres.PruneLimitTooLarge ->
      // Unreachable in practice: `handle_message` always calls
      // `prune_finished` with this exact `ValidatedPrunerPolicy`'s own
      // fields, already checked by `validate_policy` against the identical
      // bounds. Mapped rather than asserted away, since a `PruneError` from
      // a future, differently-validated call site should still report
      // *something* sensible instead of crashing the pruner actor.
      telemetry.PruneNotAttempted
  }
}

fn query_failure_kind(error: pog.QueryError) -> telemetry.PruneFailureKind {
  case error {
    pog.QueryTimeout -> telemetry.PruneReplyLost
    pog.UnexpectedResultType(_) -> telemetry.PruneResultUndecodable
    pog.ConstraintViolated(_, _, _) | pog.PostgresqlError(_, _, _) ->
      telemetry.PruneRejected
    pog.ConnectionUnavailable
    | pog.UnexpectedArgumentCount(_, _)
    | pog.UnexpectedArgumentType(_, _) -> telemetry.PruneNotAttempted
  }
}

fn emit_prune_failed(
  fwd: Forwarder,
  error: postgres.PruneError,
  max_age_ms: Int,
  limit: Int,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      telemetry.prune_failed(),
      telemetry.PruneFailedMeasurements(count: 1),
      telemetry.PruneFailedMetadata(
        older_than_ms: max_age_ms,
        limit:,
        kind: prune_failure_kind(error),
      ),
    )
  Nil
}

pub type StopError {
  PrunerSupervisorStopTimedOut
}

/// Stops a pruner's own supervisor (and, with it, the pruner actor itself),
/// bounded the same way `grind/queue.stop` bounds its own supervisor
/// shutdown (`grind_postgres_ffi:stop_consumer_supervisor/1`, a plain
/// `gen_server:stop` under a 6000ms bound — generic over any supervisor
/// pid, not queue-specific despite its name). Only meaningful for a pruner
/// started with `start`; a pruner embedded via `supervised` is stopped
/// through the caller's own supervisor instead.
pub fn stop(pruner: Pruner) -> Result(Nil, StopError) {
  let Pruner(supervisor_pid:, ..) = pruner
  case stop_pruner_supervisor(supervisor_pid) {
    Ok(Nil) -> Ok(Nil)
    Error(Nil) -> Error(PrunerSupervisorStopTimedOut)
  }
}

@external(erlang, "grind_postgres_ffi", "stop_consumer_supervisor")
fn stop_pruner_supervisor(pid: process.Pid) -> Result(Nil, Nil)
