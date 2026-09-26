import cigogne
import cigogne/config
import cigogne/migration
import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleeunit
import gleeunit/should
import grind
import grind/internal/attempt
import grind/internal/consumer_hooks
import grind/internal/lease
import grind/internal/migrations
import grind/internal/store
import grind/internal/terminal
import grind/internal/unique_admission
import grind/job
import grind/observation
import grind/postgres
import grind/pruner
import grind/queue
import grind/registry
import grind/submission
import grind/unique
import grind/worker
import one_shot
import pog
import simplifile
import sinal
import sinal/forwarder

pub fn main() -> Nil {
  gleeunit.main()
}

/// The manually-polled `ValidatedPolicy` almost every test in this suite
/// starts a consumer under: no `Poll` timer of its own, so `process_one`/
/// `process_batch` drives each attempt deterministically.
fn manual_policy() -> queue.ValidatedPolicy {
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_manual_polling
    |> queue.validate_policy
  policy
}

pub fn version_test() {
  grind.version()
  |> should.equal("0.1.0")
}

/// Whether a `job.State` is one of the six terminal states
/// `grind_jobs_finished_at_check` (`grind_v12`) requires `finished_at` to be
/// non-null for. The exhaustive `case` (no wildcard) is the point: adding a
/// new `job.State` variant fails this module to compile until it is placed
/// on one side or the other, rather than silently leaving
/// `terminal.states_sql()` behind.
fn is_terminal_job_state(state: job.State) -> Bool {
  case state {
    job.Succeeded
    | job.BusinessFailed
    | job.RuntimeFailed
    | job.ContractMismatch
    | job.Discarded
    | job.Cancelled -> True
    job.Queued
    | job.Scheduled
    | job.Retryable
    | job.Executing
    | job.Uncertain -> False
  }
}

/// Ties `terminal.states_sql()` to `job.state_to_stored`'s own terminal
/// variants directly, rather than trusting the two hand-maintained lists
/// (the SQL fragment's literal text, and `is_terminal_job_state`'s `case`
/// above) to stay in lockstep by inspection alone.
pub fn terminal_states_sql_matches_every_job_state_test() {
  let all_states = [
    job.Queued,
    job.Scheduled,
    job.Retryable,
    job.Executing,
    job.Succeeded,
    job.BusinessFailed,
    job.RuntimeFailed,
    job.ContractMismatch,
    job.Uncertain,
    job.Discarded,
    job.Cancelled,
  ]
  list.each(all_states, fn(state) {
    let quoted = "'" <> job.state_to_stored(state) <> "'"
    string.contains(terminal.states_sql(), quoted)
    |> should.equal(is_terminal_job_state(state))
  })
}

/// Canary for `grind_postgres_ffi`'s own dependency on pog's private
/// `pog.Connection` shape: a freshly named connection must still be the
/// `{pool, Name}` tuple `grind_postgres_ffi:with_deadline/3` matches on. The
/// exact pog version pin in `gleam.toml` (`>= 4.1.0 and < 4.2.0`) is what
/// actually guards this in practice — this test is the loud failure if that
/// pin is ever widened past a pog release that changes the shape.
pub fn pog_connection_pool_shape_test() {
  let name = process.new_name("grind_pool_shape_probe")
  pog.named_connection(name)
  |> pool_connection_atom()
  |> should.be_ok()
}

@external(erlang, "grind_test_env", "pool_connection_atom")
fn pool_connection_atom(connection: pog.Connection) -> Result(a, Nil)

/// Cigogne's own pinned sha256 for each *released* migration file
/// (`shasum -a 256`, uppercased to match `binary:encode_hex/1`) — a
/// Cigogne's own pinned sha256 (`shasum -a 256`, uppercased to match
/// `binary:encode_hex/1`) for every *released* migration file — one not
/// still under active development in this same change. Add one entry here,
/// computed once, whenever a migration file is released (see AGENTS.md,
/// "Adding a migration"); never recomputed from the file itself, or the pin
/// would be meaningless. `grind_migrations_conformance_test` requires a
/// pinned entry for every file except the newest (highest) version — that
/// one may still be edited in this same change, as v12 currently is.
const released_migration_sha256 = [
  #(
    "20260925000000-grind_v11.sql",
    "2B79E6CBD28A36850E31E1D69CC0C353D9CCFC4A4D8CEC688B0DC9C4ECEF17A0",
  ),
]

/// Proves `grind/internal/migrations.migrations()` (what `postgres.migrate`
/// actually executes) stays in lockstep with the cigogne-format files under
/// `priv/migrations/`, using cigogne's own public parser and its own
/// `config.get("grind")` (exercising the real `priv/cigogne.toml`) rather
/// than Grind's own ad hoc parsing or a hand-built config — so a change to
/// cigogne's file format, or to `priv/cigogne.toml` itself, is caught here
/// too. Runs with no database:
///
/// - Every file's `up` statements (trailing `;` stripped) equal the
///   matching `migrations()` entry, in order.
/// - The version the file's own name encodes (`<timestamp>-grind_v<N>.sql`)
///   equals both the `migrations()` key and the version its own trailing
///   marker statement records — which must itself be a
///   `INSERT INTO grind_schema_migrations` statement, not merely end with a
///   number that happens to parse.
/// - Every file except the newest (highest) version has a pinned sha256 in
///   `released_migration_sha256` above, and it matches.
/// - The frozen upgrade-harness fixture (`test/fixtures/schema/v11.sql`)
///   equals the pinned v11 migration's own `up` section exactly — so the
///   two can never silently drift apart.
pub fn grind_migrations_conformance_test() {
  let assert Ok(config) = config.get("grind")
  let assert Ok(files) = cigogne.read_migrations(config)
  let sorted = list.sort(files, migration.compare)
  let defined = migrations.migrations()
  list.length(sorted) |> should.equal(list.length(defined))
  let newest_version =
    list.fold(defined, 0, fn(highest, step) { int.max(highest, step.version) })
  list.zip(sorted, defined)
  |> list.each(fn(pair) {
    let #(file, step) = pair
    let file_version = version_from_migration_name(file.name)
    file_version |> should.equal(step.version)
    let up_statements =
      file.queries_up
      |> list.map(fn(statement) { drop_trailing_semicolon(statement) })
    up_statements |> should.equal(step.statements)
    let assert Ok(marker_statement) = list.last(up_statements)
    string.starts_with(marker_statement, "INSERT INTO grind_schema_migrations")
    |> should.be_true()
    let assert Some(marker_version) = marker_insert_version(marker_statement)
    marker_version |> should.equal(step.version)
    let pinned_sha256 =
      list.key_find(released_migration_sha256, filename_from_path(file.path))
    case step.version == newest_version {
      True ->
        case pinned_sha256 {
          Error(Nil) -> Nil
          Ok(sha256) -> file.sha256 |> should.equal(sha256)
        }
      False -> {
        let assert Ok(sha256) = pinned_sha256
        file.sha256 |> should.equal(sha256)
      }
    }
  })
  let assert Ok(v11_step) = list.find(defined, fn(step) { step.version == 11 })
  let assert Ok(fixture_contents) =
    simplifile.read(from: "test/fixtures/schema/v11.sql")
  let fixture_statements =
    fixture_contents
    |> string.split("\n")
    |> list.filter(fn(line) { string.trim(line) != "" })
    |> string.join("")
    |> string.split(";")
    |> list.map(string.trim)
    |> list.filter(fn(statement) { statement != "" })
  fixture_statements |> should.equal(v11_step.statements)
}

fn drop_trailing_semicolon(statement: String) -> String {
  case string.ends_with(statement, ";") {
    True -> string.drop_end(statement, 1)
    False -> statement
  }
}

fn filename_from_path(path: String) -> String {
  path |> string.split("/") |> list.last() |> result.unwrap(path)
}

/// Parses the version out of a migration's own `name` field (the part of
/// its filename after `<timestamp>-`), which this repository's naming
/// convention (`grind_v<N>`) always encodes — see AGENTS.md.
fn version_from_migration_name(name: String) -> Int {
  let assert Ok(digits) = string.split_once(name, "grind_v")
  let assert Ok(version) = int.parse(digits.1)
  version
}

/// Parses the version out of the migration's own trailing marker insert
/// (`INSERT INTO grind_schema_migrations (version) VALUES (<N>)`), so the
/// conformance test can independently cross-check the file name against
/// what the file's own last statement actually records.
fn marker_insert_version(statement: String) -> Option(Int) {
  case string.split_once(statement, "VALUES (") {
    Error(Nil) -> None
    Ok(#(_, rest)) ->
      case string.split_once(rest, ")") {
        Error(Nil) -> None
        Ok(#(digits, _)) -> int.parse(string.trim(digits)) |> option.from_result
      }
  }
}

pub type LookupFailure {
  AccountMissing(account_id: Int)
}

type WorkerProbe {
  WorkerInvoked
  LaterWorkerInvoked
}

type RetryPolicyProbe {
  RetryPolicyInvoked(Int, Int)
}

type LeaseCommand {
  ReleaseAttempt
}

type LeaseSignal {
  FirstAttemptStarted(process.Subject(LeaseCommand))
  TakeoverAttemptStarted(process.Subject(LeaseCommand))
}

type LongCallEvent {
  LongCallReturned(Result(Bool, queue.ProcessError))
  LongCallDown(process.Down)
}

type LongHandlerSignal {
  LongHandlerStarted(process.Subject(LeaseCommand))
}

type WorkerDeathSignal {
  WorkerDeathStarted(process.Pid, process.Subject(LeaseCommand))
}

type ConcurrentClaimSignal {
  ConcurrentClaimWorkerStarted(process.Subject(LeaseCommand))
}

type ClaimGateSignal {
  ClaimGateAcquired(process.Subject(LeaseCommand))
  ClaimGateReleased(Bool)
}

type CapacitySignal {
  CapacityWorkerStarted(Int, process.Subject(LeaseCommand))
}

type ConsumerOwnerStart {
  ConsumerOwnerStarted(process.Pid, queue.Consumer, process.Subject(Nil))
  ConsumerOwnerStopCompleted(Result(queue.StopOutcome, queue.StopError))
  ConsumerOwnerFailed(queue.StartError)
}

type CoordinatorLossSignal {
  CoordinatorLossStarted(process.Pid, process.Subject(LeaseCommand))
}

type OwnerPoolLossSignal {
  OwnerPoolLossStarted(process.Pid, process.Subject(LeaseCommand))
}

type OwnerPoolLossOwnerEvent {
  OwnerPoolLossOwnerReady(process.Pid, queue.Consumer)
  OwnerPoolLossOwnerFailed(queue.StartError)
}

pub fn invocation_preserves_the_application_error_test() {
  let assert Ok(input_codec) =
    worker.codec("account-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("account-output-v1", json.string, decode.string)
  let assert Ok(account_lookup) =
    worker.define(
      "accounts.lookup",
      "v1",
      input_codec,
      output_codec,
      fn(account_id) { Error(AccountMissing(account_id)) },
    )

  worker.invoke(account_lookup, 42)
  |> should.equal(Error(AccountMissing(42)))
}

pub fn queue_response_adapter_keeps_the_ordinary_worker_result_test() {
  let assert Ok(input_codec) =
    worker.codec("queue-adapter-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("queue-adapter-output-v1", json.string, decode.string)
  let assert Ok(delay) = worker.retry_delay(0)
  let probe = process.new_subject()
  let assert Ok(lookup) =
    worker.define(
      "accounts.queue-adapter",
      "v1",
      input_codec,
      output_codec,
      fn(account_id) {
        case account_id > 0 {
          True -> Ok("ordinary result")
          False -> Error(AccountMissing(account_id))
        }
      },
    )
  let queue_lookup =
    worker.with_queue_handler(lookup, fn(_) {
      process.send(probe, WorkerInvoked)
      worker.WorkerSnoozed(delay, "wait for account")
    })

  worker.invoke(queue_lookup, 42)
  |> should.equal(Ok("ordinary result"))
  worker.respond(queue_lookup, 42)
  |> should.equal(worker.WorkerSnoozed(delay, "wait for account"))
  process.receive(probe, within: 1000) |> should.equal(Ok(WorkerInvoked))
  worker.respond(lookup, 42)
  |> should.equal(worker.WorkerSucceeded("ordinary result"))
}

pub fn deterministic_default_retry_backoff_is_bounded_test() {
  worker.default_retry_delay_milliseconds(1) |> should.equal(15_000)
  worker.default_retry_delay_milliseconds(2) |> should.equal(30_000)
  worker.default_retry_delay_milliseconds(13) |> should.equal(61_440_000)
  worker.default_retry_delay_milliseconds(14) |> should.equal(86_400_000)
  worker.default_retry_delay_milliseconds(100) |> should.equal(86_400_000)
}

pub fn retry_settings_reject_invalid_values_before_resources_test() {
  let assert Ok(input_codec) =
    worker.codec("retry-validation-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("retry-validation-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("retry.validation", "v1", input_codec, output_codec, fn(v) {
      Ok(int.to_string(v))
    })

  worker.with_max_attempts(definition, 0)
  |> should.equal(Error(worker.AttemptLimitMustBePositive))
  let maximum_attempts = worker.max_attempts_supported_maximum()
  let assert Ok(_) = worker.with_max_attempts(definition, maximum_attempts)
  worker.with_max_attempts(definition, maximum_attempts + 1)
  |> should.equal(Error(worker.AttemptLimitExceedsSupportedMaximum))
  worker.retry_delay(-1)
  |> should.equal(Error(worker.RetryDelayMustNotBeNegative))
}

pub fn retry_delay_rejects_values_above_supported_precision_bound_test() {
  let maximum = worker.retry_delay_maximum_milliseconds()
  worker.retry_delay(maximum)
  |> result.map(worker.retry_delay_milliseconds)
  |> should.equal(Ok(maximum))
  worker.retry_delay(maximum + 1)
  |> should.equal(Error(worker.RetryDelayExceedsSupportedMaximum))
}

pub fn renewal_ticks_are_scoped_to_the_active_attempt_test() {
  queue.renewal_is_current(10, 2, 10, 2) |> should.equal(True)
  queue.renewal_is_current(10, 2, 11, 3) |> should.equal(False)
}

pub fn pending_shutdown_waiters_keep_the_original_deadline_test() {
  queue.next_shutdown_generation(7, True) |> should.equal(7)
  queue.next_shutdown_generation(7, False) |> should.equal(8)
}

pub fn attempt_resolution_exhausts_before_consulting_retry_policy_test() {
  let assert Ok(delay) = worker.retry_delay(500)
  let probe = process.new_subject()
  let definition = resolver_test_worker(probe, delay)
  let context = worker.RetryContext(2, 2, 3)

  worker.resolve_response(
    definition,
    worker.WorkerFailed(AccountMissing(42)),
    context,
  )
  |> should.equal(worker.ResolvedBusinessFailure(
    AccountMissing(42),
    worker.BudgetExhausted,
  ))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
}

pub fn snooze_is_not_a_retry_policy_decision_test() {
  let assert Ok(delay) = worker.retry_delay(250)
  let probe = process.new_subject()
  let definition = resolver_test_worker(probe, delay)
  let context = worker.RetryContext(2, 2, 3)

  worker.resolve_response(
    definition,
    worker.WorkerSnoozed(delay, "wait for account"),
    context,
  )
  |> should.equal(worker.ResolvedSnoozed(delay, "wait for account"))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
}

pub fn retry_policy_is_called_once_only_for_nonexhausted_business_failure_test() {
  let assert Ok(delay) = worker.retry_delay(500)
  let probe = process.new_subject()
  let definition = resolver_test_worker(probe, delay)
  let active_attempt = worker.RetryContext(1, 2, 0)
  let exhausted_attempt = worker.RetryContext(2, 2, 0)

  worker.resolve_response(
    definition,
    worker.WorkerSucceeded("ok"),
    active_attempt,
  )
  |> should.equal(worker.ResolvedSucceeded("ok"))
  worker.resolve_response(
    definition,
    worker.WorkerDiscarded("skip"),
    active_attempt,
  )
  |> should.equal(worker.ResolvedDiscarded("skip"))
  worker.resolve_response(
    definition,
    worker.WorkerCancelled("cancelled by worker"),
    active_attempt,
  )
  |> should.equal(worker.ResolvedCancelled("cancelled by worker"))
  worker.resolve_response(
    definition,
    worker.WorkerUncertain("effect may have happened"),
    active_attempt,
  )
  |> should.equal(worker.ResolvedUncertain("effect may have happened"))
  worker.resolve_response(
    definition,
    worker.WorkerFailed(AccountMissing(42)),
    exhausted_attempt,
  )
  |> should.equal(worker.ResolvedBusinessFailure(
    AccountMissing(42),
    worker.BudgetExhausted,
  ))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))

  worker.resolve_response(
    definition,
    worker.WorkerFailed(AccountMissing(43)),
    active_attempt,
  )
  |> should.equal(worker.ResolvedRetryable(AccountMissing(43), delay))
  process.receive(probe, within: 0)
  |> should.equal(Ok(RetryPolicyInvoked(1, 43)))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
}

fn resolver_test_worker(
  probe: process.Subject(RetryPolicyProbe),
  delay: worker.RetryDelay,
) -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input_codec) =
    worker.codec("resolver-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolver-output-v1", json.string, decode.string)
  let assert Ok(base) =
    worker.define("worker.resolver", "v1", input_codec, output_codec, fn(_) {
      Error(AccountMissing(0))
    })
  let assert Ok(limited) = worker.with_max_attempts(base, 2)
  let policy =
    worker.retry_policy(fn(failure, context) {
      case failure {
        worker.BusinessFailure(AccountMissing(account_id)) -> {
          let worker.RetryContext(current_attempt:, ..) = context
          process.send(probe, RetryPolicyInvoked(current_attempt, account_id))
        }
      }
      worker.RetryAfter(delay)
    })
  worker.with_retry_policy(limited, policy)
}

pub fn postgres_stopped_consumer_handle_does_not_retarget_after_restart_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_stale_consumer_handle_test(database_url)
  }
}

pub fn postgres_repeated_stop_after_coordinator_gone_reports_without_drain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_stop_without_drain_test(database_url)
  }
}

pub fn postgres_supervised_owner_restart_resumes_automatic_polling_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_owner_restart_test(database_url)
  }
}

pub fn postgres_foreign_process_cannot_stop_consumer_owner_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_foreign_stop_test(database_url)
  }
}

pub fn postgres_consumer_stop_timeout_is_reported_and_owner_survives_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_stop_timeout_test(database_url)
  }
}

pub fn postgres_consumer_stop_drains_active_attempt_before_supervisor_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_stop_drain_test(database_url)
  }
}

pub fn postgres_consumer_stop_reports_active_work_after_grace_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_forced_stop_test(database_url)
  }
}

pub fn postgres_forced_stop_releases_worker_and_pool_then_recovers_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_forced_stop_pool_cleanup_test(database_url)
  }
}

pub fn postgres_automatic_poll_pauses_and_renews_during_drain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_drain_test(database_url)
  }
}

pub fn postgres_coordinator_loss_with_active_work_quarantines_without_replay_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_coordinator_loss_test(database_url)
  }
}

pub fn postgres_owner_loss_recovers_on_fresh_consumer_after_pool_restart_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_owner_loss_recovers_after_pool_restart_test(database_url)
  }
}

pub fn postgres_stale_shutdown_grace_timer_does_not_end_a_later_drain_early_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_stale_shutdown_grace_timer_test(database_url)
  }
}

type ShutdownAttemptStarted {
  ShutdownWorkerStarted(
    pid: process.Pid,
    release: process.Subject(LeaseCommand),
  )
}

type ShutdownEvent {
  ShutdownSupervisorDown
  ShutdownWorkerDown
}

@external(erlang, "erlang", "suspend_process")
fn suspend_process(pid: process.Pid) -> Bool

@external(erlang, "erlang", "resume_process")
fn resume_process(pid: process.Pid) -> Bool

fn run_consumer_stop_timeout_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stop-timeout-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stop-timeout-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("stop.timeout", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, ShutdownWorkerStarted(process.self(), release))
      case process.receive(release, within: 20_000) {
        Ok(ReleaseAttempt) -> Ok("completed-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("stop-timeout")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "stop-timeout", definition, 20)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_shutdown_grace(0)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(ShutdownWorkerStarted(worker_pid, _release)) =
    process.receive(started, within: 5000)
  let root_pid = queue.supervisor_pid(consumer)
  let root_monitor = process.monitor(root_pid)
  let worker_monitor = process.monitor(worker_pid)
  suspend_process(root_pid) |> should.equal(True)
  use <- exception.defer(fn() { resume_suspended_test_process(root_pid) })

  // A suspended supervisor makes the bounded stop wait expire. The operation
  // must report that uncertainty and leave its original linked owner alive.
  queue.stop(consumer)
  |> should.equal(Error(queue.ConsumerStopTimedOut))
  process.is_alive(process.self()) |> should.equal(True)
  case process.is_alive(root_pid) {
    True -> resume_process(root_pid) |> should.equal(True)
    False -> Nil
  }
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(root_monitor, fn(_) {
      ShutdownSupervisorDown
    })
    |> process.select_specific_monitor(worker_monitor, fn(_) {
      ShutdownWorkerDown
    })
  case process.selector_receive(selector, within: 10_000) {
    Ok(ShutdownSupervisorDown) ->
      process.selector_receive(selector, within: 10_000)
      |> should.equal(Ok(ShutdownWorkerDown))
    Ok(ShutdownWorkerDown) ->
      process.selector_receive(selector, within: 10_000)
      |> should.equal(Ok(ShutdownSupervisorDown))
    Error(Nil) -> should.fail()
  }
  process.is_alive(process.self()) |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  let _ = process.demonitor_process(root_monitor)
  let _ = process.demonitor_process(worker_monitor)
  mark_database_test_executed("consumer-stop-timeout-owner-survived")
}

fn run_consumer_stop_drain_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stop-drain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stop-drain-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("stop.drain", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, ShutdownWorkerStarted(process.self(), release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("drained-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("stop-drain")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "stop-drain", definition, 21)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_shutdown_grace(2000)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let process_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(process_reply, queue.process_one(consumer))
    })
  let assert Ok(ShutdownWorkerStarted(_, release)) =
    process.receive(started, within: 5000)
  let shutdown_seen = process.new_subject()
  let late_process_result = process.new_subject()
  let observer_ready = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(observer_ready, Nil)
      let shutting_down = wait_for_shutdown_state(consumer, 300)
      process.send(shutdown_seen, shutting_down)
      case shutting_down {
        True -> {
          process.send(late_process_result, queue.process_one(consumer))
          process.send(release, ReleaseAttempt)
        }
        False -> Nil
      }
    })
  process.receive(observer_ready, within: 1000) |> should.equal(Ok(Nil))

  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  process.receive(shutdown_seen, within: 1000) |> should.equal(Ok(True))
  process.receive(late_process_result, within: 1000)
  |> should.equal(Ok(Error(queue.QueueShuttingDown)))
  process.receive(process_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("consumer-stop-drained-active-worker")
}

fn run_consumer_forced_stop_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stop-forced-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stop-forced-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invocations = process.new_subject()
  let assert Ok(definition) =
    worker.define("stop.forced", "v1", input_codec, output_codec, fn(value) {
      process.send(invocations, value)
      let release = process.new_subject()
      process.send(started, ShutdownWorkerStarted(process.self(), release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("forced-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("stop-forced")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "stop-forced", definition, 22)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_shutdown_grace(0)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let process_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(process_reply, queue.process_one(consumer))
    })
  let assert Ok(ShutdownWorkerStarted(_, _release)) =
    process.receive(started, within: 5000)
  process.receive(invocations, within: 1000) |> should.equal(Ok(22))

  queue.stop(consumer)
  |> should.equal(Ok(queue.StoppedWithActiveWork(1)))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  process.receive(process_reply, within: 1000)
  |> should.equal(Ok(Error(queue.QueueActorExited)))
  process.receive(invocations, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("consumer-stop-forced-active-work-retained")
}

/// Polls (bounded, never a fixed sleep used as the assertion) for backends
/// belonging to Grind's own pool that are still visible in
/// `pg_stat_activity` after `postgres.close`. Grind never sets
/// `application_name` on its connections, so leftover backends are found by
/// exclusion instead: the observer's own backend (`pg_backend_pid()`) and
/// any non-client backend (autovacuum, walsender, background workers) are
/// excluded, leaving only ordinary client connections against this
/// database and user — which, in the disposable test cluster, are only ever
/// Grind's own pool connections plus this one observer. Returns `Ok(0)`
/// once none remain, or `Ok(leftover_count)` if bounded checks are
/// exhausted first — a nonzero result here is a real finding, not
/// something this test papers over.
fn poll_leftover_grind_backends(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [0] -> Ok(0)
        [count] ->
          case checks_remaining > 0 {
            True -> {
              process.sleep(20)
              poll_leftover_grind_backends(connection, checks_remaining - 1)
            }
            False -> Ok(count)
          }
        _ -> Error(Nil)
      }
  }
}

/// Forced shutdown (grace 0) releases both the worker process and Grind's
/// own connection pool, and a fresh pool/consumer on the same pool name
/// recovers the orphaned attempt as `Uncertain` with no second invocation —
/// the same no-replay contract as every other owner-loss recovery path,
/// now exercised across a real pool close/reopen rather than only a killed
/// owner process.
fn run_forced_stop_pool_cleanup_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_settings =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = postgres.connection(observer)

  let assert Ok(input_codec) =
    worker.codec("forced-stop-cleanup-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("forced-stop-cleanup-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invocations = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "forced.stop.cleanup",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(invocations, WorkerInvoked)
        let release = process.new_subject()
        process.send(started, ShutdownWorkerStarted(process.self(), release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("cleanup-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("forced-stop-cleanup")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "forced-stop-cleanup", definition, 44)
  let job_id = job.id_value(handle)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_shutdown_grace(0)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  let process_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(process_reply, queue.process_one(consumer))
    })
  let assert Ok(ShutdownWorkerStarted(worker_pid, _release)) =
    process.receive(started, within: 5000)
  process.receive(invocations, within: 1000) |> should.equal(Ok(WorkerInvoked))
  let worker_monitor = process.monitor(worker_pid)

  queue.stop(consumer)
  |> should.equal(Ok(queue.StoppedWithActiveWork(1)))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(worker_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(..)) =
    process.selector_receive(down_selector, within: 5000)
  process.receive(process_reply, within: 1000)
  |> should.equal(Ok(Error(queue.QueueActorExited)))

  // Sanity-checks that the leftover-backend query below is not vacuously
  // always 0: with Grind's own pool still open (migrate/submit/state have
  // all just run queries through it), at least one client backend other
  // than the observer's own must be visible right now. A single check
  // (`checks_remaining: 0`) reuses the same query as the bounded poll below
  // instead of a bespoke one-off.
  let assert Ok(leftover_before_close) =
    poll_leftover_grind_backends(observer_connection, 0)
  should.be_true(leftover_before_close >= 1)

  // No ack ever ran (the worker died mid-attempt), so there is nothing to
  // reconcile against yet; the row is still `executing` with a live lease.
  let _ = postgres.close(database)

  let assert Ok(leftover) =
    poll_leftover_grind_backends(observer_connection, 300)
  leftover |> should.equal(0)

  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  postgres.state(reopened, handle) |> should.equal(Ok(job.Executing))

  let assert Ok(new_consumer) = queue.start(reopened, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(new_consumer)
    Nil
  })
  // Drives expiry at the database boundary rather than sleeping past the
  // original lease duration.
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: observer_connection)
  forced_expiry.count |> should.equal(1)

  queue.process_one(new_consumer) |> should.equal(Ok(False))
  postgres.state(reopened, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invocations, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("forced-stop-pool-cleanup-recovered")
}

fn run_automatic_drain_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-drain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-drain-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("auto.drain", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CapacityWorkerStarted(value, release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("drained-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("auto-drain")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "auto-drain", definition, 31)
  let assert Ok(second_handle) =
    postgres.submit(database, "auto-drain", definition, 32)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_jobs_per_poll(2)
    |> queue.with_maximum_concurrency(1)
    |> queue.with_lease_duration(1600)
    |> queue.with_shutdown_grace(2000)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let assert Ok(CapacityWorkerStarted(31, release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(release, ReleaseAttempt)
    Nil
  })
  let connection = postgres.connection(database)
  let shutdown_observed = process.new_subject()
  let observer_ready = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(observer_ready, Nil)
      let entered_drain = wait_for_shutdown_state(consumer, 300)
      let second_state = postgres.state(database, second_handle)
      let observed_expiry =
        lease_expiration(connection, job.id_value(first_handle))
      let renewed_during_drain = case observed_expiry {
        Ok(expiry) ->
          await_later_lease_expiry(
            connection,
            job.id_value(first_handle),
            expiry + 20,
            70,
          )
        Error(Nil) -> False
      }
      process.send(release, ReleaseAttempt)
      process.send(shutdown_observed, #(
        entered_drain,
        second_state,
        renewed_during_drain,
      ))
    })
  process.receive(observer_ready, within: 1000) |> should.equal(Ok(Nil))

  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  process.receive(shutdown_observed, within: 3000)
  |> should.equal(Ok(#(True, Ok(job.Queued), True)))
  postgres.state(database, first_handle) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, second_handle) |> should.equal(Ok(job.Queued))
  process.receive(started, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("automatic-drain-paused-poll-and-renewed")
}

fn wait_for_shutdown_state(
  consumer: queue.Consumer,
  checks_remaining: Int,
) -> Bool {
  case queue.shutdown_state(consumer) {
    Ok(True) -> True
    Ok(False) ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          wait_for_shutdown_state(consumer, checks_remaining - 1)
        }
        False -> False
      }
    Error(_) -> False
  }
}

fn resume_suspended_test_process(pid: process.Pid) -> Nil {
  // The successful explicit resume may finish the supervisor termination
  // before this failure-safe cleanup runs. OTP raises badarg if it resumes an
  // already resumed or dead process, which is harmless only in this cleanup.
  let _ = exception.rescue(fn() { resume_process(pid) })
  Nil
}

fn run_foreign_stop_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("foreign-stop-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("foreign-stop-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("foreign.stop", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("foreign-stop")
  let assert Ok(workers) = registry.register(workers, definition)
  let started = process.new_subject()
  let owner =
    process.spawn_unlinked(fn() {
      case queue.start(database, workers, manual_policy()) {
        Error(error) -> process.send(started, ConsumerOwnerFailed(error))
        Ok(consumer) -> {
          let stop = process.new_subject()
          process.send(
            started,
            ConsumerOwnerStarted(process.self(), consumer, stop),
          )
          let _ = process.receive(stop, within: 60_000)
          let result = queue.stop(consumer)
          process.send(started, ConsumerOwnerStopCompleted(result))
        }
      }
    })
  let owner_monitor = process.monitor(owner)
  let assert Ok(ConsumerOwnerStarted(owner_pid, consumer, stop_owner)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    case process.is_alive(owner_pid) {
      False -> Nil
      True -> {
        process.send(stop_owner, Nil)
        let _ = process.receive(started, within: 5000)
        Nil
      }
    }
  })
  queue.stop(consumer)
  |> should.equal(Error(queue.ConsumerOwnedByAnotherProcess))
  // A foreign caller must not terminate a supervisor that remains linked to
  // the process that created the consumer.
  process.is_alive(owner_pid) |> should.equal(True)
  process.send(stop_owner, Nil)
  process.receive(started, within: 5000)
  |> should.equal(Ok(ConsumerOwnerStopCompleted(Ok(queue.StoppedCleanly))))
  let selector =
    process.new_selector()
    |> process.select_monitors(fn(_) { Nil })
  process.selector_receive(selector, within: 5000)
  |> should.equal(Ok(Nil))
  let _ = process.demonitor_process(owner_monitor)
  mark_database_test_executed("foreign-consumer-stop-owner-preserved")
}

fn run_owner_restart_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("owner-restart-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("owner-restart-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("owner.restart", "v1", input_codec, output_codec, fn(value) {
      Ok("restarted-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("owner-restart")
  let assert Ok(workers) = registry.register(workers, definition)
  let policy =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.validate_policy
  let assert Ok(policy) = policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let assert Ok(first_coordinator) = queue.coordinator_pid(consumer)
  process.kill(first_coordinator)
  // `Consumer.subject` is named, so it retargets to whichever coordinator
  // incarnation is currently registered: wait for the restart to land, then
  // `process_one` should reach the new, idle incarnation and report no due
  // work, rather than racing an immediate call against however far the
  // restart has progressed.
  let assert Ok(_) = await_new_coordinator_pid(consumer, first_coordinator, 500)
  queue.process_one(consumer) |> should.equal(Ok(False))

  let assert Ok(handle) =
    postgres.submit(database, "owner-restart", definition, 12)
  wait_for_succeeded(database, handle, 200) |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  let root_supervisor = queue.supervisor_pid(consumer)
  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  process.is_alive(root_supervisor) |> should.equal(False)
  mark_database_test_executed("supervised-owner-restart-resumed-polling")
}

fn wait_for_succeeded(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  checks_remaining: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(job.Succeeded) -> True
    _ ->
      case checks_remaining > 0 {
        False -> False
        True -> {
          process.sleep(25)
          wait_for_succeeded(database, handle, checks_remaining - 1)
        }
      }
  }
}

fn attempt_snapshot(
  connection: pog.Connection,
  id: Int,
) -> Result(#(Int, Int, Int, Option(String)), Nil) {
  pog.query(
    "SELECT attempt_id, attempt_epoch, (extract(epoch FROM lease_expires_at) * 1000)::bigint, attempt_owner FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use attempt_id <- decode.field(0, decode.int)
    use attempt_epoch <- decode.field(1, decode.int)
    use lease_expires_at <- decode.field(2, decode.int)
    use attempt_owner <- decode.field(3, decode.optional(decode.string))
    decode.success(#(attempt_id, attempt_epoch, lease_expires_at, attempt_owner))
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [snapshot] -> Ok(snapshot)
      _ -> Error(Nil)
    }
  })
}

fn database_time_ms(connection: pog.Connection) -> Result(Int, Nil) {
  pog.query("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
  |> pog.returning({
    use now <- decode.field(0, decode.int)
    decode.success(now)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [now] -> Ok(now)
      _ -> Error(Nil)
    }
  })
}

/// Polls the database clock (never local wall-clock time) until it passes
/// `target_unix_ms`, bounded by `checks_remaining` 10ms polls.
fn await_database_time_past(
  connection: pog.Connection,
  target_unix_ms: Int,
  checks_remaining: Int,
) -> Bool {
  case database_time_ms(connection) {
    Ok(now) if now >= target_unix_ms -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          await_database_time_past(
            connection,
            target_unix_ms,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

fn attempt_accounting(
  connection: pog.Connection,
  id: Int,
) -> Result(#(Int, Int), Nil) {
  pog.query(
    "SELECT attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use attempt_count <- decode.field(0, decode.int)
    use delivery_count <- decode.field(1, decode.int)
    decode.success(#(attempt_count, delivery_count))
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [accounting] -> Ok(accounting)
      _ -> Error(Nil)
    }
  })
}

fn await_new_coordinator_pid(
  consumer: queue.Consumer,
  previous: process.Pid,
  checks_remaining: Int,
) -> Result(process.Pid, Nil) {
  case queue.coordinator_pid(consumer) {
    Ok(pid) if pid != previous -> Ok(pid)
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          await_new_coordinator_pid(consumer, previous, checks_remaining - 1)
        }
        False -> Error(Nil)
      }
  }
}

fn run_coordinator_loss_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("coordinator-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("coordinator-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "coordinator.loss",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(invoked, WorkerInvoked)
        process.send(started, CoordinatorLossStarted(process.self(), release))
        case process.receive(release, within: 20_000) {
          Ok(ReleaseAttempt) -> Ok("coordinator-loss-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("coordinator-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "coordinator-loss", definition, 91)
  let lease_duration_ms = 2000
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_concurrency(1)
    |> queue.with_lease_duration(lease_duration_ms)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(CoordinatorLossStarted(worker_pid, _first_release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))

  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)

  let worker_monitor = process.monitor(worker_pid)
  let assert Ok(first_coordinator) = queue.coordinator_pid(consumer)
  process.kill(first_coordinator)

  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(worker_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(..)) =
    process.selector_receive(down_selector, within: 5000)

  // Snapshotted only after the worker-DOWN barrier confirms the old
  // incarnation is gone, closing the window where a renewal from that
  // incarnation landing between an earlier snapshot and the kill would make
  // this snapshot's lease stale before it is ever compared against.
  let assert Ok(#(first_attempt_id, first_epoch, first_lease, _)) =
    attempt_snapshot(connection, job_id)

  let assert Ok(second_coordinator) =
    await_new_coordinator_pid(consumer, first_coordinator, 500)
  second_coordinator |> should.not_equal(first_coordinator)

  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  let assert Ok(#(still_attempt_id, still_epoch, _, _)) =
    attempt_snapshot(connection, job_id)
  still_attempt_id |> should.equal(first_attempt_id)
  still_epoch |> should.equal(first_epoch)

  // The restarted incarnation starts with no active attempts, so any stale
  // renewal timer the dead incarnation already scheduled lands on a
  // coordinator that has nothing to renew, and the orphaned lease is left
  // untouched. `first_lease = claim_time + lease_duration_ms`, so waiting
  // (via a database-time barrier, not a fixed sleep) until the database
  // clock passes `first_lease - lease_duration_ms + 2 * renewal_interval_ms`
  // is a wait past at least one full renewal tick and comfortably short of
  // the lease's own natural expiry.
  let renewal_interval_ms = lease_duration_ms / 3
  let past_one_renewal_tick =
    first_lease - lease_duration_ms + 2 * renewal_interval_ms
  await_database_time_past(connection, past_one_renewal_tick, 400)
  |> should.equal(True)
  let assert Ok(#(_, _, after_wait_lease, _)) =
    attempt_snapshot(connection, job_id)
  after_wait_lease |> should.equal(first_lease)

  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  wait_for_job_state(database, handle, job.Uncertain, 250)
  |> should.equal(True)
  process.receive(invoked, within: 200) |> should.equal(Error(Nil))

  let assert Ok(rebound) = postgres.bind_handle(database, definition, job_id)
  postgres.resolve_uncertain(
    database,
    rebound,
    postgres.ResolutionRequest(
      "coordinator-loss-authorized-replay",
      "on-call",
      "inspect the external effect before authorizing a new delivery",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))

  let assert Ok(CoordinatorLossStarted(_, second_release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))
  process.send(second_release, ReleaseAttempt)

  wait_for_job_state(database, handle, job.Succeeded, 250)
  |> should.equal(True)
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  let assert Ok(#(final_attempt_count, final_delivery_count)) =
    attempt_accounting(connection, job_id)
  final_attempt_count |> should.equal(2)
  final_delivery_count |> should.equal(2)

  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  mark_database_test_executed("coordinator-loss-quarantined-no-replay")
}

fn run_stop_without_drain_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stop-without-drain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stop-without-drain-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "stop.without-drain",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("stop-without-drain")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())

  // First call: a genuine clean stop with no active work. `stop`'s own
  // `stop_consumer_supervisor` blocks until the supervisor (and therefore
  // the coordinator, its child) is actually terminated before returning, so
  // by the time the second call runs there is no race about whether the
  // coordinator's name is still registered.
  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))

  // Second call: the same owner, the same handle, but the coordinator this
  // consumer named is now fully gone. This must not panic (a named subject
  // send with nobody registered panics) and must not be reported as an
  // ordinary clean drain, since nothing was actually drained.
  queue.stop(consumer) |> should.equal(Ok(queue.StoppedWithoutDrain))
  mark_database_test_executed("stop-after-coordinator-gone-without-drain")
}

fn run_stale_shutdown_grace_timer_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stale-grace-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stale-grace-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("stale.grace", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CoordinatorLossStarted(process.self(), release))
      case process.receive(release, within: 30_000) {
        Ok(ReleaseAttempt) -> Ok("stale-grace-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("stale-grace")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "stale-grace", definition, 71)
  let shutdown_grace_ms = 4000
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_maximum_concurrency(1)
    |> queue.with_lease_duration(15_000)
    |> queue.with_shutdown_grace(shutdown_grace_ms)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(CoordinatorLossStarted(_first_worker_pid, _first_release)) =
    process.receive(started, within: 5000)

  // Put the first incarnation into "draining with active work" so it
  // schedules a generation-1 grace timer, then kill it before that timer
  // ever fires: the timer is a plain `erlang:send_after`, unaffected by the
  // death of the process that scheduled it, so it keeps ticking regardless.
  let assert Ok(first_coordinator) = queue.coordinator_pid(consumer)
  let stale_reply = process.new_subject()
  queue.begin_shutdown_for_test(consumer, stale_reply)
  |> should.equal(Ok(Nil))
  wait_for_shutdown_state(consumer, 200) |> should.equal(True)
  process.kill(first_coordinator)

  let assert Ok(second_coordinator) =
    await_new_coordinator_pid(consumer, first_coordinator, 500)
  second_coordinator |> should.not_equal(first_coordinator)

  let assert Ok(second_handle) =
    postgres.submit(database, "stale-grace", definition, 72)
  let assert Ok(CoordinatorLossStarted(_second_worker_pid, _second_release)) =
    process.receive(started, within: 5000)

  // Wait well clear of the restart-and-reclaim overhead above before
  // starting the new incarnation's own drain, so its generation-1 deadline
  // sits comfortably later than the first incarnation's orphaned one. Both
  // incarnations reach shutdown generation 1 on this, their first-ever
  // drain with active work, so if the stale timer is not properly scoped to
  // its own incarnation, it will match this one's generation too.
  process.sleep(1000)
  let begin_at = monotonic_ms()
  let fresh_reply = process.new_subject()
  queue.begin_shutdown_for_test(consumer, fresh_reply)
  |> should.equal(Ok(Nil))

  let outcome = process.receive(fresh_reply, within: shutdown_grace_ms + 3000)
  let elapsed_ms = monotonic_ms() - begin_at

  outcome |> should.equal(Ok(queue.ShutdownForced(1)))
  // A correct implementation cannot report forced before its own
  // `shutdown_grace_ms` has elapsed since `begin_at`, so its `elapsed_ms` is
  // always close to `shutdown_grace_ms` (only ordinary scheduling/delivery
  // jitter below it). Under the pre-fix bug, the first incarnation's
  // orphaned generation-1 timer fires at a fixed point in time set long
  // before `begin_at` (when that incarnation's own drain began), so its
  // contribution to `elapsed_ms` is `shutdown_grace_ms - (time already
  // spent on the kill, restart, and resubmit above, plus the 1000ms sleep)`
  // — structurally at most `shutdown_grace_ms - 1000`, however fast that
  // setup runs, since the 1000ms sleep alone already accounts for that much
  // of the gap. The threshold below sits with an ordinary jitter margin
  // under the correct value and a hard structural margin (not a jitter
  // margin) above the bug's own ceiling: it can only fail to catch the bug
  // if that setup work took under 400ms, which it does not in practice.
  { elapsed_ms >= shutdown_grace_ms - 600 } |> should.equal(True)

  postgres.state(database, first_handle) |> should.equal(Ok(job.Executing))
  postgres.state(database, second_handle) |> should.equal(Ok(job.Executing))
  // Clears the way for `stop`'s own final drain (in the deferred cleanup
  // above) to reach a fresh, idle third incarnation and return promptly
  // instead of waiting out another full grace period for the still-blocked
  // second worker.
  process.kill(second_coordinator)
  mark_database_test_executed(
    "stale-shutdown-grace-timer-scoped-to-incarnation",
  )
}

fn run_stale_consumer_handle_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("stale-consumer-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("stale-consumer-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("stale.consumer", "v1", input_codec, output_codec, fn(value) {
      Ok("generation-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("stale-consumer")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "stale-consumer", definition, 1)
  let assert Ok(first_consumer) =
    queue.start(database, workers, manual_policy())
  let _ = queue.stop(first_consumer)
  let assert Ok(second_consumer) =
    queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(second_consumer) })

  queue.process_one(first_consumer)
  |> should.equal(Error(queue.QueueActorExited))
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  queue.process_one(second_consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("stale-consumer-handle-rejected")
}

pub fn postgres_manual_wait_survives_a_handler_longer_than_thirty_seconds_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_long_handler_wait_test(database_url)
  }
}

fn run_long_handler_wait_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("long-handler-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("long-handler-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let finished = process.new_subject()
  let assert Ok(definition) =
    worker.define("long.handler", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, LongHandlerStarted(release))
      let _ = process.receive(release, within: 60_000)
      process.send(finished, Nil)
      Ok("long-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("long-handler")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "long-handler", definition, 9)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let caller =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let monitor = process.monitor(caller)
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(release, ReleaseAttempt)
    let _ = process.receive(finished, within: 5000)
  })
  let selector =
    process.new_selector()
    |> process.select_map(reply, fn(value) { LongCallReturned(value) })
    |> process.select_monitors(fn(down) { LongCallDown(down) })
  // process_one has no implicit 30-second deadline. If its caller exits due
  // the OTP call timeout while this valid worker is active, the monitor wins.
  process.selector_receive(selector, within: 31_000)
  |> should.equal(Error(Nil))
  process.send(release, ReleaseAttempt)
  process.receive(finished, within: 5000) |> should.equal(Ok(Nil))
  process.selector_receive(selector, within: 5000)
  |> should.equal(Ok(LongCallReturned(Ok(True))))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  let _ = process.demonitor_process(monitor)
  mark_database_test_executed("long-handler-wait-passed")
}

@external(erlang, "grind_test_env", "database_url")
fn database_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "queue_database_url")
fn queue_database_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "owner_a_url")
fn owner_a_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "owner_b_url")
fn owner_b_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_bad_url")
fn schema_bad_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_fresh_url")
fn schema_fresh_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_markers_url")
fn schema_markers_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_jobs_url")
fn schema_missing_jobs_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_migrations_url")
fn schema_missing_migrations_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_resolutions_url")
fn schema_missing_resolutions_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_acknowledgements_url")
fn schema_missing_acknowledgements_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_attempt_sequence_url")
fn schema_missing_attempt_sequence_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_unique_submissions_url")
fn schema_missing_unique_submissions_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_fk_url")
fn schema_missing_fk_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_atomic_url")
fn schema_atomic_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_concurrent_url")
fn schema_concurrent_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_partial_url")
fn schema_partial_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_upgrade_url")
fn schema_upgrade_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_upgrade_fresh_url")
fn schema_upgrade_fresh_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_future_foreign_url")
fn schema_future_foreign_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_shape_url")
fn schema_shape_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_mixed_case_url")
fn schema_mixed_case_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "resolution_route_a_url")
fn resolution_route_a_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "resolution_route_b_url")
fn resolution_route_b_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "repeatable_read_url")
fn repeatable_read_url() -> Result(String, Nil)

/// A database dedicated to the global `quarantine_expired` test: since that
/// operation sweeps every expired `executing` row for its whole storage
/// owner (not scoped to one queue), running it against the shared
/// `GRIND_TEST_DATABASE_URL` database would make the test's own row counts
/// depend on whatever other tests in this same file happen to run first and
/// leave behind (storage owner is derived from `host:port/database`, so a
/// dedicated database is a dedicated storage owner — see
/// `postgres.validate`).
@external(erlang, "grind_test_env", "quarantine_url")
fn quarantine_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "mark_database_test_executed")
fn mark_database_test_executed(contract: String) -> Nil

@external(erlang, "grind_test_env", "monotonic_ms")
fn monotonic_ms() -> Int

@external(erlang, "grind_test_env", "unique_test_run_id")
fn unique_test_run_id() -> Int

pub fn postgres_admission_round_trips_typed_arguments_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_postgres_admission_test(database_url)
  }
}

fn run_postgres_admission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("integer-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("text-output-v1", json.string, decode.string)
  let assert Ok(counter) =
    worker.define(
      "counter.increment",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value + 1)) },
    )
  let assert Ok(handle) = postgres.submit(database, "default", counter, 41)

  postgres.arguments(database, handle)
  |> should.equal(Ok(41))

  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET worker_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("v2"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)
  postgres.arguments(database, handle)
  |> should.equal(
    Error(postgres.WorkerContractMismatch(
      expected_id: "counter.increment",
      expected_version: "v1",
      actual_id: "counter.increment",
      actual_version: "v2",
    )),
  )
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET worker_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("v1"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)

  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET input_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("integer-input-v2"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)
  postgres.arguments(database, handle)
  |> should.equal(
    Error(
      postgres.CodecFailed(worker.CodecVersionMismatch(
        expected: "integer-input-v1",
        got: "integer-input-v2",
      )),
    ),
  )
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET input_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("integer-input-v1"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)

  postgres.state(database, handle)
  |> should.equal(Ok(job.Queued))
  let _ = postgres.close(database)
  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  postgres.state(reopened, handle)
  |> should.equal(Ok(job.Queued))
  mark_database_test_executed("admission-read-passed")
}

pub fn postgres_handles_are_bound_to_storage_owner_test() {
  case owner_a_url(), owner_b_url() {
    Ok(database_url_a), Ok(database_url_b) ->
      run_storage_owner_test(database_url_a, database_url_b)
    _, _ -> Nil
  }
}

fn run_storage_owner_test(
  database_url_a: String,
  database_url_b: String,
) -> Nil {
  let assert Ok(validated_a) =
    postgres.settings(database_url_a)
    |> postgres.validate
  let assert Ok(database_a) = postgres.start(validated_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(validated_b) =
    postgres.settings(database_url_b)
    |> postgres.validate
  let assert Ok(database_b) = postgres.start(validated_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(Nil) = postgres.migrate(database_b)
  let assert Ok(input_codec) =
    worker.codec("integer-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("text-output-v1", json.string, decode.string)
  let assert Ok(counter) =
    worker.define(
      "counter.increment",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value + 1)) },
    )
  let assert Ok(handle_a) = postgres.submit(database_a, "default", counter, 41)
  let assert Ok(_handle_b) = postgres.submit(database_b, "default", counter, 42)

  postgres.arguments(database_b, handle_a)
  |> should.equal(Error(postgres.StorageOwnerMismatch))
  postgres.state(database_b, handle_a)
  |> should.equal(Error(postgres.StorageOwnerMismatch))
  mark_database_test_executed("storage-owner-passed")
}

pub fn postgres_migration_rejects_incompatible_existing_schema_test() {
  case schema_bad_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_incompatible_schema_test(database_url)
  }
}

pub fn postgres_migration_installs_schema_v10_test() {
  case schema_fresh_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_v10_install_test(database_url)
  }
}

fn run_schema_v10_install_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  // Only the real v11 step here: this test's own assertions below (schema
  // shape, and a hand-inserted `succeeded` row with no `finished_at`) are
  // specifically about the v11 baseline, not whatever `migrations()` grows
  // into afterwards. The idempotency re-run further down deliberately calls
  // the full `postgres.migrate` instead, so it also exercises the real v12
  // upgrade (and its `finished_at` backfill) atop these seeded rows.
  let assert Ok(Nil) = postgres.migrate_with(database, [real_v11_migration()])
  let connection = postgres.connection(database)
  let assert Ok(schema) =
    pog.query(
      "SELECT (SELECT count(*) = 1 AND min(version) = 11 AND max(version) = 11 FROM grind_schema_migrations), (SELECT count(*) = 5 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname IN ('grind_schema_migrations', 'grind_jobs', 'grind_job_resolutions', 'grind_job_acknowledgements', 'grind_unique_submissions') AND c.relkind = 'r'), (SELECT count(*) = 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_sequence s ON s.seqrelid = c.oid WHERE n.nspname = current_schema() AND c.relname = 'grind_attempts_id_seq' AND c.relkind = 'S' AND s.seqtypid = 'bigint'::regtype AND s.seqstart = 1 AND s.seqincrement = 1 AND s.seqmin = 1 AND s.seqcache = 1 AND NOT s.seqcycle), (SELECT count(*) = 13 AND count(*) FILTER (WHERE column_name IN ('storage_owner', 'command_id', 'queue', 'job_id', 'worker_id', 'worker_version', 'attempt_id', 'attempt_epoch', 'attempt_owner', 'committed_state', 'failure_cause', 'committed_at', 'proposal_sha256')) = 13 AND count(*) FILTER (WHERE column_name IN ('proposed_state', 'output', 'output_version', 'error', 'error_version', 'failure_description', 'committed_description', 'requested_delay_ms')) = 0 AND count(*) FILTER (WHERE column_name = 'proposal_sha256' AND udt_name = 'bytea') = 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_job_acknowledgements'), (SELECT count(*) = 17 FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND c.convalidated AND c.conname IN ('grind_schema_migrations_pkey', 'grind_jobs_pkey', 'grind_jobs_state_check', 'grind_jobs_max_attempts_check', 'grind_jobs_unique_key_check', 'grind_job_resolutions_pkey', 'grind_job_resolutions_decision_check', 'grind_job_resolutions_target_state_check', 'grind_job_acknowledgements_pkey', 'grind_job_acknowledgements_attempt_key', 'grind_job_acknowledgements_committed_state_check', 'grind_job_acknowledgements_failure_cause_check', 'grind_job_acknowledgements_proposal_sha256_check', 'grind_unique_submissions_pkey', 'grind_unique_submissions_decision_check', 'grind_unique_submissions_request_sha256_check', 'grind_unique_submissions_observed_state_check')), (SELECT count(*) = 2 AND count(*) FILTER (WHERE column_name = 'unique_key_contract' AND udt_name = 'text') = 1 AND count(*) FILTER (WHERE column_name = 'unique_key_sha256' AND udt_name = 'bytea') = 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs' AND column_name IN ('unique_key_contract', 'unique_key_sha256')), (SELECT count(*) = 13 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_unique_submissions'), (SELECT count(*) = 1 FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'grind_jobs' AND indexname = 'grind_jobs_unique_candidate_idx')",
    )
    |> pog.returning({
      use version <- decode.field(0, decode.bool)
      use tables <- decode.field(1, decode.bool)
      use sequence <- decode.field(2, decode.bool)
      use receipt_columns <- decode.field(3, decode.bool)
      use constraints <- decode.field(4, decode.bool)
      use unique_job_columns <- decode.field(5, decode.bool)
      use unique_submission_columns <- decode.field(6, decode.bool)
      use unique_index <- decode.field(7, decode.bool)
      decode.success(#(
        version,
        tables,
        sequence,
        receipt_columns,
        constraints,
        unique_job_columns,
        unique_submission_columns,
        unique_index,
      ))
    })
    |> pog.execute(on: connection)
  let assert [installed] = schema.rows
  installed
  |> should.equal(#(True, True, True, True, True, True, True, True))

  let assert Ok(sequence) =
    pog.query("SELECT nextval('grind_attempts_id_seq')")
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      decode.success(attempt_id)
    })
    |> pog.execute(on: connection)
  let assert [attempt_id] = sequence.rows
  let assert Ok(inserted_job) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, output, state, available_at) VALUES ('schema-owner', 'default', 'schema.worker', 'v1', 'schema-input-v1', '1'::jsonb, 'schema-output-v1', '\"kept\"'::jsonb, 'succeeded', clock_timestamp()) RETURNING id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  let assert [job_id] = inserted_job.rows
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('schema-owner', 'schema-command', 'default', $1, 'schema.worker', 'v1', $2, 1, 'schema-attempt-owner', 'succeeded', sha256(convert_to('synthetic proposal', 'UTF8'))) ",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.execute(on: connection)
  let assert Ok(before) =
    pog.query("SELECT last_value, is_called FROM grind_attempts_id_seq")
    |> pog.returning({
      use last_value <- decode.field(0, decode.int)
      use is_called <- decode.field(1, decode.bool)
      decode.success(#(last_value, is_called))
    })
    |> pog.execute(on: connection)
  let assert [sequence_before] = before.rows

  // Upgrades the seeded-then-frozen v11 schema onto the real, current latest
  // (v12) — twice, proving idempotency — rather than a v11-only re-run, so
  // this also exercises the real `finished_at` backfill against the
  // `succeeded` row seeded above under a schema that had no such column yet.
  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  let assert Ok(preserved) =
    pog.query(
      "SELECT (SELECT count(*) = 2 AND min(version) = 11 AND max(version) = 12 FROM grind_schema_migrations), (SELECT count(*) = 1 FROM grind_jobs WHERE id = $1 AND state = 'succeeded' AND finished_at IS NOT NULL), (SELECT count(*) = 1 FROM grind_job_acknowledgements WHERE command_id = 'schema-command' AND job_id = $1 AND attempt_id = $2 AND proposal_sha256 = sha256(convert_to('synthetic proposal', 'UTF8'))), (SELECT last_value = $2 AND is_called FROM grind_attempts_id_seq)",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.returning({
      use version <- decode.field(0, decode.bool)
      use job <- decode.field(1, decode.bool)
      use receipt <- decode.field(2, decode.bool)
      use sequence <- decode.field(3, decode.bool)
      decode.success(#(version, job, receipt, sequence))
    })
    |> pog.execute(on: connection)
  let assert [preserved_data] = preserved.rows
  preserved_data |> should.equal(#(True, True, True, True))
  let assert Ok(after) =
    pog.query("SELECT last_value, is_called FROM grind_attempts_id_seq")
    |> pog.returning({
      use last_value <- decode.field(0, decode.int)
      use is_called <- decode.field(1, decode.bool)
      decode.success(#(last_value, is_called))
    })
    |> pog.execute(on: connection)
  after.rows |> should.equal([sequence_before])
  mark_database_test_executed("schema-v11-fresh-install-idempotent-passed")
}

pub fn postgres_migration_rejects_legacy_and_future_markers_test() {
  case schema_markers_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_marker_rejection_test(database_url)
  }
}

fn run_schema_marker_rejection_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  // Only the real v11 step: the "marker=11 alone" case below re-applies the
  // real, full `migrations()` on top of this genuinely v11-only physical
  // schema, so it exercises an actual v11-to-v12 upgrade rather than a no-op
  // against a schema already fully at latest.
  let assert Ok(Nil) = postgres.migrate_with(database, [real_v11_migration()])
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_schema_migrations (version) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)",
    )
    |> pog.execute(on: connection)
  let assert Error(_) = postgres.migrate(database)
  let assert Ok(legacy_marker) =
    pog.query(
      "SELECT count(*)::bigint, min(version)::bigint, max(version)::bigint FROM grind_schema_migrations",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      use minimum <- decode.field(1, decode.int)
      use maximum <- decode.field(2, decode.int)
      decode.success(#(count, minimum, maximum))
    })
    |> pog.execute(on: connection)
  legacy_marker.rows |> should.equal([#(9, 1, 9)])

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (8)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(8)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (9)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(9)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (10)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(10)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (11)")
    |> pog.execute(on: connection)
  // The marker claims only v11, but this call runs the real `migrations()`
  // (v11 and v12), and the physical schema really is v11-only at this point
  // — so this is a genuine upgrade to v12, not a no-op re-run.
  postgres.migrate(database) |> should.equal(Ok(Nil))

  // The physical schema is now genuinely v12 (from the real upgrade just
  // above). A marker set of `{12}` alone is still rejected — not because
  // `12` is unknown (it is now the real latest), but because a valid marker
  // set is always the contiguous range starting at the baseline (`11`): a
  // schema can never legitimately reach `12` without a `11` marker also on
  // record, so this is `IncompatibleSchema`, never `UnsupportedSchemaVersion`.
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (12)")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))

  // A marker genuinely beyond this build's own highest known version (`12`)
  // is the real "future schema" case `UnsupportedSchemaVersion` exists for.
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (13)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(13)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (11)")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (8)")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  mark_database_test_executed("legacy-future-schema-markers-rejected")
}

pub fn postgres_migration_refuses_missing_owned_artifacts_test() {
  case
    schema_missing_jobs_url(),
    schema_missing_migrations_url(),
    schema_missing_resolutions_url(),
    schema_missing_acknowledgements_url(),
    schema_missing_attempt_sequence_url(),
    schema_missing_unique_submissions_url()
  {
    Ok(jobs),
      Ok(migrations),
      Ok(resolutions),
      Ok(acknowledgements),
      Ok(sequence),
      Ok(unique_submissions)
    -> {
      run_missing_schema_artifact_test(jobs, "grind_jobs", False)
      run_missing_schema_artifact_test(
        migrations,
        "grind_schema_migrations",
        False,
      )
      run_missing_schema_artifact_test(
        resolutions,
        "grind_job_resolutions",
        False,
      )
      run_missing_schema_artifact_test(
        acknowledgements,
        "grind_job_acknowledgements",
        False,
      )
      run_missing_schema_artifact_test(sequence, "grind_attempts_id_seq", True)
      // The exact object-count shape a dropped `grind_unique_submissions`
      // leaves behind is identical to a never-migrated schema v10 install
      // (four tables, the same attempt sequence). This proves the two are
      // not confused: a real v11 install missing only this table still
      // fails closed as `IncompatibleSchema`, not as the friendly
      // `UnsupportedSchemaVersion(10)` reserved for a genuine legacy
      // install (see `read_legacy_schema_marker` in `src/grind/postgres.gleam`).
      run_missing_schema_artifact_test(
        unique_submissions,
        "grind_unique_submissions",
        False,
      )
      mark_database_test_executed("missing-schema-artifacts-not-repaired")
    }
    _, _, _, _, _, _ -> Nil
  }
}

fn run_missing_schema_artifact_test(
  database_url: String,
  artifact: String,
  is_sequence: Bool,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let drop_statement = case is_sequence {
    True -> "DROP SEQUENCE " <> artifact
    // `CASCADE`: `grind_jobs` is now the referenced side of three foreign
    // keys (`grind_v12`'s own `ON DELETE CASCADE` constraints), so a plain
    // `DROP TABLE grind_jobs` alone fails with `2BP01
    // dependent_objects_still_exist` instead of ever reaching the
    // `IncompatibleSchema` check this test is about. Harmless for the other
    // artifacts this same helper drops, since nothing references them.
    False -> "DROP TABLE " <> artifact <> " CASCADE"
  }
  let assert Ok(_) = pog.query(drop_statement) |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(missing) =
    pog.query("SELECT to_regclass(current_schema() || '.' || $1) IS NULL")
    |> pog.parameter(pog.text(artifact))
    |> pog.returning({
      use absent <- decode.field(0, decode.bool)
      decode.success(absent)
    })
    |> pog.execute(on: connection)
  missing.rows |> should.equal([True])
}

/// Increment 25: `v12_foreign_keys` extends the physical-shape check with a
/// `pg_constraint` lookup independent of `v12_shape`'s own `pg_class`
/// relation check (a plain foreign key backs no relation of its own) — a
/// database missing one of `grind_v12`'s three `ON DELETE CASCADE`
/// constraints (dropped by hand, here) must fail closed exactly like a
/// missing relation does, not silently pass as though the receipt-orphan
/// backstop `docs/RECOVERY-EVIDENCE.md` Increment 24 describes were still
/// in place.
pub fn postgres_migration_missing_foreign_key_shape_detected_test() {
  case schema_missing_fk_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_missing_foreign_key_test(database_url)
  }
}

fn run_missing_foreign_key_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_job_id_fkey",
    )
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(dropped) =
    pog.query(
      "SELECT count(*) = 0 FROM pg_constraint WHERE conname = 'grind_job_acknowledgements_job_id_fkey'",
    )
    |> pog.returning({
      use absent <- decode.field(0, decode.bool)
      decode.success(absent)
    })
    |> pog.execute(on: connection)
  dropped.rows |> should.equal([True])
  mark_database_test_executed("missing-foreign-key-not-repaired")
}

pub fn postgres_migration_fresh_install_is_atomic_test() {
  case schema_atomic_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_atomic_install_test(database_url)
  }
}

fn run_schema_atomic_install_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION fail_grind_ack_table_creation() RETURNS event_trigger LANGUAGE plpgsql AS $body$ DECLARE command record; BEGIN FOR command IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP IF command.object_identity LIKE '%grind_job_acknowledgements' THEN RAISE EXCEPTION 'injected Grind schema failure'; END IF; END LOOP; END $body$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE EVENT TRIGGER fail_grind_ack_table_creation ON ddl_command_end EXECUTE FUNCTION fail_grind_ack_table_creation()",
    )
    |> pog.execute(on: connection)
  let assert Error(_) = postgres.migrate(database)
  let assert Ok(_) =
    pog.query("DROP EVENT TRIGGER fail_grind_ack_table_creation")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP FUNCTION fail_grind_ack_table_creation()")
    |> pog.execute(on: connection)
  let assert Ok(rolled_back) =
    pog.query(
      "SELECT count(*)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname IN ('grind_schema_migrations', 'grind_jobs', 'grind_job_resolutions', 'grind_job_acknowledgements', 'grind_attempts_id_seq')",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  rolled_back.rows |> should.equal([0])
  mark_database_test_executed("failed-fresh-install-rolled-back")
}

/// A marker claiming a schema version newer than this build's own
/// `migrations()` (`UnsupportedSchemaVersion`) must be detected *before*
/// `read_schema_generation` ever runs any per-version shape check — proven
/// here to actually discriminate: install a real, current-latest (v12)
/// schema, then *break* its own declared shape (drop
/// `grind_unique_submissions`, one of its required relations) and add a
/// `13` marker (one past this build's own real latest, `12`) on top. A
/// shape-first implementation would evaluate the now-broken shape and
/// misreport `IncompatibleSchema` (or, worse, never notice the higher
/// marker at all); the required version-first ordering still reports
/// `UnsupportedSchemaVersion(13)` regardless — the version check never
/// reaches a shape check at all once `max > latest`.
pub fn postgres_migration_future_version_precedes_shape_check_test() {
  case schema_future_foreign_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_future_version_with_foreign_objects_test(database_url)
  }
}

fn run_future_version_with_foreign_objects_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("DROP TABLE grind_unique_submissions")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (13)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(13)))
  mark_database_test_executed("future-version-precedes-shape-check-passed")
}

/// A bare `current_schema() || '.grind_schema_migrations'` fed to
/// `to_regclass` silently folds an unquoted, mixed-case schema name to lower
/// case, so `to_regclass` looks up a schema that does not exist and
/// `schema_migrations_table_exists` wrongly reports the marker table
/// absent even once it is genuinely installed and fully functional —
/// `quote_ident` fixes it. Proven against a real database whose default
/// `search_path` is a quoted, mixed-case schema
/// (`grind_schema_mixed_case_url`, set up once by
/// `scripts/test-postgres.sh` before this pool ever connects, exactly like
/// `grind_repeatable_read_test`'s own `default_transaction_isolation`
/// override): the second `migrate` call must be a clean `Ok(Nil)` no-op.
pub fn postgres_migration_quotes_mixed_case_schema_name_test() {
  case schema_mixed_case_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_mixed_case_schema_test(database_url)
  }
}

fn run_mixed_case_schema_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  let connection = postgres.connection(database)
  let assert Ok(installed_in_mixed_case_schema) =
    pog.query(
      "SELECT current_schema() = 'MixedCase' AND to_regclass('grind_schema_migrations') IS NOT NULL",
    )
    |> pog.returning({
      use present <- decode.field(0, decode.bool)
      decode.success(present)
    })
    |> pog.execute(on: connection)
  installed_in_mixed_case_schema.rows |> should.equal([True])
  mark_database_test_executed("migrate-mixed-case-schema-no-op-passed")
}

/// Two `migrate` callers racing the exact same fresh schema. Before the
/// advisory lock, both would run `CREATE TABLE`/the marker `INSERT`
/// concurrently and one would fail on a duplicate-object or duplicate-key
/// error instead of the required "both `Ok(Nil)`, one marker" outcome — an
/// observer holds the same advisory lock key `migrate` itself uses, so both
/// callers are provably still queued behind it when this test releases it.
pub fn postgres_migrate_concurrent_migrators_both_apply_once_test() {
  case schema_concurrent_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_concurrent_migrators_test(database_url)
  }
}

fn run_concurrent_migrators_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) AS grind_test_migrate_barrier",
    )
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() { postgres.migrate(database) })
  spawn_submit(result_b, fn() { postgres.migrate(database) })
  // Both migrators above are provably blocked behind the observer's held
  // advisory lock (the only way into `migrate`'s own step transaction) for
  // as long as this sleep runs, before the observer ever releases it.
  process.sleep(300)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  process.receive(result_a, within: 10_000) |> should.equal(Ok(Ok(Nil)))
  process.receive(result_b, within: 10_000) |> should.equal(Ok(Ok(Nil)))

  let assert Ok(marker) =
    pog.query("SELECT count(*)::bigint FROM grind_schema_migrations")
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  // One marker row per real step (11 and 12) — never a duplicate for either,
  // which is what would show up here had the advisory lock not actually
  // serialised the two concurrent migrators against each step.
  marker.rows |> should.equal([2])
  mark_database_test_executed("migrate-concurrent-migrators-single-marker")
}

/// The real, released `v11` step, looked up from `migrations.migrations()`
/// rather than hand-duplicated.
fn real_v11_migration() -> migrations.Migration {
  let assert Ok(step) =
    list.find(migrations.migrations(), fn(step) { step.version == 11 })
  step
}

/// The real, highest-numbered step `migrations.migrations()` currently
/// defines (`v12` as of this change) — every synthetic test-only step below
/// appends *after* this one, so its own `shape` must extend this step's own
/// cumulative shape, not `v11`'s, or `validate_expected_shape` would see the
/// synthetic step's declared shape omit every real relation `v12` itself
/// added.
fn latest_migration() -> migrations.Migration {
  let assert Ok(step) = list.last(migrations.migrations())
  step
}

/// A synthetic version `migrate_with`-only test appends after the real
/// `migrations()` baseline — never part of `grind/internal/migrations`
/// itself, and never released. Its own statements are deliberately trivial
/// (one throwaway table plus the marker insert) since only the runner's
/// step-by-step commit/skip behaviour is under test here, not any real
/// schema change. Numbered `13` (one past the real, current highest version)
/// rather than `12`, since `12` is now a genuine released step.
fn synthetic_v13_ok_migration() -> migrations.Migration {
  migrations.Migration(
    13,
    [
      "CREATE TABLE grind_test_synthetic_v13 (id integer PRIMARY KEY)",
      "INSERT INTO grind_schema_migrations (version) VALUES (13)",
    ],
    list.append(latest_migration().shape, [
      migrations.ExpectedRelation(
        "grind_test_synthetic_v13",
        migrations.Table,
        [],
      ),
      // `id integer PRIMARY KEY` also creates this backing index implicitly.
      migrations.ExpectedRelation(
        "grind_test_synthetic_v13_pkey",
        migrations.Index,
        [],
      ),
    ]),
    latest_migration().foreign_keys,
  )
}

/// Like `synthetic_v13_ok_migration`, but its own `CREATE TABLE` is the
/// exact object identity `install_synthetic_v14_failure_trigger` arms an
/// event trigger to reject.
fn synthetic_v14_migration() -> migrations.Migration {
  migrations.Migration(
    14,
    [
      "CREATE TABLE grind_test_synthetic_v14 (id integer PRIMARY KEY)",
      "INSERT INTO grind_schema_migrations (version) VALUES (14)",
    ],
    list.append(synthetic_v13_ok_migration().shape, [
      migrations.ExpectedRelation(
        "grind_test_synthetic_v14",
        migrations.Table,
        [],
      ),
      migrations.ExpectedRelation(
        "grind_test_synthetic_v14_pkey",
        migrations.Index,
        [],
      ),
    ]),
    synthetic_v13_ok_migration().foreign_keys,
  )
}

/// Installs a `ddl_command_end` event trigger that raises whenever
/// `grind_test_synthetic_v14` is created — the same fault-injection shape
/// `run_schema_atomic_install_test` above uses against a real Grind table,
/// aimed instead at the partial-failure test's own synthetic step 14.
fn install_synthetic_v14_failure_trigger(connection: pog.Connection) -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION fail_grind_synthetic_v14() RETURNS event_trigger LANGUAGE plpgsql AS $body$ DECLARE command record; BEGIN FOR command IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP IF command.object_identity LIKE '%grind_test_synthetic_v14' THEN RAISE EXCEPTION 'injected Grind migration failure'; END IF; END LOOP; END $body$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE EVENT TRIGGER fail_grind_synthetic_v14 ON ddl_command_end EXECUTE FUNCTION fail_grind_synthetic_v14()",
    )
    |> pog.execute(on: connection)
  Nil
}

fn drop_synthetic_v14_failure_trigger(connection: pog.Connection) -> Nil {
  let assert Ok(_) =
    pog.query("DROP EVENT TRIGGER fail_grind_synthetic_v14")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP FUNCTION fail_grind_synthetic_v14()")
    |> pog.execute(on: connection)
  Nil
}

/// `migrate_with(migrations() ++ [synthetic v13 ok, synthetic v14 failing])`
/// on a fresh schema: step 11, step 12 (real), and step 13 each commit in
/// their own transaction before step 14's own transaction rolls back on its
/// injected failure — proving a mid-run failure neither undoes earlier
/// committed steps nor leaves the failed step's own partial work behind, and
/// that a second `migrate_with` call (fault removed) picks up exactly where
/// the first left off.
pub fn postgres_migrate_with_partial_failure_preserves_earlier_steps_test() {
  case schema_partial_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_partial_failure_migration_test(database_url)
  }
}

fn run_partial_failure_migration_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  install_synthetic_v14_failure_trigger(connection)

  let steps =
    list.append(migrations.migrations(), [
      synthetic_v13_ok_migration(),
      synthetic_v14_migration(),
    ])
  let assert Error(postgres.MigrationStepFailed(14, _)) =
    postgres.migrate_with(database, steps)

  let assert Ok(markers) =
    pog.query(
      "SELECT array_agg(version ORDER BY version) FROM grind_schema_migrations",
    )
    |> pog.returning({
      use versions <- decode.field(0, decode.list(decode.int))
      decode.success(versions)
    })
    |> pog.execute(on: connection)
  markers.rows |> should.equal([[11, 12, 13]])
  let assert Ok(objects) =
    pog.query(
      "SELECT to_regclass(current_schema() || '.grind_test_synthetic_v13') IS NOT NULL, to_regclass(current_schema() || '.grind_test_synthetic_v14') IS NOT NULL",
    )
    |> pog.returning({
      use v13_present <- decode.field(0, decode.bool)
      use v14_present <- decode.field(1, decode.bool)
      decode.success(#(v13_present, v14_present))
    })
    |> pog.execute(on: connection)
  objects.rows |> should.equal([#(True, False)])

  drop_synthetic_v14_failure_trigger(connection)
  postgres.migrate_with(database, steps) |> should.equal(Ok(Nil))
  let assert Ok(resumed_markers) =
    pog.query(
      "SELECT array_agg(version ORDER BY version) FROM grind_schema_migrations",
    )
    |> pog.returning({
      use versions <- decode.field(0, decode.list(decode.int))
      decode.success(versions)
    })
    |> pog.execute(on: connection)
  resumed_markers.rows |> should.equal([[11, 12, 13, 14]])
  mark_database_test_executed("migrate-partial-failure-resumes-passed")
}

/// A version's own marker being present is never trusted alone: dropping a
/// relation a later version's own declared `shape` requires, after that
/// version's marker was genuinely committed, must still be caught as
/// `IncompatibleSchema` on the next `migrate_with` call — proven against a
/// synthetic v13 here since the real v11/v12 baseline's own equivalent case
/// is already covered by `postgres_migration_refuses_missing_owned_artifacts_test`.
pub fn postgres_migrate_detects_missing_relation_in_declared_shape_test() {
  case schema_shape_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_missing_relation_shape_test(database_url)
  }
}

fn run_missing_relation_shape_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  let steps =
    list.append(migrations.migrations(), [synthetic_v13_ok_migration()])
  postgres.migrate_with(database, steps) |> should.equal(Ok(Nil))
  let assert Ok(_) =
    pog.query("DROP TABLE grind_test_synthetic_v13")
    |> pog.execute(on: connection)
  postgres.migrate_with(database, steps)
  |> should.equal(Error(postgres.IncompatibleSchema))
  mark_database_test_executed("migrate-missing-relation-shape-detected")
}

@external(erlang, "grind_test_env", "migration_deadline_url")
fn migration_deadline_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "migration_lock_url")
fn migration_lock_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "prune_url")
fn prune_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "prune_owner_b_url")
fn prune_owner_b_url() -> Result(String, Nil)

/// A synthetic step whose own statement (`pg_sleep(6)`) legitimately runs
/// longer than the pool's own `statement_deadline_ms` (4000ms default) but
/// well under a deliberately shortened `migration_deadline_ms` (9000ms here,
/// instead of the 30000ms default, purely so this test does not have to
/// wait out the full default) — proving `migrate_with` bounds each step by
/// `Settings.migration_deadline_ms`, via `grind_postgres_ffi:
/// migration_transaction_safely/3`'s own explicit checkout deadline, not by
/// the pool's shared `set_deadline`-attached one. See
/// docs/RECOVERY-EVIDENCE.md, "Acknowledgement deadline", for the mutation
/// this characterizes: a step run under the pool's shared deadline instead
/// of its own times out at ~4s instead of succeeding at ~6s.
fn synthetic_v13_slow_migration() -> migrations.Migration {
  migrations.Migration(
    13,
    [
      // `pg_types` cannot decode a bare `void` result (`pg_sleep`'s own
      // return type — see `grind/internal/unique_admission`'s identical
      // `SELECT true FROM (...)` wrapping for its advisory-lock query, and
      // its own doc comment for the full driver note), so the sleep is
      // wrapped in an outer scalar `SELECT` rather than selected directly.
      "SELECT true FROM (SELECT pg_sleep(6)) AS grind_migration_deadline_probe",
      "INSERT INTO grind_schema_migrations (version) VALUES (13)",
    ],
    latest_migration().shape,
    latest_migration().foreign_keys,
  )
}

pub fn postgres_migration_deadline_long_step_succeeds_test() {
  case migration_deadline_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_migration_deadline_long_step_test(database_url)
  }
}

fn run_migration_deadline_long_step_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_migration_deadline(9000)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let steps =
    list.append(migrations.migrations(), [synthetic_v13_slow_migration()])
  let start_ms = monotonic_ms()
  postgres.migrate_with(database, steps) |> should.equal(Ok(Nil))
  let elapsed_ms = monotonic_ms() - start_ms
  // At least the sleep itself; comfortably under the shortened migration
  // deadline, never the pool's own 4000ms `statement_deadline_ms`.
  { elapsed_ms >= 6000 } |> should.equal(True)
  { elapsed_ms < 9000 } |> should.equal(True)
  mark_database_test_executed("migration-deadline-long-step-succeeds-passed")
}

fn migration_lock_timeout_probe_migration() -> migrations.Migration {
  migrations.Migration(
    13,
    [
      "ALTER TABLE grind_jobs ADD COLUMN grind_lock_timeout_probe text",
      "INSERT INTO grind_schema_migrations (version) VALUES (13)",
    ],
    latest_migration().shape,
    latest_migration().foreign_keys,
  )
}

fn migration_lock_timeout_probe_column_exists(
  connection: pog.Connection,
) -> Bool {
  let assert Ok(returned) =
    pog.query(
      "SELECT count(*) = 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs' AND column_name = 'grind_lock_timeout_probe'",
    )
    |> pog.returning({
      use exists <- decode.field(0, decode.bool)
      decode.success(exists)
    })
    |> pog.execute(on: connection)
  let assert [exists] = returned.rows
  exists
}

pub fn postgres_migration_step_lock_timeout_returns_lock_unavailable_test() {
  case migration_lock_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_migration_step_lock_timeout_test(database_url)
  }
}

/// A migration step's own DDL/DML now runs under a constant, transaction-
/// local `lock_timeout` (2000ms, set right after the advisory lock):
/// PostgreSQL's own `55P03 lock_not_available` becomes the typed, retry-safe
/// `MigrationLockUnavailable(version)` instead of blocking for up to the
/// full `migration_deadline_ms`. `main_database`'s own `migration_deadline_ms`
/// is shortened to 3500ms here (comfortably above the required
/// `migration_lock_timeout_ms + margin` of 3000ms) purely so this test does
/// not have to wait out the 30000ms default; it stays well under
/// `spawn_lock_holder`'s own plain, unwrapped `pog.transaction` — bound by
/// pog's own hardcoded ~5000ms checkout hold time exactly like the
/// pre-Increment-15 acknowledgement path was (`docs/RECOVERY-EVIDENCE.md`,
/// "Acknowledgement deadline") — which would otherwise auto-release the
/// observer's own lock before a longer deadline ever had a chance to fire.
/// Named mutation, also this change's own red-first evidence (this is
/// exactly the code shape before this fix): temporarily removing
/// `run_migration_step_transaction`'s own `set_migration_lock_timeout` call
/// makes this exact scenario instead block on the table lock until
/// `main_database`'s 3500ms `migration_deadline_ms` force-closes the
/// connection, reporting `MigrationCommitUnknown(13)` instead — confirmed
/// empirically (`docs/RECOVERY-EVIDENCE.md` has the observed timings).
fn run_migration_step_lock_timeout_test(database_url: String) -> Nil {
  let assert Ok(main_validated) =
    postgres.settings(database_url)
    |> postgres.with_migration_deadline(3500)
    |> postgres.validate
  let assert Ok(main_database) = postgres.start(main_validated)
  use <- exception.defer(fn() { postgres.close(main_database) })
  let assert Ok(Nil) =
    postgres.migrate_with(main_database, migrations.migrations())

  let assert Ok(observer_validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(observer_database) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer_database) })
  let observer_connection = postgres.connection(observer_database)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      observer_connection,
      pog.query("LOCK TABLE grind_jobs IN ACCESS SHARE MODE"),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: guarantees the observer's own transaction is never left
  // open beyond this test, even if an assertion below panics first.
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let steps =
    list.append(migrations.migrations(), [
      migration_lock_timeout_probe_migration(),
    ])
  let start_ms = monotonic_ms()
  let outcome = postgres.migrate_with(main_database, steps)
  let elapsed_ms = monotonic_ms() - start_ms
  outcome |> should.equal(Error(postgres.MigrationLockUnavailable(13)))
  // Comfortably clears the 2000ms lock_timeout plus ordinary scheduling
  // jitter, but well under the 3500ms `migration_deadline_ms` configured
  // above (never mind the 30000ms default) a caller who removed
  // `set_migration_lock_timeout` would otherwise have to wait out for the
  // exact same contention.
  { elapsed_ms < 3200 } |> should.equal(True)

  // Generation is still 12 (the real, current latest): re-running the
  // released steps alone is a no-op, and the probe column was never added.
  postgres.migrate_with(main_database, migrations.migrations())
  |> should.equal(Ok(Nil))
  migration_lock_timeout_probe_column_exists(postgres.connection(main_database))
  |> should.equal(False)

  process.send(release_lock, ReleaseAttempt)
  let assert Ok(ClaimGateReleased(True)) =
    process.receive(lock_finished, within: 5000)

  // The conflicting lock is gone: the exact same steps now succeed.
  postgres.migrate_with(main_database, steps) |> should.equal(Ok(Nil))
  migration_lock_timeout_probe_column_exists(postgres.connection(main_database))
  |> should.equal(True)

  // `SET LOCAL` never leaks past the transaction that set it: this pooled
  // connection's own session-level `lock_timeout` is still PostgreSQL's
  // ordinary default ("0", meaning no limit), not the migration step's
  // constant 2000ms.
  let assert Ok(returned) =
    pog.query("SHOW lock_timeout")
    |> pog.returning({
      use value <- decode.field(0, decode.string)
      decode.success(value)
    })
    |> pog.execute(on: postgres.connection(main_database))
  let assert [lock_timeout_after] = returned.rows
  lock_timeout_after |> should.equal("0")

  mark_database_test_executed("migration-lock-timeout-unavailable-passed")
}

/// The frozen up-dump of the real `grind_v11` migration, applied with plain
/// `pog.execute` statement by statement — deliberately independent of
/// `grind/internal/migrations` and `priv/migrations/`, so this test still
/// exercises "a schema a previous Grind release actually installed" even if
/// a future baseline's statement text ever moved on.
fn read_sql_statements_from_file(path: String) -> List(String) {
  let assert Ok(contents) = simplifile.read(from: path)
  contents
  |> string.split("\n")
  |> list.filter(fn(line) {
    let trimmed = string.trim(line)
    trimmed != "" && !string.starts_with(trimmed, "--")
  })
  |> string.join("")
  |> string.split(";")
  |> list.map(string.trim)
  |> list.filter(fn(statement) { statement != "" })
}

fn apply_sql_statements(
  connection: pog.Connection,
  statements: List(String),
) -> Nil {
  list.each(statements, fn(statement) {
    let assert Ok(_) = pog.query(statement) |> pog.execute(on: connection)
    Nil
  })
}

/// A synthetic `v13` (one past the real, current latest `v12`) used only by
/// the upgrade harness below: adds a nullable column and an index on it to
/// `grind_jobs`, the shape of change the design calls out
/// (`docs/RELEASE-READINESS.md`, "Migration mechanism") as the one most
/// likely to interact badly with rows a previous release already wrote —
/// layered on top of the real `v12` (`finished_at`) this harness now also
/// exercises for real, rather than only against a synthetic stand-in.
fn synthetic_v13_alter_migration() -> migrations.Migration {
  migrations.Migration(
    13,
    [
      "ALTER TABLE grind_jobs ADD COLUMN grind_test_note text",
      "CREATE INDEX grind_test_note_idx ON grind_jobs (grind_test_note)",
      "INSERT INTO grind_schema_migrations (version) VALUES (13)",
    ],
    list.append(latest_migration().shape, [
      migrations.ExpectedRelation("grind_test_note_idx", migrations.Index, []),
    ]),
    latest_migration().foreign_keys,
  )
}

/// One digest per catalog kind (`information_schema.columns` — including
/// `column_default` and `ordinal_position`, so a reordered or
/// differently-defaulted column would be caught, not only a renamed or
/// retyped one — `pg_constraint`, `pg_indexes`, and `pg_sequences`), scoped
/// to every `grind_`-prefixed object in the current schema,
/// order-independent by construction (`string_agg` with an explicit
/// `ORDER BY`) — compared between the seeded-then-upgraded database and a
/// fresh `migrate_with` install of the exact same step list.
fn grind_catalog_digest(
  connection: pog.Connection,
) -> #(String, String, String, String) {
  let columns =
    catalog_digest_query(
      connection,
      "SELECT coalesce(string_agg(table_name || ':' || ordinal_position || ':' || column_name || ':' || data_type || ':' || is_nullable || ':' || coalesce(column_default, ''), ',' ORDER BY table_name, ordinal_position), '') FROM information_schema.columns WHERE table_schema = current_schema() AND table_name ~ '^grind_'",
    )
  let constraints =
    catalog_digest_query(
      connection,
      "SELECT coalesce(string_agg(conrelid::regclass::text || ':' || conname || ':' || contype::text || ':' || pg_get_constraintdef(oid), ',' ORDER BY conrelid::regclass::text, conname), '') FROM pg_constraint WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND conrelid::regclass::text ~ '^grind_'",
    )
  let indexes =
    catalog_digest_query(
      connection,
      "SELECT coalesce(string_agg(indexname || ':' || indexdef, ',' ORDER BY indexname), '') FROM pg_indexes WHERE schemaname = current_schema() AND tablename ~ '^grind_'",
    )
  let sequences =
    catalog_digest_query(
      connection,
      "SELECT coalesce(string_agg(sequencename || ':' || data_type || ':' || start_value || ':' || min_value || ':' || max_value || ':' || increment_by || ':' || cache_size || ':' || cycle, ',' ORDER BY sequencename), '') FROM pg_sequences WHERE schemaname = current_schema() AND sequencename ~ '^grind_'",
    )
  #(columns, constraints, indexes, sequences)
}

fn catalog_digest_query(connection: pog.Connection, sql: String) -> String {
  let assert Ok(returned) =
    pog.query(sql)
    |> pog.returning({
      use value <- decode.field(0, decode.string)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  let assert [value] = returned.rows
  value
}

/// The upgrade-harness contract: a database carrying a previous release's
/// frozen v11 schema and real rows in queued, scheduled, retryable,
/// executing-with-an-already-expired-lease, uncertain, and succeeded states
/// (the last two also carrying a real acknowledgement receipt, a real
/// uniqueness receipt with its `unique_key_*` columns populated, and a real
/// resolution row) survives `migrate_with` onto a synthetic v12 with every
/// seeded row intact, ends up with the exact same catalog shape a fresh
/// `migrate_with` install of the same steps produces, and stays fully
/// functional afterwards both for ordinary new API traffic (submit/claim/ack
/// on a fresh job) and, specifically, against the legacy seeded rows
/// themselves: the seeded executing lease is genuinely quarantined by a
/// legacy-queue poll, the seeded uncertain row is resolved, the seeded
/// acknowledgement receipt is reconciled by its own real command ID, and the
/// seeded unique submission is replayed (its own real request hash, not a
/// synthetic one, since it was seeded through `submit_unique` itself before
/// migrating rather than inserted by hand).
pub fn postgres_migrate_upgrade_from_frozen_v11_fixture_test() {
  case schema_upgrade_url(), schema_upgrade_fresh_url() {
    Ok(upgrade_url), Ok(fresh_url) ->
      run_upgrade_harness_test(upgrade_url, fresh_url)
    _, _ -> Nil
  }
}

fn legacy_upgrade_worker() -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input_codec) =
    worker.codec("upgrade-legacy-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("upgrade-legacy-output-v1", json.string, decode.string)
  let assert Ok(worker_def) =
    worker.define(
      "upgrade.legacy-worker",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  worker_def
}

fn run_upgrade_harness_test(upgrade_url: String, fresh_url: String) -> Nil {
  let assert Ok(upgrade_validated) =
    postgres.settings(upgrade_url) |> postgres.validate
  let assert Ok(upgrade_database) = postgres.start(upgrade_validated)
  use <- exception.defer(fn() { postgres.close(upgrade_database) })
  let upgrade_connection = postgres.connection(upgrade_database)

  let assert Ok(fresh_validated) =
    postgres.settings(fresh_url) |> postgres.validate
  let assert Ok(fresh_database) = postgres.start(fresh_validated)
  use <- exception.defer(fn() { postgres.close(fresh_database) })
  let fresh_connection = postgres.connection(fresh_database)

  apply_sql_statements(
    upgrade_connection,
    read_sql_statements_from_file("test/fixtures/schema/v11.sql"),
  )

  let legacy_worker = legacy_upgrade_worker()
  // The real storage owner this pool computes (`host:port/database`), never
  // an arbitrary chosen string — every seeded row and constructed handle
  // below must use this exact value or the typed API's own owner check
  // rejects them (`StateStorageOwnerMismatch` and friends).
  let storage_owner = postgres.storage_owner(upgrade_database)

  // Seeded via `submit_unique` itself, against the pre-migration v11
  // schema — never raw SQL — so its `request_sha256` and `unique_key_*`
  // columns are exactly what a real caller's replay after the upgrade must
  // still match; a hand-written hash could never do that honestly.
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let legacy_policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let assert Ok(submission.Inserted(legacy_unique_handle)) =
    submit_keep_existing(
      upgrade_database,
      "upgrade-legacy",
      "upgrade-legacy-submission",
      legacy_worker,
      5,
      legacy_policy,
    )

  // Seed one row per remaining state via raw SQL, in a distinct queue from
  // the uniqueness submission above so neither competes with the other
  // during the legacy-consumer polls below.
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-other', 'upgrade.legacy-worker', 'v1', 'v1', '10'::jsonb, 'v1', 'queued', clock_timestamp(), NULL, 0, NULL, NULL, 0)",
    )
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-other', 'upgrade.legacy-worker', 'v1', 'v1', '11'::jsonb, 'v1', 'scheduled', clock_timestamp() + interval '1 hour', NULL, 0, NULL, NULL, 0)",
    )
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count, failure_description, failure_cause) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-other', 'upgrade.legacy-worker', 'v1', 'v1', '12'::jsonb, 'v1', 'retryable', clock_timestamp() + interval '1 minute', NULL, 1, NULL, NULL, 1, 'legacy transient failure', NULL)",
    )
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', 'upgrade.legacy-worker', 'v1', 'v1', '1'::jsonb, 'v1', 'executing', clock_timestamp(), nextval('grind_attempts_id_seq'), 1, 'legacy-attempt-owner', clock_timestamp() - interval '1 hour', 1) RETURNING id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: upgrade_connection)
  let assert Ok(executing_job) =
    pog.query(
      "SELECT id FROM grind_jobs WHERE storage_owner = '"
      <> storage_owner
      <> "' AND queue = 'upgrade-legacy' AND state = 'executing'",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: upgrade_connection)
  let assert [executing_job_id] = executing_job.rows
  let assert Ok(uncertain_job) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count, uncertain_at) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', 'upgrade.legacy-worker', 'v1', 'v1', '2'::jsonb, 'v1', 'uncertain', clock_timestamp(), nextval('grind_attempts_id_seq'), 1, 'legacy-attempt-owner', clock_timestamp() - interval '1 hour', 1, clock_timestamp()) RETURNING id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: upgrade_connection)
  let assert [uncertain_job_id] = uncertain_job.rows
  let assert Ok(succeeded_job) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, output, state, available_at, attempt_id, attempt_epoch, attempt_owner) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', 'upgrade.legacy-worker', 'v1', 'v1', '3'::jsonb, 'v1', '\"legacy-output\"'::jsonb, 'succeeded', clock_timestamp(), nextval('grind_attempts_id_seq'), 1, 'legacy-attempt-owner') RETURNING id, attempt_id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      use attempt_id <- decode.field(1, decode.int)
      decode.success(#(id, attempt_id))
    })
    |> pog.execute(on: upgrade_connection)
  let assert [#(succeeded_job_id, succeeded_attempt_id)] = succeeded_job.rows
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-command', 'upgrade-legacy', $1, 'upgrade.legacy-worker', 'v1', $2, 1, 'legacy-attempt-owner', 'succeeded', sha256(convert_to('legacy proposal', 'UTF8')))",
    )
    |> pog.parameter(pog.int(succeeded_job_id))
    |> pog.parameter(pog.int(succeeded_attempt_id))
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', $1, 'upgrade.legacy-worker', 'v1', 'upgrade-legacy-resolution', $2, 1, 'legacy-attempt-owner', clock_timestamp(), 'confirm_success', 'succeeded', 'legacy-on-call', 'legacy resolution before this upgrade')",
    )
    |> pog.parameter(pog.int(succeeded_job_id))
    |> pog.parameter(pog.int(succeeded_attempt_id))
    |> pog.execute(on: upgrade_connection)

  // Increment 25: seeds one orphaned receipt per receipt table — a
  // `job_id` that never existed in `grind_jobs` at all, exactly what a
  // database that has been running a while under `v11` (no foreign key
  // enforcing this) could already carry for reasons unrelated to this
  // upgrade (a hand rollback, an old bug, direct SQL) — before this
  // upgrade ever reaches `v12`'s own `ADD CONSTRAINT ... FOREIGN KEY`.
  // Without the `DELETE ... WHERE NOT EXISTS` cleanup immediately ahead of
  // each `ADD CONSTRAINT` in `v12_statements`, this `ADD CONSTRAINT` itself
  // would fail closed with `23503 foreign_key_violation` against these
  // three rows (confirmed red: temporarily commenting out the three
  // `DELETE`s reproduces exactly that error against this fixture). Proves
  // both halves at once below: `migrate_with` still succeeds, and the
  // orphans are genuinely gone afterward, not merely tolerated.
  let orphaned_job_id = 999_999_999
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-orphan-ack', 'upgrade-legacy', $1, 'upgrade.legacy-worker', 'v1', 999999998, 1, 'legacy-attempt-owner', 'succeeded', sha256(convert_to('legacy orphan proposal', 'UTF8')))",
    )
    |> pog.parameter(pog.int(orphaned_job_id))
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_unique_submissions (storage_owner, submission_id, queue, worker_id, worker_version, request_sha256, decision, job_id, job_queue, observed_state) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-orphan-submission', 'upgrade-legacy', 'upgrade.legacy-worker', 'v1', sha256(convert_to('legacy orphan request', 'UTF8')), 'inserted', $1, 'upgrade-legacy', 'succeeded')",
    )
    |> pog.parameter(pog.int(orphaned_job_id))
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', $1, 'upgrade.legacy-worker', 'v1', 'upgrade-legacy-orphan-resolution', 999999998, 1, 'legacy-attempt-owner', clock_timestamp(), 'confirm_success', 'succeeded', 'legacy-on-call', 'legacy orphan resolution before this upgrade')",
    )
    |> pog.parameter(pog.int(orphaned_job_id))
    |> pog.execute(on: upgrade_connection)

  let steps =
    list.append(migrations.migrations(), [synthetic_v13_alter_migration()])
  postgres.migrate_with(upgrade_database, steps) |> should.equal(Ok(Nil))
  postgres.migrate_with(fresh_database, steps) |> should.equal(Ok(Nil))

  // The three orphaned receipts seeded above did not survive the upgrade
  // (removed by `v12`'s own `DELETE ... WHERE NOT EXISTS` cleanup, ahead of
  // its own `ADD CONSTRAINT ... FOREIGN KEY`) — and, since `migrate_with`
  // above already returned `Ok(Nil)`, that `ADD CONSTRAINT` did not fail
  // closed against them either.
  let assert Ok(orphans_removed) =
    pog.query(
      "SELECT (SELECT count(*) FROM grind_job_acknowledgements WHERE job_id = $1), (SELECT count(*) FROM grind_unique_submissions WHERE job_id = $1), (SELECT count(*) FROM grind_job_resolutions WHERE job_id = $1)",
    )
    |> pog.parameter(pog.int(orphaned_job_id))
    |> pog.returning({
      use acknowledgements <- decode.field(0, decode.int)
      use unique_submissions <- decode.field(1, decode.int)
      use resolutions <- decode.field(2, decode.int)
      decode.success(#(acknowledgements, unique_submissions, resolutions))
    })
    |> pog.execute(on: upgrade_connection)
  orphans_removed.rows |> should.equal([#(0, 0, 0)])

  // Every seeded legacy row is untouched by the upgrade.
  let assert Ok(preserved) =
    pog.query(
      "SELECT (SELECT count(*) FROM grind_jobs WHERE storage_owner = '"
      <> storage_owner
      <> "' AND queue = 'upgrade-legacy-other' AND state = 'queued'), (SELECT count(*) FROM grind_jobs WHERE storage_owner = '"
      <> storage_owner
      <> "' AND state = 'scheduled'), (SELECT count(*) FROM grind_jobs WHERE storage_owner = '"
      <> storage_owner
      <> "' AND state = 'retryable'), (SELECT count(*) FROM grind_jobs WHERE storage_owner = '"
      <> storage_owner
      <> "' AND state = 'executing'), (SELECT count(*) FROM grind_jobs WHERE storage_owner = '"
      <> storage_owner
      <> "' AND state = 'uncertain'), (SELECT count(*) FROM grind_job_acknowledgements WHERE command_id = 'upgrade-legacy-command'), (SELECT count(*) FROM grind_unique_submissions WHERE submission_id = 'upgrade-legacy-submission'), (SELECT count(*) FROM grind_jobs WHERE id = $1 AND unique_key_contract IS NOT NULL AND unique_key_sha256 IS NOT NULL), (SELECT count(*) FROM grind_job_resolutions WHERE resolution_id = 'upgrade-legacy-resolution')",
    )
    |> pog.parameter(pog.int(job.id_value(legacy_unique_handle)))
    |> pog.returning({
      use queued <- decode.field(0, decode.int)
      use scheduled <- decode.field(1, decode.int)
      use retryable <- decode.field(2, decode.int)
      use executing <- decode.field(3, decode.int)
      use uncertain <- decode.field(4, decode.int)
      use acknowledgement <- decode.field(5, decode.int)
      use unique_submission <- decode.field(6, decode.int)
      use unique_key_columns <- decode.field(7, decode.int)
      use resolution <- decode.field(8, decode.int)
      decode.success(#(
        queued,
        scheduled,
        retryable,
        executing,
        uncertain,
        acknowledgement,
        unique_submission,
        unique_key_columns,
        resolution,
      ))
    })
    |> pog.execute(on: upgrade_connection)
  preserved.rows |> should.equal([#(1, 1, 1, 1, 1, 1, 1, 1, 1)])

  // The real `v12` backfill applied by this exact upgrade: the seeded
  // `succeeded` row (terminal, written under the pre-migration v11 schema
  // that had no `finished_at` column at all) picked up a non-null
  // `finished_at` dated from this migration run, while every seeded
  // non-terminal row (queued, scheduled, retryable, executing, uncertain)
  // was nulled back out by the backfill's own second pass.
  let assert Ok(finished_at_backfill) =
    pog.query(
      "SELECT (SELECT finished_at IS NOT NULL AND finished_at > clock_timestamp() - interval '1 minute' FROM grind_jobs WHERE id = $1), (SELECT count(*) = 0 FROM grind_jobs WHERE storage_owner = '"
      <> storage_owner
      <> "' AND finished_at IS NOT NULL AND state NOT IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled'))",
    )
    |> pog.parameter(pog.int(succeeded_job_id))
    |> pog.returning({
      use succeeded_finished_recently <- decode.field(0, decode.bool)
      use no_non_terminal_finished_at <- decode.field(1, decode.bool)
      decode.success(#(succeeded_finished_recently, no_non_terminal_finished_at))
    })
    |> pog.execute(on: upgrade_connection)
  finished_at_backfill.rows |> should.equal([#(True, True)])

  // The upgraded database's catalog shape is identical to a fresh install of
  // the exact same steps.
  grind_catalog_digest(upgrade_connection)
  |> should.equal(grind_catalog_digest(fresh_connection))

  // The upgraded schema is fully functional for ordinary new traffic:
  // submit/claim/ack on a fresh job.
  let assert Ok(input_codec) =
    worker.codec("upgrade-smoke-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("upgrade-smoke-output-v1", json.string, decode.string)
  let assert Ok(smoke_worker) =
    worker.define(
      "upgrade.smoke-worker",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("upgrade-smoke")
  let assert Ok(workers) = registry.register(workers, smoke_worker)
  let assert Ok(handle) =
    postgres.submit(upgrade_database, "upgrade-smoke", smoke_worker, 41)
  let assert Ok(consumer) =
    queue.start(upgrade_database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(upgrade_database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(upgrade_database, handle)
  |> should.equal(Ok(job.SucceededWith("41")))

  // ...and, specifically, against the legacy seeded rows themselves.
  //
  // Replays the real `submit_unique` seed above with its own real request
  // hash — never a hand-written one — so this genuinely proves post-upgrade
  // idempotency, not merely that a row exists.
  let assert Ok(submission.Inserted(legacy_replayed_handle)) =
    submit_keep_existing(
      upgrade_database,
      "upgrade-legacy",
      "upgrade-legacy-submission",
      legacy_worker,
      5,
      legacy_policy,
    )
  job.id_value(legacy_replayed_handle)
  |> should.equal(job.id_value(legacy_unique_handle))

  // Quarantines the seeded already-expired executing lease via a real
  // legacy-queue poll.
  let assert Ok(legacy_workers) = registry.new("upgrade-legacy")
  let assert Ok(legacy_workers) =
    registry.register(legacy_workers, legacy_worker)
  let assert Ok(legacy_consumer) =
    queue.start(upgrade_database, legacy_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(legacy_consumer) })
  // Drains the still-queued (replayed, never claimed) unique submission
  // above first, so it has no other legitimately claimable job competing in
  // the same poll as the quarantine check below.
  queue.process_one(legacy_consumer) |> should.equal(Ok(True))
  let executing_handle =
    job.new_handle(
      executing_job_id,
      storage_owner,
      "upgrade-legacy",
      legacy_worker,
    )
  queue.process_one(legacy_consumer) |> should.equal(Ok(False))
  postgres.state(upgrade_database, executing_handle)
  |> should.equal(Ok(job.Uncertain))

  // Resolves the seeded uncertain row.
  let uncertain_handle =
    job.new_handle(
      uncertain_job_id,
      storage_owner,
      "upgrade-legacy",
      legacy_worker,
    )
  postgres.resolve_uncertain(
    upgrade_database,
    uncertain_handle,
    postgres.ResolutionRequest(
      "upgrade-legacy-uncertain-resolution",
      "on-call",
      "post-upgrade smoke resolution of the seeded uncertain row",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))

  // Reconciles the seeded acknowledgement receipt by its own real command
  // ID.
  let succeeded_handle =
    job.new_handle(
      succeeded_job_id,
      storage_owner,
      "upgrade-legacy",
      legacy_worker,
    )
  let assert Ok(receipt) =
    postgres.reconcile_acknowledgement(
      upgrade_database,
      succeeded_handle,
      "upgrade-legacy-command",
    )
  receipt.command_id |> should.equal("upgrade-legacy-command")
  receipt.attempt_id |> should.equal(succeeded_attempt_id)
  receipt.attempt_epoch |> should.equal(1)
  receipt.committed_state |> should.equal(job.Succeeded)
  receipt.business_failure_cause |> should.equal(None)

  mark_database_test_executed("migrate-upgrade-harness-passed")
}

/// `grind_jobs_finished_at_check` (`grind_v12`) is PostgreSQL's own
/// enforcement of "terminal state iff `finished_at` is set" — this probes it
/// directly with raw SQL from both directions, independent of any Grind
/// write path, so a future write site that forgets to set `finished_at`
/// correctly fails loudly at the database layer rather than silently
/// drifting. `pog_ffi`'s `convert_error` reports a `pgsql_error` that
/// carries a `constraint` field (both a check and a unique violation do) as
/// `pog.ConstraintViolated(message, constraint, detail)` — SQLSTATE `23514`
/// (`check_violation`) is what PostgreSQL raises here, though `pog` itself
/// does not surface the raw code once it has already recognised the
/// constraint name.
pub fn postgres_finished_at_check_constraint_rejects_mismatch_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_finished_at_check_constraint_test(database_url)
  }
}

fn run_finished_at_check_constraint_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  // A non-terminal state (`queued`) with `finished_at` set.
  let assert Error(queued_error) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, finished_at) VALUES ('finished-at-check-owner', 'finished-at-check', 'finished-at-check.worker', 'v1', 'v1', '1'::jsonb, 'v1', 'queued', clock_timestamp(), clock_timestamp())",
    )
    |> pog.execute(on: connection)
  let assert pog.ConstraintViolated(constraint:, ..) = queued_error
  constraint |> should.equal("grind_jobs_finished_at_check")

  // A terminal state (`succeeded`) with `finished_at` left null.
  let assert Error(succeeded_error) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at) VALUES ('finished-at-check-owner', 'finished-at-check', 'finished-at-check.worker', 'v1', 'v1', '1'::jsonb, 'v1', 'succeeded', clock_timestamp())",
    )
    |> pog.execute(on: connection)
  let assert pog.ConstraintViolated(constraint: succeeded_constraint, ..) =
    succeeded_error
  succeeded_constraint |> should.equal("grind_jobs_finished_at_check")

  mark_database_test_executed("finished-at-check-constraint-rejects-mismatch")
}

fn finished_at_is_set(connection: pog.Connection, id: Int) -> Bool {
  let assert Ok(returned) =
    pog.query("SELECT finished_at IS NOT NULL FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use finished <- decode.field(0, decode.bool)
      decode.success(finished)
    })
    |> pog.execute(on: connection)
  let assert [finished] = returned.rows
  finished
}

/// Drives one already-claimed, gated job (blocked on its own `started`
/// signal) through a concurrent `postgres.cancel` while it is genuinely
/// `executing`, then releases it and waits for the ack that follows —
/// exactly the "cancel a running attempt" sequence
/// `run_cancel_running_ack_test` uses, factored out so each of the three
/// cancel-overridable acknowledge branches can reuse it without repeating
/// the same process-spawn/handshake boilerplate.
fn drive_cancel_while_executing(
  database: postgres.Database,
  consumer: queue.Consumer,
  handle: job.JobHandle(input, output, error),
  started: process.Subject(LongHandlerSignal),
) -> Nil {
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  Nil
}

/// Table-driven proof that `finished_at` ends up non-null after every
/// terminal write site `grind_v12` added it to — `attempt
/// .acknowledge_transaction`'s eight per-outcome branches (`succeeded`,
/// `business_failed`, `discarded`, `cancelled`, and `runtime_failed` are
/// unconditional, since a concurrent cancellation only ever overrides one
/// terminal outcome with another; `retryable`, `scheduled` (from a worker
/// snooze), and `uncertain` are non-terminal unless that same override
/// fires), `attempt.mark_contract_mismatch`, `sql.cancel_before_run`, and
/// `postgres.write_resolution`'s own `UPDATE` for both of its terminal
/// target states — and stays null after every non-terminal one, including
/// `AuthorizeReplay`, whose target state (`queued`) is the one non-terminal
/// outcome `resolve_uncertain` itself can produce, and the three
/// cancel-overridden branches' own ordinary (non-cancelled) path. Every job
/// below lives on its own queue and is driven through exactly the one path
/// under test, so no scenario's own claim can race another's.
pub fn postgres_finished_at_written_at_every_terminal_path_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_finished_at_paths_test(database_url)
  }
}

fn run_finished_at_paths_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let run_queue = "finished-at-run"

  let assert Ok(plain_input) =
    worker.codec("finished-at-plain-input-v1", json.int, decode.int)
  let assert Ok(plain_output) =
    worker.codec("finished-at-plain-output-v1", json.string, decode.string)
  let assert Ok(succeeded_worker) =
    worker.define(
      "finished-at.succeeded",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(error_codec) =
    worker.codec(
      "finished-at-error-v1",
      encode_lookup_failure,
      decode_lookup_failure(),
    )
  let assert Ok(business_failed_worker) =
    worker.define_with_error_codec(
      "finished-at.business-failed",
      "v1",
      plain_input,
      plain_output,
      error_codec,
      fn(account_id) { Error(AccountMissing(account_id)) },
    )
  let assert Ok(business_failed_worker) =
    worker.with_max_attempts(business_failed_worker, 1)
  let assert Ok(discard_base) =
    worker.define(
      "finished-at.discard-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let discard_worker =
    worker.with_queue_handler(discard_base, fn(_) {
      worker.WorkerDiscarded("finished-at probe")
    })
  let assert Ok(cancel_base) =
    worker.define(
      "finished-at.cancel-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let cancel_worker =
    worker.with_queue_handler(cancel_base, fn(_) {
      worker.WorkerCancelled("finished-at probe")
    })
  let assert Ok(snooze_delay) = worker.retry_delay(1000)
  let assert Ok(snooze_base) =
    worker.define(
      "finished-at.snooze-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let snooze_worker =
    worker.with_queue_handler(snooze_base, fn(_) {
      worker.WorkerSnoozed(snooze_delay, "finished-at probe")
    })
  let assert Ok(retry_worker) =
    worker.define("finished-at.retry", "v1", plain_input, plain_output, fn(_) {
      Error(AccountMissing(9))
    })

  // Gated variants of the three branches that are only non-terminal in the
  // *absence* of a concurrent cancellation (`retryable`, `scheduled`,
  // `uncertain`): each blocks mid-execution on its own `started`/`release`
  // handshake so a test can request cancellation while the row is genuinely
  // `executing`, then let the worker's own proposal run into the
  // already-cancelled row — proving the bare `CASE WHEN cancel_requested_at
  // IS NOT NULL THEN clock_timestamp() END` in each of those three branches,
  // not just their ordinary (uncancelled) path already covered above.
  let gated_retry_started = process.new_subject()
  let assert Ok(gated_retry_worker) =
    worker.define(
      "finished-at.retry-gated",
      "v1",
      plain_input,
      plain_output,
      fn(value) {
        let release = process.new_subject()
        process.send(gated_retry_started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Error(AccountMissing(value))
          Error(Nil) -> Ok(int.to_string(value))
        }
      },
    )
  let gated_snooze_started = process.new_subject()
  let assert Ok(gated_snooze_base) =
    worker.define(
      "finished-at.snooze-gated-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let gated_snooze_worker =
    worker.with_queue_handler(gated_snooze_base, fn(_) {
      let release = process.new_subject()
      process.send(gated_snooze_started, LongHandlerStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) ->
          worker.WorkerSnoozed(snooze_delay, "finished-at probe")
        Error(Nil) -> worker.WorkerDiscarded("timed out")
      }
    })
  let gated_uncertain_started = process.new_subject()
  let assert Ok(gated_uncertain_base) =
    worker.define(
      "finished-at.uncertain-gated-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let gated_uncertain_worker =
    worker.with_queue_handler(gated_uncertain_base, fn(_) {
      let release = process.new_subject()
      process.send(gated_uncertain_started, LongHandlerStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> worker.WorkerUncertain("finished-at probe")
        Error(Nil) -> worker.WorkerDiscarded("timed out")
      }
    })

  let assert Ok(workers) = registry.new(run_queue)
  let assert Ok(workers) = registry.register(workers, succeeded_worker)
  let assert Ok(workers) = registry.register(workers, business_failed_worker)
  let assert Ok(workers) = registry.register(workers, discard_worker)
  let assert Ok(workers) = registry.register(workers, cancel_worker)
  let assert Ok(workers) = registry.register(workers, snooze_worker)
  let assert Ok(workers) = registry.register(workers, retry_worker)
  let assert Ok(workers) = registry.register(workers, gated_retry_worker)
  let assert Ok(workers) = registry.register(workers, gated_snooze_worker)
  let assert Ok(workers) = registry.register(workers, gated_uncertain_worker)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // -- Terminal write sites: finished_at ends up non-null -------------------

  // acknowledge_transaction, "succeeded" branch.
  let assert Ok(succeeded_handle) =
    postgres.submit(database, run_queue, succeeded_worker, 1)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, succeeded_handle) |> should.equal(Ok(job.Succeeded))
  finished_at_is_set(connection, job.id_value(succeeded_handle))
  |> should.equal(True)

  // acknowledge_transaction, "business_failed" branch (budget exhausted).
  let assert Ok(business_failed_handle) =
    postgres.submit(database, run_queue, business_failed_worker, 2)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, business_failed_handle)
  |> should.equal(Ok(job.BusinessFailed))
  finished_at_is_set(connection, job.id_value(business_failed_handle))
  |> should.equal(True)

  // acknowledge_transaction, "discarded" branch.
  let assert Ok(discarded_handle) =
    postgres.submit(database, run_queue, discard_worker, 3)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, discarded_handle) |> should.equal(Ok(job.Discarded))
  finished_at_is_set(connection, job.id_value(discarded_handle))
  |> should.equal(True)

  // acknowledge_transaction, "cancelled" branch (worker-initiated).
  let assert Ok(cancelled_handle) =
    postgres.submit(database, run_queue, cancel_worker, 4)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, cancelled_handle) |> should.equal(Ok(job.Cancelled))
  finished_at_is_set(connection, job.id_value(cancelled_handle))
  |> should.equal(True)

  // acknowledge_transaction, "retryable" branch overridden by a concurrent
  // cancellation: the bare `CASE WHEN cancel_requested_at IS NOT NULL THEN
  // clock_timestamp() END` fires because the row committed `cancelled`, not
  // its own ordinary (non-cancelled) `NULL` path already proven below.
  let assert Ok(retry_cancelled_handle) =
    postgres.submit(database, run_queue, gated_retry_worker, 14)
  drive_cancel_while_executing(
    database,
    consumer,
    retry_cancelled_handle,
    gated_retry_started,
  )
  postgres.state(database, retry_cancelled_handle)
  |> should.equal(Ok(job.Cancelled))
  finished_at_is_set(connection, job.id_value(retry_cancelled_handle))
  |> should.equal(True)

  // acknowledge_transaction, "snoozed" branch overridden by a concurrent
  // cancellation.
  let assert Ok(snooze_cancelled_handle) =
    postgres.submit(database, run_queue, gated_snooze_worker, 15)
  drive_cancel_while_executing(
    database,
    consumer,
    snooze_cancelled_handle,
    gated_snooze_started,
  )
  postgres.state(database, snooze_cancelled_handle)
  |> should.equal(Ok(job.Cancelled))
  finished_at_is_set(connection, job.id_value(snooze_cancelled_handle))
  |> should.equal(True)

  // acknowledge_transaction, "uncertain" branch overridden by a concurrent
  // cancellation.
  let assert Ok(uncertain_cancelled_handle) =
    postgres.submit(database, run_queue, gated_uncertain_worker, 16)
  drive_cancel_while_executing(
    database,
    consumer,
    uncertain_cancelled_handle,
    gated_uncertain_started,
  )
  postgres.state(database, uncertain_cancelled_handle)
  |> should.equal(Ok(job.Cancelled))
  finished_at_is_set(connection, job.id_value(uncertain_cancelled_handle))
  |> should.equal(True)

  // acknowledge_transaction, "runtime_failed" branch: input decode fails at
  // claim time even though the input_version still matches the registered
  // codec exactly, since the stored JSON itself is not a valid encoded
  // `Int`.
  let assert Ok(runtime_failed_handle) =
    postgres.submit(database, run_queue, succeeded_worker, 5)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET input = '\"not-an-int\"'::jsonb WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(runtime_failed_handle)))
    |> pog.execute(on: connection)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, runtime_failed_handle)
  |> should.equal(Ok(job.RuntimeFailed))
  finished_at_is_set(connection, job.id_value(runtime_failed_handle))
  |> should.equal(True)

  // attempt.mark_contract_mismatch: the claim itself never runs the worker.
  let assert Ok(contract_mismatch_handle) =
    postgres.submit(database, run_queue, succeeded_worker, 6)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET output_version = 'finished-at-drifted' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(contract_mismatch_handle)))
    |> pog.execute(on: connection)
  let assert Error(_) = queue.process_one(consumer)
  postgres.state(database, contract_mismatch_handle)
  |> should.equal(Ok(job.ContractMismatch))
  finished_at_is_set(connection, job.id_value(contract_mismatch_handle))
  |> should.equal(True)

  // sql.cancel_before_run: never claimed at all.
  let assert Ok(cancel_before_run_handle) =
    postgres.submit(database, "finished-at-static", succeeded_worker, 7)
  postgres.cancel(database, cancel_before_run_handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  finished_at_is_set(connection, job.id_value(cancel_before_run_handle))
  |> should.equal(True)

  // postgres.write_resolution, confirm_success target.
  let assert Ok(resolve_success_handle) =
    postgres.submit(database, "finished-at-resolve", succeeded_worker, 8)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 501, attempt_epoch = 1, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(resolve_success_handle)))
    |> pog.execute(on: connection)
  postgres.resolve_uncertain(
    database,
    resolve_success_handle,
    postgres.ResolutionRequest(
      "finished-at-resolve-success",
      "on-call",
      "finished_at probe",
      postgres.ConfirmSuccess("approved"),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  finished_at_is_set(connection, job.id_value(resolve_success_handle))
  |> should.equal(True)

  // postgres.write_resolution, confirm_business_failure target.
  let assert Ok(resolve_failure_handle) =
    postgres.submit(database, "finished-at-resolve", business_failed_worker, 9)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 502, attempt_epoch = 1, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp(), error_version = 'finished-at-error-v1' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(resolve_failure_handle)))
    |> pog.execute(on: connection)
  postgres.resolve_uncertain(
    database,
    resolve_failure_handle,
    postgres.ResolutionRequest(
      "finished-at-resolve-failure",
      "on-call",
      "finished_at probe",
      postgres.ConfirmBusinessFailure(AccountMissing(9)),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.BusinessFailed)))
  finished_at_is_set(connection, job.id_value(resolve_failure_handle))
  |> should.equal(True)

  // -- Non-terminal write sites: finished_at stays null ----------------------

  // A freshly submitted job: never touched.
  let assert Ok(queued_handle) =
    postgres.submit(database, "finished-at-static", succeeded_worker, 10)
  postgres.state(database, queued_handle) |> should.equal(Ok(job.Queued))
  finished_at_is_set(connection, job.id_value(queued_handle))
  |> should.equal(False)

  // acknowledge_transaction, "snoozed" branch (no concurrent cancellation).
  let assert Ok(scheduled_handle) =
    postgres.submit(database, run_queue, snooze_worker, 11)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, scheduled_handle) |> should.equal(Ok(job.Scheduled))
  finished_at_is_set(connection, job.id_value(scheduled_handle))
  |> should.equal(False)

  // acknowledge_transaction, "retryable" branch (no concurrent cancellation).
  let assert Ok(retryable_handle) =
    postgres.submit(database, run_queue, retry_worker, 12)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, retryable_handle) |> should.equal(Ok(job.Retryable))
  finished_at_is_set(connection, job.id_value(retryable_handle))
  |> should.equal(False)

  // A row forced into `uncertain`, before any resolution.
  let assert Ok(uncertain_handle) =
    postgres.submit(database, "finished-at-resolve", succeeded_worker, 13)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 503, attempt_epoch = 1, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(uncertain_handle)))
    |> pog.execute(on: connection)
  postgres.state(database, uncertain_handle) |> should.equal(Ok(job.Uncertain))
  finished_at_is_set(connection, job.id_value(uncertain_handle))
  |> should.equal(False)

  // postgres.write_resolution, authorize_replay target (`queued`, the one
  // non-terminal outcome a resolution can itself produce).
  postgres.resolve_uncertain(
    database,
    uncertain_handle,
    postgres.ResolutionRequest(
      "finished-at-resolve-replay",
      "on-call",
      "finished_at probe",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  finished_at_is_set(connection, job.id_value(uncertain_handle))
  |> should.equal(False)

  mark_database_test_executed("finished-at-written-at-every-terminal-path")
}

// -- Retention (postgres.prune_finished) ------------------------------------

/// Inserts one already-terminal `grind_jobs` row directly (bypassing the
/// typed acknowledgement path entirely, the same way the upgrade harness
/// above seeds legacy rows) with `finished_at` backdated by
/// `finished_ago_ms` — old enough to prune, or not, entirely under the
/// caller's control rather than depending on real wall-clock timing.
fn seed_terminal_job(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  worker_id: String,
  state: String,
  finished_ago_ms: Int,
) -> Int {
  let assert Ok(returned) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, finished_at) VALUES ($1, $2, $3, 'v1', 'v1', '1'::jsonb, 'v1', $4, clock_timestamp(), clock_timestamp() - ($5::bigint::double precision * interval '1 millisecond')) RETURNING id",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(state))
    |> pog.parameter(pog.int(finished_ago_ms))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  let assert [id] = returned.rows
  id
}

/// Inserts one non-terminal `grind_jobs` row directly, `finished_at` left at
/// its natural `NULL` (never backdated — a non-terminal row's `finished_at`
/// is never anything else, by `grind_jobs_finished_at_check`).
fn seed_nonterminal_job(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  worker_id: String,
  state: String,
) -> Int {
  let assert Ok(returned) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at) VALUES ($1, $2, $3, 'v1', 'v1', '1'::jsonb, 'v1', $4, clock_timestamp()) RETURNING id",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(state))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  let assert [id] = returned.rows
  id
}

fn seed_acknowledgement_receipt(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  job_id: Int,
  worker_id: String,
  command_id: String,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ($1, $2, $3, $4, $5, 'v1', 1, 1, 'prune-test-owner', 'succeeded', sha256(convert_to('prune-test-proposal', 'UTF8')))",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(command_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.execute(on: connection)
  Nil
}

fn seed_unique_submission_receipt(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  job_id: Int,
  worker_id: String,
  submission_id: String,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_unique_submissions (storage_owner, submission_id, queue, worker_id, worker_version, request_sha256, decision, job_id, job_queue, observed_state) VALUES ($1, $2, $3, $4, 'v1', sha256(convert_to('prune-test-request', 'UTF8')), 'inserted', $5, $3, 'succeeded')",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(submission_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  Nil
}

fn seed_resolution_receipt(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  job_id: Int,
  worker_id: String,
  resolution_id: String,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ($1, $2, $3, $4, 'v1', $5, 1, 1, 'prune-test-owner', clock_timestamp(), 'confirm_success', 'succeeded', 'prune-test', 'prune cascade probe')",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(resolution_id))
    |> pog.execute(on: connection)
  Nil
}

fn job_row_exists(connection: pog.Connection, id: Int) -> Bool {
  let assert Ok(returned) =
    pog.query("SELECT count(*) = 1 FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use exists <- decode.field(0, decode.bool)
      decode.success(exists)
    })
    |> pog.execute(on: connection)
  let assert [exists] = returned.rows
  exists
}

/// Pure validation runs before any query reaches the database at all: every
/// invalid-argument case below is checked against a pool this block closes
/// immediately after opening, the same "closed pool proves purity" pattern
/// `run_submit_unique_pre_storage_rejection_test` already uses.
pub fn postgres_prune_finished_validates_arguments_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_prune_validation_test(database_url)
  }
}

fn run_prune_validation_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let _ = postgres.close(database)

  postgres.prune_finished(database, older_than_ms: 0, limit: 100)
  |> should.equal(Error(postgres.NonPositiveRetention))
  postgres.prune_finished(database, older_than_ms: -1, limit: 100)
  |> should.equal(Error(postgres.NonPositiveRetention))
  postgres.prune_finished(
    database,
    older_than_ms: worker.retry_delay_maximum_milliseconds() + 1,
    limit: 100,
  )
  |> should.equal(Error(postgres.RetentionAbovePrecisionBound))
  postgres.prune_finished(database, older_than_ms: 1000, limit: 0)
  |> should.equal(Error(postgres.NonPositivePruneLimit))
  postgres.prune_finished(database, older_than_ms: 1000, limit: -1)
  |> should.equal(Error(postgres.NonPositivePruneLimit))
  postgres.prune_finished(
    database,
    older_than_ms: 1000,
    limit: postgres.prune_limit_maximum() + 1,
  )
  |> should.equal(Error(postgres.PruneLimitTooLarge))
  mark_database_test_executed("prune-finished-validates-arguments")
}

pub fn postgres_prune_finished_deletes_old_terminal_rows_test() {
  case prune_url(), prune_owner_b_url() {
    Ok(database_url), Ok(owner_b_url) ->
      run_prune_finished_test(database_url, owner_b_url)
    _, _ -> Nil
  }
}

fn run_prune_finished_test(database_url: String, owner_b_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let storage_owner = postgres.storage_owner(database)

  let assert Ok(owner_b_validated) =
    postgres.settings(owner_b_url) |> postgres.validate
  let assert Ok(owner_b_database) = postgres.start(owner_b_validated)
  use <- exception.defer(fn() { postgres.close(owner_b_database) })
  let assert Ok(Nil) = postgres.migrate(owner_b_database)
  let owner_b_connection = postgres.connection(owner_b_database)
  let owner_b_storage_owner = postgres.storage_owner(owner_b_database)

  // -- Every terminal state, old enough to prune, one carrying real
  // -- receipts of every kind (proving the cascade counts) -----------------
  let old_succeeded =
    seed_terminal_job(
      connection,
      storage_owner,
      "prune-old",
      "prune.worker",
      "succeeded",
      3_600_000,
    )
  seed_acknowledgement_receipt(
    connection,
    storage_owner,
    "prune-old",
    old_succeeded,
    "prune.worker",
    "prune-cascade-command",
  )
  seed_unique_submission_receipt(
    connection,
    storage_owner,
    "prune-old",
    old_succeeded,
    "prune.worker",
    "prune-cascade-submission",
  )
  seed_resolution_receipt(
    connection,
    storage_owner,
    "prune-old",
    old_succeeded,
    "prune.worker",
    "prune-cascade-resolution",
  )
  let old_business_failed =
    seed_terminal_job(
      connection,
      storage_owner,
      "prune-old",
      "prune.worker",
      "business_failed",
      3_600_000,
    )
  let old_runtime_failed =
    seed_terminal_job(
      connection,
      storage_owner,
      "prune-old",
      "prune.worker",
      "runtime_failed",
      3_600_000,
    )
  let old_contract_mismatch =
    seed_terminal_job(
      connection,
      storage_owner,
      "prune-old",
      "prune.worker",
      "contract_mismatch",
      3_600_000,
    )
  let old_discarded =
    seed_terminal_job(
      connection,
      storage_owner,
      "prune-old",
      "prune.worker",
      "discarded",
      3_600_000,
    )
  let old_cancelled =
    seed_terminal_job(
      connection,
      storage_owner,
      "prune-old",
      "prune.worker",
      "cancelled",
      3_600_000,
    )
  let old_terminal_ids = [
    old_succeeded,
    old_business_failed,
    old_runtime_failed,
    old_contract_mismatch,
    old_discarded,
    old_cancelled,
  ]

  // Every terminal state again, but not old enough (100ms, under the 1000ms
  // retention this test prunes with below).
  let young_terminal_ids =
    list.map(
      [
        "succeeded", "business_failed", "runtime_failed", "contract_mismatch",
        "discarded", "cancelled",
      ],
      fn(state) {
        seed_terminal_job(
          connection,
          storage_owner,
          "prune-young",
          "prune.worker",
          state,
          100,
        )
      },
    )

  // Every non-terminal state: `finished_at` is always null, so never
  // prunable regardless of how much wall-clock time passes.
  let nonterminal_ids =
    list.map(
      ["queued", "scheduled", "retryable", "executing", "uncertain"],
      fn(state) {
        seed_nonterminal_job(
          connection,
          storage_owner,
          "prune-nonterminal",
          "prune.worker",
          state,
        )
      },
    )

  // A different storage owner's own old, terminal row: `prune_finished` is
  // scoped to the caller's own storage owner, never cross-owner.
  let other_owner_id =
    seed_terminal_job(
      owner_b_connection,
      owner_b_storage_owner,
      "prune-old",
      "prune.worker",
      "succeeded",
      3_600_000,
    )

  let signal = process.new_subject()
  let assert Ok(handler_id) = sinal.handler_id("grind-test-prune-completed")
  let assert Ok(attachment) =
    sinal.observe(
      handler_id,
      observation.prune_completed(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(report) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  report.jobs |> should.equal(6)

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.PruneCompletedMeasurements(jobs: 6))
  metadata
  |> should.equal(observation.PruneCompletedMetadata(
    older_than_ms: 1000,
    limit: 100,
  ))

  // The six old terminal rows, and their receipts, are gone.
  list.each(old_terminal_ids, fn(id) {
    job_row_exists(connection, id) |> should.equal(False)
  })
  let assert Ok(remaining_receipts) =
    pog.query(
      "SELECT (SELECT count(*) FROM grind_job_acknowledgements WHERE job_id = $1), (SELECT count(*) FROM grind_unique_submissions WHERE job_id = $1), (SELECT count(*) FROM grind_job_resolutions WHERE job_id = $1)",
    )
    |> pog.parameter(pog.int(old_succeeded))
    |> pog.returning({
      use acknowledgements <- decode.field(0, decode.int)
      use submissions <- decode.field(1, decode.int)
      use resolutions <- decode.field(2, decode.int)
      decode.success(#(acknowledgements, submissions, resolutions))
    })
    |> pog.execute(on: connection)
  remaining_receipts.rows |> should.equal([#(0, 0, 0)])

  // Everything else survives: young terminal rows, every non-terminal
  // state, and the other storage owner's own old terminal row.
  list.each(young_terminal_ids, fn(id) {
    job_row_exists(connection, id) |> should.equal(True)
  })
  list.each(nonterminal_ids, fn(id) {
    job_row_exists(connection, id) |> should.equal(True)
  })
  job_row_exists(owner_b_connection, other_owner_id) |> should.equal(True)

  // -- Batch size and ordering: oldest `finished_at` first ------------------
  let batch_ids =
    list.map([5, 4, 3, 2, 1], fn(hours) {
      seed_terminal_job(
        connection,
        storage_owner,
        "prune-batch",
        "prune.worker",
        "succeeded",
        hours * 3_600_000,
      )
    })
  let assert Ok(first_batch) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 2)
  first_batch.jobs |> should.equal(2)
  let assert Ok(second_batch) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 2)
  second_batch.jobs |> should.equal(2)
  let assert Ok(third_batch) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 2)
  third_batch.jobs |> should.equal(1)
  let assert Ok(fourth_batch) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 2)
  fourth_batch.jobs |> should.equal(0)
  list.each(batch_ids, fn(id) {
    job_row_exists(connection, id) |> should.equal(False)
  })

  // -- FOR UPDATE SKIP LOCKED: a concurrently locked candidate is skipped,
  // -- not blocked on, and survives until the lock clears -------------------
  let locked_id =
    seed_terminal_job(
      connection,
      storage_owner,
      "prune-locked",
      "prune.worker",
      "succeeded",
      3_600_000,
    )
  let lock_query =
    pog.query("SELECT id FROM grind_jobs WHERE id = $1 FOR UPDATE")
    |> pog.parameter(pog.int(locked_id))
  let #(lock_ready, lock_finished) = spawn_lock_holder(connection, lock_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })
  let assert Ok(skipped) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  skipped.jobs |> should.equal(0)
  job_row_exists(connection, locked_id) |> should.equal(True)
  process.send(release_lock, ReleaseAttempt)
  let assert Ok(ClaimGateReleased(True)) =
    process.receive(lock_finished, within: 5000)
  let assert Ok(unlocked) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  unlocked.jobs |> should.equal(1)
  job_row_exists(connection, locked_id) |> should.equal(False)

  // -- After a job is pruned: everything about it reports "not found",
  // -- never a different, misleading error -----------------------------------
  let assert Ok(gone_worker) =
    worker.codec("prune-gone-input-v1", json.int, decode.int)
  let assert Ok(gone_output) =
    worker.codec("prune-gone-output-v1", json.string, decode.string)
  let assert Ok(gone_worker_def) =
    worker.define(
      "prune.gone-worker",
      "v1",
      gone_worker,
      gone_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let gone_handle =
    job.new_handle(old_succeeded, storage_owner, "prune-old", gone_worker_def)
  postgres.state(database, gone_handle)
  |> should.equal(Error(postgres.JobNotFound))
  postgres.outcome(database, gone_handle)
  |> should.equal(Error(postgres.JobNotFound))
  postgres.bind_handle(database, gone_worker_def, old_succeeded)
  |> should.equal(Error(postgres.JobNotFound))
  postgres.reconcile_acknowledgement(
    database,
    gone_handle,
    "prune-cascade-command",
  )
  |> should.equal(Error(postgres.ReceiptNotFound))

  // -- submit_with_id: idempotency only holds within the retention window --
  let assert Ok(replay_submission) = submission.submission_id("prune-replay")
  let assert Ok(replay_worker) =
    worker.codec("prune-replay-input-v1", json.int, decode.int)
  let assert Ok(replay_output) =
    worker.codec("prune-replay-output-v1", json.string, decode.string)
  let assert Ok(replay_worker_def) =
    worker.define(
      "prune.replay-worker",
      "v1",
      replay_worker,
      replay_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(submission.Inserted(first_replay_handle)) =
    postgres.submit_with_id(
      database,
      "prune-replay",
      replay_submission,
      replay_worker_def,
      41,
      submission.Immediately,
    )
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'succeeded', finished_at = clock_timestamp() - interval '1 hour' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(first_replay_handle)))
    |> pog.execute(on: connection)
  let assert Ok(replay_prune) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  replay_prune.jobs |> should.equal(1)
  let assert Ok(submission.Inserted(second_replay_handle)) =
    postgres.submit_with_id(
      database,
      "prune-replay",
      replay_submission,
      replay_worker_def,
      41,
      submission.Immediately,
    )
  { job.id_value(second_replay_handle) != job.id_value(first_replay_handle) }
  |> should.equal(True)

  // -- Uniqueness: an `AllRetained`/`while_retained()` key is only occupied
  // -- "until pruned", not forever -------------------------------------------
  let assert Ok(reopen_worker) =
    worker.define(
      "prune.reopen-worker",
      "v1",
      replay_worker,
      replay_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let reopen_policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      unique.while_retained(),
      unique.AllRetained,
    )
  let assert Ok(submission.Inserted(reopen_first_handle)) =
    submit_keep_existing(
      database,
      "prune-reopen",
      "prune-reopen-first",
      reopen_worker,
      99,
      reopen_policy,
    )
  let assert Ok(submission.Existing(_)) =
    submit_keep_existing(
      database,
      "prune-reopen",
      "prune-reopen-second",
      reopen_worker,
      99,
      reopen_policy,
    )
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'succeeded', finished_at = clock_timestamp() - interval '1 hour' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(reopen_first_handle)))
    |> pog.execute(on: connection)
  let assert Ok(reopen_prune) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  reopen_prune.jobs |> should.equal(1)
  let assert Ok(submission.Inserted(reopen_third_handle)) =
    submit_keep_existing(
      database,
      "prune-reopen",
      "prune-reopen-third",
      reopen_worker,
      99,
      reopen_policy,
    )
  { job.id_value(reopen_third_handle) != job.id_value(reopen_first_handle) }
  |> should.equal(True)

  // `young_terminal_ids` were deliberately left behind (proving they
  // survive *this* test's own 1000ms retention window) — but `prune_url()`
  // is a database every test in this section shares, and a terminal row
  // seeded "100ms old" keeps aging in real wall-clock time long after this
  // test itself returns. Removed directly (not via `prune_finished`, which
  // would also sweep up anything else already old enough by now) so a
  // later test's own broad `limit` can never mistake it for a row that
  // test itself is supposed to control.
  let assert Ok(_) =
    pog.query(
      "DELETE FROM grind_jobs WHERE storage_owner = $1 AND queue = 'prune-young'",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.execute(on: connection)

  mark_database_test_executed("prune-finished-deletes-old-terminal-rows")
}

/// Installs a `BEFORE INSERT` trigger on `grind_unique_submissions`, scoped
/// to one exact `submission_id`, that blocks on `pg_advisory_xact_lock`
/// before letting that one receipt insert proceed — the same shape
/// `install_unique_insert_barrier` uses against `grind_jobs`, aimed instead
/// at the point in `submit_unique`'s own admission transaction that comes
/// strictly *after* its candidate `SELECT ... FOR KEY SHARE`/`FOR UPDATE`
/// has already run and decided `KeepExisting`, so a concurrent
/// `prune_finished` racing the same candidate row is forced to land in
/// exactly that window.
fn install_admission_receipt_barrier(
  connection: pog.Connection,
  name: String,
  submission_id: String,
  lock_key: Int,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.submission_id = '"
      <> submission_id
      <> "' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> name
      <> " BEFORE INSERT ON grind_unique_submissions FOR EACH ROW EXECUTE FUNCTION "
      <> name
      <> "()",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS " <> name <> " ON grind_unique_submissions",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
}

/// Polls (bounded) until some other backend is genuinely blocked acquiring
/// the barrier's own advisory lock from inside its `INSERT INTO
/// grind_unique_submissions` — proof the admission transaction's own
/// candidate `SELECT` has already run (and, with the fix in place, already
/// holds its `FOR KEY SHARE` lock) and is now paused strictly before its
/// receipt commits, rather than inferring this from timing alone.
fn await_admission_blocked_on_receipt_insert(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Bool {
  let waiting =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND query LIKE 'INSERT INTO grind_unique_submissions%')",
    )
    |> pog.returning({
      use waiting <- decode.field(0, decode.bool)
      decode.success(waiting)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [waiting] -> Ok(waiting)
        _ -> Error(Nil)
      }
    })
  case waiting {
    Ok(True) -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_admission_blocked_on_receipt_insert(
            connection,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

/// The race `candidate_sql`'s `FOR KEY SHARE` (non-reschedule candidates)
/// exists to close: a `submit_unique` admission deciding `KeepExisting`
/// against an old, terminal candidate, forced to overlap with a concurrent
/// `prune_finished` call racing the exact same row. The barrier above pins
/// admission strictly after its own candidate lock is taken (with the fix)
/// and before its own receipt commits, which is exactly the window
/// `prune_finished` is called from — proving `FOR UPDATE SKIP LOCKED` skips
/// this row (rather than deleting out from under a still-open admission)
/// and the row and its receipt both survive together. See
/// `docs/RECOVERY-EVIDENCE.md` for the mutation (`FOR KEY SHARE` removed)
/// that reproduces the opposite: the row deleted while admission's own
/// still-open transaction goes on to commit a receipt that names it,
/// leaving `grind_unique_submissions` an orphaned row pointing at nothing.
pub fn postgres_prune_finished_admission_race_keeps_candidate_and_receipt_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_prune_admission_race_test(database_url)
  }
}

fn run_prune_admission_race_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  let worker_def = unique_test_worker("prune.race-worker")
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.AllRetained,
    )
  let assert Ok(submission.Inserted(original_handle)) =
    submit_keep_existing(
      database,
      "prune-race",
      "prune-race-original",
      worker_def,
      7,
      policy,
    )
  let original_id = job.id_value(original_handle)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'succeeded', finished_at = clock_timestamp() - interval '1 hour' WHERE id = $1",
    )
    |> pog.parameter(pog.int(original_id))
    |> pog.execute(on: connection)

  let lock_key = unique_test_lock_key(100)
  let retry_submission_id = "prune-race-retry"
  let cleanup =
    install_admission_receipt_barrier(
      connection,
      "grind_test_prune_race_barrier",
      retry_submission_id,
      lock_key,
    )
  use <- exception.defer(cleanup)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      connection,
      pog.query(
        "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_prune_race_lock",
      )
        |> pog.parameter(pog.int(lock_key)),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let admission_result = process.new_subject()
  spawn_submit(admission_result, fn() {
    submit_keep_existing(
      database,
      "prune-race",
      retry_submission_id,
      worker_def,
      7,
      policy,
    )
  })

  await_admission_blocked_on_receipt_insert(connection, 250)
  |> should.equal(True)

  // Admission's own candidate lock is already held (with the fix); the
  // concurrent prune below must skip this exact row rather than delete it.
  let assert Ok(race_prune) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)
  race_prune.jobs |> should.equal(0)
  job_row_exists(connection, original_id) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  let assert Ok(ClaimGateReleased(True)) =
    process.receive(lock_finished, within: 5000)
  let assert Ok(Ok(submission.Existing(conflict))) =
    process.receive(admission_result, within: 5000)
  submission.conflict_job_id(conflict) |> should.equal(original_id)

  // The candidate and its own admission receipt both survived together —
  // no orphan.
  job_row_exists(connection, original_id) |> should.equal(True)
  let assert Ok(receipt_target) =
    pog.query(
      "SELECT job_id FROM grind_unique_submissions WHERE submission_id = $1",
    )
    |> pog.parameter(pog.text(retry_submission_id))
    |> pog.returning({
      use job_id <- decode.field(0, decode.int)
      decode.success(job_id)
    })
    |> pog.execute(on: connection)
  receipt_target.rows |> should.equal([original_id])

  // `original_id` deliberately survived this test (that was the point) —
  // but it is now a terminal, hour-old row left behind in a database this
  // whole section shares. Pruned directly, via the real public API, so a
  // later test's own tightly `LIMIT`ed prune call can never mistake it for
  // that test's own intended candidate.
  let assert Ok(_) =
    postgres.prune_finished(database, older_than_ms: 1000, limit: 100)

  mark_database_test_executed("prune-finished-admission-race-no-orphan")
}

/// Installs a PL/pgSQL function called from inside a `WHERE` clause, one
/// candidate row at a time, that blocks on `pg_advisory_xact_lock` only
/// when it is evaluating `target_id` — used to pause a prune-shaped query
/// mid-scan, after PostgreSQL has already fixed that statement's own
/// snapshot (`READ COMMITTED` takes one snapshot per statement, not per
/// row), but before it reaches and locks one specific candidate. Returns a
/// cleanup thunk for `exception.defer`.
fn install_snapshot_barrier(
  connection: pog.Connection,
  name: String,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "(target_id bigint, candidate_id bigint, lock_key bigint) RETURNS boolean LANGUAGE plpgsql AS $body$ BEGIN IF candidate_id = target_id THEN PERFORM pg_advisory_xact_lock(lock_key); END IF; RETURN true; END $body$",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ =
      pog.query(
        "DROP FUNCTION IF EXISTS " <> name <> "(bigint, bigint, bigint)",
      )
      |> pog.execute(on: connection)
    Nil
  }
}

/// The exact shape `sql/prune_finished.sql` itself selects candidates with,
/// plus one extra `AND` clause calling the snapshot barrier above for one
/// exact row — proving the underlying mechanism (`ON DELETE CASCADE`) reads
/// its own fresh state when a job is deleted, not the deleting statement's
/// own snapshot, since `postgres.prune_finished`'s real, fixed SQL text has
/// no injection point of its own to pause mid-scan from a test.
fn snapshot_barrier_prune_sql(barrier_name: String) -> String {
  "WITH doomed AS (SELECT id FROM grind_jobs WHERE storage_owner = $1 AND finished_at IS NOT NULL AND state IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled') AND finished_at < statement_timestamp() - ($2::bigint::double precision * interval '1 millisecond') AND "
  <> barrier_name
  <> "($4, id, $5) ORDER BY finished_at, id LIMIT $3 FOR UPDATE SKIP LOCKED) DELETE FROM grind_jobs x USING doomed d WHERE x.id = d.id RETURNING x.id"
}

/// Polls (bounded) until some other backend is genuinely blocked acquiring
/// the snapshot barrier's own advisory lock from inside the `WITH doomed
/// AS (...)` query above, the same `pg_stat_activity` discipline
/// `await_admission_blocked_on_receipt_insert` uses.
fn await_prune_blocked_on_snapshot_barrier(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Bool {
  let waiting =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND query LIKE 'WITH doomed AS (%')",
    )
    |> pog.returning({
      use waiting <- decode.field(0, decode.bool)
      decode.success(waiting)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [waiting] -> Ok(waiting)
        _ -> Error(Nil)
      }
    })
  case waiting {
    Ok(True) -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_prune_blocked_on_snapshot_barrier(
            connection,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

/// The race `grind_v12`'s `ON DELETE CASCADE` foreign keys close: under
/// `READ COMMITTED`, every CTE of one `prune_finished`-shaped statement
/// shares that one statement's own start-of-statement snapshot. A receipt
/// committed by some other writer *after* that snapshot was taken, but
/// *before* the scan actually reaches and locks the row it names, is
/// invisible to that snapshot — an explicit, snapshot-scoped `DELETE ...
/// WHERE job_id = d.id` against the receipt table (the shape this module
/// used before this fix) could never see or delete it, leaving it an
/// orphan once the job itself is deleted. `ON DELETE CASCADE` fires its own
/// fresh query when the row is actually deleted, immune to the deleting
/// statement's own snapshot, so it still finds and removes a receipt
/// committed in exactly that window.
pub fn postgres_prune_finished_cascade_survives_late_committed_receipt_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_prune_cascade_snapshot_race_test(database_url)
  }
}

fn run_prune_cascade_snapshot_race_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let storage_owner = postgres.storage_owner(database)

  let target_id =
    seed_terminal_job(
      connection,
      storage_owner,
      "prune-snapshot-race",
      "prune.worker",
      "succeeded",
      3_600_000,
    )

  let barrier_name = "grind_test_prune_snapshot_barrier"
  let cleanup = install_snapshot_barrier(connection, barrier_name)
  use <- exception.defer(cleanup)
  let lock_key = unique_test_lock_key(200)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      connection,
      pog.query(
        "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_prune_snapshot_lock",
      )
        |> pog.parameter(pog.int(lock_key)),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let prune_result = process.new_subject()
  spawn_submit(prune_result, fn() {
    pog.query(snapshot_barrier_prune_sql(barrier_name))
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.int(1000))
    // `LIMIT 1`, not a generous batch size: `prune_url()` is a database
    // this whole test module shares, and another test's own deliberately
    // "too young to prune" row can have aged well past 1000ms by the time
    // this one runs. `target_id` is seeded a full hour old
    // (`ORDER BY finished_at, id` sorts it first regardless), so `LIMIT 1`
    // selects only it, never a leftover row that merely aged into
    // eligibility in the meantime.
    |> pog.parameter(pog.int(1))
    |> pog.parameter(pog.int(target_id))
    |> pog.parameter(pog.int(lock_key))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  })

  await_prune_blocked_on_snapshot_barrier(connection, 250)
  |> should.equal(True)

  // Committed strictly after the blocked prune statement's own snapshot,
  // strictly before it reaches and locks `target_id`.
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ($1, 'prune-snapshot-race-command', 'prune-snapshot-race', $2, 'prune.worker', 'v1', 1, 1, 'prune-test-owner', 'succeeded', sha256(convert_to('prune-snapshot-race-proposal', 'UTF8')))",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.int(target_id))
    |> pog.execute(on: connection)

  process.send(release_lock, ReleaseAttempt)
  let assert Ok(ClaimGateReleased(True)) =
    process.receive(lock_finished, within: 5000)
  let assert Ok(Ok(pruned)) = process.receive(prune_result, within: 5000)
  pruned.rows |> should.equal([target_id])

  job_row_exists(connection, target_id) |> should.equal(False)
  let assert Ok(orphan_check) =
    pog.query(
      "SELECT count(*) = 0 FROM grind_job_acknowledgements WHERE command_id = 'prune-snapshot-race-command'",
    )
    |> pog.returning({
      use no_orphan <- decode.field(0, decode.bool)
      decode.success(no_orphan)
    })
    |> pog.execute(on: connection)
  orphan_check.rows |> should.equal([True])

  mark_database_test_executed("prune-finished-cascade-survives-snapshot-race")
}

// -- grind/pruner (the supervised background pruner) -------------------

pub fn pruner_validate_policy_rejects_out_of_range_values_test() {
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 0,
    limit: 10_000,
    max_age_ms: 60_000,
  ))
  |> should.equal(Error(pruner.NonPositiveInterval))
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 30_000,
    limit: 0,
    max_age_ms: 60_000,
  ))
  |> should.equal(Error(pruner.NonPositivePruneLimit))
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 30_000,
    limit: postgres.prune_limit_maximum() + 1,
    max_age_ms: 60_000,
  ))
  |> should.equal(Error(pruner.PruneLimitTooLarge))
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 30_000,
    limit: 10_000,
    max_age_ms: 0,
  ))
  |> should.equal(Error(pruner.NonPositiveMaxAge))
  pruner.validate_policy(pruner.PrunerPolicy(
    interval_ms: 30_000,
    limit: 10_000,
    max_age_ms: worker.retry_delay_maximum_milliseconds() + 1,
  ))
  |> should.equal(Error(pruner.MaxAgeAbovePrecisionBound))
  pruner.validate_policy(pruner.default_policy()) |> should.be_ok()
}

/// Polls (bounded) until `id` no longer has a `grind_jobs` row, or gives up
/// and reports whatever `job_row_exists` last saw — used to observe the
/// supervised pruner's own timer doing this without the test itself calling
/// `prune_finished`.
fn await_job_pruned(
  connection: pog.Connection,
  id: Int,
  checks_remaining: Int,
) -> Bool {
  case job_row_exists(connection, id) {
    False -> True
    True ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_job_pruned(connection, id, checks_remaining - 1)
        }
        // Retries exhausted and the row still exists: this must report
        // "not pruned", not "pruned" — the opposite is a false pass that
        // would never actually observe the timer doing anything.
        False -> False
      }
  }
}

pub fn postgres_supervised_pruner_ticks_and_prunes_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_supervised_pruner_test(database_url)
  }
}

fn run_supervised_pruner_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let storage_owner = postgres.storage_owner(database)

  let old_id =
    seed_terminal_job(
      connection,
      storage_owner,
      "pruner-old",
      "pruner.worker",
      "succeeded",
      5000,
    )
  // Seeded effectively "now": with a 2000ms `max_age_ms`, this row cannot
  // cross that threshold before the test has already finished observing
  // `old_id` disappear (typically within the first one or two 50ms ticks),
  // giving a wide, non-flaky margin rather than a tight race between the
  // two rows' own ages.
  let young_id =
    seed_terminal_job(
      connection,
      storage_owner,
      "pruner-young",
      "pruner.worker",
      "succeeded",
      0,
    )

  let signal = process.new_subject()
  let assert Ok(handler_id) =
    sinal.handler_id("grind-test-supervised-pruner-completed")
  let assert Ok(attachment) =
    sinal.observe(
      handler_id,
      observation.prune_completed(),
      fn(measurements, _metadata) { process.send(signal, measurements) },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(policy) =
    pruner.default_policy()
    |> pruner.with_interval(50)
    |> pruner.with_limit(10)
    |> pruner.with_max_age(2000)
    |> pruner.validate_policy()
  let assert Ok(running) = pruner.start(database, policy)
  use <- exception.defer(fn() { pruner.stop(running) |> should.equal(Ok(Nil)) })

  // The old row (5000ms old, over the 2000ms max_age) is pruned by the
  // supervised timer itself — this test never calls `prune_finished`.
  await_job_pruned(connection, old_id, 250) |> should.equal(True)
  // The young row (0ms old) never crosses `max_age_ms`, so it survives
  // every tick this test observes.
  job_row_exists(connection, young_id) |> should.equal(True)

  // The timer path itself emits `[grind, prune, completed]`, not only a
  // direct `prune_finished` call — the tick that pruned `old_id` must have
  // reported at least one deleted job.
  let assert Ok(observation.PruneCompletedMeasurements(jobs:)) =
    process.receive(signal, within: 5000)
  { jobs >= 1 } |> should.equal(True)

  mark_database_test_executed("supervised-pruner-ticks-and-prunes")
}

/// Drains `signal` until `deadline_ms` (`monotonic_ms()`), counting however
/// many messages arrive — used to prove a bounded window's own tick count
/// exactly, rather than inferring it from a single `process.receive`.
fn count_events_until(signal: process.Subject(a), deadline_ms: Int) -> Int {
  let remaining = deadline_ms - monotonic_ms()
  case remaining > 0 {
    False -> 0
    True ->
      case process.receive(signal, within: remaining) {
        Ok(_) -> 1 + count_events_until(signal, deadline_ms)
        Error(Nil) -> 0
      }
  }
}

fn await_new_pruner_pid(
  running: pruner.Pruner,
  previous: process.Pid,
  checks_remaining: Int,
) -> Result(process.Pid, Nil) {
  case pruner.actor_pid(running) {
    Ok(pid) if pid != previous -> Ok(pid)
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(10)
          await_new_pruner_pid(running, previous, checks_remaining - 1)
        }
        False -> Error(Nil)
      }
  }
}

/// The race a self-scheduled `Tick` timer targeting the *named* subject
/// (rather than a fresh, per-incarnation one) would create: `process
/// .send_after` against a named subject resolves to whichever process holds
/// that name at *delivery* time, not at scheduling time, so a timer an old,
/// killed incarnation scheduled for itself would still fire and land on
/// whatever later incarnation is running when it does — one genuine
/// post-restart tick plus one leaked one, arriving close together (a
/// restart is on the order of a few ms; both timers were scheduled to fire
/// roughly `interval_ms` after nearly the same moment). Proven by counting
/// `[grind, prune, completed]` events in a bounded window after a real,
/// killed-and-restarted incarnation: exactly one.
pub fn postgres_supervised_pruner_restart_ticks_exactly_once_test() {
  case prune_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_supervised_pruner_restart_test(database_url)
  }
}

fn run_supervised_pruner_restart_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let signal = process.new_subject()
  let assert Ok(handler_id) =
    sinal.handler_id("grind-test-supervised-pruner-restart")
  let assert Ok(attachment) =
    sinal.observe(
      handler_id,
      observation.prune_completed(),
      fn(_measurements, _metadata) { process.send(signal, Nil) },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let interval_ms = 200
  let assert Ok(policy) =
    pruner.default_policy()
    |> pruner.with_interval(interval_ms)
    |> pruner.validate_policy()
  let assert Ok(running) = pruner.start(database, policy)
  use <- exception.defer(fn() { pruner.stop(running) |> should.equal(Ok(Nil)) })

  // The first tick, and the moment right after it a fresh timer was
  // scheduled — killing the actor here maximises the window a leaked old
  // timer (if the bug were present) would still be pending in.
  let assert Ok(Nil) = process.receive(signal, within: 5000)
  let assert Ok(first_pid) = pruner.actor_pid(running)
  process.kill(first_pid)
  let assert Ok(_second_pid) = await_new_pruner_pid(running, first_pid, 500)

  // A window comfortably covering exactly one legitimate post-restart tick
  // (interval_ms out from *either* the kill or the restart, they are only
  // ever a few ms apart) without also reaching the next legitimate one.
  let deadline_ms = monotonic_ms() + interval_ms + interval_ms / 2
  count_events_until(signal, deadline_ms) |> should.equal(1)

  mark_database_test_executed("supervised-pruner-restart-ticks-once")
}

pub fn postgres_queue_commits_typed_worker_success_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_postgres_queue_success_test(database_url)
  }
}

pub fn postgres_queue_rejects_output_codec_drift_before_invocation_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_output_codec_mismatch_test(database_url)
  }
}

pub fn postgres_queue_persists_typed_business_failure_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_business_failure_test(database_url)
  }
}

pub fn postgres_worker_discard_has_distinct_committed_outcome_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_discard_outcome_test(database_url)
  }
}

pub fn postgres_worker_cancel_has_distinct_committed_outcome_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_cancel_outcome_test(database_url)
  }
}

pub fn postgres_worker_uncertainty_is_reconcilable_without_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_uncertainty_test(database_url)
  }
}

pub fn postgres_cancel_queued_job_before_execution_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancel_before_execution_test(database_url)
  }
}

pub fn postgres_cancel_after_completion_preserves_result_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancel_after_completion_test(database_url)
  }
}

pub fn postgres_cancel_running_worker_overrides_proposal_on_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancel_running_ack_test(database_url)
  }
}

pub fn postgres_cancelled_expired_attempt_is_quarantined_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancelled_expired_attempt_test(database_url)
  }
}

pub fn postgres_cancel_running_uncertain_proposal_is_preserved_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancel_running_uncertain_test(database_url)
  }
}

pub fn postgres_worker_snooze_commits_scheduled_state_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_snooze_test(database_url)
  }
}

pub fn postgres_automatic_queue_skips_incompatible_job_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_queue_fairness_test(database_url)
  }
}

pub fn postgres_queue_policy_limits_jobs_per_tick_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_queue_batch_policy_test(database_url)
  }
}

pub fn postgres_scheduled_jobs_observe_database_due_time_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_scheduled_due_time_test(database_url)
  }
}

pub fn postgres_automatic_consumer_wakes_for_database_deadline_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_wakeup_test(database_url)
  }
}

pub fn postgres_queue_renews_running_attempt_before_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_lease_renewal_test(database_url)
  }
}

pub fn postgres_expired_renewal_keeps_worker_fenced_and_returns_proposal_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_lease_renewal_loss_test(database_url)
  }
}

pub fn postgres_renewal_storage_error_is_unknown_then_retried_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_renewal_storage_error_test(database_url)
  }
}

pub fn postgres_closed_pool_renewal_recovers_without_rerun_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_closed_pool_renewal_test(database_url)
  }
}

/// `call_safely` is the generic sibling of `execute_safely`: Squirrel-generated
/// query functions call `pog.execute` directly rather than going through
/// `execute_safely`, so `call_safely` wraps that call instead. `call_safely`
/// is a private wrapper around `grind_postgres_ffi:guarded` (never itself
/// exported for a test to redeclare and call directly — see
/// `src/grind_postgres_ffi.erl`'s own module documentation), so this proves
/// it through the public `postgres.arguments`, one of its own callers,
/// against a closed pool — the same "pool genuinely gone" `exit` shape
/// `postgres_lease_renewal_survives_closed_pool_test` already proves through
/// `postgres.state`/`execute_safely`.
pub fn postgres_call_safely_wrapper_reports_closed_pool_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_call_safely_closed_pool_test(database_url)
  }
}

fn run_call_safely_closed_pool_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let _ = postgres.close(database)
  let handle =
    job.new_handle(
      1,
      postgres.storage_owner(database),
      "call-safely-probe",
      unique_test_worker("call-safely-probe"),
    )
  postgres.arguments(database, handle)
  |> should.equal(Error(postgres.JobReadQueryFailed(pog.ConnectionUnavailable)))
  mark_database_test_executed("call-safely-wrapper-closed-pool-passed")
}

pub fn postgres_close_stale_handle_does_not_erase_live_pool_deadline_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_close_stale_handle_preserves_deadline_test(database_url)
  }
}

/// Regression test: `close` on a *stale* `Database` handle (one whose own
/// supervisor has already stopped) must never erase the checkout deadline
/// of a different, currently live pool that has since reused the same
/// registered name. `validate` gives every `start`/`close` cycle of the
/// same `ValidatedSettings` the identical pool name (see `validate`'s own
/// doc comment), so a stray double-`close` on an old handle previously
/// erased the live pool's deadline entry unconditionally, silently
/// downgrading every later storage call on that live pool to the FFI's own
/// hardcoded 5000ms fallback instead of the smaller, distinctive value
/// configured here. Named mutation: reverting `postgres.close` to
/// unconditionally call `store.clear_deadline` (the pre-fix behavior) makes
/// the probe query below succeed instead of erroring, since 4200ms clears
/// the distinctive 3200ms deadline but not the 5000ms fallback.
fn run_close_stale_handle_preserves_deadline_test(database_url: String) -> Nil {
  let distinctive_deadline_ms = 3200
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_statement_deadline(distinctive_deadline_ms)
    |> postgres.validate
  let assert Ok(first) = postgres.start(validated)
  let assert Ok(Nil) = postgres.close(first)
  let assert Ok(second) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(second) })

  // The stale handle's own supervisor already stopped above; closing it
  // again must be a no-op with respect to `second`'s own deadline.
  let assert Ok(Nil) = postgres.close(first)

  let connection = postgres.connection(second)
  let result =
    store.call_safely(connection, fn(conn) {
      // `pg_types` cannot decode a bare `void` result (`pg_sleep`'s own
      // return type), so the sleep is wrapped in an outer scalar `SELECT`
      // — see `grind/internal/unique_admission`'s identical pattern.
      // 4.2s clears `distinctive_deadline_ms` (3200ms) but is comfortably
      // under the FFI's own hardcoded 5000ms fallback.
      pog.query(
        "SELECT true FROM (SELECT pg_sleep(4.2)) AS grind_close_stale_deadline_probe",
      )
      |> pog.execute(on: conn)
    })
  case result {
    Error(_) -> Nil
    Ok(_) ->
      panic as "expected the connection to be force-closed around the configured 3200ms statement deadline, not the FFI's own 5000ms fallback"
  }
  mark_database_test_executed("close-stale-handle-preserves-deadline-passed")
}

pub fn postgres_known_worker_start_failure_releases_unstarted_claim_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_worker_start_failure_test(database_url)
  }
}

pub fn postgres_temporary_worker_death_quarantines_without_replay_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_temporary_worker_death_test(database_url)
  }
}

pub fn postgres_independent_consumers_compete_for_one_live_claim_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_independent_consumer_claim_test(database_url)
  }
}

pub fn postgres_overlapping_claim_transactions_respect_skip_locked_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_overlapping_claim_test(database_url)
  }
}

pub fn postgres_dead_idle_worker_is_released_before_activation_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_dead_idle_worker_test(database_url)
  }
}

fn run_dead_idle_worker_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("dead-idle-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("dead-idle-output-v1", json.string, decode.string)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("dead.idle", "v1", input_codec, output_codec, fn(value) {
      process.send(invoked, WorkerInvoked)
      Ok("activated-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("dead-idle")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) = postgres.submit(database, "dead-idle", definition, 17)
  let kill_next_worker = one_shot.armed()
  let hooks =
    consumer_hooks.Hooks(
      before_worker_start: fn() { Ok(Nil) },
      after_worker_start: fn(pid) {
        case one_shot.take(kill_next_worker) {
          True -> {
            process.kill(pid)
            queue.wait_for_worker_exit(pid, 1000)
          }
          False -> Nil
        }
      },
    )
  let assert Ok(consumer) =
    queue.start_with_hooks(database, workers, manual_policy(), hooks)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Error(queue.QueueWorkerExitedBeforeActivation))
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))
  mark_database_test_executed("dead-idle-worker-claim-released")
}

pub fn postgres_consumer_enforces_configured_capacity_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_capacity_test(database_url)
  }
}

pub fn postgres_automatic_consumer_fills_only_available_slots_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_consumer_capacity_test(database_url)
  }
}

fn run_consumer_capacity_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("capacity-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("capacity-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("capacity.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CapacityWorkerStarted(value, release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("capacity-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("consumer-capacity")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "consumer-capacity", definition, 1)
  let assert Ok(second_handle) =
    postgres.submit(database, "consumer-capacity", definition, 2)
  let assert Ok(third_handle) =
    postgres.submit(database, "consumer-capacity", definition, 3)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(2)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let first_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(first_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, first_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(first_release, ReleaseAttempt)
    Nil
  })
  let second_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(second_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, second_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(second_release, ReleaseAttempt)
    Nil
  })
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  process.receive(started, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Queued))

  process.send(first_release, ReleaseAttempt)
  process.send(second_release, ReleaseAttempt)
  process.receive(first_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.receive(second_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, first_handle) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, second_handle) |> should.equal(Ok(job.Succeeded))
  let third_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(third_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, third_release)) =
    process.receive(started, within: 5000)
  process.send(third_release, ReleaseAttempt)
  process.receive(third_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("consumer-capacity-two-enforced")
}

fn run_automatic_consumer_capacity_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-capacity-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-capacity-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("auto.capacity", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CapacityWorkerStarted(value, release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("automatic-capacity-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("consumer-capacity-auto")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 11)
  let assert Ok(second_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 12)
  let assert Ok(third_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 13)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(60_000)
    |> queue.with_maximum_jobs_per_poll(3)
    |> queue.with_maximum_concurrency(2)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let assert Ok(CapacityWorkerStarted(11, first_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(first_release, ReleaseAttempt)
    Nil
  })
  let assert Ok(CapacityWorkerStarted(12, second_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(second_release, ReleaseAttempt)
    Nil
  })
  process.receive(started, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Queued))

  // Completing one child releases one local slot, which the same automatic
  // poll batch fills while the other child remains blocked.
  process.send(first_release, ReleaseAttempt)
  let assert Ok(CapacityWorkerStarted(13, third_release)) =
    process.receive(started, within: 5000)
  process.send(second_release, ReleaseAttempt)
  process.send(third_release, ReleaseAttempt)
  wait_for_job_state(database, first_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait_for_job_state(database, second_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait_for_job_state(database, third_handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed("automatic-consumer-capacity-two-enforced")
}

/// `maximum_concurrency: 2` with one long-running job must not leave the
/// other slot idle: a job submitted only after the first has already
/// claimed and started must still be picked up promptly, by a freshly
/// scheduled poll, rather than waiting for the first job to finish. Before
/// the fix, `continue_if_idle` only ever armed the next poll timer once
/// `active` was fully empty, so free capacity went unused for as long as any
/// one attempt kept running.
pub fn postgres_automatic_consumer_polls_while_capacity_free_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_automatic_consumer_polls_while_capacity_free_test(database_url)
  }
}

fn run_automatic_consumer_polls_while_capacity_free_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-free-capacity-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-free-capacity-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "auto.free-capacity",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, CapacityWorkerStarted(value, release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("free-capacity-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("consumer-free-capacity-auto")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "consumer-free-capacity-auto", definition, 21)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(50)
    |> queue.with_maximum_jobs_per_poll(1)
    |> queue.with_maximum_concurrency(2)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  // Job 1 claims the first slot and blocks on its own gate. The poll that
  // claimed it then finds nothing else due and drains its batch, which is
  // exactly the state the fix must keep polling from.
  let assert Ok(CapacityWorkerStarted(21, first_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(first_release, ReleaseAttempt)
    Nil
  })

  // Submitted only now: this job did not exist at the poll that claimed job
  // 1, so only a later, freshly scheduled poll can ever find it due.
  let assert Ok(second_handle) =
    postgres.submit(database, "consumer-free-capacity-auto", definition, 22)

  // The second slot is free and job 1 is still blocked; job 2 must start
  // within a handful of poll intervals, not only once job 1 finishes.
  let assert Ok(CapacityWorkerStarted(22, second_release)) =
    process.receive(started, within: 2000)
  postgres.state(database, first_handle) |> should.equal(Ok(job.Executing))

  process.send(first_release, ReleaseAttempt)
  process.send(second_release, ReleaseAttempt)
  wait_for_job_state(database, first_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait_for_job_state(database, second_handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed(
    "automatic-consumer-polls-while-capacity-free-passed",
  )
}

fn wait_for_job_state(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  expected: job.State,
  remaining_checks: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(state) ->
      case state == expected, remaining_checks > 0 {
        True, _ -> True
        False, True -> {
          process.sleep(20)
          wait_for_job_state(database, handle, expected, remaining_checks - 1)
        }
        False, False -> False
      }
    Error(_) -> False
  }
}

/// Like `wait_for_job_state` above, but a transient `postgres.state` error
/// counts as "not yet" and keeps retrying instead of failing the wait
/// outright. Used where the test itself just closed or killed a connection
/// on this same pool moments earlier, so an immediate read can genuinely
/// error while the pool recovers — that is not evidence the state will never
/// reach `expected`, only that this one read failed.
fn wait_for_job_state_tolerating_errors(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  expected: job.State,
  remaining_checks: Int,
) -> Bool {
  let matches = case postgres.state(database, handle) {
    Ok(state) -> state == expected
    Error(_) -> False
  }
  case matches, remaining_checks > 0 {
    True, _ -> True
    False, True -> {
      process.sleep(20)
      wait_for_job_state_tolerating_errors(
        database,
        handle,
        expected,
        remaining_checks - 1,
      )
    }
    False, False -> False
  }
}

/// Retries a query on any `Error`, tolerating the same kind of transient
/// pool-recovery failure `wait_for_job_state_tolerating_errors` above
/// tolerates for a state read — for a plain one-shot read (`arguments`,
/// `outcome`) taken moments after this test's own connection kill, where
/// there is no polling loop already absorbing that latency.
fn retry_transient_query(
  attempt: fn() -> Result(a, b),
  remaining: Int,
) -> Result(a, b) {
  case attempt() {
    Ok(value) -> Ok(value)
    Error(error) ->
      case remaining > 0 {
        True -> {
          process.sleep(50)
          retry_transient_query(attempt, remaining - 1)
        }
        False -> Error(error)
      }
  }
}

fn run_independent_consumer_claim_test(database_url: String) -> Nil {
  let assert Ok(settings_a) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database_a) = postgres.start(settings_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(database_b) = postgres.start(settings_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(input_codec) =
    worker.codec("claim-race-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("claim-race-output-v1", json.string, decode.string)
  let signals = process.new_subject()
  let assert Ok(definition) =
    worker.define("claim.race", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, ConcurrentClaimWorkerStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("single-owner-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers_a) = registry.new("claim-race")
  let assert Ok(workers_a) = registry.register(workers_a, definition)
  let assert Ok(workers_b) = registry.new("claim-race")
  let assert Ok(workers_b) = registry.register(workers_b, definition)
  let assert Ok(handle) =
    postgres.submit(database_a, "claim-race", definition, 44)
  let assert Ok(consumer_a) =
    queue.start(database_a, workers_a, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer_a) })
  let assert Ok(consumer_b) =
    queue.start(database_b, workers_b, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer_b) })

  let ready = process.new_subject()
  let results = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let go = process.new_subject()
      process.send(ready, #("a", go))
      let _ = process.receive(go, within: 10_000)
      process.send(results, #("a", queue.process_one(consumer_a)))
    })
  let _ =
    process.spawn_unlinked(fn() {
      let go = process.new_subject()
      process.send(ready, #("b", go))
      let _ = process.receive(go, within: 10_000)
      process.send(results, #("b", queue.process_one(consumer_b)))
    })
  let assert Ok(first_ready) = process.receive(ready, within: 1000)
  let assert Ok(second_ready) = process.receive(ready, within: 1000)
  case first_ready, second_ready {
    #("a", go_a), #("b", go_b) -> {
      process.send(go_a, Nil)
      process.send(go_b, Nil)
    }
    #("b", go_b), #("a", go_a) -> {
      process.send(go_a, Nil)
      process.send(go_b, Nil)
    }
    _, _ -> panic as "unexpected concurrent claim synchronization messages"
  }

  let assert Ok(ConcurrentClaimWorkerStarted(release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    process.send(release, ReleaseAttempt)
    Nil
  })
  case process.receive(results, within: 5000) {
    Ok(#(_, Ok(False))) -> Nil
    other -> other |> should.equal(Ok(#("no-second-owner", Ok(False))))
  }
  process.receive(signals, within: 0) |> should.equal(Error(Nil))
  process.send(release, ReleaseAttempt)
  case process.receive(results, within: 5000) {
    Ok(#(_, Ok(True))) -> Nil
    other -> other |> should.equal(Ok(#("no-winner", Ok(True))))
  }
  postgres.state(database_a, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(signals, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("independent-consumers-single-live-claim")
}

fn run_overlapping_claim_test(database_url: String) -> Nil {
  let assert Ok(settings_a) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database_a) = postgres.start(settings_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(database_b) = postgres.start(settings_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(input_codec) =
    worker.codec("claim-overlap-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("claim-overlap-output-v1", json.string, decode.string)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("claim.overlap", "v1", input_codec, output_codec, fn(value) {
      process.send(invoked, value)
      Ok("overlap-" <> int.to_string(value))
    })
  let assert Ok(workers_a) = registry.new("claim-overlap")
  let assert Ok(workers_a) = registry.register(workers_a, definition)
  let assert Ok(workers_b) = registry.new("claim-overlap")
  let assert Ok(workers_b) = registry.register(workers_b, definition)
  let assert Ok(handle) =
    postgres.submit(database_a, "claim-overlap", definition, 45)

  let connection = postgres.connection(database_a)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_claim_overlap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND NEW.state = 'executing' THEN PERFORM pg_advisory_xact_lock(74126, 31); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_claim_overlap BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION grind_test_claim_overlap()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS grind_test_claim_overlap ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_claim_overlap()")
      |> pog.execute(on: connection)
    Nil
  })

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      connection,
      pog.query(
        "SELECT 1 FROM (SELECT pg_advisory_xact_lock(74126, 31)) AS held",
      ),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(consumer_a) =
    queue.start(database_a, workers_a, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer_a) })
  let assert Ok(consumer_b) =
    queue.start(database_b, workers_b, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer_b) })

  let first_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(first_reply, queue.process_one(consumer_a))
    })
  await_claim_waiting_on_advisory(connection, 250) |> should.equal(True)

  // The first production claim has selected and locked the row, then blocks
  // in its UPDATE trigger. The second independent pool must skip that row.
  queue.process_one(consumer_b) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))
  process.receive(first_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.receive(invoked, within: 1000) |> should.equal(Ok(45))
  postgres.state(database_a, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("overlapping-claims-skip-locked")
}

fn await_claim_waiting_on_advisory(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Bool {
  let waiting =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND query LIKE 'WITH candidate AS (%')",
    )
    |> pog.returning({
      use waiting <- decode.field(0, decode.bool)
      decode.success(waiting)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [waiting] -> Ok(waiting)
        _ -> Error(Nil)
      }
    })
  case waiting {
    Ok(True) -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_claim_waiting_on_advisory(connection, checks_remaining - 1)
        }
        False -> False
      }
  }
}

fn run_temporary_worker_death_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("worker-death-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("worker-death-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("worker.death", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, WorkerDeathStarted(process.self(), release))
      process.send(invoked, WorkerInvoked)
      case process.receive(release, within: 30_000) {
        Ok(ReleaseAttempt) -> Ok("must-not-commit-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("worker-death")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "worker-death", definition, 31)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(WorkerDeathStarted(worker_pid, release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(release, ReleaseAttempt)
    Nil
  })
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))
  process.kill(worker_pid)
  process.receive(reply, within: 5000)
  |> should.equal(Ok(Error(queue.QueueWorkerExited)))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("temporary-worker-death-quarantined-no-replay")
}

pub fn postgres_acknowledgement_persists_a_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_receipt_test(database_url)
  }
}

pub fn postgres_ack_commit_connection_loss_is_unknown_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_commit_connection_loss_test(database_url)
  }
}

fn run_ack_commit_connection_loss_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ack-commit-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-commit-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("ack.commit.loss", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      process.send(invoked, WorkerInvoked)
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("terminated-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("ack-commit-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-commit-loss", definition, 21)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_kill_ack_backend() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_ack_backend AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_ack_backend()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_ack_backend ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_kill_ack_backend()")
      |> pog.execute(on: connection)
    Nil
  })

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 100)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckUnknown(
    command_id,
    proposed,
  )))) = process.receive(reply, within: 10_000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "ack-commit-loss-output-v1",
    "\"terminated-21\"",
  ))
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(Error(postgres.ReceiptNotFound))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("ack-commit-connection-loss-unknown-passed")
}

/// Same aborted-commit fault as `run_ack_commit_connection_loss_test` above,
/// but against an AUTOMATIC consumer instead of a manually driven one: no
/// caller is waiting on `process_one`'s reply to retry anything, so recovery
/// here depends entirely on the coordinator's own handling of
/// `QueueAckUnknown`. Before the fix, the coordinator dropped the claim on
/// any `ProcessError` (including this one), so the job sat `Executing` until
/// its lease eventually expired and a claim-time scan quarantined it to
/// `Uncertain` — recoverable only by an operator's audited resolution, never
/// on its own. With the fix, the coordinator keeps retrying the exact same
/// `acknowledge_claim` on its renewal timer; once the trigger stops sleeping
/// (the transaction that aborted never left a receipt behind, so the retry
/// performs the acknowledgement fresh, exactly like the unique-admission
/// aborted-commit test's plain retry converges), the job reaches `succeeded`
/// on its own, without ever needing `resolve_uncertain`.
pub fn postgres_automatic_ack_commit_connection_loss_recovers_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_automatic_ack_commit_connection_loss_recovers_test(database_url)
  }
}

fn run_automatic_ack_commit_connection_loss_recovers_test(
  database_url: String,
) -> Nil {
  let settings =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-ack-commit-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-ack-commit-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "auto.ack.commit.loss",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("auto-terminated-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("auto-ack-commit-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "auto-ack-commit-loss", definition, 41)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(1600)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_kill_ack_backend_auto() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_ack_backend_auto AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_ack_backend_auto()",
    )
    |> pog.execute(on: connection)
  let drop_trigger = fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_ack_backend_auto ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_kill_ack_backend_auto()")
      |> pog.execute(on: connection)
    Nil
  }
  use <- exception.defer(drop_trigger)

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  // The aborted transaction left no receipt at all — same proof
  // `run_ack_commit_connection_loss_test` uses above — so this is genuinely
  // the "never committed" case, not a lost reply after a real commit.
  // `wait_for_job_state_tolerating_errors`, not the plain
  // `wait_for_job_state`: the pool just lost the connection this test itself
  // terminated, and a read against the same pool can transiently error while
  // it recovers — that must not be mistaken for "state observed and it
  // doesn't match yet".
  wait_for_job_state_tolerating_errors(database, handle, job.Executing, 250)
  |> should.equal(True)

  // Dropping the trigger before the coordinator's own retry mirrors the
  // unique-admission aborted-commit test's own ordering: the same
  // `command_id` would hang in another 30-second sleep otherwise, with
  // nobody left to terminate that backend.
  drop_trigger()

  // `pg_terminate_backend` only signals the backend; it can take several
  // seconds of real time for the coordinator's own blocked `acknowledge_claim`
  // call to observe the closed connection (the same reason
  // `run_ack_commit_connection_loss_test` above waits up to 10 seconds for
  // its reply) — this budget must cover that detection latency plus at
  // least one retry interval afterward.
  wait_for_job_state_tolerating_errors(database, handle, job.Succeeded, 750)
  |> should.equal(True)
  // Same transient-pool-recovery tolerance as the wait above: the row is
  // already confirmed committed at this point, but a plain read moments
  // after this test's own killed connection can still transiently time out
  // while the pool recovers.
  retry_transient_query(fn() { postgres.arguments(database, handle) }, 20)
  |> should.equal(Ok(41))
  retry_transient_query(fn() { postgres.outcome(database, handle) }, 20)
  |> should.equal(Ok(job.SucceededWith("auto-terminated-41")))
  mark_database_test_executed(
    "automatic-ack-commit-connection-loss-recovers-passed",
  )
}

/// A *persistently* aborting commit — every acknowledgement attempt for
/// this job, first and every retry alike, hits the same deferred-trigger
/// abort (the trigger is never dropped mid-test) — must not retry forever.
/// Bounded renewal (`ConsumerState.pending_ack_retry_budget`, roughly one
/// lease duration's worth of ticks) means only the first few retries renew
/// the lease; once that budget is spent the lease is left to lapse, and
/// either the retry's own next attempt observes a known
/// `QueueAckStale(_, AckLeaseExpired(..))` or a poll's ordinary
/// claim-time quarantine scan gets there first — either way the job ends up
/// `uncertain`, not stuck `executing` forever. `maximum_concurrency: 2`
/// keeps this consumer polling (via the free-capacity fix) so its own
/// quarantine scan keeps running throughout. Driven entirely by
/// `pg_stat_activity` barriers (`wait_for_commit_trigger_backend`) and a
/// generous iteration cap, not a wall-clock sleep: if retrying were
/// unbounded, this loop would exhaust its cap with the job still
/// `executing`.
pub fn postgres_automatic_ack_retry_bounded_eventually_uncertain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_ack_retry_bounded_test(database_url)
  }
}

fn run_automatic_ack_retry_bounded_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-ack-bounded-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-ack-bounded-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "auto.ack.bounded",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("bounded-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("auto-ack-bounded")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "auto-ack-bounded", definition, 51)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(6100)
    |> queue.with_poll_interval(50)
    |> queue.with_maximum_concurrency(2)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_kill_ack_backend_bounded() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_ack_backend_bounded AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_ack_backend_bounded()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_ack_backend_bounded ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_kill_ack_backend_bounded()")
      |> pog.execute(on: connection)
    Nil
  })

  process.send(release, ReleaseAttempt)

  // The retry budget itself is ~3 ticks regardless of lease length (see
  // `ConsumerState.pending_ack_retry_budget`), but each tick now fires every
  // `lease_duration_ms / 3` — a real ~2000ms with this test's now-6100ms
  // lease (bumped up from 300ms so `queue.start` clears
  // `LeaseTooShortForDeadline` against the default `statement_deadline_ms`)
  // rather than ~100ms, so this iteration cap must cover several times
  // longer in wall-clock terms than before.
  kill_ack_backends_until_uncertain(database, handle, connection, 800)
  |> should.equal(True)

  mark_database_test_executed(
    "automatic-ack-retry-bounded-eventually-uncertain-passed",
  )
}

/// Repeatedly finds and kills this job's own acknowledgement backend (the
/// deferred trigger installed by the caller sleeps every single attempt),
/// checking the job's own state between kills, until it observes `uncertain`
/// or exhausts `remaining_iterations`. Once the ack retry loop itself gives
/// up (a known `QueueAckStale` once the lease lapses), no further backend
/// ever sleeps for this job — `current_ack_rejection` reads the row instead
/// of ever reaching the trigger's own `INSERT` once the row is no longer
/// `executing` — so a "miss" here just means waiting for some poll's own
/// quarantine scan to catch up, not a sign anything is wrong.
fn kill_ack_backends_until_uncertain(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  connection: pog.Connection,
  remaining_iterations: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(job.Uncertain) -> True
    _ ->
      case remaining_iterations > 0 {
        False -> False
        True ->
          case wait_for_commit_trigger_backend(connection, 10) {
            Ok(backend_pid) -> {
              let _ = terminate_backend(connection, backend_pid)
              kill_ack_backends_until_uncertain(
                database,
                handle,
                connection,
                remaining_iterations - 1,
              )
            }
            Error(Nil) -> {
              process.sleep(20)
              kill_ack_backends_until_uncertain(
                database,
                handle,
                connection,
                remaining_iterations - 1,
              )
            }
          }
      }
  }
}

fn wait_for_commit_trigger_backend(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND state = 'active' AND wait_event = 'PgSleep' ORDER BY query_start DESC LIMIT 1",
    )
    |> pog.returning({
      use pid <- decode.field(0, decode.int)
      decode.success(pid)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [pid] -> Ok(pid)
        [] ->
          case checks_remaining > 0 {
            False -> Error(Nil)
            True -> {
              process.sleep(10)
              wait_for_commit_trigger_backend(connection, checks_remaining - 1)
            }
          }
        _ -> Error(Nil)
      }
  }
}

fn backend_pid_is_alive(connection: pog.Connection, pid: Int) -> Bool {
  let query =
    pog.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
    |> pog.parameter(pog.int(pid))
    |> pog.returning({
      use alive <- decode.field(0, decode.bool)
      decode.success(alive)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> True
    Ok(returned) ->
      case returned.rows {
        [alive] -> alive
        _ -> True
      }
  }
}

fn terminate_backend(connection: pog.Connection, pid: Int) -> Bool {
  pog.query("SELECT pg_terminate_backend($1)")
  |> pog.parameter(pog.int(pid))
  |> pog.returning({
    use terminated <- decode.field(0, decode.bool)
    decode.success(terminated)
  })
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    case returned.rows {
      [terminated] -> terminated
      _ -> False
    }
  })
  |> result.unwrap(False)
}

/// Blocks (bounded) until `pid` no longer appears in `pg_stat_activity`.
/// `pg_terminate_backend` only sends the termination signal and returns
/// immediately; it does not wait for the target to actually finish
/// committing and exit. Callers that need PostgreSQL's own commit-visibility
/// side effects (ProcArray removal) to have happened before they proceed —
/// rather than relying on incidentally observing the same backend's own
/// socket close, as the reconciling-from-receipt test does — must wait for
/// this instead of proceeding immediately after termination.
fn wait_for_backend_gone(
  connection: pog.Connection,
  pid: Int,
  checks_remaining: Int,
) -> Result(Nil, Nil) {
  case backend_pid_is_alive(connection, pid) {
    False -> Ok(Nil)
    True ->
      case checks_remaining > 0 {
        False -> Error(Nil)
        True -> {
          process.sleep(10)
          wait_for_backend_gone(connection, pid, checks_remaining - 1)
        }
      }
  }
}

fn wait_for_syncrep_trigger_backend(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND wait_event = 'SyncRep' ORDER BY query_start DESC LIMIT 1",
    )
    |> pog.returning({
      use pid <- decode.field(0, decode.int)
      decode.success(pid)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [pid] -> Ok(pid)
        [] ->
          case checks_remaining > 0 {
            False -> Error(Nil)
            True -> {
              process.sleep(10)
              wait_for_syncrep_trigger_backend(connection, checks_remaining - 1)
            }
          }
        _ -> Error(Nil)
      }
  }
}

/// Fails clearly, instead of an opaque `let assert` mismatch far from the
/// real cause, when the disposable cluster was not started the way the
/// Increment 2 lost-reply tests require it. Without
/// `synchronous_standby_names=grind_never_standby`, a transaction that
/// raises its own `synchronous_commit` to `on` would either commit
/// immediately (a real standby present) or never proceed past `SyncRep` at
/// all in a way these tests can distinguish from a hang.
fn require_syncrep_cluster_configured(connection: pog.Connection) -> Nil {
  let assert Ok(returned) =
    pog.query("SHOW synchronous_standby_names")
    |> pog.returning({
      use value <- decode.field(0, decode.string)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  case returned.rows {
    ["grind_never_standby"] -> Nil
    [other] -> {
      let message =
        "scripts/test-postgres.sh must start PostgreSQL with -c synchronous_standby_names=grind_never_standby -c synchronous_commit=local for the SyncRep-based lost-reply tests to be meaningful; synchronous_standby_names was \""
        <> other
        <> "\" instead"
      panic as message
    }
    _ ->
      panic as "could not read synchronous_standby_names from the test cluster; scripts/test-postgres.sh must start PostgreSQL with -c synchronous_standby_names=grind_never_standby -c synchronous_commit=local"
  }
}

/// Installs a deferred constraint trigger on `table`, scoped by `predicate`
/// (a trusted SQL boolean expression referencing `NEW`, spliced verbatim —
/// never caller/user input), whose function raises only that one matching
/// transaction's `synchronous_commit` to `on` — see the Increment 2 tests
/// below, and Increment 11's uncertain-commit tests
/// (`grind_unique_submissions`, scoped by `submission_id` rather than a
/// server-generated `job_id`, since the submission id is known before the
/// admission transaction that would create the job id even starts).
/// Generalized from an earlier draft that hard-coded both
/// `grind_job_acknowledgements` and a `job_id` equality check. Returns a
/// cleanup thunk for the caller to register with `exception.defer`, which
/// first terminates any backend this same trigger still has parked in
/// `SyncRep` (so a failing assertion earlier in the test cannot hang the
/// whole gate run waiting on a standby that will never connect) and caps the
/// DROP itself with a lock timeout before dropping the trigger and function.
fn install_syncrep_reply_trigger(
  connection: pog.Connection,
  name: String,
  table: String,
  predicate: String,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT ("
      <> predicate
      <> ") THEN RETURN NEW; END IF; PERFORM set_config('synchronous_commit', 'on', true); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> name
      <> " AFTER INSERT ON "
      <> table
      <> " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> name
      <> "()",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ = case wait_for_syncrep_trigger_backend(connection, 0) {
      Ok(stuck_pid) -> terminate_backend(connection, stuck_pid)
      Error(Nil) -> True
    }
    // `connection` is a pool, not one physical connection: a `SET
    // lock_timeout` on its own checks out and releases a connection for
    // that one statement alone, so it would not reliably apply to whichever
    // (possibly different) connection the following `DROP`s happen to check
    // out — silently leaving the DROPs unbounded again. Running
    // `SET LOCAL` and both `DROP`s inside one `pog.transaction` pins them to
    // the same checked-out connection, where `SET LOCAL` actually scopes.
    let _ =
      pog.transaction(connection, fn(transaction_connection) {
        let _ =
          pog.query("SET LOCAL lock_timeout = '2s'")
          |> pog.execute(on: transaction_connection)
        let _ =
          pog.query("DROP TRIGGER IF EXISTS " <> name <> " ON " <> table)
          |> pog.execute(on: transaction_connection)
        let _ =
          pog.query("DROP FUNCTION IF EXISTS " <> name <> "()")
          |> pog.execute(on: transaction_connection)
        Ok(Nil)
      })
    Nil
  }
}

fn stored_attempt_identity(
  connection: pog.Connection,
  job_id: Int,
) -> Result(#(Int, Int), Nil) {
  pog.query("SELECT attempt_id, attempt_epoch FROM grind_jobs WHERE id = $1")
  |> pog.parameter(pog.int(job_id))
  |> pog.returning({
    use attempt_id <- decode.field(0, decode.int)
    use epoch <- decode.field(1, decode.int)
    decode.success(#(attempt_id, epoch))
  })
  |> pog.execute(on: connection)
  |> result.replace_error(Nil)
  |> result.try(fn(returned) {
    case returned.rows {
      [row] -> Ok(row)
      _ -> Error(Nil)
    }
  })
}

/// Increment 2: a genuinely successful ack whose reply is lost after
/// PostgreSQL has already committed locally. The disposable cluster is
/// started with `synchronous_standby_names=grind_never_standby` and
/// `synchronous_commit=local` (scripts/test-postgres.sh), so an ordinary
/// commit stays local, but a deferred constraint trigger scoped to this
/// job's acknowledgement row raises this one transaction's own
/// `synchronous_commit` to `on` (session-local, `set_config(..., true)`)
/// just before COMMIT. Because the configured standby name never connects,
/// that COMMIT parks in PostgreSQL's `SyncRep` wait *after* its WAL record is
/// already locally flushed — genuinely committed, reply not yet sent.
/// Terminating that backend at that exact moment (observed by polling
/// `pg_stat_activity` for `wait_event = 'SyncRep'`) reproduces "PostgreSQL
/// committed, but the client's connection closed before it saw the reply"
/// without a TCP proxy or any production test hook: the client observes a
/// closed connection during COMMIT, exactly like the existing aborted-commit
/// test, but this time a receipt genuinely exists to reconcile from.
pub fn postgres_ack_committed_reply_lost_reconciles_from_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_ack_committed_reply_lost_reconciles_test(database_url)
  }
}

fn run_ack_committed_reply_lost_reconciles_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)
  let assert Ok(input_codec) =
    worker.codec("ack-reply-lost-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-reply-lost-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("ack.reply.lost", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      process.send(invoked, WorkerInvoked)
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("reply-lost-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("ack-reply-lost")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-reply-lost", definition, 33)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let job_id = job.id_value(handle)
  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_syncrep_reply_lost",
    "grind_job_acknowledgements",
    "NEW.job_id = " <> int.to_string(job_id),
  ))

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  process.receive(reply, within: 10_000) |> should.equal(Ok(Ok(True)))
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("reply-lost-33")))
  let assert Ok(#(attempt_id, epoch)) =
    stored_attempt_identity(connection, job_id)
  let command_id = attempt.acknowledgement_command_id(job_id, attempt_id, epoch)
  let assert Ok(postgres.AcknowledgementReceipt(committed_state:, ..)) =
    postgres.reconcile_acknowledgement(database, handle, command_id)
  committed_state |> should.equal(job.Succeeded)
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("ack-committed-reply-lost-reconciled-passed")
}

pub fn postgres_reconcile_acknowledgement_wrong_job_command_id_is_receipt_job_mismatch_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_reconcile_acknowledgement_wrong_job_test(database_url)
  }
}

/// A receipt read back under another job's own command ID is a genuine
/// caller mistake (the command ID was copied from the wrong handle), not a
/// missing job or a missing receipt: `ReceiptJobMismatch` names it precisely
/// instead of collapsing it into `StorageOwnerMismatch`.
fn run_reconcile_acknowledgement_wrong_job_test(database_url: String) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("receipt-job-mismatch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("receipt-job-mismatch-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "receipt.job.mismatch",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("receipt-job-mismatch")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle_a) =
    postgres.submit(database, "receipt-job-mismatch", definition, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "receipt-job-mismatch", definition, 2)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  queue.process_one(consumer) |> should.equal(Ok(True))
  queue.process_one(consumer) |> should.equal(Ok(True))
  let connection = postgres.connection(database)
  let job_id_a = job.id_value(handle_a)
  let job_id_b = job.id_value(handle_b)
  let assert Ok(#(attempt_id_b, epoch_b)) =
    stored_attempt_identity(connection, job_id_b)
  let command_id_b =
    attempt.acknowledgement_command_id(job_id_b, attempt_id_b, epoch_b)
  postgres.reconcile_acknowledgement(database, handle_a, command_id_b)
  |> should.equal(
    Error(postgres.ReceiptJobMismatch(expected: job_id_a, actual: job_id_b)),
  )
  mark_database_test_executed(
    "reconcile-acknowledgement-receipt-job-mismatch-passed",
  )
}

/// Same fault as above, but Grind's own pool is closed (not the PostgreSQL
/// backend) while the ack's COMMIT is still parked in `SyncRep`, so the
/// receipt lookup that would otherwise reconcile the lost reply cannot run
/// either. A separate observer pool (independent of Grind's pool) is used to
/// poll for the SyncRep wait, read the committed attempt identity, and later
/// terminate the stuck backend once the store-unavailable assertion has been
/// made, exactly as prescribed.
pub fn postgres_ack_committed_reply_lost_with_store_unavailable_is_unknown_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_ack_committed_reply_lost_with_store_unavailable_test(database_url)
  }
}

fn run_ack_committed_reply_lost_with_store_unavailable_test(
  database_url: String,
) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_settings =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = postgres.connection(observer)
  require_syncrep_cluster_configured(observer_connection)

  let assert Ok(input_codec) =
    worker.codec("ack-reply-lost-unavailable-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "ack-reply-lost-unavailable-output-v1",
      json.string,
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "ack.reply.lost.unavailable",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("unavailable-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("ack-reply-lost-unavailable")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-reply-lost-unavailable", definition, 34)
  let attempt_owner = "ack-reply-lost-unavailable-owner"
  // Claimed directly through the postgres-level API (not `queue`), so the
  // opaque `ClaimedJob`/`Execution` values stay in scope for the same-command
  // retry through `attempt.acknowledge` after the pool is reopened,
  // below. `claim_one` itself does not block; only the worker's own handler
  // (invoked by `execute_claim`, in the spawned process) does.
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "ack-reply-lost-unavailable",
      workers,
      attempt_owner,
      30_000,
    )
  let #(claimed_id, attempt_id, epoch) = attempt.claim_identity(claimed)
  let command_id =
    attempt.acknowledgement_command_id(claimed_id, attempt_id, epoch)
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let execution = attempt.execute_claim(claimed)
      let ack_result =
        attempt.acknowledge(
          database,
          "ack-reply-lost-unavailable",
          attempt_owner,
          claimed,
          execution,
        )
      process.send(reply, #(execution, ack_result))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let job_id = job.id_value(handle)
  use <- exception.defer(install_syncrep_reply_trigger(
    observer_connection,
    "grind_test_syncrep_reply_lost_unavailable",
    "grind_job_acknowledgements",
    "NEW.job_id = " <> int.to_string(job_id),
  ))

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) =
    wait_for_syncrep_trigger_backend(observer_connection, 300)

  let _ = postgres.close(database)

  let assert Ok(#(execution, ack_result)) =
    process.receive(reply, within: 10_000)
  execution
  |> should.equal(worker.ExecutedSuccess(
    "ack-reply-lost-unavailable-output-v1",
    "\"unavailable-34\"",
  ))
  ack_result
  |> should.equal(Error(postgres.QueueAckUnknown(command_id, execution)))

  terminate_backend(observer_connection, backend_pid) |> should.equal(True)
  // `pg_terminate_backend` only signals the backend; it returns before the
  // target has actually finished `ProcArrayEndTransaction` and exited. Unlike
  // the reconciles-from-receipt test (where the coordinator's own blocked
  // read on that same backend already orders its lookup after that step),
  // here Grind's pool was closed client-side, so nothing else orders "reopen
  // and query" after "the backend actually finished committing." Wait for it
  // explicitly instead of assuming it.
  let assert Ok(Nil) =
    wait_for_backend_gone(observer_connection, backend_pid, 300)

  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  let assert Ok(postgres.AcknowledgementReceipt(committed_state:, ..)) =
    postgres.reconcile_acknowledgement(reopened, handle, command_id)
  committed_state |> should.equal(job.Succeeded)
  postgres.outcome(reopened, handle)
  |> should.equal(Ok(job.SucceededWith("unavailable-34")))

  // The same command, retried end to end through the reopened store: proves
  // idempotent replay, not just that the receipt can be read back.
  attempt.acknowledge(
    reopened,
    "ack-reply-lost-unavailable",
    attempt_owner,
    claimed,
    execution,
  )
  |> should.equal(Ok(True))

  let assert Ok(fresh_consumer) =
    queue.start(reopened, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(fresh_consumer)
    Nil
  })
  queue.process_one(fresh_consumer) |> should.equal(Ok(False))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed(
    "ack-committed-reply-lost-store-unavailable-unknown-passed",
  )
}

fn run_ack_receipt_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ack-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-output-v1", json.string, decode.string)
  let invocation = process.new_subject()
  let assert Ok(definition) =
    worker.define("ack.receipt", "v1", input_codec, output_codec, fn(value) {
      process.send(invocation, WorkerInvoked)
      Ok("result-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("ack-receipt")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "ack-receipt", definition, 8)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "ack-receipt",
      workers,
      "ack-receipt-owner",
      30_000,
    )
  let execution = attempt.execute_claim(claimed)
  process.receive(invocation, within: 1000) |> should.equal(Ok(WorkerInvoked))
  attempt.acknowledge(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    worker.ExecutedSuccess("ack-output-v2", "\"wrong-codec\""),
  )
  |> should.equal(
    Error(postgres.QueueAckProposalCodecMismatch(
      worker.OutputCodec,
      "ack-output-v1",
      "ack-output-v2",
    )),
  )
  let #(claimed_id, attempt_id, epoch) = attempt.claim_identity(claimed)
  let command_id =
    attempt.acknowledgement_command_id(claimed_id, attempt_id, epoch)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_test_reject_ack CHECK (command_id <> '"
      <> command_id
      <> "')",
    )
    |> pog.execute(on: connection)
  let assert Error(_) =
    attempt.acknowledge(
      database,
      "ack-receipt",
      "ack-receipt-owner",
      claimed,
      execution,
    )
  let assert Ok(after_failed_ack) =
    pog.query(
      "SELECT state, (SELECT count(*) FROM grind_job_acknowledgements WHERE storage_owner = $2 AND command_id = $3)::bigint FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.parameter(pog.text(postgres.storage_owner(database)))
    |> pog.parameter(pog.text(command_id))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use receipts <- decode.field(1, decode.int)
      decode.success(#(state, receipts))
    })
    |> pog.execute(on: connection)
  let assert [#(state_after_failed_ack, receipt_count_after_failed_ack)] =
    after_failed_ack.rows
  state_after_failed_ack |> should.equal("executing")
  receipt_count_after_failed_ack |> should.equal(0)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_test_reject_ack",
    )
    |> pog.execute(on: connection)
  attempt.acknowledge(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  // A retry with the same stable command and exact proposal is idempotent.
  attempt.acknowledge(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  attempt.acknowledge(
    database,
    "ack-receipt",
    "ack-receipt-owner",
    claimed,
    worker.ExecutedSuccess("ack-output-v1", "\"tampered\""),
  )
  |> should.equal(Error(postgres.QueueAckCommandConflict))
  let assert Ok(receipts) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, command_id, attempt_owner, queue, worker_id, worker_version, committed_state, failure_cause, octet_length(proposal_sha256), (extract(epoch FROM committed_at) * 1000)::bigint FROM grind_job_acknowledgements WHERE storage_owner = $1 AND job_id = $2",
    )
    |> pog.parameter(pog.text(postgres.storage_owner(database)))
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use epoch <- decode.field(1, decode.int)
      use command_id <- decode.field(2, decode.string)
      use attempt_owner <- decode.field(3, decode.string)
      use queue <- decode.field(4, decode.string)
      use worker_id <- decode.field(5, decode.string)
      use worker_version <- decode.field(6, decode.string)
      use committed_state <- decode.field(7, decode.string)
      use failure_cause <- decode.field(8, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(9, decode.int)
      use committed_at <- decode.field(10, decode.int)
      decode.success(#(
        attempt_id,
        epoch,
        command_id,
        attempt_owner,
        queue,
        worker_id,
        worker_version,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        committed_at,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      attempt_id,
      epoch,
      command_id,
      attempt_owner,
      queue,
      worker_id,
      worker_version,
      committed,
      failure_cause,
      fingerprint_bytes,
      committed_at,
    ),
  ] = receipts.rows
  should.be_true(attempt_id > 0)
  epoch |> should.equal(1)
  command_id |> should.not_equal("")
  attempt_owner |> should.equal("ack-receipt-owner")
  queue |> should.equal("ack-receipt")
  worker_id |> should.equal("ack.receipt")
  worker_version |> should.equal("v1")
  committed |> should.equal("succeeded")
  failure_cause |> should.equal(None)
  fingerprint_bytes |> should.equal(32)
  should.be_true(committed_at > 0)
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: receipt_command,
    attempt_id: receipt_attempt,
    attempt_epoch: receipt_epoch,
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at_unix_ms: receipt_time,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_command |> should.equal(command_id)
  receipt_attempt |> should.equal(attempt_id)
  receipt_epoch |> should.equal(epoch)
  receipt_state |> should.equal(job.Succeeded)
  receipt_cause |> should.equal(None)
  receipt_time |> should.equal(committed_at)
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("result-8")))
  mark_database_test_executed("durable-ack-receipt-passed")
}

fn run_lease_renewal_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("renewal-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("renewal-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define("lease.renewal", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("finished-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("lease-renewal")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "lease-renewal", slow_worker, 7)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(1600)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  let connection = postgres.connection(database)
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let observed_renewal = case
    lease_expiration(connection, job.id_value(handle))
  {
    Ok(initial_expiry) ->
      await_later_lease_expiry(
        connection,
        job.id_value(handle),
        initial_expiry + 50,
        125,
      )
    Error(Nil) -> False
  }
  process.send(release, ReleaseAttempt)
  let assert Ok(Ok(True)) = process.receive(reply, within: 5000)
  observed_renewal |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("lease-renewal-before-ack-passed")
}

fn run_lease_renewal_loss_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("renewal-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("renewal-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.renewal.loss",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("lost-lease-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("lease-renewal-loss")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "lease-renewal-loss", slow_worker, 7)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(1600)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  let connection = postgres.connection(database)
  force_lease_expired(connection, job.id_value(handle))
  |> should.equal(Ok(Nil))
  await_renewal_lost(consumer, 100) |> should.equal(True)
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))

  process.send(release, ReleaseAttempt)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
    proposed,
    postgres.AckLeaseExpired(..),
  )))) = process.receive(reply, within: 5000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "renewal-loss-output-v1",
    "\"lost-lease-7\"",
  ))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_database_test_executed("lease-renewal-loss-fenced-passed")
}

fn run_renewal_storage_error_test(database_url: String) -> Nil {
  let settings =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("renewal-storage-error-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("renewal-storage-error-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.renewal.storage-error",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("reconnected-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("renewal-storage-error")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "renewal-storage-error", slow_worker, 18)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(1600)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  // A PostgreSQL trigger returns a real query error for lease renewal while
  // leaving the connection and coordinator alive. This exercises the storage
  // error result path without conflating it with process death.
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_reject_renewal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.state = 'executing' AND NEW.state = 'executing' AND NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at THEN RAISE EXCEPTION 'injected renewal query failure'; END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_reject_renewal BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION grind_test_reject_renewal()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_reject_renewal ON grind_jobs",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_reject_renewal()")
      |> pog.execute(on: connection)
    Nil
  })
  await_renewal_status(consumer, queue.LeaseRenewalUnknown, 100)
  |> should.equal(True)
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  let assert Ok(_) =
    pog.query("DROP TRIGGER grind_test_reject_renewal ON grind_jobs")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP FUNCTION grind_test_reject_renewal()")
    |> pog.execute(on: connection)
  await_renewal_status(consumer, queue.LeaseRenewalConfirmed, 100)
  |> should.equal(True)
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("renewal-storage-error-retried-passed")
}

fn run_closed_pool_renewal_test(database_url: String) -> Nil {
  let settings =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("closed-pool-renewal-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("closed-pool-renewal-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "lease.closed-pool.renewal",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("recovered-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("closed-pool-renewal")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "closed-pool-renewal", slow_worker, 19)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(1600)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)

  // A missing named pool used to let pgo_pool:checkout exit through Pog and
  // kill the queue coordinator. The typed consumer must retain the active
  // claim, report uncertainty, and recover after the same pool is reopened.
  let _ = postgres.close(database)
  postgres.state(database, handle)
  |> should.equal(Error(postgres.JobReadQueryFailed(pog.ConnectionUnavailable)))
  await_renewal_status(consumer, queue.LeaseRenewalUnknown, 100)
  |> should.equal(True)
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  let assert Ok(reopened_database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened_database) })
  await_renewal_status(consumer, queue.LeaseRenewalConfirmed, 100)
  |> should.equal(True)
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(reopened_database, handle)
  |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("closed-pool-renewal-recovered-passed")
}

/// The owner and pool losses here are sequential, not concurrent: the pool is
/// only closed and reopened after the owner and its cascaded worker are both
/// confirmed dead, as recovery plumbing following that death, not as a second
/// failure landing during active work.
fn run_owner_loss_recovers_after_pool_restart_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("owner-pool-loss-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("owner-pool-loss-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("owner.pool.loss", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(invoked, WorkerInvoked)
      process.send(started, OwnerPoolLossStarted(process.self(), release))
      case process.receive(release, within: 20_000) {
        Ok(ReleaseAttempt) -> Ok("owner-pool-loss-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("owner-pool-loss")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "owner-pool-loss", definition, 61)
  let job_id = job.id_value(handle)

  let owner_ready = process.new_subject()
  let owner =
    process.spawn_unlinked(fn() {
      let assert Ok(policy) =
        queue.default_policy()
        |> queue.with_poll_interval(20)
        |> queue.with_maximum_concurrency(1)
        |> queue.with_lease_duration(5000)
        |> queue.validate_policy
      case queue.start(database, workers, policy) {
        Error(error) ->
          process.send(owner_ready, OwnerPoolLossOwnerFailed(error))
        Ok(consumer) -> {
          process.send(
            owner_ready,
            OwnerPoolLossOwnerReady(process.self(), consumer),
          )
          process.sleep(60_000)
        }
      }
    })
  let owner_monitor = process.monitor(owner)
  let assert Ok(OwnerPoolLossOwnerReady(started_owner, _consumer)) =
    process.receive(owner_ready, within: 5000)
  started_owner |> should.equal(owner)

  let assert Ok(OwnerPoolLossStarted(worker_pid, _release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))
  let worker_monitor = process.monitor(worker_pid)

  // The owner is not part of this test process's own supervision tree, so
  // killing it must be observed rather than assumed: the consumer's
  // top-level supervisor is linked to whichever process called
  // `queue.start`, so the owner's death cascades down through
  // that supervisor, the coordinator, and the coordinator's own linked
  // worker factory, ending in the blocked worker's death too.
  process.kill(owner)
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(owner_monitor, fn(down) {
      #("owner", down)
    })
    |> process.select_specific_monitor(worker_monitor, fn(down) {
      #("worker", down)
    })
  // Collected order-independently: the owner's death and the worker's death
  // are two separate cascading events from this test's observation point,
  // and only their causal order (owner, then worker) is guaranteed, not the
  // order in which their DOWN messages are scheduled into this mailbox.
  let assert Ok(#(first_down_tag, _)) =
    process.selector_receive(down_selector, within: 5000)
  let assert Ok(#(second_down_tag, _)) =
    process.selector_receive(down_selector, within: 5000)
  { first_down_tag != second_down_tag } |> should.equal(True)
  { first_down_tag == "owner" || first_down_tag == "worker" }
  |> should.equal(True)
  { second_down_tag == "owner" || second_down_tag == "worker" }
  |> should.equal(True)

  let _ = postgres.close(database)
  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  let connection = postgres.connection(reopened)
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  let assert Ok(fresh_consumer) =
    queue.start(reopened, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(fresh_consumer)
    Nil
  })

  queue.process_one(fresh_consumer) |> should.equal(Ok(False))
  postgres.state(reopened, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))

  let assert Ok(rebound) = postgres.bind_handle(reopened, definition, job_id)
  postgres.resolve_uncertain(
    reopened,
    rebound,
    postgres.ResolutionRequest(
      "owner-pool-loss-authorized-replay",
      "on-call",
      "inspect the external effect before authorizing a new delivery",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))

  let replay_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(replay_reply, queue.process_one(fresh_consumer))
    })
  let assert Ok(OwnerPoolLossStarted(_, replay_release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))
  process.send(replay_release, ReleaseAttempt)
  process.receive(replay_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(reopened, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  let assert Ok(#(final_attempt_count, final_delivery_count)) =
    attempt_accounting(connection, job_id)
  final_attempt_count |> should.equal(2)
  final_delivery_count |> should.equal(2)
  let _ = process.demonitor_process(owner_monitor)
  mark_database_test_executed("owner-loss-pool-restart-quarantined-no-replay")
}

fn run_worker_start_failure_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("start-failure-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("start-failure-output-v1", json.string, decode.string)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("start.failure", "v1", input_codec, output_codec, fn(value) {
      process.send(invoked, Nil)
      Ok("started-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("start-failure")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "start-failure", definition, 15)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'scheduled', available_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: postgres.connection(database))
  let fail_next_worker_start = one_shot.armed()
  let hooks =
    consumer_hooks.Hooks(
      before_worker_start: fn() {
        case one_shot.take(fail_next_worker_start) {
          True -> Error("injected start failure")
          False -> Ok(Nil)
        }
      },
      after_worker_start: fn(_pid) { Nil },
    )
  let assert Ok(consumer) =
    queue.start_with_hooks(database, workers, manual_policy(), hooks)
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueWorkerStartFailed(actor.InitFailed("injected start failure")),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let connection = postgres.connection(database)
  attempt_count_for(connection, job.id_value(handle))
  |> should.equal(Ok(0))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  process.receive(invoked, within: 0) |> should.equal(Ok(Nil))
  mark_database_test_executed("unstarted-worker-claim-released-passed")
}

fn attempt_count_for(connection: pog.Connection, id: Int) -> Result(Int, Nil) {
  let query =
    pog.query("SELECT attempt_count FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [count] -> Ok(count)
        _ -> Error(Nil)
      }
  }
}

fn await_renewal_lost(consumer: queue.Consumer, checks_remaining: Int) -> Bool {
  case queue.renewal_status(consumer) {
    Ok(Some(queue.LeaseRenewalLost)) -> True
    _ ->
      case checks_remaining > 0 {
        False -> False
        True -> {
          process.sleep(10)
          await_renewal_lost(consumer, checks_remaining - 1)
        }
      }
  }
}

fn await_renewal_status(
  consumer: queue.Consumer,
  expected: queue.RenewalStatus,
  checks_remaining: Int,
) -> Bool {
  case queue.renewal_status(consumer) {
    Ok(Some(actual)) ->
      case actual == expected {
        True -> True
        False -> retry_renewal_status(consumer, expected, checks_remaining)
      }
    _ -> retry_renewal_status(consumer, expected, checks_remaining)
  }
}

fn retry_renewal_status(
  consumer: queue.Consumer,
  expected: queue.RenewalStatus,
  checks_remaining: Int,
) -> Bool {
  case checks_remaining > 0 {
    False -> False
    True -> {
      process.sleep(10)
      await_renewal_status(consumer, expected, checks_remaining - 1)
    }
  }
}

fn force_lease_expired(
  connection: pog.Connection,
  id: Int,
) -> Result(Nil, Nil) {
  pog.query(
    "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.map(fn(_) { Nil })
}

fn lease_expiration(connection: pog.Connection, id: Int) -> Result(Int, Nil) {
  pog.query(
    "SELECT (extract(epoch FROM lease_expires_at) * 1000)::bigint FROM grind_jobs WHERE id = $1",
  )
  |> pog.parameter(pog.int(id))
  |> pog.returning({
    use expiry <- decode.field(0, decode.int)
    decode.success(expiry)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [expiry] -> Ok(expiry)
      _ -> Error(Nil)
    }
  })
}

fn await_later_lease_expiry(
  connection: pog.Connection,
  id: Int,
  threshold: Int,
  remaining_checks: Int,
) -> Bool {
  case lease_expiration(connection, id) {
    Ok(expiry) ->
      case expiry > threshold, remaining_checks > 0 {
        True, _ -> True
        False, True -> {
          process.sleep(20)
          await_later_lease_expiry(
            connection,
            id,
            threshold,
            remaining_checks - 1,
          )
        }
        False, False -> False
      }
    Error(Nil) -> False
  }
}

fn run_scheduled_due_time_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("scheduled-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("scheduled-output-v1", json.int, decode.int)
  let probe = process.new_subject()
  let assert Ok(scheduled_worker) =
    worker.define("scheduled.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(value)
    })
  let assert Ok(workers) = registry.new("scheduled-boundary")
  let assert Ok(workers) = registry.register(workers, scheduled_worker)
  let connection = postgres.connection(database)
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint + 60000",
    )
    |> pog.returning({
      use value <- decode.field(0, decode.int)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  let assert [future_unix_ms] = returned.rows
  let assert Ok(available_at) = job.available_at(future_unix_ms)
  let assert Ok(handle) =
    postgres.submit_at(
      database,
      "scheduled-boundary",
      scheduled_worker,
      17,
      available_at,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2 AND state = 'scheduled'",
    )
    |> pog.parameter(pog.text("scheduled.echo"))
    |> pog.parameter(pog.text("scheduled-boundary"))
    |> pog.execute(on: connection)

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(probe, within: 0) |> should.equal(Ok(WorkerInvoked))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("scheduled-due-time-passed")
}

/// Proves the automatic consumer actually waits for the database's own
/// clock to reach `available_at` before claiming, rather than merely being
/// eligible to run afterward. Honest limit: Grind has no LISTEN/NOTIFY
/// wakeup path (confirmed by reading the source — `claim_registered_job`
/// and the coordinator's self-scheduled `Poll` timer are the only ways a
/// row ever gets claimed; no PostgreSQL channel is ever subscribed to);
/// this proves wakeup via polling after the deadline elapses, not a
/// notification-driven wakeup. Two independent database-time observations
/// back this claim: (1) immediately after the consumer starts, the job is
/// still `Scheduled` and the database clock is still before `available_at`
/// — proving at least one pre-deadline poll tick genuinely skipped the row
/// (this assertion is not retried, so a too-slow environment fails it
/// honestly instead of silently passing); (2) the handler itself, at the
/// moment it actually runs, compares `available_at` against the row's own
/// recorded *claim* time (`lease_expires_at - lease_duration`) rather than
/// its own later `clock_timestamp()` call, so the observation is pinned to
/// when the claim SQL actually admitted the row, not to whatever moment the
/// handler happens to be scheduled afterward.
fn run_automatic_wakeup_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("auto-wakeup-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("auto-wakeup-output-v1", json.bool, decode.bool)
  let connection = postgres.connection(database)
  let observed = process.new_subject()
  let lease_duration_ms = 5000
  let assert Ok(auto_worker) =
    worker.define("auto.wakeup", "v1", input_codec, output_codec, fn(_value) {
      let assert Ok(returned) =
        pog.query(
          "SELECT (lease_expires_at - ($1::double precision * interval '1 millisecond')) >= available_at FROM grind_jobs WHERE worker_id = 'auto.wakeup' AND queue = 'auto-wakeup'",
        )
        |> pog.parameter(pog.int(lease_duration_ms))
        |> pog.returning({
          use due <- decode.field(0, decode.bool)
          decode.success(due)
        })
        |> pog.execute(on: connection)
      let assert [due] = returned.rows
      process.send(observed, due)
      Ok(due)
    })
  let assert Ok(workers) = registry.new("auto-wakeup")
  let assert Ok(workers) = registry.register(workers, auto_worker)
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint + 300",
    )
    |> pog.returning({
      use value <- decode.field(0, decode.int)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  let assert [future_unix_ms] = returned.rows
  let assert Ok(available_at) = job.available_at(future_unix_ms)
  let assert Ok(handle) =
    postgres.submit_at(database, "auto-wakeup", auto_worker, 1, available_at)
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(20)
    |> queue.with_lease_duration(lease_duration_ms)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  // Not retried: this proves a pre-deadline poll tick genuinely observed
  // the row as not-yet-due. A too-slow environment (already past the
  // ~300ms deadline by the time this runs) fails this assertion honestly
  // rather than the test silently skipping the proof.
  let assert Ok(pre_deadline) =
    pog.query(
      "SELECT clock_timestamp() < available_at FROM grind_jobs WHERE worker_id = 'auto.wakeup' AND queue = 'auto-wakeup'",
    )
    |> pog.returning({
      use before_deadline <- decode.field(0, decode.bool)
      decode.success(before_deadline)
    })
    |> pog.execute(on: connection)
  let assert [before_deadline] = pre_deadline.rows
  before_deadline |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  process.receive(observed, within: 5000) |> should.equal(Ok(True))
  wait_for_job_state(database, handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed("automatic-wakeup-database-deadline-passed")
}

pub fn postgres_manual_batch_reports_acknowledged_prefix_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_batch_partial_error_test(database_url)
  }
}

fn run_batch_partial_error_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("partial-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("partial-output-v1", json.int, decode.int)
  let assert Ok(first_worker) =
    worker.define(
      "batch.partial.first",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(incompatible_worker) =
    worker.define(
      "batch.partial.incompatible",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(last_worker) =
    worker.define(
      "batch.partial.last",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(workers) = registry.new("batch-partial")
  let assert Ok(workers) = registry.register(workers, first_worker)
  let assert Ok(workers) = registry.register(workers, incompatible_worker)
  let assert Ok(workers) = registry.register(workers, last_worker)
  let assert Ok(first) =
    postgres.submit(database, "batch-partial", first_worker, 1)
  let assert Ok(incompatible) =
    postgres.submit(database, "batch-partial", incompatible_worker, 2)
  let assert Ok(last) =
    postgres.submit(database, "batch-partial", last_worker, 3)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2 AND queue = $3",
    )
    |> pog.parameter(pog.text("partial-output-v2"))
    |> pog.parameter(pog.text("batch.partial.incompatible"))
    |> pog.parameter(pog.text("batch-partial"))
    |> pog.execute(on: connection)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_jobs_per_poll(3)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_available(consumer)
  |> should.equal(queue.BatchStopped(
    acknowledged_before_error: 1,
    error: queue.QueueProcessFailed(postgres.QueueCodecMismatch(
      kind: worker.OutputCodec,
      expected: "partial-output-v2",
      actual: "partial-output-v1",
    )),
  ))
  postgres.state(database, first) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, incompatible)
  |> should.equal(Ok(job.ContractMismatch))
  postgres.state(database, last) |> should.equal(Ok(job.Queued))
  mark_database_test_executed("batch-partial-commit-count-passed")
}

pub fn postgres_expired_attempt_requires_audited_replay_and_stale_ack_is_fenced_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_takeover_fencing_test(database_url)
  }
}

fn run_takeover_fencing_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("takeover-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("takeover-output-v1", json.string, decode.string)
  let signals = process.new_subject()
  let assert Ok(first_worker) =
    worker.define("takeover.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, FirstAttemptStarted(release))
      case process.receive(release, within: 15_000) {
        Ok(ReleaseAttempt) -> Ok("obsolete-" <> int.to_string(value))
        Error(Nil) -> Error(Nil)
      }
    })
  let assert Ok(replay_worker) =
    worker.define("takeover.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, TakeoverAttemptStarted(release))
      case process.receive(release, within: 15_000) {
        Ok(ReleaseAttempt) -> Ok("current-" <> int.to_string(value))
        Error(Nil) -> Error(Nil)
      }
    })
  let assert Ok(first_registry) = registry.new("takeover-fence")
  let assert Ok(first_registry) =
    registry.register(first_registry, first_worker)
  let assert Ok(replay_registry) = registry.new("takeover-fence")
  let assert Ok(replay_registry) =
    registry.register(replay_registry, replay_worker)
  let assert Ok(first_consumer) =
    queue.start(database, first_registry, manual_policy())
  use <- exception.defer(fn() { queue.stop(first_consumer) })
  let assert Ok(replay_consumer) =
    queue.start(database, replay_registry, manual_policy())
  use <- exception.defer(fn() { queue.stop(replay_consumer) })
  let assert Ok(handle) =
    postgres.submit(database, "takeover-fence", first_worker, 7)
  let first_reply = process.new_subject()
  let first_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(first_reply, queue.process_one(first_consumer))
    })
  let assert Ok(FirstAttemptStarted(first_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(first_finished, first_release, first_reply)
  })

  let connection = postgres.connection(database)
  let assert Ok(first_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use attempt_count <- decode.field(3, decode.int)
      use delivery_count <- decode.field(4, decode.int)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        attempt_count,
        delivery_count,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#(first_attempt_id, first_epoch, first_owner, 1, 1)] =
    first_claim.rows
  first_epoch |> should.equal(1)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)

  replay_consumer
  |> queue.process_one
  |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  process.receive(signals, within: 0) |> should.equal(Error(Nil))

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "takeover-fence-authorized-replay",
      "on-call",
      "inspect the external effect before authorizing a new delivery",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(authorized_accounting) =
    pog.query(
      "SELECT attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use delivery_count <- decode.field(1, decode.int)
      decode.success(#(attempt_count, delivery_count))
    })
    |> pog.execute(on: connection)
  let assert [#(1, 1)] = authorized_accounting.rows

  let replay_reply = process.new_subject()
  let replay_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(replay_reply, queue.process_one(replay_consumer))
    })
  let assert Ok(TakeoverAttemptStarted(replay_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(replay_finished, replay_release, replay_reply)
  })
  let assert Ok(replay_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use attempt_count <- decode.field(3, decode.int)
      use delivery_count <- decode.field(4, decode.int)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        attempt_count,
        delivery_count,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#(replay_attempt_id, replay_epoch, replay_owner, 2, 2)] =
    replay_claim.rows
  replay_attempt_id |> should.not_equal(first_attempt_id)
  replay_epoch |> should.equal(first_epoch + 1)
  replay_owner |> should.not_equal(first_owner)

  process.send(first_release, ReleaseAttempt)
  process.receive(first_reply, within: 5000)
  |> should.equal(
    Ok(
      Error(
        queue.QueueProcessFailed(postgres.QueueAckStale(
          worker.ExecutedSuccess("takeover-output-v1", "\"obsolete-7\""),
          postgres.AckOwnershipChanged(
            attempt_id: Some(replay_attempt_id),
            epoch: Some(replay_epoch),
            owner: Some(replay_owner),
          ),
        )),
      ),
    ),
  )
  process.send(first_finished, Nil)

  process.send(replay_release, ReleaseAttempt)
  process.receive(replay_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.send(replay_finished, Nil)
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("current-7")))
  mark_database_test_executed("expired-attempt-audited-replay-passed")
}

pub fn postgres_expired_attempt_requires_reconciliation_by_default_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_expired_attempt_quarantine_test(database_url)
  }
}

pub fn postgres_expiry_quarantine_is_bounded_per_attempt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_bounded_quarantine_test(database_url)
  }
}

pub fn postgres_acknowledgement_rejects_exact_database_expiry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_exact_expiry_test(database_url)
  }
}

pub fn postgres_ack_after_database_expiry_is_stale_without_receipt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_after_database_expiry_test(database_url)
  }
}

pub fn postgres_uncertain_replay_requires_audited_resolution_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_uncertain_resolution_test(database_url)
  }
}

fn run_uncertain_resolution_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolve-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolve-output-v1", json.string, decode.string)
  let invocations = process.new_subject()
  let assert Ok(worker) =
    worker.define("resolve.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(invocations, WorkerInvoked)
      Ok("resolved-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("uncertain-resolution")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(handle) =
    postgres.submit(database, "uncertain-resolution", worker, 12)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 121, attempt_epoch = 6, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-121",
      "on-call",
      "confirm external idempotency record before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-121",
      "on-call",
      "confirm external idempotency record before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))
  let assert Ok(audit) =
    pog.query(
      "SELECT resolution_id, job_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at IS NOT NULL, decision, resolved_by, details FROM grind_job_resolutions WHERE job_id = $1 AND resolution_id = $2",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text("resolution-121"))
    |> pog.returning({
      use resolution_id <- decode.field(0, decode.string)
      use job_id <- decode.field(1, decode.int)
      use attempt_id <- decode.field(2, decode.int)
      use attempt_epoch <- decode.field(3, decode.int)
      use attempt_owner <- decode.field(4, decode.string)
      use expiry_retained <- decode.field(5, decode.bool)
      use decision <- decode.field(6, decode.string)
      use resolved_by <- decode.field(7, decode.string)
      use details <- decode.field(8, decode.string)
      decode.success(#(
        resolution_id,
        job_id,
        attempt_id,
        attempt_epoch,
        attempt_owner,
        expiry_retained,
        decision,
        resolved_by,
        details,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      resolution_id,
      job_id,
      attempt_id,
      attempt_epoch,
      attempt_owner,
      expiry_retained,
      decision,
      resolved_by,
      details,
    ),
  ] = audit.rows
  resolution_id |> should.equal("resolution-121")
  job_id |> should.equal(id)
  attempt_id |> should.equal(121)
  attempt_epoch |> should.equal(6)
  attempt_owner |> should.equal("lost-consumer")
  expiry_retained |> should.equal(True)
  decision |> should.equal("authorize_replay")
  resolved_by |> should.equal("on-call")
  details |> should.equal("confirm external idempotency record before replay")
  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(invocations, within: 0) |> should.equal(Ok(WorkerInvoked))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("resolved-12")))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-121",
      "on-call",
      "confirm external idempotency record before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))
  mark_database_test_executed("audited-uncertain-resolution-passed")
}

pub fn postgres_resolution_command_binds_typed_payload_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_resolution_payload_test(database_url)
  }
}

pub fn postgres_resolution_rebind_checks_storage_owner_test() {
  case resolution_route_a_url(), resolution_route_b_url() {
    Ok(database_a_url), Ok(database_b_url) ->
      run_resolution_rebind_route_test(database_a_url, database_b_url)
    _, _ -> Nil
  }
}

fn run_resolution_rebind_route_test(
  database_a_url: String,
  database_b_url: String,
) -> Nil {
  let assert Ok(settings_a) =
    postgres.settings(database_a_url) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_b_url) |> postgres.validate
  let assert Ok(settings_a_after_restart) =
    postgres.settings(database_a_url) |> postgres.validate
  let assert Ok(database_a) = postgres.start(settings_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(database_b) = postgres.start(settings_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(database_a_after_restart) =
    postgres.start(settings_a_after_restart)
  use <- exception.defer(fn() { postgres.close(database_a_after_restart) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(Nil) = postgres.migrate(database_b)
  let assert Ok(input_codec) =
    worker.codec("route-recovery-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("route-recovery-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("route.recovery", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(other_worker) =
    worker.define("route.other", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(wrong_input_codec) =
    worker.codec("route-recovery-input-v2", json.int, decode.int)
  let assert Ok(wrong_codec_worker) =
    worker.define(
      "route.recovery",
      "v1",
      wrong_input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle_a) =
    postgres.submit(database_a, "route-recovery", definition, 3)
  let assert Ok(handle_b) =
    postgres.submit(database_b, "route-recovery", definition, 3)
  let durable_id = job.id_value(handle_a)
  job.id_value(handle_b) |> should.equal(durable_id)
  let connection_a = postgres.connection(database_a)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 302, attempt_epoch = 5, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp() WHERE storage_owner = $1 AND id = $2",
    )
    |> pog.parameter(pog.text(postgres.storage_owner(database_a)))
    |> pog.parameter(pog.int(durable_id))
    |> pog.execute(on: connection_a)
  postgres.bind_handle(database_a_after_restart, other_worker, durable_id)
  |> should.equal(
    Error(postgres.WorkerContractMismatch(
      expected_id: "route.other",
      expected_version: "v1",
      actual_id: "route.recovery",
      actual_version: "v1",
    )),
  )
  postgres.bind_handle(database_a_after_restart, wrong_codec_worker, durable_id)
  |> should.equal(
    Error(postgres.CodecContractMismatch(
      kind: worker.InputCodec,
      expected: "route-recovery-input-v2",
      actual: "route-recovery-input-v1",
    )),
  )
  let rebound = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(
        rebound,
        postgres.bind_handle(database_a_after_restart, definition, durable_id),
      )
    })
  let assert Ok(Ok(recovered_handle)) = process.receive(rebound, within: 5000)
  postgres.state(database_a_after_restart, recovered_handle)
  |> should.equal(Ok(job.Uncertain))
  postgres.resolve_uncertain(
    database_a_after_restart,
    recovered_handle,
    postgres.ResolutionRequest(
      "same-id-different-store",
      "operator",
      "rebind after storage owner restart",
      postgres.ConfirmSuccess("approved"),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.resolve_uncertain(
    database_a_after_restart,
    handle_b,
    postgres.ResolutionRequest(
      "same-id-different-store",
      "operator",
      "rebind after storage owner restart",
      postgres.ConfirmSuccess("approved"),
    ),
  )
  |> should.equal(Error(postgres.ResolutionRouteMismatch))
  mark_database_test_executed("resolution-rebind-owner-checked")
}

fn run_resolution_payload_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolution-payload-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolution-payload-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define(
      "resolution.payload",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "resolution-payload", worker, 2)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 222, attempt_epoch = 3, attempt_owner = 'lost-payload-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolution-payload")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-payload-222",
      "on-call",
      "operator observed committed application key",
      postgres.ConfirmSuccess("approved"),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-payload-222",
      "on-call",
      "operator observed committed application key",
      postgres.ConfirmSuccess("different"),
    ),
  )
  |> should.equal(Error(postgres.ResolutionCommandConflict))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("approved")))
  mark_database_test_executed("resolution-payload-bound")
}

fn run_exact_expiry_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("exact-expiry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("exact-expiry-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define(
      "exact-expiry.echo",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) = postgres.submit(database, "exact-expiry", worker, 5)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = postgres.connection(database)
  let assert Ok(boundary) =
    pog.query(
      "WITH database_time AS MATERIALIZED (SELECT clock_timestamp() AS instant), boundary AS MATERIALIZED (UPDATE grind_jobs AS job SET lease_expires_at = database_time.instant FROM database_time WHERE job.id = $1 RETURNING job.lease_expires_at, database_time.instant) SELECT lease_expires_at = instant, "
      <> lease.live_lease_predicate("instant")
      <> ", "
      <> lease.expired_lease_predicate("instant")
      <> " FROM boundary",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use exact <- decode.field(0, decode.bool)
      use acknowledgement_allowed <- decode.field(1, decode.bool)
      use quarantine_eligible <- decode.field(2, decode.bool)
      decode.success(#(exact, acknowledgement_allowed, quarantine_eligible))
    })
    |> pog.execute(on: connection)
  let assert [#(exact, acknowledgement_allowed, quarantine_eligible)] =
    boundary.rows
  exact |> should.equal(True)
  acknowledgement_allowed |> should.equal(False)
  // At the exact boundary, `expired_lease_predicate` must be the complement
  // of `live_lease_predicate` (a strict `<=` and a strict `>` on the same
  // pair can never both be true or both be false), proving the quarantine
  // scan's own fragment agrees with the acknowledgement fragment on exactly
  // this tie instead of merely happening not to disagree elsewhere.
  quarantine_eligible |> should.equal(!acknowledgement_allowed)
  quarantine_eligible |> should.equal(True)
  mark_database_test_executed("exact-expiry-rejected")
}

/// Proves the same fenced-lease predicate rejects acknowledgement once the
/// lease has already expired by database time, on the *production*
/// acknowledgement path (the test above only exercises the SQL predicate
/// directly). The lease is deliberately much longer than this test's whole
/// run so no automatic renewal tick can fire and confuse the result with a
/// renewal-detected loss instead of the forced write below. Honest wording:
/// this proves the "lease already expired" side of the boundary on the real
/// `acknowledge_claim` path, not exact-instant equality — real time elapses
/// between the forced write below and the ack transaction's own later
/// `clock_timestamp()` call, so by the time production code evaluates the
/// predicate the lease is already in the past, not tied to it. Exact
/// equality at a single instant is what the predicate-only test above
/// proves, against this same shared fragment.
fn run_ack_after_database_expiry_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ack-after-expiry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ack-after-expiry-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "ack.after.expiry",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("after-expiry-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("ack-after-expiry")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let assert Ok(handle) =
    postgres.submit(database, "ack-after-expiry", slow_worker, 9)
  let job_id = job.id_value(handle)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(30_000)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let connection = postgres.connection(database)
  let assert Ok(#(attempt_id, epoch, _, Some(attempt_owner))) =
    attempt_snapshot(connection, job_id)

  // Tightest reachable forced expiry: the row's lease is set to the
  // database's own "now" rather than a value already further in the past.
  // The elapsed time between this UPDATE committing and the ack
  // transaction's own later clock_timestamp() call is what pushes the
  // lease into the past by the time production code evaluates it.
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  // No renewal tick has fired yet (lease_duration_ms / 3 is far outside this
  // test's whole run), so the coordinator's own renewal status is still
  // whatever the claim left it at. This confirms the ack rejection below
  // comes from the forced write, not from a renewal loss the coordinator
  // already detected on its own.
  queue.renewal_status(consumer)
  |> should.equal(Ok(Some(queue.LeaseRenewalConfirmed)))

  process.send(release, ReleaseAttempt)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
    proposed,
    postgres.AckLeaseExpired(stale_attempt_id, stale_epoch, stale_owner),
  )))) = process.receive(reply, within: 5000)
  proposed
  |> should.equal(worker.ExecutedSuccess(
    "ack-after-expiry-output-v1",
    "\"after-expiry-9\"",
  ))
  stale_attempt_id |> should.equal(attempt_id)
  stale_epoch |> should.equal(epoch)
  stale_owner |> should.equal(attempt_owner)

  let command_id = attempt.acknowledgement_command_id(job_id, attempt_id, epoch)
  let assert Ok(receipt_rows) =
    pog.query(
      "SELECT count(*) FROM grind_job_acknowledgements WHERE command_id = $1",
    )
    |> pog.parameter(pog.text(command_id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  let assert [0] = receipt_rows.rows

  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(Error(postgres.ReceiptNotFound))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(invoked, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed(
    "ack-after-database-expiry-stale-no-receipt-passed",
  )
}

fn run_bounded_quarantine_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("bounded-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("bounded-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define("bounded.echo", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("bounded-quarantine")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(first) =
    postgres.submit(database, "bounded-quarantine", worker, 1)
  let assert Ok(second) =
    postgres.submit(database, "bounded-quarantine", worker, 2)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'expired-owner', lease_expires_at = clock_timestamp(), attempt_count = 1, delivery_count = 1, cancel_requested_at = CASE WHEN input = '1'::jsonb THEN clock_timestamp() ELSE NULL END WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("bounded.echo"))
    |> pog.parameter(pog.text("bounded-quarantine"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  let states = [
    postgres.state(database, first),
    postgres.state(database, second),
  ]
  let uncertain_count =
    list.count(states, fn(state) { state == Ok(job.Uncertain) })
  uncertain_count |> should.equal(1)
  let executing_count =
    list.count(states, fn(state) { state == Ok(job.Executing) })
  executing_count |> should.equal(1)
  postgres.state(database, first) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, first)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired after cancellation request; prior effect unknown",
    )),
  )

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, second) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, second)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  mark_database_test_executed("quarantine-bounded-passed")
}

fn run_expired_attempt_quarantine_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("quarantine-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("quarantine-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(worker) =
    worker.define("quarantine.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("expired-quarantine")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(handle) =
    postgres.submit(database, "expired-quarantine", worker, 9)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'dead-consumer', lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("quarantine.echo"))
    |> pog.parameter(pog.text("expired-quarantine"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  postgres.arguments(database, handle) |> should.equal(Ok(9))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  let assert Ok(expired_attempt) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, lease_expires_at <= clock_timestamp(), failure_description, uncertain_at IS NOT NULL FROM grind_jobs WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("quarantine.echo"))
    |> pog.parameter(pog.text("expired-quarantine"))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use lease_expired <- decode.field(3, decode.bool)
      use reason <- decode.field(4, decode.string)
      use uncertainty_time_recorded <- decode.field(5, decode.bool)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        lease_expired,
        reason,
        uncertainty_time_recorded,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      attempt_id,
      attempt_epoch,
      attempt_owner,
      lease_expired,
      reason,
      uncertainty_time_recorded,
    ),
  ] = expired_attempt.rows
  attempt_id |> should.not_equal(0)
  attempt_epoch |> should.equal(1)
  attempt_owner |> should.equal("dead-consumer")
  lease_expired |> should.equal(True)
  reason |> should.equal("expired attempt requires outcome reconciliation")
  uncertainty_time_recorded |> should.equal(True)
  mark_database_test_executed("expired-attempt-quarantine-passed")
}

fn settle_attempt(
  finished: process.Subject(Nil),
  release: process.Subject(LeaseCommand),
  reply: process.Subject(Result(Bool, queue.ProcessError)),
) -> Nil {
  case process.receive(finished, within: 0) {
    Ok(Nil) -> Nil
    Error(Nil) -> {
      process.send(release, ReleaseAttempt)
      let _ = process.receive(reply, within: 5000)
      Nil
    }
  }
}

fn run_queue_batch_policy_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("batch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("batch-output-v1", json.int, decode.int)
  let assert Ok(increment) =
    worker.define("batch.increment", "v1", input_codec, output_codec, fn(value) {
      Ok(value + 1)
    })
  let assert Ok(workers) = registry.new("batch-policy")
  let assert Ok(workers) = registry.register(workers, increment)
  let assert Ok(first) = postgres.submit(database, "batch-policy", increment, 1)
  let assert Ok(second) =
    postgres.submit(database, "batch-policy", increment, 2)
  let assert Ok(third) = postgres.submit(database, "batch-policy", increment, 3)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_jobs_per_poll(2)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_available(consumer)
  |> should.equal(queue.BatchCompleted(2))
  postgres.state(database, first) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, second) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, third) |> should.equal(Ok(job.Queued))
  queue.process_available(consumer)
  |> should.equal(queue.BatchCompleted(1))
  postgres.state(database, third) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("queue-batch-policy-passed")
}

fn run_automatic_queue_fairness_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fairness-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fairness-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(incompatible) =
    worker.define("queue.drift", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(later) =
    worker.define("queue.later", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, LaterWorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("automatic-fairness")
  let assert Ok(workers) = registry.register(workers, incompatible)
  let assert Ok(workers) = registry.register(workers, later)
  let assert Ok(incompatible_handle) =
    postgres.submit(database, "automatic-fairness", incompatible, 1)
  let assert Ok(later_handle) =
    postgres.submit(database, "automatic-fairness", later, 2)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("fairness-output-v2"))
    |> pog.parameter(pog.text("queue.drift"))
    |> pog.execute(on: connection)
  let assert Ok(policy) = queue.default_policy() |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  process.receive(probe, within: 5000)
  |> should.equal(Ok(LaterWorkerInvoked))
  wait_for_job_state(database, incompatible_handle, job.ContractMismatch, 250)
  |> should.equal(True)
  wait_for_job_state(database, later_handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed("automatic-contract-skip-passed")
}

fn run_business_failure_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("lookup-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("lookup-output-v1", json.string, decode.string)
  let assert Ok(error_codec) =
    worker.codec(
      "lookup-error-v1",
      encode_lookup_failure,
      decode_lookup_failure(),
    )
  let assert Ok(lookup) =
    worker.define_with_error_codec(
      "accounts.lookup.failure",
      "v1",
      input_codec,
      output_codec,
      error_codec,
      fn(account_id) { Error(AccountMissing(account_id)) },
    )
  let assert Ok(lookup) = worker.with_max_attempts(lookup, 1)
  let assert Ok(workers) = registry.new("business-failures")
  let assert Ok(workers) = registry.register(workers, lookup)
  let assert Ok(handle) =
    postgres.submit(database, "business-failures", lookup, 42)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(42), worker.BudgetExhausted)),
  )
  postgres.state(database, handle)
  |> should.equal(Ok(job.BusinessFailed))
  mark_database_test_executed("typed-business-failure-passed")
}

fn run_worker_discard_outcome_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("discard-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("discard-output-v1", json.string, decode.string)
  let assert Ok(ordinary) =
    worker.define("worker.discard", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let discarding =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerDiscarded("not needed")
    })
  let assert Ok(workers) = registry.new("worker-discard")
  let assert Ok(workers) = registry.register(workers, discarding)
  let assert Ok(handle) =
    postgres.submit(database, "worker-discard", discarding, 8)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Discarded))
  let assert Ok(receipt) =
    pog.query(
      "SELECT job.failure_description, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256), job.error IS NULL, job.error_version IS NULL FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use failure_description <- decode.field(0, decode.optional(decode.string))
      use committed_state <- decode.field(1, decode.string)
      use failure_cause <- decode.field(2, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(3, decode.int)
      use error_empty <- decode.field(4, decode.bool)
      use error_version_empty <- decode.field(5, decode.bool)
      decode.success(#(
        failure_description,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        error_empty,
        error_version_empty,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(Some("not needed"), "discarded", None, 32, True, True)] =
    receipt.rows
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.DiscardedWithReason("not needed")))
  mark_database_test_executed("worker-discard-distinct-outcome-passed")
}

fn run_worker_cancel_outcome_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("worker-cancel-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("worker-cancel-output-v1", json.string, decode.string)
  let assert Ok(ordinary) =
    worker.define("worker.cancel", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let cancelling =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerCancelled("worker declined the job")
    })
  let assert Ok(workers) = registry.new("worker-cancel")
  let assert Ok(workers) = registry.register(workers, cancelling)
  let assert Ok(handle) =
    postgres.submit(database, "worker-cancel", cancelling, 8)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))
  let assert Ok(receipt) =
    pog.query(
      "SELECT job.failure_description, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256), job.error IS NULL, job.error_version IS NULL FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use failure_description <- decode.field(0, decode.optional(decode.string))
      use committed_state <- decode.field(1, decode.string)
      use failure_cause <- decode.field(2, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(3, decode.int)
      use error_empty <- decode.field(4, decode.bool)
      use error_version_empty <- decode.field(5, decode.bool)
      decode.success(#(
        failure_description,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        error_empty,
        error_version_empty,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(Some("worker declined the job"), "cancelled", None, 32, True, True),
  ] = receipt.rows
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("worker declined the job")))
  mark_database_test_executed("worker-cancel-distinct-outcome-passed")
}

fn run_worker_uncertainty_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("uncertain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("uncertain-output-v1", json.string, decode.string)
  let effect_probe = process.new_subject()
  let policy_probe = process.new_subject()
  let assert Ok(ordinary) =
    worker.define(
      "worker.effect.uncertain",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(effect_probe, WorkerInvoked)
        Error(AccountMissing(value))
      },
    )
  let assert Ok(limited) = worker.with_max_attempts(ordinary, 2)
  let assert Ok(retry_delay) = worker.retry_delay(1000)
  let retry_policy =
    worker.retry_policy(fn(failure, context) {
      case failure {
        worker.BusinessFailure(_) -> {
          let worker.RetryContext(current_attempt:, ..) = context
          process.send(policy_probe, RetryPolicyInvoked(current_attempt, 9))
          worker.RetryAfter(retry_delay)
        }
      }
    })
  let with_policy = worker.with_retry_policy(limited, retry_policy)
  let uncertain =
    worker.with_queue_handler(with_policy, fn(_) {
      process.send(effect_probe, WorkerInvoked)
      worker.WorkerUncertain("external effect may have completed")
    })
  let assert Ok(workers) = registry.new("worker-uncertainty")
  let assert Ok(workers) = registry.register(workers, uncertain)
  let assert Ok(handle) =
    postgres.submit(database, "worker-uncertainty", uncertain, 17)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(effect_probe, within: 0) |> should.equal(Ok(WorkerInvoked))
  process.receive(policy_probe, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired("external effect may have completed")),
  )
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.AlreadyUncertain))
  let assert Ok(evidence) =
    pog.query(
      "SELECT job.attempt_count, job.max_attempts, job.delivery_count, job.attempt_id IS NOT NULL, job.attempt_owner IS NOT NULL, receipt.command_id, receipt.attempt_id, receipt.attempt_epoch, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256), job.error IS NULL, job.error_version IS NULL FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use max_attempts <- decode.field(1, decode.int)
      use delivery_count <- decode.field(2, decode.int)
      use has_attempt_id <- decode.field(3, decode.bool)
      use has_attempt_owner <- decode.field(4, decode.bool)
      use command_id <- decode.field(5, decode.string)
      use attempt_id <- decode.field(6, decode.int)
      use attempt_epoch <- decode.field(7, decode.int)
      use committed_state <- decode.field(8, decode.string)
      use failure_cause <- decode.field(9, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(10, decode.int)
      use no_error <- decode.field(11, decode.bool)
      use no_error_version <- decode.field(12, decode.bool)
      decode.success(#(
        attempt_count,
        max_attempts,
        delivery_count,
        has_attempt_id,
        has_attempt_owner,
        command_id,
        attempt_id,
        attempt_epoch,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        no_error,
        no_error_version,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(
      1,
      2,
      1,
      True,
      True,
      command_id,
      attempt_id,
      attempt_epoch,
      "uncertain",
      None,
      32,
      True,
      True,
    ),
  ] = evidence.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: receipt_command,
    attempt_id: receipt_attempt,
    attempt_epoch: receipt_epoch,
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at_unix_ms: _,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_command |> should.equal(command_id)
  receipt_attempt |> should.equal(attempt_id)
  receipt_epoch |> should.equal(attempt_epoch)
  receipt_state |> should.equal(job.Uncertain)
  receipt_cause |> should.equal(None)
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(effect_probe, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("worker-uncertainty-reconciliable-no-retry")
}

fn run_cancel_before_execution_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-before-run-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-before-run-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "worker.cancel.before.run",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(probe, WorkerInvoked)
        Ok(int.to_string(value))
      },
    )
  let assert Ok(workers) = registry.new("cancel-before-run")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cancel-before-run", definition, 5)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.AlreadyCancelled))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  let assert Ok(accounting) =
    pog.query(
      "SELECT attempt_count, delivery_count, cancel_requested_at IS NULL, (SELECT count(*) = 0 FROM grind_job_acknowledgements WHERE job_id = $1) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use delivery_count <- decode.field(1, decode.int)
      use no_cancel_request <- decode.field(2, decode.bool)
      use no_ack <- decode.field(3, decode.bool)
      decode.success(#(attempt_count, delivery_count, no_cancel_request, no_ack))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(0, 0, True, True)] = accounting.rows
  mark_database_test_executed("cancel-before-run-committed")
}

fn run_cancel_after_completion_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-complete-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-complete-output-v1", json.string, decode.string)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "worker.cancel.complete",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(invoked, WorkerInvoked)
        Ok("done-" <> int.to_string(value))
      },
    )
  let assert Ok(workers) = registry.new("cancel-after-completion")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cancel-after-completion", definition, 12)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(invoked, within: 0) |> should.equal(Ok(WorkerInvoked))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.AlreadyFinished(job.Succeeded)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("done-12")))
  mark_database_test_executed("cancel-after-completion-preserved")
}

fn run_cancel_running_ack_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-running-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-running-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "worker.cancel.running",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) ->
            Ok("completed-despite-cancel-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("cancel-running")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cancel-running", definition, 9)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })

  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  let assert Ok(receipt) =
    pog.query(
      "SELECT command_id, attempt_id, attempt_epoch, committed_state, failure_cause, octet_length(proposal_sha256) FROM grind_job_acknowledgements WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use command_id <- decode.field(0, decode.string)
      use attempt_id <- decode.field(1, decode.int)
      use attempt_epoch <- decode.field(2, decode.int)
      use committed_state <- decode.field(3, decode.string)
      use failure_cause <- decode.field(4, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(5, decode.int)
      decode.success(#(
        command_id,
        attempt_id,
        attempt_epoch,
        committed_state,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(command_id, attempt_id, attempt_epoch, "cancelled", None, 32)] =
    receipt.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: receipt_command,
    attempt_id: receipt_attempt,
    attempt_epoch: receipt_epoch,
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at_unix_ms: committed_at,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_command |> should.equal(command_id)
  receipt_attempt |> should.equal(attempt_id)
  receipt_epoch |> should.equal(attempt_epoch)
  receipt_state |> should.equal(job.Cancelled)
  receipt_cause |> should.equal(None)
  should.be_true(committed_at > 0)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET failure_description = 'changed current job diagnostic' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: postgres.connection(database))
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(
    Ok(postgres.AcknowledgementReceipt(
      command_id: receipt_command,
      attempt_id: receipt_attempt,
      attempt_epoch: receipt_epoch,
      committed_state: receipt_state,
      business_failure_cause: receipt_cause,
      committed_at_unix_ms: committed_at,
    )),
  )
  mark_database_test_executed("cancel-running-ack-wins")
}

fn run_cancel_running_uncertain_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-uncertain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-uncertain-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(ordinary) =
    worker.define(
      "worker.cancel.uncertain",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("ordinary-" <> int.to_string(value)) },
    )
  let definition =
    worker.with_queue_handler(ordinary, fn(value) {
      let release = process.new_subject()
      process.send(started, LongHandlerStarted(release))
      let _ = process.receive(release, within: 10_000)
      case value {
        13 ->
          worker.WorkerUncertain("effect may have happened before cancellation")
        14 -> worker.WorkerCancelled("worker proposed its own cancellation")
        _ -> worker.WorkerUncertain("unexpected cancellation-test input")
      }
    })
  let assert Ok(workers) = registry.new("cancel-running-uncertain")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "cancel-running-uncertain", definition, 13)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  let assert Ok(evidence) =
    pog.query(
      "SELECT receipt.command_id, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256), job.cancel_requested_at IS NULL, job.uncertain_at IS NULL FROM grind_job_acknowledgements AS receipt JOIN grind_jobs AS job ON job.id = receipt.job_id WHERE receipt.job_id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use command_id <- decode.field(0, decode.string)
      use committed_state <- decode.field(1, decode.string)
      use failure_cause <- decode.field(2, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(3, decode.int)
      use request_cleared <- decode.field(4, decode.bool)
      use uncertainty_cleared <- decode.field(5, decode.bool)
      decode.success(#(
        command_id,
        committed_state,
        failure_cause,
        fingerprint_bytes,
        request_cleared,
        uncertainty_cleared,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(command_id, "cancelled", None, 32, True, True)] = evidence.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    committed_state: receipt_state,
    business_failure_cause: receipt_cause,
    committed_at_unix_ms: _,
    ..,
  )) = postgres.reconcile_acknowledgement(database, handle, command_id)
  receipt_state |> should.equal(job.Cancelled)
  receipt_cause |> should.equal(None)

  let assert Ok(worker_cancel_handle) =
    postgres.submit(database, "cancel-running-uncertain", definition, 14)
  let worker_cancel_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(worker_cancel_reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(worker_cancel_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(worker_cancel_release, ReleaseAttempt)
  })
  postgres.cancel(database, worker_cancel_handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(worker_cancel_release, ReleaseAttempt)
  process.receive(worker_cancel_reply, within: 5000)
  |> should.equal(Ok(Ok(True)))
  postgres.outcome(database, worker_cancel_handle)
  |> should.equal(Ok(job.CancelledWithReason("cancelled by caller")))
  let assert Ok(worker_cancel_command) =
    pog.query(
      "SELECT command_id FROM grind_job_acknowledgements WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(worker_cancel_handle)))
    |> pog.returning({
      use command_id <- decode.field(0, decode.string)
      decode.success(command_id)
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [worker_cancel_command_id] = worker_cancel_command.rows
  let assert Ok(postgres.AcknowledgementReceipt(
    committed_state: worker_cancel_state,
    business_failure_cause: worker_cancel_cause,
    ..,
  )) =
    postgres.reconcile_acknowledgement(
      database,
      worker_cancel_handle,
      worker_cancel_command_id,
    )
  worker_cancel_state |> should.equal(job.Cancelled)
  worker_cancel_cause |> should.equal(None)
  mark_database_test_executed("cancel-running-worker-cancel-compact-receipt")
  mark_database_test_executed("cancel-running-uncertain-compact-receipt")
}

fn run_cancelled_expired_attempt_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancel-expired-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancel-expired-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(ordinary) =
    worker.define(
      "worker.cancel.expired.replay",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let definition =
    worker.with_queue_handler(ordinary, fn(value) {
      let release = process.new_subject()
      process.send(started, LongHandlerStarted(release))
      let _ = process.receive(release, within: 10_000)
      worker.WorkerSucceeded(
        "effect-completed-after-cancel-" <> int.to_string(value),
      )
    })
  let queue_name = "cancel-expired"
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) = postgres.submit(database, queue_name, definition, 11)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing' AND cancel_requested_at IS NOT NULL",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: postgres.connection(database))

  process.send(release, ReleaseAttempt)
  let ack_result = process.receive(reply, within: 5000)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
    worker.ExecutedSuccess(_, encoded_output),
    postgres.AckLeaseExpired(_, _, _),
  )))) = ack_result
  encoded_output
  |> should.equal("\"effect-completed-after-cancel-11\"")
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired after cancellation request; prior effect unknown",
    )),
  )
  let assert Ok(row) =
    pog.query(
      "SELECT state, attempt_id, attempt_epoch, attempt_owner, attempt_count, delivery_count, cancel_requested_at IS NOT NULL, failure_description, (SELECT count(*) = 0 FROM grind_job_acknowledgements WHERE job_id = $1) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_id <- decode.field(1, decode.int)
      use attempt_epoch <- decode.field(2, decode.int)
      use attempt_owner <- decode.field(3, decode.string)
      use attempt_count <- decode.field(4, decode.int)
      use delivery_count <- decode.field(5, decode.int)
      use cancel_requested <- decode.field(6, decode.bool)
      use description <- decode.field(7, decode.string)
      use no_ack_receipt <- decode.field(8, decode.bool)
      decode.success(#(
        state,
        attempt_id,
        attempt_epoch,
        attempt_owner,
        attempt_count,
        delivery_count,
        cancel_requested,
        description,
        no_ack_receipt,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(
      "uncertain",
      attempt_id,
      attempt_epoch,
      attempt_owner,
      1,
      1,
      True,
      description,
      True,
    ),
  ] = row.rows
  attempt_id |> should.not_equal(0)
  attempt_epoch |> should.not_equal(0)
  attempt_owner |> should.not_equal("")
  description
  |> should.equal("expired after cancellation request; prior effect unknown")
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "cancel-pending-replay",
      "on-call",
      "the cancellation request blocks replay until effect evidence is reviewed",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Error(postgres.ResolutionCancellationPending))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  let assert Ok(no_audit) =
    pog.query(
      "SELECT count(*) FROM grind_job_resolutions WHERE job_id = $1 AND resolution_id = $2",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.parameter(pog.text("cancel-pending-replay"))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [0] = no_audit.rows
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "cancel-pending-confirmed",
      "on-call",
      "external effect evidence confirms the known result",
      postgres.ConfirmSuccess("confirmed-without-replay"),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("confirmed-without-replay")))
  mark_database_test_executed("cancel-pending-expiry-quarantined")
}

fn run_worker_snooze_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("snooze-output-v1", json.string, decode.string)
  let assert Ok(delay) = worker.retry_delay(60_000)
  let ordinary_probe = process.new_subject()
  let assert Ok(ordinary) =
    worker.define("worker.snooze", "v1", input_codec, output_codec, fn(_) {
      process.send(ordinary_probe, WorkerInvoked)
      Error(AccountMissing(1))
    })
  let queue_probe = process.new_subject()
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      process.send(queue_probe, LaterWorkerInvoked)
      worker.WorkerSnoozed(delay, "awaiting external account")
    })
  let assert Ok(workers) = registry.new("snoozes")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) = postgres.submit(database, "snoozes", snoozing, 1)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let before_ack_ms = database_time_milliseconds(postgres.connection(database))
  queue.process_one(consumer) |> should.equal(Ok(True))
  let after_ack_ms = database_time_milliseconds(postgres.connection(database))
  process.receive(queue_probe, within: 0)
  |> should.equal(Ok(LaterWorkerInvoked))
  process.receive(queue_probe, within: 0) |> should.equal(Error(Nil))
  process.receive(ordinary_probe, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.Pending(job.Scheduled)))
  let assert Ok(snooze_evidence) =
    pog.query(
      "SELECT job.attempt_count, job.snooze_count, floor(extract(epoch FROM job.available_at) * 1000)::bigint, floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use snooze_count <- decode.field(1, decode.int)
      use available_at_ms <- decode.field(2, decode.int)
      use sampled_now_ms <- decode.field(3, decode.int)
      use committed_state <- decode.field(4, decode.string)
      use failure_cause <- decode.field(5, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(6, decode.int)
      decode.success(#(
        attempt_count,
        snooze_count,
        available_at_ms,
        sampled_now_ms,
        committed_state,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(
      attempt_count,
      snooze_count,
      available_at_ms,
      sampled_now_ms,
      "scheduled",
      None,
      32,
    ),
  ] = snooze_evidence.rows
  attempt_count |> should.equal(0)
  snooze_count |> should.equal(1)
  should.be_true(available_at_ms >= before_ack_ms + 60_000)
  should.be_true(available_at_ms <= after_ack_ms + 60_000)
  should.be_true(sampled_now_ms >= after_ack_ms)
  queue.process_one(consumer) |> should.equal(Ok(False))
  mark_database_test_executed("worker-snooze-scheduled-passed")
}

fn database_time_milliseconds(connection: pog.Connection) -> Int {
  let assert Ok(sample) =
    pog.query(
      "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
    )
    |> pog.returning({
      use milliseconds <- decode.field(0, decode.int)
      decode.success(milliseconds)
    })
    |> pog.execute(on: connection)
  let assert [milliseconds] = sample.rows
  milliseconds
}

pub fn postgres_worker_snooze_receipt_write_failure_rolls_back_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_snooze_receipt_rollback_test(database_url)
  }
}

fn run_snooze_receipt_rollback_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let connection = postgres.connection(database)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_reject_snooze_receipt ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_reject_snooze_receipt()")
      |> pog.execute(on: connection)
    let _ = postgres.close(database)
  })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-rollback-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("snooze-rollback-output-v1", json.string, decode.string)
  let assert Ok(delay) = worker.retry_delay(60_000)
  let assert Ok(ordinary) =
    worker.define(
      "worker.snooze.rollback",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerSnoozed(delay, "rollback receipt test")
    })
  let assert Ok(workers) = registry.new("snooze-rollback")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) =
    postgres.submit(database, "snooze-rollback", snoozing, 8)
  let attempt_owner = "snooze-rollback-owner"
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "snooze-rollback",
      workers,
      attempt_owner,
      30_000,
    )
  let proposed = attempt.execute_claim(claimed)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_reject_snooze_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.committed_state = 'scheduled' THEN RAISE EXCEPTION 'injected snooze receipt failure'; END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_reject_snooze_receipt BEFORE INSERT ON grind_job_acknowledgements FOR EACH ROW EXECUTE FUNCTION grind_test_reject_snooze_receipt()",
    )
    |> pog.execute(on: connection)
  let acknowledgement_failed = case
    attempt.acknowledge(
      database,
      "snooze-rollback",
      attempt_owner,
      claimed,
      proposed,
    )
  {
    Error(_) -> True
    Ok(_) -> False
  }
  acknowledgement_failed |> should.equal(True)
  let assert Ok(state_after_rollback) =
    pog.query(
      "SELECT state, attempt_count, snooze_count, (SELECT count(*) = 0 FROM grind_job_acknowledgements WHERE job_id = $1) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_count <- decode.field(1, decode.int)
      use snooze_count <- decode.field(2, decode.int)
      use no_receipt <- decode.field(3, decode.bool)
      decode.success(#(state, attempt_count, snooze_count, no_receipt))
    })
    |> pog.execute(on: connection)
  let assert [#("executing", 1, 0, True)] = state_after_rollback.rows
  mark_database_test_executed("worker-snooze-receipt-rollback-passed")
}

pub fn postgres_worker_snooze_ack_receipt_binds_delay_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_snooze_delay_receipt_test(database_url)
  }
}

pub fn postgres_snooze_after_audited_replay_refunds_current_attempt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_snooze_after_replay_test(database_url)
  }
}

fn run_snooze_after_replay_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-replay-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("snooze-replay-output-v1", json.string, decode.string)
  let assert Ok(delay) = worker.retry_delay(0)
  let ordinary_probe = process.new_subject()
  let queue_probe = process.new_subject()
  let assert Ok(ordinary) =
    worker.define(
      "worker.snooze.replay",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        process.send(ordinary_probe, value)
        Ok(int.to_string(value))
      },
    )
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      let release = process.new_subject()
      process.send(queue_probe, LongHandlerStarted(release))
      let _ = process.receive(release, within: 10_000)
      worker.WorkerSnoozed(delay, "audited replay snooze")
    })
  let assert Ok(workers) = registry.new("snooze-audited-replay")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) =
    postgres.submit(database, "snooze-audited-replay", snoozing, 8)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 9, attempt_owner = 'expired-snooze-owner', lease_expires_at = clock_timestamp(), attempt_count = 1, delivery_count = 1 WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "snooze-audited-replay",
      "on-call",
      "inspect the prior effect before authorizing a new delivery",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(before_claim) =
    pog.query(
      "SELECT attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use delivery_count <- decode.field(1, decode.int)
      decode.success(#(attempt_count, delivery_count))
    })
    |> pog.execute(on: connection)
  before_claim.rows |> should.equal([#(1, 1)])

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(queue_probe, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })
  let assert Ok(during_attempt) =
    pog.query(
      "SELECT attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use delivery_count <- decode.field(1, decode.int)
      decode.success(#(attempt_count, delivery_count))
    })
    |> pog.execute(on: connection)
  during_attempt.rows |> should.equal([#(2, 2)])

  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.receive(ordinary_probe, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.Pending(job.Scheduled)))
  let assert Ok(evidence) =
    pog.query(
      "SELECT attempt_count, max_attempts, delivery_count, snooze_count, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use max_attempts <- decode.field(1, decode.int)
      use delivery_count <- decode.field(2, decode.int)
      use snooze_count <- decode.field(3, decode.int)
      use committed <- decode.field(4, decode.string)
      use failure_cause <- decode.field(5, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(6, decode.int)
      decode.success(#(
        attempt_count,
        max_attempts,
        delivery_count,
        snooze_count,
        committed,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: connection)
  evidence.rows |> should.equal([#(1, 20, 2, 1, "scheduled", None, 32)])
  mark_database_test_executed(
    "worker-snooze-audited-replay-refunds-current-attempt",
  )
}

pub fn postgres_business_failure_is_scheduled_before_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_business_retry_test(database_url)
  }
}

pub fn postgres_default_retry_backoff_is_persisted_at_database_time_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_default_retry_backoff_test(database_url)
  }
}

pub fn postgres_retry_delay_maximum_commits_without_precision_loss_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_retry_delay_maximum_test(database_url)
  }
}

fn run_default_retry_backoff_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("default-retry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("default-retry-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "worker.default.retry",
      "v1",
      input_codec,
      output_codec,
      fn(_) { Error(AccountMissing(71)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "default-retry", definition, 1)
  let assert Ok(workers) = registry.new("default-retry")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let connection = postgres.connection(database)
  let before_ack_us = database_time_microseconds(connection)

  queue.process_one(consumer) |> should.equal(Ok(True))

  let after_ack_us = database_time_microseconds(connection)
  postgres.state(database, handle) |> should.equal(Ok(job.Retryable))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.Pending(job.Retryable)))
  let assert Ok(evidence) =
    pog.query(
      "SELECT state, attempt_count, max_attempts, delivery_count, floor(extract(epoch FROM available_at) * 1000000)::bigint, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_count <- decode.field(1, decode.int)
      use max_attempts <- decode.field(2, decode.int)
      use delivery_count <- decode.field(3, decode.int)
      use available_at_us <- decode.field(4, decode.int)
      use committed <- decode.field(5, decode.string)
      use failure_cause <- decode.field(6, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(7, decode.int)
      decode.success(#(
        state,
        attempt_count,
        max_attempts,
        delivery_count,
        available_at_us,
        committed,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#("retryable", 1, 20, 1, available_at_us, "retryable", None, 32)] =
    evidence.rows
  should.be_true(available_at_us >= before_ack_us + 15_000_000)
  should.be_true(available_at_us <= after_ack_us + 15_000_000)
  queue.process_one(consumer) |> should.equal(Ok(False))
  mark_database_test_executed("default-retry-backoff-database-time-passed")
}

fn run_retry_delay_maximum_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("maximum-delay-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("maximum-delay-output-v1", json.string, decode.string)
  let maximum_delay_ms = worker.retry_delay_maximum_milliseconds()
  let assert Ok(delay) = worker.retry_delay(maximum_delay_ms)
  let assert Ok(ordinary) =
    worker.define(
      "worker.maximum.delay",
      "v1",
      input_codec,
      output_codec,
      fn(_) { Ok("ordinary path unused") },
    )
  let definition =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerSnoozed(delay, "maximum supported delay")
    })
  let assert Ok(handle) =
    postgres.submit(database, "maximum-delay", definition, 1)
  let assert Ok(workers) = registry.new("maximum-delay")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let connection = postgres.connection(database)
  let before_ack_us = database_time_microseconds(connection)

  queue.process_one(consumer) |> should.equal(Ok(True))

  let after_ack_us = database_time_microseconds(connection)
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let assert Ok(evidence) =
    pog.query(
      "SELECT floor(extract(epoch FROM job.available_at) * 1000000)::bigint, receipt.committed_state, receipt.failure_cause, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use available_at_us <- decode.field(0, decode.int)
      use committed <- decode.field(1, decode.string)
      use failure_cause <- decode.field(2, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(3, decode.int)
      decode.success(#(
        available_at_us,
        committed,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: connection)
  let assert [#(available_at_us, "scheduled", None, 32)] = evidence.rows
  let delay_us = maximum_delay_ms * 1000
  should.be_true(available_at_us >= before_ack_us + delay_us)
  should.be_true(available_at_us <= after_ack_us + delay_us)
  mark_database_test_executed("retry-delay-maximum-postgres-ack-passed")
}

fn database_time_microseconds(connection: pog.Connection) -> Int {
  let assert Ok(sample) =
    pog.query(
      "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000000)::bigint",
    )
    |> pog.returning({
      use microseconds <- decode.field(0, decode.int)
      decode.success(microseconds)
    })
    |> pog.execute(on: connection)
  let assert [microseconds] = sample.rows
  microseconds
}

pub fn postgres_retry_policy_can_decline_without_an_error_codec_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_retry_declined_without_error_codec_test(database_url)
  }
}

fn run_retry_declined_without_error_codec_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("retry-declined-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("retry-declined-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "worker.retry.declined",
      "v1",
      input_codec,
      output_codec,
      fn(_) { Error(AccountMissing(91)) },
    )
  let policy_calls = process.new_subject()
  let policy =
    worker.retry_policy(fn(failure, _) {
      case failure {
        worker.BusinessFailure(AccountMissing(account_id)) ->
          process.send(policy_calls, account_id)
      }
      worker.DoNotRetry
    })
  let assert Ok(limited) = worker.with_max_attempts(definition, 2)
  let limited = worker.with_retry_policy(limited, policy)
  let assert Ok(error_codec) =
    worker.codec(
      "retry-declined-error-v1",
      encode_lookup_failure,
      decode_lookup_failure(),
    )
  let assert Ok(typed_definition) =
    worker.define_with_error_codec(
      "worker.retry.declined.typed",
      "v1",
      input_codec,
      output_codec,
      error_codec,
      fn(_) { Error(AccountMissing(92)) },
    )
  let assert Ok(typed_limited) = worker.with_max_attempts(typed_definition, 2)
  let typed_limited = worker.with_retry_policy(typed_limited, policy)
  let assert Ok(workers) = registry.new("retry-declined")
  let assert Ok(workers) = registry.register(workers, limited)
  let assert Ok(workers) = registry.register(workers, typed_limited)
  let assert Ok(handle) =
    postgres.submit(database, "retry-declined", limited, 3)
  let assert Ok(typed_handle) =
    postgres.submit(database, "retry-declined", typed_limited, 4)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(policy_calls, within: 0) |> should.equal(Ok(91))
  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.FailedOperationallyWithCause(
      "worker returned an application error",
      worker.RetryDeclined,
    )),
  )
  process.receive(policy_calls, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(policy_calls, within: 0) |> should.equal(Ok(92))
  postgres.outcome(database, typed_handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(92), worker.RetryDeclined)),
  )
  process.receive(policy_calls, within: 0) |> should.equal(Error(Nil))
  let assert Ok(committed_failure) =
    pog.query(
      "SELECT attempt_count, max_attempts, delivery_count, failure_cause, error IS NULL, error_version IS NULL FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use max_attempts <- decode.field(1, decode.int)
      use delivery_count <- decode.field(2, decode.int)
      use cause <- decode.field(3, decode.optional(decode.string))
      use no_error <- decode.field(4, decode.bool)
      use no_error_version <- decode.field(5, decode.bool)
      decode.success(#(
        attempt_count,
        max_attempts,
        delivery_count,
        cause,
        no_error,
        no_error_version,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  committed_failure.rows
  |> should.equal([#(1, 2, 1, Some("retry_declined"), True, True)])
  let assert Ok(typed_failure) =
    pog.query(
      "SELECT failure_cause, error IS NOT NULL, error_version FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(typed_handle)))
    |> pog.returning({
      use cause <- decode.field(0, decode.optional(decode.string))
      use has_error <- decode.field(1, decode.bool)
      use error_version <- decode.field(2, decode.optional(decode.string))
      decode.success(#(cause, has_error, error_version))
    })
    |> pog.execute(on: postgres.connection(database))
  typed_failure.rows
  |> should.equal([
    #(Some("retry_declined"), True, Some("retry-declined-error-v1")),
  ])
  mark_database_test_executed("worker-retry-declined-without-error-codec")
}

fn run_business_retry_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("business-retry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("business-retry-output-v1", json.string, decode.string)
  let assert Ok(error_codec) =
    worker.codec(
      "business-retry-error-v1",
      encode_lookup_failure,
      decode_lookup_failure(),
    )
  let assert Ok(delay) = worker.retry_delay(60_000)
  let assert Ok(definition) =
    worker.define_with_error_codec(
      "worker.business.retry",
      "v1",
      input_codec,
      output_codec,
      error_codec,
      fn(_) { Error(AccountMissing(42)) },
    )
  let retry_probe = process.new_subject()
  let policy =
    worker.retry_policy(fn(failure, context) {
      case failure {
        worker.BusinessFailure(AccountMissing(account_id)) -> {
          let worker.RetryContext(current_attempt:, ..) = context
          process.send(
            retry_probe,
            RetryPolicyInvoked(current_attempt, account_id),
          )
          worker.RetryAfter(delay)
        }
      }
    })
  let assert Ok(retrying) = worker.with_max_attempts(definition, 2)
  let retrying = worker.with_retry_policy(retrying, policy)
  let assert Ok(workers) = registry.new("business-retry")
  let assert Ok(workers) = registry.register(workers, retrying)
  let assert Ok(handle) =
    postgres.submit(database, "business-retry", retrying, 17)
  let fail_next_worker_start = one_shot.new()
  let hooks =
    consumer_hooks.Hooks(
      before_worker_start: fn() {
        case one_shot.take(fail_next_worker_start) {
          True -> Error("injected start failure")
          False -> Ok(Nil)
        }
      },
      after_worker_start: fn(_pid) { Nil },
    )
  let assert Ok(consumer) =
    queue.start_with_hooks(database, workers, manual_policy(), hooks)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Retryable))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.Pending(job.Retryable)))
  process.receive(retry_probe, within: 0)
  |> should.equal(Ok(RetryPolicyInvoked(1, 42)))
  let assert Ok(first_attempt) =
    pog.query(
      "SELECT state, attempt_count, max_attempts, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_count <- decode.field(1, decode.int)
      use max_attempts <- decode.field(2, decode.int)
      use delivery_count <- decode.field(3, decode.int)
      decode.success(#(state, attempt_count, max_attempts, delivery_count))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#("retryable", 1, 2, 1)] = first_attempt.rows
  let before_due = database_time_milliseconds(postgres.connection(database))
  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(retry_probe, within: 0) |> should.equal(Error(Nil))

  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: postgres.connection(database))
  one_shot.arm(fail_next_worker_start)
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueWorkerStartFailed(actor.InitFailed("injected start failure")),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Retryable))
  let assert Ok(after_unstarted_retry) =
    pog.query(
      "SELECT attempt_count, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use delivery_count <- decode.field(1, decode.int)
      decode.success(#(attempt_count, delivery_count))
    })
    |> pog.execute(on: postgres.connection(database))
  after_unstarted_retry.rows |> should.equal([#(1, 2)])
  process.receive(retry_probe, within: 0) |> should.equal(Error(Nil))
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.BusinessFailedWithCause(AccountMissing(42), worker.BudgetExhausted)),
  )
  process.receive(retry_probe, within: 0) |> should.equal(Error(Nil))
  let assert Ok(attempt_receipts) =
    pog.query(
      "SELECT attempt_id, command_id, committed_state, failure_cause, octet_length(proposal_sha256) FROM grind_job_acknowledgements WHERE job_id = $1 ORDER BY attempt_id",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use command_id <- decode.field(1, decode.string)
      use committed <- decode.field(2, decode.string)
      use failure_cause <- decode.field(3, decode.optional(decode.string))
      use fingerprint_bytes <- decode.field(4, decode.int)
      decode.success(#(
        attempt_id,
        command_id,
        committed,
        failure_cause,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [
    #(first_attempt, first_command_id, "retryable", None, 32),
    #(second_attempt, _, "business_failed", Some("budget_exhausted"), 32),
  ] = attempt_receipts.rows
  should.be_true(first_attempt < second_attempt)
  let assert Ok(postgres.AcknowledgementReceipt(
    command_id: reconciled_command,
    attempt_id: reconciled_attempt,
    committed_state: reconciled_state,
    business_failure_cause: reconciled_cause,
    ..,
  )) = postgres.reconcile_acknowledgement(database, handle, first_command_id)
  reconciled_command |> should.equal(first_command_id)
  reconciled_attempt |> should.equal(first_attempt)
  reconciled_state |> should.equal(job.Retryable)
  reconciled_cause |> should.equal(None)
  let assert Ok(counters) =
    pog.query(
      "SELECT attempt_count, max_attempts, delivery_count FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use attempt_count <- decode.field(0, decode.int)
      use max_attempts <- decode.field(1, decode.int)
      use delivery_count <- decode.field(2, decode.int)
      decode.success(#(attempt_count, max_attempts, delivery_count))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(2, 2, 3)] = counters.rows
  should.be_true(before_due > 0)
  mark_database_test_executed("worker-retry-first-attempt-scheduled")
}

fn run_snooze_delay_receipt_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("snooze-delay-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("snooze-delay-output-v1", json.string, decode.string)
  let assert Ok(delay) = worker.retry_delay(60_000)
  let assert Ok(ordinary) =
    worker.define(
      "worker.snooze.delay.receipt",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerSnoozed(delay, "receipt payload conflict")
    })
  let assert Ok(workers) = registry.new("snooze-delay-receipt")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) =
    postgres.submit(database, "snooze-delay-receipt", snoozing, 9)
  let attempt_owner = "snooze-delay-owner"
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "snooze-delay-receipt",
      workers,
      attempt_owner,
      30_000,
    )
  let proposal = worker.ExecutedSnoozed(60_000, "receipt payload conflict")
  attempt.acknowledge(
    database,
    "snooze-delay-receipt",
    attempt_owner,
    claimed,
    proposal,
  )
  |> should.equal(Ok(True))
  attempt.acknowledge(
    database,
    "snooze-delay-receipt",
    attempt_owner,
    claimed,
    worker.ExecutedSnoozed(70_000, "receipt payload conflict"),
  )
  |> should.equal(Error(postgres.QueueAckCommandConflict))
  attempt.acknowledge(
    database,
    "snooze-delay-receipt",
    attempt_owner,
    claimed,
    worker.ExecutedSnoozed(60_000, "changed proposal reason"),
  )
  |> should.equal(Error(postgres.QueueAckCommandConflict))
  let assert Ok(receipt) =
    pog.query(
      "SELECT state, attempt_count, snooze_count, receipt.committed_state, octet_length(receipt.proposal_sha256) FROM grind_jobs AS job JOIN grind_job_acknowledgements AS receipt ON receipt.job_id = job.id WHERE job.id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use attempt_count <- decode.field(1, decode.int)
      use snooze_count <- decode.field(2, decode.int)
      use committed_state <- decode.field(3, decode.string)
      use fingerprint_bytes <- decode.field(4, decode.int)
      decode.success(#(
        state,
        attempt_count,
        snooze_count,
        committed_state,
        fingerprint_bytes,
      ))
    })
    |> pog.execute(on: postgres.connection(database))
  receipt.rows |> should.equal([#("scheduled", 0, 1, "scheduled", 32)])
  mark_database_test_executed("worker-snooze-delay-receipt-conflict-passed")
}

fn encode_lookup_failure(error: LookupFailure) -> json.Json {
  case error {
    AccountMissing(account_id) ->
      json.object([
        #("kind", json.string("account_missing")),
        #("account_id", json.int(account_id)),
      ])
  }
}

fn decode_lookup_failure() -> decode.Decoder(LookupFailure) {
  use kind <- decode.field("kind", decode.string)
  use account_id <- decode.field("account_id", decode.int)
  case kind {
    "account_missing" -> decode.success(AccountMissing(account_id))
    _ -> decode.failure(AccountMissing(account_id), "known lookup failure kind")
  }
}

fn run_output_codec_mismatch_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("mismatch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("mismatch-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(effect) =
    worker.define("codec.drift", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("codec-drift")
  let assert Ok(workers) = registry.register(workers, effect)
  let assert Ok(handle) = postgres.submit(database, "codec-drift", effect, 9)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("mismatch-output-v2"))
    |> pog.parameter(pog.text("codec.drift"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: worker.OutputCodec,
        expected: "mismatch-output-v2",
        actual: "mismatch-output-v1",
      )),
    ),
  )
  postgres.state(database, handle)
  |> should.equal(Ok(job.ContractMismatch))
  process.receive(probe, within: 0)
  |> should.equal(Error(Nil))
  mark_database_test_executed("codec-contract-rejected")
}

fn run_postgres_queue_success_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("queue-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("queue-output-v1", json.string, decode.string)
  let assert Ok(increment) =
    worker.define("queue.increment", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value + 1))
    })
  let assert Ok(workers) = registry.new("default")
  let assert Ok(workers) = registry.register(workers, increment)
  let assert Ok(handle) = postgres.submit(database, "default", increment, 41)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.state(database, handle)
  |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("42")))
  mark_database_test_executed("committed-success-passed")
}

fn run_incompatible_schema_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE grind_jobs (id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, output_version text NOT NULL, output jsonb, error_version text, error jsonb, state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state <> 'executing' AND state <> 'succeeded'), available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, failure_description text)",
    )
    |> pog.execute(on: connection)

  postgres.migrate(database)
  |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(unrepaired) =
    pog.query(
      "SELECT to_regclass(current_schema() || '.grind_schema_migrations') IS NULL, to_regclass(current_schema() || '.grind_job_resolutions') IS NULL, to_regclass(current_schema() || '.grind_job_acknowledgements') IS NULL, to_regclass(current_schema() || '.grind_attempts_id_seq') IS NULL",
    )
    |> pog.returning({
      use migrations <- decode.field(0, decode.bool)
      use resolutions <- decode.field(1, decode.bool)
      use acknowledgements <- decode.field(2, decode.bool)
      use attempt_sequence <- decode.field(3, decode.bool)
      decode.success(#(
        migrations,
        resolutions,
        acknowledgements,
        attempt_sequence,
      ))
    })
    |> pog.execute(on: connection)
  unrepaired.rows |> should.equal([#(True, True, True, True)])
  mark_database_test_executed("incompatible-schema-rejected")
}

// -- Uniqueness (grind/unique, submit_unique/reconcile_unique) --------------
//
// Shared helpers for the uniqueness test suite below (this section and the
// increments-4-7 section that follows it). `unique_test_suffix` gives each
// test run a fresh, per-process-unique numeric string; every fixed worker
// id, queue name, and submission id these tests use includes it, so
// re-running this suite against a persistent development database (not the
// gate's disposable per-run cluster) never collides with rows or receipts a
// previous run left behind.

fn unique_test_suffix() -> String {
  int.to_string(unique_test_run_id())
}

/// Starts a pool, migrates, hands the database and a raw connection to
/// `run`, and closes the pool afterwards — the setup every uniqueness test
/// below needs except `run_submit_unique_pre_storage_rejection_test`, which
/// deliberately closes its pool before migrating. `_label` is unused (the
/// pool's own name is created inside `postgres.start` and never exposed
/// back to the caller) — kept as a parameter purely so every call site below
/// still reads as "which scenario this pool is for", not renumbered.
fn with_unique_database(
  database_url: String,
  _label: String,
  run: fn(postgres.Database, pog.Connection) -> Nil,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  run(database, postgres.connection(database))
}

/// The multi-pool counterpart to `with_unique_database` above: starts one
/// separate pool (a separate physical connection) per entry in `labels`
/// (the entries' own text is unused, purely a per-call-site count-and-name
/// the way `with_unique_database`'s own `_label` is), migrates via the
/// first, defers closing every one, and hands the whole list of
/// `#(Database, Connection)` pairs to `run` — for the concurrent-admission
/// tests below that need several independent connections to the same
/// database rather than one. Paired with its own connection (rather than
/// returning `Database` alone) because `postgres.connection` is `@internal`
/// — available to this test suite, but not part of the public API these
/// tests are meant to exercise through `run`'s own callback boundary.
fn with_unique_databases(
  database_url: String,
  labels: List(String),
  run: fn(List(#(postgres.Database, pog.Connection))) -> Nil,
) -> Nil {
  let entries =
    list.map(labels, fn(_label) {
      let assert Ok(validated) =
        postgres.settings(database_url) |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      #(database, postgres.connection(database))
    })
  use <- exception.defer(fn() {
    list.each(entries, fn(entry) { postgres.close(entry.0) })
  })
  let assert [#(first, _), ..] = entries
  let assert Ok(Nil) = postgres.migrate(first)
  run(entries)
}

/// Spawns a background process that runs `submit` (a zero-argument closure
/// so callers can partially apply `submit_keep_existing`/`submit_reschedule`/
/// `attempt.acknowledge`/etc. with whichever database/queue/submission
/// it needs) and sends the result to `result` — the small boilerplate every
/// concurrent test below otherwise repeats once per concurrent caller.
/// Generic over the result type so both the uniqueness admission tests and
/// the acknowledgement contention test share it.
fn spawn_submit(result: process.Subject(a), submit: fn() -> a) -> Nil {
  let _ = process.spawn_unlinked(fn() { process.send(result, submit()) })
  Nil
}

/// The `Int` input / `String` output (`int.to_string`) worker shape most
/// uniqueness tests below use. `id` should already carry a
/// `unique_test_suffix()`.
fn unique_test_worker(id: String) -> worker.Worker(Int, String, e) {
  unique_test_worker_versioned(id, "v1")
}

/// Like `unique_test_worker`, but with a caller-chosen worker version — used
/// by the cross-version quarantine test below, which registers two different
/// versions of the same worker id against two different consumers.
fn unique_test_worker_versioned(
  id: String,
  version: String,
) -> worker.Worker(Int, String, e) {
  let assert Ok(input_codec) =
    worker.codec(id <> "-input-" <> version, json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(id <> "-output-" <> version, json.string, decode.string)
  let assert Ok(worker_def) =
    worker.define(id, version, input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  worker_def
}

/// Like `unique_test_worker`, but the handler blocks (reporting
/// `FirstAttemptStarted(release)` on `started` first) until explicitly
/// released, instead of returning immediately. Used by the reschedule/claim
/// race test below, which needs the claimed row to stay genuinely
/// `executing` for a controlled window — a worker that returns immediately
/// lets the coordinator's own subsequent acknowledgement race ahead to
/// `succeeded` before the concurrent reschedule submission's blocked row
/// lock is even granted, an environment-dependent race, not a deterministic
/// proof.
fn unique_test_blocking_worker(
  id: String,
  started: process.Subject(LeaseSignal),
) -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input_codec) =
    worker.codec(id <> "-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(id <> "-output-v1", json.string, decode.string)
  let assert Ok(worker_def) =
    worker.define(id, "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok(int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  worker_def
}

/// `submit_unique` under the `Immediately`/`KeepExisting` case every test
/// below needs (none of these increments exercise rescheduling).
fn submit_keep_existing(
  database: postgres.Database,
  queue: String,
  id_text: String,
  worker_def: worker.Worker(input, output, error),
  input: input,
  policy: unique.Policy(input),
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let assert Ok(submission) = submission.submission_id(id_text)
  postgres.submit_unique(
    database,
    queue,
    submission,
    worker_def,
    input,
    submission.Immediately,
    policy,
    unique.KeepExisting,
  )
}

pub fn unique_period_validation_test() {
  unique.within_milliseconds(0, unique.FromInsertion)
  |> should.equal(Error(unique.NonPositivePeriod))
  unique.within_milliseconds(-1, unique.FromSchedule)
  |> should.equal(Error(unique.NonPositivePeriod))
  let too_large = worker.retry_delay_maximum_milliseconds() + 1
  unique.within_milliseconds(too_large, unique.FromInsertion)
  |> should.equal(Error(unique.PeriodAbovePrecisionBound))
  let assert Ok(_) = unique.within_milliseconds(1000, unique.FromInsertion)
  let assert Ok(_) =
    unique.within_milliseconds(
      worker.retry_delay_maximum_milliseconds(),
      unique.FromSchedule,
    )
  Nil
}

pub fn unique_key_and_submission_id_validation_test() {
  let assert Ok(codec) = worker.codec("unique-key-v1", json.int, decode.int)
  unique.selected("", fn(input: Int) { input }, codec)
  |> should.equal(Error(unique.EmptyKeyName))
  let assert Ok(_) = unique.selected("account", fn(input: Int) { input }, codec)

  submission.submission_id("")
  |> should.equal(Error(submission.EmptySubmissionId))
  let assert Ok(id) = submission.submission_id("abc-123")
  submission.submission_id_value(id) |> should.equal("abc-123")
}

pub fn postgres_unique_lock_wait_must_be_positive_test() {
  let base = postgres.settings("postgres://grind@127.0.0.1:5432/unused")

  base
  |> postgres.with_unique_lock_wait(0)
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidUniqueLockWait))

  base
  |> postgres.with_unique_lock_wait(-5)
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidUniqueLockWait))

  case base |> postgres.with_unique_lock_wait(200) |> postgres.validate {
    Ok(_) -> Nil
    Error(_) -> should.fail()
  }
}

type RawInput {
  RawInput(json.Json)
}

fn encode_raw_input(input: RawInput) -> json.Json {
  let RawInput(value) = input
  value
}

fn raw_input_decoder() -> decode.Decoder(RawInput) {
  decode.success(RawInput(json.null()))
}

pub fn postgres_submit_unique_rejects_before_touching_storage_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_unique_pre_storage_rejection_test(database_url)
  }
}

fn run_submit_unique_pre_storage_rejection_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  // Closed immediately: any query attempt beyond this point would fail at
  // the storage boundary, so a passing test here proves the empty-queue
  // rejection is a pure check that runs before `submit_unique` ever reaches
  // the database.
  let _ = postgres.close(database)

  let worker_def = unique_test_worker("unique.closed-" <> suffix)
  let assert Ok(period) = unique.within_milliseconds(1000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  submit_keep_existing(
    database,
    "",
    "unique-closed-" <> suffix,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.EmptyQueueName))

  mark_database_test_executed("unique-pre-storage-rejections-passed")
}

pub fn postgres_submit_unique_admits_and_detects_existing_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_existing_conflict_test(database_url)
  }
}

fn run_submit_unique_existing_conflict_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_pool",
  )
  let worker_def = unique_test_worker("unique.existing-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "default-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-existing-1-" <> suffix,
      worker_def,
      7,
      policy,
    )

  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-existing-2-" <> suffix,
      worker_def,
      7,
      policy,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))
  submission.conflict_queue(conflict) |> should.equal(test_queue)
  submission.conflict_state(conflict) |> should.equal(job.Queued)

  let assert Ok(bound) =
    postgres.bind_handle(
      database,
      worker_def,
      submission.conflict_job_id(conflict),
    )
  postgres.arguments(database, bound) |> should.equal(Ok(7))

  let assert Ok(submission.Inserted(other_handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-existing-3-" <> suffix,
      worker_def,
      8,
      policy,
    )
  job.id_value(other_handle) |> should.not_equal(job.id_value(handle))

  mark_database_test_executed("unique-admission-existing-conflict-passed")
}

pub fn postgres_submit_unique_json_equality_matches_postgres_jsonb_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_json_equality_test(database_url)
  }
}

fn run_submit_unique_json_equality_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_json_pool",
  )
  let assert Ok(input_codec) =
    worker.codec(
      "unique-json-input-" <> suffix <> "-v1",
      encode_raw_input,
      raw_input_decoder(),
    )
  let assert Ok(output_codec) =
    worker.codec(
      "unique-json-output-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(
      "unique.json-equality-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(_) { Ok("done") },
    )
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "json-equality-" <> suffix

  let admit = fn(tag: String, value: json.Json) {
    submit_keep_existing(
      database,
      test_queue,
      tag <> "-" <> suffix,
      worker_def,
      RawInput(value),
      policy,
    )
  }

  // Field order is irrelevant: {"a":1,"b":2} conflicts with {"b":2,"a":1}.
  let assert Ok(submission.Inserted(_)) =
    admit(
      "field-order-1",
      json.object([#("a", json.int(1)), #("b", json.int(2))]),
    )
  let assert Ok(submission.Existing(_)) =
    admit(
      "field-order-2",
      json.object([#("b", json.int(2)), #("a", json.int(1))]),
    )

  // 1 and 1.0 are distinct scalars under PostgreSQL's own jsonb::text
  // rendering: a deliberate departure from a cross-language canonical JSON
  // equality (see docs/UNIQUENESS-CONTRACT.md).
  let assert Ok(submission.Inserted(_)) = admit("numeric-int", json.int(1))
  let assert Ok(submission.Inserted(_)) =
    admit("numeric-float", json.float(1.0))

  // Array order is significant.
  let assert Ok(submission.Inserted(_)) =
    admit("array-order-1", json.array([1, 2], of: json.int))
  let assert Ok(submission.Inserted(_)) =
    admit("array-order-2", json.array([2, 1], of: json.int))

  // {id:1} does not conflict with {id:1, extra:2}: exact equality, not
  // Oban's containment semantics.
  let assert Ok(submission.Inserted(_)) =
    admit("subset-1", json.object([#("id", json.int(1))]))
  let assert Ok(submission.Inserted(_)) =
    admit(
      "subset-2",
      json.object([#("id", json.int(1)), #("extra", json.int(2))]),
    )

  // An empty object does not conflict with a non-empty one.
  let assert Ok(submission.Inserted(_)) = admit("empty", json.object([]))
  let assert Ok(submission.Inserted(_)) =
    admit("non-empty", json.object([#("a", json.int(1))]))

  mark_database_test_executed("unique-json-equality-cases-passed")
}

pub fn postgres_submit_unique_scopes_key_to_worker_identity_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_worker_identity_test(database_url)
  }
}

fn run_submit_unique_worker_identity_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_worker_pool",
  )
  let assert Ok(input_codec) =
    worker.codec(
      "unique-identity-input-" <> suffix <> "-v1",
      json.int,
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "unique-identity-output-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let worker_id = "unique.identity-" <> suffix
  let assert Ok(worker_v1) =
    worker.define(worker_id, "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(worker_v2) =
    worker.define(worker_id, "v2", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(other_worker) =
    worker.define(
      "unique.identity-other-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "identity-" <> suffix

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-identity-v1-" <> suffix,
      worker_v1,
      5,
      policy,
    )

  // Same worker id, different worker version: proven-by-mutation isolation
  // (dropping worker_version from the candidate match makes this line see
  // the v1 row above as a false conflict instead of `Inserted`).
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-identity-v2-" <> suffix,
      worker_v2,
      5,
      policy,
    )

  // A different worker id entirely: does not conflict either.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-identity-other-" <> suffix,
      other_worker,
      5,
      policy,
    )

  // The same worker and version, same input, does still conflict.
  let assert Ok(submission.Existing(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-identity-repeat-" <> suffix,
      worker_v1,
      5,
      policy,
    )

  mark_database_test_executed("unique-worker-identity-isolation-passed")
}

pub fn postgres_submit_unique_ignores_plain_submitted_rows_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_plain_submit_test(database_url)
  }
}

fn run_submit_unique_plain_submit_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_plain_pool",
  )
  let worker_def = unique_test_worker("unique.plain-" <> suffix)
  let test_queue = "plain-" <> suffix
  let assert Ok(_plain_handle) =
    postgres.submit(database, test_queue, worker_def, 99)

  // AllRetained deliberately widens eligibility to every persisted state, so
  // a false match here could only come from the plain row's NULL
  // unique_key_contract/unique_key_sha256 being mishandled, not from a
  // states filter accidentally excluding it.
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.AllRetained,
    )
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-plain-1-" <> suffix,
      worker_def,
      99,
      policy,
    )

  mark_database_test_executed("unique-plain-submit-non-participation-passed")
}

// -- Uniqueness increments 4-7 (queue scope, state eligibility, period
// boundaries at database time, receipts/idempotency) -----------------------
//
// Shared helpers for forcing a persisted column via raw SQL and for counting
// rows, used by the tests below.

fn force_job_timestamp(
  connection: pog.Connection,
  job_id: Int,
  column: String,
  sql_expression: String,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET "
      <> column
      <> " = "
      <> sql_expression
      <> " WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  Nil
}

fn force_job_state(
  connection: pog.Connection,
  job_id: Int,
  state: String,
) -> Nil {
  // `grind_jobs_finished_at_check` (grind_v12) requires `finished_at` to be
  // set iff `state` is one of the six terminal states, so a raw state flip
  // must set it consistently too, not just leave whatever the row already
  // had.
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = $1, finished_at = CASE WHEN $1 IN ("
      <> terminal.states_sql()
      <> ") THEN clock_timestamp() ELSE NULL END WHERE id = $2",
    )
    |> pog.parameter(pog.text(state))
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  Nil
}

fn count_jobs_in_queue(connection: pog.Connection, queue: String) -> Int {
  let assert Ok(returned) =
    pog.query("SELECT count(*)::bigint FROM grind_jobs WHERE queue = $1")
    |> pog.parameter(pog.text(queue))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  let assert [count] = returned.rows
  count
}

fn count_acknowledgements_for_job(
  connection: pog.Connection,
  job_id: Int,
) -> Int {
  let assert Ok(returned) =
    pog.query(
      "SELECT count(*)::bigint FROM grind_job_acknowledgements WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  let assert [count] = returned.rows
  count
}

/// Polls (bounded) until the number of other active backends waiting on an
/// **advisory** lock equals `advisory_target` and the number waiting on a
/// **row** lock (`transactionid`, PostgreSQL's wait event for a tuple lock
/// held by another transaction) equals `transactionid_target` — both from
/// one query over one snapshot, the same discipline `await_overlap_shape`
/// above uses. Unlike `await_overlap_shape`, this does not key off query
/// text: the two waiters here run byte-identical SQL (the same
/// acknowledgement command retried), so only `wait_event` tells them apart.
fn await_lock_wait_counts(
  connection: pog.Connection,
  advisory_target: Int,
  transactionid_target: Int,
  checks_remaining: Int,
) -> Bool {
  let counts =
    pog.query(
      "SELECT count(*) FILTER (WHERE wait_event = 'advisory'), count(*) FILTER (WHERE wait_event = 'transactionid') FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'Lock'",
    )
    |> pog.returning({
      use advisory <- decode.field(0, decode.int)
      use transactionid <- decode.field(1, decode.int)
      decode.success(#(advisory, transactionid))
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [pair] -> Ok(pair)
        _ -> Error(Nil)
      }
    })
  case counts {
    Ok(#(advisory, transactionid))
      if advisory == advisory_target && transactionid == transactionid_target
    -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_lock_wait_counts(
            connection,
            advisory_target,
            transactionid_target,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

// -- Uniqueness (increments 8-9: forced concurrent overlap, contention) ----
//
// Shared helpers for the barrier-forced-overlap and lock-contention tests
// below. These reuse `ClaimGateSignal`/`LeaseCommand` (already declared for
// `run_overlapping_claim_test`'s inline hold-then-release-on-cue shape) and
// generalize that same shape into `spawn_lock_holder`, rather than
// re-declaring it per test.

/// Spawns a background process that opens its own transaction on
/// `connection`, executes `acquire_query` (expected to run and hold some
/// PostgreSQL lock for the rest of that transaction — an advisory lock or a
/// row lock), signals `ClaimGateAcquired` once `acquire_query` has returned,
/// then waits (bounded) for a release before committing (which releases
/// whatever lock it holds). `run_overlapping_claim_test` uses this exact
/// shape inline for its own claim-`UPDATE` barrier; factored out here so
/// every uniqueness barrier/contention test below shares one
/// implementation instead of re-declaring it.
fn spawn_lock_holder(
  connection: pog.Connection,
  acquire_query: pog.Query(a),
) -> #(process.Subject(ClaimGateSignal), process.Subject(ClaimGateSignal)) {
  let lock_ready = process.new_subject()
  let lock_finished = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      // `release_lock` must be created by this spawned process, not the
      // caller: `process.receive` only allows the subject's own creator to
      // receive on it, exactly like `run_overlapping_claim_test`'s inline
      // version of this same shape above declares it inside the spawned
      // closure and hands it to the caller via `ClaimGateAcquired`.
      let release_lock = process.new_subject()
      let transaction_result =
        pog.transaction(connection, fn(transaction_connection) {
          case pog.execute(acquire_query, on: transaction_connection) {
            Error(_) -> Error(Nil)
            Ok(_) -> {
              process.send(lock_ready, ClaimGateAcquired(release_lock))
              case process.receive(release_lock, within: 10_000) {
                Ok(ReleaseAttempt) -> Ok(Nil)
                Error(Nil) -> Error(Nil)
              }
            }
          }
        })
      process.send(
        lock_finished,
        ClaimGateReleased(result.is_ok(transaction_result)),
      )
    })
  #(lock_ready, lock_finished)
}

/// Installs a `BEFORE INSERT` trigger on `grind_jobs`, scoped to
/// `worker_id`, that blocks any insert for that worker behind
/// `pg_advisory_xact_lock(lock_key)` — the same held-then-released-on-cue
/// barrier shape `run_overlapping_claim_test`'s `grind_test_claim_overlap`
/// trigger uses for a claim `UPDATE`, generalized here to an `INSERT` and
/// parameterized by worker id and lock key so the forced-overlap tests
/// below (including the mixed-scope variant, which needs its own separate
/// lock key) share one trigger implementation. Returns a cleanup thunk for
/// `exception.defer`.
fn install_unique_insert_barrier(
  connection: pog.Connection,
  name: String,
  worker_id: String,
  lock_key: Int,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.worker_id = '"
      <> worker_id
      <> "' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> name
      <> " BEFORE INSERT ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> name
      <> "()",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> name <> " ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
}

/// A deterministic advisory-lock key, distinct per test run (derived from
/// `unique_test_run_id()`) and offset well clear of every other literal
/// advisory-lock key this suite hard-codes elsewhere (74126/31 in
/// `run_overlapping_claim_test`), for the forced-overlap barrier triggers
/// below. `salt` lets one test declare more than one distinct key (the
/// mixed-scope variant runs alongside the main overlap test, in the same
/// `gleam test` process, and must not share a lock key with it).
fn unique_test_lock_key(salt: Int) -> Int {
  let assert Ok(reduced) = int.modulo(unique_test_run_id(), by: 100_000_000)
  900_000_000 + reduced + salt
}

/// Polls (bounded) until the number of other active backends whose query
/// text matches `insert_like` equals `insert_target`, and the number
/// matching `lock_like` equals `lock_target`, **at the same instant** — both
/// counted from one query so the two figures are never read from two
/// different moments in time. Used to prove the forced-overlap barrier's
/// exact expected shape (one backend blocked inserting behind the test's
/// own held trigger lock, N others blocked acquiring the real uniqueness
/// domain lock) rather than inferring it from timing alone, the same
/// discipline `await_claim_waiting_on_advisory` above uses for the
/// claim-overlap barrier.
fn await_overlap_shape(
  connection: pog.Connection,
  insert_like: String,
  lock_like: String,
  insert_target: Int,
  lock_target: Int,
  checks_remaining: Int,
) -> Bool {
  let counts =
    pog.query(
      "SELECT count(*) FILTER (WHERE query LIKE $1), count(*) FILTER (WHERE query LIKE $2) FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory'",
    )
    |> pog.parameter(pog.text(insert_like))
    |> pog.parameter(pog.text(lock_like))
    |> pog.returning({
      use inserting <- decode.field(0, decode.int)
      use locking <- decode.field(1, decode.int)
      decode.success(#(inserting, locking))
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [pair] -> Ok(pair)
        _ -> Error(Nil)
      }
    })
  case counts {
    Ok(#(inserting, locking))
      if inserting == insert_target && locking == lock_target
    -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_overlap_shape(
            connection,
            insert_like,
            lock_like,
            insert_target,
            lock_target,
            checks_remaining - 1,
          )
        }
        False -> False
      }
  }
}

/// The exact query text `grind/internal/unique_admission`'s `insert_job`
/// issues, as a `LIKE` prefix for `await_overlap_shape`/`pg_stat_activity`.
const unique_insert_query_like = "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version%"

/// The exact query text `grind/internal/unique_admission`'s `acquire_lock`
/// issues, as a `LIKE` prefix for `await_overlap_shape`/`pg_stat_activity`.
const unique_domain_lock_query_like = "SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended%"

fn unique_receipt_exists(
  connection: pog.Connection,
  storage_owner: String,
  submission_id_text: String,
) -> Bool {
  let assert Ok(returned) =
    pog.query(
      "SELECT EXISTS(SELECT 1 FROM grind_unique_submissions WHERE storage_owner = $1 AND submission_id = $2)",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(submission_id_text))
    |> pog.returning({
      use exists <- decode.field(0, decode.bool)
      decode.success(exists)
    })
    |> pog.execute(on: connection)
  let assert [exists] = returned.rows
  exists
}

fn job_available_at_ms(connection: pog.Connection, job_id: Int) -> Int {
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM available_at) * 1000)::bigint FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.returning({
      use ms <- decode.field(0, decode.int)
      decode.success(ms)
    })
    |> pog.execute(on: connection)
  let assert [ms] = returned.rows
  ms
}

/// Forces a row's `available_at` to a due (past) database time directly,
/// leaving `state` untouched — used by the Increment 10 reschedule/claim race
/// test to make a genuinely `scheduled` row immediately claimable without
/// waiting on wall-clock time, so the only real synchronization point in that
/// test is the barrier-forced lock overlap itself.
fn force_available_at_due(connection: pog.Connection, job_id: Int) -> Nil {
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() - interval '2 seconds' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  Nil
}

/// Reads a receipt's `rescheduled_from`/`rescheduled_to` columns (both
/// `NULL` for a non-reschedule decision), in milliseconds, for the Increment
/// 10 reschedule tests.
fn unique_receipt_reschedule_fields(
  connection: pog.Connection,
  storage_owner: String,
  submission_id_text: String,
) -> #(Option(Int), Option(Int)) {
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM rescheduled_from) * 1000)::bigint, (extract(epoch FROM rescheduled_to) * 1000)::bigint FROM grind_unique_submissions WHERE storage_owner = $1 AND submission_id = $2",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(submission_id_text))
    |> pog.returning({
      use from_ms <- decode.field(0, decode.optional(decode.int))
      use to_ms <- decode.field(1, decode.optional(decode.int))
      decode.success(#(from_ms, to_ms))
    })
    |> pog.execute(on: connection)
  let assert [row] = returned.rows
  row
}

/// `submit_unique` under `Immediately`/`RescheduleScheduledTo(target)`, the
/// counterpart to `submit_keep_existing` above for the reschedule-action
/// tests below.
fn submit_reschedule(
  database: postgres.Database,
  queue: String,
  id_text: String,
  worker_def: worker.Worker(input, output, error),
  input: input,
  policy: unique.Policy(input),
  target: job.AvailableAt,
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let assert Ok(submission) = submission.submission_id(id_text)
  postgres.submit_unique(
    database,
    queue,
    submission,
    worker_def,
    input,
    submission.Immediately,
    policy,
    unique.RescheduleScheduledTo(target),
  )
}

/// A future database-time `AvailableAt`, `offset_ms` ahead of the test
/// cluster's own `clock_timestamp()` — never the calling BEAM node's clock —
/// read through `connection`.
fn future_available_at(
  connection: pog.Connection,
  offset_ms: Int,
) -> job.AvailableAt {
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint + $1",
    )
    |> pog.parameter(pog.int(offset_ms))
    |> pog.returning({
      use ms <- decode.field(0, decode.int)
      decode.success(ms)
    })
    |> pog.execute(on: connection)
  let assert [future_ms] = returned.rows
  let assert Ok(available_at) = job.available_at(future_ms)
  available_at
}

/// The domain-wide uniqueness advisory lock's own SQL and parameters (`@internal
/// unique_admission.lock_key_sql`), built from a worker/input pair exactly the
/// way `grind/internal/unique_admission`'s `acquire_lock` would for a real
/// `submit_unique` call against that worker and input under `full_input()`
/// — used by the increment 9 contention tests below to hold, from the test
/// itself, the *same* lock a concurrent `submit_unique` call would need.
fn unique_domain_lock_query(
  database: postgres.Database,
  worker_def: worker.Worker(input, output, error),
  input: input,
) -> pog.Query(Bool) {
  let storage_owner = postgres.storage_owner(database)
  let worker_meta = worker.metadata(worker_def)
  let encoded_input = worker.encode_input(worker_def, input)
  let #(key_contract, encoded_key) =
    unique.key_material(
      unique.full_input(),
      input,
      worker_meta.input_version,
      encoded_input,
    )
  unique_admission.lock_query(
    storage_owner,
    worker_meta.id,
    worker_meta.worker_version,
    key_contract,
    encoded_key,
  )
}

/// Increment 4: `WithinQueue` admits the same key independently in two
/// queues; `AcrossQueues` then conflicts with the earlier of the two rows
/// (lowest id) regardless of which queue it lives in.
pub fn postgres_submit_unique_respects_queue_scope_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_unique_queue_scope_test(database_url)
  }
}

fn run_submit_unique_queue_scope_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_queue_scope",
  )
  let worker_def = unique_test_worker("unique.queue-scope-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy_within =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let policy_across =
    unique.policy(
      unique.full_input(),
      unique.AcrossQueues,
      period,
      unique.Incomplete,
    )
  let queue_1 = "q1-" <> suffix
  let queue_2 = "q2-" <> suffix

  let assert Ok(submission.Inserted(handle_q1)) =
    submit_keep_existing(
      database,
      queue_1,
      "unique-queue-scope-q1-" <> suffix,
      worker_def,
      1,
      policy_within,
    )

  // WithinQueue: the same key admits independently in a second queue.
  let assert Ok(submission.Inserted(handle_q2)) =
    submit_keep_existing(
      database,
      queue_2,
      "unique-queue-scope-q2-" <> suffix,
      worker_def,
      1,
      policy_within,
    )
  job.id_value(handle_q2) |> should.not_equal(job.id_value(handle_q1))

  // AcrossQueues submitted against q2 conflicts with the earlier q1 row.
  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      queue_2,
      "unique-queue-scope-across-" <> suffix,
      worker_def,
      1,
      policy_across,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle_q1))
  submission.conflict_queue(conflict) |> should.equal(queue_1)

  mark_database_test_executed("unique-queue-scope-passed")
}

/// Increment 5(a): force each of the 11 persisted states by raw SQL and
/// assert each of the four named `States` groups' eligibility for it.
type StateEligibility {
  StateEligibility(
    stored: String,
    incomplete: Bool,
    scheduled_only: Bool,
    incomplete_or_succeeded: Bool,
    all_retained: Bool,
  )
}

fn unique_state_eligibility_matrix() -> List(StateEligibility) {
  [
    StateEligibility("queued", True, False, True, True),
    StateEligibility("scheduled", True, True, True, True),
    StateEligibility("retryable", True, False, True, True),
    StateEligibility("executing", True, False, True, True),
    StateEligibility("succeeded", False, False, True, True),
    StateEligibility("business_failed", False, False, False, True),
    StateEligibility("runtime_failed", False, False, False, True),
    StateEligibility("contract_mismatch", False, False, False, True),
    StateEligibility("uncertain", True, False, True, True),
    StateEligibility("discarded", False, False, False, True),
    StateEligibility("cancelled", False, False, False, True),
  ]
}

pub fn postgres_submit_unique_state_eligibility_matrix_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_state_eligibility_matrix_test(database_url)
  }
}

fn run_state_eligibility_matrix_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_states_pool",
  )
  let assert Ok(input_codec) =
    worker.codec(
      "unique-states-input-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "unique-states-output-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(
      "unique.states-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value) },
    )
  let period = unique.while_retained()
  let test_queue = "states-" <> suffix

  let groups = [
    #(unique.Incomplete, "incomplete", fn(row: StateEligibility) {
      row.incomplete
    }),
    #(unique.ScheduledOnly, "scheduled-only", fn(row: StateEligibility) {
      row.scheduled_only
    }),
    #(
      unique.IncompleteOrSucceeded,
      "incomplete-or-succeeded",
      fn(row: StateEligibility) { row.incomplete_or_succeeded },
    ),
    #(unique.AllRetained, "all-retained", fn(row: StateEligibility) {
      row.all_retained
    }),
  ]

  unique_state_eligibility_matrix()
  |> list.each(fn(row) {
    groups
    |> list.each(fn(group) {
      let #(states, label, eligible) = group
      let key_input = suffix <> "/" <> row.stored <> "/" <> label
      let policy =
        unique.policy(unique.full_input(), unique.WithinQueue, period, states)

      let assert Ok(submission.Inserted(handle)) =
        submit_keep_existing(
          database,
          test_queue,
          "unique-states-insert-" <> key_input,
          worker_def,
          key_input,
          policy,
        )
      let job_id = job.id_value(handle)
      force_job_state(connection, job_id, row.stored)

      let result =
        submit_keep_existing(
          database,
          test_queue,
          "unique-states-check-" <> key_input,
          worker_def,
          key_input,
          policy,
        )
      case eligible(row) {
        True -> {
          let assert Ok(submission.Existing(conflict)) = result
          submission.conflict_job_id(conflict) |> should.equal(job_id)
        }
        False -> {
          let assert Ok(submission.Inserted(_)) = result
          Nil
        }
      }
    })
  })

  mark_database_test_executed("unique-state-eligibility-matrix-passed")
}

/// Increment 5(b): a live transition through a real, manually-driven
/// consumer — `Incomplete` sees the row while it is genuinely `queued`, then
/// stops seeing it once the job has genuinely succeeded, while
/// `IncompleteOrSucceeded` still matches the original succeeded row.
pub fn postgres_submit_unique_state_live_transition_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_state_live_transition_test(database_url)
  }
}

fn run_state_live_transition_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_live_transition",
  )
  let worker_def = unique_test_worker("unique.live-" <> suffix)
  let test_queue = "unique-live-" <> suffix
  let assert Ok(registry_workers) = registry.new(test_queue)
  let assert Ok(registry_workers) =
    registry.register(registry_workers, worker_def)
  let assert Ok(consumer) =
    queue.start(database, registry_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let period = unique.while_retained()
  let policy_incomplete =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let policy_incomplete_or_succeeded =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.IncompleteOrSucceeded,
    )

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-live-1-" <> suffix,
      worker_def,
      99,
      policy_incomplete,
    )

  // While the row is genuinely still queued, `Incomplete` sees it.
  let assert Ok(submission.Existing(conflict_while_queued)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-live-2-" <> suffix,
      worker_def,
      99,
      policy_incomplete,
    )
  submission.conflict_job_id(conflict_while_queued)
  |> should.equal(job.id_value(handle))

  // Run it to a real, committed success.
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  // After a genuine success, `Incomplete` no longer counts it: a fresh row
  // is admitted.
  let assert Ok(submission.Inserted(handle_after_success)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-live-3-" <> suffix,
      worker_def,
      99,
      policy_incomplete,
    )
  job.id_value(handle_after_success)
  |> should.not_equal(job.id_value(handle))

  // `IncompleteOrSucceeded` still matches the original succeeded row (lowest
  // id), not the fresh one `Incomplete` just admitted.
  let assert Ok(submission.Existing(conflict_after_success)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-live-4-" <> suffix,
      worker_def,
      99,
      policy_incomplete_or_succeeded,
    )
  submission.conflict_job_id(conflict_after_success)
  |> should.equal(job.id_value(handle))
  submission.conflict_state(conflict_after_success)
  |> should.equal(job.Succeeded)

  mark_database_test_executed("unique-state-live-transition-passed")
}

/// Increment 6(a): the shared `@internal` period predicate at an exact
/// database-time instant, against literal timestamps only (no table
/// involved) — `now = ts + period` is inclusive (`true`); one microsecond
/// later is not.
pub fn postgres_unique_period_predicate_matches_the_exact_instant_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_period_predicate_exact_instant_test(database_url)
  }
}

fn run_period_predicate_exact_instant_test(database_url: String) -> Nil {
  use _database, connection <- with_unique_database(
    database_url,
    "grind_unique_period_predicate",
  )
  let ts = "timestamptz '2024-01-01 00:00:00+00'"
  let now_at_boundary = "timestamptz '2024-01-01 00:00:05+00'"
  let now_past_boundary = "timestamptz '2024-01-01 00:00:05.000001+00'"
  let period_ms = "5000"

  let assert Ok(returned) =
    pog.query(
      "SELECT "
      <> unique_admission.period_predicate(ts, now_at_boundary, period_ms)
      <> ", "
      <> unique_admission.period_predicate(ts, now_past_boundary, period_ms),
    )
    |> pog.returning({
      use at_boundary <- decode.field(0, decode.bool)
      use past_boundary <- decode.field(1, decode.bool)
      decode.success(#(at_boundary, past_boundary))
    })
    |> pog.execute(on: connection)
  let assert [#(at_boundary, past_boundary)] = returned.rows
  at_boundary |> should.equal(True)
  past_boundary |> should.equal(False)

  mark_database_test_executed("unique-period-predicate-exact-instant-passed")
}

/// Increment 6(b): `FromInsertion` at the database clock, live through
/// `submit_unique` (not the predicate alone) — 58 seconds inside a 60-second
/// window still conflicts; 62 seconds outside it does not.
pub fn postgres_submit_unique_from_insertion_period_matches_database_time_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_period_from_insertion_boundary_test(database_url)
  }
}

fn run_period_from_insertion_boundary_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_from_insertion",
  )
  let worker_def = unique_test_worker("unique.from-insertion-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(60_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "from-insertion-" <> suffix

  // Inserted 58 seconds ago: still inside the 60-second window.
  let assert Ok(submission.Inserted(handle_within)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-insertion-within-1-" <> suffix,
      worker_def,
      1,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle_within),
    "inserted_at",
    "clock_timestamp() - interval '58 seconds'",
  )
  let assert Ok(submission.Existing(conflict_within)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-insertion-within-2-" <> suffix,
      worker_def,
      1,
      policy,
    )
  submission.conflict_job_id(conflict_within)
  |> should.equal(job.id_value(handle_within))

  // Inserted 62 seconds ago: outside the 60-second window.
  let assert Ok(submission.Inserted(handle_outside)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-insertion-outside-1-" <> suffix,
      worker_def,
      2,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle_outside),
    "inserted_at",
    "clock_timestamp() - interval '62 seconds'",
  )
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-insertion-outside-2-" <> suffix,
      worker_def,
      2,
      policy,
    )

  mark_database_test_executed("unique-period-from-insertion-boundary-passed")
}

/// Increment 6(c): `FromSchedule`, "compared to the scheduled time" (Oban's
/// own framing) — a row whose `available_at` is 121 seconds in the past no
/// longer conflicts under a 120-second period; 119 seconds still does.
pub fn postgres_submit_unique_from_schedule_period_past_boundary_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_period_from_schedule_past_boundary_test(database_url)
  }
}

fn run_period_from_schedule_past_boundary_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_from_schedule_past",
  )
  let worker_def = unique_test_worker("unique.from-schedule-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(120_000, unique.FromSchedule)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "from-schedule-past-" <> suffix

  // Scheduled 121 seconds in the past: outside the 120-second window.
  let assert Ok(submission.Inserted(handle_outside)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-outside-1-" <> suffix,
      worker_def,
      1,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle_outside),
    "available_at",
    "clock_timestamp() - interval '121 seconds'",
  )
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-outside-2-" <> suffix,
      worker_def,
      1,
      policy,
    )

  // Scheduled 119 seconds in the past: still inside the 120-second window.
  let assert Ok(submission.Inserted(handle_inside)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-inside-1-" <> suffix,
      worker_def,
      2,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle_inside),
    "available_at",
    "clock_timestamp() - interval '119 seconds'",
  )
  let assert Ok(submission.Existing(conflict_inside)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-inside-2-" <> suffix,
      worker_def,
      2,
      policy,
    )
  submission.conflict_job_id(conflict_inside)
  |> should.equal(job.id_value(handle_inside))

  mark_database_test_executed(
    "unique-period-from-schedule-past-boundary-passed",
  )
}

/// Increment 6(d): a future `FromSchedule` deadline extends the occupancy
/// window well beyond what the same period length would already have let
/// expire under `FromInsertion` — the same row, the same 60-second period,
/// inserted 5 minutes ago (long past a `FromInsertion` window) but scheduled
/// 5 minutes from now (comfortably inside a `FromSchedule` window).
pub fn postgres_submit_unique_from_schedule_future_extends_window_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_period_from_schedule_future_extends_window_test(database_url)
  }
}

fn run_period_from_schedule_future_extends_window_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_from_schedule_future",
  )
  let worker_def = unique_test_worker("unique.from-schedule-future-" <> suffix)
  let assert Ok(period_from_schedule) =
    unique.within_milliseconds(60_000, unique.FromSchedule)
  let policy_from_schedule =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period_from_schedule,
      unique.Incomplete,
    )
  let assert Ok(period_from_insertion) =
    unique.within_milliseconds(60_000, unique.FromInsertion)
  let policy_from_insertion =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period_from_insertion,
      unique.Incomplete,
    )
  let test_queue = "from-schedule-future-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-future-1-" <> suffix,
      worker_def,
      9,
      policy_from_schedule,
    )
  let job_id = job.id_value(handle)
  force_job_timestamp(
    connection,
    job_id,
    "inserted_at",
    "clock_timestamp() - interval '300 seconds'",
  )
  force_job_timestamp(
    connection,
    job_id,
    "available_at",
    "clock_timestamp() + interval '300 seconds'",
  )

  // `FromInsertion`, same key: the row's insertion is long past the
  // 60-second period, so it no longer conflicts.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-future-2-" <> suffix,
      worker_def,
      9,
      policy_from_insertion,
    )

  // `FromSchedule`, same key, same 60-second period: measured from a
  // schedule 5 minutes in the future, it still covers the original row.
  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-from-schedule-future-3-" <> suffix,
      worker_def,
      9,
      policy_from_schedule,
    )
  submission.conflict_job_id(conflict) |> should.equal(job_id)

  mark_database_test_executed(
    "unique-period-from-schedule-future-extends-window-passed",
  )
}

/// Increment 6(e): `while_retained()` has no time boundary at all — a row
/// inserted a year ago still conflicts.
pub fn postgres_submit_unique_while_retained_matches_a_year_old_row_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_period_while_retained_old_row_test(database_url)
  }
}

fn run_period_while_retained_old_row_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_while_retained",
  )
  let worker_def = unique_test_worker("unique.while-retained-" <> suffix)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      unique.while_retained(),
      unique.Incomplete,
    )
  let test_queue = "while-retained-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-while-retained-1-" <> suffix,
      worker_def,
      3,
      policy,
    )
  force_job_timestamp(
    connection,
    job.id_value(handle),
    "inserted_at",
    "clock_timestamp() - interval '1 year'",
  )

  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-while-retained-2-" <> suffix,
      worker_def,
      3,
      policy,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))

  mark_database_test_executed("unique-period-while-retained-old-row-passed")
}

/// Increment 7(a): the same `SubmissionId` with the same request, retried
/// after the original row genuinely succeeded and its short period has
/// elapsed, returns the original `Inserted` handle (same job id) from the
/// receipt — not a second row (which a fresh, receipt-blind candidate
/// lookup would create, since the succeeded row is by then outside its own
/// period).
pub fn postgres_submit_unique_receipt_replay_is_idempotent_after_period_elapses_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_receipt_idempotent_replay_test(database_url)
  }
}

fn run_receipt_idempotent_replay_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_receipt_idempotent",
  )
  let worker_def = unique_test_worker("unique.receipt-idempotent-" <> suffix)
  let test_queue = "receipt-idempotent-" <> suffix
  let assert Ok(registry_workers) = registry.new(test_queue)
  let assert Ok(registry_workers) =
    registry.register(registry_workers, worker_def)
  let assert Ok(consumer) =
    queue.start(database, registry_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let assert Ok(period) = unique.within_milliseconds(5000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.IncompleteOrSucceeded,
    )
  let submission_text = "unique-receipt-idempotent-1-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  // The 5-second period has elapsed: without the receipt, a fresh candidate
  // lookup for this key would now find nothing eligible.
  force_job_timestamp(
    connection,
    job.id_value(handle),
    "inserted_at",
    "clock_timestamp() - interval '6 seconds'",
  )

  let assert Ok(submission.Inserted(replayed_handle)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  job.id_value(replayed_handle) |> should.equal(job.id_value(handle))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  mark_database_test_executed("unique-receipt-idempotent-replay-passed")
}

/// Increment 7(b): the same `SubmissionId` with a different input conflicts.
pub fn postgres_submit_unique_receipt_replay_with_different_input_conflicts_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_receipt_different_input_conflict_test(database_url)
  }
}

fn run_receipt_different_input_conflict_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_receipt_conflict",
  )
  let worker_def = unique_test_worker("unique.receipt-conflict-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "receipt-conflict-" <> suffix
  let submission_text = "unique-receipt-conflict-1-" <> suffix

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  submit_keep_existing(
    database,
    test_queue,
    submission_text,
    worker_def,
    2,
    policy,
  )
  |> should.equal(Error(submission.SubmissionConflict))

  mark_database_test_executed("unique-receipt-different-input-conflict-passed")
}

/// `reconcile_unique` carrying a `PendingSubmission` whose fingerprint does
/// not match the receipt actually committed under this `SubmissionId` (as if
/// it were reconciling a different request B's `CommitUnknown` against an id
/// request A already committed under) must report `SubmissionConflict`, the
/// same as `submit_unique`'s own in-transaction receipt check — never
/// `CommitUnknown`, which would tell request B's caller to keep retrying an
/// admission that, correctly, already belongs to someone else forever.
pub fn postgres_reconcile_unique_mismatched_pending_reports_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_reconcile_unique_mismatched_pending_test(database_url)
  }
}

fn run_reconcile_unique_mismatched_pending_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_reconcile_mismatch_" <> suffix,
  )
  let worker_def = unique_test_worker("unique.reconcile-mismatch-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "reconcile-mismatch-" <> suffix
  let assert Ok(submission_id) =
    submission.submission_id("unique-reconcile-mismatch-" <> suffix)

  // Request A commits under `submission_id`.
  let assert Ok(submission.Inserted(_)) =
    postgres.submit_unique(
      database,
      test_queue,
      submission_id,
      worker_def,
      1,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )

  // A `PendingSubmission` standing in for a *different* request B, reusing
  // the same `submission_id` but carrying a fingerprint that does not match
  // what request A actually committed.
  let mismatched_pending =
    submission.new_pending_submission(
      postgres.storage_owner(database),
      submission_id,
      worker_def,
      <<9, 9, 9>>,
    )

  postgres.reconcile_unique(database, mismatched_pending)
  |> should.equal(Error(submission.SubmissionConflict))

  mark_database_test_executed(
    "reconcile-unique-mismatched-pending-conflict-passed",
  )
}

/// Increment 7(c): a replayed `Existing` decision returns the observed state
/// recorded in the receipt at decision time, not the row's current
/// (possibly since-progressed) state.
pub fn postgres_submit_unique_receipt_replay_returns_originally_observed_state_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_receipt_replay_observed_state_test(database_url)
  }
}

fn run_receipt_replay_observed_state_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_receipt_observed_state",
  )
  let worker_def = unique_test_worker("unique.receipt-observed-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "receipt-observed-" <> suffix
  let submission_replay = "unique-receipt-observed-2-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-receipt-observed-1-" <> suffix,
      worker_def,
      1,
      policy,
    )
  let assert Ok(submission.Existing(conflict_first)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_replay,
      worker_def,
      1,
      policy,
    )
  submission.conflict_state(conflict_first) |> should.equal(job.Queued)

  // The row genuinely progresses after the receipt was recorded.
  force_job_state(connection, job.id_value(handle), "succeeded")

  let assert Ok(submission.Existing(conflict_replayed)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_replay,
      worker_def,
      1,
      policy,
    )
  submission.conflict_job_id(conflict_replayed)
  |> should.equal(job.id_value(handle))
  submission.conflict_state(conflict_replayed) |> should.equal(job.Queued)

  mark_database_test_executed(
    "unique-receipt-replay-returns-observed-state-passed",
  )
}

/// Increment 7(d), proving R2 (Decision 9): replaying the same
/// `SubmissionId` and input against a worker whose output codec version has
/// changed conflicts, rather than returning a handle bound to a different
/// codec than the one it was originally admitted under.
pub fn postgres_submit_unique_receipt_replay_with_changed_output_codec_conflicts_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_receipt_output_codec_change_conflict_test(database_url)
  }
}

fn run_receipt_output_codec_change_conflict_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_receipt_codec_change",
  )
  let assert Ok(input_codec) =
    worker.codec(
      "unique-receipt-codec-input-" <> suffix <> "-v1",
      json.int,
      decode.int,
    )
  let assert Ok(output_codec_v1) =
    worker.codec(
      "unique-receipt-codec-output-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let worker_id = "unique.receipt-codec-change-" <> suffix
  let assert Ok(worker_v1) =
    worker.define(worker_id, "v1", input_codec, output_codec_v1, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(output_codec_v2) =
    worker.codec(
      "unique-receipt-codec-output-" <> suffix <> "-v2",
      json.string,
      decode.string,
    )
  let assert Ok(worker_v1_recoded) =
    worker.define(worker_id, "v1", input_codec, output_codec_v2, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "receipt-codec-change-" <> suffix
  let submission_text = "unique-receipt-codec-1-" <> suffix

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_v1,
      1,
      policy,
    )
  submit_keep_existing(
    database,
    test_queue,
    submission_text,
    worker_v1_recoded,
    1,
    policy,
  )
  |> should.equal(Error(submission.SubmissionConflict))

  mark_database_test_executed(
    "unique-receipt-output-codec-change-conflict-passed",
  )
}

// -- Increment 8: concurrent admission under a forced barrier --------------
//
// See `docs/RECOVERY-EVIDENCE.md`, Increment 8, for the mutation evidence
// (a genuine red run with the domain lock skipped, and one with the lock
// key widened to include the queue) these tests were checked against.

/// Three `submit_unique` calls, same key, `KeepExisting`, distinct
/// `SubmissionId`s, from three separate pools, forced to actually overlap by
/// a test-only `BEFORE INSERT` barrier trigger: exactly one settles as
/// `Inserted` and the other two settle as `Existing` referencing that same
/// job id, and exactly one row is ever persisted.
pub fn postgres_submit_unique_concurrent_admission_forced_overlap_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_concurrent_overlap_test(database_url)
  }
}

fn run_unique_concurrent_overlap_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.overlap-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-overlap-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  use entries <- with_unique_databases(database_url, [
    "grind_unique_overlap_a_" <> suffix,
    "grind_unique_overlap_b_" <> suffix,
    "grind_unique_overlap_c_" <> suffix,
  ])
  let assert [
    #(database_a, barrier_connection),
    #(database_b, _),
    #(database_c, _),
  ] = entries
  let lock_key = unique_test_lock_key(0)
  let cleanup_trigger =
    install_unique_insert_barrier(
      barrier_connection,
      "grind_test_unique_overlap_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_unique_overlap_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(barrier_connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let submission_a = "unique-overlap-a-" <> suffix
  let submission_b = "unique-overlap-b-" <> suffix
  let submission_c = "unique-overlap-c-" <> suffix
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  let result_c = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_keep_existing(
      database_a,
      test_queue,
      submission_a,
      worker_def,
      1,
      policy,
    )
  })
  spawn_submit(result_b, fn() {
    submit_keep_existing(
      database_b,
      test_queue,
      submission_b,
      worker_def,
      1,
      policy,
    )
  })
  spawn_submit(result_c, fn() {
    submit_keep_existing(
      database_c,
      test_queue,
      submission_c,
      worker_def,
      1,
      policy,
    )
  })

  // Exactly one submitter has won the domain lock and blocked inserting
  // behind the test's own held trigger lock; the other two are blocked
  // acquiring that same domain lock.
  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    2,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  let assert Ok(outcome_c) = process.receive(result_c, within: 5000)
  let outcomes = [outcome_a, outcome_b, outcome_c]

  let inserted_ids =
    list.filter_map(outcomes, fn(outcome) {
      case outcome {
        Ok(submission.Inserted(handle)) -> Ok(job.id_value(handle))
        _ -> Error(Nil)
      }
    })
  let existing_conflicts =
    list.filter_map(outcomes, fn(outcome) {
      case outcome {
        Ok(submission.Existing(conflict)) -> Ok(conflict)
        _ -> Error(Nil)
      }
    })
  list.length(inserted_ids) |> should.equal(1)
  list.length(existing_conflicts) |> should.equal(2)
  let assert [inserted_id] = inserted_ids
  list.each(existing_conflicts, fn(conflict) {
    submission.conflict_job_id(conflict) |> should.equal(inserted_id)
  })
  count_jobs_in_queue(barrier_connection, test_queue) |> should.equal(1)

  mark_database_test_executed("unique-concurrent-forced-overlap-passed")
}

/// The uniqueness domain lock key deliberately excludes queue
/// (`docs/UNIQUENESS-CONTRACT.md`, admission transaction step 3), so a
/// `WithinQueue` submission in one queue and an `AcrossQueues` submission in
/// another, on the same key, still serialize against each other: one row,
/// not two.
pub fn postgres_submit_unique_concurrent_admission_mixed_scope_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_concurrent_mixed_scope_test(database_url)
  }
}

fn run_unique_concurrent_mixed_scope_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.mixed-scope-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let queue_a = "unique-mixed-scope-q1-" <> suffix
  let queue_b = "unique-mixed-scope-q2-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy_within =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let policy_across =
    unique.policy(
      unique.full_input(),
      unique.AcrossQueues,
      period,
      unique.Incomplete,
    )

  use entries <- with_unique_databases(database_url, [
    "grind_unique_mixed_a_" <> suffix,
    "grind_unique_mixed_b_" <> suffix,
  ])
  let assert [#(database_a, barrier_connection), #(database_b, _)] = entries
  let lock_key = unique_test_lock_key(1)
  let cleanup_trigger =
    install_unique_insert_barrier(
      barrier_connection,
      "grind_test_unique_mixed_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_unique_mixed_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(barrier_connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let submission_a = "unique-mixed-scope-a-" <> suffix
  let submission_b = "unique-mixed-scope-b-" <> suffix
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_keep_existing(
      database_a,
      queue_a,
      submission_a,
      worker_def,
      1,
      policy_within,
    )
  })

  // A (`WithinQueue`, `queue_a`) must actually be blocked inserting behind
  // the barrier before B starts. A `WithinQueue` candidate query only ever
  // looks inside its own queue, so which of A/B reaches the domain lock
  // first is not incidental here the way it was for the same-queue overlap
  // test above: if B (`AcrossQueues`, `queue_b`) inserted *first*, A's own
  // `queue_a`-scoped candidate query would never see B's `queue_b` row and
  // would legitimately insert its own — two rows, correctly, by the
  // documented per-queue semantics `WithinQueue` already promises (Increment
  // 4). Starting A alone first and waiting for it to reach the barrier
  // fixes the order without weakening the concurrency being proved: B still
  // arrives while A's insert transaction is genuinely open and still needs
  // the same domain lock A holds, which is exactly what the lock key
  // excluding queue (`docs/UNIQUENESS-CONTRACT.md`, admission transaction
  // step 2) is being proved to guarantee.
  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    0,
    500,
  )
  |> should.equal(True)

  spawn_submit(result_b, fn() {
    submit_keep_existing(
      database_b,
      queue_b,
      submission_b,
      worker_def,
      1,
      policy_across,
    )
  })

  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    1,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(submission.Inserted(handle_a)) = outcome_a
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  let assert Ok(submission.Existing(conflict_b)) = outcome_b
  submission.conflict_job_id(conflict_b) |> should.equal(job.id_value(handle_a))
  submission.conflict_queue(conflict_b) |> should.equal(queue_a)
  count_jobs_in_queue(barrier_connection, queue_a) |> should.equal(1)
  count_jobs_in_queue(barrier_connection, queue_b) |> should.equal(0)

  mark_database_test_executed("unique-concurrent-mixed-scope-passed")
}

/// Increment 8's deferred receipt-ordering evidence: submitter A commits its
/// `Inserted` decision (and its receipt) while submitter B — the *same*
/// `SubmissionId` and the same request — is still waiting on the domain
/// lock A holds. Once A releases, B must return A's recorded `Inserted`
/// decision (same job id, same `JobHandle`-carrying variant), not a fresh
/// `Existing` conflict against the row A just committed — proving the
/// receipt lookup genuinely runs, and matches, before B ever performs its
/// own candidate selection.
pub fn postgres_submit_unique_receipt_ordering_returns_committed_decision_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_receipt_ordering_test(database_url)
  }
}

fn run_unique_receipt_ordering_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.receipt-order-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-receipt-order-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  use entries <- with_unique_databases(database_url, [
    "grind_unique_receipt_order_a_" <> suffix,
    "grind_unique_receipt_order_b_" <> suffix,
  ])
  let assert [#(database_a, barrier_connection), #(database_b, _)] = entries
  let lock_key = unique_test_lock_key(2)
  let cleanup_trigger =
    install_unique_insert_barrier(
      barrier_connection,
      "grind_test_unique_receipt_order_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_unique_receipt_order_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(barrier_connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let shared_submission = "unique-receipt-order-shared-" <> suffix
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_keep_existing(
      database_a,
      test_queue,
      shared_submission,
      worker_def,
      1,
      policy,
    )
  })

  // A must actually be blocked inserting behind the barrier before B
  // starts, or B could race A for the domain lock instead of waiting
  // behind it.
  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    0,
    500,
  )
  |> should.equal(True)

  spawn_submit(result_b, fn() {
    submit_keep_existing(
      database_b,
      test_queue,
      shared_submission,
      worker_def,
      1,
      policy,
    )
  })

  // B must be waiting on the domain lock A still holds before we release A
  // — otherwise this run proves nothing about ordering.
  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    1,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(submission.Inserted(handle_a)) = outcome_a
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  let assert Ok(submission.Inserted(handle_b)) = outcome_b
  job.id_value(handle_b) |> should.equal(job.id_value(handle_a))
  count_jobs_in_queue(barrier_connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "unique-receipt-ordering-b-returns-a-decision-passed",
  )
}

// -- Increment 9: contention -------------------------------------------------
//
// See `docs/RECOVERY-EVIDENCE.md`, Increment 9, for this section's evidence.

/// The test itself holds the real domain lock (via `unique_domain_lock_query`,
/// built from the same `@internal lock_key_sql` production code uses) in its
/// own open transaction; a concurrent `submit_unique` with a 200ms
/// `unique_lock_wait` for the same key contends and reports
/// `AdmissionContended`, with no job row and no receipt. Once the lock is
/// released, the same `SubmissionId` succeeds.
pub fn postgres_submit_unique_contended_lock_wait_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_contended_lock_wait_test(database_url)
  }
}

fn run_unique_contended_lock_wait_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.contended-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-contended-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  let assert Ok(holder_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(holder_database) = postgres.start(holder_settings)
  use <- exception.defer(fn() { postgres.close(holder_database) })
  let assert Ok(Nil) = postgres.migrate(holder_database)
  let holder_connection = postgres.connection(holder_database)
  let storage_owner = postgres.storage_owner(holder_database)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      holder_connection,
      unique_domain_lock_query(holder_database, worker_def, 1),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(submitter_settings) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(200)
    |> postgres.validate
  let assert Ok(submitter_database) = postgres.start(submitter_settings)
  use <- exception.defer(fn() { postgres.close(submitter_database) })

  let submission_text = "unique-contended-1-" <> suffix
  submit_keep_existing(
    submitter_database,
    test_queue,
    submission_text,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.AdmissionContended))
  count_jobs_in_queue(holder_connection, test_queue) |> should.equal(0)
  unique_receipt_exists(holder_connection, storage_owner, submission_text)
  |> should.equal(False)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      submitter_database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  count_jobs_in_queue(holder_connection, test_queue) |> should.equal(1)

  mark_database_test_executed("unique-contended-lock-wait-passed")
}

/// DEFECT 1 (docs/RELEASE-READINESS.md, "Defects found while designing the
/// deadline"): the same contention as above, but with *default*
/// `postgres.settings` on the submitter — no `unique_lock_wait` override.
/// Before the fix, the default `unique_lock_wait_ms` (5000) was equal to
/// pgo's own hardcoded pool checkout deadline (also ~5000 ms — see
/// `docs/RECOVERY-EVIDENCE.md`, "Acknowledgement deadline"), so contention
/// could surface as the checkout being force-closed
/// (`NotCommitted(pog.QueryTimeout)`/`CommitUnknown`) instead of the
/// clean, typed `AdmissionContended` a caller can actually branch on — a
/// race, not deterministically wrong every time, which is exactly why it
/// went unnoticed: `postgres_submit_unique_contended_lock_wait_test` above
/// always overrode `unique_lock_wait` to 200 ms and never exercised the
/// default at all. `postgres.validate` now rejects any `Settings` where
/// `unique_lock_wait_ms` is within `unique_lock_wait_margin_ms` (1000 ms) of
/// `statement_deadline_ms` (`UniqueLockWaitTooCloseToDeadline`), and the
/// shipped defaults (`unique_lock_wait_ms` 2000, `statement_deadline_ms`
/// 4000) clear that margin — so this is deterministic today, not a race.
pub fn postgres_submit_unique_contended_lock_wait_default_settings_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_unique_contended_lock_wait_default_settings_test(database_url)
  }
}

fn run_unique_contended_lock_wait_default_settings_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.contended-default-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-contended-default-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  let assert Ok(holder_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(holder_database) = postgres.start(holder_settings)
  use <- exception.defer(fn() { postgres.close(holder_database) })
  let assert Ok(Nil) = postgres.migrate(holder_database)
  let holder_connection = postgres.connection(holder_database)
  let storage_owner = postgres.storage_owner(holder_database)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      holder_connection,
      unique_domain_lock_query(holder_database, worker_def, 1),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  // No `unique_lock_wait` override: exercises the shipped default exactly
  // as any caller who never touches this setting would experience it.
  let assert Ok(submitter_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(submitter_database) = postgres.start(submitter_settings)
  use <- exception.defer(fn() { postgres.close(submitter_database) })

  let submission_text = "unique-contended-default-1-" <> suffix
  submit_keep_existing(
    submitter_database,
    test_queue,
    submission_text,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.AdmissionContended))
  count_jobs_in_queue(holder_connection, test_queue) |> should.equal(0)
  unique_receipt_exists(holder_connection, storage_owner, submission_text)
  |> should.equal(False)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      submitter_database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  count_jobs_in_queue(holder_connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "unique-contended-lock-wait-default-settings-passed",
  )
}

/// The row-lock variant of contention: a scheduled row's own row lock (held
/// by the test through an open `SELECT ... FOR UPDATE` transaction, not the
/// domain lock) blocks a `RescheduleScheduledTo` submission's candidate
/// selection (which takes that same row lock, per
/// `docs/UNIQUENESS-CONTRACT.md`'s admission transaction step 6) until its
/// 200ms `unique_lock_wait` elapses; the row is left completely unchanged.
/// Once released, the same reschedule request succeeds.
pub fn postgres_submit_unique_reschedule_row_lock_contention_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_row_lock_test(database_url)
  }
}

fn run_unique_reschedule_row_lock_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_lock_" <> suffix,
  )
  let worker_id = "unique.reschedule-lock-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-reschedule-lock-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.ScheduledOnly,
    )

  let far_future = future_available_at(connection, 3_600_000)
  let assert Ok(seed_submission) =
    submission.submission_id("unique-reschedule-lock-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      1,
      submission.At(far_future),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let original_available_at_ms =
    job_available_at_ms(connection, job.id_value(handle))

  let row_lock_query =
    pog.query("SELECT 1 FROM grind_jobs WHERE id = $1 FOR UPDATE")
    |> pog.parameter(pog.int(job.id_value(handle)))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, row_lock_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(contended_settings) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(200)
    |> postgres.validate
  let assert Ok(contended_database) = postgres.start(contended_settings)
  use <- exception.defer(fn() { postgres.close(contended_database) })

  let later_target = future_available_at(connection, 7_200_000)
  let reschedule_submission = "unique-reschedule-lock-retry-" <> suffix
  submit_reschedule(
    contended_database,
    test_queue,
    reschedule_submission,
    worker_def,
    1,
    policy,
    later_target,
  )
  |> should.equal(Error(submission.AdmissionContended))
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  job_available_at_ms(connection, job.id_value(handle))
  |> should.equal(original_available_at_ms)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(submission.Rescheduled(conflict)) =
    submit_reschedule(
      contended_database,
      test_queue,
      reschedule_submission,
      worker_def,
      1,
      policy,
      later_target,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))
  job_available_at_ms(connection, job.id_value(handle))
  |> should.equal(job.available_at_unix_milliseconds(later_target))

  mark_database_test_executed("unique-reschedule-row-lock-contention-passed")
}

/// `set_config('lock_timeout', ..., true)` (step 1 of the admission
/// transaction) is transaction-local: it must not leak into a later
/// statement that reuses the same pooled physical connection. A
/// single-connection pool guarantees the reuse; after a contended attempt
/// on it, `SHOW lock_timeout` on that same pool must read back the
/// cluster's own default, not `200ms`.
pub fn postgres_unique_lock_timeout_does_not_leak_to_later_statements_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_lock_timeout_no_leak_test(database_url)
  }
}

fn run_unique_lock_timeout_no_leak_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.timeout-leak-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-timeout-leak-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  let assert Ok(holder_settings) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(holder_database) = postgres.start(holder_settings)
  use <- exception.defer(fn() { postgres.close(holder_database) })
  let assert Ok(Nil) = postgres.migrate(holder_database)
  let holder_connection = postgres.connection(holder_database)

  let #(lock_ready, lock_finished) =
    spawn_lock_holder(
      holder_connection,
      unique_domain_lock_query(holder_database, worker_def, 1),
    )
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let assert Ok(probe_settings) =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
    |> postgres.with_unique_lock_wait(200)
    |> postgres.validate
  let assert Ok(probe_database) = postgres.start(probe_settings)
  use <- exception.defer(fn() { postgres.close(probe_database) })

  submit_keep_existing(
    probe_database,
    test_queue,
    "unique-timeout-leak-1-" <> suffix,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.AdmissionContended))

  let probe_connection = postgres.connection(probe_database)
  let show_lock_timeout = fn() {
    let assert Ok(returned) =
      pog.query("SHOW lock_timeout")
      |> pog.returning({
        use value <- decode.field(0, decode.string)
        decode.success(value)
      })
      |> pog.execute(on: probe_connection)
    let assert [value] = returned.rows
    value
  }

  // A *rolled-back* transaction's `SET`/`set_config` change is undone
  // regardless of `is_local` — PostgreSQL reverts GUC changes made inside
  // an aborted transaction either way, so a contended (and hence
  // rolled-back) attempt alone cannot distinguish `is_local: true` from
  // `false`. Checked anyway, for completeness, but the assertion below
  // (after a *committed* attempt on this same connection) is the one that
  // actually exercises `is_local`'s documented difference.
  show_lock_timeout() |> should.equal("0")

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  // With the domain lock now free, this same pooled connection commits a
  // fresh submission. `is_local: true` (`set_config`'s third argument)
  // means `SET LOCAL`-style transaction-local scope: the setting reverts at
  // COMMIT, not only at ROLLBACK. `is_local: false` would instead behave
  // like a plain session-level `SET`, which survives the COMMIT and would
  // leave `lock_timeout` at `200` for every later statement on this pool.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      probe_database,
      test_queue,
      "unique-timeout-leak-2-" <> suffix,
      worker_def,
      1,
      policy,
    )
  show_lock_timeout() |> should.equal("0")

  mark_database_test_executed("unique-lock-timeout-no-leak-passed")
}

// -- Isolation-level pinning (R1) --------------------------------------------
//
// See `docs/RECOVERY-EVIDENCE.md`, "Isolation-level pinning", for the red
// evidence this test was checked against.

/// The admission transaction's correctness (a waiter's plain reads after the
/// domain lock must see whatever committed while it waited) depends on
/// `READ COMMITTED` semantics, not merely the cluster's *default* being
/// `READ COMMITTED` — a role or database configured with
/// `default_transaction_isolation = 'repeatable read'` would otherwise
/// silently break admission with no code-visible signal. This test runs the
/// same forced-overlap barrier as Increment 8's main test, but against a
/// dedicated disposable database whose own configured default really is
/// `repeatable read` (`GRIND_TEST_REPEATABLE_READ_URL`,
/// `scripts/test-postgres.sh`), and proves the admission transaction still
/// produces exactly one `Inserted` and one `Existing` against that same job
/// id — not two rows.
pub fn postgres_submit_unique_admission_safe_under_repeatable_read_test() {
  case repeatable_read_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_repeatable_read_test(database_url)
  }
}

fn run_unique_repeatable_read_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let worker_id = "unique.repeatable-read-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-repeatable-read-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  use entries <- with_unique_databases(database_url, [
    "grind_unique_rr_a_" <> suffix,
    "grind_unique_rr_b_" <> suffix,
  ])
  let assert [#(database_a, barrier_connection), #(database_b, _)] = entries

  // Confirm this database's own *persisted, configured* default directly,
  // rather than trust `scripts/test-postgres.sh`'s setup silently — and
  // read it from `pg_db_role_setting`/`pg_database`, not `SHOW
  // default_transaction_isolation` on a Grind-managed connection: Grind's
  // own pool now pins `default_transaction_isolation` to `read committed`
  // as a startup connection parameter (see `postgres.validate`), so a
  // Grind connection's *active* session setting reads `read committed`
  // regardless of what this database is configured to default to. The
  // catalog query below reads the database-level configuration itself,
  // which this connection's own override does not change.
  let assert Ok(isolation_returned) =
    pog.query(
      "SELECT EXISTS (SELECT 1 FROM pg_db_role_setting JOIN pg_database ON pg_database.oid = pg_db_role_setting.setdatabase WHERE pg_database.datname = current_database() AND pg_db_role_setting.setrole = 0 AND EXISTS (SELECT 1 FROM unnest(pg_db_role_setting.setconfig) AS cfg WHERE cfg = 'default_transaction_isolation=repeatable read'))",
    )
    |> pog.returning({
      use configured <- decode.field(0, decode.bool)
      decode.success(configured)
    })
    |> pog.execute(on: barrier_connection)
  let assert [True] = isolation_returned.rows

  let lock_key = unique_test_lock_key(3)
  let cleanup_trigger =
    install_unique_insert_barrier(
      barrier_connection,
      "grind_test_unique_rr_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_unique_rr_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(barrier_connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net: releases the barrier unconditionally on the way out,
  // registered after the trigger-cleanup defer above so it unwinds first —
  // a panic between here and the explicit release below must not leave a
  // deferred `DROP TRIGGER`/`DROP FUNCTION` waiting (up to
  // `spawn_lock_holder`'s own 10-second bound) on a transaction still
  // blocked inside that very trigger. Sending `ReleaseAttempt` again after
  // the explicit release further down is harmless (the holder process has
  // already exited by then).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let submission_a = "unique-rr-a-" <> suffix
  let submission_b = "unique-rr-b-" <> suffix
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_keep_existing(
      database_a,
      test_queue,
      submission_a,
      worker_def,
      1,
      policy,
    )
  })
  spawn_submit(result_b, fn() {
    submit_keep_existing(
      database_b,
      test_queue,
      submission_b,
      worker_def,
      1,
      policy,
    )
  })

  await_overlap_shape(
    barrier_connection,
    unique_insert_query_like,
    unique_domain_lock_query_like,
    1,
    1,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  let outcomes = [outcome_a, outcome_b]

  let inserted_ids =
    list.filter_map(outcomes, fn(outcome) {
      case outcome {
        Ok(submission.Inserted(handle)) -> Ok(job.id_value(handle))
        _ -> Error(Nil)
      }
    })
  let existing_conflicts =
    list.filter_map(outcomes, fn(outcome) {
      case outcome {
        Ok(submission.Existing(conflict)) -> Ok(conflict)
        _ -> Error(Nil)
      }
    })
  list.length(inserted_ids) |> should.equal(1)
  list.length(existing_conflicts) |> should.equal(1)
  let assert [inserted_id] = inserted_ids
  let assert [existing_conflict] = existing_conflicts
  submission.conflict_job_id(existing_conflict) |> should.equal(inserted_id)
  count_jobs_in_queue(barrier_connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "unique-admission-safe-under-repeatable-read-passed",
  )
}

/// A retried (duplicate) acknowledgement forced to genuinely overlap the
/// first one's commit: A's fenced `UPDATE` is blocked behind a test-only
/// `BEFORE UPDATE` barrier trigger scoped to this job (the same
/// held-then-released-on-cue shape used throughout); B — the identical
/// acknowledgement command, from a separate pool — starts while A is still
/// blocked, and B's own fenced `UPDATE` then genuinely waits on the row
/// lock A's in-flight `UPDATE` holds (a real PostgreSQL tuple-lock wait,
/// confirmed via `pg_stat_activity`'s `transactionid` wait event — not the
/// advisory wait A is parked on). Once A completes and commits, B's
/// `UPDATE` no longer matches (the row is no longer `executing`), so B
/// falls through to `acknowledge_transaction`'s own re-read of the
/// acknowledgement receipt and must return `Ok(True)`, exactly as A did —
/// not a query failure. See `docs/RECOVERY-EVIDENCE.md`, "Isolation-level
/// pinning", for the genuine red this test produced before
/// `postgres.validate` pinned every pooled connection's own
/// `default_transaction_isolation` to `read committed`.
pub fn postgres_ack_duplicate_reports_ok_under_pinned_isolation_test() {
  case repeatable_read_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_ack_duplicate_repeatable_read_test(database_url)
  }
}

fn run_ack_duplicate_repeatable_read_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use entries <- with_unique_databases(database_url, [
    "grind_ack_rr_a_" <> suffix,
    "grind_ack_rr_b_" <> suffix,
  ])
  let assert [#(database_a, connection_a), #(database_b, _)] = entries

  let assert Ok(input_codec) =
    worker.codec("ack-rr-input-" <> suffix <> "-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "ack-rr-output-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "ack.rr-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("ack-rr-" <> suffix)
  let assert Ok(workers) = registry.register(workers, definition)
  let test_queue = "ack-rr-" <> suffix
  let attempt_owner = "ack-rr-owner-" <> suffix

  let assert Ok(_handle) =
    postgres.submit(database_a, test_queue, definition, 8)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(database_a, test_queue, workers, attempt_owner, 30_000)
  let execution = attempt.execute_claim(claimed)
  let #(job_id, _, _) = attempt.claim_identity(claimed)

  let lock_key = unique_test_lock_key(4)
  let trigger_name = "grind_test_ack_overlap_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND OLD.state = 'executing' AND NEW.state <> 'executing' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection_a)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> trigger_name
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection_a)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection_a)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection_a)
    Nil
  })

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_ack_overlap_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection_a, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net (see the uniqueness barrier tests above for why this is
  // registered here, right after obtaining `release_lock`, rather than only
  // sending it explicitly further down).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    attempt.acknowledge(
      database_a,
      test_queue,
      attempt_owner,
      claimed,
      execution,
    )
  })

  // A must actually be blocked updating behind the barrier (an advisory
  // wait) before B starts.
  await_lock_wait_counts(connection_a, 1, 0, 500) |> should.equal(True)

  spawn_submit(result_b, fn() {
    attempt.acknowledge(
      database_b,
      test_queue,
      attempt_owner,
      claimed,
      execution,
    )
  })

  // B must be genuinely waiting on the row lock A's own `UPDATE` holds
  // (`transactionid`, a real tuple-lock wait — not the advisory wait A is
  // parked on) before we release the barrier, or this proves nothing about
  // the overlap.
  await_lock_wait_counts(connection_a, 1, 1, 500) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  outcome_a |> should.equal(Ok(True))
  outcome_b |> should.equal(Ok(True))
  count_acknowledgements_for_job(connection_a, job_id) |> should.equal(1)

  mark_database_test_executed("ack-duplicate-ok-under-pinned-isolation-passed")
}

/// R4: `reconcile_matching_owner`'s idempotency check
/// (`resolution_receipt_outcome`, shared with `apply_uncertain_resolution`'s
/// own re-check after its `FOR UPDATE`) runs once before that lock and is
/// never re-checked when the locked row is no longer `uncertain` — a
/// concurrent retry of the *same* `resolution_id` and payload that waits
/// behind the first resolution's row lock would otherwise misreport
/// `ReconciliationNotRequired` instead of the recorded
/// `ResolutionAlreadyApplied` outcome. Forced to genuinely overlap: A's
/// `write_resolution` update is blocked behind a test-only `BEFORE UPDATE`
/// barrier trigger scoped to this job (`OLD.state = 'uncertain'`); B — the
/// identical resolution command, from a separate pool — starts while A is
/// blocked, and B's own `SELECT ... FOR UPDATE` then genuinely waits on the
/// row lock A already holds (confirmed via `pg_stat_activity`'s
/// `transactionid` wait event). Once A completes and commits, B must return
/// `Ok(ResolutionAlreadyApplied(target_state))` — not
/// `Error(ReconciliationNotRequired)` — and only one resolution row and one
/// state transition (one redelivery) must exist.
pub fn postgres_resolution_concurrent_same_outcome_applied_once_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_resolution_concurrent_test(database_url)
  }
}

fn run_resolution_concurrent_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use entries <- with_unique_databases(database_url, [
    "grind_resolution_concurrent_a_" <> suffix,
    "grind_resolution_concurrent_b_" <> suffix,
  ])
  let assert [#(database_a, connection_a), #(database_b, _)] = entries

  let assert Ok(input_codec) =
    worker.codec(
      "resolution-concurrent-input-" <> suffix <> "-v1",
      json.int,
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "resolution-concurrent-output-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "resolution.concurrent-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let test_queue = "resolution-concurrent-" <> suffix
  let assert Ok(handle) = postgres.submit(database_a, test_queue, definition, 3)
  let #(job_id, _, _, _, _, _) = job.storage_fields(handle)

  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 1, attempt_epoch = 1, attempt_owner = 'resolution-concurrent-owner', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection_a)

  let lock_key = unique_test_lock_key(5)
  let trigger_name = "grind_test_resolution_overlap_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND OLD.state = 'uncertain' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection_a)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> trigger_name
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection_a)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection_a)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection_a)
    Nil
  })

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_resolution_overlap_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection_a, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let resolution_id = "resolution-concurrent-" <> suffix
  let resolved_by = "on-call-" <> suffix
  let details = "confirm external idempotency record before replay"
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    postgres.resolve_uncertain(
      database_a,
      handle,
      postgres.ResolutionRequest(
        resolution_id,
        resolved_by,
        details,
        postgres.AuthorizeReplay,
      ),
    )
  })

  // A must actually be blocked inside `write_resolution`'s `UPDATE`,
  // behind the barrier (an advisory wait), before B starts.
  await_lock_wait_counts(connection_a, 1, 0, 500) |> should.equal(True)

  spawn_submit(result_b, fn() {
    postgres.resolve_uncertain(
      database_b,
      handle,
      postgres.ResolutionRequest(
        resolution_id,
        resolved_by,
        details,
        postgres.AuthorizeReplay,
      ),
    )
  })

  // B must be genuinely waiting on the row lock A's own `SELECT ... FOR
  // UPDATE` (still held through A's blocked `UPDATE`) holds
  // (`transactionid`, a real tuple-lock wait — not the advisory wait A is
  // parked on) before we release the barrier.
  await_lock_wait_counts(connection_a, 1, 1, 500) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  outcome_a |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  outcome_b |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))

  let assert Ok(resolution_count_returned) =
    pog.query(
      "SELECT count(*)::bigint FROM grind_job_resolutions WHERE job_id = $1 AND resolution_id = $2",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.text(resolution_id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection_a)
  let assert [1] = resolution_count_returned.rows
  postgres.state(database_a, handle) |> should.equal(Ok(job.Queued))

  mark_database_test_executed("resolution-concurrent-same-outcome-applied-once")
}

// -- Increment 10: rescheduling ---------------------------------------------
//
// Full contract: `docs/UNIQUENESS-CONTRACT.md`, `ConflictAction`,
// `RescheduleScheduledTo`, and admission transaction steps 5-6.
// `postgres_submit_unique_reschedule_row_lock_contention_test` above already
// proves lock contention on the reschedule candidate's row; the tests below
// prove the reschedule decision itself (moving `available_at`, leaving other
// states alone, making a rescheduled row genuinely claimable, and the live
// race against a real claim).

/// A scheduled conflict rescheduled to `t2` settles `Rescheduled`;
/// `available_at` in the database equals `t2` exactly; the job id, worker,
/// and input are unchanged (rebinding the same id under the same worker
/// still reads the originally submitted input); the receipt records both the
/// previous and the new `available_at`.
pub fn postgres_submit_unique_reschedule_moves_available_at_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_basic_test(database_url)
  }
}

fn run_unique_reschedule_basic_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_basic_" <> suffix,
  )
  let worker_id = "unique.reschedule-basic-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-reschedule-basic-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.ScheduledOnly,
    )

  let original_target = future_available_at(connection, 3_600_000)
  let assert Ok(seed_submission) =
    submission.submission_id("unique-reschedule-basic-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      7,
      submission.At(original_target),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let job_id = job.id_value(handle)

  let new_target = future_available_at(connection, 7_200_000)
  let reschedule_submission = "unique-reschedule-basic-retry-" <> suffix
  let assert Ok(submission.Rescheduled(conflict)) =
    submit_reschedule(
      database,
      test_queue,
      reschedule_submission,
      worker_def,
      7,
      policy,
      new_target,
    )

  submission.conflict_job_id(conflict) |> should.equal(job_id)
  submission.conflict_queue(conflict) |> should.equal(test_queue)
  submission.conflict_state(conflict) |> should.equal(job.Scheduled)
  job_available_at_ms(connection, job_id)
  |> should.equal(job.available_at_unix_milliseconds(new_target))

  // The job id, worker, and input are unchanged: the same id under the same
  // worker still reads the originally submitted input, and only one row for
  // this key exists.
  postgres.arguments(database, handle) |> should.equal(Ok(7))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  let #(from_ms, to_ms) =
    unique_receipt_reschedule_fields(
      connection,
      postgres.storage_owner(database),
      reschedule_submission,
    )
  from_ms
  |> should.equal(Some(job.available_at_unix_milliseconds(original_target)))
  to_ms |> should.equal(Some(job.available_at_unix_milliseconds(new_target)))

  mark_database_test_executed("unique-reschedule-moves-available-at-passed")
}

/// A `RescheduleScheduledTo` submission against a conflict in `queued`,
/// `retryable`, `executing`, or `uncertain` — every non-`scheduled` state
/// `Incomplete` admits — settles `Existing` with the row completely
/// unchanged (`available_at` untouched), never `Rescheduled`. Oban's own
/// "replacing fields based on job state" is inspired-by-upstream here,
/// limited to `available_at` on `scheduled` rows specifically.
pub fn postgres_submit_unique_reschedule_leaves_non_scheduled_states_unchanged_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_non_scheduled_test(database_url)
  }
}

fn run_unique_reschedule_non_scheduled_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_states_" <> suffix,
  )
  let worker_def = unique_test_worker("unique.reschedule-states-" <> suffix)
  let test_queue = "unique-reschedule-states-" <> suffix
  let period = unique.while_retained()
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )

  let cases = [
    #("queued", job.Queued, 501),
    #("retryable", job.Retryable, 502),
    #("executing", job.Executing, 503),
    #("uncertain", job.Uncertain, 504),
  ]

  cases
  |> list.each(fn(entry) {
    let #(stored, expected_state, input_value) = entry
    let assert Ok(submission.Inserted(handle)) =
      submit_keep_existing(
        database,
        test_queue,
        "unique-reschedule-states-seed-" <> suffix <> "-" <> stored,
        worker_def,
        input_value,
        policy,
      )
    let job_id = job.id_value(handle)
    force_job_state(connection, job_id, stored)
    let before_ms = job_available_at_ms(connection, job_id)

    let target = future_available_at(connection, 3_600_000)
    let assert Ok(submission.Existing(conflict)) =
      submit_reschedule(
        database,
        test_queue,
        "unique-reschedule-states-check-" <> suffix <> "-" <> stored,
        worker_def,
        input_value,
        policy,
        target,
      )
    submission.conflict_job_id(conflict) |> should.equal(job_id)
    submission.conflict_state(conflict) |> should.equal(expected_state)
    job_available_at_ms(connection, job_id) |> should.equal(before_ms)
  })

  mark_database_test_executed(
    "unique-reschedule-non-scheduled-unchanged-passed",
  )
}

/// Rescheduling a `scheduled` row to a due (past) database time makes it
/// genuinely claimable by a real manually-driven consumer on its very next
/// `process_one` call.
pub fn postgres_submit_unique_reschedule_to_due_time_makes_row_claimable_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_claimable_test(database_url)
  }
}

fn run_unique_reschedule_claimable_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_claimable_" <> suffix,
  )
  let worker_id = "unique.reschedule-claimable-" <> suffix
  let worker_def = unique_test_worker(worker_id)
  let test_queue = "unique-reschedule-claimable-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.ScheduledOnly,
    )

  let far_future = future_available_at(connection, 3_600_000)
  let assert Ok(seed_submission) =
    submission.submission_id("unique-reschedule-claimable-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      9,
      submission.At(far_future),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  let due_target = future_available_at(connection, -2000)
  let assert Ok(submission.Rescheduled(_)) =
    submit_reschedule(
      database,
      test_queue,
      "unique-reschedule-claimable-retry-" <> suffix,
      worker_def,
      9,
      policy,
      due_target,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  let assert Ok(registry_workers) = registry.new(test_queue)
  let assert Ok(registry_workers) =
    registry.register(registry_workers, worker_def)
  let assert Ok(consumer) =
    queue.start(database, registry_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))

  mark_database_test_executed("unique-reschedule-due-time-claimable-passed")
}

/// The live race between a manual consumer's claim (which locks the
/// scheduled row as part of its own claim `UPDATE`) and a concurrent
/// `RescheduleScheduledTo` submission for the same key (whose candidate
/// selection also locks that row, per `docs/UNIQUENESS-CONTRACT.md`'s
/// admission transaction step 5). The claim is blocked mid-`UPDATE` — after
/// it has already locked the row via its own `FOR UPDATE SKIP LOCKED`
/// candidate CTE, before it commits — by a `BEFORE UPDATE` trigger scoped to
/// this job id (the same `grind_test_claim_overlap` shape
/// `run_overlapping_claim_test` uses), holding a fresh test-only advisory
/// lock via `spawn_lock_holder`; the reschedule submission's own candidate
/// `SELECT ... FOR UPDATE` then genuinely waits on the row lock the claim's
/// still-open transaction holds (`pg_stat_activity`'s `transactionid` wait
/// event — a real tuple-lock wait, not a second advisory wait, confirmed
/// alongside the claim's own advisory wait via `await_lock_wait_counts`).
/// Releasing the barrier lets the claim finish (`state -> executing`,
/// commit), which releases the row lock. PostgreSQL's own `EvalPlanQual`
/// re-check for a `SELECT ... FOR UPDATE` whose target row was concurrently
/// updated then re-evaluates the reschedule submission's eligible-states
/// filter against the row's *fresh* post-commit state, not the stale
/// `scheduled` value the row had when the wait began:
///
/// - Under `Incomplete` (which admits `executing`), the fresh state still
///   matches the filter, so the row is returned with `state = "executing"`;
///   the Gleam-level `RescheduleScheduledTo(_), "scheduled"` pattern match
///   does not fire, and the call settles `Existing` with the observed
///   `Executing` state, `available_at` completely untouched.
/// - Under `ScheduledOnly` (which does not admit `executing`), the fresh
///   state no longer matches the filter at all, so `find_candidate` returns
///   no row and the call settles `Inserted` — a fresh row.
///
/// The claimed job's handler (`unique_test_blocking_worker`) deliberately
/// blocks rather than returning immediately, and is released only *after*
/// this test has already observed the reschedule submission's own result: a
/// handler that returns immediately would let the coordinator's own
/// subsequent acknowledgement race ahead to `succeeded` (which `Incomplete`
/// does not admit either) before the reschedule's blocked row lock is even
/// granted — an environment-dependent race, not a deterministic proof. The
/// handler starting (`FirstAttemptStarted`) is itself proof the claim's row
/// lock has already been released (the claim's `UPDATE` commits, as a
/// single-statement transaction, strictly before the coordinator invokes
/// the handler), so waiting for it before checking the reschedule's result
/// is a real synchronization point, not a sleep.
pub fn postgres_submit_unique_reschedule_race_incomplete_returns_existing_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_unique_reschedule_claim_race_test(
        database_url,
        unique.Incomplete,
        6,
        "unique-reschedule-race-incomplete-existing-passed",
      )
  }
}

pub fn postgres_submit_unique_reschedule_race_scheduled_only_returns_inserted_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_unique_reschedule_claim_race_test(
        database_url,
        unique.ScheduledOnly,
        7,
        "unique-reschedule-race-scheduled-only-inserted-passed",
      )
  }
}

fn run_unique_reschedule_claim_race_test(
  database_url: String,
  states: unique.States,
  salt: Int,
  marker: String,
) -> Nil {
  let run_id = unique_test_suffix()
  let suffix = run_id <> "-" <> int.to_string(salt)
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_reschedule_race_" <> suffix,
  )
  let worker_id = "unique.reschedule-race-" <> suffix
  let handler_started = process.new_subject()
  let worker_def = unique_test_blocking_worker(worker_id, handler_started)
  let test_queue = "unique-reschedule-race-" <> suffix
  let assert Ok(registry_workers) = registry.new(test_queue)
  let assert Ok(registry_workers) =
    registry.register(registry_workers, worker_def)
  let assert Ok(consumer) =
    queue.start(database, registry_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let period = unique.while_retained()
  let policy =
    unique.policy(unique.full_input(), unique.WithinQueue, period, states)

  let far_future = future_available_at(connection, 3_600_000)
  let assert Ok(seed_submission) =
    submission.submission_id("unique-reschedule-race-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      11,
      submission.At(far_future),
      policy,
      unique.KeepExisting,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let job_id = job.id_value(handle)
  force_available_at_due(connection, job_id)
  let original_available_at_ms = job_available_at_ms(connection, job_id)

  let lock_key = unique_test_lock_key(salt)
  let trigger_name =
    "grind_test_reschedule_race_" <> run_id <> "_" <> int.to_string(salt)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND NEW.state = 'executing' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> trigger_name
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection)
    Nil
  })

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_reschedule_race_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  // Safety net (see the uniqueness barrier tests above for why this is
  // registered right after obtaining `release_lock`).
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let claim_reply = process.new_subject()
  spawn_submit(claim_reply, fn() { queue.process_one(consumer) })
  await_claim_waiting_on_advisory(connection, 250) |> should.equal(True)

  let reschedule_target = future_available_at(connection, 7_200_000)
  let reschedule_submission = "unique-reschedule-race-retry-" <> suffix
  let reschedule_reply = process.new_subject()
  spawn_submit(reschedule_reply, fn() {
    submit_reschedule(
      database,
      test_queue,
      reschedule_submission,
      worker_def,
      11,
      policy,
      reschedule_target,
    )
  })

  // The claim is blocked in its trigger (advisory wait) and the reschedule
  // submission is genuinely waiting on the row lock the claim's still-open
  // transaction holds (a real tuple-lock wait) — both from one snapshot.
  await_lock_wait_counts(connection, 1, 1, 500) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  // The claim's own `UPDATE` has now committed (`state = 'executing'`, its
  // row lock released) and the coordinator has invoked the handler, which
  // blocks immediately — the row stays genuinely `executing` (no
  // acknowledgement has run yet) for as long as this barrier is held, which
  // is deterministically until this test releases it below, *after*
  // confirming the reschedule submission's own result. Without this
  // barrier, a handler that returns immediately would let the
  // acknowledgement race ahead to `succeeded` before the reschedule's
  // blocked row lock is even granted — an environment-dependent race, not
  // proof.
  let assert Ok(FirstAttemptStarted(handler_release)) =
    process.receive(handler_started, within: 5000)

  let assert Ok(reschedule_result) =
    process.receive(reschedule_reply, within: 5000)

  process.send(handler_release, ReleaseAttempt)
  process.receive(claim_reply, within: 5000) |> should.equal(Ok(Ok(True)))

  case states {
    unique.Incomplete -> {
      let assert Ok(submission.Existing(conflict)) = reschedule_result
      submission.conflict_job_id(conflict) |> should.equal(job_id)
      submission.conflict_state(conflict) |> should.equal(job.Executing)
      job_available_at_ms(connection, job_id)
      |> should.equal(original_available_at_ms)
      count_jobs_in_queue(connection, test_queue) |> should.equal(1)
    }
    unique.ScheduledOnly -> {
      let assert Ok(submission.Inserted(new_handle)) = reschedule_result
      job.id_value(new_handle) |> should.not_equal(job_id)
      count_jobs_in_queue(connection, test_queue) |> should.equal(2)
    }
    _ -> should.fail()
  }

  mark_database_test_executed(marker)
}

// -- Increment 11: uncertain admission commits -------------------------------
//
// Full contract: `docs/UNIQUENESS-CONTRACT.md`, "Admission transaction" (the
// `pog.TransactionQueryError` classification and `CommitUnknown`/
// `reconcile_unique`). `install_syncrep_reply_trigger` above is generalized
// (table + predicate) so the same mechanism Increment 2 proved for the
// acknowledgement path proves the same claims here, scoped by
// `submission_id` on `grind_unique_submissions` rather than a
// server-generated `job_id` — the submission id is chosen by the caller and
// known before the admission transaction that would create a job id even
// starts, which the acknowledgement path's job-id scoping could not offer.

/// (a) A pool closed *before* `submit_unique` ever sends anything: `run`
/// (`src/grind/internal/unique_admission.gleam`) calls
/// `transaction_or_checkout_failure`, whose checkout-failure branch (the
/// pool could not hand out a connection at all, so `BEGIN` never ran) is
/// reported directly as `NotCommitted(ConnectionUnavailable)`, with no
/// `PendingSubmission` constructed and no receipt lookup attempted — this is
/// knowably not-committed, not merely uncertain. See
/// `docs/RECOVERY-EVIDENCE.md`, Increment 11, for why this needed its own
/// FFI wrapper distinguishing a checkout failure from `run`'s other,
/// genuinely uncertain `pog.TransactionQueryError` case (case (d) below).
/// Reopening the same pool name and retrying the identical `SubmissionId` —
/// a plain `submit_unique`, not `reconcile_unique` (there is no
/// `PendingSubmission` to reconcile from) — then succeeds normally:
/// `Inserted`, exactly one row.
pub fn postgres_submit_unique_closed_pool_before_send_is_admission_failed_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_closed_before_send_test(database_url)
  }
}

fn run_unique_closed_before_send_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)
  let worker_def = unique_test_worker("unique.closed-before-send-" <> suffix)
  let test_queue = "unique-closed-before-send-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "unique-closed-before-send-" <> suffix

  let _ = postgres.close(database)

  submit_keep_existing(
    database,
    test_queue,
    submission_text,
    worker_def,
    1,
    policy,
  )
  |> should.equal(Error(submission.NotCommitted(pog.ConnectionUnavailable)))

  let assert Ok(reopened_validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(reopened) = postgres.start(reopened_validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      reopened,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  count_jobs_in_queue(postgres.connection(reopened), test_queue)
  |> should.equal(1)
  postgres.arguments(reopened, handle) |> should.equal(Ok(1))

  mark_database_test_executed("unique-closed-before-send-recovers-passed")
}

/// (b) An aborted commit: a deferred constraint trigger's `pg_sleep(30)`
/// fires during the admission transaction's own COMMIT (the same mechanism
/// the pre-existing `ack-commit-connection-loss-unknown` test uses for the
/// acknowledgement path), scoped by `submission_id` on
/// `grind_unique_submissions`. Terminating the backend while it sleeps
/// aborts the whole transaction before it is ever marked committed — unlike
/// (c)/(d) below, nothing is visible to any other connection, not even
/// briefly. `submit_unique`'s own reply is `CommitUnknown(pending)`, exactly
/// as (c)/(d) also report, because the follow-up receipt lookup — run on a
/// fresh connection after the connection loss — genuinely cannot tell an
/// aborted commit from a lost reply after a real one; that ambiguity is
/// exactly what `CommitUnknown` documents. An independent read confirms zero
/// jobs and zero receipts for this key. Because nothing was ever durably
/// recorded, `reconcile_unique` alone can never resolve this (it would find
/// nothing again, forever) — the only correct recovery is a plain retry of
/// the same `SubmissionId`, which this test proves converges to `Inserted`,
/// exactly one row.
pub fn postgres_submit_unique_aborted_commit_is_commit_unknown_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_aborted_commit_test(database_url)
  }
}

fn run_unique_aborted_commit_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_unique_aborted_commit_" <> suffix,
  )
  let worker_def = unique_test_worker("unique.aborted-commit-" <> suffix)
  let test_queue = "unique-aborted-commit-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "unique-aborted-commit-" <> suffix

  let trigger_name = "grind_test_unique_aborted_commit_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT (NEW.submission_id = '"
      <> submission_text
      <> "') THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> trigger_name
      <> " AFTER INSERT ON grind_unique_submissions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection)
  let drop_trigger = fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS "
        <> trigger_name
        <> " ON grind_unique_submissions",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
  use <- exception.defer(drop_trigger)

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  })

  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Error(submission.CommitUnknown(pending))) =
    process.receive(reply, within: 10_000)

  count_jobs_in_queue(connection, test_queue) |> should.equal(0)
  unique_receipt_exists(
    connection,
    postgres.storage_owner(database),
    submission_text,
  )
  |> should.equal(False)

  // `reconcile_unique` alone can never recover this: nothing was ever
  // committed, so the receipt lookup finds nothing, forever.
  let assert Error(submission.CommitUnknown(_)) =
    postgres.reconcile_unique(database, pending)

  // Drop the trigger *before* retrying: it is still scoped by this exact
  // `submission_id`, so a retry reusing the same `SubmissionId` (the whole
  // point of this claim) would otherwise fire it again and hang the retry's
  // own commit in another 30-second `pg_sleep`, with nobody left to
  // terminate that backend — exactly the trap this early cleanup avoids.
  drop_trigger()

  // The pool just had a connection deliberately terminated
  // (`terminate_backend`, above): `pgo_connection`'s own supervised restart
  // of that connection is a real, transient recovery window (not a
  // steady-state failure), matching every other terminate-then-retry test
  // in this file — `run_ack_commit_connection_loss_test`'s own
  // `retry_transient_query` (documented `docs/RECOVERY-EVIDENCE.md`,
  // "Acknowledgement deadline") is the pattern this test was previously
  // missing, which is exactly why it flaked under load: a plain,
  // unretried call here could observe `QueryTimeout`/`ConnectionUnavailable`
  // during that same window instead of the deterministic recovered state.
  let assert Ok(submission.Inserted(handle)) =
    retry_transient_query(
      fn() {
        submit_keep_existing(
          database,
          test_queue,
          submission_text,
          worker_def,
          1,
          policy,
        )
      },
      20,
    )
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  retry_transient_query(fn() { postgres.arguments(database, handle) }, 20)
  |> should.equal(Ok(1))

  mark_database_test_executed("unique-aborted-commit-is-commit-unknown-passed")
}

/// (c) A genuinely committed admission whose reply is lost after PostgreSQL
/// has already committed locally (the same SyncRep-park-then-terminate
/// mechanism Increment 2 uses for the acknowledgement path).
/// `submit_unique` itself still returns `Ok(Inserted(handle))` — resolved by
/// the follow-up receipt lookup `run` performs on a fresh connection after
/// the connection loss, not a `CommitUnknown` the caller must separately
/// reconcile. `bind_handle` agrees on the same job id and input.
pub fn postgres_submit_unique_committed_reply_lost_returns_inserted_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_committed_reply_lost_test(database_url)
  }
}

fn run_unique_committed_reply_lost_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)

  let worker_def = unique_test_worker("unique.reply-lost-" <> suffix)
  let test_queue = "unique-reply-lost-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "unique-reply-lost-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_unique_reply_lost_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> submission_text <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      5,
      policy,
    )
  })

  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Ok(submission.Inserted(handle))) =
    process.receive(reply, within: 10_000)
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)

  postgres.arguments(database, handle) |> should.equal(Ok(5))
  let assert Ok(rebound) =
    postgres.bind_handle(database, worker_def, job.id_value(handle))
  postgres.arguments(database, rebound) |> should.equal(Ok(5))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  unique_receipt_exists(
    connection,
    postgres.storage_owner(database),
    submission_text,
  )
  |> should.equal(True)

  mark_database_test_executed("unique-committed-reply-lost-inserted-passed")
}

/// (d) Committed, reply lost, *and* Grind's own pool closed while the commit
/// is still parked in `SyncRep`.
///
/// **This was found to be a correctness bug in `run`
/// (`src/grind/internal/unique_admission.gleam`), not a classification
/// difference to document and move on from.** The admission transaction's
/// own "commit" call correctly unblocks with `pog.TransactionQueryError`
/// (checked out fine, then lost the connection — genuinely uncertain, might
/// have committed), and `run` correctly routes it to
/// `reconcile_from_receipt` to check. But that follow-up `find_receipt`
/// query then *also* fails — the pool is now fully closed — and the
/// unfixed code's `Error(error) -> Error(error)` branch returned that
/// *lookup's own* connectivity failure, `NotCommitted(ConnectionUnavailable)`,
/// as if it were the *admission's* outcome, silently discarding the
/// `pending: PendingSubmission` that was already in hand. A caller told
/// `NotCommitted` reasonably treats that as "did not happen, safe to
/// retry independently" — but the zombie transaction can still commit
/// later. **Fixed** by two changes: (R1) `reconcile_from_receipt` now maps
/// a failed lookup to `Error(submission.CommitUnknown(pending))`, the same as
/// finding no receipt yet — mirroring `reconcile_unknown_ack`'s `Ok(None) |
/// Error(_) -> QueueAckUnknown` in `grind/postgres`, so a transient failure
/// while *checking* is never confused with a definite answer; (R2) `run`
/// now calls a new FFI wrapper, `transaction_or_checkout_failure`
/// (`grind_postgres_ffi.erl`), that distinguishes a checkout failure
/// (nothing was ever attempted — genuinely `NotCommitted`, no
/// `PendingSubmission`, no receipt lookup even tried) from pog's own
/// transaction outcome, so a checkout failure is no longer disguised as the
/// same `TransactionQueryError` shape a genuinely uncertain mid-transaction
/// loss produces — R1 alone would have made every checkout failure
/// (including (a) above) report `CommitUnknown` too, imprecisely; R2
/// restores (a)'s precise `NotCommitted`. See
/// `docs/RECOVERY-EVIDENCE.md` for the red-before-fix output and the R1
/// mutation that reverts to the bug.
///
/// With the fix: `submit_unique` reports `CommitUnknown(pending)`.
/// `reconcile_unique(reopened, pending)` while the zombie is still parked is
/// a pure receipt lookup with no lock of its own — the zombie's receipt
/// insert is not yet visible to any other session, so it still reports
/// `CommitUnknown` (not a persisted-conflict inference). A second,
/// independent recovery path — a *plain* `submit_unique` retry of the same
/// `SubmissionId`, attempted while the zombie is still parked — genuinely
/// needs the domain lock the zombie's still-open transaction holds, and
/// reports `AdmissionContended`; no second row either way. Only after the
/// zombie backend is terminated and confirmed gone (`wait_for_backend_gone`
/// — an independent observer connection is what later reads visibility
/// here, not Grind's own closed-then-reopened socket) does
/// `reconcile_unique(reopened, pending)` resolve from the now-visible
/// receipt: `Inserted`, with the original job id, exactly one row — and the
/// plain-retry path, tried again, converges on that same job id.
pub fn postgres_submit_unique_committed_reply_lost_store_unavailable_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_unique_committed_reply_lost_store_unavailable_test(database_url)
  }
}

fn run_unique_committed_reply_lost_store_unavailable_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_settings =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = postgres.connection(observer)
  require_syncrep_cluster_configured(observer_connection)

  let worker_def = unique_test_worker("unique.reply-lost-unavail-" <> suffix)
  let test_queue = "unique-reply-lost-unavail-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "unique-reply-lost-unavail-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    observer_connection,
    "grind_test_unique_reply_lost_unavail_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> submission_text <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      13,
      policy,
    )
  })

  let assert Ok(backend_pid) =
    wait_for_syncrep_trigger_backend(observer_connection, 300)

  let _ = postgres.close(database)

  // The admission transaction reached the database (its own "commit" call
  // was genuinely mid-flight when the pool closed) — genuinely uncertain,
  // not knowably absent: `CommitUnknown`, carrying a `PendingSubmission` to
  // reconcile from.
  let assert Ok(Error(submission.CommitUnknown(pending))) =
    process.receive(reply, within: 10_000)

  let assert Ok(reopened_validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(reopened) = postgres.start(reopened_validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  // While the zombie is still parked, its receipt insert is not yet visible
  // to any other session — a pure receipt lookup still finds nothing and
  // reports `CommitUnknown` again (not a persisted-conflict inference).
  let assert Error(submission.CommitUnknown(_)) =
    postgres.reconcile_unique(reopened, pending)

  // A fresh admission retry, unlike `reconcile_unique`, genuinely needs the
  // domain lock the zombie's still-open transaction holds — the same
  // safety `reconcile_unique` alone already provided above, confirmed here
  // as a second, independent recovery path.
  let assert Ok(contended_settings) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(200)
    |> postgres.validate
  let assert Ok(contended) = postgres.start(contended_settings)
  use <- exception.defer(fn() { postgres.close(contended) })
  submit_keep_existing(
    contended,
    test_queue,
    submission_text,
    worker_def,
    13,
    policy,
  )
  |> should.equal(Error(submission.AdmissionContended))
  count_jobs_in_queue(observer_connection, test_queue) |> should.equal(0)

  terminate_backend(observer_connection, backend_pid) |> should.equal(True)
  let assert Ok(Nil) =
    wait_for_backend_gone(observer_connection, backend_pid, 300)

  // `reconcile_unique`, once the zombie is gone, resolves from the now-
  // visible receipt — not by candidate selection reinterpreting the row as
  // a fresh conflict.
  let assert Ok(submission.Inserted(handle)) =
    postgres.reconcile_unique(reopened, pending)
  let original_job_id = job.id_value(handle)
  count_jobs_in_queue(observer_connection, test_queue) |> should.equal(1)
  postgres.arguments(reopened, handle) |> should.equal(Ok(13))

  // Second recovery path: a plain retry of the same `SubmissionId` (no
  // retained `PendingSubmission` needed) converges on the identical job id
  // through `admission_transaction`'s own receipt lookup — still one row.
  let assert Ok(submission.Inserted(retried_handle)) =
    submit_keep_existing(
      reopened,
      test_queue,
      submission_text,
      worker_def,
      13,
      policy,
    )
  job.id_value(retried_handle) |> should.equal(original_job_id)
  count_jobs_in_queue(observer_connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "unique-committed-reply-lost-store-unavailable-passed",
  )
}

/// (e) A reschedule whose commit reply is lost: replay via the same internal
/// receipt lookup returns `Rescheduled`, not `Existing`, even though the
/// row's current state (`scheduled`, at its new `available_at`) looks
/// exactly like an ordinary scheduled conflict either way — the receipt's
/// own recorded *decision* column, not the row's current state, is what
/// `find_receipt`/`outcome_of_receipt` decodes.
pub fn postgres_submit_unique_reschedule_reply_lost_returns_rescheduled_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_unique_reschedule_reply_lost_test(database_url)
  }
}

fn run_unique_reschedule_reply_lost_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)

  let worker_def = unique_test_worker("unique.reschedule-reply-lost-" <> suffix)
  let test_queue = "unique-reschedule-reply-lost-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.ScheduledOnly,
    )

  let original_target = future_available_at(connection, 3_600_000)
  let assert Ok(seed_submission) =
    submission.submission_id("unique-reschedule-reply-lost-seed-" <> suffix)
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database,
      test_queue,
      seed_submission,
      worker_def,
      21,
      submission.At(original_target),
      policy,
      unique.KeepExisting,
    )
  let job_id = job.id_value(handle)

  let new_target = future_available_at(connection, 7_200_000)
  let reschedule_submission = "unique-reschedule-reply-lost-retry-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_unique_reschedule_reply_lost_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> reschedule_submission <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_reschedule(
      database,
      test_queue,
      reschedule_submission,
      worker_def,
      21,
      policy,
      new_target,
    )
  })

  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Ok(submission.Rescheduled(conflict))) =
    process.receive(reply, within: 10_000)
  submission.conflict_job_id(conflict) |> should.equal(job_id)
  job_available_at_ms(connection, job_id)
  |> should.equal(job.available_at_unix_milliseconds(new_target))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  mark_database_test_executed("unique-reschedule-reply-lost-rescheduled-passed")
}

// -- Uniqueness increment 12 (selected keys) ---------------------------------
//
// `unique.selected` projects part of an admitted input into its own key,
// encoded with its own codec, independently of the rest of the input. These
// tests prove: a non-selected field never enters the key (only the projected
// value matters); the key contract string is `"selected:" <> name <> ":" <>
// codec_version`, so a different name or a different codec version alone
// isolates two selected keys that project the identical value; a full-input
// key and a selected key never collide, even over the exact same input,
// because their contract prefixes differ; and a selected key's equality is
// exact, not containment, the same departure from Oban's own selected-field
// semantics already proven for full-input keys
// (`postgres_submit_unique_json_equality_matches_postgres_jsonb_test`) --
// inspired by `oracle/deps/oban/test/oban/engine_test.exs`, "scoping
// uniqueness to specific argument keys".

/// An input with one field the key projects (`account`, itself a raw JSON
/// value via `RawInput`, reused from the JSON-equality tests above) and one
/// field the key never sees (`other`).
type SelectedInput {
  SelectedInput(account: RawInput, other: Int)
}

fn encode_selected_input(input: SelectedInput) -> json.Json {
  let SelectedInput(account:, other:) = input
  json.object([
    #("account", encode_raw_input(account)),
    #("other", json.int(other)),
  ])
}

fn selected_input_decoder() -> decode.Decoder(SelectedInput) {
  decode.success(SelectedInput(RawInput(json.null()), 0))
}

fn account_projection(input: SelectedInput) -> RawInput {
  let SelectedInput(account:, ..) = input
  account
}

fn selected_input_worker(
  id: String,
) -> worker.Worker(SelectedInput, String, e) {
  let assert Ok(input_codec) =
    worker.codec(
      id <> "-input-v1",
      encode_selected_input,
      selected_input_decoder(),
    )
  let assert Ok(output_codec) =
    worker.codec(id <> "-output-v1", json.string, decode.string)
  let assert Ok(worker_def) =
    worker.define(id, "v1", input_codec, output_codec, fn(input) {
      let SelectedInput(other:, ..) = input
      Ok(int.to_string(other))
    })
  worker_def
}

pub fn postgres_submit_unique_selected_key_scoping_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_unique_selected_key_scoping_test(database_url)
  }
}

fn run_submit_unique_selected_key_scoping_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_selected_pool",
  )
  let worker_def = selected_input_worker("unique.selected-" <> suffix)
  let assert Ok(account_codec) =
    worker.codec(
      "unique-selected-account-" <> suffix <> "-v1",
      encode_raw_input,
      raw_input_decoder(),
    )
  let assert Ok(other_version_codec) =
    worker.codec(
      "unique-selected-account-" <> suffix <> "-v2",
      encode_raw_input,
      raw_input_decoder(),
    )
  let assert Ok(account_key) =
    unique.selected("account", account_projection, account_codec)
  let assert Ok(account_key_other_name) =
    unique.selected("account-alt", account_projection, account_codec)
  let assert Ok(account_key_other_codec_version) =
    unique.selected("account", account_projection, other_version_codec)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let account_policy =
    unique.policy(account_key, unique.WithinQueue, period, unique.Incomplete)
  let account_policy_other_name =
    unique.policy(
      account_key_other_name,
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let account_policy_other_codec_version =
    unique.policy(
      account_key_other_codec_version,
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let full_input_policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "selected-" <> suffix
  let shared_account = SelectedInput(RawInput(json.int(1)), 10)

  // Same projected key, a different non-selected field: still `Existing` --
  // `other` never enters the key.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-1-" <> suffix,
      worker_def,
      shared_account,
      account_policy,
    )
  let assert Ok(submission.Existing(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-2-" <> suffix,
      worker_def,
      SelectedInput(RawInput(json.int(1)), 20),
      account_policy,
    )

  // A different key name, same projection and codec, same projected value:
  // `Inserted` -- the key contract string ("selected:" <> name <> ":" <>
  // codec_version) differs by name alone.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-3-" <> suffix,
      worker_def,
      shared_account,
      account_policy_other_name,
    )

  // A different key codec version, same name and projection, same projected
  // value: `Inserted` -- the contract string differs by codec version alone.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-4-" <> suffix,
      worker_def,
      shared_account,
      account_policy_other_codec_version,
    )

  // A full-input key and a selected key never collide, even over the exact
  // same input: their contract prefixes ("full-input:" vs "selected:")
  // differ unconditionally.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-5-" <> suffix,
      worker_def,
      shared_account,
      full_input_policy,
    )

  mark_database_test_executed("unique-selected-key-scoping-passed")
}

pub fn postgres_submit_unique_selected_key_equality_not_containment_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_unique_selected_key_containment_test(database_url)
  }
}

fn run_submit_unique_selected_key_containment_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_selected_containment_pool",
  )
  let worker_def =
    selected_input_worker("unique.selected-containment-" <> suffix)
  let assert Ok(account_codec) =
    worker.codec(
      "unique-selected-containment-account-" <> suffix <> "-v1",
      encode_raw_input,
      raw_input_decoder(),
    )
  let assert Ok(account_key) =
    unique.selected("account", account_projection, account_codec)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(account_key, unique.WithinQueue, period, unique.Incomplete)
  let test_queue = "selected-containment-" <> suffix

  // A subset projected value never conflicts with a stored superset: a
  // selected key compares by exact equality, the same as a full-input key
  // (docs/UNIQUENESS-CONTRACT.md, Decision 2) -- a deliberate departure from
  // Oban's own containment semantics for a selected-field comparison.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-subset-" <> suffix,
      worker_def,
      SelectedInput(RawInput(json.object([#("id", json.int(1))])), 0),
      policy,
    )
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-superset-" <> suffix,
      worker_def,
      SelectedInput(
        RawInput(json.object([#("id", json.int(1)), #("extra", json.int(2))])),
        0,
      ),
      policy,
    )

  mark_database_test_executed(
    "unique-selected-key-equality-not-containment-passed",
  )
}

// -- `[grind, job, acknowledged]` observation (grind/observation) ----------
//
// `grind/postgres` emits this event through its own `sinal/forwarder`, never
// through a plain `sinal.emit` — see `grind/observation`'s module
// documentation. These tests attach with plain `sinal.observe`, exactly as
// an application would; the isolation test below is what actually proves
// dispatch happens in the forwarder process rather than the coordinator.

type AcknowledgedSignal {
  AcknowledgedSignal(
    measurements: observation.AcknowledgedMeasurements,
    metadata: observation.AcknowledgedMetadata,
  )
}

type DroppedSignal {
  DroppedSignal(
    measurements: forwarder.Dropped,
    metadata: forwarder.DroppedMetadata,
  )
}

/// Carries a release gate created *inside* a blocked handler (so it is owned
/// by the forwarder process, which is the one that will `process.receive`
/// it) back out to the test process, which only ever `process.send`s to it.
type IsolationGateEntered {
  IsolationGateEntered(process.Subject(Nil))
}

type OverflowGateEntered {
  OverflowGateEntered(process.Subject(Nil))
}

fn attach_acknowledged_observer(
  id_suffix: String,
  run: fn(
    observation.AcknowledgedMeasurements,
    observation.AcknowledgedMetadata,
  ) -> Nil,
) -> sinal.Attachment {
  let assert Ok(id) =
    sinal.handler_id("grind-test-observation-acknowledged-" <> id_suffix)
  let assert Ok(attachment) = sinal.observe(id, observation.acknowledged(), run)
  attachment
}

fn attach_dropped_observer(
  id_suffix: String,
  run: fn(forwarder.Dropped, forwarder.DroppedMetadata) -> Nil,
) -> sinal.Attachment {
  let assert Ok(id) =
    sinal.handler_id("grind-test-observation-dropped-" <> id_suffix)
  let assert Ok(attachment) = sinal.observe(id, forwarder.dropped_event(), run)
  attachment
}

/// Detach is best-effort cleanup, not part of what a test proves: native
/// `:telemetry` can already have auto-detached a handler on its own (a
/// raising handler is auto-detached after it raises — see the raising-handler
/// test below), so a `NotAttached` result here is not a test failure.
fn detach(attachment: sinal.Attachment) -> Nil {
  let _ = sinal.detach(attachment)
  Nil
}

/// Counts how many pending messages are already waiting on `subject`,
/// draining them. A short per-check timeout (rather than `within: 0`) tolerates
/// a message still in flight from a handler that just ran, without turning
/// this into a fixed wall-clock wait for a specific count.
fn drain_subject_count(subject: process.Subject(Nil), count: Int) -> Int {
  case process.receive(subject, within: 50) {
    Ok(Nil) -> drain_subject_count(subject, count + 1)
    Error(Nil) -> count
  }
}

/// Deterministic negative/"exactly N" check for a sentinel-observed
/// `signal`: rather than a fixed wall-clock wait for "nothing more arrives"
/// (fragile — either too short under load, or slow), this asserts the very
/// *next* event received is a distinct, known-good sentinel acknowledgement
/// run through the same consumer/producer afterward. `sinal/forwarder`
/// guarantees per-producer FIFO delivery, so if the code under test had
/// wrongly emitted an extra event for the original job, it would have been
/// enqueued ahead of the sentinel's and would arrive here instead —
/// deterministically, not racily.
fn assert_next_observation_is_sentinel(
  signal: process.Subject(AcknowledgedSignal),
  sentinel_job_id: Int,
) -> Nil {
  let assert Ok(AcknowledgedSignal(_, metadata)) =
    process.receive(signal, within: 5000)
  metadata.ref.job_id |> should.equal(sentinel_job_id)
}

/// Registers a trivial, instantly-completing worker onto an existing
/// registry (same queue), for the "run a sentinel job through the same
/// consumer/producer afterward" pattern `assert_next_observation_is_sentinel`
/// needs. Kept separate from whatever worker the test under negative-path
/// scrutiny uses (which may block waiting on a release gate), so driving the
/// sentinel to completion can never itself hang.
fn register_sentinel_worker(
  workers: registry.Registry,
  id_suffix: String,
) -> #(registry.Registry, worker.Worker(Int, String, LookupFailure)) {
  let assert Ok(input_codec) =
    worker.codec(
      "observation-sentinel-" <> id_suffix <> "-input-v1",
      json.int,
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-sentinel-" <> id_suffix <> "-output-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "observation.sentinel." <> id_suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("sentinel-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.register(workers, definition)
  #(workers, definition)
}

/// Pure: `InvalidObservationCapacity` is rejected before any process starts.
pub fn postgres_settings_reject_non_positive_observation_capacity_test() {
  postgres.settings("postgres://ignored/ignored")
  |> postgres.with_observation_capacity(0)
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidObservationCapacity))
  postgres.settings("postgres://ignored/ignored")
  |> postgres.with_observation_capacity(-1)
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidObservationCapacity))
}

pub fn postgres_acknowledged_observation_commit_ordering_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_acknowledged_commit_ordering_test(database_url)
  }
}

/// Commit ordering: reading `postgres.state` *from inside* the attached
/// handler (which runs in the forwarder process) already observes the
/// committed state — proof the observation is emitted strictly after the
/// commit, not before it. A handler that read `Executing` here would mean
/// the emit ran ahead of (or racing) the transaction, not after it.
fn run_acknowledged_commit_ordering_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-ordering-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("observation-ordering-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "observation.ordering",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("ordering-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("observation-ordering")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "observation-ordering", definition, 5)
  let job_id = job.id_value(handle)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer("commit-ordering", fn(measurements, metadata) {
      let observed_state = postgres.state(database, handle)
      process.send(signal, #(
        observed_state,
        AcknowledgedSignal(measurements, metadata),
      ))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  let connection = postgres.connection(database)
  let assert Ok(#(attempt_id, epoch)) =
    stored_attempt_identity(connection, job_id)
  let expected_command_id =
    attempt.acknowledgement_command_id(job_id, attempt_id, epoch)

  let assert Ok(#(observed_state, AcknowledgedSignal(measurements, metadata))) =
    process.receive(signal, within: 5000)
  observed_state |> should.equal(Ok(job.Succeeded))
  measurements |> should.equal(observation.AcknowledgedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job_id)
  metadata.ref.queue |> should.equal("observation-ordering")
  metadata.ref.worker_id |> should.equal("observation.ordering")
  metadata.ref.worker_version |> should.equal("v1")
  metadata.attempt.attempt_id |> should.equal(attempt_id)
  metadata.attempt.epoch |> should.equal(epoch)
  metadata.attempt.attempt |> should.equal(1)
  metadata.proposed |> should.equal(observation.ProposedSuccess)
  metadata.committed_state |> should.equal(job.Succeeded)
  metadata.failure_cause |> should.equal(None)
  metadata.available_at_unix_ms |> should.equal(None)
  metadata.confirmation |> should.equal(observation.Replied)
  metadata.command_id |> should.equal(expected_command_id)
  process.receive(signal, within: 0) |> should.equal(Error(Nil))
  mark_database_test_executed("acknowledged-observation-commit-ordering-passed")
}

pub fn postgres_acknowledged_observation_isolation_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_isolation_test(database_url)
  }
}

/// Isolation: job A's acknowledgement observation is gate-blocked in the
/// forwarder process while job B is still executing under the same
/// coordinator. B's lease keeps renewing and B completes normally while A's
/// gate stays shut — proof the coordinator was never blocked by A's
/// observation. Named mutation: replacing `grind/postgres`'s
/// `forwarder.emit` call with a direct `sinal.emit` call makes this test
/// hang (the coordinator itself would run the blocked handler, starving B's
/// renewal), which is exactly the regression this test exists to catch.
fn run_acknowledged_observation_isolation_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-isolation-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("observation-isolation-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "observation.isolation",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, CapacityWorkerStarted(value, release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("isolated-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-isolation")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle_a) =
    postgres.submit(database, "observation-isolation", definition, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "observation-isolation", definition, 2)
  let job_a_id = job.id_value(handle_a)

  // The release gate must be created *inside* the handler (owned by the
  // forwarder process that will `process.receive` it) and handed back to
  // the test over `gate_entered`; a `process.Subject` created in the test
  // process cannot be received on from a different process.
  let gate_entered = process.new_subject()
  let attachment =
    attach_acknowledged_observer("isolation", fn(_measurements, metadata) {
      case metadata.ref.job_id == job_a_id {
        True -> {
          let gate = process.new_subject()
          process.send(gate_entered, IsolationGateEntered(gate))
          let assert Ok(Nil) = process.receive(gate, within: 10_000)
          Nil
        }
        False -> Nil
      }
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(2)
    |> queue.with_lease_duration(6100)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let reply_a = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply_a, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, release_a)) =
    process.receive(started, within: 5000)

  let reply_b = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply_b, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, release_b)) =
    process.receive(started, within: 5000)

  process.send(release_a, ReleaseAttempt)
  process.receive(reply_a, within: 5000) |> should.equal(Ok(Ok(True)))
  // A's own acknowledged observation is now gate-blocked in the forwarder.
  let assert Ok(IsolationGateEntered(gate)) =
    process.receive(gate_entered, within: 5000)

  // While it stays blocked, B's lease keeps renewing and B completes.
  await_renewal_status(consumer, queue.LeaseRenewalConfirmed, 200)
  |> should.equal(True)
  process.send(release_b, ReleaseAttempt)
  process.receive(reply_b, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle_b) |> should.equal(Ok(job.Succeeded))

  process.send(gate, Nil)
  mark_database_test_executed("acknowledged-observation-isolation-passed")
}

pub fn postgres_acknowledged_observation_absent_on_commit_unknown_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_absent_on_commit_unknown_test(database_url)
  }
}

/// Negative (abort/unknown, sentinel pattern): the ack's COMMIT is aborted by
/// killing its backend mid-trigger (the same technique as
/// `postgres_ack_commit_connection_loss_is_unknown_test`), so nothing is
/// durably committed and `acknowledge_claim` reports `QueueAckUnknown`. No
/// `[grind, job, acknowledged]` observation is emitted for either "abort" or
/// "unknown" here, because in this codebase they are the exact same code
/// path: a connection lost during `COMMIT` is unconditionally reported
/// `QueueAckUnknown`, whether or not the transaction actually reached
/// commit. Named mutation: emitting on this `Error(QueueAckUnknown(..))`
/// result (instead of only ever emitting from `resolve_ack_transaction_result`'s
/// proven-commit branches) makes this test fail.
fn run_acknowledged_observation_absent_on_commit_unknown_test(
  database_url: String,
) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-commit-unknown-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "observation-commit-unknown-output-v1",
      json.string,
      decode.string,
    )
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "observation.commit.unknown",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("commit-unknown-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-commit-unknown")
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "commit-unknown")
  let assert Ok(handle) =
    postgres.submit(database, "observation-commit-unknown", definition, 21)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer("commit-unknown", fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_kill_observation_ack_backend() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id <> "
      <> int.to_string(job_id)
      <> " THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER grind_test_kill_observation_ack_backend AFTER INSERT ON grind_job_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION grind_test_kill_observation_ack_backend()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS grind_test_kill_observation_ack_backend ON grind_job_acknowledgements",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query(
        "DROP FUNCTION IF EXISTS grind_test_kill_observation_ack_backend()",
      )
      |> pog.execute(on: connection)
    Nil
  })

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 100)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckUnknown(_, _)))) =
    process.receive(reply, within: 10_000)
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  // Deterministic absence check: a sentinel job's own commit/observation
  // through the same consumer must be the very next event on `signal`.
  let assert Ok(sentinel) =
    postgres.submit(database, "observation-commit-unknown", sentinel_worker, 22)
  queue.process_one(consumer) |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, job.id_value(sentinel))

  mark_database_test_executed(
    "acknowledged-observation-absent-on-commit-unknown-passed",
  )
}

pub fn postgres_acknowledged_observation_absent_on_stale_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_absent_on_stale_ack_test(database_url)
  }
}

/// Negative (rollback/stale, sentinel pattern): a forced lease expiry makes
/// the fenced `UPDATE` inside `acknowledge_transaction` affect zero rows,
/// which (finding no matching receipt either) is reported as
/// `QueueAckStale`. Any `Error(..)` returned from inside that transaction
/// callback rolls the whole ack transaction back — "stale" and "rollback"
/// are the same mechanism here, not two independent ones. No observation is
/// emitted. Named mutation: moving the emit call to run unconditionally
/// after `acknowledge_transaction` returns (instead of only after
/// `resolve_ack_transaction_result` reports a proven commit) makes this
/// test fail.
fn run_acknowledged_observation_absent_on_stale_ack_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-stale-ack-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("observation-stale-ack-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(slow_worker) =
    worker.define(
      "observation.stale.ack",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("stale-ack-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-stale-ack")
  let assert Ok(workers) = registry.register(workers, slow_worker)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "stale-ack")
  let assert Ok(handle) =
    postgres.submit(database, "observation-stale-ack", slow_worker, 9)
  let job_id = job.id_value(handle)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer("stale-ack", fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(30_000)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(reply, queue.process_one(consumer)) })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let connection = postgres.connection(database)
  let assert Ok(forced_expiry) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection)
  forced_expiry.count |> should.equal(1)

  process.send(release, ReleaseAttempt)
  let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(_, _)))) =
    process.receive(reply, within: 5000)
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  let assert Ok(sentinel) =
    postgres.submit(database, "observation-stale-ack", sentinel_worker, 10)
  queue.process_one(consumer) |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, job.id_value(sentinel))

  mark_database_test_executed(
    "acknowledged-observation-absent-on-stale-ack-passed",
  )
}

pub fn postgres_acknowledged_observation_reconciled_after_lost_reply_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_reconciled_after_lost_reply_test(
        database_url,
      )
  }
}

/// Lost reply (SyncRep harness, same technique as
/// `postgres_ack_committed_reply_lost_reconciles_from_receipt_test`): the
/// ack genuinely commits, but this call's own connection is severed while
/// its `COMMIT` is parked in `SyncRep`, so `acknowledge` only learns the
/// outcome via `reconcile_unknown_ack` reading the receipt back. Exactly one
/// `[grind, job, acknowledged]` observation is emitted, and its
/// `confirmation` is `Reconciled`, never `Replied`. Named mutation:
/// hard-coding `Replied` for every `AckCommit` (ignoring
/// `via_receipt_match`) makes this test fail.
fn run_acknowledged_observation_reconciled_after_lost_reply_test(
  database_url: String,
) -> Nil {
  let settings = postgres.settings(database_url)
  let assert Ok(validated) = postgres.validate(settings)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)
  let assert Ok(input_codec) =
    worker.codec("observation-reply-lost-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("observation-reply-lost-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "observation.reply.lost",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, FirstAttemptStarted(release))
        process.send(invoked, WorkerInvoked)
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("reply-lost-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-reply-lost")
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "reply-lost")
  let assert Ok(handle) =
    postgres.submit(database, "observation-reply-lost", definition, 33)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer("reply-lost", fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(FirstAttemptStarted(release)) =
    process.receive(started, within: 5000)
  process.receive(invoked, within: 1000) |> should.equal(Ok(WorkerInvoked))

  let job_id = job.id_value(handle)
  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_syncrep_observation_reply_lost",
    "grind_job_acknowledgements",
    "NEW.job_id = " <> int.to_string(job_id),
  ))

  process.send(release, ReleaseAttempt)
  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  process.receive(reply, within: 10_000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  let assert Ok(#(attempt_id, epoch)) =
    stored_attempt_identity(connection, job_id)
  let expected_command_id =
    attempt.acknowledgement_command_id(job_id, attempt_id, epoch)

  let assert Ok(AcknowledgedSignal(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.AcknowledgedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job_id)
  metadata.committed_state |> should.equal(job.Succeeded)
  metadata.confirmation |> should.equal(observation.Reconciled)
  metadata.command_id |> should.equal(expected_command_id)

  // Exactly one observation for this command — no duplicate `Replied` also
  // arrived from the same lost-reply commit. Checked deterministically: a
  // sentinel job's own observation, run through the same consumer, must be
  // the very next event.
  let assert Ok(sentinel) =
    postgres.submit(database, "observation-reply-lost", sentinel_worker, 34)
  queue.process_one(consumer) |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, job.id_value(sentinel))

  mark_database_test_executed(
    "acknowledged-observation-reconciled-after-lost-reply-passed",
  )
}

pub fn postgres_acknowledged_observation_committed_state_overrides_proposal_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_committed_state_overrides_proposal_test(
        database_url,
      )
  }
}

/// Proposed vs. committed (same cancel-while-running technique as
/// `postgres_cancel_running_worker_overrides_proposal_on_ack_test`): the
/// worker proposes success, but a concurrent cancellation overrides it, and
/// the durable commit is `Cancelled`. The observation's `proposed` and
/// `committed_state` fields diverge accordingly and `committed_state` comes
/// from the commit, never from the proposal. Named mutation: building
/// `committed_state` from the worker's proposed disposition instead of
/// `AckCommit`'s own committed value makes this test fail.
fn run_acknowledged_observation_committed_state_overrides_proposal_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-cancel-running-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "observation-cancel-running-output-v1",
      json.string,
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "observation.cancel.running",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) ->
            Ok("completed-despite-cancel-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-cancel-running")
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "cancel-running")
  let assert Ok(handle) =
    postgres.submit(database, "observation-cancel-running", definition, 9)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer("cancel-running", fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))

  let assert Ok(AcknowledgedSignal(_measurements, metadata)) =
    process.receive(signal, within: 5000)
  metadata.proposed |> should.equal(observation.ProposedSuccess)
  metadata.committed_state |> should.equal(job.Cancelled)
  metadata.confirmation |> should.equal(observation.Replied)

  let assert Ok(sentinel) =
    postgres.submit(database, "observation-cancel-running", sentinel_worker, 10)
  queue.process_one(consumer) |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, job.id_value(sentinel))

  mark_database_test_executed(
    "acknowledged-observation-committed-state-overrides-proposal-passed",
  )
}

pub fn postgres_acknowledged_observation_overflow_reports_dropped_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_overflow_reports_dropped_test(database_url)
  }
}

/// Overflow: a `Database` started with `observation_capacity(1)` and a
/// gate-blocked acknowledged handler holds the forwarder's single in-flight
/// slot; job B's own `[grind, job, claimed]` and `[grind, job, acknowledged]`
/// observations are both forwarded while that slot is still held (one
/// `Forwarder` per `Database` carries every `[grind, job, *]` event, not one
/// per event kind), so both exceed capacity and are dropped — coalesced into
/// one `[sinal, forwarder, dropped]` report with `rejected: 2` — never
/// affecting either job's committed state. Job A's own `claimed` observation
/// is not among them: it is emitted and drained before A's `acknowledged`
/// handler ever blocks the forwarder.
fn run_acknowledged_observation_overflow_reports_dropped_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_observation_capacity(1)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-overflow-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("observation-overflow-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "observation.overflow",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("overflow-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("observation-overflow")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle_a) =
    postgres.submit(database, "observation-overflow", definition, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "observation-overflow", definition, 2)

  // See the isolation test above: the release gate is created *inside* the
  // handler so it is owned by the forwarder process that receives it.
  let gate_entered = process.new_subject()
  let acknowledged_attachment =
    attach_acknowledged_observer("overflow", fn(_measurements, _metadata) {
      let gate = process.new_subject()
      process.send(gate_entered, OverflowGateEntered(gate))
      let assert Ok(Nil) = process.receive(gate, within: 10_000)
      Nil
    })
  use <- exception.defer(fn() { detach(acknowledged_attachment) })
  let dropped_signal = process.new_subject()
  let dropped_attachment =
    attach_dropped_observer("overflow", fn(measurements, metadata) {
      process.send(dropped_signal, DroppedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(dropped_attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // Job A occupies the forwarder's only in-flight slot and blocks there.
  queue.process_one(consumer) |> should.equal(Ok(True))
  let assert Ok(OverflowGateEntered(gate)) =
    process.receive(gate_entered, within: 5000)

  // Job B's own acknowledgement still commits normally; only its forwarded
  // observation is dropped for exceeding capacity while A's slot is held.
  queue.process_one(consumer) |> should.equal(Ok(True))

  process.send(gate, Nil)
  let assert Ok(DroppedSignal(dropped_measurements, dropped_metadata)) =
    process.receive(dropped_signal, within: 10_000)
  dropped_measurements.rejected |> should.equal(2)
  dropped_metadata.forwarder |> should.not_equal("")

  postgres.state(database, handle_a) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, handle_b) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed(
    "acknowledged-observation-overflow-reports-dropped-passed",
  )
}

pub fn postgres_acknowledged_observation_raising_handler_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_raising_handler_test(database_url)
  }
}

/// A handler that raises never affects the job's own committed outcome:
/// native `:telemetry` isolates the raise (detaching the faulty handler),
/// and by the time any handler runs at all, `forwarder.emit`'s own hand-off
/// to the forwarder has already returned, decoupled from the coordinator.
fn run_acknowledged_observation_raising_handler_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-raising-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("observation-raising-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "observation.raising",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("raising-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("observation-raising")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "observation-raising", definition, 7)
  let attachment =
    attach_acknowledged_observer("raising", fn(_measurements, _metadata) {
      panic as "deliberately raising acknowledged observer"
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("raising-7")))
  mark_database_test_executed(
    "acknowledged-observation-raising-handler-outcome-unchanged-passed",
  )
}

pub fn postgres_forwarder_crash_loop_does_not_stop_the_pool_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_forwarder_crash_loop_test(database_url)
  }
}

/// Isolation hole (coordinator review, round 1 follow-up): a handler that
/// itself exits or is killed is not isolated by native `:telemetry` the way
/// a raise is (see `sinal/forwarder`'s own module documentation) and can
/// take the forwarder process down. Before the fix, the forwarder was a
/// permanent sibling of the PostgreSQL pool under one `OneForOne` supervisor
/// with the default restart intensity (2 restarts / 5 seconds); repeatedly
/// killing the forwarder exhausted that shared supervisor's own restart
/// budget, which then terminated *all* of its children, including the pool
/// — acks and submits after that point failed with the pool gone. The fix
/// (`grind/postgres.start`) nests the forwarder under its own supervisor,
/// added to the root as a `Temporary` child: a `Temporary` child's
/// termination is never restarted and never counts toward the parent
/// supervisor's own restart intensity, so the forwarder subtree exhausting
/// itself can never affect the pool. This test drives enough acknowledged
/// events, each killing whichever forwarder incarnation handles it, to
/// exceed the default restart intensity well within its period, then proves
/// the pool still serves a fresh submit and ack afterward.
fn run_forwarder_crash_loop_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("forwarder-crash-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("forwarder-crash-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("forwarder.crash", "v1", input_codec, output_codec, fn(value) {
      Ok("crash-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("forwarder-crash")
  let assert Ok(workers) = registry.register(workers, definition)

  let assert Ok(id) = sinal.handler_id("grind-test-forwarder-crash-loop")
  let assert Ok(attachment) =
    sinal.observe(id, observation.acknowledged(), fn(_measurements, _metadata) {
      process.kill(process.self())
    })
  use <- exception.defer(fn() { detach(attachment) })

  // A second, non-killing handler on the same descriptor: since both
  // handlers run in whichever forwarder incarnation is currently live,
  // this one is delivered exactly when the killing handler above is —
  // giving a direct runtime count of how many `acknowledged` events the
  // forwarder actually managed to deliver, instead of only inferring
  // "the restart budget must be exhausted by now" from elapsed sleep time.
  let observed = process.new_subject()
  let assert Ok(counter_id) =
    sinal.handler_id("grind-test-forwarder-crash-loop-counter")
  let assert Ok(counter_attachment) =
    sinal.observe(counter_id, observation.acknowledged(), fn(_, _) {
      process.send(observed, Nil)
    })
  use <- exception.defer(fn() { detach(counter_attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // Six acknowledged events, each killing the forwarder incarnation that
  // handles it, well exceeds the default restart intensity (2 restarts / 5s)
  // while staying comfortably inside its 5-second period. A short sleep
  // between each keeps each emit landing on a live incarnation rather than
  // racing a mid-flight restart.
  list.repeat(Nil, 6)
  |> list.each(fn(_) {
    let assert Ok(_) =
      postgres.submit(database, "forwarder-crash", definition, 1)
    queue.process_one(consumer) |> should.equal(Ok(True))
    process.sleep(80)
  })

  // Degraded state actually reached, not assumed: strictly fewer than six
  // of the loop's own acknowledgements were ever delivered to either
  // handler, proving the nested supervisor's restart budget was genuinely
  // exhausted partway through — every acknowledgement after that point got
  // `ForwarderUnavailable` and never reached `:telemetry` dispatch at all.
  let received_during_loop = drain_subject_count(observed, 0)
  { received_during_loop < 6 } |> should.equal(True)

  // The pool must still be alive and serving submit/ack after the
  // forwarder's own restart budget is long exhausted.
  let assert Ok(final_handle) =
    postgres.submit(database, "forwarder-crash", definition, 99)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, final_handle) |> should.equal(Ok(job.Succeeded))

  // And the degraded state is permanent, not transient: this job committed
  // and the pool is plainly healthy, yet its own acknowledgement produced
  // zero further deliveries — a `Temporary` child's exhausted subtree is
  // never restarted, so observations do not quietly come back on their own.
  drain_subject_count(observed, 0) |> should.equal(0)
  mark_database_test_executed("forwarder-crash-loop-pool-survives-passed")
}

pub fn postgres_acknowledged_observation_available_at_for_committed_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_available_at_retry_test(database_url)
  }
}

/// `available_at_unix_ms` for a genuinely committed `retryable` outcome:
/// `Some` and within the default backoff's bounds, read from the commit
/// (`RETURNING`), not re-derived from the proposal.
fn run_acknowledged_observation_available_at_retry_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-available-at-retry-input-v1",
      json.int,
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-available-at-retry-output-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "observation.available-at.retry",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Error(AccountMissing(value)) },
    )
  let assert Ok(workers) = registry.new("observation-available-at-retry")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "observation-available-at-retry", definition, 1)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(
      "available-at-retry",
      fn(measurements, metadata) {
        process.send(signal, AcknowledgedSignal(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let connection = postgres.connection(database)
  let before_ack_ms = database_time_milliseconds(connection)
  queue.process_one(consumer) |> should.equal(Ok(True))
  let after_ack_ms = database_time_milliseconds(connection)
  postgres.state(database, handle) |> should.equal(Ok(job.Retryable))

  let assert Ok(AcknowledgedSignal(_measurements, metadata)) =
    process.receive(signal, within: 5000)
  metadata.proposed |> should.equal(observation.ProposedRetryable)
  metadata.committed_state |> should.equal(job.Retryable)
  let assert Some(available_at_ms) = metadata.available_at_unix_ms
  should.be_true(available_at_ms >= before_ack_ms + 15_000)
  should.be_true(available_at_ms <= after_ack_ms + 15_000)
  mark_database_test_executed(
    "acknowledged-observation-available-at-committed-retry-passed",
  )
}

pub fn postgres_acknowledged_observation_available_at_for_committed_snooze_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_available_at_snooze_test(database_url)
  }
}

/// `available_at_unix_ms` for a genuinely committed snooze (`scheduled`)
/// outcome: `Some` and within the requested delay's bounds.
fn run_acknowledged_observation_available_at_snooze_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-available-at-snooze-input-v1",
      json.int,
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-available-at-snooze-output-v1",
      json.string,
      decode.string,
    )
  let assert Ok(delay) = worker.retry_delay(60_000)
  let assert Ok(ordinary) =
    worker.define(
      "observation.available-at.snooze",
      "v1",
      input_codec,
      output_codec,
      fn(_) { Error(AccountMissing(1)) },
    )
  let snoozing =
    worker.with_queue_handler(ordinary, fn(_) {
      worker.WorkerSnoozed(delay, "awaiting external account")
    })
  let assert Ok(workers) = registry.new("observation-available-at-snooze")
  let assert Ok(workers) = registry.register(workers, snoozing)
  let assert Ok(handle) =
    postgres.submit(database, "observation-available-at-snooze", snoozing, 1)
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(
      "available-at-snooze",
      fn(measurements, metadata) {
        process.send(signal, AcknowledgedSignal(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let connection = postgres.connection(database)
  let before_ack_ms = database_time_milliseconds(connection)
  queue.process_one(consumer) |> should.equal(Ok(True))
  let after_ack_ms = database_time_milliseconds(connection)
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))

  let assert Ok(AcknowledgedSignal(_measurements, metadata)) =
    process.receive(signal, within: 5000)
  metadata.proposed |> should.equal(observation.ProposedSnoozed)
  metadata.committed_state |> should.equal(job.Scheduled)
  let assert Some(available_at_ms) = metadata.available_at_unix_ms
  should.be_true(available_at_ms >= before_ack_ms + 60_000)
  should.be_true(available_at_ms <= after_ack_ms + 60_000)
  mark_database_test_executed(
    "acknowledged-observation-available-at-committed-snooze-passed",
  )
}

pub fn postgres_acknowledged_observation_available_at_none_when_cancel_overrides_retry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_available_at_cancel_overrides_retry_test(
        database_url,
      )
  }
}

/// `available_at_unix_ms` must come from the *committed* state, not the
/// proposed one: a proposed retry (`ProposedRetryable`) overridden by a
/// concurrent cancellation commits `cancelled`, whose row's `available_at`
/// is left at its unrelated pre-ack value — this must never be surfaced as
/// `Some`. Named mutation: gating on `proposed_state` instead of
/// `committed_state` (the exact bug this test was written to catch) makes
/// this test fail — see `docs/RECOVERY-EVIDENCE.md`, "Acknowledged
/// observation".
fn run_acknowledged_observation_available_at_cancel_overrides_retry_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "observation-available-at-cancel-retry-input-v1",
      json.int,
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "observation-available-at-cancel-retry-output-v1",
      json.string,
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "observation.available-at.cancel-retry",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Error(AccountMissing(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("observation-available-at-cancel-retry")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(
      database,
      "observation-available-at-cancel-retry",
      definition,
      9,
    )
  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(
      "available-at-cancel-retry",
      fn(measurements, metadata) {
        process.send(signal, AcknowledgedSignal(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Cancelled))

  let assert Ok(AcknowledgedSignal(_measurements, metadata)) =
    process.receive(signal, within: 5000)
  metadata.proposed |> should.equal(observation.ProposedRetryable)
  metadata.committed_state |> should.equal(job.Cancelled)
  metadata.available_at_unix_ms |> should.equal(None)
  mark_database_test_executed(
    "acknowledged-observation-available-at-none-cancel-overrides-retry-passed",
  )
}

pub fn postgres_acknowledged_observation_reconciled_on_sequential_duplicate_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_reconciled_sequential_duplicate_test(
        database_url,
      )
  }
}

/// Coordinator review, round 1 follow-up: proves the *early* receipt-match
/// site — the `matching_acknowledgement` check at the very top of
/// `acknowledge_transaction`, before any `UPDATE` is attempted — is also
/// `Reconciled`, not just `reconcile_unknown_ack`'s post-lost-reply site
/// already proven in round 1. Reached deterministically, with no forced
/// concurrency needed: a second, purely sequential call to
/// `acknowledge_claim` with the exact same `ClaimedJob`/`Execution` finds
/// the first call's own receipt already durably recorded. Named mutation:
/// hardcoding `via_receipt_match: False` at this site makes this test fail
/// (the second event would report `Replied`, not `Reconciled`).
fn run_acknowledged_observation_reconciled_sequential_duplicate_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-dup-sequential-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "observation-dup-sequential-output-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "observation.dup.sequential",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("observation-dup-sequential")
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "dup-sequential")
  let assert Ok(_handle) =
    postgres.submit(database, "observation-dup-sequential", definition, 6)
  let attempt_owner = "observation-dup-sequential-owner"
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "observation-dup-sequential",
      workers,
      attempt_owner,
      30_000,
    )
  let execution = attempt.execute_claim(claimed)

  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer("dup-sequential", fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  attempt.acknowledge(
    database,
    "observation-dup-sequential",
    attempt_owner,
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  let assert Ok(AcknowledgedSignal(_, first_metadata)) =
    process.receive(signal, within: 5000)
  first_metadata.confirmation |> should.equal(observation.Replied)
  first_metadata.committed_state |> should.equal(job.Succeeded)

  // The exact same claim/execution, acknowledged a second time: this is the
  // early receipt-match site, reached with no concurrency at all.
  attempt.acknowledge(
    database,
    "observation-dup-sequential",
    attempt_owner,
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  let assert Ok(AcknowledgedSignal(_, second_metadata)) =
    process.receive(signal, within: 5000)
  second_metadata.confirmation |> should.equal(observation.Reconciled)
  second_metadata.committed_state |> should.equal(job.Succeeded)
  second_metadata.command_id |> should.equal(first_metadata.command_id)

  // Exactly two events, deterministically: a sentinel claim/ack through the
  // same database's forwarder must be the very next event.
  let assert Ok(_sentinel_handle) =
    postgres.submit(database, "observation-dup-sequential", sentinel_worker, 7)
  let assert Ok(Some(sentinel_claimed)) =
    attempt.claim_one(
      database,
      "observation-dup-sequential",
      workers,
      attempt_owner,
      30_000,
    )
  let sentinel_execution = attempt.execute_claim(sentinel_claimed)
  let #(sentinel_job_id, _, _) = attempt.claim_identity(sentinel_claimed)
  attempt.acknowledge(
    database,
    "observation-dup-sequential",
    attempt_owner,
    sentinel_claimed,
    sentinel_execution,
  )
  |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, sentinel_job_id)

  mark_database_test_executed(
    "acknowledged-observation-reconciled-on-sequential-duplicate-passed",
  )
}

pub fn postgres_acknowledged_observation_reconciled_on_concurrent_duplicate_ack_test() {
  case repeatable_read_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_reconciled_concurrent_duplicate_test(
        database_url,
      )
  }
}

/// Coordinator review, round 1 follow-up: proves the *other* untested
/// receipt-match site — the re-check after a 0-row fenced `UPDATE` inside
/// `acknowledge_transaction` — is `Reconciled`. Reuses
/// `run_ack_duplicate_repeatable_read_test`'s exact forced-overlap
/// mechanism (a `BEFORE UPDATE` trigger parking the first acknowledgement
/// behind a held advisory lock while a second, concurrent acknowledgement
/// for the *same* claim genuinely waits on the row lock the first holds —
/// confirmed via `pg_stat_activity` wait events, not inferred): A's
/// `UPDATE` commits first (a fresh write, `Replied`); B's own `UPDATE` then
/// affects zero rows against the now-committed row and re-checks the
/// receipt, finding A's — this is the site under test, and only reachable
/// this way, not sequentially. Named mutation: hardcoding
/// `via_receipt_match: False` at this site makes this test fail (B's event
/// would report `Replied`, not `Reconciled`, and/or a second `Replied`
/// event would appear instead of one `Replied` and one `Reconciled`).
fn run_acknowledged_observation_reconciled_concurrent_duplicate_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use entries <- with_unique_databases(database_url, [
    "grind_ack_obs_rr_a_" <> suffix,
    "grind_ack_obs_rr_b_" <> suffix,
  ])
  let assert [#(database_a, connection_a), #(database_b, _)] = entries

  let assert Ok(input_codec) =
    worker.codec("ack-obs-rr-input-" <> suffix <> "-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "ack-obs-rr-output-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "ack.obs.rr-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("ack-obs-rr-" <> suffix)
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "dup-concurrent-" <> suffix)
  let test_queue = "ack-obs-rr-" <> suffix
  let attempt_owner = "ack-obs-rr-owner-" <> suffix

  let assert Ok(_handle) =
    postgres.submit(database_a, test_queue, definition, 8)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(database_a, test_queue, workers, attempt_owner, 30_000)
  let execution = attempt.execute_claim(claimed)
  let #(job_id, _, _) = attempt.claim_identity(claimed)

  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(
      "dup-concurrent-" <> suffix,
      fn(measurements, metadata) {
        process.send(signal, AcknowledgedSignal(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let lock_key = unique_test_lock_key(5)
  let trigger_name = "grind_test_ack_obs_overlap_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND OLD.state = 'executing' AND NEW.state <> 'executing' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection_a)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> trigger_name
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection_a)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection_a)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection_a)
    Nil
  })

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_ack_obs_overlap_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection_a, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    attempt.acknowledge(
      database_a,
      test_queue,
      attempt_owner,
      claimed,
      execution,
    )
  })
  await_lock_wait_counts(connection_a, 1, 0, 500) |> should.equal(True)

  spawn_submit(result_b, fn() {
    attempt.acknowledge(
      database_b,
      test_queue,
      attempt_owner,
      claimed,
      execution,
    )
  })
  await_lock_wait_counts(connection_a, 1, 1, 500) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  outcome_a |> should.equal(Ok(True))
  outcome_b |> should.equal(Ok(True))
  count_acknowledgements_for_job(connection_a, job_id) |> should.equal(1)

  let assert Ok(AcknowledgedSignal(_, event_1)) =
    process.receive(signal, within: 5000)
  let assert Ok(AcknowledgedSignal(_, event_2)) =
    process.receive(signal, within: 5000)

  // Exactly two events, deterministically for `database_a`'s own producer
  // stream (the only one anything further runs through here): a sentinel
  // claim/ack through the same database's forwarder must be the very next
  // event. `database_b` is not exercised again after its one duplicate-ack
  // call above, so nothing further could arrive from it either.
  let assert Ok(_sentinel_handle) =
    postgres.submit(database_a, test_queue, sentinel_worker, 11)
  let assert Ok(Some(sentinel_claimed)) =
    attempt.claim_one(database_a, test_queue, workers, attempt_owner, 30_000)
  let sentinel_execution = attempt.execute_claim(sentinel_claimed)
  let #(sentinel_job_id, _, _) = attempt.claim_identity(sentinel_claimed)
  attempt.acknowledge(
    database_a,
    test_queue,
    attempt_owner,
    sentinel_claimed,
    sentinel_execution,
  )
  |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, sentinel_job_id)

  let confirmations = [event_1.confirmation, event_2.confirmation]
  list.contains(confirmations, observation.Replied) |> should.equal(True)
  list.contains(confirmations, observation.Reconciled) |> should.equal(True)
  event_1.command_id |> should.equal(event_2.command_id)
  event_1.committed_state |> should.equal(job.Succeeded)
  event_2.committed_state |> should.equal(job.Succeeded)
  mark_database_test_executed(
    "acknowledged-observation-reconciled-on-concurrent-duplicate-passed",
  )
}

// -- Round 2 `[grind, job, *]` observations --------------------------------
//
// Every descriptor below is emitted through the same `sinal/forwarder` as
// `[grind, job, acknowledged]` above; see `grind/observation`'s module
// documentation for the shared delivery semantics. Each descriptor gets at
// least one test proving emission (with the exact metadata a consumer would
// read) and one proving a read-only/negative outcome emits nothing, using
// the same "assert the very next observation on this exact channel is a
// known sentinel" technique `assert_next_observation_is_sentinel` documents
// above (per-producer FIFO through one Forwarder makes this deterministic,
// not racy).

pub fn postgres_admitted_observation_plain_submit_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_plain_submit_test(database_url)
  }
}

/// Plain `submit`/`submit_at`: `committed_state` and `available_at_unix_ms`
/// come from the insert's own `RETURNING`, `submission_id` is `None`, and
/// `confirmation` is always `Replied` (a plain submission has no receipt
/// concept to reconcile from).
fn run_admitted_plain_submit_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("admitted-plain-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("admitted-plain-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("admitted.plain", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-admitted-plain")
  let assert Ok(attachment) =
    sinal.observe(id, observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(handle) =
    postgres.submit(database, "admitted-plain", definition, 5)
  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.AdmittedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.ref.queue |> should.equal("admitted-plain")
  metadata.ref.worker_id |> should.equal("admitted.plain")
  metadata.ref.worker_version |> should.equal("v1")
  metadata.committed_state |> should.equal(job.Queued)
  metadata.submission_id |> should.equal(None)
  metadata.confirmation |> should.equal(observation.Replied)
  let assert Some(immediate_available_at) = metadata.available_at_unix_ms
  immediate_available_at |> should.not_equal(0)

  let assert Ok(future) = job.available_at(immediate_available_at + 3_600_000)
  let assert Ok(scheduled_handle) =
    postgres.submit_at(database, "admitted-plain", definition, 6, future)
  let assert Ok(#(_, scheduled_metadata)) =
    process.receive(signal, within: 5000)
  scheduled_metadata.ref.job_id
  |> should.equal(job.id_value(scheduled_handle))
  scheduled_metadata.committed_state |> should.equal(job.Scheduled)
  scheduled_metadata.available_at_unix_ms
  |> should.equal(Some(immediate_available_at + 3_600_000))
  scheduled_metadata.confirmation |> should.equal(observation.Replied)

  // A `submit_at` whose target is already in the past by the *database's*
  // clock still commits `queued` (`grind_jobs`'s own `CASE ... <=
  // clock_timestamp()`), not `scheduled` — proving `committed_state` is read
  // back from that same `RETURNING`, never inferred client-side from the
  // request's own "immediate vs. future" intent.
  let assert Ok(past) = job.available_at(immediate_available_at - 3_600_000)
  let assert Ok(past_handle) =
    postgres.submit_at(database, "admitted-plain", definition, 7, past)
  let assert Ok(#(_, past_metadata)) = process.receive(signal, within: 5000)
  past_metadata.ref.job_id |> should.equal(job.id_value(past_handle))
  past_metadata.committed_state |> should.equal(job.Queued)
  mark_database_test_executed("admitted-observation-plain-submit-passed")
}

pub fn postgres_admitted_observation_unique_inserted_and_reconciled_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_admitted_unique_inserted_reconciled_test(database_url)
  }
}

/// A fresh unique admission (`Inserted`) is `Replied`, with a known
/// `available_at_unix_ms`. Replaying the exact same `submission_id` and
/// request hits the receipt inside `admission_transaction` itself before any
/// candidate row is even looked up — proven committed by that receipt read,
/// not a fresh write — so the second observation is `Reconciled` with
/// `available_at_unix_ms: None` (the receipt-matched path never re-derives
/// it, the same limitation `acknowledged` documents for its own
/// receipt-matched commits).
fn run_admitted_unique_inserted_reconciled_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_admitted_unique_inserted",
  )
  let worker_def = unique_test_worker("admitted.unique.inserted-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "admitted-unique-" <> suffix
  let submission_text = "admitted-unique-1-" <> suffix

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-admitted-unique-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(id, observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
      policy,
    )
  let assert Ok(#(_, first_metadata)) = process.receive(signal, within: 5000)
  first_metadata.ref.job_id |> should.equal(job.id_value(handle))
  first_metadata.submission_id |> should.equal(Some(submission_text))
  first_metadata.confirmation |> should.equal(observation.Replied)
  first_metadata.committed_state |> should.equal(job.Queued)
  let assert Some(_) = first_metadata.available_at_unix_ms

  let assert Ok(submission.Inserted(replayed)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
      policy,
    )
  job.id_value(replayed) |> should.equal(job.id_value(handle))
  let assert Ok(#(_, second_metadata)) = process.receive(signal, within: 5000)
  second_metadata.ref.job_id |> should.equal(job.id_value(handle))
  second_metadata.submission_id |> should.equal(Some(submission_text))
  second_metadata.confirmation |> should.equal(observation.Reconciled)
  second_metadata.available_at_unix_ms |> should.equal(None)
  mark_database_test_executed(
    "admitted-observation-unique-inserted-reconciled-passed",
  )
}

pub fn postgres_admitted_observation_unique_existing_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_unique_existing_test(database_url)
  }
}

/// A distinct `submission_id` that lands on an already-occupied uniqueness
/// key (`Existing`) is its own fresh commit (`Replied`) — this exact
/// submission's own receipt row is what got written, even though the job row
/// itself is untouched.
fn run_admitted_unique_existing_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_admitted_unique_existing",
  )
  let worker_def = unique_test_worker("admitted.unique.existing-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "admitted-unique-existing-" <> suffix

  // Attached before any submission: `forwarder.emit` only sends the event to
  // the forwarder process and returns — it does not wait for that process to
  // actually run the attached handler — so a handler attached only *after* a
  // call returns can still race that call's own not-yet-processed emission.
  let signal = process.new_subject()
  let assert Ok(id) =
    sinal.handler_id("grind-test-admitted-unique-existing-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(id, observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-existing-1-" <> suffix,
      worker_def,
      3,
      policy,
    )
  let assert Ok(#(_, first_metadata)) = process.receive(signal, within: 5000)
  first_metadata.submission_id
  |> should.equal(Some("admitted-existing-1-" <> suffix))
  first_metadata.confirmation |> should.equal(observation.Replied)

  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-existing-2-" <> suffix,
      worker_def,
      3,
      policy,
    )
  let assert Ok(#(_, metadata)) = process.receive(signal, within: 5000)
  metadata.ref.job_id |> should.equal(submission.conflict_job_id(conflict))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.submission_id |> should.equal(Some("admitted-existing-2-" <> suffix))
  metadata.confirmation |> should.equal(observation.Replied)
  metadata.committed_state |> should.equal(job.Queued)
  mark_database_test_executed(
    "admitted-observation-unique-existing-conflict-passed",
  )
}

pub fn postgres_admitted_observation_existing_over_executing_available_at_none_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_existing_over_executing_test(database_url)
  }
}

/// `available_at_unix_ms` is `Some` only when the committed/observed state
/// is `Queued`, `Scheduled`, or `Retryable` — an `Existing` conflict can
/// land on any other policy-eligible state too (`Incomplete` reaches as far
/// as `Executing`), where the row's raw `available_at` is not a real
/// next-run time and must be reported `None`.
fn run_admitted_existing_over_executing_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_admitted_existing_executing_" <> suffix,
  )
  let worker_def = unique_test_worker("admitted.existing.executing-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "admitted-existing-executing-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-existing-executing-1-" <> suffix,
      worker_def,
      9,
      policy,
    )
  // Force the row into `executing` directly (no queue actor needed) so the
  // later `Existing` conflict below lands on a non-eligibility state.
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 1, attempt_epoch = 1, attempt_owner = 'x', lease_expires_at = clock_timestamp() + interval '1 hour' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let assert Ok(id) =
    sinal.handler_id("grind-test-admitted-existing-executing-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(id, observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Existing(conflict)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-existing-executing-2-" <> suffix,
      worker_def,
      9,
      policy,
    )
  submission.conflict_state(conflict) |> should.equal(job.Executing)
  let assert Ok(#(_, metadata)) = process.receive(signal, within: 5000)
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.committed_state |> should.equal(job.Executing)
  metadata.available_at_unix_ms |> should.equal(None)
  mark_database_test_executed(
    "admitted-observation-existing-over-executing-available-at-none-passed",
  )
}

pub fn postgres_admitted_observation_absent_on_submission_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_absent_on_conflict_test(database_url)
  }
}

/// `SubmissionConflict` (the same `submission_id` reused for a materially
/// different request) never reaches the database's own commit, so it must
/// never emit — proven here by a subsequent, distinct submission through the
/// exact same producer/forwarder arriving as the very next `admitted`
/// observation.
fn run_admitted_absent_on_conflict_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_admitted_conflict",
  )
  let worker_def = unique_test_worker("admitted.conflict-" <> suffix)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "admitted-conflict-" <> suffix
  let submission_text = "admitted-conflict-1-" <> suffix

  let signal = process.new_subject()
  let assert Ok(id) =
    sinal.handler_id("grind-test-admitted-conflict-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(id, observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
      policy,
    )
  let assert Ok(#(_, _)) = process.receive(signal, within: 5000)

  submit_keep_existing(
    database,
    test_queue,
    submission_text,
    worker_def,
    4,
    policy,
  )
  |> should.equal(Error(submission.SubmissionConflict))

  let assert Ok(submission.Inserted(sentinel_handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-conflict-sentinel-" <> suffix,
      worker_def,
      5,
      policy,
    )
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  mark_database_test_executed(
    "admitted-observation-absent-on-submission-conflict-passed",
  )
}

pub fn postgres_admitted_observation_absent_from_reconcile_unique_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_admitted_absent_from_reconcile_unique_test(database_url)
  }
}

/// Public `reconcile_unique` follows round 1's decision for
/// `reconcile_acknowledgement`: it never emits, even though it can recover a
/// genuine committed outcome (`Inserted`) from a `CommitUnknown` command —
/// reusing the exact "(d) committed, reply lost, pool closed" scenario from
/// `run_unique_committed_reply_lost_store_unavailable_test` above. Both are
/// pure receipt reads offered for a caller to recover its own return value
/// after a lost reply, not a fresh proof of commit tied to a call this
/// module owns end to end — `submit_unique` itself already emits
/// `Reconciled` for the equivalent in-call recovery (see the
/// inserted-and-reconciled test above); a separate, possibly much later
/// `reconcile_unique` call must not double-report the same commit.
fn run_admitted_absent_from_reconcile_unique_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let signal = process.new_subject()
  let assert Ok(id) =
    sinal.handler_id("grind-test-admitted-reconcile-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(id, observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(Nil) = postgres.migrate(database)

  let observer_settings =
    postgres.settings(database_url)
    |> postgres.with_pool_size(1)
  let assert Ok(observer_validated) = postgres.validate(observer_settings)
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = postgres.connection(observer)
  require_syncrep_cluster_configured(observer_connection)

  let worker_def = unique_test_worker("admitted.reconcile-" <> suffix)
  let test_queue = "admitted-reconcile-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "admitted-reconcile-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    observer_connection,
    "grind_test_admitted_reconcile_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> submission_text <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
      policy,
    )
  })
  let assert Ok(backend_pid) =
    wait_for_syncrep_trigger_backend(observer_connection, 300)

  let _ = postgres.close(database)
  let assert Ok(Error(submission.CommitUnknown(pending))) =
    process.receive(reply, within: 10_000)
  // No `admitted` observation for the `CommitUnknown` outcome itself.
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  let assert Ok(reopened_validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(reopened) = postgres.start(reopened_validated)
  use <- exception.defer(fn() { postgres.close(reopened) })

  // Zombie still parked: a pure receipt lookup still finds nothing.
  let assert Error(submission.CommitUnknown(_)) =
    postgres.reconcile_unique(reopened, pending)
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  terminate_backend(observer_connection, backend_pid) |> should.equal(True)
  let assert Ok(Nil) =
    wait_for_backend_gone(observer_connection, backend_pid, 300)

  let assert Ok(submission.Inserted(handle)) =
    postgres.reconcile_unique(reopened, pending)
  postgres.arguments(reopened, handle) |> should.equal(Ok(1))
  // Still nothing on the `admitted` channel from `reconcile_unique` itself,
  // even though it just recovered a genuinely committed `Inserted` outcome.
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  // Sentinel: the producer/forwarder itself is still alive and correctly
  // wired for a genuine fresh admission afterward.
  let assert Ok(submission.Inserted(sentinel_handle)) =
    submit_keep_existing(
      reopened,
      test_queue,
      "admitted-reconcile-sentinel-" <> suffix,
      worker_def,
      2,
      policy,
    )
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  mark_database_test_executed(
    "admitted-observation-absent-from-reconcile-unique-passed",
  )
}

pub fn postgres_admitted_observation_in_call_post_commit_unknown_reconciled_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_admitted_in_call_reconciled_test(database_url)
  }
}

/// The other `Reconciled` path, distinct from the in-transaction receipt hit
/// the inserted-and-reconciled test above proves: `submit_unique`'s own
/// commit reply is lost (the `SyncRep`-park-then-terminate harness), so its
/// transaction result comes back as `pog.TransactionQueryError` — but
/// `run`'s own follow-up `reconcile_from_receipt` call, made within this
/// exact same `submit_unique` call before it ever returns, finds the
/// now-visible receipt and resolves `Ok(Inserted(handle))` transparently
/// (`run_unique_committed_reply_lost_test`'s own scenario). This is proven
/// committed by a receipt read, not a fresh write, so exactly one `admitted`
/// event is emitted, `Reconciled`, with `available_at_unix_ms: None`.
fn run_admitted_in_call_reconciled_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let signal = process.new_subject()
  let assert Ok(id) =
    sinal.handler_id("grind-test-admitted-in-call-reconciled-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(id, observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)

  let worker_def = unique_test_worker("admitted.in-call-reconciled-" <> suffix)
  let test_queue = "admitted-in-call-reconciled-" <> suffix
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let submission_text = "admitted-in-call-reconciled-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_admitted_in_call_reconciled_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> submission_text <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_keep_existing(
      database,
      test_queue,
      submission_text,
      worker_def,
      7,
      policy,
    )
  })

  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Ok(submission.Inserted(handle))) =
    process.receive(reply, within: 10_000)
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)

  let assert Ok(#(_, metadata)) = process.receive(signal, within: 5000)
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.submission_id |> should.equal(Some(submission_text))
  metadata.confirmation |> should.equal(observation.Reconciled)
  metadata.available_at_unix_ms |> should.equal(None)

  // Exactly one: the sentinel through the exact same producer is the very
  // next observation on this channel.
  let assert Ok(submission.Inserted(sentinel_handle)) =
    submit_keep_existing(
      database,
      test_queue,
      "admitted-in-call-reconciled-sentinel-" <> suffix,
      worker_def,
      8,
      policy,
    )
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  mark_database_test_executed(
    "admitted-observation-in-call-post-commit-unknown-reconciled-passed",
  )
}

pub fn postgres_claimed_observation_emission_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_claimed_observation_emission_test(database_url)
  }
}

/// The claim itself autocommits as a single fenced `UPDATE ... RETURNING`,
/// so a returned row is already the proof of commit: `attempt_id`/`epoch`
/// come from that same row, `attempt` is the row's own `attempt_count`, and
/// `previous_state` is what the row held immediately before this claim.
fn run_claimed_observation_emission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("claimed-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("claimed-emission-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "claimed.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("claimed-emission")
  let assert Ok(workers) = registry.register(workers, definition)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-claimed-emission")
  let assert Ok(attachment) =
    sinal.observe(id, observation.claimed(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(handle) =
    postgres.submit(database, "claimed-emission", definition, 5)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "claimed-emission",
      workers,
      "claimed-emission-owner",
      30_000,
    )
  let #(claimed_id, attempt_id, epoch) = attempt.claim_identity(claimed)
  claimed_id |> should.equal(job.id_value(handle))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.ClaimedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(claimed_id)
  metadata.ref.queue |> should.equal("claimed-emission")
  metadata.ref.worker_id |> should.equal("claimed.emission")
  metadata.ref.worker_version |> should.equal("v1")
  metadata.attempt.attempt_id |> should.equal(attempt_id)
  metadata.attempt.epoch |> should.equal(epoch)
  metadata.attempt.attempt |> should.equal(1)
  metadata.previous_state |> should.equal(job.Queued)
  mark_database_test_executed("claimed-observation-emission-passed")
}

pub fn postgres_claimed_observation_absent_when_nothing_due_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_claimed_observation_absent_when_nothing_due_test(database_url)
  }
}

/// `claim_one` returning `Ok(None)` (nothing due) never calls the emit path
/// at all — proven here by a real claim through the exact same producer
/// arriving as the very next `claimed` observation.
fn run_claimed_observation_absent_when_nothing_due_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("claimed-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("claimed-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("claimed.absent", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("claimed-absent")
  let assert Ok(workers) = registry.register(workers, definition)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-claimed-absent")
  let assert Ok(attachment) =
    sinal.observe(id, observation.claimed(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  attempt.claim_one(
    database,
    "claimed-absent",
    workers,
    "claimed-absent-owner",
    30_000,
  )
  |> should.equal(Ok(None))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  let assert Ok(sentinel_handle) =
    postgres.submit(database, "claimed-absent", definition, 6)
  let assert Ok(Some(sentinel_claimed)) =
    attempt.claim_one(
      database,
      "claimed-absent",
      workers,
      "claimed-absent-owner",
      30_000,
    )
  let #(sentinel_id, _, _) = attempt.claim_identity(sentinel_claimed)
  sentinel_id |> should.equal(job.id_value(sentinel_handle))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(sentinel_id)
  mark_database_test_executed(
    "claimed-observation-absent-when-nothing-due-passed",
  )
}

pub fn postgres_quarantined_observation_emission_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_quarantined_observation_emission_test(database_url)
  }
}

/// The claim-time quarantine scan finds at most one abandoned attempt per
/// call (`LIMIT 1`); this drives it twice to observe one event per row, with
/// `cancellation_was_requested` distinguishing an ordinary abandoned attempt
/// from one that also had a pending cancellation request.
fn run_quarantined_observation_emission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("quarantined-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("quarantined-emission-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "quarantined.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("quarantined-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle_a) =
    postgres.submit(database, "quarantined-emission", definition, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "quarantined-emission", definition, 2)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_count = 1, attempt_owner = 'dead-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle_a)))
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_count = 1, attempt_owner = 'dead-consumer', lease_expires_at = clock_timestamp(), cancel_requested_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle_b)))
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-quarantined-emission")
  let assert Ok(attachment) =
    sinal.observe(id, observation.quarantined(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  attempt.claim_one(
    database,
    "quarantined-emission",
    workers,
    "quarantined-emission-owner",
    30_000,
  )
  |> should.equal(Ok(None))
  let assert Ok(#(measurements_a, metadata_a)) =
    process.receive(signal, within: 5000)
  measurements_a |> should.equal(observation.QuarantinedMeasurements(count: 1))
  metadata_a.ref.job_id |> should.equal(job.id_value(handle_a))
  metadata_a.ref.queue |> should.equal("quarantined-emission")
  metadata_a.ref.worker_id |> should.equal("quarantined.emission")
  metadata_a.attempt.epoch |> should.equal(1)
  metadata_a.attempt.attempt |> should.equal(1)
  metadata_a.cancellation_was_requested |> should.equal(False)

  attempt.claim_one(
    database,
    "quarantined-emission",
    workers,
    "quarantined-emission-owner",
    30_000,
  )
  |> should.equal(Ok(None))
  let assert Ok(#(_, metadata_b)) = process.receive(signal, within: 5000)
  metadata_b.ref.job_id |> should.equal(job.id_value(handle_b))
  metadata_b.cancellation_was_requested |> should.equal(True)

  postgres.state(database, handle_a) |> should.equal(Ok(job.Uncertain))
  postgres.state(database, handle_b) |> should.equal(Ok(job.Uncertain))
  mark_database_test_executed("quarantined-observation-emission-passed")
}

pub fn postgres_quarantined_observation_absent_when_nothing_expired_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_quarantined_observation_absent_test(database_url)
  }
}

/// The quarantine scan runs on every `claim_one` call, whether or not
/// anything is actually expired; an ordinary claim with nothing to quarantine
/// must never emit — proven by a genuinely quarantined row through the exact
/// same producer arriving as the very next `quarantined` observation.
fn run_quarantined_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("quarantined-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("quarantined-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "quarantined.absent",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("quarantined-absent")
  let assert Ok(workers) = registry.register(workers, definition)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-quarantined-absent")
  let assert Ok(attachment) =
    sinal.observe(id, observation.quarantined(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(handle) =
    postgres.submit(database, "quarantined-absent", definition, 3)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "quarantined-absent",
      workers,
      "quarantined-absent-owner",
      30_000,
    )
  let #(claimed_id, _, _) = attempt.claim_identity(claimed)
  claimed_id |> should.equal(job.id_value(handle))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(claimed_id))
    |> pog.execute(on: connection)
  attempt.claim_one(
    database,
    "quarantined-absent",
    workers,
    "quarantined-absent-owner",
    30_000,
  )
  |> should.equal(Ok(None))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(claimed_id)
  mark_database_test_executed("quarantined-observation-absent-passed")
}

pub fn postgres_resolved_observation_replied_and_reconciled_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_resolved_observation_replied_reconciled_test(database_url)
  }
}

/// The first audited resolution of an `uncertain` job is this call's own
/// fresh commit (`Replied`); replaying the exact same `resolution_id` is
/// proven by `resolution_receipt_outcome`'s own receipt read
/// (`ResolutionAlreadyApplied`), so the second observation is `Reconciled`.
fn run_resolved_observation_replied_reconciled_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolved-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolved-emission-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "resolved.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "resolved-emission", definition, 12)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 501, attempt_epoch = 3, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolved-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-resolved-emission")
  let assert Ok(attachment) =
    sinal.observe(id, observation.resolved(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-emission-1",
      "on-call",
      "confirm before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(#(measurements, first_metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.ResolvedMeasurements(count: 1))
  first_metadata.ref.job_id |> should.equal(job.id_value(handle))
  first_metadata.ref.queue |> should.equal("resolved-emission")
  first_metadata.decision |> should.equal(observation.DecisionAuthorizeReplay)
  first_metadata.committed_state |> should.equal(job.Queued)
  first_metadata.resolution_id |> should.equal("resolution-emission-1")
  first_metadata.resolved_by |> should.equal("on-call")
  first_metadata.confirmation |> should.equal(observation.Replied)

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-emission-1",
      "on-call",
      "confirm before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))
  let assert Ok(#(_, second_metadata)) = process.receive(signal, within: 5000)
  second_metadata.ref.job_id |> should.equal(job.id_value(handle))
  second_metadata.confirmation |> should.equal(observation.Reconciled)
  mark_database_test_executed("resolved-observation-replied-reconciled-passed")
}

pub fn postgres_resolved_observation_absent_on_reconciliation_not_required_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_resolved_observation_absent_test(database_url)
  }
}

/// `resolve_uncertain` against a job that is not (or no longer) `uncertain`
/// commits nothing (`ReconciliationNotRequired`) and must never emit —
/// proven by a genuine audited resolution through the exact same producer
/// arriving as the very next `resolved` observation.
fn run_resolved_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolved-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolved-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("resolved.absent", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(queued_handle) =
    postgres.submit(database, "resolved-absent", definition, 4)
  let assert Ok(uncertain_handle) =
    postgres.submit(database, "resolved-absent", definition, 5)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 777, attempt_epoch = 2, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(uncertain_handle)))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolved-absent")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  // The quarantine scan moves `uncertain_handle` to `uncertain`; the
  // still-genuinely-due `queued_handle` is what this same call then claims
  // and runs to completion.
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, queued_handle) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, uncertain_handle) |> should.equal(Ok(job.Uncertain))

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-resolved-absent")
  let assert Ok(attachment) =
    sinal.observe(id, observation.resolved(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  postgres.resolve_uncertain(
    database,
    queued_handle,
    postgres.ResolutionRequest(
      "resolution-absent-1",
      "on-call",
      "not actually uncertain",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Error(postgres.ReconciliationNotRequired))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  postgres.resolve_uncertain(
    database,
    uncertain_handle,
    postgres.ResolutionRequest(
      "resolution-absent-2",
      "on-call",
      "genuinely uncertain",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(uncertain_handle))
  mark_database_test_executed("resolved-observation-absent-passed")
}

pub fn postgres_resolved_observation_absent_on_commit_unknown_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_resolved_observation_commit_unknown_test(database_url)
  }
}

/// A genuinely aborted commit (a deferred trigger's `pg_sleep` fires during
/// `resolve_uncertain`'s own transaction `COMMIT`; killing that backend
/// aborts the whole transaction — nothing committed) reports
/// `ResolutionCommitUnknown` and must never emit — the same "commit
/// genuinely unknown" case `postgres_submit_unique_aborted_commit_is_commit_unknown_test`
/// proves for admission. Once the trigger is dropped, retrying the exact
/// same `resolution_id` against the still-`uncertain` job (the aborted
/// transaction rolled back its own `grind_jobs` update too) is the sentinel
/// through the same producer.
fn run_resolved_observation_commit_unknown_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolved-commit-unknown-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "resolved-commit-unknown-output-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "resolved.commit.unknown-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(
      database,
      "resolved-commit-unknown-" <> suffix,
      definition,
      3,
    )
  let assert Ok(sentinel_handle) =
    postgres.submit(
      database,
      "resolved-commit-unknown-" <> suffix,
      definition,
      4,
    )
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 909, attempt_epoch = 4, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 910, attempt_epoch = 4, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(sentinel_handle)))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolved-commit-unknown-" <> suffix)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(False))
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.state(database, sentinel_handle) |> should.equal(Ok(job.Uncertain))

  let resolution_id = "resolution-commit-unknown-" <> suffix
  let trigger_name = "grind_test_resolved_commit_unknown_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT (NEW.resolution_id = '"
      <> resolution_id
      <> "') THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> trigger_name
      <> " AFTER INSERT ON grind_job_resolutions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection)
  let drop_trigger = fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_job_resolutions",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
  use <- exception.defer(drop_trigger)

  let signal = process.new_subject()
  let assert Ok(id) =
    sinal.handler_id("grind-test-resolved-commit-unknown-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(id, observation.resolved(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(
        reply,
        postgres.resolve_uncertain(
          database,
          handle,
          postgres.ResolutionRequest(
            resolution_id,
            "on-call",
            "aborted commit proof",
            postgres.AuthorizeReplay,
          ),
        ),
      )
    })
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(postgres.ResolutionCommitUnknown(returned_resolution_id))) =
    process.receive(reply, within: 10_000)
  returned_resolution_id |> should.equal(resolution_id)
  process.receive(signal, within: 0) |> should.equal(Error(Nil))
  drop_trigger()

  // The pool just had a connection deliberately terminated
  // (`terminate_backend`, above): `pgo_connection`'s own supervised restart
  // of that connection is a real, transient recovery window, not a
  // steady-state failure — see `retry_transient_query`'s other call sites
  // (for example `run_unique_aborted_commit_test`) for the same pattern.
  retry_transient_query(fn() { postgres.state(database, handle) }, 20)
  |> should.equal(Ok(job.Uncertain))

  // Sentinel: a distinct job/resolution through the exact same producer, so
  // a stray event wrongly emitted for the commit-unknown job above (which
  // would carry *that* job's id) is caught as a mismatch here rather than
  // coincidentally matching.
  retry_transient_query(
    fn() {
      postgres.resolve_uncertain(
        database,
        sentinel_handle,
        postgres.ResolutionRequest(
          "resolution-commit-unknown-sentinel-" <> suffix,
          "on-call",
          "aborted commit proof",
          postgres.AuthorizeReplay,
        ),
      )
    },
    20,
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  sentinel_metadata.confirmation |> should.equal(observation.Replied)
  mark_database_test_executed(
    "resolved-observation-absent-on-commit-unknown-passed",
  )
}

pub fn postgres_cancellation_observation_before_run_and_requested_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancellation_observation_emission_test(database_url)
  }
}

/// `CancelledBeforeRun` (a queued job cancelled before any attempt) and
/// `CancellationRequested` (an executing job) are the only two genuine
/// writes; `CancellationRequested` can repeat verbatim for an idempotent
/// re-request (`cancel_executing`'s own `COALESCE` re-affirms rather than
/// rejects), proven here by cancelling the same executing job twice.
fn run_cancellation_observation_emission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancellation-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancellation-emission-output-v1", json.string, decode.string)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "cancellation.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok(int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("cancellation-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(queued_handle) =
    postgres.submit(database, "cancellation-emission", definition, 1)
  let assert Ok(executing_handle) =
    postgres.submit(database, "cancellation-emission", definition, 2)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-cancellation-emission")
  let assert Ok(attachment) =
    sinal.observe(
      id,
      observation.cancellation_decided(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  postgres.cancel(database, queued_handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  let assert Ok(#(measurements, before_run_metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.CancellationMeasurements(count: 1))
  before_run_metadata.ref.job_id |> should.equal(job.id_value(queued_handle))
  before_run_metadata.ref.queue |> should.equal("cancellation-emission")
  before_run_metadata.previous_state |> should.equal(job.Queued)
  before_run_metadata.outcome
  |> should.equal(observation.CancellationDecidedBeforeRun)

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() { process.send(release, ReleaseAttempt) })

  postgres.cancel(database, executing_handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  let assert Ok(#(_, requested_metadata_1)) =
    process.receive(signal, within: 5000)
  requested_metadata_1.ref.job_id
  |> should.equal(job.id_value(executing_handle))
  requested_metadata_1.previous_state |> should.equal(job.Executing)
  requested_metadata_1.outcome
  |> should.equal(observation.CancellationDecidedWhileRunning)

  // Idempotent re-request: the same outcome, delivered again.
  postgres.cancel(database, executing_handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  let assert Ok(#(_, requested_metadata_2)) =
    process.receive(signal, within: 5000)
  requested_metadata_2.outcome
  |> should.equal(observation.CancellationDecidedWhileRunning)

  process.send(release, ReleaseAttempt)
  let assert Ok(_) = process.receive(reply, within: 5000)
  postgres.state(database, executing_handle) |> should.equal(Ok(job.Cancelled))
  mark_database_test_executed("cancellation-observation-emission-passed")
}

pub fn postgres_cancellation_observation_absent_on_already_cancelled_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_cancellation_observation_absent_test(database_url)
  }
}

/// The read-only outcomes (`AlreadyCancelled`, `AlreadyUncertain`,
/// `AlreadyFinished`) commit nothing and must never emit — proven here by
/// cancelling an already-cancelled job, then a genuine cancellation through
/// the exact same producer arriving as the very next `cancellation`
/// observation.
fn run_cancellation_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancellation-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("cancellation-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "cancellation.absent",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "cancellation-absent", definition, 1)
  let assert Ok(other_handle) =
    postgres.submit(database, "cancellation-absent", definition, 2)

  // Attach *before* the first (genuinely emitting) cancellation, not after:
  // `forwarder.emit` hands the event to a separate forwarder process that
  // dispatches it asynchronously, so a handler attached immediately after
  // an emitting call returns can still race that call's own not-yet-
  // delivered event and wrongly observe it as if it belonged to a later,
  // supposedly silent operation. Draining and asserting this first event
  // ourselves — rather than starting from a subject that might already
  // have it queued — removes that race entirely.
  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-cancellation-absent")
  let assert Ok(attachment) =
    sinal.observe(
      id,
      observation.cancellation_decided(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  let assert Ok(#(_, first_metadata)) = process.receive(signal, within: 5000)
  first_metadata.ref.job_id |> should.equal(job.id_value(handle))

  // `AlreadyCancelled` is read-only and commits nothing, so it must never
  // emit — proven the same deterministic way as every other `_absent_`
  // observation test in this file: a following genuine cancellation
  // through the exact same producer must be the very next event on
  // `signal`, not a bounded `receive(within: 0)` that cannot distinguish
  // "genuinely never emitted" from "emitted, but not yet delivered".
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.AlreadyCancelled))

  postgres.cancel(database, other_handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(other_handle))
  mark_database_test_executed("cancellation-observation-absent-passed")
}

pub fn postgres_cancellation_observation_absent_on_commit_unknown_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_cancellation_observation_commit_unknown_test(database_url)
  }
}

/// A genuinely aborted commit (a deferred trigger's `pg_sleep` fires during
/// `cancel`'s own transaction `COMMIT`; killing that backend aborts the
/// whole transaction, including its own `grind_jobs` update) reports
/// `CancellationCommitUnknown` and must never emit. Once the trigger is
/// dropped, cancelling the same still-`queued` job for real is the sentinel
/// through the same producer.
fn run_cancellation_observation_commit_unknown_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("cancellation-commit-unknown-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "cancellation-commit-unknown-output-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "cancellation.commit.unknown",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "cancellation-commit-unknown", definition, 1)
  let assert Ok(sentinel_handle) =
    postgres.submit(database, "cancellation-commit-unknown", definition, 2)
  let connection = postgres.connection(database)
  let job_id = job.id_value(handle)

  let trigger_name = "grind_test_cancellation_commit_unknown_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT (NEW.id = "
      <> int.to_string(job_id)
      <> ") THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> trigger_name
      <> " AFTER UPDATE ON grind_jobs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection)
  let drop_trigger = fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
  use <- exception.defer(drop_trigger)

  let signal = process.new_subject()
  let assert Ok(id) =
    sinal.handler_id("grind-test-cancellation-commit-unknown-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(
      id,
      observation.cancellation_decided(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, postgres.cancel(database, handle))
    })
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(postgres.CancellationCommitUnknown)) =
    process.receive(reply, within: 10_000)
  process.receive(signal, within: 0) |> should.equal(Error(Nil))
  drop_trigger()

  postgres.state(database, handle) |> should.equal(Ok(job.Queued))

  // Sentinel: a distinct job through the exact same producer, so a stray
  // event wrongly emitted for the commit-unknown job above (which would
  // carry *that* job's id) is caught as a mismatch here rather than
  // coincidentally matching (cancelling the same job again would emit the
  // same `CancelledBeforeRun` either way, which could not tell the
  // two apart).
  postgres.cancel(database, sentinel_handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  mark_database_test_executed(
    "cancellation-observation-absent-on-commit-unknown-passed",
  )
}

pub fn postgres_released_observation_emission_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_released_observation_emission_test(database_url)
  }
}

/// `release_unstarted_claim` refunds a claim whose temporary worker child
/// never started (before `execute_claim`/`acknowledge_claim` ever run).
/// `restored_state` is the same state the row held before this claim.
fn run_released_observation_emission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("released-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("released-emission-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "released.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("released-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "released-emission", definition, 1)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-released-emission")
  let assert Ok(attachment) =
    sinal.observe(id, observation.released(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "released-emission",
      workers,
      "released-emission-owner",
      30_000,
    )
  let #(claimed_id, attempt_id, epoch) = attempt.claim_identity(claimed)
  attempt.release_unstarted(
    database,
    "released-emission",
    "released-emission-owner",
    claimed,
  )
  |> should.equal(Ok(True))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.ReleasedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(claimed_id)
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.attempt.attempt_id |> should.equal(attempt_id)
  metadata.attempt.epoch |> should.equal(epoch)
  metadata.attempt.attempt |> should.equal(1)
  metadata.restored_state |> should.equal(job.Queued)
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  mark_database_test_executed("released-observation-emission-passed")
}

pub fn postgres_released_observation_absent_when_not_unstarted_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_released_observation_absent_test(database_url)
  }
}

/// `release_unstarted_claim` returning `Ok(False)` (the attempt fence no
/// longer matches — here, because the claim was already acknowledged) must
/// never emit — proven by a genuine release through the exact same producer
/// arriving as the very next `released` observation.
fn run_released_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("released-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("released-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("released.absent", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("released-absent")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(_) = postgres.submit(database, "released-absent", definition, 1)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-released-absent")
  let assert Ok(attachment) =
    sinal.observe(id, observation.released(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "released-absent",
      workers,
      "released-absent-owner",
      30_000,
    )
  let execution = attempt.execute_claim(claimed)
  attempt.acknowledge(
    database,
    "released-absent",
    "released-absent-owner",
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  attempt.release_unstarted(
    database,
    "released-absent",
    "released-absent-owner",
    claimed,
  )
  |> should.equal(Ok(False))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  let assert Ok(sentinel_handle) =
    postgres.submit(database, "released-absent", definition, 2)
  let assert Ok(Some(sentinel_claimed)) =
    attempt.claim_one(
      database,
      "released-absent",
      workers,
      "released-absent-owner",
      30_000,
    )
  let #(sentinel_id, _, _) = attempt.claim_identity(sentinel_claimed)
  sentinel_id |> should.equal(job.id_value(sentinel_handle))
  attempt.release_unstarted(
    database,
    "released-absent",
    "released-absent-owner",
    sentinel_claimed,
  )
  |> should.equal(Ok(True))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(sentinel_id)
  mark_database_test_executed("released-observation-absent-passed")
}

pub fn postgres_contract_mismatch_observation_emission_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_contract_mismatch_observation_emission_test(database_url)
  }
}

/// A stored `output_version` that no longer matches the currently registered
/// worker's codec (a deploy changed the codec without a worker/version bump)
/// releases the claim as `contract_mismatch` — the same forced mismatch
/// `run_batch_partial_error_test` uses.
fn run_contract_mismatch_observation_emission_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("contract-mismatch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("contract-mismatch-output-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define(
      "contract.mismatch.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(workers) = registry.new("contract-mismatch-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "contract-mismatch-emission", definition, 1)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("contract-mismatch-output-v2"))
    |> pog.parameter(pog.text("contract.mismatch.emission"))
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-contract-mismatch-emission")
  let assert Ok(attachment) =
    sinal.observe(
      id,
      observation.contract_mismatch_recorded(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: worker.OutputCodec,
        expected: "contract-mismatch-output-v2",
        actual: "contract-mismatch-output-v1",
      )),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.ContractMismatch))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements
  |> should.equal(observation.ContractMismatchMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(job.id_value(handle))
  metadata.ref.queue |> should.equal("contract-mismatch-emission")
  metadata.ref.worker_id |> should.equal("contract.mismatch.emission")
  metadata.attempt.attempt |> should.equal(1)
  metadata.kind |> should.equal(worker.OutputCodec)
  metadata.expected_version |> should.equal("contract-mismatch-output-v2")
  metadata.actual_version |> should.equal("contract-mismatch-output-v1")
  mark_database_test_executed("contract-mismatch-observation-emission-passed")
}

pub fn postgres_contract_mismatch_observation_absent_on_matching_codec_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_contract_mismatch_observation_absent_test(database_url)
  }
}

/// An ordinary claim whose stored codec versions match the registered worker
/// never releases as `contract_mismatch` — proven by a following genuine
/// mismatch through the exact same producer arriving as the very next
/// `contract_mismatch` observation.
fn run_contract_mismatch_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("contract-mismatch-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("contract-mismatch-absent-output-v1", json.int, decode.int)
  let assert Ok(definition) =
    worker.define(
      "contract.mismatch.absent",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(workers) = registry.new("contract-mismatch-absent")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(matching_handle) =
    postgres.submit(database, "contract-mismatch-absent", definition, 1)
  let assert Ok(mismatched_handle) =
    postgres.submit(database, "contract-mismatch-absent", definition, 2)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE id = $2")
    |> pog.parameter(pog.text("contract-mismatch-absent-output-v2"))
    |> pog.parameter(pog.int(job.id_value(mismatched_handle)))
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let assert Ok(id) = sinal.handler_id("grind-test-contract-mismatch-absent")
  let assert Ok(attachment) =
    sinal.observe(
      id,
      observation.contract_mismatch_recorded(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, matching_handle) |> should.equal(Ok(job.Succeeded))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: worker.OutputCodec,
        expected: "contract-mismatch-absent-output-v2",
        actual: "contract-mismatch-absent-output-v1",
      )),
    ),
  )
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(mismatched_handle))
  mark_database_test_executed("contract-mismatch-observation-absent-passed")
}

type OrderingEvent {
  ClaimedOrderingEvent(attempt_id: Int)
  AcknowledgedOrderingEvent(attempt_id: Int)
}

pub fn postgres_claimed_observation_precedes_acknowledged_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_claimed_precedes_acknowledged_test(database_url)
  }
}

/// A coordinator's `[grind, job, claimed]` for one attempt always arrives
/// before that same attempt's `[grind, job, acknowledged]`: both are emitted
/// by the same producer (the queue actor claiming, then acknowledging, one
/// attempt) through the one `Forwarder` a `Database` owns, and
/// `sinal/forwarder` guarantees per-producer FIFO delivery — proven here by
/// receiving both, tagged by their shared `attempt_id`, in that exact order.
fn run_claimed_precedes_acknowledged_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ordering-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ordering-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "ordering.claimed.acknowledged",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("ordering-claimed-acknowledged")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(_) =
    postgres.submit(database, "ordering-claimed-acknowledged", definition, 7)

  let signal = process.new_subject()
  let assert Ok(claimed_id) = sinal.handler_id("grind-test-ordering-claimed")
  let assert Ok(claimed_attachment) =
    sinal.observe(
      claimed_id,
      observation.claimed(),
      fn(_measurements, metadata) {
        process.send(signal, ClaimedOrderingEvent(metadata.attempt.attempt_id))
      },
    )
  use <- exception.defer(fn() { detach(claimed_attachment) })
  let assert Ok(acknowledged_id) =
    sinal.handler_id("grind-test-ordering-acknowledged")
  let assert Ok(acknowledged_attachment) =
    sinal.observe(
      acknowledged_id,
      observation.acknowledged(),
      fn(_measurements, metadata) {
        process.send(
          signal,
          AcknowledgedOrderingEvent(metadata.attempt.attempt_id),
        )
      },
    )
  use <- exception.defer(fn() { detach(acknowledged_attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  let assert Ok(ClaimedOrderingEvent(claimed_attempt_id)) =
    process.receive(signal, within: 5000)
  let assert Ok(AcknowledgedOrderingEvent(acknowledged_attempt_id)) =
    process.receive(signal, within: 5000)
  claimed_attempt_id |> should.equal(acknowledged_attempt_id)
  mark_database_test_executed("claimed-precedes-acknowledged-ordering-passed")
}

// -- Decision A (2026-09-25): cross-version quarantine coverage -------------
//
// `docs/RELEASE-READINESS.md`, "Old-version executing rows". Before this
// decision, a consumer's per-poll quarantine scan (`claim_one`, via
// `quarantine_expired_in_queue`) only ever considered rows whose
// `(worker_id, worker_version)` the polling consumer itself registered, so
// after a worker-version bump an old version's still-`executing` row (its
// consumer long gone) was never quarantined by anything: the new consumer's
// scan skipped it (unregistered identity), and nothing else ever looked at
// it. The fix drops that identity filter from the per-queue scan entirely —
// quarantining never decodes or runs code, so there is no reason to gate it
// on registration — and adds a public, storage-owner-wide
// `postgres.quarantine_expired` for a queue no consumer polls at all.

/// (i) A job claimed under worker version `v1`, whose consumer is long gone,
/// still gets quarantined by a *different* consumer that now only registers
/// `v2` of the same worker id, once its lease has expired — proving the
/// per-queue scan no longer restricts itself to registered identities.
pub fn postgres_quarantine_covers_unregistered_worker_version_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_quarantine_covers_unregistered_worker_version_test(database_url)
  }
}

fn run_quarantine_covers_unregistered_worker_version_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_id = "old-version.echo-" <> suffix
  let worker_v1 = unique_test_worker_versioned(worker_id, "v1")
  let worker_v2 = unique_test_worker_versioned(worker_id, "v2")
  let test_queue = "old-version-quarantine-" <> suffix

  let assert Ok(handle) = postgres.submit(database, test_queue, worker_v1, 1)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'retired-v1-consumer', lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND worker_version = 'v1' AND queue = $2",
    )
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(test_queue))
    |> pog.execute(on: connection)

  // Only `v2` is ever registered from here on — the `v1` consumer that
  // claimed this row is gone for good, exactly as after a worker-version
  // bump. Nothing in this registry can ever claim the `v1` row, but this
  // consumer's own quarantine scan must still see it.
  let assert Ok(workers_v2) = registry.new(test_queue)
  let assert Ok(workers_v2) = registry.register(workers_v2, worker_v2)
  let assert Ok(consumer) = queue.start(database, workers_v2, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  mark_database_test_executed(
    "quarantine-covers-unregistered-worker-version-passed",
  )
}

/// (ii) The public, storage-owner-wide `quarantine_expired` sweeps an
/// expired `executing` row in a queue no consumer ever polls at all — the
/// per-queue scan above only ever runs as part of some consumer's own
/// `claim_one`, so a queue nothing polls needs this separate operation. Also
/// proves `limit` validation and bounding (a non-positive limit is rejected
/// before touching storage, and a limit smaller than the number of expired
/// rows quarantines only that many, idempotent to call again for the
/// remainder), that the sweep genuinely crosses queues (two different
/// unpolled queues, one row each, both eventually quarantined), and that
/// the `[grind, job, quarantined]` observation's own `queue` field names
/// each row's real queue — never a hardcoded or swapped one — since the
/// global sweep, unlike the per-queue scan, spans more than one queue in a
/// single call.
///
/// Runs against a database dedicated to this test alone
/// (`GRIND_TEST_QUARANTINE_URL`), not the shared `GRIND_TEST_DATABASE_URL`:
/// `quarantine_expired` sweeps every expired `executing` row for its whole
/// storage owner, and storage owner is derived from `host:port/database`
/// (`postgres.validate`), so sharing a database with dozens of other tests
/// would make this test's own row/observation counts depend on whatever
/// unrelated expired rows those other tests happen to leave behind at the
/// moment this one runs — a dedicated database is a dedicated storage
/// owner, immune to that ordering.
pub fn postgres_quarantine_expired_global_operation_test() {
  case quarantine_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_quarantine_expired_global_test(database_url)
  }
}

fn run_quarantine_expired_global_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  let worker_def = unique_test_worker("unpolled-queue.echo-" <> suffix)
  let queue_a = "unpolled-quarantine-a-" <> suffix
  let queue_b = "unpolled-quarantine-b-" <> suffix

  let assert Ok(first) = postgres.submit(database, queue_a, worker_def, 1)
  let assert Ok(second) = postgres.submit(database, queue_b, worker_def, 2)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'no-consumer-ever-polled', lease_expires_at = clock_timestamp() WHERE id = ANY($1::bigint[])",
    )
    |> pog.parameter(
      pog.array(pog.int, [job.id_value(first), job.id_value(second)]),
    )
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let assert Ok(handler_id) =
    sinal.handler_id("grind-test-quarantine-global-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(
      handler_id,
      observation.quarantined(),
      fn(measurements, metadata) {
        process.send(signal, #(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  postgres.quarantine_expired(database, limit: 0)
  |> should.equal(Error(postgres.NonPositiveLimit))
  postgres.quarantine_expired(database, limit: -1)
  |> should.equal(Error(postgres.NonPositiveLimit))
  postgres.state(database, first) |> should.equal(Ok(job.Executing))
  postgres.state(database, second) |> should.equal(Ok(job.Executing))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  postgres.quarantine_expired(database, limit: 1) |> should.equal(Ok(1))
  let assert Ok(#(measurements_one, metadata_one)) =
    process.receive(signal, within: 5000)
  measurements_one
  |> should.equal(observation.QuarantinedMeasurements(count: 1))
  let states_after_one = [
    postgres.state(database, first),
    postgres.state(database, second),
  ]
  list.count(states_after_one, fn(state) { state == Ok(job.Uncertain) })
  |> should.equal(1)
  list.count(states_after_one, fn(state) { state == Ok(job.Executing) })
  |> should.equal(1)
  // Whichever row the sweep picked first (by `id`, not scoped to either
  // queue), the observation's own `queue` field must name that exact row's
  // real queue.
  case metadata_one.ref.job_id == job.id_value(first) {
    True -> metadata_one.ref.queue |> should.equal(queue_a)
    False -> {
      metadata_one.ref.job_id |> should.equal(job.id_value(second))
      metadata_one.ref.queue |> should.equal(queue_b)
    }
  }

  postgres.quarantine_expired(database, limit: 10) |> should.equal(Ok(1))
  let assert Ok(#(_, metadata_two)) = process.receive(signal, within: 5000)
  postgres.state(database, first) |> should.equal(Ok(job.Uncertain))
  postgres.state(database, second) |> should.equal(Ok(job.Uncertain))
  case metadata_two.ref.job_id == job.id_value(first) {
    True -> metadata_two.ref.queue |> should.equal(queue_a)
    False -> {
      metadata_two.ref.job_id |> should.equal(job.id_value(second))
      metadata_two.ref.queue |> should.equal(queue_b)
    }
  }
  // The sweep genuinely crossed queues -- both distinct queues were the
  // subject of one event each, not both events reporting the same queue.
  list.contains([metadata_one.ref.queue, metadata_two.ref.queue], queue_a)
  |> should.equal(True)
  list.contains([metadata_one.ref.queue, metadata_two.ref.queue], queue_b)
  |> should.equal(True)

  // Nothing left to quarantine now.
  postgres.quarantine_expired(database, limit: 10) |> should.equal(Ok(0))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  mark_database_test_executed("quarantine-expired-global-operation-passed")
}

// -- Decision B (2026-09-25): retry-safe plain submit (`submit_with_id`) ----
//
// `docs/RELEASE-READINESS.md`, "Retry-safe plain submit", and
// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts". Plain `submit`/
// `submit_at` have no request identity, so a caller that retries after
// `SubmitQueryFailed` (which may itself have committed) risks a duplicate
// row. `submit_with_id` reuses `submit_unique`'s own admission receipt,
// request fingerprint, and reconciliation machinery through a "no policy"
// path in `grind/internal/unique_admission` — no uniqueness key, no
// candidate selection, no conflict decision, always `Inserted` once
// resolved.

/// `submit_with_id` under `Immediately`, the shape every test below needs.
fn submit_with_id_immediately(
  database: postgres.Database,
  queue: String,
  id_text: String,
  worker_def: worker.Worker(input, output, error),
  input: input,
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let assert Ok(submission) = submission.submission_id(id_text)
  postgres.submit_with_id(
    database,
    queue,
    submission,
    worker_def,
    input,
    submission.Immediately,
  )
}

pub fn postgres_submit_with_id_first_submit_inserted_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_with_id_first_submit_test(database_url)
  }
}

fn run_submit_with_id_first_submit_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_first_" <> suffix,
  )
  let worker_def = unique_test_worker("plain-first.echo-" <> suffix)
  let test_queue = "plain-first-" <> suffix
  let submission_text = "plain-first-" <> suffix

  let assert Ok(submission.Inserted(handle)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      7,
    )
  postgres.arguments(database, handle) |> should.equal(Ok(7))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  mark_database_test_executed("submit-with-id-first-submit-inserted-passed")
}

pub fn postgres_submit_with_id_same_request_retry_returns_original_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_with_id_retry_test(database_url)
  }
}

fn run_submit_with_id_retry_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_retry_" <> suffix,
  )
  let worker_def = unique_test_worker("plain-retry.echo-" <> suffix)
  let test_queue = "plain-retry-" <> suffix
  let submission_text = "plain-retry-" <> suffix

  let assert Ok(submission.Inserted(first_handle)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      11,
    )
  let assert Ok(submission.Inserted(retry_handle)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      11,
    )
  job.id_value(first_handle) |> should.equal(job.id_value(retry_handle))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  mark_database_test_executed("submit-with-id-retry-returns-original-passed")
}

pub fn postgres_submit_with_id_different_input_same_id_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_with_id_different_input_conflict_test(database_url)
  }
}

fn run_submit_with_id_different_input_conflict_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_conflict_" <> suffix,
  )
  let worker_def = unique_test_worker("plain-conflict.echo-" <> suffix)
  let test_queue = "plain-conflict-" <> suffix
  let submission_text = "plain-conflict-" <> suffix

  let assert Ok(submission.Inserted(_)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      1,
    )
  submit_with_id_immediately(
    database,
    test_queue,
    submission_text,
    worker_def,
    2,
  )
  |> should.equal(Error(submission.SubmissionConflict))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  mark_database_test_executed("submit-with-id-different-input-conflict-passed")
}

/// A genuinely committed plain admission whose reply is lost after
/// PostgreSQL has already committed locally (the same SyncRep-park-then-
/// terminate mechanism Increment 2/11 use), scoped on `grind_unique_submissions`
/// by `submission_id` — the same receipt table `submit_unique` uses, since
/// `submit_with_id` reuses it directly. `submit_with_id` itself still
/// returns `Ok(Inserted(handle))`, resolved by the shared `run`'s own
/// follow-up receipt lookup (the identical code `submit_unique` runs
/// through), not a `CommitUnknown` the caller must separately reconcile.
pub fn postgres_submit_with_id_committed_reply_lost_returns_inserted_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_with_id_committed_reply_lost_test(database_url)
  }
}

fn run_submit_with_id_committed_reply_lost_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  require_syncrep_cluster_configured(connection)

  let worker_def = unique_test_worker("plain-reply-lost-" <> suffix)
  let test_queue = "plain-reply-lost-" <> suffix
  let submission_text = "plain-reply-lost-" <> suffix

  use <- exception.defer(install_syncrep_reply_trigger(
    connection,
    "grind_test_plain_reply_lost_" <> suffix,
    "grind_unique_submissions",
    "NEW.submission_id = '" <> submission_text <> "'",
  ))

  let reply = process.new_subject()
  spawn_submit(reply, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      5,
    )
  })

  let assert Ok(backend_pid) = wait_for_syncrep_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)

  let assert Ok(Ok(submission.Inserted(handle))) =
    process.receive(reply, within: 10_000)
  backend_pid_is_alive(connection, backend_pid) |> should.equal(False)

  postgres.arguments(database, handle) |> should.equal(Ok(5))
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)
  unique_receipt_exists(
    connection,
    postgres.storage_owner(database),
    submission_text,
  )
  |> should.equal(True)

  mark_database_test_executed(
    "submit-with-id-committed-reply-lost-inserted-passed",
  )
}

/// The exact query text `insert_job` (`grind/internal/unique_admission`)
/// issues for a `policy: None` request, as a `LIKE` prefix for
/// `await_overlap_shape`/`pg_stat_activity` — the "no policy" counterpart to
/// `unique_insert_query_like` above (that one has the trailing
/// `unique_key_contract, unique_key_sha256` columns this one omits).
const plain_insert_query_like = "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, error_version, max_attempts, state, available_at, inserted_at) VALUES%"

/// Two concurrent `submit_with_id` callers, same `SubmissionId` and
/// identical request, forced to actually overlap: a `BEFORE INSERT` barrier
/// trigger (`install_unique_insert_barrier`, the same mechanism the
/// uniqueness-admission overlap tests use) blocks both callers' own
/// `grind_jobs` insert behind one held advisory lock until both are
/// genuinely waiting, proven by `pg_stat_activity` rather than timing. There
/// is no domain-wide advisory lock on this "no policy" path (see
/// `admission_transaction`'s doc comment in
/// `grind/internal/unique_admission.gleam`), so releasing the barrier lets
/// whichever caller happens to proceed first commit its row and receipt;
/// the other's own `record_receipt` then hits a real `23505` against that
/// just-committed receipt, which the shared `run` resolves by re-reading
/// the exact same receipt rather than surfacing a bare conflict for what
/// is, from that caller's perspective, an ordinary successful retry. Both
/// callers converge on `Inserted` with the same job id, and exactly one row
/// is ever persisted.
pub fn postgres_submit_with_id_concurrent_same_id_one_row_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_submit_with_id_concurrent_test(database_url)
  }
}

fn run_submit_with_id_concurrent_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_concurrent_" <> suffix,
  )
  let worker_id = "plain-concurrent.echo-" <> suffix
  let worker_def = unique_test_worker_versioned(worker_id, "v1")
  let test_queue = "plain-concurrent-" <> suffix
  let submission_text = "plain-concurrent-" <> suffix

  let lock_key = unique_test_lock_key(0)
  let cleanup_trigger =
    install_unique_insert_barrier(
      connection,
      "grind_test_plain_concurrent_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_plain_concurrent_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      42,
    )
  })
  spawn_submit(result_b, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      42,
    )
  })

  await_overlap_shape(
    connection,
    plain_insert_query_like,
    plain_insert_query_like,
    2,
    2,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 10_000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 10_000)

  case outcome_a, outcome_b {
    Ok(submission.Inserted(handle_a)), Ok(submission.Inserted(handle_b)) ->
      job.id_value(handle_a) |> should.equal(job.id_value(handle_b))
    _, _ -> should.fail()
  }
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  mark_database_test_executed("submit-with-id-concurrent-one-row-passed")
}

/// The same barrier-forced overlap as
/// `postgres_submit_with_id_concurrent_same_id_one_row_test` above, but the
/// two concurrent callers submit *different* inputs under the identical
/// `SubmissionId`. Whichever the barrier releases first commits and is
/// `Inserted`; the other's own `record_receipt` still hits the same real
/// `23505` against that just-committed receipt, but this time
/// `reconcile_from_receipt`'s fingerprint check does not match (different
/// `encoded_input`, hence a different `request_sha256`) — it stays
/// `SubmissionConflict` rather than converging, exactly like a sequential
/// different-input-same-id retry, and exactly one row is ever persisted.
pub fn postgres_submit_with_id_concurrent_different_input_conflict_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_with_id_concurrent_different_input_test(database_url)
  }
}

fn run_submit_with_id_concurrent_different_input_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use database, connection <- with_unique_database(
    database_url,
    "grind_submit_with_id_concurrent_diff_" <> suffix,
  )
  let worker_id = "plain-concurrent-diff.echo-" <> suffix
  let worker_def = unique_test_worker_versioned(worker_id, "v1")
  let test_queue = "plain-concurrent-diff-" <> suffix
  let submission_text = "plain-concurrent-diff-" <> suffix

  let lock_key = unique_test_lock_key(1)
  let cleanup_trigger =
    install_unique_insert_barrier(
      connection,
      "grind_test_plain_concurrent_diff_" <> suffix,
      worker_id,
      lock_key,
    )
  use <- exception.defer(cleanup_trigger)

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_plain_concurrent_diff_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      42,
    )
  })
  spawn_submit(result_b, fn() {
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      99,
    )
  })

  await_overlap_shape(
    connection,
    plain_insert_query_like,
    plain_insert_query_like,
    2,
    2,
    500,
  )
  |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 10_000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 10_000)

  let inserted =
    list.filter_map([outcome_a, outcome_b], fn(outcome) {
      case outcome {
        Ok(submission.Inserted(handle)) -> Ok(handle)
        _ -> Error(Nil)
      }
    })
  let conflicts =
    list.filter([outcome_a, outcome_b], fn(outcome) {
      outcome == Error(submission.SubmissionConflict)
    })
  list.length(inserted) |> should.equal(1)
  list.length(conflicts) |> should.equal(1)
  count_jobs_in_queue(connection, test_queue) |> should.equal(1)

  mark_database_test_executed(
    "submit-with-id-concurrent-different-input-conflict-passed",
  )
}

/// `submit_with_id`'s own `[grind, job, admitted]` observation: `Some`
/// `submission_id`, `Replied` on the first genuine commit, `Reconciled` on a
/// same-id/same-request replay resolved from the receipt without a fresh
/// write — the same contract `submit_unique`'s admitted observation already
/// has (`postgres_admitted_observation_unique_inserted_and_reconciled_test`),
/// proven here for the "no policy" path.
pub fn postgres_admitted_observation_submit_with_id_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_admitted_observation_submit_with_id_test(database_url)
  }
}

fn run_admitted_observation_submit_with_id_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_admitted_submit_with_id_" <> suffix,
  )
  let worker_def = unique_test_worker("admitted.submit-with-id-" <> suffix)
  let test_queue = "admitted-submit-with-id-" <> suffix
  let submission_text = "admitted-submit-with-id-" <> suffix

  let signal = process.new_subject()
  let assert Ok(id) =
    sinal.handler_id("grind-test-admitted-submit-with-id-" <> suffix)
  let assert Ok(attachment) =
    sinal.observe(id, observation.admitted(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(submission.Inserted(handle)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
    )
  let assert Ok(#(_, first_metadata)) = process.receive(signal, within: 5000)
  first_metadata.ref.job_id |> should.equal(job.id_value(handle))
  first_metadata.submission_id |> should.equal(Some(submission_text))
  first_metadata.confirmation |> should.equal(observation.Replied)
  first_metadata.committed_state |> should.equal(job.Queued)
  let assert Some(_) = first_metadata.available_at_unix_ms

  let assert Ok(submission.Inserted(replayed)) =
    submit_with_id_immediately(
      database,
      test_queue,
      submission_text,
      worker_def,
      3,
    )
  job.id_value(replayed) |> should.equal(job.id_value(handle))
  let assert Ok(#(_, second_metadata)) = process.receive(signal, within: 5000)
  second_metadata.ref.job_id |> should.equal(job.id_value(handle))
  second_metadata.submission_id |> should.equal(Some(submission_text))
  second_metadata.confirmation |> should.equal(observation.Reconciled)
  second_metadata.available_at_unix_ms |> should.equal(None)

  mark_database_test_executed("admitted-observation-submit-with-id-passed")
}
