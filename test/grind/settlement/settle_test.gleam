//// One settlement try at a time, driven by `settlement.settle_next`
//// (docs/design/design.typ, "Declared settlement of abandoned jobs").
//// Leases expire by moving `lease_expires_at` to the database clock, and
//// the abandoned node is an attempt claimed without running its handler.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{Some}
import gleam/string
import gleeunit/should
import grind/internal/attempt
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/settlement
import grind/internal/worker as definition
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import grind/worker
import pog

const lease_ms = 30_000

pub fn postgres_declared_failure_commits_with_the_application_writes_test() {
  use database <- with_database()
  let name = queue_name("settle-fail")
  let notices = notices_table(database, name)
  let seen = process.new_subject()
  let agent =
    agent_worker(name, fn(transaction, abandoned) {
      process.send(seen, abandoned)
      insert_notice(transaction, notices, abandoned.job_id)
      Ok(worker.ConfirmFailure("left uncertain: settled as a failed run"))
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let #(_, abandoned_attempt) = abandon(database, name, agent)

  let assert Ok(settlement.Settled(job_id, job.BusinessFailed)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  job_id |> should.equal(job.id_value(handle))
  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  count_notices(database, notices, job_id) |> should.equal(1)
  let assert Ok(abandoned) = process.receive(seen, within: 0)
  abandoned.input |> should.equal("read inbox")
  abandoned.attempt_id |> should.equal(abandoned_attempt)
  abandoned.settlement_try |> should.equal(1)
  abandoned.cancellation_requested |> should.be_false
  resolution_ids(database, job_id)
  |> should.equal([
    "grind-settlement:"
    <> int.to_string(job_id)
    <> ":"
    <> int.to_string(abandoned_attempt),
  ])
  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.NothingDue))
  mark_database_test_executed("settlement-commits-with-application-writes")
}

pub fn postgres_failed_try_commits_nothing_and_is_due_later_test() {
  use database <- with_database()
  let name = queue_name("settle-error")
  let notices = notices_table(database, name)
  let agent =
    agent_worker(name, fn(transaction, abandoned) {
      insert_notice(transaction, notices, abandoned.job_id)
      Error("the agent provider is unreachable")
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)

  let assert Ok(settlement.TryFailed(job_id, settlement.FunctionFailed(reason))) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  reason |> should.equal("the agent provider is unreachable")
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  count_notices(database, notices, job_id) |> should.equal(0)
  // The take moved the due time one lease later, so the job is not retaken.
  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.NothingDue))
  mark_tries(database, job_id) |> should.equal(1)
  mark_database_test_executed("settlement-failed-try-due-later")
}

pub fn postgres_retried_try_sees_its_count_and_can_decline_test() {
  use database <- with_database()
  let name = queue_name("settle-decline")
  let notices = notices_table(database, name)
  let agent =
    agent_worker(name, fn(transaction, abandoned) {
      case abandoned.settlement_try {
        1 -> Error("not yet")
        _ -> {
          insert_notice(transaction, notices, abandoned.job_id)
          Ok(worker.LeaveUncertain("an operator must check the provider"))
        }
      }
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)
  let assert Ok(settlement.TryFailed(job_id, _)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)
  make_due_now(database, job_id)

  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.Declined(job_id)))

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  count_notices(database, notices, job_id) |> should.equal(1)
  let assert Ok(job.ReconciliationRequired(description)) =
    postgres.outcome(database, handle)
  description
  |> should.equal(
    "expired attempt requires outcome reconciliation; an operator must check the provider",
  )
  make_due_now(database, job_id)
  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.NothingDue))
  mark_database_test_executed("settlement-declined-keeps-job")
}

pub fn postgres_crashing_function_is_a_failed_try_test() {
  use database <- with_database()
  let name = queue_name("settle-crash")
  let agent =
    agent_worker(name, fn(_transaction, _abandoned) { panic as "boom" })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)

  let assert Ok(settlement.TryFailed(_, settlement.FunctionCrashed(_))) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_database_test_executed("settlement-crash-failed-try")
}

pub fn postgres_function_that_ends_its_transaction_is_a_failed_try_test() {
  use database <- with_database()
  let name = queue_name("settle-commit")
  let agent =
    agent_worker(name, fn(transaction, _abandoned) {
      let assert Ok(_) = pog.query("COMMIT") |> pog.execute(on: transaction)
      let assert Ok(_) = pog.query("BEGIN") |> pog.execute(on: transaction)
      Ok(worker.ConfirmFailure("never applied"))
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)

  let assert Ok(settlement.TryFailed(_, settlement.TransactionEnded)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_database_test_executed("settlement-transaction-ended-failed-try")
}

pub fn postgres_refused_decision_is_a_failed_try_test() {
  use database <- with_database()
  let name = queue_name("settle-refused")
  // No error codec: the resolution protocol refuses ConfirmFailure.
  let agent =
    worker.new(
      "settlement.agent",
      input: string_codec(),
      output: string_codec(),
      perform: fn(_) { Ok("done") },
    )
    |> worker.with_queue(name)
    |> worker.with_settlement(fn(_transaction, _abandoned) {
      Ok(worker.ConfirmFailure("no codec"))
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)

  let assert Ok(settlement.TryFailed(job_id, settlement.Refused(_))) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_tries(database, job_id) |> should.equal(1)
  mark_database_test_executed("settlement-refused-failed-try")
}

pub fn postgres_authorized_replay_requeues_the_job_test() {
  use database <- with_database()
  let name = queue_name("settle-replay")
  let agent =
    agent_worker(name, fn(_transaction, _abandoned) {
      Ok(worker.AuthorizeReplay)
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)

  let assert Ok(settlement.Settled(_, job.Queued)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  mark_database_test_executed("settlement-authorized-replay")
}

pub fn postgres_late_acknowledgement_cannot_contradict_the_settlement_test() {
  use database <- with_database()
  let name = queue_name("settle-late-ack")
  let agent =
    agent_worker(name, fn(_transaction, _abandoned) {
      Ok(worker.ConfirmFailure("settled"))
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let #(claimed, _) = abandon(database, name, agent)
  let assert Ok(settlement.Settled(_, job.BusinessFailed)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  // The abandoned attempt's process returns and acknowledges success.
  let _ =
    attempt.acknowledge(
      database,
      name,
      "gone-node",
      claimed,
      definition.ExecutedSuccess("1", "\"late\""),
    )

  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  mark_database_test_executed("settlement-late-ack-refused")
}

pub fn postgres_operator_resolution_first_leaves_nothing_due_test() {
  use database <- with_database()
  let name = queue_name("settle-operator")
  let calls = process.new_subject()
  let agent =
    agent_worker(name, fn(_transaction, _abandoned) {
      process.send(calls, Nil)
      Ok(worker.ConfirmFailure("settled"))
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)
  let assert Ok(_) =
    postgres.resolve_uncertain(
      database,
      handle,
      postgres.ResolutionRequest(
        "operator-first-" <> name,
        "operator",
        "checked by hand",
        postgres.ConfirmSuccess("done"),
      ),
    )

  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.NothingDue))
  process.receive(calls, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("settlement-operator-first")
}

pub fn postgres_worker_without_settlement_is_never_taken_test() {
  use database <- with_database()
  let name = queue_name("settle-none")
  let plain =
    worker.new(
      "settlement.agent",
      input: string_codec(),
      output: string_codec(),
      perform: fn(_) { Ok("done") },
    )
    |> worker.with_queue(name)
  let assert Ok(handle) = postgres.submit(database, name, plain, "read inbox")
  let _ = abandon(database, name, plain)

  settlement.settle_next(database, registry_of(name, plain), lease_ms)
  |> should.equal(Ok(settlement.NothingDue))

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_tries(database, job.id_value(handle)) |> should.equal(0)
  mark_database_test_executed("settlement-none-declared")
}

pub fn postgres_function_writes_under_the_application_search_path_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      // A pool shared with the application: Grind's statements run under
      // its own schema, the application's under the session's search_path.
      let schema = "settle_shared_" <> int.to_string(unique_test_run_id())
      let assert Ok(config) =
        pog.url_config(process.new_name("grind_settlement_shared"), url)
      let assert Ok(validated) =
        postgres.settings_from_config(config)
        |> postgres.with_schema(schema)
        |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let assert Ok(Nil) = postgres.migrate(database)
      let name = queue_name("settle-shared")
      let table = "notices_" <> string.replace(name, "-", "_")
      let assert Ok(_) =
        pog.query(
          "CREATE TABLE public." <> table <> " (job_id bigint NOT NULL)",
        )
        |> pog.execute(on: postgres.connection(database))
      let agent =
        agent_worker(name, fn(transaction, abandoned) {
          // Unqualified: resolves through the application's search_path.
          case
            pog.query("INSERT INTO " <> table <> " (job_id) VALUES ($1)")
            |> pog.parameter(pog.int(abandoned.job_id))
            |> pog.execute(on: transaction)
          {
            Ok(_) -> Ok(worker.ConfirmFailure("settled"))
            Error(error) -> Error(string.inspect(error))
          }
        })
      let assert Ok(handle) =
        postgres.submit(database, name, agent, "read inbox")
      let _ =
        abandon_in(database, "\"" <> schema <> "\".grind_jobs", name, agent)

      let assert Ok(settlement.Settled(job_id, job.BusinessFailed)) =
        settlement.settle_next(database, registry_of(name, agent), lease_ms)

      job_id |> should.equal(job.id_value(handle))
      count_notices(database, "public." <> table, job_id) |> should.equal(1)
      mark_database_test_executed("settlement-application-search-path")
    }
  }
}

pub fn postgres_hanging_function_is_ended_at_the_deadline_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(validated) =
        postgres.settings(url)
        |> postgres.with_statement_deadline(1500)
        |> postgres.with_unique_lock_wait(100)
        |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let assert Ok(Nil) = postgres.migrate(database)
      let name = queue_name("settle-hang")
      let agent =
        agent_worker(name, fn(_transaction, _abandoned) {
          process.sleep(60_000)
          Ok(worker.ConfirmFailure("never"))
        })
      let assert Ok(handle) =
        postgres.submit(database, name, agent, "read inbox")
      let _ = abandon(database, name, agent)

      let assert Ok(settlement.TryFailed(_, settlement.FunctionTimedOut)) =
        settlement.settle_next(database, registry_of(name, agent), lease_ms)

      postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
      // The settler outlived the try and the job's row is free again.
      let assert Ok(_) =
        postgres.resolve_uncertain(
          database,
          handle,
          postgres.ResolutionRequest(
            "after-hang-" <> name,
            "operator",
            "checked by hand",
            postgres.ConfirmSuccess("done"),
          ),
        )
      mark_database_test_executed("settlement-hanging-function-ended")
    }
  }
}

pub fn postgres_settler_start_marks_jobs_left_unmarked_test() {
  use database <- with_database()
  let name = queue_name("settle-recognise")
  let agent =
    agent_worker(name, fn(_transaction, _abandoned) {
      Ok(worker.LeaveUncertain("operator"))
    })
  let assert Ok(unmarked) = postgres.submit(database, name, agent, "a")
  let assert Ok(declined) = postgres.submit(database, name, agent, "b")
  let _ = abandon(database, name, agent)
  let _ = abandon(database, name, agent)
  let assert Ok(settlement.Declined(declined_id)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)
  let unmarked_id = case declined_id == job.id_value(declined) {
    True -> job.id_value(unmarked)
    False -> job.id_value(declined)
  }
  // A node of the previous release held this one without a mark.
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET settle_attempt_id = NULL, settle_at = NULL WHERE id = $1",
    )
    |> pog.parameter(pog.int(unmarked_id))
    |> pog.execute(on: postgres.connection(database))
  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.NothingDue))

  settlement.recognise_unmarked(database, registry_of(name, agent))
  |> should.equal(Ok(1))

  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.Declined(unmarked_id)))
  settlement.recognise_unmarked(database, registry_of(name, agent))
  |> should.equal(Ok(0))
  mark_database_test_executed("settlement-recognises-unmarked")
}

// -- Support -----------------------------------------------------------------

fn with_database(run: fn(postgres.Database) -> Nil) -> Nil {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(validated) = postgres.settings(url) |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let assert Ok(Nil) = postgres.migrate(database)
      run(database)
    }
  }
}

fn queue_name(prefix: String) -> String {
  prefix <> "-" <> int.to_string(unique_test_run_id())
}

fn string_codec() -> worker.Codec(String) {
  worker.codec(worker.infallible(json.string), decode.string)
}

/// The dash-crm shape: an agent run whose settlement may write the
/// application's own rows and resolve the run.
fn agent_worker(
  name: String,
  settle: fn(pog.Connection, worker.Abandoned(String)) ->
    Result(worker.Settlement(String, String), String),
) -> worker.Worker(String, String, String) {
  worker.new(
    "settlement.agent",
    input: string_codec(),
    output: string_codec(),
    perform: fn(_) { Ok("done") },
  )
  |> worker.with_queue(name)
  |> worker.with_error_codec(string_codec())
  |> worker.with_abandonment(worker.HoldUncertain)
  |> worker.with_settlement(settle)
}

fn registry_of(
  name: String,
  definition: worker.Worker(i, o, e),
) -> registry.Registry {
  let assert Ok(workers) = registry.new(name)
  let assert Ok(workers) = registry.register(workers, definition)
  workers
}

/// Claims the job as a node that then died would, expires its lease and
/// lets the cross-queue scan hold it uncertain.
fn abandon(
  database: postgres.Database,
  name: String,
  definition: worker.Worker(i, o, e),
) -> #(attempt.ClaimedJob, Int) {
  abandon_in(database, "grind_jobs", name, definition)
}

/// `abandon` for a schema the test's own pool does not resolve by default:
/// `jobs` names Grind's job table.
fn abandon_in(
  database: postgres.Database,
  jobs: String,
  name: String,
  definition: worker.Worker(i, o, e),
) -> #(attempt.ClaimedJob, Int) {
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      name,
      registry_of(name, definition),
      "gone-node",
      lease_ms,
    )
  let #(id, attempt_id, _epoch) = attempt.claim_identity(claimed)
  let assert Ok(_) =
    pog.query(
      "UPDATE "
      <> jobs
      <> " SET lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: postgres.connection(database))
  let assert Ok(_) = postgres.quarantine_expired(database, limit: 10_000)
  #(claimed, attempt_id)
}

fn notices_table(database: postgres.Database, name: String) -> String {
  let table = "public.\"notices_" <> name <> "\""
  let assert Ok(_) =
    pog.query("CREATE TABLE " <> table <> " (job_id bigint NOT NULL)")
    |> pog.execute(on: postgres.connection(database))
  table
}

fn insert_notice(
  transaction: pog.Connection,
  table: String,
  job_id: Int,
) -> Nil {
  let assert Ok(_) =
    pog.query("INSERT INTO " <> table <> " (job_id) VALUES ($1)")
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: transaction)
  Nil
}

fn count_notices(
  database: postgres.Database,
  table: String,
  job_id: Int,
) -> Int {
  let assert Ok(returned) =
    pog.query("SELECT count(*)::bigint FROM " <> table <> " WHERE job_id = $1")
    |> pog.parameter(pog.int(job_id))
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(on: postgres.connection(database))
  let assert [count] = returned.rows
  count
}

fn mark_tries(database: postgres.Database, job_id: Int) -> Int {
  let assert Ok(returned) =
    pog.query("SELECT settle_tries FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(job_id))
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(on: postgres.connection(database))
  let assert [tries] = returned.rows
  tries
}

fn make_due_now(database: postgres.Database, job_id: Int) -> Nil {
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET settle_at = clock_timestamp() WHERE id = $1 AND settle_at IS NOT NULL",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: postgres.connection(database))
  Nil
}

fn resolution_ids(database: postgres.Database, job_id: Int) -> List(String) {
  let assert Ok(returned) =
    pog.query(
      "SELECT resolution_id FROM grind_job_resolutions WHERE job_id = $1 ORDER BY resolution_id",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.returning(decode.field(0, decode.string, decode.success))
    |> pog.execute(on: postgres.connection(database))
  returned.rows
}
