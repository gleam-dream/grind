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
import grind/abandoned as abandonment
import grind/internal/attempt
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/settlement
import grind/internal/worker as definition
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import grind/support/syncrep
import grind/testing
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
      insert_notice(transaction, notices, abandonment.job_id(abandoned))
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
  abandonment.input(abandoned) |> should.equal("read inbox")
  abandonment.attempt_id(abandoned) |> should.equal(abandoned_attempt)
  abandonment.try_number(abandoned) |> should.equal(1)
  abandonment.cancellation_requested(abandoned) |> should.be_false
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
      insert_notice(transaction, notices, abandonment.job_id(abandoned))
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
      case abandonment.try_number(abandoned) {
        1 -> Error("not yet")
        _ -> {
          insert_notice(transaction, notices, abandonment.job_id(abandoned))
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
  // The output codec rejects the confirmed value, so the resolution
  // protocol refuses it before any write.
  let picky =
    worker.codec(
      fn(value) {
        case value {
          "unstorable" -> Error("not a value this worker stores")
          _ -> Ok(json.string(value))
        }
      },
      decode.string,
    )
  let agent =
    worker.new(
      "settlement.agent",
      input: string_codec(),
      output: picky,
      perform: fn(_) { Ok("done") },
    )
    |> worker.with_queue(name)
    |> worker.with_settlement(fn(_transaction, _abandoned) {
      Ok(worker.ConfirmSuccess("unstorable"))
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)

  let assert Ok(settlement.TryFailed(job_id, settlement.Refused(_))) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_tries(database, job_id) |> should.equal(1)
  mark_database_test_executed("settlement-refused-failed-try")
}

pub fn postgres_failure_without_an_error_codec_is_unrecorded_test() {
  use database <- with_database()
  let name = queue_name("settle-no-codec")
  let agent =
    worker.new(
      "settlement.agent",
      input: string_codec(),
      output: string_codec(),
      perform: fn(_) { Ok("done") },
    )
    |> worker.with_queue(name)
    |> worker.with_settlement(fn(_transaction, _abandoned) {
      Ok(worker.ConfirmFailure(Nil))
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)

  let assert Ok(settlement.Settled(_, job.BusinessFailed)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  // As a handler's `Failed` on a worker without an error codec commits.
  let assert Ok(job.FailedOperationally(_)) = postgres.outcome(database, handle)
  mark_database_test_executed("settlement-failure-without-codec")
}

pub fn postgres_replay_under_pending_cancellation_is_declined_test() {
  use database <- with_database()
  let name = queue_name("settle-cancelled")
  let notices = notices_table(database, name)
  let agent =
    agent_worker(name, fn(transaction, abandoned) {
      insert_notice(transaction, notices, abandonment.job_id(abandoned))
      Ok(worker.AuthorizeReplay)
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let #(claimed, _) = claim_expired(database, name, agent)
  let #(job_id, _, _) = attempt.claim_identity(claimed)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET cancel_requested_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: postgres.connection(database))
  let assert Ok(_) = postgres.quarantine_expired(database, limit: 10_000)

  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.Declined(job_id)))

  // The replay can never commit: the mark ends, the job waits for an
  // operator, and the function's writes rolled back with the refusal.
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  count_notices(database, notices, job_id) |> should.equal(0)
  make_due_now(database, job_id)
  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.NothingDue))
  mark_database_test_executed("settlement-replay-under-cancellation-declined")
}

pub fn postgres_function_sees_why_the_attempt_was_abandoned_test() {
  use database <- with_database()
  let name = queue_name("settle-cause")
  let causes = process.new_subject()
  let agent =
    agent_worker(name, fn(_transaction, abandoned) {
      process.send(causes, abandonment.cause(abandoned))
      Ok(worker.ConfirmFailure("settled"))
    })
  let assert Ok(_) = postgres.submit(database, name, agent, "lease")
  let _ = abandon(database, name, agent)
  let assert Ok(settlement.Settled(_, _)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)
  process.receive(causes, within: 0)
  |> should.equal(Ok(abandonment.LeaseExpired))

  // A timeout under HoldUncertain is acknowledged as an abandonment.
  let assert Ok(_) = postgres.submit(database, name, agent, "timeout")
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      name,
      registry_of(name, agent),
      "gone-node",
      lease_ms,
    )
  let assert Ok(_) =
    attempt.acknowledge(
      database,
      name,
      "gone-node",
      claimed,
      definition.ExecutedAbandoned(
        definition.StoppedAtTimeout,
        "handler exceeded its timeout",
      ),
    )
  let assert Ok(settlement.Settled(_, _)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)
  process.receive(causes, within: 0) |> should.equal(Ok(abandonment.TimedOut))
  mark_database_test_executed("settlement-cause")
}

pub fn postgres_replayed_job_abandoned_again_is_settled_again_test() {
  use database <- with_database()
  let name = queue_name("settle-again")
  let seen = process.new_subject()
  let agent =
    agent_worker(name, fn(_transaction, abandoned) {
      process.send(seen, #(
        abandonment.attempt_id(abandoned),
        abandonment.authorized_replays(abandoned),
        abandonment.try_number(abandoned),
      ))
      case abandonment.authorized_replays(abandoned) {
        0 -> Ok(worker.AuthorizeReplay)
        _ -> Ok(worker.ConfirmFailure("replayed once already"))
      }
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let #(_, first) = abandon(database, name, agent)
  let assert Ok(settlement.Settled(_, job.Queued)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  let #(_, second) = abandon(database, name, agent)
  let assert Ok(settlement.Settled(_, job.BusinessFailed)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  process.receive(seen, within: 0) |> should.equal(Ok(#(first, 0, 1)))
  process.receive(seen, within: 0) |> should.equal(Ok(#(second, 1, 1)))
  { second != first } |> should.be_true
  postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
  mark_database_test_executed("settlement-second-abandonment")
}

pub fn postgres_concurrent_settlers_commit_one_decision_test() {
  use database <- with_database()
  let name = queue_name("settle-concurrent")
  let notices = notices_table(database, name)
  let entered = process.new_subject()
  let agent =
    agent_worker(name, fn(transaction, abandoned) {
      insert_notice(transaction, notices, abandonment.job_id(abandoned))
      let release = process.new_subject()
      process.send(entered, release)
      let _ = process.receive(release, within: 5000)
      Ok(worker.ConfirmFailure("settled"))
    })
  let assert Ok(_) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)
  let first = process.new_subject()
  process.spawn(fn() {
    process.send(
      first,
      settlement.settle_next(database, registry_of(name, agent), lease_ms),
    )
  })
  let assert Ok(release) = process.receive(entered, within: 5000)

  // A second settler, while the first holds the job inside its function,
  // finds nothing due: the first take moved the due time, and the take
  // skips a row another try has locked.
  settlement.settle_next(database, registry_of(name, agent), lease_ms)
  |> should.equal(Ok(settlement.NothingDue))

  process.send(release, Nil)
  let assert Ok(Ok(settlement.Settled(job_id, job.BusinessFailed))) =
    process.receive(first, within: 5000)
  count_notices(database, notices, job_id) |> should.equal(1)
  mark_database_test_executed("settlement-concurrent-settlers")
}

pub fn testing_abandoned_builds_what_a_settlement_receives_test() {
  let held =
    testing.abandoned(job_id: 7, input: "read inbox", attempt_id: 70)
    |> testing.with_cause(abandonment.TimedOut)
    |> testing.with_try_number(3)
    |> testing.with_authorized_replays(1)
    |> testing.with_cancellation_requested(True)
  abandonment.job_id(held) |> should.equal(7)
  abandonment.input(held) |> should.equal("read inbox")
  abandonment.attempt_id(held) |> should.equal(70)
  abandonment.cause(held) |> should.equal(abandonment.TimedOut)
  abandonment.try_number(held) |> should.equal(3)
  abandonment.authorized_replays(held) |> should.equal(1)
  abandonment.cancellation_requested(held) |> should.be_true
  abandonment.replays(held) |> should.equal(0)
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
  let assert Error(_) =
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
            |> pog.parameter(pog.int(abandonment.job_id(abandoned)))
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
      // The killed try holds no lock on the job's row.
      let assert Ok(_) =
        pog.transaction(postgres.connection(database), fn(transaction) {
          pog.query("SELECT id FROM grind_jobs WHERE id = $1 FOR UPDATE NOWAIT")
          |> pog.parameter(pog.int(job.id_value(handle)))
          |> pog.execute(on: transaction)
        })
      // The settler outlived the try.
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

pub fn postgres_commit_cut_at_the_deadline_is_unknown_test() {
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
      let name = queue_name("settle-commit-cut")
      let agent =
        agent_worker(name, fn(transaction, _abandoned) {
          // This transaction's COMMIT waits for a standby that never
          // exists, so its reply never arrives before the deadline.
          let assert Ok(_) =
            pog.query("SET LOCAL synchronous_commit = on")
            |> pog.execute(on: transaction)
          Ok(worker.ConfirmFailure("settled"))
        })
      let assert Ok(handle) =
        postgres.submit(database, name, agent, "read inbox")
      let _ = abandon(database, name, agent)

      let assert Ok(settlement.CommitUnknown(job_id)) =
        settlement.settle_next(database, registry_of(name, agent), lease_ms)

      job_id |> should.equal(job.id_value(handle))
      let connection = postgres.connection(database)
      let assert Ok(parked) =
        syncrep.wait_for_syncrep_trigger_backend(connection, 200)
      let _ = syncrep.terminate_backend(connection, parked)
      let assert Ok(Nil) =
        syncrep.wait_for_backend_gone(connection, parked, 500)
      // The commit was written locally before it waited: the decision
      // stands, and the next take finds nothing due.
      postgres.state(database, handle) |> should.equal(Ok(job.BusinessFailed))
      settlement.settle_next(database, registry_of(name, agent), lease_ms)
      |> should.equal(Ok(settlement.NothingDue))
      mark_database_test_executed("settlement-commit-cut-unknown")
    }
  }
}

pub fn postgres_function_that_swallows_a_failed_statement_failed_test() {
  use database <- with_database()
  let name = queue_name("settle-aborted")
  let agent =
    agent_worker(name, fn(transaction, _abandoned) {
      let _ =
        pog.query("SELECT no_such_column FROM pg_class")
        |> pog.execute(on: transaction)
      Ok(worker.ConfirmFailure("never applied"))
    })
  let assert Ok(handle) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)

  let assert Ok(settlement.TryFailed(_, settlement.FunctionFailed(_))) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  mark_database_test_executed("settlement-aborted-transaction-function-failed")
}

pub fn postgres_a_job_taken_in_this_round_is_not_taken_again_test() {
  use database <- with_database()
  let name = queue_name("settle-once")
  let agent =
    agent_worker(name, fn(_transaction, _abandoned) { Error("not yet") })
  let assert Ok(_) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)
  // A lease of zero makes the failed job due again at once, as a long round
  // would after a lease.
  let assert Ok(settlement.TryFailed(job_id, _)) =
    settlement.settle_next(database, registry_of(name, agent), 0)

  settlement.settle_next_except(database, registry_of(name, agent), 0, [
    job_id,
  ])
  |> should.equal(Ok(settlement.NothingDue))
  let assert Ok(settlement.TryFailed(again, _)) =
    settlement.settle_next(database, registry_of(name, agent), 0)
  again |> should.equal(job_id)
  mark_database_test_executed("settlement-once-per-round")
}

pub fn postgres_settled_try_leaves_no_message_behind_test() {
  use database <- with_database()
  let name = queue_name("settle-mailbox")
  let agent =
    agent_worker(name, fn(_transaction, _abandoned) {
      Ok(worker.ConfirmFailure("settled"))
    })
  let assert Ok(_) = postgres.submit(database, name, agent, "read inbox")
  let _ = abandon(database, name, agent)
  let before = message_queue_len()

  let assert Ok(settlement.Settled(_, _)) =
    settlement.settle_next(database, registry_of(name, agent), lease_ms)

  // A long-lived settler must not collect one stale message per settlement.
  message_queue_len() |> should.equal(before)
  mark_database_test_executed("settlement-no-stale-message")
}

@external(erlang, "grind_test_env", "message_queue_len")
fn message_queue_len() -> Int

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
  settle: fn(pog.Connection, abandonment.Abandoned(String)) ->
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

/// Claims the job as a dead node would and expires its lease, without the
/// scan that holds it.
fn claim_expired(
  database: postgres.Database,
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
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: postgres.connection(database))
  #(claimed, attempt_id)
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
