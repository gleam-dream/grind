//// Schema version 14 adds the settlement mark; a settler's start marks the
//// jobs a lease-expiry scan already held uncertain (docs/design/design.typ,
//// "Declared settlement of abandoned jobs"; ADR-0021).

import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleeunit/should
import grind/internal/job
import grind/internal/migrations
import grind/internal/postgres
import grind/internal/registry
import grind/internal/settlement
import grind/internal/worker
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import pog

pub fn postgres_v14_marks_jobs_held_uncertain_by_lease_expiry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_backfill(url)
  }
}

fn run_backfill(url: String) -> Nil {
  let schema = "settlement_v14_" <> int.to_string(unique_test_run_id())
  let assert Ok(validated) =
    postgres.settings(url) |> postgres.with_schema(schema) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) =
    postgres.migrate_with(database, list.take(migrations.migrations(), 3))
  let assert Ok(codec) =
    worker.codec("v14-int", worker.infallible(json.int), decode.int)
  let assert Ok(definition) =
    worker.define("v14.backfill", "1", codec, codec, fn(value) { Ok(value) })
  let connection = postgres.connection(database)
  let assert Ok(expired) =
    postgres.submit(database, "v14-backfill", definition, 1)
  let assert Ok(reported) =
    postgres.submit(database, "v14-backfill", definition, 2)
  let assert Ok(waiting) =
    postgres.submit(database, "v14-backfill", definition, 3)
  // Both jobs held uncertain under schema 13: one by the lease-expiry scan
  // (no acknowledgement), one by a handler's own Uncertain acknowledgement.
  list.each([expired, reported], fn(handle) {
    let assert Ok(_) =
      pog.query(
        "UPDATE grind_jobs SET state = 'uncertain', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'gone', attempt_count = 1, lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp() WHERE id = $1",
      )
      |> pog.parameter(pog.int(job.id_value(handle)))
      |> pog.execute(on: connection)
  })
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) SELECT 'v14-reported', queue, id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, 'uncertain', sha256('reported'::bytea) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(reported)))
    |> pog.execute(on: connection)

  let assert Ok(Nil) = postgres.migrate(database)

  // The migration only adds the columns: it scans nothing under the lock
  // they take.
  mark(connection, expired) |> should.equal(None)

  // A settler's start marks the job a lease expiry held, and not the one a
  // handler reported uncertain.
  let assert Ok(workers) = registry.new("v14-backfill")
  let assert Ok(workers) =
    registry.register(
      workers,
      definition
        |> fn(definition) {
          worker.Worker(
            ..definition,
            settlement: Some(fn(_transaction, _abandoned) {
              Ok(worker.SettleLeaveUncertain("test"))
            }),
          )
        },
    )
  settlement.recognise_unmarked(database, workers) |> should.equal(Ok(1))
  mark(connection, expired)
  |> should.equal(Some(attempt_id(connection, expired)))
  mark(connection, reported) |> should.equal(None)
  mark(connection, waiting) |> should.equal(None)
  mark_database_test_executed("settlement-v14-backfill")
}

/// The attempt the job's settlement mark names, when it is due.
fn mark(
  connection: pog.Connection,
  handle: job.JobHandle(a, b, c),
) -> Option(Int) {
  let assert Ok(returned) =
    pog.query(
      "SELECT settle_attempt_id FROM grind_jobs WHERE id = $1 AND settle_at IS NOT NULL",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(on: connection)
  case returned.rows {
    [attempt] -> Some(attempt)
    _ -> None
  }
}

fn attempt_id(
  connection: pog.Connection,
  handle: job.JobHandle(a, b, c),
) -> Int {
  let assert Ok(returned) =
    pog.query("SELECT attempt_id FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(on: connection)
  let assert [attempt] = returned.rows
  attempt
}
