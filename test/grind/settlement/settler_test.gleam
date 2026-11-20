//// The settler a polling consumer runs (docs/design/design.typ,
//// "Settler"; behavior "A job abandoned while no node ran is settled at
//// start" in "Declared settlement of abandoned jobs").

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{Some}
import gleeunit/should
import grind/internal/attempt
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import grind/worker
import pog

pub fn postgres_job_abandoned_while_no_node_ran_is_settled_at_start_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(validated) = postgres.settings(url) |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let assert Ok(Nil) = postgres.migrate(database)
      let name = "settler-start-" <> int.to_string(unique_test_run_id())
      let table = "public.\"notices_" <> name <> "\""
      let assert Ok(_) =
        pog.query("CREATE TABLE " <> table <> " (job_id bigint NOT NULL)")
        |> pog.execute(on: postgres.connection(database))
      let codec = worker.codec(worker.infallible(json.string), decode.string)
      let agent =
        worker.new("settler.agent", input: codec, output: codec, perform: Ok)
        |> worker.with_queue(name)
        |> worker.with_error_codec(codec)
        |> worker.with_abandonment(worker.ReplayAfterLeaseExpiry(1))
        |> worker.with_settlement(fn(transaction, abandoned) {
          let assert Ok(_) =
            pog.query("INSERT INTO " <> table <> " (job_id) VALUES ($1)")
            |> pog.parameter(pog.int(abandoned.job_id))
            |> pog.execute(on: transaction)
          Ok(worker.ConfirmFailure("the run stopped mid-way"))
        })
      let assert Ok(workers) = registry.new(name)
      let assert Ok(workers) = registry.register(workers, agent)
      let assert Ok(handle) = postgres.submit(database, name, agent, "inbox")
      // The node that claimed it died, and its replay died too; no node
      // ran while both leases expired.
      expire_claim(database, name, workers)
      let assert Ok(_) = postgres.quarantine_expired(database, limit: 10_000)
      postgres.state(database, handle) |> should.equal(Ok(job.Queued))
      expire_claim(database, name, workers)

      let assert Ok(policy) =
        queue.default_policy()
        |> queue.with_poll_interval(20)
        |> queue.validate_policy
      let assert Ok(consumer) = queue.start(database, workers, policy)
      use <- exception.defer(fn() { queue.stop(consumer) })

      await_state(database, handle, job.BusinessFailed, 150)
      |> should.equal(Ok(job.BusinessFailed))
      let assert Ok(returned) =
        pog.query("SELECT count(*)::bigint FROM " <> table)
        |> pog.returning(decode.field(0, decode.int, decode.success))
        |> pog.execute(on: postgres.connection(database))
      returned.rows |> should.equal([1])
      mark_database_test_executed("settler-settles-at-start")
    }
  }
}

fn expire_claim(
  database: postgres.Database,
  name: String,
  workers: registry.Registry,
) -> Nil {
  let assert Ok(Some(claimed)) =
    attempt.claim_one(database, name, workers, "gone-node", 30_000)
  let #(id, _, _) = attempt.claim_identity(claimed)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: postgres.connection(database))
  Nil
}

/// Reads the committed state until it is `expected` or `checks` run out,
/// 20 ms apart: the settler acts on its own schedule.
fn await_state(
  database: postgres.Database,
  handle: job.JobHandle(a, b, c),
  expected: job.State,
  checks: Int,
) -> Result(job.State, job.State) {
  let assert Ok(state) = postgres.state(database, handle)
  case state == expected, checks {
    True, _ -> Ok(state)
    False, 0 -> Error(state)
    False, _ -> {
      process.sleep(20)
      await_state(database, handle, expected, checks - 1)
    }
  }
}
