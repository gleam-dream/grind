//// What a settlement reports (docs/design/design.typ, "Observations"): an
//// applied decision is the `resolved` lifecycle event; every other try is
//// a `settlement` diagnostic.

import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/option.{Some}
import gleeunit/should
import grind/internal/attempt
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/settlement
import grind/job as public_job
import grind/support/diagnostics
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import grind/telemetry
import grind/worker
import pog
import sinal

pub fn postgres_settlement_reports_each_try_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(validated) = postgres.settings(url) |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let assert Ok(Nil) = postgres.migrate(database)
      let name = "settle-observed-" <> int.to_string(unique_test_run_id())
      let codec = worker.codec(worker.infallible(json.string), decode.string)
      let agent =
        worker.new("settle.observed", input: codec, output: codec, perform: Ok)
        |> worker.with_queue(name)
        |> worker.with_error_codec(codec)
        |> worker.with_settlement(fn(_transaction, abandoned) {
          case abandoned.settlement_try {
            1 -> Error("not yet")
            _ -> Ok(worker.ConfirmFailure("settled"))
          }
        })
      let assert Ok(workers) = registry.new(name)
      let assert Ok(workers) = registry.register(workers, agent)
      let #(tried, tries) =
        diagnostics.capture(telemetry.settlement(), fn(metadata) {
          metadata.ref.queue == name
        })
      use <- exception.defer(fn() {
        let _ = sinal.detach(tries)
        Nil
      })
      let #(resolved, resolutions) =
        diagnostics.capture(telemetry.resolved(), fn(metadata) {
          metadata.ref.queue == name
        })
      use <- exception.defer(fn() {
        let _ = sinal.detach(resolutions)
        Nil
      })
      let assert Ok(handle) = postgres.submit(database, name, agent, "inbox")
      let assert Ok(Some(claimed)) =
        attempt.claim_one(database, name, workers, "gone-node", 30_000)
      let #(id, attempt_id, _) = attempt.claim_identity(claimed)
      let assert Ok(_) =
        pog.query(
          "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1",
        )
        |> pog.parameter(pog.int(id))
        |> pog.execute(on: postgres.connection(database))
      let assert Ok(_) = postgres.quarantine_expired(database, limit: 10_000)

      let assert Ok(settlement.TryFailed(_, _)) =
        settlement.settle_next(database, workers, 30_000)
      let assert Ok(#(measurements, failed)) =
        diagnostics.await(tried, fn(_) { True }, 1000)
      failed.ref.job_id |> should.equal(job.id_value(handle))
      failed.attempt_id |> should.equal(attempt_id)
      failed.outcome |> should.equal(telemetry.SettlementFailed)
      failed.failure |> should.equal(Some(telemetry.SettlementFunctionFailed))
      measurements.settlement_try |> should.equal(1)

      let assert Ok(_) =
        pog.query(
          "UPDATE grind_jobs SET settle_at = clock_timestamp() WHERE id = $1",
        )
        |> pog.parameter(pog.int(id))
        |> pog.execute(on: postgres.connection(database))
      let assert Ok(settlement.Settled(_, job.BusinessFailed)) =
        settlement.settle_next(database, workers, 30_000)
      let assert Ok(#(_, applied)) =
        diagnostics.await(resolved, fn(_) { True }, 1000)
      applied.ref.job_id |> should.equal(job.id_value(handle))
      applied.decision |> should.equal(telemetry.DecisionConfirmBusinessFailure)
      applied.committed_state |> should.equal(public_job.BusinessFailed)
      applied.confirmation |> should.equal(telemetry.Replied)
      applied.resolution_id
      |> should.equal(
        "grind-settlement:"
        <> int.to_string(id)
        <> ":"
        <> int.to_string(attempt_id),
      )
      diagnostics.await(tried, fn(_) { True }, 100) |> should.equal(Error(Nil))
      mark_database_test_executed("settlement-observations")
    }
  }
}
