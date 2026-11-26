import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import grind/support/job_state.{wait_for_job_state}
import pog

/// A batch claim starts its runnable attempts before it parks the rows
/// whose codec contract no longer matches. Parking writes therefore never
/// sit between a runnable row's claim and its start: here six slow parking
/// writes take longer than the lease, and the valid row claimed with them
/// still runs under its lease and succeeds.
pub fn postgres_batch_claim_starts_runnable_rows_before_parking_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_batch_parking_test(database_url)
  }
}

fn run_batch_parking_test(database_url: String) -> Nil {
  let queue_name = "batch-parking-" <> int.to_string(unique_test_run_id())
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_statement_deadline(1500)
    |> postgres.with_unique_lock_wait(100)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(input_codec) =
    worker.codec("parking-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "parking-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(drifted) =
    worker.define("parking.drifted", "v1", input_codec, output_codec, fn(n) {
      Ok(int.to_string(n))
    })
  let assert Ok(valid) =
    worker.define("parking.valid", "v1", input_codec, output_codec, fn(n) {
      Ok(int.to_string(n))
    })
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, drifted)
  let assert Ok(workers) = registry.register(workers, valid)

  // Six rows whose stored output codec differs from the registered one come
  // first in selection order, then one runnable row.
  list.each([1, 2, 3, 4, 5, 6], fn(n) {
    let assert Ok(_) = postgres.submit(database, queue_name, drifted, n)
    Nil
  })
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET output_version = 'parking-output-v2' WHERE queue = $1",
    )
    |> pog.parameter(pog.text(queue_name))
    |> pog.execute(on: connection)
  let assert Ok(runnable) = postgres.submit(database, queue_name, valid, 7)

  // Each parking write of this queue takes 1.2 s, under the 1.5 s statement
  // deadline: six of them outlast the 6 s lease.
  let function = "slow_parking_" <> int.to_string(unique_test_run_id())
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> function
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1.2); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> function
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW WHEN (NEW.state = 'contract_mismatch' AND NEW.queue = '"
      <> queue_name
      <> "') EXECUTE FUNCTION "
      <> function
      <> "()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> function <> " ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> function <> "()")
      |> pog.execute(on: connection)
    Nil
  })

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(60_000)
    |> queue.with_maximum_concurrency(7)
    |> queue.with_lease_duration(6000)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  wait_for_job_state(database, runnable, job.Succeeded, 600)
  |> should.equal(True)
  mark_database_test_executed("batch-claim-starts-runnable-before-parking")
}

/// Parking does not depend on the claim's lease: a mismatched row whose
/// lease lapsed before it was parked is still parked under its claim token,
/// instead of staying executing for lease-expiry recovery to hold uncertain
/// although its handler never ran.
pub fn postgres_mismatch_with_lapsed_lease_is_parked_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_lapsed_park_test(database_url)
  }
}

fn run_lapsed_park_test(database_url: String) -> Nil {
  let queue_name = "lapsed-parking-" <> int.to_string(unique_test_run_id())
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(input_codec) =
    worker.codec("lapsed-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "lapsed-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(drifted) =
    worker.define("lapsed.drifted", "v1", input_codec, output_codec, fn(n) {
      Ok(int.to_string(n))
    })
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, drifted)
  let assert Ok(handle) = postgres.submit(database, queue_name, drifted, 1)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET output_version = 'lapsed-output-v2' WHERE queue = $1",
    )
    |> pog.parameter(pog.text(queue_name))
    |> pog.execute(on: connection)

  // The claim of this queue writes a lease that has already lapsed.
  let function = "lapsed_claim_" <> int.to_string(unique_test_run_id())
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> function
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.lease_expires_at := clock_timestamp() - interval '1 second'; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> function
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW WHEN (NEW.state = 'executing' AND OLD.state <> 'executing' AND NEW.queue = '"
      <> queue_name
      <> "') EXECUTE FUNCTION "
      <> function
      <> "()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> function <> " ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> function <> "()")
      |> pog.execute(on: connection)
    Nil
  })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: worker.OutputCodec,
        expected: "lapsed-output-v2",
        actual: "lapsed-output-v1",
      )),
    ),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.ContractMismatch))
  mark_database_test_executed("mismatch-with-lapsed-lease-parked")
}
