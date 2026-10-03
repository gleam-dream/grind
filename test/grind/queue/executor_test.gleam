import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/string
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/diagnostic
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/diagnostics
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/job_state.{wait_for_succeeded}
import grind/support/lease_queries.{lease_expiration}
import grind/support/observers.{detach}
import pog

pub fn postgres_first_ack_rollback_retries_proposal_without_rerun_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> first_ack_rollback(url)
  }
}

fn first_ack_rollback(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(codec) =
    worker.codec("first-rollback-int", worker.infallible(json.int), decode.int)
  let invoked = process.new_subject()
  let assert Ok(definition) =
    worker.define("first.rollback", "v1", codec, codec, fn(value) {
      process.send(invoked, value)
      Ok(value + 1)
    })
  let assert Ok(workers) = registry.new("first-rollback")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "first-rollback", definition, 41)
  let #(acks, ack_attachment) =
    diagnostics.capture(diagnostic.acknowledgement(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(ack_attachment) })
  let #(retries, retry_attachment) =
    diagnostics.capture(diagnostic.acknowledgement_retry(), fn(meta) {
      meta.context.ref.job_id == job.id_value(handle)
    })
  use <- exception.defer(fn() { detach(retry_attachment) })
  let #(capacity, capacity_attachment) =
    diagnostics.capture(diagnostic.capacity(), fn(meta) {
      meta.queue.queue == "first-rollback"
    })
  use <- exception.defer(fn() { detach(capacity_attachment) })
  // Sequence advancement survives rollback. A single real acknowledgement
  // fails before COMMIT; its retry must preserve the completed proposal.
  execute(connection, "CREATE SEQUENCE first_ack_failure_count")
  execute(
    connection,
    "CREATE FUNCTION fail_first_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job.id_value(handle))
      <> " AND NEW.state = 'succeeded' AND nextval('first_ack_failure_count') = 1 THEN RAISE EXCEPTION 'injected serialization failure' USING ERRCODE = '40001'; END IF; RETURN NEW; END $$",
  )
  execute(
    connection,
    "CREATE TRIGGER fail_first_ack BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION fail_first_ack()",
  )
  use <- exception.defer(fn() {
    execute(connection, "DROP TRIGGER fail_first_ack ON grind_jobs")
    execute(connection, "DROP FUNCTION fail_first_ack()")
    execute(connection, "DROP SEQUENCE first_ack_failure_count")
  })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(10)
    |> queue.with_lease_duration(6000)
    |> queue.with_shutdown_grace(4500)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  process.receive(invoked, 2000) |> should.equal(Ok(41))
  // Draining must keep the completed proposal active through its first
  // rollback and retry; handler return alone cannot make shutdown clean.
  queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
  postgres.outcome(database, handle) |> should.equal(Ok(job.SucceededWith(42)))
  process.receive(invoked, 0) |> should.equal(Error(Nil))
  let assert Ok(#(failed_measurements, failed)) = process.receive(acks, 5000)
  failed.outcome |> should.equal(diagnostic.AckRolledBack)
  failed_measurements.count |> should.equal(1)
  { failed_measurements.duration_us > 0 } |> should.be_true()
  let assert Ok(#(retry_measurements, retry)) = process.receive(retries, 5000)
  retry.reason |> should.equal(diagnostic.RetryAfterFailure)
  retry_measurements.retry_number |> should.equal(1)
  retry_measurements.delay_ms |> should.equal(2000)
  { retry_measurements.pending_duration_us >= 0 } |> should.be_true()
  retry.context |> should.equal(failed.context)
  retry.command_id |> should.equal(failed.command_id)
  let assert Ok(#(_, completed)) = process.receive(acks, 5000)
  completed.outcome |> should.equal(diagnostic.AckReplied)
  completed.context |> should.equal(failed.context)
  completed.context.attempt.attempt |> should.equal(1)
  completed.context.consumer.node |> should.not_equal("")
  completed.context.consumer.consumer |> should.not_equal("")
  completed.command_id |> should.equal(failed.command_id)
  let assert Ok(#(pending, _)) =
    diagnostics.await(capacity, fn(sample) { sample.0.ack_pending == 1 }, 5000)
  pending.active |> should.equal(1)
  pending.running |> should.equal(0)
  pending.available |> should.equal(pending.maximum - 1)
  let assert Ok(#(empty, drained)) =
    diagnostics.await(
      capacity,
      fn(sample) { sample.0.active == 0 && sample.1.draining },
      5000,
    )
  empty.ack_pending |> should.equal(0)
  empty.available |> should.equal(empty.maximum)
  drained.queue.consumer |> should.equal(completed.context.consumer)
  mark_database_test_executed("diagnostic-ack-rollback-retry-capacity")
  mark_database_test_executed("first-ack-rollback-retried-without-rerun")
}

fn execute(connection: pog.Connection, sql: String) -> Nil {
  let assert Ok(_) = pog.query(sql) |> pog.execute(on: connection)
  Nil
}

type Started {
  Started(value: Int, release: process.Subject(Nil))
}

pub fn postgres_slow_acks_do_not_starve_healthy_sibling_renewal_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> slow_acks(url)
  }
}

fn slow_acks(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.with_pool_size(10)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(codec) =
    worker.codec("slow-acks-int", worker.infallible(json.int), decode.int)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("slow.acks", "v1", codec, codec, fn(value) {
      let release = process.new_subject()
      process.send(started, Started(value, release))
      let assert Ok(Nil) = process.receive(release, 20_000)
      Ok(value)
    })
  let assert Ok(workers) = registry.new("slow-acks")
  let assert Ok(workers) = registry.register(workers, definition)
  let handles =
    list.map([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], fn(value) {
      let assert Ok(handle) =
        postgres.submit(database, "slow-acks", definition, value)
      handle
    })
  let ids =
    handles
    |> list.take(8)
    |> list.map(fn(handle) { int.to_string(job.id_value(handle)) })
    |> string.join(",")
  execute(connection, "CREATE SEQUENCE slow_ack_activation")
  execute(
    connection,
    "CREATE FUNCTION delay_executor_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id IN ("
      <> ids
      <> ") AND NEW.state = 'succeeded' THEN PERFORM nextval('slow_ack_activation'); PERFORM pg_sleep(0.85); END IF; RETURN NEW; END $$",
  )
  execute(
    connection,
    "CREATE TRIGGER delay_executor_ack BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION delay_executor_ack()",
  )
  use <- exception.defer(fn() {
    execute(connection, "DROP TRIGGER delay_executor_ack ON grind_jobs")
    execute(connection, "DROP FUNCTION delay_executor_ack()")
    execute(connection, "DROP SEQUENCE slow_ack_activation")
  })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(10)
    |> queue.with_poll_interval(10)
    |> queue.with_lease_duration(6012)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let running =
    list.map(handles, fn(_) {
      let assert Ok(signal) = process.receive(started, 5000)
      signal
    })
  // Eight completions begin just before the first renewal is due. Healthy
  // siblings keep running across more than one original lease duration.
  process.sleep(1500)
  list.each(running, fn(signal) {
    case signal.value <= 8 {
      True -> process.send(signal.release, Nil)
      False -> Nil
    }
  })
  process.sleep(6500)
  let assert Ok(activation) =
    pog.query("SELECT is_called FROM slow_ack_activation")
    |> pog.returning({
      use called <- decode.field(0, decode.bool)
      decode.success(called)
    })
    |> pog.execute(on: connection)
  activation.rows |> should.equal([True])
  list.each(running, fn(signal) {
    case signal.value > 8 {
      True -> process.send(signal.release, Nil)
      False -> Nil
    }
  })
  handles
  |> list.drop(8)
  |> list.each(fn(handle) {
    { wait_for_succeeded(database, handle, 120) } |> should.be_true()
  })
  process.receive(started, 0) |> should.equal(Error(Nil))
  mark_database_test_executed("slow-acks-independent-healthy-renewal")
}

pub fn postgres_saturated_main_pool_cannot_starve_renewal_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> saturated_pool(url)
  }
}

fn saturated_pool(url: String) -> Nil {
  let assert Ok(settings) =
    postgres.settings(url)
    |> postgres.with_pool_size(1)
    |> postgres.with_unique_lock_wait(1)
    |> postgres.with_statement_deadline(1002)
    |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(observer_settings) = postgres.settings(url) |> postgres.validate
  let assert Ok(observer) = postgres.start(observer_settings)
  use <- exception.defer(fn() { postgres.close(observer) })
  let assert Ok(codec) =
    worker.codec("pool-saturation-int", worker.infallible(json.int), decode.int)
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("pool.saturation", "v1", codec, codec, fn(value) {
      let release = process.new_subject()
      process.send(started, Started(value, release))
      let assert Ok(Nil) = process.receive(release, 20_000)
      Ok(value)
    })
  let assert Ok(workers) = registry.new("pool-saturation")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "pool-saturation", definition, 7)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(2)
    |> queue.with_poll_interval(10)
    |> queue.with_lease_duration(6012)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })
  let assert Ok(Started(7, release)) = process.receive(started, 5000)
  let observer_connection = postgres.connection(observer)
  let assert Ok(initial_expiry) =
    lease_expiration(observer_connection, job.id_value(handle))
  let occupied = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      // Deliberately bypass Grind's deadline to occupy the only ordinary
      // connection longer than a complete lease. Both claim and ACK callers
      // must contend for it, while renewal has its own supervised pool.
      let outcome =
        pog.query("SELECT 1 FROM pg_sleep(8) /* grind_pool_saturation */")
        |> pog.timeout(15_000)
        |> pog.execute(on: postgres.connection(database))
      process.send(occupied, outcome)
    })
  { wait_for_saturated_pool(observer_connection, 200) } |> should.be_true()
  process.sleep(6500)
  let assert Ok(renewed_expiry) =
    lease_expiration(observer_connection, job.id_value(handle))
  { renewed_expiry > initial_expiry + 3000 } |> should.be_true()
  postgres.state(observer, handle) |> should.equal(Ok(job.Executing))
  process.send(release, Nil)
  let assert Ok(Ok(_)) = process.receive(occupied, 5000)
  { wait_for_succeeded(observer, handle, 240) } |> should.be_true()
  postgres.outcome(observer, handle) |> should.equal(Ok(job.SucceededWith(7)))
  process.receive(started, 0) |> should.equal(Error(Nil))
  mark_database_test_executed("saturated-pool-independent-renewal")
}

fn wait_for_saturated_pool(connection: pog.Connection, remaining: Int) -> Bool {
  let assert Ok(result) =
    pog.query(
      "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state = 'active' AND query LIKE '%grind_pool_saturation%')",
    )
    |> pog.returning({
      use active <- decode.field(0, decode.bool)
      decode.success(active)
    })
    |> pog.execute(on: connection)
  case result.rows, remaining > 0 {
    [True], _ -> True
    _, False -> False
    _, True -> {
      process.sleep(10)
      wait_for_saturated_pool(connection, remaining - 1)
    }
  }
}
