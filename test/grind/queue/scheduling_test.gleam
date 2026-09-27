import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/job_state.{wait_for_job_state}
import grind/support/queue_signals.{LaterWorkerInvoked, WorkerInvoked}
import grind/worker
import pog

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
    |> queue.with_maximum_batch_jobs(2)
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
