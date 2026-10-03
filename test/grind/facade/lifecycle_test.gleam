//// One supervised runtime with a stable, name-based handle.

import exception
import gleam/erlang/process
import gleam/otp/static_supervisor as supervisor
import gleam/string
import gleam/time/duration
import gleeunit/should
import grind
import grind/facade/support.{int_codec, pool_config, unique}
import grind/job
import grind/queue
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/worker
import pog

fn echo_worker(name: String) -> worker.Worker(Int, Int, Nil) {
  worker.new(
    unique("lifecycle." <> name),
    input: int_codec(),
    output: int_codec(),
    perform: fn(n) { Ok(n) },
  )
  |> worker.with_queue(unique("lifecycle-" <> name))
}

pub fn facade_supervised_handle_survives_a_restart_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_worker = echo_worker("restart")
      let name = process.new_name("lifecycle_grind")
      let jobs = grind.named(name)
      // A handle exists before its runtime starts.
      grind.submit(jobs, job.new(echo_worker, 1))
      |> should.equal(Error(grind.SubmitNotRunning))
      let config = grind.new(pool_config(url)) |> grind.with_worker(echo_worker)
      let assert Ok(started) =
        supervisor.new(supervisor.OneForOne)
        |> supervisor.add(grind.supervised(config, name))
        |> supervisor.start
      use <- exception.defer(fn() { stop_tree(started.pid) })
      let assert Ok(Nil) = grind.migrate(jobs)
      let assert Ok(grind.Inserted(first)) =
        grind.submit(jobs, job.new(echo_worker, 1))
      grind.await(jobs, first, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(1)))

      // Killing the runtime restarts it under the same name; the handle
      // keeps working.
      let assert Ok(runtime) = process.named(name)
      process.kill(runtime)
      wait_until_restarted(name, runtime, 100)
      let assert Ok(grind.Inserted(second)) =
        grind.submit(jobs, job.new(echo_worker, 2))
      grind.await(jobs, second, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(2)))

      // A supervised runtime is stopped by its supervisor.
      grind.stop(jobs) |> should.equal(Error(grind.OwnedBySupervisor))
      mark_database_test_executed("facade-supervised-restart-passed")
    }
  }
}

fn wait_until_restarted(
  name: process.Name(grind.Message),
  old: process.Pid,
  attempts: Int,
) -> Nil {
  case process.named(name), attempts {
    Ok(pid), _ if pid != old -> Nil
    _, 0 -> panic as "the runtime did not restart"
    _, _ -> {
      process.sleep(20)
      wait_until_restarted(name, old, attempts - 1)
    }
  }
}

@external(erlang, "grind_postgres_ffi", "stop_supervisor")
fn stop_supervisor(pid: process.Pid) -> Result(Bool, Nil)

fn stop_tree(pid: process.Pid) -> Nil {
  let _ = stop_supervisor(pid)
  Nil
}

pub fn facade_stop_from_another_process_drains_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let started_handler = process.new_subject()
      let slow =
        worker.new(
          unique("lifecycle.slow"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) {
            process.send(started_handler, Nil)
            process.sleep(300)
            Ok(n)
          },
        )
        |> worker.with_queue(unique("lifecycle-slow"))
      let name = process.new_name("lifecycle_stop")
      let assert Ok(jobs) =
        grind.start(
          grind.new(pool_config(url)) |> grind.with_worker(slow),
          name,
        )
      let assert Ok(Nil) = grind.migrate(jobs)
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(slow, 9))
      let assert Ok(Nil) = process.receive(started_handler, within: 10_000)
      // Stop from a process that did not start the runtime: the running job
      // finishes within the grace.
      let stopped = process.new_subject()
      process.spawn(fn() { process.send(stopped, grind.stop(jobs)) })
      process.receive(stopped, within: 20_000)
      |> should.equal(Ok(Ok(grind.StoppedCleanly)))
      grind.state(jobs, handle) |> should.equal(Error(grind.ReadNotRunning))
      grind.stop(jobs) |> should.equal(Error(grind.NotStarted))
      // The same name starts again.
      let assert Ok(jobs) =
        grind.start(
          grind.new(pool_config(url)) |> grind.with_worker(slow),
          name,
        )
      grind.state(jobs, handle) |> should.equal(Ok(job.Succeeded))
      let assert Ok(grind.StoppedCleanly) = grind.stop(jobs)
      mark_database_test_executed("facade-stop-any-process-passed")
    }
  }
}

pub fn facade_supervisor_shutdown_drains_running_jobs_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let started_handler = process.new_subject()
      let slow =
        worker.new(
          unique("lifecycle.drain"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) {
            process.send(started_handler, Nil)
            process.sleep(500)
            Ok(n)
          },
        )
        |> worker.with_queue(unique("lifecycle-drain"))
      let name = process.new_name("lifecycle_drain")
      let config = grind.new(pool_config(url)) |> grind.with_worker(slow)
      let assert Ok(started) =
        supervisor.new(supervisor.OneForOne)
        |> supervisor.add(grind.supervised(config, name))
        |> supervisor.start
      let jobs = grind.named(name)
      let assert Ok(Nil) = grind.migrate(jobs)
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(slow, 3))
      let assert Ok(Nil) = process.receive(started_handler, within: 10_000)
      // The application's supervisor stops Grind mid-job: the consumer
      // drains before the pool stops, so the job commits.
      let _ = stop_supervisor(started.pid)
      let name_2 = process.new_name("lifecycle_drain_check")
      let assert Ok(check) =
        grind.start(
          grind.new(pool_config(url)) |> grind.without_consumers,
          name_2,
        )
      let assert Ok(rebound) = grind.bind(check, slow, job.id(handle))
      grind.outcome(check, rebound) |> should.equal(Ok(grind.Succeeded(3)))
      let assert Ok(_) = grind.stop(check)
      mark_database_test_executed("facade-supervisor-shutdown-drains-passed")
    }
  }
}

pub fn facade_start_fails_within_the_connect_timeout_test() {
  let assert Ok(config) =
    pog.url_config(
      process.new_name("lifecycle_unreachable"),
      "postgres://grind@127.0.0.1:1/missing",
    )
  let started_at = monotonic_ms()
  let result =
    grind.new(config)
    |> grind.with_connect_timeout(duration.milliseconds(500))
    |> grind.start(process.new_name("lifecycle_unreachable_grind"))
  let assert Error(grind.Unavailable(_)) = result
  { monotonic_ms() - started_at < 10_000 } |> should.be_true
}

pub fn facade_configuration_is_checked_with_typed_errors_test() {
  let assert Ok(pool) =
    pog.url_config(process.new_name("lifecycle_check"), "postgres://a@b/c")
  let checked = echo_worker("check")
  grind.new(pool)
  |> grind.with_worker(checked)
  |> grind.with_worker(checked)
  |> grind.check
  |> should.equal(Error(grind.DuplicateWorker(checked.id, "1")))
  grind.new(pool)
  |> grind.with_queue(queue.new("nobody"))
  |> grind.check
  |> should.equal(Error(grind.QueueWithoutWorkers("nobody")))
  grind.new(pool)
  |> grind.with_worker(checked)
  |> grind.with_queue(
    queue.new(checked.queue) |> queue.with_lease(duration.seconds(5)),
  )
  |> grind.check
  |> should.equal(Error(grind.LeaseTooShort(checked.queue, 5000, 16_000)))
  grind.new(pool)
  |> grind.with_unique_lock_wait(duration.seconds(4))
  |> grind.check
  |> should.equal(
    Error(grind.UniqueLockWaitTooCloseToDeadline(4000, 1000, 4000)),
  )
  grind.new(pool)
  |> grind.with_max_payload_bytes(0)
  |> grind.check
  |> should.equal(Error(grind.NotPositive("payload limit", 0)))
  grind.new(pool)
  |> grind.with_pruner(max_age: duration.seconds(0))
  |> grind.check
  |> should.equal(Error(grind.NotPositive("pruner max age", 0)))
  grind.new(pool) |> grind.check |> should.equal(Ok(Nil))
}

pub fn facade_second_start_under_one_name_fails_without_secrets_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let name = process.new_name("lifecycle_twice")
      let assert Ok(jobs) =
        grind.start(
          grind.new(pool_config(url)) |> grind.without_consumers,
          name,
        )
      let assert Error(grind.StartFailed(description)) =
        grind.start(
          grind.new(pool_config(url)) |> grind.without_consumers,
          name,
        )
      description
      |> should.equal("a runtime is already running under this name")
      let assert Ok(_) = grind.stop(jobs)
      mark_database_test_executed("facade-second-start-fails-passed")
    }
  }
}

pub fn facade_config_hides_the_password_test() {
  let assert Ok(pool) =
    pog.url_config(
      process.new_name("lifecycle_secret"),
      "postgres://app:s3cret-pass@localhost/app",
    )
  let config = grind.new(pool)
  string.contains(string.inspect(config), "s3cret-pass") |> should.be_false
}

@external(erlang, "grind_queue_ffi", "monotonic_ms")
fn monotonic_ms() -> Int
