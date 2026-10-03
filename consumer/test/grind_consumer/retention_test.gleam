import exception
import gleam/erlang/process
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind_consumer/support/env
import grind_consumer/support/wait
import grind_consumer/support/workers

pub fn public_consumer_prune_finished_deletes_old_jobs_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_prune_finished_test(url)
  }
}

fn run_prune_finished_test(url: String) -> Nil {
  let assert Ok(settings) = postgres.settings(url) |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_def = workers.unique_echo_worker("consumer.prune-echo")
  let assert Ok(workers) = registry.new("consumer-prune")
  let assert Ok(workers) = registry.register(workers, worker_def)
  let assert Ok(handle) =
    postgres.submit(database, "consumer-prune", worker_def, 1)
  let assert Ok(consumer) = queue.start(database, workers, env.manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))
  wait.await_state(database, handle, job.Succeeded, 250) |> should.equal(True)

  // Comfortably longer than this suite's own per-test overhead, so an
  // earlier test's already-finished row is never mistaken for "just
  // finished" by this exact call's own 100ms retention window.
  process.sleep(120)

  // `limit` is generous, not tight: `prune_finished` has no queue filter, so
  // this call also prunes every earlier test's own now-old-enough
  // `succeeded` row sharing this database — `limit: 10` reliably missed
  // this exact job once enough of those existed (oldest-`finished_at`-first
  // ordering picked ten *other* rows instead).
  let assert Ok(report) =
    postgres.prune_finished(database, older_than_ms: 100, limit: 1000)
  { report.jobs >= 1 } |> should.equal(True)

  wait.await_pruned(database, handle, 100) |> should.equal(True)
  postgres.state(database, handle) |> should.equal(Error(postgres.JobNotFound))
  postgres.outcome(database, handle)
  |> should.equal(Error(postgres.JobNotFound))

  env.mark("consumer-prune-finished-passed")
}
