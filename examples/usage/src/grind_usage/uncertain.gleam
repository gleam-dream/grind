//// Authority and failure boundaries: a handler that cannot tell whether its
//// effect happened reports `Uncertain`; an operator investigates and
//// resolves the job with an attributed, idempotent decision. A worker whose
//// attempts can be abandoned declares how such a job is settled, and the
//// application's writes commit with the decision.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/option.{Some}
import gleam/result
import gleam/string
import gleam/time/duration
import grind
import grind/admin
import grind/job
import grind/worker
import grind_usage/support
import pog

fn amount() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

const evidence = "the provider may have captured payment op-42"

fn charge(queue: String) -> worker.Worker(Int, Int, Nil) {
  worker.responding(
    support.unique("uncertain.charge"),
    input: amount(),
    output: amount(),
    handle: fn(_context, _value) { worker.Uncertain(evidence) },
  )
  |> worker.with_queue(queue)
}

pub fn run(url: String) -> Nil {
  let charge = charge(support.unique("uncertain"))
  use jobs <- support.with_grind(url, fn(config) {
    config |> grind.with_worker(charge) |> grind.without_pruner
  })
  let within = duration.seconds(10)

  // The job is held, never run twice without a decision.
  let assert Ok(grind.Inserted(handle)) =
    grind.submit(jobs, job.new(charge, 42))
  let assert Ok(grind.Uncertain(_)) = grind.await(jobs, handle, within:)
  let assert Ok([summary]) =
    admin.list(
      jobs,
      admin.query(limit: 10)
        |> admin.in_queue(charge.queue)
        |> admin.in_state(job.Uncertain),
    )
  let assert True = summary.id == job.id(handle)
  let assert Some(_) = summary.description

  // The decision identity is persisted before it is applied, qualified by
  // the job, so a retry after a lost reply repeats the same command.
  // snips-start: resolve-uncertain
  let command =
    admin.resolution(
      admin.ConfirmSuccess(42),
      id: "job:" <> int.to_string(job.id(handle)) <> ":decision:1",
      by: "operator@example.com",
      details: "the provider ledger confirms op-42",
    )
  let assert Ok(admin.Applied(job.Succeeded)) =
    admin.resolve_uncertain(jobs, handle, command)
  // snips-end: resolve-uncertain
  let assert Ok(admin.AlreadyApplied(job.Succeeded)) =
    admin.resolve_uncertain(jobs, handle, command)
  let assert Ok(grind.Succeeded(42)) = grind.outcome(jobs, handle)

  // Resolution can share a transaction with the application's own writes.
  let assert Ok(grind.Inserted(second)) =
    grind.submit(jobs, job.new(charge, 43))
  let assert Ok(grind.Uncertain(_)) = grind.await(jobs, second, within:)
  let staged =
    admin.resolution(
      admin.ConfirmSuccess(43),
      id: "job:" <> int.to_string(job.id(second)) <> ":decision:1",
      by: "operator@example.com",
      details: "the provider ledger confirms op-43",
    )
  let assert Ok(admin.Staged(admin.Applied(job.Succeeded))) =
    pog.transaction(grind.connection(jobs), fn(tx) {
      admin.resolve_uncertain_in(jobs, tx, second, staged)
    })

  io.println("uncertain: two uncertain jobs resolved")
  settled(url)
  support.mark("usage-uncertain-passed")
}

/// An agent run that stops mid-way. Its timeout holds it uncertain, and its
/// declared settlement writes a notice and records the run as failed, in
/// one transaction.
fn settled(url: String) -> Nil {
  let notices = support.identifier("agent_notices")
  let agent = agent(support.unique("agent"), notices)
  use jobs <- support.with_grind(url, fn(config) {
    config |> grind.with_worker(agent) |> grind.without_pruner
  })
  let assert Ok(_) =
    pog.query("CREATE TABLE " <> notices <> " (run_id bigint PRIMARY KEY)")
    |> pog.execute(on: grind.connection(jobs))
  let assert Ok(grind.Inserted(run)) =
    grind.submit(jobs, job.new(agent, "summarise the inbox"))
  let assert Ok(grind.Failed(grind.Business(reason), _, _)) =
    await_decided(jobs, run, 200)
  let assert True = reason == "the agent run stopped mid-way"
  let assert Ok(returned) =
    pog.query("SELECT count(*)::bigint FROM " <> notices)
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(on: grind.connection(jobs))
  let assert [1] = returned.rows
  io.println("uncertain: an abandoned agent run settled as failed")
}

fn text() -> worker.Codec(String) {
  worker.codec(worker.infallible(json.string), decode.string)
}

fn agent(
  queue: String,
  notices: String,
) -> worker.Worker(String, String, String) {
  // snips-start: declare-settlement
  worker.new("agent.run", input: text(), output: text(), perform: run_agent)
  |> worker.with_error_codec(text())
  |> worker.with_timeout(worker.After(duration.milliseconds(100)))
  |> worker.with_settlement(fn(transaction, abandoned) {
    // Short writes on the transaction only: they commit with the decision.
    use _ <- result.try(
      pog.query("INSERT INTO " <> notices <> " (run_id) VALUES ($1)")
      |> pog.parameter(pog.int(abandoned.job_id))
      |> pog.execute(on: transaction)
      |> result.map_error(string.inspect),
    )
    Ok(worker.ConfirmFailure("the agent run stopped mid-way"))
  })
  // snips-end: declare-settlement
  |> worker.with_queue(queue)
}

/// Never returns in time: its timeout abandons the attempt.
fn run_agent(_prompt: String) -> Result(String, String) {
  process.sleep(60_000)
  Ok("unreachable")
}

/// Reads the outcome until it is no longer pending or uncertain: the
/// settler decides on its own schedule.
fn await_decided(
  jobs: grind.Grind,
  run: job.JobHandle(String, String, String),
  checks: Int,
) -> Result(grind.Outcome(String, String), grind.ReadError) {
  case grind.outcome(jobs, run) {
    Ok(grind.Pending(_)) | Ok(grind.Uncertain(_)) if checks > 0 -> {
      process.sleep(50)
      await_decided(jobs, run, checks - 1)
    }
    other -> other
  }
}
