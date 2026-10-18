//// Authority and failure boundaries: a handler that cannot tell whether its
//// effect happened reports `Uncertain`; an operator investigates and
//// resolves the job with an attributed, idempotent decision.

import gleam/dynamic/decode
import gleam/int
import gleam/io
import gleam/json
import gleam/option.{Some}
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
  let command =
    admin.resolution(
      admin.ConfirmSuccess(42),
      id: "job:" <> int.to_string(job.id(handle)) <> ":decision:1",
      by: "operator@example.com",
      details: "the provider ledger confirms op-42",
    )
  let assert Ok(admin.Applied(job.Succeeded)) =
    admin.resolve_uncertain(jobs, handle, command)
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
  support.mark("usage-uncertain-passed")
}
