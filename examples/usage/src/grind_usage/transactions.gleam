//// Enqueueing inside the application's own transaction: the job commits or
//// rolls back with the application's writes.

import gleam/dynamic/decode
import gleam/io
import gleam/json
import gleam/time/duration
import grind
import grind/job
import grind/worker
import grind_usage/support
import pog

fn amount() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

fn receipt(queue: String) -> worker.Worker(Int, Int, Nil) {
  worker.new(
    support.unique("orders.receipt"),
    input: amount(),
    output: amount(),
    perform: fn(value) { Ok(value) },
  )
  |> worker.with_queue(queue)
}

pub fn run(url: String) -> Nil {
  let receipt = receipt(support.unique("orders"))
  use jobs <- support.with_grind(url, grind.with_worker(_, receipt))
  let db = grind.connection(jobs)
  let within = duration.seconds(10)
  let assert Ok(_) =
    pog.query("CREATE TABLE IF NOT EXISTS usage_orders (id text PRIMARY KEY)")
    |> pog.execute(db)

  // The order row and its receipt job commit together. The reply means the
  // admission is staged in the application's transaction, not committed.
  // snips-start: transaction
  let order = support.unique("order")
  let assert Ok(grind.Inserted(handle)) =
    pog.transaction(db, fn(tx) {
      let assert Ok(_) =
        pog.query("INSERT INTO usage_orders (id) VALUES ($1)")
        |> pog.parameter(pog.text(order))
        |> pog.execute(tx)
      grind.submit_in(jobs, tx, job.new(receipt, 9) |> job.with_id(order))
    })
  // snips-end: transaction
  let assert Ok(grind.Succeeded(9)) = grind.await(jobs, handle, within:)

  // A rollback takes the job with it: the same id is still free afterwards.
  let rolled_back = support.unique("order")
  let assert Error(_) =
    pog.transaction(db, fn(tx) {
      let assert Ok(grind.Inserted(_)) =
        grind.submit_in(
          jobs,
          tx,
          job.new(receipt, 5) |> job.with_id(rolled_back),
        )
      Error("the order write failed")
    })
  let assert Ok(grind.Inserted(fresh)) =
    grind.submit(jobs, job.new(receipt, 6) |> job.with_id(rolled_back))
  let assert Ok(grind.Succeeded(6)) = grind.await(jobs, fresh, within:)

  io.println("transactions: one job committed, one rolled back")
  support.mark("usage-transactions-passed")
}
