//// One installation per schema, and one pool shared with the application:
//// two schemas in one database never see each other's jobs, and a handler
//// writes the application's own tables through its context.

import gleam/dynamic/decode
import gleam/io
import gleam/json
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

fn count(db: pog.Connection, table: String) -> Int {
  let assert Ok(returned) =
    pog.query("SELECT count(*)::int FROM " <> table)
    |> pog.returning({
      use n <- decode.field(0, decode.int)
      decode.success(n)
    })
    |> pog.execute(db)
  let assert [n] = returned.rows
  n
}

/// Which schema is explicit configuration, never inferred from the session.
fn two_schemas(url: String) -> Nil {
  let queue = support.unique("isolation")
  let work =
    worker.new(
      support.unique("isolation.copy"),
      input: amount(),
      output: amount(),
      perform: fn(value) { Ok(value) },
    )
    |> worker.with_queue(queue)
  let schema_a = support.identifier("usage_a")
  let schema_b = support.identifier("usage_b")
  let configure = fn(schema) {
    fn(config) {
      config
      |> grind.with_schema(schema)
      |> grind.with_worker(work)
      |> grind.without_consumers
    }
  }
  use first <- support.with_grind(url, configure(schema_a))
  use second <- support.with_grind(url, configure(schema_b))
  let assert Ok(grind.Inserted(_)) = grind.submit(first, job.new(work, 1))
  let in_queue = admin.query(limit: 10) |> admin.in_queue(queue)
  let assert Ok([_]) = admin.list(first, in_queue)
  let assert Ok([]) = admin.list(second, in_queue)
  Nil
}

/// Grind sets its own schema for each storage call and restores the
/// session's, so the application keeps querying its own tables unqualified.
fn shared_pool(url: String) -> Nil {
  let table = support.identifier("usage_ledger")
  let ledger =
    worker.responding(
      support.unique("isolation.ledger"),
      input: amount(),
      output: amount(),
      handle: fn(context, value) {
        let db = worker.connection(context)
        let assert Ok(_) =
          pog.query("INSERT INTO " <> table <> " (amount) VALUES ($1)")
          |> pog.parameter(pog.int(value))
          |> pog.execute(db)
        worker.Succeeded(count(db, table))
      },
    )
    |> worker.with_queue(support.unique("isolation-ledger"))
  use jobs <- support.with_grind(url, fn(config) {
    config
    |> grind.with_schema(support.identifier("usage_jobs"))
    |> grind.with_worker(ledger)
  })
  let db = grind.connection(jobs)
  let assert Ok(_) =
    pog.query("CREATE TABLE " <> table <> " (amount int NOT NULL)")
    |> pog.execute(db)
  let assert Ok(admission) = grind.submit(jobs, job.new(ledger, 5))
  let assert Ok(grind.Succeeded(1)) =
    grind.await(jobs, grind.handle(admission), within: duration.seconds(10))
  let assert 1 = count(db, table)
  Nil
}

pub fn run(url: String) -> Nil {
  two_schemas(url)
  shared_pool(url)
  io.println("isolation: two schemas isolated, one pool shared")
  support.mark("usage-isolation-passed")
}
