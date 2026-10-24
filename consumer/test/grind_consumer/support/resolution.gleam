//// Public consumer fixtures for caller-owned resolution transactions.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/time/duration
import grind
import grind/admin
import grind/job
import grind/testing
import grind/worker
import grind_consumer/support/env
import pog

pub type OperationId {
  OperationId(Int)
}

pub type Confirmation {
  Confirmation(String)
}

pub type ProviderError {
  Refused(String)
}

pub type Fixture {
  Fixture(jobs: grind.Grind, db: pog.Connection, schema: String)
}

pub fn worker(
  db: pog.Connection,
) -> worker.Worker(OperationId, Confirmation, ProviderError) {
  worker.responding(
    "lab.operation",
    input: worker.codec(
      worker.infallible(fn(id) {
        let OperationId(n) = id
        json.int(n)
      }),
      decode.map(decode.int, OperationId),
    ),
    output: worker.codec(
      worker.infallible(fn(value) {
        let Confirmation(ref) = value
        json.string(ref)
      }),
      decode.map(decode.string, Confirmation),
    ),
    handle: fn(_, _) {
      let assert Ok(_) =
        pog.query("INSERT INTO invocation_log DEFAULT VALUES")
        |> pog.execute(db)
      worker.Uncertain(
        "external effect was independently settled; queue confirmation remains",
      )
    },
  )
  |> worker.with_error_codec(worker.codec(
    worker.infallible(fn(error) {
      let Refused(reason) = error
      json.string(reason)
    }),
    decode.map(decode.string, Refused),
  ))
  |> worker.with_queue("lab")
}

pub fn with_runtime(schema: String, body: fn(Fixture) -> Nil) -> Nil {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> with_url(schema, url, body)
  }
}

pub fn with_url(schema: String, url: String, body: fn(Fixture) -> a) -> a {
  let name = process.new_name("candidate_pool")
  let assert Ok(pool) = pog.url_config(name, url)
  let config =
    pool
    |> pog.pool_size(12)
    |> pog.connection_parameter("search_path", schema <> ",public")
  let db = pog.named_connection(name)
  let assert Ok(jobs) =
    grind.start(
      grind.new(config)
        |> grind.with_schema(schema)
        |> grind.with_startup_migration
        |> grind.with_worker(worker(db))
        |> grind.without_consumers
        |> grind.without_pruner
        |> grind.with_statement_deadline(duration.seconds(2))
        |> grind.with_unique_lock_wait(duration.milliseconds(100)),
      process.new_name("candidate_jobs"),
    )
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE local_ack (id bigint PRIMARY KEY, confirmed boolean NOT NULL DEFAULT false)",
    )
    |> pog.execute(db)
  let assert Ok(_) =
    pog.query("CREATE TABLE invocation_log (id bigserial PRIMARY KEY)")
    |> pog.execute(db)
  use <- exception.defer(fn() { grind.stop(jobs) })
  body(Fixture(jobs, db, schema))
}

pub fn held(
  f: Fixture,
  id: Int,
) -> job.JobHandle(OperationId, Confirmation, ProviderError) {
  let assert [handle] = batch(f, [id])
  handle
}

pub fn batch(
  f: Fixture,
  ids: List(Int),
) -> List(job.JobHandle(OperationId, Confirmation, ProviderError)) {
  let handles =
    list.map(ids, fn(id) {
      let assert Ok(_) =
        pog.query("INSERT INTO local_ack(id) VALUES($1)")
        |> pog.parameter(pog.int(id))
        |> pog.execute(f.db)
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(f.jobs, job.new(worker(f.db), OperationId(id)))
      handle
    })
  let assert Ok(count) =
    testing.drain(
      f.jobs,
      queue: "lab",
      limit: list.length(ids),
      within: duration.seconds(120),
    )
  assert count == list.length(ids)
  handles
}

pub fn decision(id: Int) -> admin.Resolution(Confirmation, ProviderError) {
  admin.resolution(
    admin.ConfirmSuccess(Confirmation("provider-" <> int.to_string(id))),
    id: "r-" <> int.to_string(id),
    by: "operator",
    details: "Application settlement committed",
  )
}

pub fn ack(db: pog.Connection, id: Int) -> Nil {
  let assert Ok(_) =
    pog.query("UPDATE local_ack SET confirmed=true WHERE id=$1")
    |> pog.parameter(pog.int(id))
    |> pog.execute(db)
  Nil
}

pub fn confirmed(db: pog.Connection, id: Int) -> Bool {
  let assert Ok(rows) =
    pog.query("SELECT confirmed FROM local_ack WHERE id=$1")
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use value <- decode.field(0, decode.bool)
      decode.success(value)
    })
    |> pog.execute(db)
  let assert [value] = rows.rows
  value
}

pub fn scalar(db: pog.Connection, sql: String) -> Int {
  let assert Ok(rows) =
    pog.query(sql)
    |> pog.returning({
      use value <- decode.field(0, decode.int)
      decode.success(value)
    })
    |> pog.execute(db)
  let assert [value] = rows.rows
  value
}

pub fn invocations(db: pog.Connection) -> Int {
  scalar(db, "SELECT count(*) FROM invocation_log")
}

pub fn wait_until(check: fn() -> Bool, attempts: Int) -> Nil {
  case check() {
    True -> Nil
    False -> {
      assert attempts > 0
      process.sleep(5)
      wait_until(check, attempts - 1)
    }
  }
}

pub fn gate(ready: process.Subject(process.Pid)) -> Nil {
  process.send(ready, process.self())
  let wait = process.new_subject()
  let assert Ok(Nil) = process.receive(wait, 30_000)
  Nil
}

@external(erlang, "consumer_resolution_ffi", "kill_wait")
pub fn kill_wait(pid: process.Pid) -> Nil

pub fn prune(f: Fixture, count: Int) -> Int {
  // Age is the only test fixture mutation; deletion runs the real public pruner.
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET finished_at=now()-interval '1 day' WHERE finished_at IS NOT NULL",
    )
    |> pog.execute(f.db)
  let assert Ok(pruned) =
    admin.prune_finished(f.jobs, older_than: duration.seconds(1), limit: count)
  pruned
}
