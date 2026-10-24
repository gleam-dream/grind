//// One pool shared with the application, from outside the package: a
//// handler queries the application's own tables through its context, the
//// application's `search_path` survives Grind's work, a plain submit needs
//// no `case` on the admission, and consumers wait for a current schema.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{None}
import gleam/string
import gleam/time/duration
import gleeunit/should
import grind
import grind/job
import grind/worker
import grind_consumer
import grind_consumer/support/env
import pog

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

pub fn a_handler_writes_the_application_tables_on_the_shared_pool_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let suffix = string.replace(env.unique(""), "-", "_")
      // Grind in its own schema; the application's table in public,
      // queried unqualified.
      let table = "consumer_ledger" <> suffix
      let ledger =
        worker.responding(
          env.unique("shared.ledger"),
          input: grind_consumer.amount_codec(),
          output: grind_consumer.amount_codec(),
          handle: fn(context, amount) {
            let db = worker.connection(context)
            let assert Ok(_) =
              pog.query("INSERT INTO " <> table <> " (amount) VALUES ($1)")
              |> pog.parameter(pog.int(amount))
              |> pog.execute(db)
            worker.Succeeded(count(db, table))
          },
        )
        |> worker.with_queue(env.unique("shared-ledger"))
      use jobs <- env.with_grind(url, fn(config) {
        config
        |> grind.with_schema("consumer_jobs" <> suffix)
        |> grind.with_worker(ledger)
      })
      let db = grind.connection(jobs)
      let assert Ok(_) =
        pog.query("CREATE TABLE " <> table <> " (amount int NOT NULL)")
        |> pog.execute(db)
      let assert Ok(admission) = grind.submit(jobs, job.new(ledger, 5))
      grind.await(jobs, grind.handle(admission), within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(1)))
      count(db, table) |> should.equal(1)
      count(db, "public." <> table) |> should.equal(1)
      env.mark("consumer-shared-pool-passed")
    }
  }
}

pub fn consumers_wait_for_a_current_schema_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let suffix = string.replace(env.unique(""), "-", "_")
      let echo_job =
        worker.new(
          env.unique("schema.echo"),
          input: grind_consumer.amount_codec(),
          output: grind_consumer.amount_codec(),
          perform: fn(amount) { Ok(amount) },
        )
        |> worker.with_queue(env.unique("schema-echo"))
      let assert Error(grind.SchemaNotMigrated(found: None, required:)) =
        grind.new(env.pool(url))
        |> grind.with_schema("consumer_fresh" <> suffix)
        |> grind.with_worker(echo_job)
        |> grind.start(process.new_name("consumer_unmigrated"))
      { required > 0 } |> should.be_true
      env.mark("consumer-schema-not-migrated-passed")
    }
  }
}
