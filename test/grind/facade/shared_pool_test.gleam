//// The pool Grind shares with the application keeps the application's
//// `search_path`; handlers reach it through their context; and a runtime
//// that runs consumers starts only on a current schema.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/option.{None}
import gleam/otp/static_supervisor as supervisor
import gleam/result
import gleam/string
import gleam/time/duration
import gleeunit/should
import grind
import grind/facade/support.{int_codec, pool_config, unique}
import grind/internal/postgres
import grind/job
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import grind/worker
import pog

fn text_column() -> decode.Decoder(String) {
  use value <- decode.field(0, decode.string)
  decode.success(value)
}

fn int_column() -> decode.Decoder(Int) {
  use value <- decode.field(0, decode.int)
  decode.success(value)
}

fn one(
  connection: pog.Connection,
  sql: String,
  decoder: decode.Decoder(a),
) -> a {
  let assert Ok(returned) =
    pog.query(sql) |> pog.returning(decoder) |> pog.execute(connection)
  let assert [value] = returned.rows
  value
}

pub fn facade_shared_pool_keeps_the_application_search_path_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let id = int.to_string(unique_test_run_id())
      let schema = "facade_shared_" <> id
      let table = "facade_app_items_" <> id
      // The handler writes and reads the application's table, unqualified,
      // through the pool its context carries.
      let recording =
        worker.responding(
          unique("shared.record"),
          input: int_codec(),
          output: int_codec(),
          handle: fn(context, n) {
            let db = worker.connection(context)
            let assert Ok(_) =
              pog.query("INSERT INTO " <> table <> " (n) VALUES ($1)")
              |> pog.parameter(pog.int(n))
              |> pog.execute(db)
            worker.Succeeded(one(
              db,
              "SELECT count(*)::int FROM " <> table,
              int_column(),
            ))
          },
        )
        |> worker.with_queue(unique("shared-record"))
      // One connection, so every application query below runs on the
      // session Grind's own storage calls used.
      let config =
        grind.new(pool_config(url) |> pog.pool_size(1))
        |> grind.with_schema(schema)
        |> grind.with_worker(recording)
        |> grind.with_startup_migration
      let name = process.new_name("shared_pool_grind")
      let assert Ok(jobs) = grind.start(config, name)
      let db = grind.connection(jobs)
      let assert Ok(_) =
        pog.query("CREATE TABLE " <> table <> " (n int NOT NULL)")
        |> pog.execute(db)

      let assert Ok(admission) = grind.submit(jobs, job.new(recording, 7))
      grind.await(jobs, grind.handle(admission), within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(1)))

      // After Grind's claims, renewals and acknowledgement, the session
      // still has the application's search_path, and its unqualified table
      // resolves in public.
      one(db, "SELECT current_setting('search_path')", text_column())
      |> should.equal("\"$user\", public")
      one(
        db,
        "SELECT n.nspname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = '"
          <> table
          <> "'::regclass",
        text_column(),
      )
      |> should.equal("public")
      one(db, "SELECT count(*)::int FROM " <> table, int_column())
      |> should.equal(1)
      // Grind's own rows are in its schema.
      one(
        db,
        "SELECT count(*)::int FROM \"" <> schema <> "\".grind_jobs",
        int_column(),
      )
      |> should.equal(1)

      let assert Ok(_) = grind.stop(jobs)
      mark_database_test_executed("facade-shared-pool-search-path-passed")
    }
  }
}

pub fn facade_start_refuses_consumers_on_an_unmigrated_schema_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let id = int.to_string(unique_test_run_id())
      let schema = "facade_unmigrated_" <> id
      let echo_worker =
        worker.new(
          unique("unmigrated.echo"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n) },
        )
        |> worker.with_queue(unique("unmigrated-echo"))
      let config = fn() {
        grind.new(pool_config(url))
        |> grind.with_schema(schema)
        |> grind.with_worker(echo_worker)
      }
      let required = postgres.latest_schema_version()
      let name = process.new_name("unmigrated_grind")

      // Consumers would poll tables that do not exist: start refuses.
      let refused = grind.start(config(), name)
      refused
      |> should.equal(Error(grind.SchemaNotMigrated(found: None, required:)))
      let assert Error(error) = refused
      grind.describe_start_error(error)
      |> string.contains("grind.with_startup_migration")
      |> should.be_true
      grind.describe_start_error(error)
      |> string.contains("grind.migrate")
      |> should.be_true

      // So does a supervised child. A failed start exits its linked
      // caller, so it runs in a process of its own.
      let reply = process.new_subject()
      process.spawn_unlinked(fn() {
        process.trap_exits(True)
        supervisor.new(supervisor.OneForOne)
        |> supervisor.add(grind.supervised(config(), name))
        |> supervisor.start
        |> result.is_error
        |> process.send(reply, _)
      })
      process.receive(reply, within: 20_000) |> should.equal(Ok(True))

      // A runtime without consumers migrates at deploy time; then the
      // consumers start.
      let assert Ok(deploy) =
        grind.start(config() |> grind.without_consumers, name)
      let assert Ok(Nil) = grind.migrate(deploy)
      let assert Ok(_) = grind.stop(deploy)
      let assert Ok(jobs) = grind.start(config(), name)
      let assert Ok(admission) = grind.submit(jobs, job.new(echo_worker, 3))
      grind.await(jobs, grind.handle(admission), within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(3)))
      let assert Ok(_) = grind.stop(jobs)
      mark_database_test_executed("facade-start-refuses-unmigrated-passed")
    }
  }
}

pub fn facade_startup_migration_runs_before_the_consumers_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let schema = "facade_startup_" <> int.to_string(unique_test_run_id())
      let echo_worker =
        worker.new(
          unique("startup.echo"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n + 1) },
        )
        |> worker.with_queue(unique("startup-echo"))
      let config =
        grind.new(pool_config(url))
        |> grind.with_schema(schema)
        |> grind.with_worker(echo_worker)
        |> grind.with_startup_migration
      let name = process.new_name("startup_grind")
      let assert Ok(started) =
        supervisor.new(supervisor.OneForOne)
        |> supervisor.add(grind.supervised(config, name))
        |> supervisor.start
      let jobs = grind.named(name)
      let assert Ok(admission) = grind.submit(jobs, job.new(echo_worker, 1))
      grind.await(jobs, grind.handle(admission), within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(2)))
      // Migrating again is a no-op.
      grind.migrate(jobs) |> should.equal(Ok(Nil))
      let _ = stop_supervisor(started.pid)
      // The installed schema now satisfies a runtime without the option.
      let assert Ok(again) =
        grind.start(
          grind.new(pool_config(url))
            |> grind.with_schema(schema)
            |> grind.with_worker(echo_worker),
          process.new_name("startup_grind_again"),
        )
      let assert Ok(_) = grind.stop(again)
      mark_database_test_executed("facade-startup-migration-passed")
    }
  }
}

@external(erlang, "grind_postgres_ffi", "stop_supervisor")
fn stop_supervisor(pid: process.Pid) -> Result(Bool, Nil)
