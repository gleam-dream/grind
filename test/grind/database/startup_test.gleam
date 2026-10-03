import exception
import fault_proxy
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process
import gleam/int
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleeunit/should
import grind/internal/pool
import grind/internal/postgres
import grind/internal/store
import grind/support/env.{database_url, mark_database_test_executed}
import pog

@external(erlang, "grind_startup_probe", "resources")
fn resources() -> Dynamic

@external(erlang, "grind_startup_probe", "pool_cache_counts")
fn pool_cache_counts(connection: pog.Connection) -> #(Int, Int)

/// A startup lookup that loses its reply must release its entire tree before
/// returning an error. Retrying the identical settings uses the same pool name.
pub fn postgres_failed_installation_lookup_releases_resources_before_retry_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run_failed_installation_lookup_test(url)
  }
}

fn run_failed_installation_lookup_test(url: String) -> Nil {
  let assert Ok(config) =
    pog.url_config(process.new_name("startup_proxy_parse"), url)
  let assert Ok(#(proxy, port)) = fault_proxy.start(config.host, config.port)
  use <- exception.defer(fn() { fault_proxy.stop(proxy) })
  let proxy_url =
    "postgres://"
    <> config.user
    <> "@127.0.0.1:"
    <> int.to_string(port)
    <> "/"
    <> config.database
    <> "?sslmode=disable"
  let assert Ok(validated) =
    postgres.settings(proxy_url)
    |> postgres.with_pool_size(1)
    |> postgres.with_unique_lock_wait(100)
    |> postgres.with_statement_deadline(1500)
    |> postgres.validate
  let events = process.new_subject()
  let before = resources()
  fault_proxy.arm(
    proxy,
    fault_proxy.Armed(
      fault_proxy.OnSql("SELECT oid::int4 FROM pg_database"),
      fault_proxy.DropReply,
    ),
    events,
  )
  let assert Error(postgres.InstallationQueryFailed(_)) =
    postgres.start(validated)
  let assert Ok(_) = process.receive(events, 1000)
  resources() |> should.equal(before)
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  postgres.migrate(database) |> should.be_ok()
  mark_database_test_executed("startup-lookup-failure-releases-resources")
}

pub fn postgres_duplicate_start_preserves_live_pool_and_deadline_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(validated) =
        postgres.settings(url)
        |> postgres.with_pool_size(1)
        |> postgres.with_unique_lock_wait(100)
        |> postgres.with_statement_deadline(1500)
        |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let before = resources()
      let assert Error(postgres.PoolStartFailed(_)) = postgres.start(validated)
      resources() |> should.equal(before)
      postgres.migrate(database) |> should.be_ok()
      store.call_safely(postgres.connection(database), fn(connection) {
        pog.query("SELECT true FROM (SELECT pg_sleep(2.0)) AS deadline_probe")
        |> pog.execute(on: connection)
      })
      |> should.be_error()
      mark_database_test_executed("duplicate-start-preserves-live-pool")
    }
  }
}

/// The deadline owner has started when pog refuses a registered pool name.
/// OTP must unwind that owner; the unrelated registered process must survive.
pub fn postgres_pool_child_start_failure_releases_deadline_owner_test() {
  let name = process.new_name("grind_postgres_start_failure")
  let assert Ok(blocker) = actor.new(Nil) |> actor.named(name) |> actor.start
  use <- exception.defer(fn() {
    process.unlink(blocker.pid)
    process.kill(blocker.pid)
  })
  let config = pog.default_config(name)
  let before = resources()
  let root =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pool.supervised(config, 1500))
  pool.start_unlinked(fn() { static_supervisor.start(root) })
  |> should.be_error()
  resources() |> should.equal(before)
}

/// A later sibling can fail after the owned pool has successfully started.
/// This is the same OTP unwind path as any forwarder initialization failure.
pub fn postgres_later_child_start_failure_releases_pool_and_deadline_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(config) =
        pog.url_config(process.new_name("grind_postgres_later_failure"), url)
      let before = resources()
      let root =
        static_supervisor.new(static_supervisor.OneForOne)
        |> static_supervisor.add(pool.supervised(config, 1500))
        |> static_supervisor.add(
          supervision.worker(fn() {
            Error(actor.InitFailed(
              "injected later child initialization failure",
            ))
          }),
        )
      pool.start_unlinked(fn() { static_supervisor.start(root) })
      |> should.be_error()
      resources() |> should.equal(before)
      mark_database_test_executed("later-start-failure-releases-resources")
    }
  }
}

pub fn postgres_close_cleans_own_type_cache_and_preserves_live_sibling_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(first_settings) =
        postgres.settings(url) |> postgres.validate
      let assert Ok(second_settings) =
        postgres.settings(url) |> postgres.validate
      let assert Ok(first) = postgres.start(first_settings)
      use <- exception.defer(fn() { postgres.close(first) })
      let assert Ok(_) = postgres.migrate(first)
      let before = resources()
      let assert Ok(second) = postgres.start(second_settings)
      let assert Ok(_) = postgres.migrate(second)
      let assert Ok(_) = postgres.close(second)
      resources() |> should.equal(before)
      // Existing handles and a same-name restart both retain working decoders.
      postgres.migrate(first) |> should.be_ok()
      let assert Ok(reopened) = postgres.start(second_settings)
      let assert Ok(_) = postgres.migrate(reopened)
      let assert Ok(_) = postgres.close(reopened)
      resources() |> should.equal(before)
      postgres.migrate(first) |> should.be_ok()
      mark_database_test_executed("closed-pool-type-cache-owned-cleanup")
    }
  }
}

pub fn postgres_reserved_pool_stop_cleans_queried_caches_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(settings) = postgres.settings(url) |> postgres.validate
      let assert Ok(database) = postgres.start(settings)
      use <- exception.defer(fn() { postgres.close(database) })
      let assert Ok(_) = postgres.migrate(database)
      let before = resources()
      let name = process.new_name("grind_postgres_reserved_probe")
      let config = postgres.renewal_pool_config(database, name)
      let root =
        static_supervisor.new(static_supervisor.OneForOne)
        |> static_supervisor.add(pool.supervised(config, 1500))
      let assert Ok(started) =
        pool.start_unlinked(fn() { static_supervisor.start(root) })
      let assert Ok(_) =
        store.call_safely(pog.named_connection(name), fn(connection) {
          pog.query("SELECT 1234 AS owned_renewal_cache_probe")
          |> pog.execute(on: connection)
        })
      let #(types, queries) = pool_cache_counts(pog.named_connection(name))
      { types > 0 && queries > 0 } |> should.equal(True)
      pool.abort_start(started.pid)
      resources() |> should.equal(before)
      postgres.migrate(database) |> should.be_ok()
      mark_database_test_executed("reserved-pool-cache-owned-cleanup")
    }
  }
}

@external(erlang, "grind_startup_probe", "late_type_writer_drain")
fn late_type_writer_drain(
  connection: pog.Connection,
  close: fn() -> Result(Nil, postgres.CloseError),
) -> Bool

@external(erlang, "grind_startup_probe", "late_query_writer_drain")
fn late_query_writer_drain(
  connection: pog.Connection,
  close: fn() -> Result(Nil, postgres.CloseError),
  kill_caller: Bool,
) -> Bool

pub fn postgres_close_waits_for_internal_type_writer_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(settings) = postgres.settings(url) |> postgres.validate
      let assert Ok(database) = postgres.start(settings)
      use <- exception.defer(fn() { postgres.close(database) })
      late_type_writer_drain(postgres.connection(database), fn() {
        postgres.close(database)
      })
      |> should.equal(True)
      mark_database_test_executed("close-waits-internal-type-writer")
    }
  }
}

pub fn postgres_close_waits_for_managed_query_cache_writer_test() {
  run_query_writer_drain(False, "close-waits-managed-query-cache-writer")
}

pub fn postgres_close_releases_dead_managed_caller_test() {
  run_query_writer_drain(True, "close-releases-dead-managed-caller")
}

fn run_query_writer_drain(kill_caller: Bool, marker: String) {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(settings) = postgres.settings(url) |> postgres.validate
      let assert Ok(database) = postgres.start(settings)
      use <- exception.defer(fn() { postgres.close(database) })
      late_query_writer_drain(
        postgres.connection(database),
        fn() { postgres.close(database) },
        kill_caller,
      )
      |> should.equal(True)
      mark_database_test_executed(marker)
    }
  }
}
