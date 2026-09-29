import exception
import gleam/erlang/process
import gleam/list
import gleam/otp/static_supervisor
import gleeunit/should
import grind/internal/pool
import grind/internal/store
import grind/support/env.{database_url, mark_database_test_executed}
import pog

type Counter

@external(erlang, "grind_measured_probe", "counter")
fn counter() -> Counter

@external(erlang, "grind_measured_probe", "increment")
fn increment(counter: Counter) -> Nil

@external(erlang, "grind_measured_probe", "count")
fn count(counter: Counter) -> Int

@external(erlang, "grind_measured_probe", "with_contention")
fn with_contention(
  connection: pog.Connection,
  hold_ms: Int,
  run: fn() -> a,
) -> #(a, Int)

@external(erlang, "grind_measured_probe", "exceptions_preserved")
fn exceptions_preserved(connection: pog.Connection) -> Bool

@external(erlang, "grind_reconnect_probe", "stale_holder_queries")
fn stale_holder_queries(
  connection: pog.Connection,
  kill_owner: Bool,
  query: fn() -> Bool,
) -> List(Bool)

/// A real, occupied connection separates waiting for the pool from running SQL.
pub fn postgres_measured_checkout_separates_wait_from_query_test() {
  use connection <- with_pool(2000)
  let calls = counter()
  let #(measured, held_us) =
    with_contention(connection, 200, fn() {
      store.call_measured(connection, fn(checked_out) {
        increment(calls)
        pog.query("SELECT 1 FROM pg_sleep(0.1)") |> pog.execute(on: checked_out)
      })
    })
  measured.value |> should.be_ok()
  count(calls) |> should.equal(1)
  let assert store.CheckoutTiming(wait_us:, candidates:, outcome:) =
    measured.checkout
  outcome |> should.equal(store.CheckedOut)
  candidates |> should.equal(1)
  { wait_us >= held_us } |> should.be_true()
  { measured.call_duration_us - wait_us >= 90_000 } |> should.be_true()
  store.execute_measured(pog.query("SELECT 1"), on: connection).value
  |> should.be_ok()
  mark_database_test_executed("measured-checkout-contention")
}

pub fn postgres_measured_checkout_does_not_reset_deadline_after_wait_test() {
  use connection <- with_pool(250)
  let calls = counter()
  let #(measured, held_us) =
    with_contention(connection, 600, fn() {
      store.call_measured(connection, fn(checked_out) {
        increment(calls)
        pog.query("SELECT 1") |> pog.execute(on: checked_out)
      })
    })
  measured.value |> should.equal(Error(pog.ConnectionUnavailable))
  count(calls) |> should.equal(0)
  let assert store.CheckoutTiming(wait_us:, candidates:, outcome:) =
    measured.checkout
  outcome |> should.equal(store.CheckoutUnavailable)
  candidates |> should.equal(1)
  { wait_us >= held_us && wait_us > 250_000 } |> should.be_true()
  { measured.call_duration_us >= wait_us } |> should.be_true()
  mark_database_test_executed("measured-checkout-original-deadline")
}

pub fn measured_missing_owner_never_invokes_callback_test() {
  let connection =
    process.new_name("grind_measured_missing_owner") |> pog.named_connection()
  let calls = counter()
  let measured =
    store.call_measured(connection, fn(checked_out) {
      increment(calls)
      pog.query("SELECT 1") |> pog.execute(on: checked_out)
    })
  measured.value |> should.equal(Error(pog.ConnectionUnavailable))
  measured.checkout
  |> should.equal(store.CheckoutTiming(0, 0, store.CheckoutUnavailable))
  count(calls) |> should.equal(0)
  { measured.call_duration_us >= 0 } |> should.be_true()
}

pub fn postgres_measured_nested_calls_and_rollback_preserve_results_test() {
  use connection <- with_pool(2000)
  let calls = counter()
  let measured =
    store.transaction_measured(connection, fn(transaction) {
      increment(calls)
      let nested =
        store.execute_measured(pog.query("SELECT 1"), on: transaction)
      nested.value |> should.be_ok()
      nested.checkout |> should.equal(store.NoCheckout)
      let nested =
        store.call_measured(transaction, fn(checked_out) {
          pog.query("SELECT 2") |> pog.execute(on: checked_out)
        })
      nested.value |> should.be_ok()
      nested.checkout |> should.equal(store.NoCheckout)
      Ok(781)
    })
  measured.value |> should.equal(Ok(781))
  count(calls) |> should.equal(1)
  let assert store.CheckoutTiming(outcome: store.CheckedOut, candidates: 1, ..) =
    measured.checkout
  let rollback = fn(transaction) {
    store.execute_safely(pog.query("SELECT 3"), on: transaction)
    |> should.be_ok()
    Error("rollback sentinel")
  }
  let legacy = store.transaction_safely(connection, rollback)
  let measured = store.transaction_measured(connection, rollback)
  measured.value |> should.equal(legacy)
  measured.value
  |> should.equal(Error(pog.TransactionRolledBack("rollback sentinel")))
  let assert store.CheckoutTiming(outcome: store.CheckedOut, candidates: 1, ..) =
    measured.checkout
  store.execute_measured(pog.query("SELECT 4"), on: connection).value
  |> should.be_ok()
  mark_database_test_executed("measured-checkout-nested-and-rollback")
}

pub fn postgres_measured_callback_exception_preserves_stack_and_cleanup_test() {
  use connection <- with_pool(2000)
  exceptions_preserved(connection) |> should.be_true()
  mark_database_test_executed("measured-checkout-exception-cleanup")
}

/// The existing real reconnect fixture leaves one closed holder ahead of its
/// usable replacement. Admission retries count; an entered callback does not.
pub fn postgres_measured_checkout_counts_stale_candidates_without_retrying_work_test() {
  use connection <- with_pool(2000)
  let calls = counter()
  let results =
    stale_holder_queries(connection, False, fn() {
      let measured =
        store.call_measured(connection, fn(checked_out) {
          increment(calls)
          pog.query("SELECT 913 AS measured_stale_holder")
          |> pog.execute(on: checked_out)
          |> should.be_ok()
          // Preserve a callback's error after SQL; never repeat its work.
          Error(pog.QueryTimeout)
        })
      measured.value |> should.equal(Error(pog.QueryTimeout))
      let assert store.CheckoutTiming(wait_us:, candidates:, outcome:) =
        measured.checkout
      outcome |> should.equal(store.CheckedOut)
      candidates
      |> should.equal(case count(calls) == 1 {
        True -> 2
        False -> 1
      })
      { wait_us >= 0 && measured.call_duration_us >= wait_us }
      |> should.be_true()
      True
    })
  list.length(results) |> should.equal(6)
  list.all(results, fn(ok) { ok }) |> should.be_true()
  count(calls) |> should.equal(6)
  mark_database_test_executed("measured-checkout-stale-candidates")
}

fn with_pool(deadline_ms: Int, run: fn(pog.Connection) -> Nil) -> Nil {
  case database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let name = process.new_name("grind_measured_checkout_probe")
      let assert Ok(config) = pog.url_config(name, url)
      // Let the test's explicit release settle contention. This does not
      // change Grind's deadline or pgo's checkout implementation.
      let config =
        pog.Config(
          ..config,
          pool_size: 1,
          idle_interval: 60_000,
          queue_target: 10_000,
          queue_interval: 10_000,
        )
      let root =
        static_supervisor.new(static_supervisor.OneForOne)
        |> static_supervisor.add(pool.supervised(config, deadline_ms))
      let assert Ok(started) =
        pool.start_unlinked(fn() { static_supervisor.start(root) })
      use <- exception.defer(fn() { pool.abort_start(started.pid) })
      let connection = pog.named_connection(name)
      store.execute_safely(pog.query("SELECT 1"), on: connection)
      |> should.be_ok()
      run(connection)
    }
  }
}
