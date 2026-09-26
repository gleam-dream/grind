//// Fault-proxy tests (docs/RELEASE-READINESS.md, "Acknowledgement deadline").
////
//// These sit a real TCP relay (`grind_fault_proxy.erl`, bound through
//// `fault_proxy.gleam`) between a Grind `Database` and the disposable test
//// PostgreSQL cluster, so a `COMMIT`/`BEGIN`/renewal `UPDATE` can be turned
//// into a genuine half-open socket fault (never closed, either silently
//// dropped in one direction or forwarded-then-blackholed in the other) —
//// not a `pg_terminate_backend`-style server-initiated close, which the
//// existing `postgres_ack_committed_reply_lost_*` tests already cover.
////
//// `fault_proxy_pass_through_test` proves the harness itself works (plain
//// traffic through the proxy is byte-for-byte transparent to pog/PostgreSQL)
//// before any test relies on it to prove something about Grind.
////
//// T1/T2/T4/T5 print how long the affected call actually took, in
//// milliseconds, so `docs/RECOVERY-EVIDENCE.md` can quote real numbers, and
//// also assert `elapsed < 2 * postgres.statement_deadline_ms(database)` —
//// tight enough to fail if the fix regressed to an unbounded wait, loose
//// enough to tolerate ordinary scheduling/GC jitter around one deadline
//// window, and derived from the pool's own configured deadline rather than
//// a hardcoded constant so it tracks a caller-chosen `statement_deadline`
//// instead of silently passing regardless of it (see the mutation evidence
//// in `docs/RECOVERY-EVIDENCE.md`).

import exception
import fault_proxy
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleam/string
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/worker
import pog

@external(erlang, "grind_test_env", "fault_proxy_url")
fn fault_proxy_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "mark_database_test_executed")
fn mark_database_test_executed(contract: String) -> Nil

@external(erlang, "grind_test_env", "monotonic_ms")
fn monotonic_ms() -> Int

/// Starts a proxy in front of `base_url`'s real host/port and returns the
/// handle plus a database URL pointing at the proxy instead.
fn start_proxy_for(base_url: String) -> #(fault_proxy.Proxy, String) {
  let assert Ok(config) =
    pog.url_config(process.new_name("grind_fault_proxy_parse"), base_url)
  let assert Ok(#(proxy, proxy_port)) =
    fault_proxy.start(config.host, config.port)
  let url =
    "postgres://"
    <> config.user
    <> "@127.0.0.1:"
    <> int.to_string(proxy_port)
    <> "/"
    <> config.database
    <> "?sslmode=disable"
  #(proxy, url)
}

fn wait_for_job_state(
  database: postgres.Database,
  handle: job.JobHandle(input, output, error),
  target: job.State,
  budget_ms: Int,
) -> Bool {
  case postgres.state(database, handle) {
    Ok(state) if state == target -> True
    _ ->
      case budget_ms > 0 {
        False -> False
        True -> {
          process.sleep(20)
          wait_for_job_state(database, handle, target, budget_ms - 20)
        }
      }
  }
}

fn wait_for_idle_in_transaction_backend(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND state = 'idle in transaction' ORDER BY query_start DESC LIMIT 1",
    )
    |> pog.returning({
      use pid <- decode.field(0, decode.int)
      decode.success(pid)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      case returned.rows {
        [pid] -> Ok(pid)
        [] ->
          case checks_remaining > 0 {
            False -> Error(Nil)
            True -> {
              process.sleep(25)
              wait_for_idle_in_transaction_backend(
                connection,
                checks_remaining - 1,
              )
            }
          }
        _ -> Error(Nil)
      }
  }
}

fn terminate_backend(connection: pog.Connection, pid: Int) -> Bool {
  pog.query("SELECT pg_terminate_backend($1)")
  |> pog.parameter(pog.int(pid))
  |> pog.returning({
    use terminated <- decode.field(0, decode.bool)
    decode.success(terminated)
  })
  |> pog.execute(on: connection)
  |> fn(result) {
    case result {
      Ok(returned) ->
        case returned.rows {
          [terminated] -> terminated
          _ -> False
        }
      Error(_) -> False
    }
  }
}

fn backend_pid_is_alive(connection: pog.Connection, pid: Int) -> Bool {
  let query =
    pog.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
    |> pog.parameter(pog.int(pid))
    |> pog.returning({
      use alive <- decode.field(0, decode.bool)
      decode.success(alive)
    })
  case pog.execute(query, on: connection) {
    Error(_) -> True
    Ok(returned) ->
      case returned.rows {
        [alive] -> alive
        _ -> True
      }
  }
}

fn wait_for_backend_gone(
  connection: pog.Connection,
  pid: Int,
  checks_remaining: Int,
) -> Nil {
  case backend_pid_is_alive(connection, pid) {
    False -> Nil
    True ->
      case checks_remaining > 0 {
        False -> Nil
        True -> {
          process.sleep(20)
          wait_for_backend_gone(connection, pid, checks_remaining - 1)
        }
      }
  }
}

/// The observer backstop the plan calls for: finds and kills whatever real
/// backend a dropped request left idle in transaction, so a retry that
/// needs the row lock it was holding is not left waiting on
/// `idle_in_transaction_session_timeout` (or forever, before that setting
/// exists). A no-op if nothing is stuck (already cleared, or the server-side
/// timeout already got there first).
fn clear_stuck_backend(observer_connection: pog.Connection) -> Nil {
  case wait_for_idle_in_transaction_backend(observer_connection, 200) {
    Ok(pid) -> {
      terminate_backend(observer_connection, pid) |> should.equal(True)
      wait_for_backend_gone(observer_connection, pid, 300)
    }
    Error(Nil) -> Nil
  }
}

fn retry_ack_until(
  database: postgres.Database,
  queue_name: String,
  attempt_owner: String,
  claimed: postgres.ClaimedJob,
  execution: worker.Execution,
  remaining: Int,
) -> Result(Bool, postgres.QueueRunError) {
  let result =
    postgres.acknowledge_claim(
      database,
      queue_name,
      attempt_owner,
      claimed,
      execution,
    )
  case result, remaining > 0 {
    Ok(True), _ -> result
    _, False -> result
    _, True -> {
      process.sleep(50)
      retry_ack_until(
        database,
        queue_name,
        attempt_owner,
        claimed,
        execution,
        remaining - 1,
      )
    }
  }
}

/// Proves the harness itself works before any test relies on it: a plain
/// `Database` behind the proxy can connect, authenticate, and run the full
/// multi-statement, multi-transaction `migrate` — byte-for-byte transparent
/// relaying, not just "a socket accepted a connection".
pub fn fault_proxy_pass_through_test() {
  case fault_proxy_url() {
    Error(Nil) -> Nil
    Ok(base_url) -> {
      let #(proxy, url) = start_proxy_for(base_url)
      use <- exception.defer(fn() { fault_proxy.stop(proxy) })
      let pool_name = process.new_name("grind_fault_proxy_pass_through")
      let assert Ok(validated) =
        postgres.settings(url, pool_name) |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let assert Ok(Nil) = postgres.migrate(database)
      let assert Ok(input_codec) =
        worker.codec("fp-pass-through-input-v1", json.int, decode.int)
      let assert Ok(output_codec) =
        worker.codec("fp-pass-through-output-v1", json.string, decode.string)
      let assert Ok(definition) =
        worker.define(
          "fault.proxy.pass.through",
          "v1",
          input_codec,
          output_codec,
          fn(value) { Ok("pass-through-" <> int.to_string(value)) },
        )
      let assert Ok(workers) = registry.new("fault-proxy-pass-through")
      let assert Ok(workers) = registry.register(workers, definition)
      let assert Ok(handle) =
        postgres.submit(database, "fault-proxy-pass-through", definition, 5)
      let assert Ok(consumer) = queue.start_manual(database, workers)
      use <- exception.defer(fn() {
        let _ = queue.stop(consumer)
        Nil
      })
      queue.process_one(consumer) |> should.equal(Ok(True))
      postgres.outcome(database, handle)
      |> should.equal(Ok(job.SucceededWith("pass-through-5")))
      mark_database_test_executed("fault-proxy-pass-through-passed")
    }
  }
}

/// T1: `drop_reply` on a manual acknowledgement's own `COMMIT`. The request
/// genuinely reaches PostgreSQL and commits; only the reply is lost, on a
/// socket that is never closed. Prints how long `acknowledge_claim` actually
/// took to return, then reconciles the lost reply from the durable receipt.
pub fn fault_proxy_t1_drop_reply_test() {
  case fault_proxy_url() {
    Error(Nil) -> Nil
    Ok(base_url) -> run_t1(base_url)
  }
}

fn run_t1(base_url: String) -> Nil {
  let #(proxy, url) = start_proxy_for(base_url)
  use <- exception.defer(fn() { fault_proxy.stop(proxy) })
  let pool_name = process.new_name("grind_fault_proxy_t1")
  let assert Ok(validated) =
    postgres.settings(url, pool_name)
    |> postgres.pool_size(1)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let deadline_ms = postgres.statement_deadline_ms(database)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fp-t1-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fp-t1-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("fault.proxy.t1", "v1", input_codec, output_codec, fn(value) {
      Ok("t1-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("fault-proxy-t1")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "fault-proxy-t1", definition, 7)
  let attempt_owner = "fault-proxy-t1-owner"
  let assert Ok(Some(claimed)) =
    postgres.claim_one(
      database,
      "fault-proxy-t1",
      workers,
      attempt_owner,
      30_000,
    )
  let execution = postgres.execute_claim(claimed)
  let job_id = job.id_value(handle)
  let #(_, attempt_id, epoch) = postgres.claim_identity(claimed)
  let command_id =
    postgres.acknowledgement_command_id(job_id, attempt_id, epoch)

  let notify = process.new_subject()
  fault_proxy.arm(
    proxy,
    fault_proxy.Armed(fault_proxy.OnCommit, fault_proxy.DropReply),
    notify,
  )

  let reply = process.new_subject()
  let start_ms = monotonic_ms()
  let _ =
    process.spawn_unlinked(fn() {
      let ack_result =
        postgres.acknowledge_claim(
          database,
          "fault-proxy-t1",
          attempt_owner,
          claimed,
          execution,
        )
      process.send(reply, ack_result)
    })
  let assert Ok(fault_proxy.CommitSeen(_, _)) =
    process.receive(notify, within: 3000)

  // Bounded by roughly `deadline_ms` (the checkout deadline armed at the
  // ack's own `BEGIN`), not by this call's own generous outer wait: assert
  // `elapsed < 2 * deadline_ms`, not merely "returned at all before 20000ms"
  // — see the mutation evidence in docs/RECOVERY-EVIDENCE.md proving this
  // bound is not vacuous (a deliberately widened `statement_deadline`
  // widens the observed `elapsed` proportionally, and a hardcoded bound
  // that failed to track it would go red).
  case process.receive(reply, within: 20_000) {
    Ok(Ok(True)) -> {
      let elapsed = monotonic_ms() - start_ms
      io.println(
        "T1 drop_reply manual ack: bounded, acknowledge_claim itself returned Ok(True) after "
        <> int.to_string(elapsed)
        <> " ms (no reconciliation needed)",
      )
      { elapsed < 2 * deadline_ms } |> should.equal(True)
    }
    Ok(Error(postgres.QueueAckUnknown(returned_command_id, _))) -> {
      let elapsed = monotonic_ms() - start_ms
      io.println(
        "T1 drop_reply manual ack: bounded, acknowledge_claim returned QueueAckUnknown after "
        <> int.to_string(elapsed)
        <> " ms",
      )
      { elapsed < 2 * deadline_ms } |> should.equal(True)
      returned_command_id |> should.equal(command_id)
      let assert Ok(postgres.AcknowledgementReceipt(committed_state:, ..)) =
        postgres.reconcile_acknowledgement(database, handle, command_id)
      committed_state |> should.equal(job.Succeeded)
    }
    Ok(other) ->
      panic as { "T1: unexpected ack result " <> string.inspect(other) }
    Error(Nil) -> {
      io.println(
        "T1 drop_reply manual ack: UNBOUNDED — acknowledge_claim did not return within 20000 ms",
      )
      panic as "T1: acknowledge_claim never returned; see stdout for the unbounded finding"
    }
  }
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("t1-7")))
  mark_database_test_executed("fault-proxy-t1-observed")
}

/// T2: `drop_request` on a manual acknowledgement's own `COMMIT`. PostgreSQL
/// never sees the statement at all and is left idle in transaction, holding
/// the row's lock, until the server-side timeout (or this test's observer
/// backstop) clears it. Prints how long `acknowledge_claim` itself took,
/// confirms no receipt exists (nothing ever committed), then retries the
/// identical acknowledgement once the lock is free.
pub fn fault_proxy_t2_drop_request_test() {
  case fault_proxy_url() {
    Error(Nil) -> Nil
    Ok(base_url) -> run_t2(base_url)
  }
}

fn run_t2(base_url: String) -> Nil {
  let #(proxy, url) = start_proxy_for(base_url)
  use <- exception.defer(fn() { fault_proxy.stop(proxy) })

  let observer_pool_name = process.new_name("grind_fault_proxy_t2_observer")
  let assert Ok(observer_validated) =
    postgres.settings(base_url, observer_pool_name)
    |> postgres.pool_size(2)
    |> postgres.validate
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = pog.named_connection(observer_pool_name)

  let pool_name = process.new_name("grind_fault_proxy_t2")
  let assert Ok(validated) =
    postgres.settings(url, pool_name)
    |> postgres.pool_size(1)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let deadline_ms = postgres.statement_deadline_ms(database)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fp-t2-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fp-t2-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("fault.proxy.t2", "v1", input_codec, output_codec, fn(value) {
      Ok("t2-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("fault-proxy-t2")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "fault-proxy-t2", definition, 9)
  let attempt_owner = "fault-proxy-t2-owner"
  let assert Ok(Some(claimed)) =
    postgres.claim_one(
      database,
      "fault-proxy-t2",
      workers,
      attempt_owner,
      30_000,
    )
  let execution = postgres.execute_claim(claimed)
  let job_id = job.id_value(handle)
  let #(_, attempt_id, epoch) = postgres.claim_identity(claimed)
  let command_id =
    postgres.acknowledgement_command_id(job_id, attempt_id, epoch)

  let notify = process.new_subject()
  fault_proxy.arm(
    proxy,
    fault_proxy.Armed(fault_proxy.OnCommit, fault_proxy.DropRequest),
    notify,
  )

  let reply = process.new_subject()
  let start_ms = monotonic_ms()
  let _ =
    process.spawn_unlinked(fn() {
      let ack_result =
        postgres.acknowledge_claim(
          database,
          "fault-proxy-t2",
          attempt_owner,
          claimed,
          execution,
        )
      process.send(reply, ack_result)
    })
  let assert Ok(fault_proxy.CommitSeen(_, _)) =
    process.receive(notify, within: 3000)

  // Bounded by the checkout deadline armed at the ack's own `BEGIN` (the
  // request itself was dropped, but that arms exactly like T1's dropped
  // reply) — a genuine `Error(Nil)` here (no reply within the outer 20000ms
  // wait) is now a hard failure, not merely a printed observation: after
  // this increment's fix, this call must always be bounded on its own,
  // never relying on the observer backstop below just to *return*.
  case process.receive(reply, within: 20_000) {
    Ok(result) -> {
      let elapsed = monotonic_ms() - start_ms
      io.println(
        "T2 drop_request manual ack: bounded, acknowledge_claim returned "
        <> string.inspect(result)
        <> " after "
        <> int.to_string(elapsed)
        <> " ms",
      )
      { elapsed < 2 * deadline_ms } |> should.equal(True)
    }
    Error(Nil) -> {
      io.println(
        "T2 drop_request manual ack: UNBOUNDED — acknowledge_claim did not return within 20000 ms",
      )
      panic as "T2: acknowledge_claim never returned; see stdout for the unbounded finding"
    }
  }

  // Nothing ever reached the server, so nothing ever committed.
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(Error(postgres.AckReceiptNotFound))

  clear_stuck_backend(observer_connection)

  retry_ack_until(
    database,
    "fault-proxy-t2",
    attempt_owner,
    claimed,
    execution,
    60,
  )
  |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("t2-9")))
  mark_database_test_executed("fault-proxy-t2-observed")
}

/// T4: the coordinator's own lease-renewal `UPDATE`, `drop_reply`'d. Not
/// wrapped in a transaction — a single autocommit statement through the pool.
pub fn fault_proxy_t4_renewal_drop_reply_test() {
  case fault_proxy_url() {
    Error(Nil) -> Nil
    Ok(base_url) -> run_t4(base_url)
  }
}

fn run_t4(base_url: String) -> Nil {
  let #(proxy, url) = start_proxy_for(base_url)
  use <- exception.defer(fn() { fault_proxy.stop(proxy) })
  let pool_name = process.new_name("grind_fault_proxy_t4")
  let assert Ok(validated) =
    postgres.settings(url, pool_name)
    |> postgres.pool_size(1)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let deadline_ms = postgres.statement_deadline_ms(database)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fp-t4-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fp-t4-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("fault.proxy.t4", "v1", input_codec, output_codec, fn(value) {
      Ok("t4-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("fault-proxy-t4")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(_handle) =
    postgres.submit(database, "fault-proxy-t4", definition, 3)
  let attempt_owner = "fault-proxy-t4-owner"
  let assert Ok(Some(claimed)) =
    postgres.claim_one(
      database,
      "fault-proxy-t4",
      workers,
      attempt_owner,
      30_000,
    )

  let notify = process.new_subject()
  fault_proxy.arm(
    proxy,
    fault_proxy.Armed(
      fault_proxy.OnSql("UPDATE grind_jobs SET lease_expires_at"),
      fault_proxy.DropReply,
    ),
    notify,
  )

  let reply = process.new_subject()
  let start_ms = monotonic_ms()
  let _ =
    process.spawn_unlinked(fn() {
      let renew_result =
        postgres.renew_claim(
          database,
          "fault-proxy-t4",
          attempt_owner,
          claimed,
          30_000,
        )
      process.send(reply, renew_result)
    })
  let assert Ok(fault_proxy.CommitSeen(_, _)) =
    process.receive(notify, within: 3000)

  case process.receive(reply, within: 20_000) {
    Ok(result) -> {
      let elapsed = monotonic_ms() - start_ms
      io.println(
        "T4 renewal drop_reply: bounded, renew_claim returned "
        <> string.inspect(result)
        <> " after "
        <> int.to_string(elapsed)
        <> " ms",
      )
      { elapsed < 2 * deadline_ms } |> should.equal(True)
      result
      |> should.equal(Error(postgres.QueueClaimFailed(pog.QueryTimeout)))
    }
    Error(Nil) -> {
      io.println(
        "T4 renewal drop_reply: UNBOUNDED — renew_claim did not return within 20000 ms",
      )
      panic as "T4: renew_claim never returned; see stdout for the unbounded finding"
    }
  }
  mark_database_test_executed("fault-proxy-t4-observed")
}

/// T5: `drop_reply` on a manual acknowledgement's own implicit `BEGIN`
/// (pog's `pog.transaction` sends `begin` as the transaction's first
/// statement on the connection it just checked out) — the same checkout
/// holds through `BEGIN`, so any client-side bound armed at checkout time
/// applies here exactly as it does to `COMMIT` in T1.
pub fn fault_proxy_t5_begin_drop_reply_test() {
  case fault_proxy_url() {
    Error(Nil) -> Nil
    Ok(base_url) -> run_t5(base_url)
  }
}

fn run_t5(base_url: String) -> Nil {
  let #(proxy, url) = start_proxy_for(base_url)
  use <- exception.defer(fn() { fault_proxy.stop(proxy) })
  let pool_name = process.new_name("grind_fault_proxy_t5")
  let assert Ok(validated) =
    postgres.settings(url, pool_name)
    |> postgres.pool_size(1)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let deadline_ms = postgres.statement_deadline_ms(database)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fp-t5-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fp-t5-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("fault.proxy.t5", "v1", input_codec, output_codec, fn(value) {
      Ok("t5-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("fault-proxy-t5")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "fault-proxy-t5", definition, 11)
  let attempt_owner = "fault-proxy-t5-owner"
  let assert Ok(Some(claimed)) =
    postgres.claim_one(
      database,
      "fault-proxy-t5",
      workers,
      attempt_owner,
      30_000,
    )
  let execution = postgres.execute_claim(claimed)
  let job_id = job.id_value(handle)
  let #(_, attempt_id, epoch) = postgres.claim_identity(claimed)
  let command_id =
    postgres.acknowledgement_command_id(job_id, attempt_id, epoch)

  let notify = process.new_subject()
  fault_proxy.arm(
    proxy,
    fault_proxy.Armed(fault_proxy.OnBegin, fault_proxy.DropReply),
    notify,
  )

  let reply = process.new_subject()
  let start_ms = monotonic_ms()
  let _ =
    process.spawn_unlinked(fn() {
      let ack_result =
        postgres.acknowledge_claim(
          database,
          "fault-proxy-t5",
          attempt_owner,
          claimed,
          execution,
        )
      process.send(reply, ack_result)
    })
  let assert Ok(fault_proxy.CommitSeen(_, _)) =
    process.receive(notify, within: 3000)

  // Unlike T1's dropped COMMIT reply, a dropped BEGIN reply means pog's
  // `transaction_layer` never even calls the callback (the `use _ <-
  // result.try(do(conn, "begin"))` short-circuits first) — no acknowledgement
  // was ever attempted, so this must come back `QueueAckUnknown` with no
  // receipt and the job left `Executing`, never `QueueAckFailed` (pog does
  // not expose which statement inside the transaction actually failed, so
  // Grind conservatively treats every `TransactionQueryError` alike — see
  // `docs/RECOVERY-EVIDENCE.md`).
  case process.receive(reply, within: 20_000) {
    Ok(Error(postgres.QueueAckUnknown(returned_command_id, _))) -> {
      let elapsed = monotonic_ms() - start_ms
      io.println(
        "T5 BEGIN drop_reply: bounded, acknowledge_claim returned QueueAckUnknown after "
        <> int.to_string(elapsed)
        <> " ms (BEGIN itself never got a reply, so the callback never ran)",
      )
      { elapsed < 2 * deadline_ms } |> should.equal(True)
      returned_command_id |> should.equal(command_id)
    }
    Ok(other) ->
      panic as { "T5: unexpected ack result " <> string.inspect(other) }
    Error(Nil) -> {
      io.println(
        "T5 BEGIN drop_reply: UNBOUNDED — acknowledge_claim did not return within 20000 ms",
      )
      panic as "T5: acknowledge_claim never returned; see stdout for the unbounded finding"
    }
  }
  postgres.reconcile_acknowledgement(database, handle, command_id)
  |> should.equal(Error(postgres.AckReceiptNotFound))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  // The client-side forced disconnect (once armed) closes the relay's
  // upstream socket too, so the real backend's own open `BEGIN` rolls back
  // on connection loss — no observer backstop needed here. A fresh
  // acknowledgement attempt on a healthy connection now succeeds normally.
  postgres.acknowledge_claim(
    database,
    "fault-proxy-t5",
    attempt_owner,
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("t5-11")))
  mark_database_test_executed("fault-proxy-t5-observed")
}

/// T3: automatic consumer, `maximum_concurrency: 2`. A's own acknowledgement
/// `COMMIT` is `drop_request`'d; B runs on a sibling attempt under the same
/// coordinator. B's own renewal must survive the stall A's stuck ack call
/// causes on the coordinator's single message loop (README, "The
/// coordinator runs claim and acknowledgement SQL synchronously"), and A
/// must eventually converge to `Succeeded` through the automatic
/// pending-ack retry (queue.gleam), not get stuck `executing` forever.
pub fn fault_proxy_t3_concurrent_divergence_test() {
  case fault_proxy_url() {
    Error(Nil) -> Nil
    Ok(base_url) -> run_t3(base_url)
  }
}

type T3Started {
  T3Started(name: String, release: process.Subject(Nil))
}

fn run_t3(base_url: String) -> Nil {
  let #(proxy, url) = start_proxy_for(base_url)
  use <- exception.defer(fn() { fault_proxy.stop(proxy) })

  let observer_pool_name = process.new_name("grind_fault_proxy_t3_observer")
  let assert Ok(observer_validated) =
    postgres.settings(base_url, observer_pool_name)
    |> postgres.pool_size(2)
    |> postgres.validate
  let assert Ok(observer) = postgres.start(observer_validated)
  use <- exception.defer(fn() { postgres.close(observer) })
  let observer_connection = pog.named_connection(observer_pool_name)

  let pool_name = process.new_name("grind_fault_proxy_t3")
  let assert Ok(validated) =
    postgres.settings(url, pool_name)
    |> postgres.pool_size(3)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let assert Ok(input_codec) =
    worker.codec("fp-t3-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fp-t3-output-v1", json.string, decode.string)
  let started = process.new_subject()
  // Each worker's own release subject is created *inside* the worker
  // closure, in the worker actor's own process — a `Subject` can only be
  // `receive`d by the process that created it (`gleam/erlang/process`), so
  // a subject created ahead of time in the test process and closed over
  // here would crash the worker with "Cannot receive with a subject owned
  // by another process" the moment it tried to receive on it.
  let assert Ok(definition_a) =
    worker.define(
      "fault.proxy.t3.a",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, T3Started("a", release))
        case process.receive(release, within: 20_000) {
          Ok(Nil) -> Ok("a-" <> int.to_string(value))
          Error(Nil) -> Ok("a-timed-out")
        }
      },
    )
  let assert Ok(definition_b) =
    worker.define(
      "fault.proxy.t3.b",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, T3Started("b", release))
        case process.receive(release, within: 20_000) {
          Ok(Nil) -> Ok("b-" <> int.to_string(value))
          Error(Nil) -> Ok("b-timed-out")
        }
      },
    )
  let assert Ok(workers) = registry.new("fault-proxy-t3")
  let assert Ok(workers) = registry.register(workers, definition_a)
  let assert Ok(workers) = registry.register(workers, definition_b)

  let assert Ok(handle_a) =
    postgres.submit(database, "fault-proxy-t3", definition_a, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "fault-proxy-t3", definition_b, 2)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(2)
    |> queue.with_maximum_jobs_per_poll(2)
    |> queue.with_poll_interval(50)
    |> queue.with_lease_duration(30_000)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start_with_policy(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  let assert Ok(T3Started(first_name, first_release)) =
    process.receive(started, within: 5000)
  let assert Ok(T3Started(second_name, second_release)) =
    process.receive(started, within: 5000)
  let #(release_a, release_b) = case first_name, second_name {
    "a", "b" -> #(first_release, second_release)
    "b", "a" -> #(second_release, first_release)
    _, _ -> panic as "T3: expected exactly one start each for a and b"
  }

  let notify = process.new_subject()
  fault_proxy.arm(
    proxy,
    fault_proxy.Armed(fault_proxy.OnCommit, fault_proxy.DropRequest),
    notify,
  )

  let start_ms = monotonic_ms()
  process.send(release_a, Nil)
  let assert Ok(fault_proxy.CommitSeen(_, _)) =
    process.receive(notify, within: 3000)
  process.send(release_b, Nil)

  wait_for_job_state(database, handle_b, job.Succeeded, 10_000)
  |> should.equal(True)
  let b_elapsed = monotonic_ms() - start_ms
  io.println(
    "T3: B succeeded in "
    <> int.to_string(b_elapsed)
    <> " ms total while A's own ack COMMIT request was stuck (drop_request)",
  )

  clear_stuck_backend(observer_connection)

  wait_for_job_state(database, handle_a, job.Succeeded, 15_000)
  |> should.equal(True)
  let a_elapsed = monotonic_ms() - start_ms
  io.println(
    "T3: A converged to Succeeded via the automatic pending-ack retry in "
    <> int.to_string(a_elapsed)
    <> " ms total",
  )

  mark_database_test_executed("fault-proxy-t3-observed")
}

/// DEFECT 2 probe: no proxy involved — pure `pool_size: 1` queue contention.
/// One caller holds the sole connection for 8 real seconds (`pg_sleep(8)`,
/// its own `pog.timeout` raised so *that* checkout's deadline does not fire
/// first); a second, ordinary Grind storage call is issued shortly after and
/// must wait behind it. That second call's own checkout deadline elapses
/// while it is still queued, before the first ever releases the connection —
/// this is `pgo_pool`'s "connection not available because deadline reached
/// while in queue" shape (`pgo_pool.erl`, `checkout_info/2`), a plain string
/// `pgo_ffi:query`'s `convert_error/1` has no clause for. Before Decision 1's
/// catch-all, that could raise `error:function_clause` inside the calling
/// process instead of returning a typed `pog.QueryError` — crashing whatever
/// called `postgres.state` (or any other storage function), not just failing
/// its own request. This test monitors every contended caller and *asserts*
/// none of them went down abnormally — not merely printing "CONFIRMED" and
/// letting the test pass regardless either way — though in this environment
/// `pgo_pool`'s own CoDel overload shedding has consistently returned the
/// already-handled `none_available` shape before the narrower race this
/// defect targets is ever reached (`docs/RECOVERY-EVIDENCE.md`, "DEFECT 2
/// probe").
pub fn fault_proxy_defect2_queue_deadline_test() {
  case fault_proxy_url() {
    Error(Nil) -> Nil
    Ok(base_url) -> run_defect2(base_url)
  }
}

fn run_defect2(base_url: String) -> Nil {
  let pool_name = process.new_name("grind_defect2_queue_deadline")
  let assert Ok(validated) =
    postgres.settings(base_url, pool_name)
    |> postgres.pool_size(1)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fp-defect2-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fp-defect2-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "fault.proxy.defect2",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("defect2-" <> int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "fault-proxy-defect2", definition, 1)

  let connection = pog.named_connection(pool_name)
  let holder_started = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(holder_started, Nil)
      let _ =
        pog.query("SELECT pg_sleep(8)")
        |> pog.timeout(15_000)
        |> pog.execute(on: connection)
      Nil
    })
  let assert Ok(Nil) = process.receive(holder_started, within: 2000)
  // Give the holder a moment to actually check out the sole connection
  // before the contended calls below start queueing behind it.
  process.sleep(300)

  // A burst of several contended callers, not just one: `pgo_pool`'s CoDel
  // overload shedding needs sustained observed queueing delay before it
  // starts returning the (already-handled) `none_available` shape, so a
  // single contended caller mostly just gets that graceful rejection. A
  // burst gives the narrower "reached the front of the queue, but its own
  // deadline fired a moment before it could claim the holder" race (the
  // shape with no `convert_error` clause) more chances to actually occur.
  let reply = process.new_subject()
  let start_ms = monotonic_ms()
  let monitors =
    list.repeat(Nil, 8)
    |> list.map(fn(_) {
      let pid =
        process.spawn_unlinked(fn() {
          let outcome = postgres.state(database, handle)
          process.send(reply, outcome)
        })
      process.monitor(pid)
    })
  let selector =
    list.fold(monitors, process.new_selector(), fn(selector, monitor) {
      process.select_specific_monitor(selector, monitor, Defect2Down)
    })
    |> process.select_map(reply, Defect2Replied)
  observe_defect2(selector, start_ms, 8)
  mark_database_test_executed("fault-proxy-defect2-observed")
}

fn observe_defect2(
  selector: process.Selector(Defect2Event),
  start_ms: Int,
  remaining: Int,
) -> Nil {
  case remaining > 0 {
    False -> Nil
    True ->
      case process.selector_receive(selector, within: 8000) {
        Ok(Defect2Replied(outcome)) -> {
          let elapsed = monotonic_ms() - start_ms
          io.println(
            "DEFECT 2 probe: a contended postgres.state call returned "
            <> string.inspect(outcome)
            <> " after "
            <> int.to_string(elapsed)
            <> " ms — no crash",
          )
          observe_defect2(selector, start_ms, remaining - 1)
        }
        Ok(Defect2Down(process.ProcessDown(reason: process.Normal, ..))) ->
          // The same process both replied and then exited normally
          // afterward — its own monitor's `DOWN` is not a crash signal,
          // just bookkeeping noise from a caller that already reported
          // its result via `Defect2Replied` above. Does not consume the
          // `remaining` budget; only unexpected exits do.
          observe_defect2(selector, start_ms, remaining)
        Ok(Defect2Down(down)) -> {
          let elapsed = monotonic_ms() - start_ms
          panic as {
            "DEFECT 2 probe: a contended caller crashed after "
            <> int.to_string(elapsed)
            <> " ms instead of getting a typed error back: "
            <> string.inspect(down)
          }
        }
        Error(Nil) ->
          io.println(
            "DEFECT 2 probe: "
            <> int.to_string(remaining)
            <> " contended caller(s) neither replied nor went down within 8000 ms",
          )
      }
  }
}

type Defect2Event {
  Defect2Replied(Result(job.State, postgres.StateError))
  Defect2Down(process.Down)
}
