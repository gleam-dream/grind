import exception
import fault_proxy
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option
import gleam/uri
import grind
import grind/admin
import grind/job
import grind/telemetry
import grind_consumer/support/env
import grind_consumer/support/resolution as s
import pog
import sinal

pub fn resolution_and_application_acknowledgment_roll_back_together_test() {
  use f <- s.with_runtime("resolution_rollback")
  let handle = s.held(f, 1)
  let assert Error(pog.TransactionRolledBack("rollback")) =
    pog.transaction(f.db, fn(tx) {
      let assert Ok(admin.Staged(admin.Applied(job.Succeeded))) =
        admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
      s.ack(tx, 1)
      Error("rollback")
    })
  assert !s.confirmed(f.db, 1)
  assert grind.state(f.jobs, handle) == Ok(job.Uncertain)
}

pub fn atomic_process_death_before_commit_test() {
  use f <- s.with_runtime("lab_atomic_kill_before")
  let handle = s.held(f, 1)
  let ready = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let _ =
        pog.transaction(f.db, fn(tx) {
          let assert Ok(_) =
            admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
          s.ack(tx, 1)
          s.gate(ready)
          Ok(Nil)
        })
    })
  let assert Ok(pid) = process.receive(ready, 5000)
  // Both writes are still invisible to this independent connection.
  assert !s.confirmed(f.db, 1)
  assert grind.state(f.jobs, handle) == Ok(job.Uncertain)
  s.kill_wait(pid)
  let assert Ok(_) =
    pog.transaction(f.db, fn(tx) {
      let assert Ok(_) =
        admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
      s.ack(tx, 1)
      Ok(Nil)
    })
  assert s.prune(f, 1) == 1
  assert s.confirmed(f.db, 1)
  assert s.invocations(f.db) == 1
}

pub fn atomic_process_death_after_commit_and_pruning_test() {
  use f <- s.with_runtime("lab_atomic_kill_after")
  let handle = s.held(f, 1)
  let ready = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let assert Ok(_) =
        pog.transaction(f.db, fn(tx) {
          let assert Ok(_) =
            admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
          s.ack(tx, 1)
          Ok(Nil)
        })
      s.gate(ready)
    })
  let assert Ok(pid) = process.receive(ready, 5000)
  s.kill_wait(pid)
  assert s.prune(f, 1) == 1
  assert s.confirmed(f.db, 1)
  assert s.invocations(f.db) == 1
}

pub fn borrowed_configuration_and_cancellation_test() {
  use f <- s.with_runtime("lab_borrowed_settings")
  let handle = s.held(f, 1)
  assert admin.resolve_uncertain_in(f.jobs, f.db, handle, s.decision(1))
    == Error(admin.NotInTransaction)
  let assert Ok(_) =
    pog.transaction(f.db, fn(tx) {
      let assert Ok(_) =
        pog.query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        |> pog.execute(tx)
      assert admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
        == Error(admin.TransactionIsolationUnsupported("serializable"))
      Ok(Nil)
    })
  let assert Ok(_) = grind.cancel(f.jobs, handle)
  let assert Ok(_) =
    pog.transaction(f.db, fn(tx) {
      let replay =
        admin.resolution(
          admin.AuthorizeReplay,
          id: "replay",
          by: "operator",
          details: "test",
        )
      assert admin.resolve_uncertain_in(f.jobs, tx, handle, replay)
        == Error(admin.CancellationPending)
      let assert Ok(_) =
        pog.query("SET LOCAL search_path = public") |> pog.execute(tx)
      let assert Ok(_) =
        pog.query("SET LOCAL lock_timeout='57ms'") |> pog.execute(tx)
      let assert Ok(_) =
        admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
      let assert Ok(rows) =
        pog.query(
          "SELECT current_setting('search_path')='public' AND current_setting('lock_timeout')='57ms'",
        )
        |> pog.returning({
          use value <- decode.field(0, decode.bool)
          decode.success(value)
        })
        |> pog.execute(tx)
      assert rows.rows == [True]
      // Application search_path is restored before it performs its own writes.
      Ok(Nil)
    })
  assert grind.state(f.jobs, handle) == Ok(job.Succeeded)
}

pub fn exact_retry_and_changed_attribution_test() {
  use f <- s.with_runtime("resolution_exact")
  let handle = s.held(f, 1)
  let request = s.decision(1)
  let assert Ok(admin.Staged(admin.Applied(job.Succeeded))) =
    pog.transaction(f.db, admin.resolve_uncertain_in(f.jobs, _, handle, request))
  let assert Ok(admin.Staged(admin.AlreadyApplied(job.Succeeded))) =
    pog.transaction(f.db, admin.resolve_uncertain_in(f.jobs, _, handle, request))
  let other =
    admin.resolution(
      admin.ConfirmSuccess(s.Confirmation("provider-1")),
      id: "r-1",
      by: "other",
      details: "Application settlement committed",
    )
  assert pog.transaction(f.db, admin.resolve_uncertain_in(
      f.jobs,
      _,
      handle,
      other,
    ))
    == Error(pog.TransactionRolledBack(admin.ResolutionConflict))
  let other_value =
    admin.resolution(
      admin.ConfirmSuccess(s.Confirmation("different")),
      id: "r-1",
      by: "operator",
      details: "Application settlement committed",
    )
  assert pog.transaction(f.db, admin.resolve_uncertain_in(
      f.jobs,
      _,
      handle,
      other_value,
    ))
    == Error(pog.TransactionRolledBack(admin.ResolutionConflict))
  assert s.invocations(f.db) == 1
}

pub fn staged_resolution_does_not_publish_a_commit_test() {
  use f <- s.with_runtime("resolution_observation")
  let assert [staged, committed] = s.batch(f, [1, 2])
  let events = process.new_subject()
  let observer =
    sinal.observe(telemetry.resolved(), fn(_, metadata) {
      process.send(events, metadata.ref.job_id)
    })
  use <- exception.defer(fn() { sinal.detach(observer) })
  let assert Error(pog.TransactionRolledBack(Nil)) =
    pog.transaction(f.db, fn(tx) {
      let assert Ok(_) =
        admin.resolve_uncertain_in(f.jobs, tx, staged, s.decision(1))
      Error(Nil)
    })
  // Same producer and forwarder: this later committed event is an ordering barrier.
  let assert Ok(_) = admin.resolve_uncertain(f.jobs, committed, s.decision(2))
  assert process.receive(events, 5000) == Ok(job.id(committed))
}

pub fn settings_are_bounded_during_resolution_and_restored_test() {
  use f <- s.with_runtime("resolution_limits")
  let assert [first, second] = s.batch(f, [1, 2])
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE observed_settings (lock_wait text, statement_wait text)",
    )
    |> pog.execute(f.db)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION observe_limits() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO observed_settings VALUES (current_setting('lock_timeout'),current_setting('statement_timeout')); RETURN NEW; END $$",
    )
    |> pog.execute(f.db)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER observe_limits BEFORE INSERT ON grind_job_resolutions FOR EACH ROW EXECUTE FUNCTION observe_limits()",
    )
    |> pog.execute(f.db)
  let assert Ok(_) =
    pog.transaction(f.db, fn(tx) {
      let assert Ok(_) =
        pog.query("SET LOCAL statement_timeout = 0") |> pog.execute(tx)
      let assert Ok(_) =
        pog.query("SET LOCAL lock_timeout = 0") |> pog.execute(tx)
      let assert Ok(_) =
        admin.resolve_uncertain_in(f.jobs, tx, first, s.decision(1))
      assert settings(tx) == #("0", "0")
      Ok(Nil)
    })
  let assert Ok(_) =
    pog.transaction(f.db, fn(tx) {
      let assert Ok(_) =
        pog.query("SET LOCAL statement_timeout = '1500ms'") |> pog.execute(tx)
      let assert Ok(_) =
        pog.query("SET LOCAL lock_timeout = '57ms'") |> pog.execute(tx)
      let assert Ok(_) =
        admin.resolve_uncertain_in(f.jobs, tx, second, s.decision(2))
      assert settings(tx) == #("57ms", "1500ms")
      Ok(Nil)
    })
  let assert Ok(rows) =
    pog.query(
      "SELECT lock_wait,statement_wait FROM observed_settings ORDER BY lock_wait",
    )
    |> pog.returning(pair_decoder())
    |> pog.execute(f.db)
  assert rows.rows == [#("2s", "2s"), #("57ms", "1500ms")]
}

fn pair_decoder() -> decode.Decoder(#(String, String)) {
  use first <- decode.field(0, decode.string)
  use second <- decode.field(1, decode.string)
  decode.success(#(first, second))
}

fn settings(tx: pog.Connection) -> #(String, String) {
  let assert Ok(rows) =
    pog.query(
      "SELECT current_setting('lock_timeout'),current_setting('statement_timeout')",
    )
    |> pog.returning(pair_decoder())
    |> pog.execute(tx)
  let assert [settings] = rows.rows
  settings
}

pub fn same_job_contention_preserves_other_jobs_and_pruning_test() {
  use f <- s.with_runtime("resolution_contention")
  let assert [first, other] = s.batch(f, [1, 2])
  let ready = process.new_subject()
  let first_done = process.new_subject()
  let second_done = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let assert Ok(_) =
        pog.transaction(f.db, fn(tx) {
          let assert Ok(_) =
            admin.resolve_uncertain_in(f.jobs, tx, first, s.decision(1))
          let gate = process.new_subject()
          process.send(ready, gate)
          let assert Ok(Nil) = process.receive(gate, 5000)
          s.ack(tx, 1)
          Ok(Nil)
        })
      process.send(first_done, Nil)
    })
  let assert Ok(gate) = process.receive(ready, 5000)
  let _ =
    process.spawn_unlinked(fn() {
      let answer =
        pog.transaction(f.db, fn(tx) {
          let assert Ok(_) =
            pog.query("SET LOCAL application_name='resolution_contender'")
            |> pog.execute(tx)
          admin.resolve_uncertain_in(f.jobs, tx, first, s.decision(1))
        })
      process.send(second_done, answer)
    })
  s.wait_until(
    fn() {
      s.scalar(
        f.db,
        "SELECT count(*) FROM pg_stat_activity WHERE application_name='resolution_contender' AND wait_event_type='Lock'",
      )
      == 1
    },
    100,
  )
  let assert Ok(_) = admin.resolve_uncertain(f.jobs, other, s.decision(2))
  assert s.prune(f, 10) == 1
  assert !s.confirmed(f.db, 1)
  assert grind.state(f.jobs, first) == Ok(job.Uncertain)
  process.send(gate, Nil)
  assert process.receive(first_done, 5000) == Ok(Nil)
  assert process.receive(second_done, 5000)
    == Ok(Ok(admin.Staged(admin.AlreadyApplied(job.Succeeded))))
  assert s.confirmed(f.db, 1)
  assert s.invocations(f.db) == 2
}

pub fn lost_commit_reply_recovers_from_the_application_acknowledgment_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(parsed) = uri.parse(url)
      let assert Ok(#(proxy, port)) =
        fault_proxy.start("127.0.0.1", option.unwrap(parsed.port, 5432))
      use <- exception.defer(fn() { fault_proxy.stop(proxy) })
      use f <- s.with_url(
        "resolution_reply_loss",
        uri.to_string(uri.Uri(..parsed, port: option.Some(port))),
      )
      let handle = s.held(f, 1)
      let fired = process.new_subject()
      fault_proxy.arm(
        proxy,
        fault_proxy.Armed(fault_proxy.OnCommit, fault_proxy.DropReply),
        fired,
      )
      let caller =
        process.spawn_unlinked(fn() {
          let _ =
            pog.transaction(f.db, fn(tx) {
              let assert Ok(_) =
                admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
              s.ack(tx, 1)
              Ok(Nil)
            })
        })
      let assert Ok(fault_proxy.CommitSeen(_, _)) = process.receive(fired, 5000)
      s.wait_until(
        fn() { grind.state(f.jobs, handle) == Ok(job.Succeeded) },
        400,
      )
      s.kill_wait(caller)
      assert s.prune(f, 1) == 1
      assert s.confirmed(f.db, 1)
      assert s.invocations(f.db) == 1
      env.mark("consumer-resolution-transaction-passed")
    }
  }
}

pub fn lock_timeout_rolls_back_application_work_and_allows_retry_test() {
  use f <- s.with_runtime("resolution_lock_timeout")
  let handle = s.held(f, 1)
  let ready = process.new_subject()
  let released = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      let assert Ok(_) =
        pog.transaction(f.db, fn(tx) {
          let assert Ok(_) =
            pog.query("SELECT id FROM grind_jobs WHERE id=$1 FOR NO KEY UPDATE")
            |> pog.parameter(pog.int(job.id(handle)))
            |> pog.execute(tx)
          let gate = process.new_subject()
          process.send(ready, gate)
          let assert Ok(Nil) = process.receive(gate, 5000)
          Ok(Nil)
        })
      process.send(released, Nil)
    })
  let assert Ok(gate) = process.receive(ready, 5000)
  let assert Error(pog.TransactionRolledBack(admin.Unavailable(pog.PostgresqlError(
    code,
    _,
    _,
  )))) =
    pog.transaction(f.db, fn(tx) {
      let assert Ok(_) =
        pog.query("SET LOCAL lock_timeout='57ms'") |> pog.execute(tx)
      s.ack(tx, 1)
      admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
    })
  assert code == "55P03"
  assert !s.confirmed(f.db, 1)
  assert grind.state(f.jobs, handle) == Ok(job.Uncertain)
  assert s.scalar(f.db, "SELECT count(*) FROM grind_job_resolutions") == 0
  process.send(gate, Nil)
  assert process.receive(released, 5000) == Ok(Nil)
  let assert Ok(_) =
    pog.transaction(f.db, fn(tx) {
      assert settings(tx).0 == "0"
      let assert Ok(_) =
        admin.resolve_uncertain_in(f.jobs, tx, handle, s.decision(1))
      s.ack(tx, 1)
      Ok(Nil)
    })
  assert s.confirmed(f.db, 1)
}

pub fn native_failure_and_authorized_replay_remain_available_test() {
  use f <- s.with_runtime("resolution_decisions")
  let assert [failed, replayed] = s.batch(f, [1, 2])
  let failure =
    admin.resolution(
      admin.ConfirmFailure(s.Refused("provider refused")),
      id: "failed-1",
      by: "operator",
      details: "Checked provider record",
    )
  let assert Ok(admin.Staged(admin.Applied(job.BusinessFailed))) =
    pog.transaction(f.db, admin.resolve_uncertain_in(f.jobs, _, failed, failure))
  let assert Ok(grind.Failed(
    grind.Business(s.Refused("provider refused")),
    _,
    _,
  )) = grind.outcome(f.jobs, failed)
  let replay =
    admin.resolution(
      admin.AuthorizeReplay,
      id: "replay-2",
      by: "operator",
      details: "Coordinator safely reads completed application work",
    )
  let assert Ok(admin.Staged(admin.Applied(job.Queued))) =
    pog.transaction(f.db, admin.resolve_uncertain_in(
      f.jobs,
      _,
      replayed,
      replay,
    ))
  let assert Ok(admin.Staged(admin.AlreadyApplied(job.Queued))) =
    pog.transaction(f.db, admin.resolve_uncertain_in(
      f.jobs,
      _,
      replayed,
      replay,
    ))
  assert grind.state(f.jobs, replayed) == Ok(job.Queued)
  assert s.invocations(f.db) == 2
}

pub fn transaction_database_and_handle_installation_are_checked_test() {
  use f <- s.with_runtime("resolution_owner")
  let handle = s.held(f, 1)
  let assert Ok(url) = env.database_url()
  let assert Ok(parsed) = uri.parse(url)
  use other <- s.with_url(
    "resolution_other_database",
    uri.to_string(uri.Uri(..parsed, path: "/postgres")),
  )
  assert pog.transaction(other.db, admin.resolve_uncertain_in(
      f.jobs,
      _,
      handle,
      s.decision(1),
    ))
    == Error(pog.TransactionRolledBack(admin.WrongDatabase))
  assert pog.transaction(other.db, admin.resolve_uncertain_in(
      other.jobs,
      _,
      handle,
      s.decision(1),
    ))
    == Error(pog.TransactionRolledBack(admin.WrongDatabase))
  assert grind.state(f.jobs, handle) == Ok(job.Uncertain)
}
