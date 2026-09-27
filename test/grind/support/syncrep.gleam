import gleam/dynamic/decode
import gleam/erlang/process
import gleam/result
import pog

pub fn backend_pid_is_alive(connection: pog.Connection, pid: Int) -> Bool {
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

pub fn terminate_backend(connection: pog.Connection, pid: Int) -> Bool {
  pog.query("SELECT pg_terminate_backend($1)")
  |> pog.parameter(pog.int(pid))
  |> pog.returning({
    use terminated <- decode.field(0, decode.bool)
    decode.success(terminated)
  })
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    case returned.rows {
      [terminated] -> terminated
      _ -> False
    }
  })
  |> result.unwrap(False)
}

/// Blocks (bounded) until `pid` no longer appears in `pg_stat_activity`.
/// `pg_terminate_backend` only sends the termination signal and returns
/// immediately; it does not wait for the target to actually finish
/// committing and exit. Callers that need PostgreSQL's own commit-visibility
/// side effects (ProcArray removal) to have happened before they proceed —
/// rather than relying on incidentally observing the same backend's own
/// socket close, as the reconciling-from-receipt test does — must wait for
/// this instead of proceeding immediately after termination.
pub fn wait_for_backend_gone(
  connection: pog.Connection,
  pid: Int,
  checks_remaining: Int,
) -> Result(Nil, Nil) {
  case backend_pid_is_alive(connection, pid) {
    False -> Ok(Nil)
    True ->
      case checks_remaining > 0 {
        False -> Error(Nil)
        True -> {
          process.sleep(10)
          wait_for_backend_gone(connection, pid, checks_remaining - 1)
        }
      }
  }
}

pub fn wait_for_syncrep_trigger_backend(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Result(Int, Nil) {
  let query =
    pog.query(
      "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND usename = current_user AND pid <> pg_backend_pid() AND wait_event = 'SyncRep' ORDER BY query_start DESC LIMIT 1",
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
              process.sleep(10)
              wait_for_syncrep_trigger_backend(connection, checks_remaining - 1)
            }
          }
        _ -> Error(Nil)
      }
  }
}

/// Fails clearly, instead of an opaque `let assert` mismatch far from the
/// real cause, when the disposable cluster was not started the way the
/// Increment 2 lost-reply tests require it. Without
/// `synchronous_standby_names=grind_never_standby`, a transaction that
/// raises its own `synchronous_commit` to `on` would either commit
/// immediately (a real standby present) or never proceed past `SyncRep` at
/// all in a way these tests can distinguish from a hang.
pub fn require_syncrep_cluster_configured(connection: pog.Connection) -> Nil {
  let assert Ok(returned) =
    pog.query("SHOW synchronous_standby_names")
    |> pog.returning({
      use value <- decode.field(0, decode.string)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  case returned.rows {
    ["grind_never_standby"] -> Nil
    [other] -> {
      let message =
        "scripts/test-postgres.sh must start PostgreSQL with -c synchronous_standby_names=grind_never_standby -c synchronous_commit=local for the SyncRep-based lost-reply tests to be meaningful; synchronous_standby_names was \""
        <> other
        <> "\" instead"
      panic as message
    }
    _ ->
      panic as "could not read synchronous_standby_names from the test cluster; scripts/test-postgres.sh must start PostgreSQL with -c synchronous_standby_names=grind_never_standby -c synchronous_commit=local"
  }
}

/// Installs a deferred constraint trigger on `table`, scoped by `predicate`
/// (a trusted SQL boolean expression referencing `NEW`, spliced verbatim —
/// never caller/user input), whose function raises only that one matching
/// transaction's `synchronous_commit` to `on` — see the acknowledgement lost-reply tests
/// in `grind/queue/ack_failure_test` and the uncertain-commit tests
/// (`grind_unique_submissions`, scoped by `submission_id` rather than a
/// server-generated `job_id`, since the submission id is known before the
/// admission transaction that would create the job id even starts).
/// Generalized from an earlier draft that hard-coded both
/// `grind_job_acknowledgements` and a `job_id` equality check. Returns a
/// cleanup thunk for the caller to register with `exception.defer`, which
/// first terminates any backend this same trigger still has parked in
/// `SyncRep` (so a failing assertion earlier in the test cannot hang the
/// whole gate run waiting on a standby that will never connect) and caps the
/// DROP itself with a lock timeout before dropping the trigger and function.
pub fn install_syncrep_reply_trigger(
  connection: pog.Connection,
  name: String,
  table: String,
  predicate: String,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT ("
      <> predicate
      <> ") THEN RETURN NEW; END IF; PERFORM set_config('synchronous_commit', 'on', true); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> name
      <> " AFTER INSERT ON "
      <> table
      <> " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> name
      <> "()",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ = case wait_for_syncrep_trigger_backend(connection, 0) {
      Ok(stuck_pid) -> terminate_backend(connection, stuck_pid)
      Error(Nil) -> True
    }
    // `connection` is a pool, not one physical connection: a `SET
    // lock_timeout` on its own checks out and releases a connection for
    // that one statement alone, so it would not reliably apply to whichever
    // (possibly different) connection the following `DROP`s happen to check
    // out — silently leaving the DROPs unbounded again. Running
    // `SET LOCAL` and both `DROP`s inside one `pog.transaction` pins them to
    // the same checked-out connection, where `SET LOCAL` actually scopes.
    let _ =
      pog.transaction(connection, fn(transaction_connection) {
        let _ =
          pog.query("SET LOCAL lock_timeout = '2s'")
          |> pog.execute(on: transaction_connection)
        let _ =
          pog.query("DROP TRIGGER IF EXISTS " <> name <> " ON " <> table)
          |> pog.execute(on: transaction_connection)
        let _ =
          pog.query("DROP FUNCTION IF EXISTS " <> name <> "()")
          |> pog.execute(on: transaction_connection)
        Ok(Nil)
      })
    Nil
  }
}
