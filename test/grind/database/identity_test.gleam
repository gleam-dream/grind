import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{None, Some}
import gleam/string
import gleeunit/should
import grind/internal/store
import grind/job
import grind/postgres
import grind/support/env.{
  database_url, mark_database_test_executed, owner_a_url, postgres_log_path,
  queue_database_url, unique_test_run_id,
}
import grind/support/schema_roles.{
  create_isolated_schema_role, drop_isolated_schema_role, role_scoped_url,
}
import grind/support/submissions.{unique_test_worker}
import grind/worker
import pog
import simplifile

/// `job.Installation` tokens for two genuinely different physical
/// databases in the *same* cluster (both defaulting to schema `"public"`,
/// so the schema component alone cannot distinguish them) must differ —
/// backstopped by the database's own OID, never only its configured
/// schema. See `postgres_migrate_concurrent_first_time_schema_creation_both_succeed_test`
/// and the module-level doc comment on `job.Installation` for the
/// complementary cross-*cluster* case this does not cover on its own (two
/// different clusters could still coincidentally share a database OID;
/// the cluster identifier, when readable, is what disambiguates that case
/// — untestable from a single disposable cluster, so documented instead).
pub fn postgres_installations_differ_across_databases_in_one_cluster_test() {
  case database_url(), queue_database_url() {
    Ok(database_url), Ok(queue_database_url) ->
      run_installations_differ_across_databases_test(
        database_url,
        queue_database_url,
      )
    _, _ -> Nil
  }
}

fn run_installations_differ_across_databases_test(
  database_url: String,
  queue_database_url: String,
) -> Nil {
  let assert Ok(validated_a) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database_a) = postgres.start(validated_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(validated_b) =
    postgres.settings(queue_database_url) |> postgres.validate
  let assert Ok(database_b) = postgres.start(validated_b)
  use <- exception.defer(fn() { postgres.close(database_b) })

  postgres.installation(database_a)
  |> should.not_equal(postgres.installation(database_b))
  mark_database_test_executed("installations-differ-across-databases")
}

/// Red-first proof of the coordinator review's handle-binding fix (item 2):
/// a `JobHandle` minted against one schema, used against a `Database`
/// pointed at a *different* schema of the same physical database, where
/// both schemas happen to hold a row with the identical numeric job id —
/// today (before this fix) `state`/`cancel`/etc. would silently read or
/// mutate the other schema's row, since nothing on the handle ever recorded
/// which installation minted it. After this fix, every read/write function
/// checks the handle's own `Installation` token against the `Database` it
/// is called on first, purely in memory, before any storage call —
/// `postgres.HandleFromAnotherInstallation` for `state`,
/// `CancellationFromAnotherInstallation` for `cancel`.
pub fn postgres_handle_from_another_installation_is_rejected_test() {
  case owner_a_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_handle_cross_installation_test(database_url)
  }
}

fn run_handle_cross_installation_test(base_url: String) -> Nil {
  let assert Ok(admin_validated) =
    postgres.settings(base_url) |> postgres.validate
  let assert Ok(admin_database) = postgres.start(admin_validated)
  use <- exception.defer(fn() { postgres.close(admin_database) })
  let admin_connection = postgres.connection(admin_database)
  let suffix = int.to_string(unique_test_run_id())
  let role_a = "grind_cross_inst_a_" <> suffix
  let role_b = "grind_cross_inst_b_" <> suffix
  create_isolated_schema_role(admin_connection, role_a)
  create_isolated_schema_role(admin_connection, role_b)
  use <- exception.defer(fn() {
    drop_isolated_schema_role(admin_connection, role_a)
    drop_isolated_schema_role(admin_connection, role_b)
  })

  let assert Ok(validated_a) =
    postgres.settings(role_scoped_url(base_url, role_a))
    |> postgres.with_schema(role_a)
    |> postgres.validate
  let assert Ok(database_a) = postgres.start(validated_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(validated_b) =
    postgres.settings(role_scoped_url(base_url, role_b))
    |> postgres.with_schema(role_b)
    |> postgres.validate
  let assert Ok(database_b) = postgres.start(validated_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(Nil) = postgres.migrate(database_b)

  // Different physical schemas, so `postgres.installation` must disagree —
  // the precondition this whole test depends on.
  postgres.installation(database_a)
  |> should.not_equal(postgres.installation(database_b))

  let assert Ok(input_codec) =
    worker.codec(
      "cross-installation-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "cross-installation-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(
      "cross-installation.worker",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )

  // Both schemas mint a job with the identical numeric id (each has its own
  // independent `grind_jobs_id_seq`, both freshly migrated, so both mint id
  // 1 for their own first submission — a real, expected consequence of
  // per-schema isolation, not a contrivance).
  let assert Ok(handle_a) =
    postgres.submit(database_a, "cross-installation", worker_def, 1)
  let assert Ok(handle_b) =
    postgres.submit(database_b, "cross-installation", worker_def, 2)
  job.id_value(handle_a) |> should.equal(job.id_value(handle_b))

  // The handle minted against schema A, used against schema B: rejected
  // purely from the two in-memory installation tokens, never silently
  // reading or mutating schema B's own same-id row.
  postgres.state(database_b, handle_a)
  |> should.equal(Error(postgres.HandleFromAnotherInstallation))
  postgres.arguments(database_b, handle_a)
  |> should.equal(Error(postgres.HandleFromAnotherInstallation))
  postgres.cancel(database_b, handle_a)
  |> should.equal(Error(postgres.CancellationFromAnotherInstallation))
  postgres.outcome(database_b, handle_a)
  |> should.equal(Error(postgres.HandleFromAnotherInstallation))

  // The matching same-schema call is unaffected — proving the rejection
  // above is genuinely about installation identity, not a broken handle.
  postgres.state(database_a, handle_a) |> should.equal(Ok(job.Queued))

  mark_database_test_executed("handle-cross-installation-rejected")
}

/// Red-first proof of the coordinator review's `read_cluster_identifier`
/// guard. Stock, unmodified PostgreSQL does *not* actually restrict
/// `EXECUTE` on `pg_control_system()` at all (confirmed empirically against
/// this disposable cluster — any ordinary role can call it by default);
/// some managed/hardened deployments revoke it from `PUBLIC`, which is the
/// scenario `read_cluster_identifier`'s guard exists for. This test
/// reproduces that scenario directly, by revoking it from `PUBLIC` for the
/// life of this one test and restoring it unconditionally afterward, rather
/// than relying on a default this cluster does not actually enforce. Once
/// revoked, an ordinary role calling `pg_control_system()` raw surfaces a
/// genuine PostgreSQL `ERROR:  permission denied for function
/// pg_control_system` on the server — even though this read is meant to be
/// a silent, best-effort improvement `postgres.start` never depends on. The
/// pre-existing swallow into `None` already made that failure invisible to
/// the *caller*, so a plain return-value assertion cannot tell the guarded
/// query apart from the unguarded one (both resolve to `None` for this role
/// either way); the disposable cluster's own server log
/// (`GRIND_TEST_POSTGRES_LOG`) is what actually distinguishes them:
/// `postgres.start` must still succeed for the role, its `Installation`
/// still falls back to no cluster identifier, and — the part only the
/// guard changes — PostgreSQL itself must never have raised, or logged,
/// that permission error while getting there.
pub fn postgres_start_with_non_superuser_role_never_logs_a_permission_error_test() {
  case owner_a_url(), postgres_log_path() {
    Error(Nil), _ | _, Error(Nil) -> Nil
    Ok(base_url), Ok(log_path) ->
      run_non_superuser_cluster_identifier_test(base_url, log_path)
  }
}

fn run_non_superuser_cluster_identifier_test(
  base_url: String,
  log_path: String,
) -> Nil {
  let assert Ok(admin_validated) =
    postgres.settings(base_url) |> postgres.validate
  let assert Ok(admin_database) = postgres.start(admin_validated)
  use <- exception.defer(fn() { postgres.close(admin_database) })
  let admin_connection = postgres.connection(admin_database)

  // Revoked (and restored) here, per database, rather than assumed:
  // `owner_a_url()`'s own database is used by several other tests in this
  // suite, each with their own roles, but none of them ever assert
  // anything about a cluster identifier specifically (only installation
  // *equality*, which this revoke does not change), so this is safe
  // regardless of test ordering within one run.
  let assert Ok(_) =
    pog.query("REVOKE EXECUTE ON FUNCTION pg_control_system() FROM PUBLIC")
    |> pog.execute(on: admin_connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query("GRANT EXECUTE ON FUNCTION pg_control_system() TO PUBLIC")
      |> pog.execute(on: admin_connection)
    Nil
  })

  let suffix = int.to_string(unique_test_run_id())
  let role = "grind_no_cluster_id_" <> suffix
  create_isolated_schema_role(admin_connection, role)
  use <- exception.defer(fn() {
    drop_isolated_schema_role(admin_connection, role)
  })

  let assert Ok(oid_returned) =
    pog.query(
      "SELECT oid::int4 FROM pg_database WHERE datname = current_database()",
    )
    |> pog.returning({
      use oid <- decode.field(0, decode.int)
      decode.success(oid)
    })
    |> pog.execute(on: admin_connection)
  let assert [oid] = oid_returned.rows

  let assert Ok(validated) =
    postgres.settings(role_scoped_url(base_url, role))
    |> postgres.with_schema(role)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })

  // `postgres.start` succeeds, and the resulting installation's own
  // cluster identifier is `None` for this non-superuser role — exactly as
  // documented. Identical with or without the guard; the guard's own
  // effect is server-log-only, checked below.
  postgres.installation(database)
  |> should.equal(job.new_installation(oid, role, None))

  await_log_never_shows(
    log_path,
    "permission denied for function pg_control_system",
    25,
  )
  |> should.equal(True)

  mark_database_test_executed("start-non-superuser-no-permission-error-logged")
}

/// Polls (bounded, ~500ms total) the disposable cluster's own server log,
/// waiting out any write-buffering delay before concluding `fragment` never
/// appeared — the passing case once `read_cluster_identifier`'s guard is in
/// place. Returns `False` the instant `fragment` is seen, rather than
/// waiting out the rest of the budget, since presence is decisive already.
fn await_log_never_shows(
  log_path: String,
  fragment: String,
  checks_remaining: Int,
) -> Bool {
  let seen = case simplifile.read(from: log_path) {
    Ok(contents) -> string.contains(contents, fragment)
    Error(_) -> False
  }
  case seen {
    True -> False
    False ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          await_log_never_shows(log_path, fragment, checks_remaining - 1)
        }
        False -> True
      }
  }
}

/// `call_safely` is the generic sibling of `execute_safely`: Squirrel-generated
/// query functions call `pog.execute` directly rather than going through
/// `execute_safely`, so `call_safely` wraps that call instead. `call_safely`
/// is a private wrapper around `grind_postgres_ffi:guarded` (never itself
/// exported for a test to redeclare and call directly — see
/// `src/grind_postgres_ffi.erl`'s own module documentation), so this proves
/// it through the public `postgres.arguments`, one of its own callers,
/// against a closed pool — the same "pool genuinely gone" `exit` shape
/// `postgres_lease_renewal_survives_closed_pool_test` already proves through
/// `postgres.state`/`execute_safely`.
pub fn postgres_call_safely_wrapper_reports_closed_pool_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_call_safely_closed_pool_test(database_url)
  }
}

fn run_call_safely_closed_pool_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let _ = postgres.close(database)
  let handle =
    job.new_handle(
      1,
      postgres.installation(database),
      "call-safely-probe",
      unique_test_worker("call-safely-probe"),
    )
  postgres.arguments(database, handle)
  |> should.equal(Error(postgres.JobReadQueryFailed(pog.ConnectionUnavailable)))
  mark_database_test_executed("call-safely-wrapper-closed-pool-passed")
}

pub fn postgres_close_stale_handle_does_not_erase_live_pool_deadline_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_close_stale_handle_preserves_deadline_test(database_url)
  }
}

/// Regression test: `close` on a *stale* `Database` handle (one whose own
/// supervisor has already stopped) must never erase the checkout deadline
/// of a different, currently live pool that has since reused the same
/// registered name. `validate` gives every `start`/`close` cycle of the
/// same `ValidatedSettings` the identical pool name (see `validate`'s own
/// doc comment), so a stray double-`close` on an old handle previously
/// erased the live pool's deadline entry unconditionally, silently
/// downgrading every later storage call on that live pool to the FFI's own
/// hardcoded 5000ms fallback instead of the smaller, distinctive value
/// configured here. Named mutation: reverting `postgres.close` to
/// unconditionally call `store.clear_deadline` (the pre-fix behavior) makes
/// the probe query below succeed instead of erroring, since 4200ms clears
/// the distinctive 3200ms deadline but not the 5000ms fallback.
fn run_close_stale_handle_preserves_deadline_test(database_url: String) -> Nil {
  let distinctive_deadline_ms = 3200
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_statement_deadline(distinctive_deadline_ms)
    |> postgres.validate
  let assert Ok(first) = postgres.start(validated)
  let assert Ok(Nil) = postgres.close(first)
  let assert Ok(second) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(second) })

  // The stale handle's own supervisor already stopped above; closing it
  // again must be a no-op with respect to `second`'s own deadline.
  let assert Ok(Nil) = postgres.close(first)

  let connection = postgres.connection(second)
  let result =
    store.call_safely(connection, fn(conn) {
      // `pg_types` cannot decode a bare `void` result (`pg_sleep`'s own
      // return type), so the sleep is wrapped in an outer scalar `SELECT`
      // — see `grind/internal/unique_admission`'s identical pattern.
      // 4.2s clears `distinctive_deadline_ms` (3200ms) but is comfortably
      // under the FFI's own hardcoded 5000ms fallback.
      pog.query(
        "SELECT true FROM (SELECT pg_sleep(4.2)) AS grind_close_stale_deadline_probe",
      )
      |> pog.execute(on: conn)
    })
  case result {
    Error(_) -> Nil
    Ok(_) ->
      panic as "expected the connection to be force-closed around the configured 3200ms statement deadline, not the FFI's own 5000ms fallback"
  }
  mark_database_test_executed("close-stale-handle-preserves-deadline-passed")
}

/// Pure, in-memory coverage of `job.same_installation` — no `Database` and
/// no PostgreSQL involved, since the function itself is a pure comparison
/// over two already-constructed `job.Installation` tokens. Covers every
/// combination the coordinator review named: identical cluster identifiers
/// match; different cluster identifiers differ even when OID and schema
/// agree; either side missing a cluster identifier (one `None`, or both
/// `None`) falls back to comparing OID and schema alone; and, within that
/// fallback, a differing OID or a differing schema each still makes the
/// installations differ.
pub fn job_same_installation_matches_when_cluster_identifiers_agree_test() {
  let a = job.new_installation(1, "public", Some(42))
  let b = job.new_installation(1, "public", Some(42))
  job.same_installation(a, b) |> should.equal(True)
}

pub fn job_same_installation_differs_when_cluster_identifiers_disagree_test() {
  let a = job.new_installation(1, "public", Some(42))
  let b = job.new_installation(1, "public", Some(43))
  job.same_installation(a, b) |> should.equal(False)
}

pub fn job_same_installation_falls_back_when_one_side_unreadable_test() {
  let a = job.new_installation(1, "public", Some(42))
  let b = job.new_installation(1, "public", None)
  job.same_installation(a, b) |> should.equal(True)
}

pub fn job_same_installation_falls_back_when_neither_side_readable_test() {
  let a = job.new_installation(1, "public", None)
  let b = job.new_installation(1, "public", None)
  job.same_installation(a, b) |> should.equal(True)
}

pub fn job_same_installation_fallback_still_rejects_differing_oid_test() {
  let a = job.new_installation(1, "public", None)
  let b = job.new_installation(2, "public", None)
  job.same_installation(a, b) |> should.equal(False)
}

pub fn job_same_installation_fallback_still_rejects_differing_schema_test() {
  let a = job.new_installation(1, "public", None)
  let b = job.new_installation(1, "other", None)
  job.same_installation(a, b) |> should.equal(False)
}
