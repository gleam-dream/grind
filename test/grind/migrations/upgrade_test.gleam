import exception
import fault_proxy
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{None}
import gleam/result
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/migrations
import grind/internal/postgres
import grind/internal/registry
import grind/internal/submission
import grind/internal/unique
import grind/internal/worker
import grind/support/concurrency.{spawn_submit}
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, schema_upgrade_fresh_url, schema_upgrade_url,
}
import grind/support/job_queries.{count_jobs_in_queue}
import grind/support/migration_fixtures.{
  apply_sql_statements, grind_catalog_digest, latest_migration,
  read_sql_statements_from_file,
}
import grind/support/submissions.{submit_keep_existing, unique_receipt_exists}
import grind/support/syncrep.{
  install_syncrep_reply_trigger, require_syncrep_cluster_configured,
  terminate_backend, wait_for_backend_gone, wait_for_syncrep_trigger_backend,
}
import grind/support/worker_failure.{type LookupFailure}
import pog

/// A synthetic `v13` (one past the real, current latest `v12`) used only by
/// the upgrade harness below: adds a nullable column and an index on it to
/// `grind_jobs`, the shape of change the design calls out
/// (`docs/RELEASE-READINESS.md`, "Migration mechanism") as the one most
/// likely to interact badly with rows a previous release already wrote —
/// layered on top of the real `v12` (`finished_at`) this harness now also
/// exercises for real, rather than only against a synthetic stand-in.
fn synthetic_v13_alter_migration() -> migrations.Migration {
  migrations.Migration(
    13,
    [
      "ALTER TABLE grind_jobs ADD COLUMN grind_test_note text",
      "CREATE INDEX grind_test_note_idx ON grind_jobs (grind_test_note)",
      "INSERT INTO grind_schema_migrations (version) VALUES (13)",
    ],
    list.append(latest_migration().shape, [
      migrations.ExpectedRelation("grind_test_note_idx", migrations.Index, []),
    ]),
    latest_migration().foreign_keys,
    latest_migration().forbidden_columns,
  )
}

/// The upgrade-harness contract: a database carrying a previous release's
/// frozen v11 schema and real rows in queued, scheduled, retryable,
/// executing-with-an-already-expired-lease, uncertain, and succeeded states
/// (the last two also carrying a real acknowledgement receipt, a real
/// uniqueness receipt with its `unique_key_*` columns populated, and a real
/// resolution row) survives `migrate_with` onto a synthetic v12 with every
/// seeded row intact, ends up with the exact same catalog shape a fresh
/// `migrate_with` install of the same steps produces, and stays fully
/// functional afterwards both for ordinary new API traffic (submit/claim/ack
/// on a fresh job) and, specifically, against the legacy seeded rows
/// themselves: the seeded executing lease is genuinely quarantined by a
/// legacy-queue poll, the seeded uncertain row is resolved, the seeded
/// acknowledgement receipt is reconciled by its own real command ID, and the
/// seeded unique submission is replayed (its own real request hash, not a
/// synthetic one, since it was seeded through `submit_unique` itself before
/// migrating rather than inserted by hand).
pub fn postgres_migrate_upgrade_from_frozen_v11_fixture_test() {
  case schema_upgrade_url(), schema_upgrade_fresh_url() {
    Ok(upgrade_url), Ok(fresh_url) ->
      run_upgrade_harness_test(upgrade_url, fresh_url)
    _, _ -> Nil
  }
}

fn legacy_upgrade_worker() -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input_codec) =
    worker.codec(
      "upgrade-legacy-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "upgrade-legacy-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(
      "upgrade.legacy-worker",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  worker_def
}

fn run_upgrade_harness_test(upgrade_url: String, fresh_url: String) -> Nil {
  let assert Ok(upgrade_validated) =
    postgres.settings(upgrade_url) |> postgres.validate
  let assert Ok(upgrade_database) = postgres.start(upgrade_validated)
  use <- exception.defer(fn() { postgres.close(upgrade_database) })
  let upgrade_connection = postgres.connection(upgrade_database)

  let assert Ok(fresh_validated) =
    postgres.settings(fresh_url) |> postgres.validate
  let assert Ok(fresh_database) = postgres.start(fresh_validated)
  use <- exception.defer(fn() { postgres.close(fresh_database) })
  let fresh_connection = postgres.connection(fresh_database)

  apply_sql_statements(
    upgrade_connection,
    read_sql_statements_from_file("test/fixtures/schema/v11.sql"),
  )
  // `storage_owner` still exists as a `NOT NULL` column (no default) on
  // this pre-migration v11 fixture schema exactly as v11 originally shipped
  // it (dropped only once `grind_v12` runs, below) — but today's
  // application code never writes it at all any more (see README,
  // "Isolation"), so `submit_unique`'s own `INSERT` below would otherwise
  // fail `23502 not_null_violation` against this exact table shape. Adding
  // a default here — a test-only convenience, not a change to the frozen
  // v11 definition `grind_migrations_conformance_test` checks byte-for-byte
  // elsewhere — lets that real seed call succeed the same way a database
  // that happened to already have one (a perfectly legal v11 schema
  // variant) would. Every other raw-SQL seed below still states its own
  // literal value explicitly, so this default never masks anything.
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_jobs ALTER COLUMN storage_owner SET DEFAULT 'upgrade-legacy-owner'",
    )
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_unique_submissions ALTER COLUMN storage_owner SET DEFAULT 'upgrade-legacy-owner'",
    )
    |> pog.execute(on: upgrade_connection)

  let legacy_worker = legacy_upgrade_worker()
  // Any non-empty literal works here now, since nothing reads it back: the
  // typed API no longer compares a row's owner to anything at all, only
  // its queue/worker identity (see README, "Isolation").
  let storage_owner = "upgrade-legacy-owner"

  // Seeded via `submit_unique` itself, against the pre-migration v11
  // schema — never raw SQL — so its `request_sha256` and `unique_key_*`
  // columns are exactly what a real caller's replay after the upgrade must
  // still match; a hand-written hash could never do that honestly.
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let legacy_policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let assert Ok(submission.Inserted(legacy_unique_handle)) =
    submit_keep_existing(
      upgrade_database,
      "upgrade-legacy",
      "upgrade-legacy-submission",
      legacy_worker,
      5,
      legacy_policy,
    )

  // Seed one row per remaining state via raw SQL, in a distinct queue from
  // the uniqueness submission above so neither competes with the other
  // during the legacy-consumer polls below.
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-other', 'upgrade.legacy-worker', 'v1', 'v1', '10'::jsonb, 'v1', 'queued', clock_timestamp(), NULL, 0, NULL, NULL, 0)",
    )
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-other', 'upgrade.legacy-worker', 'v1', 'v1', '11'::jsonb, 'v1', 'scheduled', clock_timestamp() + interval '1 hour', NULL, 0, NULL, NULL, 0)",
    )
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count, failure_description, failure_cause) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-other', 'upgrade.legacy-worker', 'v1', 'v1', '12'::jsonb, 'v1', 'retryable', clock_timestamp() + interval '1 minute', NULL, 1, NULL, NULL, 1, 'legacy transient failure', NULL)",
    )
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', 'upgrade.legacy-worker', 'v1', 'v1', '1'::jsonb, 'v1', 'executing', clock_timestamp(), nextval('grind_attempts_id_seq'), 1, 'legacy-attempt-owner', clock_timestamp() - interval '1 hour', 1) RETURNING id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: upgrade_connection)
  let assert Ok(executing_job) =
    pog.query(
      "SELECT id FROM grind_jobs WHERE storage_owner = '"
      <> storage_owner
      <> "' AND queue = 'upgrade-legacy' AND state = 'executing'",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: upgrade_connection)
  let assert [executing_job_id] = executing_job.rows
  let assert Ok(uncertain_job) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count, uncertain_at) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', 'upgrade.legacy-worker', 'v1', 'v1', '2'::jsonb, 'v1', 'uncertain', clock_timestamp(), nextval('grind_attempts_id_seq'), 1, 'legacy-attempt-owner', clock_timestamp() - interval '1 hour', 1, clock_timestamp()) RETURNING id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: upgrade_connection)
  let assert [uncertain_job_id] = uncertain_job.rows
  let assert Ok(succeeded_job) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, output, state, available_at, attempt_id, attempt_epoch, attempt_owner) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', 'upgrade.legacy-worker', 'v1', 'v1', '3'::jsonb, 'v1', '\"legacy-output\"'::jsonb, 'succeeded', clock_timestamp(), nextval('grind_attempts_id_seq'), 1, 'legacy-attempt-owner') RETURNING id, attempt_id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      use attempt_id <- decode.field(1, decode.int)
      decode.success(#(id, attempt_id))
    })
    |> pog.execute(on: upgrade_connection)
  let assert [#(succeeded_job_id, succeeded_attempt_id)] = succeeded_job.rows
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-command', 'upgrade-legacy', $1, 'upgrade.legacy-worker', 'v1', $2, 1, 'legacy-attempt-owner', 'succeeded', sha256(convert_to('legacy proposal', 'UTF8')))",
    )
    |> pog.parameter(pog.int(succeeded_job_id))
    |> pog.parameter(pog.int(succeeded_attempt_id))
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', $1, 'upgrade.legacy-worker', 'v1', 'upgrade-legacy-resolution', $2, 1, 'legacy-attempt-owner', clock_timestamp(), 'confirm_success', 'succeeded', 'legacy-on-call', 'legacy resolution before this upgrade')",
    )
    |> pog.parameter(pog.int(succeeded_job_id))
    |> pog.parameter(pog.int(succeeded_attempt_id))
    |> pog.execute(on: upgrade_connection)

  // Increment 25: seeds one orphaned receipt per receipt table — a
  // `job_id` that never existed in `grind_jobs` at all, exactly what a
  // database that has been running a while under `v11` (no foreign key
  // enforcing this) could already carry for reasons unrelated to this
  // upgrade (a hand rollback, an old bug, direct SQL) — before this
  // upgrade ever reaches `v12`'s own `ADD CONSTRAINT ... FOREIGN KEY`.
  // Without the `DELETE ... WHERE NOT EXISTS` cleanup immediately ahead of
  // each `ADD CONSTRAINT` in `v12_statements`, this `ADD CONSTRAINT` itself
  // would fail closed with `23503 foreign_key_violation` against these
  // three rows (confirmed red: temporarily commenting out the three
  // `DELETE`s reproduces exactly that error against this fixture). Proves
  // both halves at once below: `migrate_with` still succeeds, and the
  // orphans are genuinely gone afterward, not merely tolerated.
  let orphaned_job_id = 999_999_999
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-orphan-ack', 'upgrade-legacy', $1, 'upgrade.legacy-worker', 'v1', 999999998, 1, 'legacy-attempt-owner', 'succeeded', sha256(convert_to('legacy orphan proposal', 'UTF8')))",
    )
    |> pog.parameter(pog.int(orphaned_job_id))
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_unique_submissions (storage_owner, submission_id, queue, worker_id, worker_version, request_sha256, decision, job_id, job_queue, observed_state) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy-orphan-submission', 'upgrade-legacy', 'upgrade.legacy-worker', 'v1', sha256(convert_to('legacy orphan request', 'UTF8')), 'inserted', $1, 'upgrade-legacy', 'succeeded')",
    )
    |> pog.parameter(pog.int(orphaned_job_id))
    |> pog.execute(on: upgrade_connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ('"
      <> storage_owner
      <> "', 'upgrade-legacy', $1, 'upgrade.legacy-worker', 'v1', 'upgrade-legacy-orphan-resolution', 999999998, 1, 'legacy-attempt-owner', clock_timestamp(), 'confirm_success', 'succeeded', 'legacy-on-call', 'legacy orphan resolution before this upgrade')",
    )
    |> pog.parameter(pog.int(orphaned_job_id))
    |> pog.execute(on: upgrade_connection)

  let steps =
    list.append(migrations.migrations(), [synthetic_v13_alter_migration()])
  postgres.migrate_with(upgrade_database, steps) |> should.equal(Ok(Nil))
  postgres.migrate_with(fresh_database, steps) |> should.equal(Ok(Nil))

  // The three orphaned receipts seeded above did not survive the upgrade
  // (removed by `v12`'s own `DELETE ... WHERE NOT EXISTS` cleanup, ahead of
  // its own `ADD CONSTRAINT ... FOREIGN KEY`) — and, since `migrate_with`
  // above already returned `Ok(Nil)`, that `ADD CONSTRAINT` did not fail
  // closed against them either.
  let assert Ok(orphans_removed) =
    pog.query(
      "SELECT (SELECT count(*) FROM grind_job_acknowledgements WHERE job_id = $1), (SELECT count(*) FROM grind_unique_submissions WHERE job_id = $1), (SELECT count(*) FROM grind_job_resolutions WHERE job_id = $1)",
    )
    |> pog.parameter(pog.int(orphaned_job_id))
    |> pog.returning({
      use acknowledgements <- decode.field(0, decode.int)
      use unique_submissions <- decode.field(1, decode.int)
      use resolutions <- decode.field(2, decode.int)
      decode.success(#(acknowledgements, unique_submissions, resolutions))
    })
    |> pog.execute(on: upgrade_connection)
  orphans_removed.rows |> should.equal([#(0, 0, 0)])

  // Every seeded legacy row is untouched by the upgrade.
  let assert Ok(preserved) =
    pog.query(
      "SELECT (SELECT count(*) FROM grind_jobs WHERE queue = 'upgrade-legacy-other' AND state = 'queued'), (SELECT count(*) FROM grind_jobs WHERE state = 'scheduled'), (SELECT count(*) FROM grind_jobs WHERE state = 'retryable'), (SELECT count(*) FROM grind_jobs WHERE state = 'executing'), (SELECT count(*) FROM grind_jobs WHERE state = 'uncertain'), (SELECT count(*) FROM grind_job_acknowledgements WHERE command_id = 'upgrade-legacy-command'), (SELECT count(*) FROM grind_unique_submissions WHERE submission_id = 'upgrade-legacy-submission'), (SELECT count(*) FROM grind_jobs WHERE id = $1 AND unique_key_contract IS NOT NULL AND unique_key_sha256 IS NOT NULL), (SELECT count(*) FROM grind_job_resolutions WHERE resolution_id = 'upgrade-legacy-resolution')",
    )
    |> pog.parameter(pog.int(job.id_value(legacy_unique_handle)))
    |> pog.returning({
      use queued <- decode.field(0, decode.int)
      use scheduled <- decode.field(1, decode.int)
      use retryable <- decode.field(2, decode.int)
      use executing <- decode.field(3, decode.int)
      use uncertain <- decode.field(4, decode.int)
      use acknowledgement <- decode.field(5, decode.int)
      use unique_submission <- decode.field(6, decode.int)
      use unique_key_columns <- decode.field(7, decode.int)
      use resolution <- decode.field(8, decode.int)
      decode.success(#(
        queued,
        scheduled,
        retryable,
        executing,
        uncertain,
        acknowledgement,
        unique_submission,
        unique_key_columns,
        resolution,
      ))
    })
    |> pog.execute(on: upgrade_connection)
  preserved.rows |> should.equal([#(1, 1, 1, 1, 1, 1, 1, 1, 1)])

  // The real `v12` backfill applied by this exact upgrade: the seeded
  // `succeeded` row (terminal, written under the pre-migration v11 schema
  // that had no `finished_at` column at all) picked up a non-null
  // `finished_at` dated from this migration run, while every seeded
  // non-terminal row (queued, scheduled, retryable, executing, uncertain)
  // was nulled back out by the backfill's own second pass.
  let assert Ok(finished_at_backfill) =
    pog.query(
      "SELECT (SELECT finished_at IS NOT NULL AND finished_at > clock_timestamp() - interval '1 minute' FROM grind_jobs WHERE id = $1), (SELECT count(*) = 0 FROM grind_jobs WHERE finished_at IS NOT NULL AND state NOT IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled'))",
    )
    |> pog.parameter(pog.int(succeeded_job_id))
    |> pog.returning({
      use succeeded_finished_recently <- decode.field(0, decode.bool)
      use no_non_terminal_finished_at <- decode.field(1, decode.bool)
      decode.success(#(succeeded_finished_recently, no_non_terminal_finished_at))
    })
    |> pog.execute(on: upgrade_connection)
  finished_at_backfill.rows |> should.equal([#(True, True)])

  // The upgraded database's catalog shape is identical to a fresh install of
  // the exact same steps.
  grind_catalog_digest(upgrade_connection)
  |> should.equal(grind_catalog_digest(fresh_connection))

  // The upgraded schema is fully functional for ordinary new traffic:
  // submit/claim/ack on a fresh job.
  let assert Ok(input_codec) =
    worker.codec(
      "upgrade-smoke-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "upgrade-smoke-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(smoke_worker) =
    worker.define(
      "upgrade.smoke-worker",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("upgrade-smoke")
  let assert Ok(workers) = registry.register(workers, smoke_worker)
  let assert Ok(handle) =
    postgres.submit(upgrade_database, "upgrade-smoke", smoke_worker, 41)
  let assert Ok(consumer) =
    queue.start(upgrade_database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(upgrade_database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(upgrade_database, handle)
  |> should.equal(Ok(job.SucceededWith("41")))

  // ...and, specifically, against the legacy seeded rows themselves.
  //
  // Replays the real `submit_unique` seed above with its own real request
  // hash — never a hand-written one — so this genuinely proves post-upgrade
  // idempotency, not merely that a row exists.
  let assert Ok(submission.Inserted(legacy_replayed_handle)) =
    submit_keep_existing(
      upgrade_database,
      "upgrade-legacy",
      "upgrade-legacy-submission",
      legacy_worker,
      5,
      legacy_policy,
    )
  job.id_value(legacy_replayed_handle)
  |> should.equal(job.id_value(legacy_unique_handle))

  // Quarantines the seeded already-expired executing lease via a real
  // legacy-queue poll.
  let assert Ok(legacy_workers) = registry.new("upgrade-legacy")
  let assert Ok(legacy_workers) =
    registry.register(legacy_workers, legacy_worker)
  let assert Ok(legacy_consumer) =
    queue.start(upgrade_database, legacy_workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(legacy_consumer) })
  // Drains the still-queued (replayed, never claimed) unique submission
  // above first, so it has no other legitimately claimable job competing in
  // the same poll as the quarantine check below.
  queue.process_one(legacy_consumer) |> should.equal(Ok(True))
  let executing_handle =
    job.new_handle(
      executing_job_id,
      postgres.installation(upgrade_database),
      "upgrade-legacy",
      legacy_worker,
    )
  queue.process_one(legacy_consumer) |> should.equal(Ok(False))
  postgres.state(upgrade_database, executing_handle)
  |> should.equal(Ok(job.Uncertain))

  // Resolves the seeded uncertain row.
  let uncertain_handle =
    job.new_handle(
      uncertain_job_id,
      postgres.installation(upgrade_database),
      "upgrade-legacy",
      legacy_worker,
    )
  postgres.resolve_uncertain(
    upgrade_database,
    uncertain_handle,
    postgres.ResolutionRequest(
      "upgrade-legacy-uncertain-resolution",
      "on-call",
      "post-upgrade smoke resolution of the seeded uncertain row",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))

  // Reconciles the seeded acknowledgement receipt by its own real command
  // ID.
  let succeeded_handle =
    job.new_handle(
      succeeded_job_id,
      postgres.installation(upgrade_database),
      "upgrade-legacy",
      legacy_worker,
    )
  let assert Ok(receipt) =
    postgres.reconcile_acknowledgement(
      upgrade_database,
      succeeded_handle,
      "upgrade-legacy-command",
    )
  receipt.command_id |> should.equal("upgrade-legacy-command")
  receipt.attempt_id |> should.equal(succeeded_attempt_id)
  receipt.attempt_epoch |> should.equal(1)
  receipt.committed_state |> should.equal(job.Succeeded)
  receipt.business_failure_cause |> should.equal(None)

  mark_database_test_executed("migrate-upgrade-harness-passed")
}

@external(erlang, "grind_test_env", "upgrade_lost_reply_url")
fn upgrade_lost_reply_url() -> Result(String, Nil)

/// Starts a real TCP fault proxy (`test/grind_fault_proxy.erl`, bound
/// through `fault_proxy.gleam` — the same mechanism
/// `test/grind_fault_proxy_test.gleam`'s T1-T5 use for the acknowledgement
/// path) in front of `base_url`'s real host/port, and returns the handle
/// plus a database URL pointing at the proxy, at `database`, instead of the
/// real cluster.
fn start_lost_reply_proxy(
  base_url: String,
  database: String,
) -> #(fault_proxy.Proxy, String) {
  let assert Ok(config) =
    pog.url_config(process.new_name("grind_upgrade_lost_reply_proxy"), base_url)
  let assert Ok(#(proxy, proxy_port)) =
    fault_proxy.start(config.host, config.port)
  let url =
    "postgres://"
    <> config.user
    <> "@127.0.0.1:"
    <> int.to_string(proxy_port)
    <> "/"
    <> database
    <> "?sslmode=disable"
  #(proxy, url)
}

/// Polls (bounded) until no other backend on this database is sitting
/// `idle in transaction` — used before migrating, so a just-recovered fault
/// scenario's own transaction (rolled back or about to be, but not
/// necessarily processed by PostgreSQL yet at the instant the client itself
/// gave up waiting) cannot still be holding a lock the migration's own DDL
/// would block on.
fn wait_for_no_idle_in_transaction(
  connection: pog.Connection,
  checks_remaining: Int,
) -> Bool {
  let assert Ok(returned) =
    pog.query(
      "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'idle in transaction'",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  case returned.rows {
    [0] -> True
    _ ->
      case checks_remaining > 0 {
        True -> {
          process.sleep(20)
          wait_for_no_idle_in_transaction(connection, checks_remaining - 1)
        }
        False -> False
      }
  }
}

fn job_id_for_queue(
  connection: pog.Connection,
  queue: String,
) -> Result(Int, Nil) {
  pog.query("SELECT id FROM grind_jobs WHERE queue = $1")
  |> pog.parameter(pog.text(queue))
  |> pog.returning({
    use id <- decode.field(0, decode.int)
    decode.success(id)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [id] -> Ok(id)
      _ -> Error(Nil)
    }
  })
}

/// The genuine lost-reply gap named in `docs/RELEASE-READINESS.md`
/// ("Migration gaps"): every other `reconcile_unique` lost-reply test in
/// this file proves the mechanism against a stable, already-latest schema.
/// This one proves it survives a real schema upgrade landing *between* the
/// lost reply and the reconciliation call — the exact interaction a caller
/// retrying across a deploy window would hit.
///
/// Two independent submissions are seeded against the frozen v11 fixture:
///
/// (a) Genuinely committed, reply lost — the same deferred-constraint
/// `synchronous_commit` trigger `run_unique_committed_reply_lost_store_unavailable_test`
/// uses (Increment 11), not the TCP fault proxy: **empirically, the TCP
/// proxy's `OnCommit`/`DropReply` does not produce this half for
/// `submit_unique`.** Traced with `pg_stat_activity` while debugging this
/// test: the admission transaction's own final `commit` reliably parks the
/// real backend in `wait_event = 'Client'/'ClientRead'` — PostgreSQL
/// received the extended-protocol `Parse "commit"` message and is waiting
/// for the client's next protocol message (`Bind`), which pog's own
/// connection process never sends once the `Parse` reply is swallowed — so
/// the transaction never actually reaches `Execute`/commits at all; it
/// stays open until Grind's own deadline force-closes the client socket,
/// which cascades through the proxy to closing its upstream socket too,
/// and PostgreSQL rolls the whole thing back on the disconnect. Confirmed
/// with zero rows ever appearing on an independent, unproxied connection
/// across ten seconds of polling, both before and after `submit_unique`
/// itself returned. This is a genuine, reproducible finding about this
/// specific transaction shape, not a flaw in the proxy or in T1-T5
/// (`test/grind_fault_proxy_test.gleam`), which exercise a shorter,
/// differently-timed transaction (a single acknowledgement `UPDATE`) and
/// document their own non-determinism (sometimes a transparent recovery,
/// sometimes `QueueAckUnknown`) rather than a guaranteed commit either.
/// Documented here rather than silently worked around, matching this
/// codebase's own practice (see `docs/RECOVERY-EVIDENCE.md`, Increment 33,
/// "two findings ... rest on premises that did not hold empirically").
///
/// (b) Genuinely never reaches PostgreSQL — the real TCP fault proxy
/// (`test/grind_fault_proxy.erl`, the same mechanism
/// `test/grind_fault_proxy_test.gleam`'s T1-T5 use for the acknowledgement
/// path), `OnCommit`/`DropRequest`: the triggering `commit` chunk is never
/// forwarded, so PostgreSQL never even attempts it — reliable and
/// deterministic, unlike (a) above, since there is no partially-completed
/// protocol exchange to get stuck on.
///
/// `migrate_with` then upgrades the schema from v11 to v12 — narrowing
/// `grind_unique_submissions`'s own primary key from
/// `(storage_owner, submission_id)` to `(submission_id)`-only, among other
/// changes — while both `PendingSubmission`s are still outstanding,
/// unreconciled. `reconcile_unique` against the *upgraded* schema then
/// resolves each correctly: (a) to `Inserted`, with the real job id, not
/// another `CommitUnknown`; (b) to `CommitUnknown` again — nothing was ever
/// committed, so there is nothing to find on either schema, never a false
/// positive.
pub fn postgres_migrate_upgrade_reconcile_unique_lost_reply_test() {
  case upgrade_lost_reply_url() {
    Error(Nil) -> Nil
    Ok(direct_url) -> run_upgrade_reconcile_unique_lost_reply_test(direct_url)
  }
}

fn run_upgrade_reconcile_unique_lost_reply_test(direct_url: String) -> Nil {
  let assert Ok(direct_validated) =
    postgres.settings(direct_url) |> postgres.validate
  let assert Ok(direct_database) = postgres.start(direct_validated)
  use <- exception.defer(fn() { postgres.close(direct_database) })
  let direct_connection = postgres.connection(direct_database)
  require_syncrep_cluster_configured(direct_connection)
  apply_sql_statements(
    direct_connection,
    read_sql_statements_from_file("test/fixtures/schema/v11.sql"),
  )
  // Same test-only convenience `run_upgrade_harness_test` uses: today's
  // application code never writes `storage_owner` at all (see README,
  // "Isolation"), so `submit_unique`'s own `INSERT` below would otherwise
  // fail `23502 not_null_violation` against this exact pre-migration column
  // shape.
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_jobs ALTER COLUMN storage_owner SET DEFAULT 'upgrade-lost-reply-owner'",
    )
    |> pog.execute(on: direct_connection)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_unique_submissions ALTER COLUMN storage_owner SET DEFAULT 'upgrade-lost-reply-owner'",
    )
    |> pog.execute(on: direct_connection)

  let legacy_worker = legacy_upgrade_worker()
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let direct_database_name = "grind_upgrade_lost_reply"

  // (a) Genuinely committed, reply lost (deferred-constraint SyncRep
  // trigger; see the module doc comment above for why this half does not
  // use the TCP fault proxy).
  let committed_queue = "upgrade-lost-reply-committed"
  let committed_submission = "upgrade-lost-reply-committed-submission"
  let cleanup_committed_trigger =
    install_syncrep_reply_trigger(
      direct_connection,
      "grind_test_upgrade_lost_reply_committed",
      "grind_unique_submissions",
      "NEW.submission_id = '" <> committed_submission <> "'",
    )
  use <- exception.defer(cleanup_committed_trigger)

  let assert Ok(committed_validated) =
    postgres.settings(direct_url) |> postgres.validate
  let assert Ok(committed_database) = postgres.start(committed_validated)
  let committed_reply = process.new_subject()
  spawn_submit(committed_reply, fn() {
    submit_keep_existing(
      committed_database,
      committed_queue,
      committed_submission,
      legacy_worker,
      21,
      policy,
    )
  })
  let assert Ok(committed_backend_pid) =
    wait_for_syncrep_trigger_backend(direct_connection, 300)
  // The admission transaction reached the database (its own "commit" call
  // was genuinely mid-flight, parked in SyncRep, when the pool closed) —
  // genuinely uncertain, not knowably absent.
  let _ = postgres.close(committed_database)
  let assert Ok(Error(submission.CommitUnknown(committed_pending))) =
    process.receive(committed_reply, within: 10_000)

  // The orphaned backend does not go away on its own — PostgreSQL's own
  // SyncRep wait deliberately does not notice a client disconnect, to avoid
  // ever partially applying a commit. Terminate and confirm it is gone
  // *before* migrating: the migration's own DDL needs an `ACCESS EXCLUSIVE`
  // lock on `grind_unique_submissions`/`grind_jobs`, which this still-open,
  // still-lock-holding transaction would otherwise block indefinitely.
  terminate_backend(direct_connection, committed_backend_pid)
  |> should.equal(True)
  let assert Ok(Nil) =
    wait_for_backend_gone(direct_connection, committed_backend_pid, 300)

  // (b) Genuinely never reaches PostgreSQL, via the real TCP fault proxy.
  let absent_queue = "upgrade-lost-reply-absent"
  let absent_submission = "upgrade-lost-reply-absent-submission"
  let #(absent_proxy, absent_proxy_url) =
    start_lost_reply_proxy(direct_url, direct_database_name)
  let assert Ok(absent_validated) =
    postgres.settings(absent_proxy_url)
    |> postgres.with_pool_size(1)
    |> postgres.validate
  let assert Ok(absent_database) = postgres.start(absent_validated)
  use <- exception.defer(fn() { postgres.close(absent_database) })
  let absent_notify = process.new_subject()
  fault_proxy.arm(
    absent_proxy,
    fault_proxy.Armed(fault_proxy.OnCommit, fault_proxy.DropRequest),
    absent_notify,
  )
  let absent_reply = process.new_subject()
  spawn_submit(absent_reply, fn() {
    submit_keep_existing(
      absent_database,
      absent_queue,
      absent_submission,
      legacy_worker,
      22,
      policy,
    )
  })
  let assert Ok(fault_proxy.CommitSeen(_, _)) =
    process.receive(absent_notify, within: 5000)
  let assert Ok(Error(submission.CommitUnknown(absent_pending))) =
    process.receive(absent_reply, within: 10_000)
  fault_proxy.stop(absent_proxy)

  // Independent confirmation, on the direct (unproxied) connection, of what
  // actually happened before ever migrating: (a) committed one real job and
  // receipt, (b) committed nothing.
  count_jobs_in_queue(direct_connection, committed_queue) |> should.equal(1)
  unique_receipt_exists(direct_connection, committed_submission)
  |> should.equal(True)
  count_jobs_in_queue(direct_connection, absent_queue) |> should.equal(0)
  unique_receipt_exists(direct_connection, absent_submission)
  |> should.equal(False)

  // No lingering `idle in transaction` backend (from either half above, or
  // (b)'s own client-disconnect-triggered rollback still settling) could
  // block the migration's own DDL below.
  wait_for_no_idle_in_transaction(direct_connection, 250) |> should.equal(True)

  // The migration boundary: both `PendingSubmission`s are still outstanding
  // when the schema moves from v11 to v12.
  postgres.migrate_with(direct_database, migrations.migrations())
  |> should.equal(Ok(Nil))

  // (a) resolves correctly post-upgrade: found and committed, with the real
  // job id — not a fabricated one, and not another `CommitUnknown`.
  let assert Ok(submission.Inserted(committed_handle)) =
    postgres.reconcile_unique(direct_database, committed_pending)
  let assert Ok(real_job_id) =
    job_id_for_queue(direct_connection, committed_queue)
  job.id_value(committed_handle) |> should.equal(real_job_id)
  postgres.arguments(direct_database, committed_handle)
  |> should.equal(Ok(21))

  // (b) still correctly reports `CommitUnknown` post-upgrade — nothing was
  // ever committed, so there is nothing to find, on either schema.
  postgres.reconcile_unique(direct_database, absent_pending)
  |> should.equal(Error(submission.CommitUnknown(absent_pending)))
  count_jobs_in_queue(direct_connection, absent_queue) |> should.equal(0)

  mark_database_test_executed("upgrade-reconcile-unique-lost-reply-passed")
}
