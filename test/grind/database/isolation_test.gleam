import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleeunit/should
import grind/internal/job
import grind/internal/postgres
import grind/internal/submission
import grind/internal/unique
import grind/internal/worker
import grind/support/env.{
  database_url, mark_database_test_executed, owner_a_url, unique_test_run_id,
  user_schema_fallback_url,
}
import grind/support/schema_roles.{
  create_isolated_schema_role, drop_isolated_schema_role, role_scoped_url,
}
import pog

pub fn postgres_two_schemas_share_a_database_but_stay_isolated_test() {
  case owner_a_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_two_schemas_isolated_test(database_url)
  }
}

/// Isolation between logically distinct Grind installations is the
/// PostgreSQL schema (`search_path`) alone now — never a `storage_owner`
/// column scoping rows within one shared schema; see README, "Isolation".
/// Two roles sharing one physical database, each with its own like-named
/// schema, prove this directly: `CREATE SCHEMA AUTHORIZATION <role>` gives
/// each role its own private namespace, and `postgres.with_schema` pins
/// each pool's `search_path` to exactly that one schema — deliberately
/// explicit, not inferred from the role or the URL (see `with_schema`'s own
/// doc comment and `docs/RISKS.md` #7 for why relying on PostgreSQL's own
/// `"$user", public` `search_path` fallback instead is the fragile shape
/// this package no longer recommends: see
/// `postgres_user_schema_fallback_shares_one_installation_test` for exactly
/// the duplicate-admission hazard that fallback has). Jobs, uniqueness,
/// quarantine, and pruning are each checked directly against both schemas'
/// own row counts, never by comparing a job id or handle across schemas: an
/// id is only unique *within* one schema now (each has its own independent
/// `grind_jobs_id_seq`), so two schemas started fresh in the same test can
/// legitimately mint the identical id for two unrelated jobs — a real,
/// expected consequence of per-schema isolation, not a bug, and precisely
/// why row counts (not cross-schema handle reads) are the correct proof
/// here.
fn run_two_schemas_isolated_test(base_url: String) -> Nil {
  let assert Ok(admin_validated) =
    postgres.settings(base_url) |> postgres.validate
  let assert Ok(admin_database) = postgres.start(admin_validated)
  use <- exception.defer(fn() { postgres.close(admin_database) })
  let admin_connection = postgres.connection(admin_database)
  let suffix = int.to_string(unique_test_run_id())
  let role_a = "grind_iso_a_" <> suffix
  let role_b = "grind_iso_b_" <> suffix
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
  let connection_a = postgres.connection(database_a)
  let connection_b = postgres.connection(database_b)

  let assert Ok(input_codec) =
    worker.codec(
      "schema-isolation-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "schema-isolation-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(
      "schema.isolation",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )

  // Jobs: one submission per schema, but each schema's own `grind_jobs`
  // holds exactly its own row — never the other schema's.
  let assert Ok(_) = postgres.submit(database_a, "default", worker_def, 41)
  let assert Ok(_) = postgres.submit(database_b, "default", worker_def, 42)
  count_grind_jobs(connection_a) |> should.equal(1)
  count_grind_jobs(connection_b) |> should.equal(1)

  // Uniqueness: the identical key, queue, and `SubmissionId` independently
  // admits in both schemas without ever contending — the domain advisory
  // lock is itself keyed by each pool's own configured schema (see
  // `unique_admission.lock_key_sql`) precisely so this holds.
  let assert Ok(period) =
    unique.within_milliseconds(60_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let assert Ok(submission_id_a) =
    submission.submission_id("schema-iso-shared-id")
  let assert Ok(submission_id_b) =
    submission.submission_id("schema-iso-shared-id")
  let assert Ok(submission.Inserted(_)) =
    postgres.submit_unique(
      database_a,
      "unique-default",
      submission_id_a,
      worker_def,
      1,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  let assert Ok(submission.Inserted(_)) =
    postgres.submit_unique(
      database_b,
      "unique-default",
      submission_id_b,
      worker_def,
      1,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )

  // Quarantine: an already-expired executing row seeded directly in schema
  // A's own connection is invisible to schema B's own sweep.
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, attempt_count) VALUES ('quarantine-default', 'schema.isolation', 'v1', 'schema-isolation-input-v1', '99'::jsonb, 'schema-isolation-output-v1', 'executing', clock_timestamp(), nextval('grind_attempts_id_seq'), 1, 'schema-iso-owner', clock_timestamp() - interval '1 hour', 1)",
    )
    |> pog.execute(on: connection_a)
  postgres.quarantine_expired(database_b, limit: 100) |> should.equal(Ok(0))
  postgres.quarantine_expired(database_a, limit: 100) |> should.equal(Ok(1))

  // Retention: a finished, old-enough row in schema A is never touched by a
  // `prune_finished` call against schema B.
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, output, state, available_at, finished_at) VALUES ('prune-default', 'schema.isolation', 'v1', 'schema-isolation-input-v1', '1'::jsonb, 'schema-isolation-output-v1', '\"done\"'::jsonb, 'succeeded', clock_timestamp(), clock_timestamp() - interval '1 hour')",
    )
    |> pog.execute(on: connection_a)
  postgres.prune_finished(database_b, older_than_ms: 1, limit: 100)
  |> should.equal(Ok(postgres.PruneReport(jobs: 0)))
  postgres.prune_finished(database_a, older_than_ms: 1, limit: 100)
  |> should.equal(Ok(postgres.PruneReport(jobs: 1)))

  mark_database_test_executed("two-schemas-share-database-isolated")
}

fn count_grind_jobs(connection: pog.Connection) -> Int {
  let assert Ok(returned) =
    pog.query("SELECT count(*) FROM grind_jobs")
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  let assert [count] = returned.rows
  count
}

pub fn postgres_two_urls_to_the_same_schema_share_it_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_two_urls_same_schema_test(database_url)
  }
}

/// Two pools reaching the exact same physical database/schema through
/// textually different connection strings — the second carries an extra,
/// unrecognized query parameter `pog.url_config` inspects only for
/// `sslmode` and otherwise ignores entirely (see that function in
/// `build/packages/pog/src/pog.gleam`), so it changes nothing about how or
/// where the pool connects — see each other's jobs through `state`:
/// genuinely the same schema, not merely "the same-looking URL" (the two
/// URLs are asserted different first, so this is not a vacuous check).
/// Isolation is the schema a pool's `search_path` actually resolves to,
/// never a property of the connection string that reached it — see README,
/// "Isolation". Deliberately never varies the hostname (`127.0.0.1` versus
/// `localhost`, as an earlier version of this test did): that construction
/// silently depended on `localhost` resolving to the same IPv4 loopback
/// address this suite's disposable cluster binds, which is not guaranteed
/// on every machine or CI image (an IPv6-first resolver could send
/// `localhost` to `::1` instead, where nothing is listening).
fn run_two_urls_same_schema_test(database_url: String) -> Nil {
  let differently_decorated_url = database_url <> "&grind_test_marker=b"
  differently_decorated_url |> should.not_equal(database_url)
  let assert Ok(validated_a) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database_a) = postgres.start(validated_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(validated_b) =
    postgres.settings(differently_decorated_url) |> postgres.validate
  let assert Ok(database_b) = postgres.start(validated_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)

  let assert Ok(input_codec) =
    worker.codec(
      "cross-endpoint-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "cross-endpoint-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "cross-endpoint.owner",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database_a, "cross-endpoint-owner", definition, 7)

  // Submitted through database_a, read back through database_b — a
  // different connection, reached through a genuinely different connection
  // string, but the same physical schema.
  postgres.state(database_b, handle) |> should.equal(Ok(job.Queued))
  mark_database_test_executed("two-urls-same-schema-share")
}

/// Red-first proof of the `$user` `search_path`-fallback hazard item 1 of
/// the coordinator's review fixes: two roles, each with its own personal,
/// empty schema on `search_path` ahead of `public` (the ordinary PostgreSQL
/// default, `"$user", public`) — a realistic "recommended setup" a caller
/// might reach for without ever calling `postgres.with_schema` — both
/// actually operate against the *same* physical `grind_jobs` table in
/// `public`, because neither personal schema ever holds Grind's own tables.
/// Before `postgres.validate` pinned `search_path` to exactly one
/// explicitly configured schema (default `"public"`, `with_schema`
/// override), `current_schema()` alone — as the advisory lock key's own
/// schema component used to be computed — would report each role's own
/// distinct personal schema (`current_schema()` returns the first schema in
/// `search_path` that merely *exists*, regardless of whether it holds any
/// Grind object at all), so two concurrent `submit_unique` calls for the
/// identical uniqueness key, one per role, would acquire two *different*
/// advisory locks despite both racing to insert into the exact same
/// `public.grind_jobs` table — a genuine duplicate-admission hazard. Now,
/// with neither role ever calling `with_schema` (both simply take the
/// `"public"` default), `search_path` is forced to `"public"` for both
/// regardless of either role's own personal schema, so both share one
/// `Installation` and one advisory lock: this test submits the identical
/// key from both roles concurrently and asserts exactly one is `Inserted`
/// and the other observes it as `Existing` — never two independent rows.
pub fn postgres_user_schema_fallback_shares_one_installation_test() {
  case user_schema_fallback_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_user_schema_fallback_test(database_url)
  }
}

fn run_user_schema_fallback_test(base_url: String) -> Nil {
  let assert Ok(admin_validated) =
    postgres.settings(base_url) |> postgres.validate
  let assert Ok(admin_database) = postgres.start(admin_validated)
  use <- exception.defer(fn() { postgres.close(admin_database) })
  let admin_connection = postgres.connection(admin_database)
  let assert Ok(Nil) = postgres.migrate(admin_database)

  let suffix = int.to_string(unique_test_run_id())
  let role_x = "grind_fallback_x_" <> suffix
  let role_y = "grind_fallback_y_" <> suffix
  create_superuser_role_with_own_empty_schema(admin_connection, role_x)
  create_superuser_role_with_own_empty_schema(admin_connection, role_y)
  use <- exception.defer(fn() {
    drop_isolated_schema_role(admin_connection, role_x)
    drop_isolated_schema_role(admin_connection, role_y)
  })

  // Neither settings value below ever calls `with_schema` — both take the
  // `"public"` default, exactly the point of this test: no per-role
  // configuration is needed to converge on the one real installation.
  let assert Ok(validated_x) =
    postgres.settings(role_scoped_url(base_url, role_x)) |> postgres.validate
  let assert Ok(database_x) = postgres.start(validated_x)
  use <- exception.defer(fn() { postgres.close(database_x) })
  let assert Ok(validated_y) =
    postgres.settings(role_scoped_url(base_url, role_y)) |> postgres.validate
  let assert Ok(database_y) = postgres.start(validated_y)
  use <- exception.defer(fn() { postgres.close(database_y) })

  postgres.installation(database_x)
  |> should.equal(postgres.installation(database_y))

  let assert Ok(input_codec) =
    worker.codec(
      "user-schema-fallback-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "user-schema-fallback-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(
      "user-schema-fallback.worker",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(period) =
    unique.within_milliseconds(60_000, unique.FromInsertion)
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let assert Ok(submission_id_x) =
    submission.submission_id("user-schema-fallback-shared-id")
  let assert Ok(submission.Inserted(handle)) =
    postgres.submit_unique(
      database_x,
      "user-schema-fallback",
      submission_id_x,
      worker_def,
      1,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  let assert Ok(submission_id_y) =
    submission.submission_id("user-schema-fallback-shared-id-2")
  let assert Ok(submission.Existing(conflict)) =
    postgres.submit_unique(
      database_y,
      "user-schema-fallback",
      submission_id_y,
      worker_def,
      1,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  submission.conflict_job_id(conflict) |> should.equal(job.id_value(handle))

  mark_database_test_executed(
    "user-schema-fallback-shares-one-installation-passed",
  )
}

fn create_superuser_role_with_own_empty_schema(
  connection: pog.Connection,
  role: String,
) -> Nil {
  let assert Ok(_) =
    pog.query("CREATE ROLE " <> role <> " LOGIN SUPERUSER")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("CREATE SCHEMA AUTHORIZATION " <> role)
    |> pog.execute(on: connection)
  Nil
}
