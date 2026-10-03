import exception
import gleam/dynamic/decode
import gleam/list
import gleam/string
import gleeunit/should
import grind/internal/migrations
import grind/internal/postgres
import grind/support/env.{
  mark_database_test_executed, migration_collision_resolutions_url,
  migration_collision_submissions_url, schema_atomic_url, schema_bad_url,
  schema_fresh_url, schema_future_foreign_url, schema_markers_url,
  schema_missing_acknowledgements_url, schema_missing_attempt_sequence_url,
  schema_missing_fk_url, schema_missing_jobs_url, schema_missing_migrations_url,
  schema_missing_resolutions_url, schema_missing_unique_submissions_url,
  schema_mixed_case_url, schema_partial_url, schema_shape_url,
}
import grind/support/migration_fixtures.{
  apply_sql_statements, latest_migration, read_sql_statements_from_file,
  schema_marker_max_version,
}
import pog

pub fn postgres_migration_rejects_incompatible_existing_schema_test() {
  case schema_bad_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_incompatible_schema_test(database_url)
  }
}

pub fn postgres_migration_installs_schema_v10_test() {
  case schema_fresh_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_v10_install_test(database_url)
  }
}

fn run_schema_v10_install_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  // Only the real v11 step here: this test's own assertions below (schema
  // shape, and a hand-inserted `succeeded` row with no `finished_at`) are
  // specifically about the v11 baseline, not whatever `migrations()` grows
  // into afterwards. The idempotency re-run further down deliberately calls
  // the full `postgres.migrate` instead, so it also exercises the real v12
  // upgrade (and its `finished_at` backfill) atop these seeded rows.
  let assert Ok(Nil) = postgres.migrate_with(database, [real_v11_migration()])
  let connection = postgres.connection(database)
  let assert Ok(schema) =
    pog.query(
      "SELECT (SELECT count(*) = 1 AND min(version) = 11 AND max(version) = 11 FROM grind_schema_migrations), (SELECT count(*) = 5 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname IN ('grind_schema_migrations', 'grind_jobs', 'grind_job_resolutions', 'grind_job_acknowledgements', 'grind_unique_submissions') AND c.relkind = 'r'), (SELECT count(*) = 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_sequence s ON s.seqrelid = c.oid WHERE n.nspname = current_schema() AND c.relname = 'grind_attempts_id_seq' AND c.relkind = 'S' AND s.seqtypid = 'bigint'::regtype AND s.seqstart = 1 AND s.seqincrement = 1 AND s.seqmin = 1 AND s.seqcache = 1 AND NOT s.seqcycle), (SELECT count(*) = 13 AND count(*) FILTER (WHERE column_name IN ('storage_owner', 'command_id', 'queue', 'job_id', 'worker_id', 'worker_version', 'attempt_id', 'attempt_epoch', 'attempt_owner', 'committed_state', 'failure_cause', 'committed_at', 'proposal_sha256')) = 13 AND count(*) FILTER (WHERE column_name IN ('proposed_state', 'output', 'output_version', 'error', 'error_version', 'failure_description', 'committed_description', 'requested_delay_ms')) = 0 AND count(*) FILTER (WHERE column_name = 'proposal_sha256' AND udt_name = 'bytea') = 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_job_acknowledgements'), (SELECT count(*) = 17 FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND c.convalidated AND c.conname IN ('grind_schema_migrations_pkey', 'grind_jobs_pkey', 'grind_jobs_state_check', 'grind_jobs_max_attempts_check', 'grind_jobs_unique_key_check', 'grind_job_resolutions_pkey', 'grind_job_resolutions_decision_check', 'grind_job_resolutions_target_state_check', 'grind_job_acknowledgements_pkey', 'grind_job_acknowledgements_attempt_key', 'grind_job_acknowledgements_committed_state_check', 'grind_job_acknowledgements_failure_cause_check', 'grind_job_acknowledgements_proposal_sha256_check', 'grind_unique_submissions_pkey', 'grind_unique_submissions_decision_check', 'grind_unique_submissions_request_sha256_check', 'grind_unique_submissions_observed_state_check')), (SELECT count(*) = 2 AND count(*) FILTER (WHERE column_name = 'unique_key_contract' AND udt_name = 'text') = 1 AND count(*) FILTER (WHERE column_name = 'unique_key_sha256' AND udt_name = 'bytea') = 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs' AND column_name IN ('unique_key_contract', 'unique_key_sha256')), (SELECT count(*) = 13 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_unique_submissions'), (SELECT count(*) = 1 FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'grind_jobs' AND indexname = 'grind_jobs_unique_candidate_idx')",
    )
    |> pog.returning({
      use version <- decode.field(0, decode.bool)
      use tables <- decode.field(1, decode.bool)
      use sequence <- decode.field(2, decode.bool)
      use receipt_columns <- decode.field(3, decode.bool)
      use constraints <- decode.field(4, decode.bool)
      use unique_job_columns <- decode.field(5, decode.bool)
      use unique_submission_columns <- decode.field(6, decode.bool)
      use unique_index <- decode.field(7, decode.bool)
      decode.success(#(
        version,
        tables,
        sequence,
        receipt_columns,
        constraints,
        unique_job_columns,
        unique_submission_columns,
        unique_index,
      ))
    })
    |> pog.execute(on: connection)
  let assert [installed] = schema.rows
  installed
  |> should.equal(#(True, True, True, True, True, True, True, True))

  let assert Ok(sequence) =
    pog.query("SELECT nextval('grind_attempts_id_seq')")
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      decode.success(attempt_id)
    })
    |> pog.execute(on: connection)
  let assert [attempt_id] = sequence.rows
  let assert Ok(inserted_job) =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, output, state, available_at) VALUES ('schema-owner', 'default', 'schema.worker', 'v1', 'schema-input-v1', '1'::jsonb, 'schema-output-v1', '\"kept\"'::jsonb, 'succeeded', clock_timestamp()) RETURNING id",
    )
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
    |> pog.execute(on: connection)
  let assert [job_id] = inserted_job.rows
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, proposal_sha256) VALUES ('schema-owner', 'schema-command', 'default', $1, 'schema.worker', 'v1', $2, 1, 'schema-attempt-owner', 'succeeded', sha256(convert_to('synthetic proposal', 'UTF8'))) ",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.execute(on: connection)
  let assert Ok(before) =
    pog.query("SELECT last_value, is_called FROM grind_attempts_id_seq")
    |> pog.returning({
      use last_value <- decode.field(0, decode.int)
      use is_called <- decode.field(1, decode.bool)
      decode.success(#(last_value, is_called))
    })
    |> pog.execute(on: connection)
  let assert [sequence_before] = before.rows

  // Upgrades the seeded-then-frozen v11 schema onto the real, current latest
  // (v12) — twice, proving idempotency — rather than a v11-only re-run, so
  // this also exercises the real `finished_at` backfill against the
  // `succeeded` row seeded above under a schema that had no such column yet.
  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  let assert Ok(preserved) =
    pog.query(
      "SELECT (SELECT count(*) = 2 AND min(version) = 11 AND max(version) = 12 FROM grind_schema_migrations), (SELECT count(*) = 1 FROM grind_jobs WHERE id = $1 AND state = 'succeeded' AND finished_at IS NOT NULL), (SELECT count(*) = 1 FROM grind_job_acknowledgements WHERE command_id = 'schema-command' AND job_id = $1 AND attempt_id = $2 AND proposal_sha256 = sha256(convert_to('synthetic proposal', 'UTF8'))), (SELECT last_value = $2 AND is_called FROM grind_attempts_id_seq)",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.returning({
      use version <- decode.field(0, decode.bool)
      use job <- decode.field(1, decode.bool)
      use receipt <- decode.field(2, decode.bool)
      use sequence <- decode.field(3, decode.bool)
      decode.success(#(version, job, receipt, sequence))
    })
    |> pog.execute(on: connection)
  let assert [preserved_data] = preserved.rows
  preserved_data |> should.equal(#(True, True, True, True))
  let assert Ok(after) =
    pog.query("SELECT last_value, is_called FROM grind_attempts_id_seq")
    |> pog.returning({
      use last_value <- decode.field(0, decode.int)
      use is_called <- decode.field(1, decode.bool)
      decode.success(#(last_value, is_called))
    })
    |> pog.execute(on: connection)
  after.rows |> should.equal([sequence_before])
  mark_database_test_executed("schema-v11-fresh-install-idempotent-passed")
}

pub fn postgres_migration_rejects_legacy_and_future_markers_test() {
  case schema_markers_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_marker_rejection_test(database_url)
  }
}

fn run_schema_marker_rejection_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  // Only the real v11 step: the "marker=11 alone" case below re-applies the
  // real, full `migrations()` on top of this genuinely v11-only physical
  // schema, so it exercises an actual v11-to-v12 upgrade rather than a no-op
  // against a schema already fully at latest.
  let assert Ok(Nil) = postgres.migrate_with(database, [real_v11_migration()])
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_schema_migrations (version) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)",
    )
    |> pog.execute(on: connection)
  let assert Error(_) = postgres.migrate(database)
  let assert Ok(legacy_marker) =
    pog.query(
      "SELECT count(*)::bigint, min(version)::bigint, max(version)::bigint FROM grind_schema_migrations",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      use minimum <- decode.field(1, decode.int)
      use maximum <- decode.field(2, decode.int)
      decode.success(#(count, minimum, maximum))
    })
    |> pog.execute(on: connection)
  legacy_marker.rows |> should.equal([#(9, 1, 9)])

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (8)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(8)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (9)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(9)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (10)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(10)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (11)")
    |> pog.execute(on: connection)
  // The marker claims only v11, but this call runs the real `migrations()`
  // (v11 and v12), and the physical schema really is v11-only at this point
  // — so this is a genuine upgrade to v12, not a no-op re-run.
  postgres.migrate(database) |> should.equal(Ok(Nil))

  // The physical schema is now genuinely v12 (from the real upgrade just
  // above). A marker set of `{12}` alone is still rejected — not because
  // `12` is unknown (it is now the real latest), but because a valid marker
  // set is always the contiguous range starting at the baseline (`11`): a
  // schema can never legitimately reach `12` without a `11` marker also on
  // record, so this is `IncompatibleSchema`, never `UnsupportedSchemaVersion`.
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (12)")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))

  // A marker genuinely beyond this build's own highest known version (`12`)
  // is the real "future schema" case `UnsupportedSchemaVersion` exists for.
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (13)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(13)))

  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (11)")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (8)")
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  mark_database_test_executed("legacy-future-schema-markers-rejected")
}

pub fn postgres_migration_refuses_missing_owned_artifacts_test() {
  case
    schema_missing_jobs_url(),
    schema_missing_migrations_url(),
    schema_missing_resolutions_url(),
    schema_missing_acknowledgements_url(),
    schema_missing_attempt_sequence_url(),
    schema_missing_unique_submissions_url()
  {
    Ok(jobs),
      Ok(migrations),
      Ok(resolutions),
      Ok(acknowledgements),
      Ok(sequence),
      Ok(unique_submissions)
    -> {
      run_missing_schema_artifact_test(jobs, "grind_jobs", False)
      run_missing_schema_artifact_test(
        migrations,
        "grind_schema_migrations",
        False,
      )
      run_missing_schema_artifact_test(
        resolutions,
        "grind_job_resolutions",
        False,
      )
      run_missing_schema_artifact_test(
        acknowledgements,
        "grind_job_acknowledgements",
        False,
      )
      run_missing_schema_artifact_test(sequence, "grind_attempts_id_seq", True)
      // The exact object-count shape a dropped `grind_unique_submissions`
      // leaves behind is identical to a never-migrated schema v10 install
      // (four tables, the same attempt sequence). This proves the two are
      // not confused: a real v11 install missing only this table still
      // fails closed as `IncompatibleSchema`, not as the friendly
      // `UnsupportedSchemaVersion(10)` reserved for a genuine legacy
      // install (see `read_legacy_schema_marker` in `src/grind/postgres.gleam`).
      run_missing_schema_artifact_test(
        unique_submissions,
        "grind_unique_submissions",
        False,
      )
      mark_database_test_executed("missing-schema-artifacts-not-repaired")
    }
    _, _, _, _, _, _ -> Nil
  }
}

fn run_missing_schema_artifact_test(
  database_url: String,
  artifact: String,
  is_sequence: Bool,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let drop_statement = case is_sequence {
    True -> "DROP SEQUENCE " <> artifact
    // `CASCADE`: `grind_jobs` is now the referenced side of three foreign
    // keys (`grind_v12`'s own `ON DELETE CASCADE` constraints), so a plain
    // `DROP TABLE grind_jobs` alone fails with `2BP01
    // dependent_objects_still_exist` instead of ever reaching the
    // `IncompatibleSchema` check this test is about. Harmless for the other
    // artifacts this same helper drops, since nothing references them.
    False -> "DROP TABLE " <> artifact <> " CASCADE"
  }
  let assert Ok(_) = pog.query(drop_statement) |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(missing) =
    pog.query("SELECT to_regclass(current_schema() || '.' || $1) IS NULL")
    |> pog.parameter(pog.text(artifact))
    |> pog.returning({
      use absent <- decode.field(0, decode.bool)
      decode.success(absent)
    })
    |> pog.execute(on: connection)
  missing.rows |> should.equal([True])
}

/// Increment 25: `v12_foreign_keys` extends the physical-shape check with a
/// `pg_constraint` lookup independent of `v12_shape`'s own `pg_class`
/// relation check (a plain foreign key backs no relation of its own) — a
/// database missing one of `grind_v12`'s three `ON DELETE CASCADE`
/// constraints (dropped by hand, here) must fail closed exactly like a
/// missing relation does, not silently pass as though the receipt-orphan
/// backstop `docs/RECOVERY-EVIDENCE.md` Increment 24 describes were still
/// in place.
pub fn postgres_migration_missing_foreign_key_shape_detected_test() {
  case schema_missing_fk_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_missing_foreign_key_test(database_url)
  }
}

fn run_missing_foreign_key_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_job_id_fkey",
    )
    |> pog.execute(on: connection)
  postgres.migrate(database) |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(dropped) =
    pog.query(
      "SELECT count(*) = 0 FROM pg_constraint WHERE conname = 'grind_job_acknowledgements_job_id_fkey'",
    )
    |> pog.returning({
      use absent <- decode.field(0, decode.bool)
      decode.success(absent)
    })
    |> pog.execute(on: connection)
  dropped.rows |> should.equal([True])
  mark_database_test_executed("missing-foreign-key-not-repaired")
}

pub fn postgres_migration_fresh_install_is_atomic_test() {
  case schema_atomic_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_schema_atomic_install_test(database_url)
  }
}

fn run_schema_atomic_install_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION fail_grind_ack_table_creation() RETURNS event_trigger LANGUAGE plpgsql AS $body$ DECLARE command record; BEGIN FOR command IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP IF command.object_identity LIKE '%grind_job_acknowledgements' THEN RAISE EXCEPTION 'injected Grind schema failure'; END IF; END LOOP; END $body$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE EVENT TRIGGER fail_grind_ack_table_creation ON ddl_command_end EXECUTE FUNCTION fail_grind_ack_table_creation()",
    )
    |> pog.execute(on: connection)
  let assert Error(_) = postgres.migrate(database)
  let assert Ok(_) =
    pog.query("DROP EVENT TRIGGER fail_grind_ack_table_creation")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP FUNCTION fail_grind_ack_table_creation()")
    |> pog.execute(on: connection)
  let assert Ok(rolled_back) =
    pog.query(
      "SELECT count(*)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname IN ('grind_schema_migrations', 'grind_jobs', 'grind_job_resolutions', 'grind_job_acknowledgements', 'grind_attempts_id_seq')",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  rolled_back.rows |> should.equal([0])
  mark_database_test_executed("failed-fresh-install-rolled-back")
}

/// A marker claiming a schema version newer than this build's own
/// `migrations()` (`UnsupportedSchemaVersion`) must be detected *before*
/// `read_schema_generation` ever runs any per-version shape check — proven
/// here to actually discriminate: install a real, current-latest (v12)
/// schema, then *break* its own declared shape (drop
/// `grind_unique_submissions`, one of its required relations) and add a
/// `13` marker (one past this build's own real latest, `12`) on top. A
/// shape-first implementation would evaluate the now-broken shape and
/// misreport `IncompatibleSchema` (or, worse, never notice the higher
/// marker at all); the required version-first ordering still reports
/// `UnsupportedSchemaVersion(13)` regardless — the version check never
/// reaches a shape check at all once `max > latest`.
pub fn postgres_migration_future_version_precedes_shape_check_test() {
  case schema_future_foreign_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_future_version_with_foreign_objects_test(database_url)
  }
}

fn run_future_version_with_foreign_objects_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("DROP TABLE grind_unique_submissions")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (13)")
    |> pog.execute(on: connection)
  postgres.migrate(database)
  |> should.equal(Error(postgres.UnsupportedSchemaVersion(13)))
  mark_database_test_executed("future-version-precedes-shape-check-passed")
}

/// A bare `current_schema() || '.grind_schema_migrations'` fed to
/// `to_regclass` silently folds an unquoted, mixed-case schema name to lower
/// case, so `to_regclass` looks up a schema that does not exist and
/// `schema_migrations_table_exists` wrongly reports the marker table
/// absent even once it is genuinely installed and fully functional —
/// `quote_ident` fixes it, both there and in the `search_path` connection
/// parameter `postgres.validate` sets from `postgres.with_schema`: the
/// second `migrate` call must be a clean `Ok(Nil)` no-op.
pub fn postgres_migration_quotes_mixed_case_schema_name_test() {
  case schema_mixed_case_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_mixed_case_schema_test(database_url)
  }
}

fn run_mixed_case_schema_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_schema("MixedCase")
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  let connection = postgres.connection(database)
  let assert Ok(installed_in_mixed_case_schema) =
    pog.query(
      "SELECT current_schema() = 'MixedCase' AND to_regclass('grind_schema_migrations') IS NOT NULL",
    )
    |> pog.returning({
      use present <- decode.field(0, decode.bool)
      decode.success(present)
    })
    |> pog.execute(on: connection)
  installed_in_mixed_case_schema.rows |> should.equal([True])
  mark_database_test_executed("migrate-mixed-case-schema-no-op-passed")
}

/// The real, released `v11` step, looked up from `migrations.migrations()`
/// rather than hand-duplicated.
fn real_v11_migration() -> migrations.Migration {
  let assert Ok(step) =
    list.find(migrations.migrations(), fn(step) { step.version == 11 })
  step
}

/// A synthetic version `migrate_with`-only test appends after the real
/// `migrations()` baseline — never part of `grind/internal/migrations`
/// itself, and never released. Its own statements are deliberately trivial
/// (one throwaway table plus the marker insert) since only the runner's
/// step-by-step commit/skip behaviour is under test here, not any real
/// schema change. Numbered `13` (one past the real, current highest version)
/// rather than `12`, since `12` is now a genuine released step.
fn synthetic_v13_ok_migration() -> migrations.Migration {
  migrations.Migration(
    13,
    [
      "CREATE TABLE grind_test_synthetic_v13 (id integer PRIMARY KEY)",
      "INSERT INTO grind_schema_migrations (version) VALUES (13)",
    ],
    list.append(latest_migration().shape, [
      migrations.ExpectedRelation(
        "grind_test_synthetic_v13",
        migrations.Table,
        [],
      ),
      // `id integer PRIMARY KEY` also creates this backing index implicitly.
      migrations.ExpectedRelation(
        "grind_test_synthetic_v13_pkey",
        migrations.Index,
        [],
      ),
    ]),
    latest_migration().foreign_keys,
    latest_migration().forbidden_columns,
  )
}

/// Like `synthetic_v13_ok_migration`, but its own `CREATE TABLE` is the
/// exact object identity `install_synthetic_v14_failure_trigger` arms an
/// event trigger to reject.
fn synthetic_v14_migration() -> migrations.Migration {
  migrations.Migration(
    14,
    [
      "CREATE TABLE grind_test_synthetic_v14 (id integer PRIMARY KEY)",
      "INSERT INTO grind_schema_migrations (version) VALUES (14)",
    ],
    list.append(synthetic_v13_ok_migration().shape, [
      migrations.ExpectedRelation(
        "grind_test_synthetic_v14",
        migrations.Table,
        [],
      ),
      migrations.ExpectedRelation(
        "grind_test_synthetic_v14_pkey",
        migrations.Index,
        [],
      ),
    ]),
    synthetic_v13_ok_migration().foreign_keys,
    synthetic_v13_ok_migration().forbidden_columns,
  )
}

/// Installs a `ddl_command_end` event trigger that raises whenever
/// `grind_test_synthetic_v14` is created — the same fault-injection shape
/// `run_schema_atomic_install_test` above uses against a real Grind table,
/// aimed instead at the partial-failure test's own synthetic step 14.
fn install_synthetic_v14_failure_trigger(connection: pog.Connection) -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION fail_grind_synthetic_v14() RETURNS event_trigger LANGUAGE plpgsql AS $body$ DECLARE command record; BEGIN FOR command IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP IF command.object_identity LIKE '%grind_test_synthetic_v14' THEN RAISE EXCEPTION 'injected Grind migration failure'; END IF; END LOOP; END $body$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE EVENT TRIGGER fail_grind_synthetic_v14 ON ddl_command_end EXECUTE FUNCTION fail_grind_synthetic_v14()",
    )
    |> pog.execute(on: connection)
  Nil
}

fn drop_synthetic_v14_failure_trigger(connection: pog.Connection) -> Nil {
  let assert Ok(_) =
    pog.query("DROP EVENT TRIGGER fail_grind_synthetic_v14")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DROP FUNCTION fail_grind_synthetic_v14()")
    |> pog.execute(on: connection)
  Nil
}

/// `migrate_with(migrations() ++ [synthetic v13 ok, synthetic v14 failing])`
/// on a fresh schema: step 11, step 12 (real), and step 13 each commit in
/// their own transaction before step 14's own transaction rolls back on its
/// injected failure — proving a mid-run failure neither undoes earlier
/// committed steps nor leaves the failed step's own partial work behind, and
/// that a second `migrate_with` call (fault removed) picks up exactly where
/// the first left off.
pub fn postgres_migrate_with_partial_failure_preserves_earlier_steps_test() {
  case schema_partial_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_partial_failure_migration_test(database_url)
  }
}

fn run_partial_failure_migration_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  install_synthetic_v14_failure_trigger(connection)

  let steps =
    list.append(migrations.migrations(), [
      synthetic_v13_ok_migration(),
      synthetic_v14_migration(),
    ])
  let assert Error(postgres.MigrationStepFailed(14, _)) =
    postgres.migrate_with(database, steps)

  let assert Ok(markers) =
    pog.query(
      "SELECT array_agg(version ORDER BY version) FROM grind_schema_migrations",
    )
    |> pog.returning({
      use versions <- decode.field(0, decode.list(decode.int))
      decode.success(versions)
    })
    |> pog.execute(on: connection)
  markers.rows |> should.equal([[11, 12, 13]])
  let assert Ok(objects) =
    pog.query(
      "SELECT to_regclass(current_schema() || '.grind_test_synthetic_v13') IS NOT NULL, to_regclass(current_schema() || '.grind_test_synthetic_v14') IS NOT NULL",
    )
    |> pog.returning({
      use v13_present <- decode.field(0, decode.bool)
      use v14_present <- decode.field(1, decode.bool)
      decode.success(#(v13_present, v14_present))
    })
    |> pog.execute(on: connection)
  objects.rows |> should.equal([#(True, False)])

  drop_synthetic_v14_failure_trigger(connection)
  postgres.migrate_with(database, steps) |> should.equal(Ok(Nil))
  let assert Ok(resumed_markers) =
    pog.query(
      "SELECT array_agg(version ORDER BY version) FROM grind_schema_migrations",
    )
    |> pog.returning({
      use versions <- decode.field(0, decode.list(decode.int))
      decode.success(versions)
    })
    |> pog.execute(on: connection)
  resumed_markers.rows |> should.equal([[11, 12, 13, 14]])
  mark_database_test_executed("migrate-partial-failure-resumes-passed")
}

/// A version's own marker being present is never trusted alone: dropping a
/// relation a later version's own declared `shape` requires, after that
/// version's marker was genuinely committed, must still be caught as
/// `IncompatibleSchema` on the next `migrate_with` call — proven against a
/// synthetic v13 here since the real v11/v12 baseline's own equivalent case
/// is already covered by `postgres_migration_refuses_missing_owned_artifacts_test`.
pub fn postgres_migrate_detects_missing_relation_in_declared_shape_test() {
  case schema_shape_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_missing_relation_shape_test(database_url)
  }
}

fn run_missing_relation_shape_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  let steps =
    list.append(migrations.migrations(), [synthetic_v13_ok_migration()])
  postgres.migrate_with(database, steps) |> should.equal(Ok(Nil))
  let assert Ok(_) =
    pog.query("DROP TABLE grind_test_synthetic_v13")
    |> pog.execute(on: connection)
  postgres.migrate_with(database, steps)
  |> should.equal(Error(postgres.IncompatibleSchema))
  mark_database_test_executed("migrate-missing-relation-shape-detected")
}

/// Automated (not merely manual/psql) proof of `v12_statements`'s own
/// `grind_unique_submissions` collision-detection `DO` block: a frozen v11
/// fixture seeded with two distinct `storage_owner` values sharing one
/// `submission_id` makes `migrate` fail with the named
/// `MigrationStepFailed(12, _)` message, and the schema marker stays at 11
/// (this step's own transaction rolled back cleanly, never partially
/// applied) — see `v12_statements`'s own doc comment, "Dropping
/// `storage_owner`", and `docs/RECOVERY-EVIDENCE.md`.
pub fn postgres_migration_submission_collision_detected_test() {
  case migration_collision_submissions_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_migration_submission_collision_test(database_url)
  }
}

fn run_migration_submission_collision_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  apply_sql_statements(
    connection,
    read_sql_statements_from_file("test/fixtures/schema/v11.sql"),
  )
  // A real `grind_jobs` row per side: `v12_statements`'s own orphan cleanup
  // (`DELETE FROM grind_unique_submissions r WHERE NOT EXISTS (SELECT 1 FROM
  // grind_jobs j WHERE j.id = r.job_id)`, Increment 25) runs *before* the
  // collision `DO` block, so a receipt naming a `job_id` with no real row
  // would otherwise be silently swept away as an orphan before the
  // collision it seeds is ever checked — this bit this exact test on its
  // first draft (both seeded rows vanished with zero rows left, "proving"
  // no collision existed) before this fix.
  let job_id_a = seed_legacy_job(connection, "collision-owner-a")
  let job_id_b = seed_legacy_job(connection, "collision-owner-b")
  // Two distinct `storage_owner` values sharing one `submission_id` — legal
  // under v11's own `(storage_owner, submission_id)` primary key, exactly
  // the shape `v12_statements`'s own `grind_unique_submissions` `DO` block
  // exists to catch before it would otherwise silently merge onto v12's
  // `(submission_id)`-only key.
  seed_legacy_submission(connection, "collision-owner-a", job_id_a, "a")
  seed_legacy_submission(connection, "collision-owner-b", job_id_b, "b")

  case postgres.migrate(database) {
    Error(postgres.MigrationStepFailed(12, pog.PostgresqlError(_, _, message))) ->
      message
      |> should_contain("two distinct storage owners share a submission_id")
    other ->
      panic as {
        "expected MigrationStepFailed(12, _), got " <> string.inspect(other)
      }
  }
  schema_marker_max_version(connection) |> should.equal(11)
  mark_database_test_executed("migration-submission-collision-detected")
}

/// The `grind_job_resolutions` sibling of
/// `postgres_migration_submission_collision_detected_test`: two distinct
/// `storage_owner` values sharing one `resolution_id`, legal under v11's own
/// `(storage_owner, resolution_id)` primary key, must make `migrate` fail
/// the same way — `MigrationStepFailed(12, _)`, marker still 11 — via
/// `v12_statements`'s `grind_job_resolutions` `DO` block, which runs earlier
/// in `v12_statements` than the submissions one, so this needs its own
/// database rather than sharing the previous test's (that one would never
/// reach its own submissions collision, since the migration step's single
/// transaction fails, and rolls back, at whichever `DO` block it hits
/// first).
pub fn postgres_migration_resolution_collision_detected_test() {
  case migration_collision_resolutions_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_migration_resolution_collision_test(database_url)
  }
}

fn run_migration_resolution_collision_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  apply_sql_statements(
    connection,
    read_sql_statements_from_file("test/fixtures/schema/v11.sql"),
  )
  // See `run_migration_submission_collision_test`'s own comment: a real
  // `grind_jobs` row per side is required, or `v12_statements`'s own orphan
  // cleanup deletes both seeded receipt rows before the collision `DO` block
  // ever runs.
  let job_id_a = seed_legacy_job(connection, "collision-owner-a")
  let job_id_b = seed_legacy_job(connection, "collision-owner-b")
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ('collision-owner-a', 'default', $1, 'collision.worker', 'v1', 'shared-resolution-id', 1, 1, 'collision-attempt-owner', clock_timestamp(), 'authorize_replay', 'queued', 'on-call', 'collision test a')",
    )
    |> pog.parameter(pog.int(job_id_a))
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, target_state, resolved_by, details) VALUES ('collision-owner-b', 'default', $1, 'collision.worker', 'v1', 'shared-resolution-id', 1, 1, 'collision-attempt-owner', clock_timestamp(), 'authorize_replay', 'queued', 'on-call', 'collision test b')",
    )
    |> pog.parameter(pog.int(job_id_b))
    |> pog.execute(on: connection)

  case postgres.migrate(database) {
    Error(postgres.MigrationStepFailed(12, pog.PostgresqlError(_, _, message))) ->
      message
      |> should_contain("two distinct storage owners share a resolution_id")
    other ->
      panic as {
        "expected MigrationStepFailed(12, _), got " <> string.inspect(other)
      }
  }
  schema_marker_max_version(connection) |> should.equal(11)
  mark_database_test_executed("migration-resolution-collision-detected")
}

fn should_contain(haystack: String, needle: String) -> Nil {
  case string.contains(haystack, needle) {
    True -> Nil
    False ->
      panic as {
        "expected " <> string.inspect(haystack) <> " to contain " <> needle
      }
  }
}

/// Seeds one minimal, real `grind_jobs` row against the frozen v11 fixture
/// (`storage_owner` still `NOT NULL`, no default) and returns its id — used
/// by the migration-collision tests so their own seeded receipt rows name a
/// `job_id` that genuinely exists, never an orphan `v12_statements`'s own
/// cleanup `DELETE` (Increment 25) would otherwise remove before the
/// collision `DO` block ever runs.
fn seed_legacy_job(connection: pog.Connection, storage_owner: String) -> Int {
  let query =
    pog.query(
      "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, state, available_at) VALUES ($1, 'default', 'collision.worker', 'v1', 'collision-input-v1', '1'::jsonb, 'collision-output-v1', 'queued', clock_timestamp()) RETURNING id",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  let assert Ok(returned) = pog.execute(query, on: connection)
  let assert [id] = returned.rows
  id
}

/// Seeds one legacy `grind_unique_submissions` receipt row against the
/// frozen v11 fixture, sharing `submission_id: "shared-submission-id"`
/// across every `discriminator` this is called with — see
/// `run_migration_submission_collision_test`.
fn seed_legacy_submission(
  connection: pog.Connection,
  storage_owner: String,
  job_id: Int,
  discriminator: String,
) -> Nil {
  let query =
    pog.query(
      "INSERT INTO grind_unique_submissions (storage_owner, submission_id, queue, worker_id, worker_version, request_sha256, decision, job_id, job_queue, observed_state) VALUES ($1, 'shared-submission-id', 'default', 'collision.worker', 'v1', sha256(convert_to($2, 'UTF8')), 'inserted', $3, 'default', 'queued')",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text("collision-" <> discriminator))
    |> pog.parameter(pog.int(job_id))
  let assert Ok(_) = pog.execute(query, on: connection)
  Nil
}

fn run_incompatible_schema_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE grind_jobs (id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, output_version text NOT NULL, output jsonb, error_version text, error jsonb, state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state <> 'executing' AND state <> 'succeeded'), available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, failure_description text)",
    )
    |> pog.execute(on: connection)

  postgres.migrate(database)
  |> should.equal(Error(postgres.IncompatibleSchema))
  let assert Ok(unrepaired) =
    pog.query(
      "SELECT to_regclass(current_schema() || '.grind_schema_migrations') IS NULL, to_regclass(current_schema() || '.grind_job_resolutions') IS NULL, to_regclass(current_schema() || '.grind_job_acknowledgements') IS NULL, to_regclass(current_schema() || '.grind_attempts_id_seq') IS NULL",
    )
    |> pog.returning({
      use migrations <- decode.field(0, decode.bool)
      use resolutions <- decode.field(1, decode.bool)
      use acknowledgements <- decode.field(2, decode.bool)
      use attempt_sequence <- decode.field(3, decode.bool)
      decode.success(#(
        migrations,
        resolutions,
        acknowledgements,
        attempt_sequence,
      ))
    })
    |> pog.execute(on: connection)
  unrepaired.rows |> should.equal([#(True, True, True, True)])
  mark_database_test_executed("incompatible-schema-rejected")
}

/// Pure, in-memory coverage of `postgres.validate`'s schema-name rejection
/// — no PostgreSQL connection involved, since `validate` itself never
/// acquires one. Covers every condition `ConfigError.InvalidSchema`'s own
/// doc comment names: empty, over PostgreSQL's 63-*byte* `NAMEDATALEN`
/// limit (both a 64-byte ASCII name and a multibyte name that crosses 63
/// bytes at well under 63 *characters*), a NUL byte, the literal `"$user"`
/// token, and the reserved `pg_` prefix — plus the boundary case (exactly
/// 63 bytes) that must still be accepted.
fn schema_settings(schema: String) -> postgres.Settings {
  postgres.settings("postgres://grind@127.0.0.1:5432/unused")
  |> postgres.with_schema(schema)
}

pub fn postgres_schema_rejects_empty_name_test() {
  schema_settings("")
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidSchema))
}

pub fn postgres_schema_rejects_64_byte_ascii_name_test() {
  schema_settings(string.repeat("a", 64))
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidSchema))
}

pub fn postgres_schema_rejects_multibyte_name_crossing_63_bytes_test() {
  // "é" is 2 bytes in UTF-8; 32 of them is 64 bytes at only 32 characters —
  // over PostgreSQL's own byte-counted `NAMEDATALEN` limit despite looking
  // short by character count.
  schema_settings(string.repeat("é", 32))
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidSchema))
}

pub fn postgres_schema_rejects_nul_byte_test() {
  schema_settings("bad\u{0}name")
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidSchema))
}

pub fn postgres_schema_accepts_exactly_63_bytes_test() {
  case schema_settings(string.repeat("a", 63)) |> postgres.validate {
    Ok(_) -> Nil
    Error(_) -> should.fail()
  }
}

pub fn postgres_schema_rejects_dollar_user_test() {
  schema_settings("$user")
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidSchema))
}

pub fn postgres_schema_rejects_pg_prefix_test() {
  schema_settings("pg_temp")
  |> postgres.validate
  |> should.equal(Error(postgres.InvalidSchema))
}
