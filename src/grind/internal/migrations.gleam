//// Grind's hand-maintained schema migrations. This module is the single
//// source of truth `grind/postgres.migrate` executes against at runtime;
//// Grind never reads `priv/migrations/*.sql` itself. Those files exist so
//// an application can install or inspect Grind's schema through cigogne
//// (`cigogne.include_lib`/`gleam run -m cigogne`) instead of through Grind's
//// own `postgres.migrate` — see the README, "Migrations". Their up sections
//// are kept byte-identical (statement text, minus the trailing `;` a `.sql`
//// file needs) to each `Migration.statements` below, including the
//// advisory-lock statement every version's list starts with, so an
//// application applying migrations directly through cigogne serialises
//// against a concurrent `postgres.migrate` caller exactly the same way;
//// `grind_migrations_conformance_test` (test/grind_test.gleam) proves this
//// with cigogne's own public parser, with no database required.
////
//// See AGENTS.md, "Adding a migration", for the steps to add a new version
//// here.

/// A relation kind as PostgreSQL's own `pg_class.relkind` distinguishes them
/// (`r`/`S`/`i`) — never a bare string, so a typo can't silently widen or
/// narrow a shape check.
pub type RelationKind {
  Table
  Sequence
  Index
}

/// One object a version's *cumulative* shape requires to exist — every
/// version's `shape` lists the complete `grind_`-prefixed object set once
/// that version and every one before it have applied, not just what that
/// one version's own statements added. `key_columns` additionally requires
/// those columns to exist on the relation (used for `grind_jobs`'s
/// uniqueness key columns, added without a dedicated migration when the
/// schema was still pre-release); empty for a relation that needs no
/// column-level check beyond its own existence.
pub type ExpectedRelation {
  ExpectedRelation(name: String, kind: RelationKind, key_columns: List(String))
}

/// One released schema version: its own statements (in the order
/// `postgres.migrate_with` runs them, including the trailing
/// `grind_schema_migrations` marker `INSERT`) and its cumulative expected
/// shape, checked by `postgres.read_schema_generation` before trusting a
/// marker claiming this version is genuinely installed.
pub type Migration {
  Migration(
    version: Int,
    statements: List(String),
    shape: List(ExpectedRelation),
  )
}

/// The advisory-lock statement every version's own `statements` list starts
/// with — see the module doc comment above for why this lives in the
/// statement text itself rather than only as a separate Gleam-side call.
/// Must stay identical to `postgres`'s own hardcoded copy (used so
/// `postgres.migrate_with` still locks even if a future version's author
/// forgets to keep this as their own first statement).
pub fn advisory_lock_statement() -> String {
  "SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) l"
}

/// One entry per released schema version, in the order `postgres.migrate`
/// (`migrate_with(database, migrations())`) applies them.
///
/// `@internal`: exposed only for `postgres.migrate_with` and the test suite,
/// never part of the public API.
@internal
pub fn migrations() -> List(Migration) {
  [Migration(11, v11_statements(), v11_shape())]
}

fn v11_statements() -> List(String) {
  [
    advisory_lock_statement(),
    "CREATE TABLE grind_schema_migrations (version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp())",
    "CREATE TABLE grind_jobs ("
      <> "id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, "
      <> "worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, "
      <> "output_version text NOT NULL, output jsonb, error_version text, error jsonb, "
      <> "state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state IN ('queued', 'scheduled', 'retryable', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain', 'discarded', 'cancelled')), "
      <> "available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
      <> "attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, "
      <> "lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, max_attempts bigint NOT NULL DEFAULT 20, delivery_count bigint NOT NULL DEFAULT 0, snooze_count bigint NOT NULL DEFAULT 0, failure_description text, failure_cause text, uncertain_at timestamptz, cancel_requested_at timestamptz, "
      <> "unique_key_contract text, unique_key_sha256 bytea, "
      <> "CONSTRAINT grind_jobs_max_attempts_check CHECK (max_attempts > 0), "
      <> "CONSTRAINT grind_jobs_unique_key_check CHECK ((unique_key_contract IS NULL) = (unique_key_sha256 IS NULL) AND (unique_key_sha256 IS NULL OR octet_length(unique_key_sha256) = 32)))",
    "CREATE INDEX grind_jobs_unique_candidate_idx ON grind_jobs (storage_owner, worker_id, worker_version, unique_key_contract, unique_key_sha256) WHERE unique_key_sha256 IS NOT NULL",
    "CREATE TABLE grind_job_resolutions ("
      <> "storage_owner text NOT NULL, queue text NOT NULL, job_id bigint NOT NULL, worker_id text, worker_version text, "
      <> "resolution_id text NOT NULL, attempt_id bigint NOT NULL, attempt_epoch bigint NOT NULL, "
      <> "attempt_owner text NOT NULL, lease_expires_at timestamptz NOT NULL, "
      <> "decision text NOT NULL CONSTRAINT grind_job_resolutions_decision_check CHECK (decision IN ('confirm_success', 'confirm_business_failure', 'authorize_replay')), "
      <> "target_state text NOT NULL CONSTRAINT grind_job_resolutions_target_state_check CHECK (target_state IN ('queued', 'succeeded', 'business_failed')), "
      <> "payload_version text, payload jsonb, "
      <> "resolved_by text NOT NULL, details text NOT NULL, resolved_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
      <> "CONSTRAINT grind_job_resolutions_pkey PRIMARY KEY (storage_owner, resolution_id))",
    "CREATE TABLE grind_job_acknowledgements ("
      <> "storage_owner text NOT NULL, command_id text NOT NULL, queue text NOT NULL, job_id bigint NOT NULL, "
      <> "worker_id text NOT NULL, worker_version text NOT NULL, attempt_id bigint NOT NULL, attempt_epoch bigint NOT NULL, "
      <> "attempt_owner text NOT NULL, committed_state text NOT NULL CONSTRAINT grind_job_acknowledgements_committed_state_check CHECK (committed_state IN ('succeeded', 'business_failed', 'retryable', 'runtime_failed', 'scheduled', 'discarded', 'cancelled', 'uncertain')), "
      <> "failure_cause text CONSTRAINT grind_job_acknowledgements_failure_cause_check CHECK (failure_cause IS NULL OR failure_cause IN ('budget_exhausted', 'retry_declined')), "
      <> "proposal_sha256 bytea NOT NULL CONSTRAINT grind_job_acknowledgements_proposal_sha256_check CHECK (octet_length(proposal_sha256) = 32), "
      <> "committed_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
      <> "CONSTRAINT grind_job_acknowledgements_pkey PRIMARY KEY (storage_owner, command_id), "
      <> "CONSTRAINT grind_job_acknowledgements_attempt_key UNIQUE (storage_owner, job_id, attempt_id, attempt_epoch))",
    "CREATE SEQUENCE grind_attempts_id_seq AS bigint START WITH 1 INCREMENT BY 1 MINVALUE 1 CACHE 1 NO CYCLE",
    "CREATE TABLE grind_unique_submissions ("
      <> "storage_owner text NOT NULL, submission_id text NOT NULL, queue text NOT NULL, "
      <> "worker_id text NOT NULL, worker_version text NOT NULL, "
      <> "request_sha256 bytea NOT NULL CONSTRAINT grind_unique_submissions_request_sha256_check CHECK (octet_length(request_sha256) = 32), "
      <> "decision text NOT NULL CONSTRAINT grind_unique_submissions_decision_check CHECK (decision IN ('inserted', 'existing', 'rescheduled')), "
      <> "job_id bigint NOT NULL, job_queue text NOT NULL, "
      <> "observed_state text NOT NULL CONSTRAINT grind_unique_submissions_observed_state_check CHECK (observed_state IN ('queued', 'scheduled', 'retryable', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain', 'discarded', 'cancelled')), "
      <> "decided_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
      <> "rescheduled_from timestamptz, rescheduled_to timestamptz, "
      <> "CONSTRAINT grind_unique_submissions_pkey PRIMARY KEY (storage_owner, submission_id))",
    "INSERT INTO grind_schema_migrations (version) VALUES (11)",
  ]
}

/// The *complete* set of `grind_`-prefixed relations a real v11 install
/// produces, including every implicit object PostgreSQL itself creates
/// alongside an explicit `CREATE TABLE` — a `bigserial` column's own
/// sequence (`grind_jobs_id_seq`) and the backing index PostgreSQL creates
/// for every `PRIMARY KEY`/`UNIQUE` constraint, named after the constraint
/// (explicit or auto-generated). Confirmed empirically against a real,
/// freshly installed v11 schema (`SELECT relname, relkind FROM pg_class
/// WHERE relname LIKE 'grind\_%'`), not derived from the DDL text alone —
/// easy to under-count otherwise, exactly as the first version of this
/// shape did (missing `grind_jobs_id_seq`, `grind_jobs_pkey`,
/// `grind_schema_migrations_pkey`, `grind_job_resolutions_pkey`,
/// `grind_job_acknowledgements_pkey`, `grind_job_acknowledgements_attempt_key`,
/// and `grind_unique_submissions_pkey`).
fn v11_shape() -> List(ExpectedRelation) {
  [
    ExpectedRelation("grind_schema_migrations", Table, []),
    ExpectedRelation("grind_schema_migrations_pkey", Index, []),
    ExpectedRelation("grind_jobs", Table, [
      "unique_key_contract", "unique_key_sha256",
    ]),
    ExpectedRelation("grind_jobs_id_seq", Sequence, []),
    ExpectedRelation("grind_jobs_pkey", Index, []),
    ExpectedRelation("grind_jobs_unique_candidate_idx", Index, []),
    ExpectedRelation("grind_job_resolutions", Table, []),
    ExpectedRelation("grind_job_resolutions_pkey", Index, []),
    ExpectedRelation("grind_job_acknowledgements", Table, []),
    ExpectedRelation("grind_job_acknowledgements_pkey", Index, []),
    ExpectedRelation("grind_job_acknowledgements_attempt_key", Index, []),
    ExpectedRelation("grind_attempts_id_seq", Sequence, []),
    ExpectedRelation("grind_unique_submissions", Table, []),
    ExpectedRelation("grind_unique_submissions_pkey", Index, []),
  ]
}
