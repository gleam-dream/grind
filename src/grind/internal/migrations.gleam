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
//// `grind_migrations_conformance_test`
//// (test/grind/migrations/conformance_test.gleam) proves this
//// with cigogne's own public parser, with no database required.
////
//// See AGENTS.md, "Adding a migration", for the steps to add a new version
//// here.

import grind/internal/terminal

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
/// `grind_schema_migrations` marker `INSERT`), its cumulative expected
/// relation shape, and its cumulative expected foreign-key constraint set
/// (by name — `pg_constraint`, not `pg_class`, so these never appear in
/// `shape` itself), all checked by `postgres.read_schema_generation` before
/// trusting a marker claiming this version is genuinely installed. A
/// database missing one of `foreign_keys` (an `ON DELETE CASCADE` dropped
/// by hand, say) fails this check exactly like a missing relation or a
/// missing `key_columns` entry does — closed, not silently tolerated.
pub type Migration {
  Migration(
    version: Int,
    statements: List(String),
    shape: List(ExpectedRelation),
    foreign_keys: List(String),
    /// `#(table_name, column_name)` pairs that must NOT exist once this
    /// version is genuinely installed — the inverse of `shape`'s
    /// `key_columns` ("must exist"). Empty for every version before a
    /// column was ever dropped from an already-released shape. Catches a
    /// stale database whose `grind_schema_migrations` marker claims this
    /// version but whose physical shape predates an in-place edit made to
    /// this version *before* it was ever released — see `v12_statements`'s
    /// own doc comment ("Dropping `storage_owner`") for the one case this
    /// exists for today, and AGENTS.md, "Adding a migration".
    forbidden_columns: List(#(String, String)),
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
  [
    Migration(11, v11_statements(), v11_shape(), [], []),
    Migration(
      12,
      v12_statements(),
      v12_shape(),
      v12_foreign_keys(),
      v12_forbidden_columns(),
    ),
  ]
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

/// `grind_jobs_quarantine_idx` deliberately omits `queue` and
/// `lease_expires_at` from its columns, even though both the per-queue scan
/// (`grind/internal/lease.quarantine_expired_in_queue`) and the public
/// cross-queue sweep (`postgres.quarantine_expired`) filter on them: neither
/// query's own `ORDER BY id` needs `lease_expires_at` in the index at all
/// (it is only ever a range filter, applied against however many rows the
/// leading columns already narrowed down to), and `queue` narrows the wrong
/// direction for the cross-queue sweep, which does not filter by it — `(id)
/// WHERE state = 'executing'` measured 2.6ms for the cross-queue sweep and
/// 1.3ms per-queue against a 2M-row `grind_jobs` (no sort either way, since
/// `id` is already the index order), against 384ms for `(queue,
/// lease_expires_at, id)` — that shape forces the cross-queue sweep (no
/// `queue` predicate to seek on) to walk the primary key across every row
/// instead. (These three indexes were originally measured, and shipped, with
/// a leading `storage_owner` column, matching every other index on this
/// table at the time — see "Dropping `storage_owner`" below for why that
/// column is gone from all of them now; a single schema's own
/// `grind_jobs` never held more than one distinct `storage_owner` value to
/// begin with; per-relation cost was otherwise unaffected by its removal.)
///
/// Each receipt table's own `DELETE ... WHERE NOT EXISTS (SELECT 1 FROM
/// grind_jobs ...)`, immediately before that table's own `ADD CONSTRAINT
/// ... FOREIGN KEY`, removes any already-orphaned receipt row (one whose
/// `job_id` no longer exists in `grind_jobs`, however that happened) before
/// the constraint is added — an existing orphan would otherwise make the
/// `ADD CONSTRAINT` itself fail with `23503 foreign_key_violation` on a
/// database that has been running a while, since `ALTER TABLE ... ADD
/// CONSTRAINT` validates every existing row by default. See
/// `docs/RECOVERY-EVIDENCE.md`, Increment 25, for the red-first proof (a
/// seeded orphan in the frozen v11 upgrade fixture makes this migration
/// fail with `23503` without these three `DELETE`s, and succeed with the
/// orphan gone once they run).
///
/// **Dropping `storage_owner`.** Isolation between logically distinct Grind
/// installations is now the PostgreSQL schema (`search_path`) alone, not a
/// `storage_owner` value scoping rows within one shared schema — see
/// `docs/UNIQUENESS-CONTRACT.md` and `README.md`, "Isolation". `v11`
/// (frozen, already released — its own statements cannot change) created
/// `storage_owner` on `grind_jobs` and every receipt/resolution table, plus
/// every composite key or index that included it; `v12`, still unreleased
/// and edited in place for this change, both adds `finished_at` (above) and
/// removes `storage_owner` everywhere, since both belong to the same
/// not-yet-shipped version. Every PK/unique constraint/index that included
/// `storage_owner` is dropped and rebuilt without it:
/// `grind_jobs_unique_candidate_idx` (a non-unique index — no collision risk
/// from dropping its leading column), `grind_job_acknowledgements_pkey`
/// (`(storage_owner, command_id)` -> `(command_id)`) and
/// `_attempt_key` (`(storage_owner, job_id, attempt_id, attempt_epoch)` ->
/// `(job_id, attempt_id, attempt_epoch)` — both provably collision-free,
/// since `command_id` and `job_id` are themselves derived from
/// `grind_jobs.id` (a single `bigserial` sequence, already unique across
/// every owner in one physical table) and `grind_attempts_id_seq`
/// (likewise one shared sequence), never from anything a caller could
/// independently reuse across owners), `grind_job_resolutions_pkey`
/// (`(storage_owner, resolution_id)` -> `(resolution_id)`), and
/// `grind_unique_submissions_pkey` (`(storage_owner, submission_id)` ->
/// `(submission_id)`). `resolution_id` and `submission_id`, unlike
/// `command_id`, are caller-chosen strings (an operator's own audit label; a
/// caller's own idempotency key) with no such structural uniqueness
/// guarantee: two distinct `storage_owner` values that shared one physical
/// schema on `v11` specifically to stay mutually isolated (the deliberate,
/// documented use of the now-removed `postgres.with_storage_owner`
/// override) could have coincidentally reused the identical string for
/// unrelated purposes. Dropping `storage_owner` from either table would then
/// silently merge two different rows onto one now-shared key — so each
/// table's own `DO` block, immediately before that table's `DROP COLUMN`,
/// checks for exactly that collision first and fails the whole step closed
/// with a clear `RAISE EXCEPTION` (never silently merging) before any data
/// is touched; the ordinary case (no collision — including every fresh v11
/// install, which only ever had one implicit `storage_owner` value per
/// schema to begin with under the derived, non-override resolution) passes
/// through with no observable effect beyond the schema change itself. See
/// `docs/RECOVERY-EVIDENCE.md` for the frozen-fixture proof of both the
/// ordinary (no-collision) upgrade and the collision-fails-closed case.
///
/// **The down direction is lossy.** `priv/migrations/*.sql`'s own `v12`
/// `down` section re-adds `storage_owner` as `NOT NULL DEFAULT ''` on every
/// table (never the original per-row value, which this `up` direction
/// already discarded) — a v11-era caller reading rows through that
/// down-migrated schema would see every row as if it belonged to one
/// `storage_owner: ""` installation, indistinguishable from every other row,
/// not restored to whatever distinct owners (if any) existed before this
/// migration first ran.
fn v12_statements() -> List(String) {
  [
    advisory_lock_statement(),
    "ALTER TABLE grind_jobs ADD COLUMN finished_at timestamptz DEFAULT now()",
    "ALTER TABLE grind_jobs ALTER COLUMN finished_at DROP DEFAULT",
    "UPDATE grind_jobs SET finished_at = NULL WHERE state NOT IN ("
      <> terminal.states_sql()
      <> ")",
    "ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_finished_at_check CHECK ((state IN ("
      <> terminal.states_sql()
      <> ")) = (finished_at IS NOT NULL))",
    "CREATE INDEX grind_jobs_finished_idx ON grind_jobs (finished_at, id) WHERE finished_at IS NOT NULL",
    "CREATE INDEX grind_jobs_claim_idx ON grind_jobs (queue, available_at, id) WHERE state IN ('queued', 'scheduled', 'retryable')",
    "CREATE INDEX grind_jobs_quarantine_idx ON grind_jobs (id) WHERE state = 'executing'",
    "CREATE INDEX grind_job_acknowledgements_job_idx ON grind_job_acknowledgements (job_id)",
    "CREATE INDEX grind_unique_submissions_job_idx ON grind_unique_submissions (job_id)",
    "CREATE INDEX grind_job_resolutions_job_idx ON grind_job_resolutions (job_id)",
    "DELETE FROM grind_job_acknowledgements r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id)",
    "DELETE FROM grind_unique_submissions r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id)",
    "DELETE FROM grind_job_resolutions r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id)",
    "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE",
    "ALTER TABLE grind_unique_submissions ADD CONSTRAINT grind_unique_submissions_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE",
    "ALTER TABLE grind_job_resolutions ADD CONSTRAINT grind_job_resolutions_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE",
    "DROP INDEX grind_jobs_unique_candidate_idx",
    "CREATE INDEX grind_jobs_unique_candidate_idx ON grind_jobs (worker_id, worker_version, unique_key_contract, unique_key_sha256) WHERE unique_key_sha256 IS NOT NULL",
    "ALTER TABLE grind_jobs DROP COLUMN storage_owner",
    "DO $$ BEGIN IF EXISTS (SELECT 1 FROM grind_job_resolutions GROUP BY resolution_id HAVING count(DISTINCT storage_owner) > 1) THEN RAISE EXCEPTION 'grind_v12: two distinct storage owners share a resolution_id; dropping storage_owner would silently merge their grind_job_resolutions rows. Resolve this collision manually (rename or remove one side) before migrating.'; END IF; END $$",
    "ALTER TABLE grind_job_resolutions DROP CONSTRAINT grind_job_resolutions_pkey",
    "ALTER TABLE grind_job_resolutions DROP COLUMN storage_owner",
    "ALTER TABLE grind_job_resolutions ADD CONSTRAINT grind_job_resolutions_pkey PRIMARY KEY (resolution_id)",
    "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_pkey",
    "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_attempt_key",
    "ALTER TABLE grind_job_acknowledgements DROP COLUMN storage_owner",
    "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_pkey PRIMARY KEY (command_id)",
    "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_attempt_key UNIQUE (job_id, attempt_id, attempt_epoch)",
    "DO $$ BEGIN IF EXISTS (SELECT 1 FROM grind_unique_submissions GROUP BY submission_id HAVING count(DISTINCT storage_owner) > 1) THEN RAISE EXCEPTION 'grind_v12: two distinct storage owners share a submission_id; dropping storage_owner would silently merge their grind_unique_submissions rows. Resolve this collision manually (rename or remove one side) before migrating.'; END IF; END $$",
    "ALTER TABLE grind_unique_submissions DROP CONSTRAINT grind_unique_submissions_pkey",
    "ALTER TABLE grind_unique_submissions DROP COLUMN storage_owner",
    "ALTER TABLE grind_unique_submissions ADD CONSTRAINT grind_unique_submissions_pkey PRIMARY KEY (submission_id)",
    "INSERT INTO grind_schema_migrations (version) VALUES (12)",
  ]
}

/// `grind_v12`'s own cumulative shape: every v11 relation, `grind_jobs`'s
/// `key_columns` extended with the new `finished_at` column, plus the claim
/// and quarantine hot-path partial indexes
/// (`grind_jobs_claim_idx`/`grind_jobs_quarantine_idx`) and one bare
/// `(job_id)` index per receipt table (`grind_job_acknowledgements_job_idx`,
/// `grind_unique_submissions_job_idx`, `grind_job_resolutions_job_idx`) —
/// deliberately *not* `(storage_owner, job_id)`: the only query that ever
/// seeks these tables by `job_id` alone is the `ON DELETE CASCADE` from
/// `grind_jobs(id)` each receipt table's own foreign key declares, which
/// never has a `storage_owner` to filter by, so `storage_owner` leading
/// would only get in the way. `postgres.prune_finished` deletes only from
/// `grind_jobs`; every receipt row cascades, by the database itself, rather
/// than by a second explicit `DELETE` this module used to also generate —
/// see `docs/RECOVERY-EVIDENCE.md`, Increment 24, for why an explicit,
/// same-statement receipt `DELETE` was not enough on its own. Confirmed
/// empirically the same way `v11_shape`'s own doc comment describes, against
/// a real freshly `migrate`d v12 schema.
fn v12_shape() -> List(ExpectedRelation) {
  [
    ExpectedRelation("grind_schema_migrations", Table, []),
    ExpectedRelation("grind_schema_migrations_pkey", Index, []),
    ExpectedRelation("grind_jobs", Table, [
      "unique_key_contract", "unique_key_sha256", "finished_at",
    ]),
    ExpectedRelation("grind_jobs_id_seq", Sequence, []),
    ExpectedRelation("grind_jobs_pkey", Index, []),
    ExpectedRelation("grind_jobs_unique_candidate_idx", Index, []),
    ExpectedRelation("grind_jobs_finished_idx", Index, []),
    ExpectedRelation("grind_jobs_claim_idx", Index, []),
    ExpectedRelation("grind_jobs_quarantine_idx", Index, []),
    ExpectedRelation("grind_job_resolutions", Table, []),
    ExpectedRelation("grind_job_resolutions_pkey", Index, []),
    ExpectedRelation("grind_job_resolutions_job_idx", Index, []),
    ExpectedRelation("grind_job_acknowledgements", Table, []),
    ExpectedRelation("grind_job_acknowledgements_pkey", Index, []),
    ExpectedRelation("grind_job_acknowledgements_attempt_key", Index, []),
    ExpectedRelation("grind_job_acknowledgements_job_idx", Index, []),
    ExpectedRelation("grind_attempts_id_seq", Sequence, []),
    ExpectedRelation("grind_unique_submissions", Table, []),
    ExpectedRelation("grind_unique_submissions_pkey", Index, []),
    ExpectedRelation("grind_unique_submissions_job_idx", Index, []),
  ]
}

/// `grind_v12`'s own cumulative foreign-key set: the three `ON DELETE
/// CASCADE` constraints backstopping `postgres.prune_finished` against the
/// snapshot-timing race `docs/RECOVERY-EVIDENCE.md` Increment 24 describes.
/// Checked by name against `pg_constraint`, independently of `v12_shape`'s
/// own `pg_class` relation check, since a plain foreign key (no backing
/// index of its own beyond whatever `v12_shape` already lists) never
/// appears in `pg_class` at all.
fn v12_foreign_keys() -> List(String) {
  [
    "grind_job_acknowledgements_job_id_fkey",
    "grind_unique_submissions_job_id_fkey",
    "grind_job_resolutions_job_id_fkey",
  ]
}

/// `grind_v12`'s own forbidden-column set: `storage_owner`, on every table
/// it was ever dropped from by this same, not-yet-released version (see
/// `v12_statements`'s own doc comment, "Dropping `storage_owner`"). Physical
/// presence here despite a `grind_schema_migrations` marker already claiming
/// version 12 means this database ran an *older* copy of `v12_statements`
/// (from before that in-place edit) — a dev database created while this
/// unreleased version was still being developed, never a genuinely
/// conforming v12 install. `postgres.read_schema_generation` fails this
/// closed (`IncompatibleSchema`) rather than trusting the marker alone,
/// exactly like a missing `shape` relation or `foreign_keys` entry already
/// does for the opposite case (something the marker claims but the physical
/// schema lacks). See AGENTS.md, "Adding a migration", for what to do if
/// this is ever hit: recreate the database and let `postgres.migrate` apply
/// the current `v12_statements` from scratch — there is no in-place repair
/// path for an unreleased version edited after being run.
fn v12_forbidden_columns() -> List(#(String, String)) {
  [
    #("grind_jobs", "storage_owner"),
    #("grind_job_resolutions", "storage_owner"),
    #("grind_job_acknowledgements", "storage_owner"),
    #("grind_unique_submissions", "storage_owner"),
  ]
}
