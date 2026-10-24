--- migration:up

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) l;

ALTER TABLE grind_jobs ADD COLUMN finished_at timestamptz DEFAULT now();

ALTER TABLE grind_jobs ALTER COLUMN finished_at DROP DEFAULT;

UPDATE grind_jobs SET finished_at = NULL WHERE state NOT IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled');

ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_finished_at_check CHECK ((state IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled')) = (finished_at IS NOT NULL));

CREATE INDEX grind_jobs_finished_idx ON grind_jobs (finished_at, id) WHERE finished_at IS NOT NULL;

CREATE INDEX grind_jobs_claim_idx ON grind_jobs (queue, available_at, id) WHERE state IN ('queued', 'scheduled', 'retryable');

CREATE INDEX grind_jobs_quarantine_idx ON grind_jobs (id) WHERE state = 'executing';

CREATE INDEX grind_job_acknowledgements_job_idx ON grind_job_acknowledgements (job_id);

CREATE INDEX grind_unique_submissions_job_idx ON grind_unique_submissions (job_id);

CREATE INDEX grind_job_resolutions_job_idx ON grind_job_resolutions (job_id);

DELETE FROM grind_job_acknowledgements r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id);

DELETE FROM grind_unique_submissions r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id);

DELETE FROM grind_job_resolutions r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id);

ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE;

ALTER TABLE grind_unique_submissions ADD CONSTRAINT grind_unique_submissions_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE;

ALTER TABLE grind_job_resolutions ADD CONSTRAINT grind_job_resolutions_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE;

DROP INDEX grind_jobs_unique_candidate_idx;

CREATE INDEX grind_jobs_unique_candidate_idx ON grind_jobs (worker_id, worker_version, unique_key_contract, unique_key_sha256) WHERE unique_key_sha256 IS NOT NULL;

ALTER TABLE grind_jobs DROP COLUMN storage_owner;

DO $$ BEGIN IF EXISTS (SELECT 1 FROM grind_job_resolutions GROUP BY resolution_id HAVING count(DISTINCT storage_owner) > 1) THEN RAISE EXCEPTION 'grind_v12: two distinct storage owners share a resolution_id; dropping storage_owner would silently merge their grind_job_resolutions rows. Resolve this collision manually (rename or remove one side) before migrating.'; END IF; END $$;

ALTER TABLE grind_job_resolutions DROP CONSTRAINT grind_job_resolutions_pkey;

ALTER TABLE grind_job_resolutions DROP COLUMN storage_owner;

ALTER TABLE grind_job_resolutions ADD CONSTRAINT grind_job_resolutions_pkey PRIMARY KEY (resolution_id);

ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_pkey;

ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_attempt_key;

ALTER TABLE grind_job_acknowledgements DROP COLUMN storage_owner;

ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_pkey PRIMARY KEY (command_id);

ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_attempt_key UNIQUE (job_id, attempt_id, attempt_epoch);

DO $$ BEGIN IF EXISTS (SELECT 1 FROM grind_unique_submissions GROUP BY submission_id HAVING count(DISTINCT storage_owner) > 1) THEN RAISE EXCEPTION 'grind_v12: two distinct storage owners share a submission_id; dropping storage_owner would silently merge their grind_unique_submissions rows. Resolve this collision manually (rename or remove one side) before migrating.'; END IF; END $$;

ALTER TABLE grind_unique_submissions DROP CONSTRAINT grind_unique_submissions_pkey;

ALTER TABLE grind_unique_submissions DROP COLUMN storage_owner;

ALTER TABLE grind_unique_submissions ADD CONSTRAINT grind_unique_submissions_pkey PRIMARY KEY (submission_id);

INSERT INTO grind_schema_migrations (version) VALUES (12);

--- migration:down

DELETE FROM grind_schema_migrations WHERE version = 12;

ALTER TABLE grind_unique_submissions DROP CONSTRAINT grind_unique_submissions_pkey;

ALTER TABLE grind_unique_submissions ADD COLUMN storage_owner text NOT NULL DEFAULT '';

ALTER TABLE grind_unique_submissions ALTER COLUMN storage_owner DROP DEFAULT;

ALTER TABLE grind_unique_submissions ADD CONSTRAINT grind_unique_submissions_pkey PRIMARY KEY (storage_owner, submission_id);

ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_attempt_key;

ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_pkey;

ALTER TABLE grind_job_acknowledgements ADD COLUMN storage_owner text NOT NULL DEFAULT '';

ALTER TABLE grind_job_acknowledgements ALTER COLUMN storage_owner DROP DEFAULT;

ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_pkey PRIMARY KEY (storage_owner, command_id);

ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_attempt_key UNIQUE (storage_owner, job_id, attempt_id, attempt_epoch);

ALTER TABLE grind_job_resolutions DROP CONSTRAINT grind_job_resolutions_pkey;

ALTER TABLE grind_job_resolutions ADD COLUMN storage_owner text NOT NULL DEFAULT '';

ALTER TABLE grind_job_resolutions ALTER COLUMN storage_owner DROP DEFAULT;

ALTER TABLE grind_job_resolutions ADD CONSTRAINT grind_job_resolutions_pkey PRIMARY KEY (storage_owner, resolution_id);

ALTER TABLE grind_jobs ADD COLUMN storage_owner text NOT NULL DEFAULT '';

ALTER TABLE grind_jobs ALTER COLUMN storage_owner DROP DEFAULT;

DROP INDEX grind_jobs_unique_candidate_idx;

CREATE INDEX grind_jobs_unique_candidate_idx ON grind_jobs (storage_owner, worker_id, worker_version, unique_key_contract, unique_key_sha256) WHERE unique_key_sha256 IS NOT NULL;

ALTER TABLE grind_job_resolutions DROP CONSTRAINT grind_job_resolutions_job_id_fkey;

ALTER TABLE grind_unique_submissions DROP CONSTRAINT grind_unique_submissions_job_id_fkey;

ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_job_id_fkey;

DROP INDEX grind_job_resolutions_job_idx;

DROP INDEX grind_unique_submissions_job_idx;

DROP INDEX grind_job_acknowledgements_job_idx;

DROP INDEX grind_jobs_quarantine_idx;

DROP INDEX grind_jobs_claim_idx;

DROP INDEX grind_jobs_finished_idx;

ALTER TABLE grind_jobs DROP CONSTRAINT grind_jobs_finished_at_check;

ALTER TABLE grind_jobs DROP COLUMN finished_at;

--- migration:end
