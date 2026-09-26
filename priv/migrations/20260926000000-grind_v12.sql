--- migration:up

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) l;

ALTER TABLE grind_jobs ADD COLUMN finished_at timestamptz DEFAULT now();

ALTER TABLE grind_jobs ALTER COLUMN finished_at DROP DEFAULT;

UPDATE grind_jobs SET finished_at = NULL WHERE state NOT IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled');

ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_finished_at_check CHECK ((state IN ('succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled')) = (finished_at IS NOT NULL));

CREATE INDEX grind_jobs_finished_idx ON grind_jobs (storage_owner, finished_at, id) WHERE finished_at IS NOT NULL;

CREATE INDEX grind_jobs_claim_idx ON grind_jobs (storage_owner, queue, available_at, id) WHERE state IN ('queued', 'scheduled', 'retryable');

CREATE INDEX grind_jobs_quarantine_idx ON grind_jobs (storage_owner, id) WHERE state = 'executing';

CREATE INDEX grind_job_acknowledgements_job_idx ON grind_job_acknowledgements (job_id);

CREATE INDEX grind_unique_submissions_job_idx ON grind_unique_submissions (job_id);

CREATE INDEX grind_job_resolutions_job_idx ON grind_job_resolutions (job_id);

DELETE FROM grind_job_acknowledgements r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id);

DELETE FROM grind_unique_submissions r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id);

DELETE FROM grind_job_resolutions r WHERE NOT EXISTS (SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id);

ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE;

ALTER TABLE grind_unique_submissions ADD CONSTRAINT grind_unique_submissions_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE;

ALTER TABLE grind_job_resolutions ADD CONSTRAINT grind_job_resolutions_job_id_fkey FOREIGN KEY (job_id) REFERENCES grind_jobs (id) ON DELETE CASCADE;

INSERT INTO grind_schema_migrations (version) VALUES (12);

--- migration:down

DELETE FROM grind_schema_migrations WHERE version = 12;

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
