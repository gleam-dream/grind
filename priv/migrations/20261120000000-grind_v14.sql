--- migration:up

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) l;

ALTER TABLE grind_jobs ADD COLUMN settle_attempt_id bigint;

ALTER TABLE grind_jobs ADD COLUMN settle_at timestamptz;

ALTER TABLE grind_jobs ADD COLUMN settle_tries bigint NOT NULL DEFAULT 0;

ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_settlement_mark_check CHECK ((settle_at IS NULL OR settle_attempt_id IS NOT NULL) AND settle_tries >= 0);

UPDATE grind_jobs AS job SET settle_attempt_id = job.attempt_id, settle_at = clock_timestamp() WHERE job.state = 'uncertain' AND job.attempt_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM grind_job_acknowledgements AS receipt WHERE receipt.job_id = job.id AND receipt.attempt_id = job.attempt_id AND receipt.attempt_epoch = job.attempt_epoch);

CREATE INDEX grind_jobs_settlement_idx ON grind_jobs (queue, settle_at, id) WHERE state = 'uncertain' AND settle_at IS NOT NULL AND settle_attempt_id = attempt_id;

INSERT INTO grind_schema_migrations (version) VALUES (14);

--- migration:down

DELETE FROM grind_schema_migrations WHERE version = 14;

DROP INDEX grind_jobs_settlement_idx;

ALTER TABLE grind_jobs DROP CONSTRAINT grind_jobs_settlement_mark_check;

ALTER TABLE grind_jobs DROP COLUMN settle_tries;

ALTER TABLE grind_jobs DROP COLUMN settle_at;

ALTER TABLE grind_jobs DROP COLUMN settle_attempt_id;

--- migration:end
