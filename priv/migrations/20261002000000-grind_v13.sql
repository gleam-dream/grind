--- migration:up

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) l;

ALTER TABLE grind_jobs ADD COLUMN correlation text;

ALTER TABLE grind_jobs ADD COLUMN max_replays bigint;

ALTER TABLE grind_jobs ADD COLUMN replay_count bigint NOT NULL DEFAULT 0;

ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_correlation_check CHECK (correlation IS NULL OR octet_length(correlation) BETWEEN 1 AND 128);

ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_max_replays_check CHECK (max_replays IS NULL OR max_replays > 0);

ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_failure_cause_check;

ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_failure_cause_check CHECK (failure_cause IS NULL OR failure_cause IN ('budget_exhausted', 'retry_declined', 'snooze_limit_reached'));

CREATE INDEX grind_jobs_uncertain_idx ON grind_jobs (id) WHERE state = 'uncertain';

INSERT INTO grind_schema_migrations (version) VALUES (13);

--- migration:down

DELETE FROM grind_schema_migrations WHERE version = 13;

DROP INDEX grind_jobs_uncertain_idx;

ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_failure_cause_check;

ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_failure_cause_check CHECK (failure_cause IS NULL OR failure_cause IN ('budget_exhausted', 'retry_declined'));

ALTER TABLE grind_jobs DROP CONSTRAINT grind_jobs_max_replays_check;

ALTER TABLE grind_jobs DROP CONSTRAINT grind_jobs_correlation_check;

ALTER TABLE grind_jobs DROP COLUMN replay_count;

ALTER TABLE grind_jobs DROP COLUMN max_replays;

ALTER TABLE grind_jobs DROP COLUMN correlation;

--- migration:end
