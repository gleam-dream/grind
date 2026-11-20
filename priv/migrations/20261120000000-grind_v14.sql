--- migration:up

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) l;

ALTER TABLE grind_jobs ADD COLUMN settle_attempt_id bigint;

ALTER TABLE grind_jobs ADD COLUMN settle_at timestamptz;

ALTER TABLE grind_jobs ADD COLUMN settle_tries bigint NOT NULL DEFAULT 0;

ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_settlement_mark_check CHECK ((settle_at IS NULL OR settle_attempt_id IS NOT NULL) AND settle_tries >= 0) NOT VALID;


INSERT INTO grind_schema_migrations (version) VALUES (14);

--- migration:down

DELETE FROM grind_schema_migrations WHERE version = 14;

ALTER TABLE grind_jobs DROP CONSTRAINT grind_jobs_settlement_mark_check;

ALTER TABLE grind_jobs DROP COLUMN settle_tries;

ALTER TABLE grind_jobs DROP COLUMN settle_at;

ALTER TABLE grind_jobs DROP COLUMN settle_attempt_id;

--- migration:end
