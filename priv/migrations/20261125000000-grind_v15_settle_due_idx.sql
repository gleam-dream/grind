--- migration:up:disable_transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS grind_jobs_settle_due_idx ON grind_jobs (queue, settle_at, id) WHERE state = 'uncertain' AND settle_at IS NOT NULL;

--- migration:down

DROP INDEX CONCURRENTLY IF EXISTS grind_jobs_settle_due_idx;

--- migration:end
