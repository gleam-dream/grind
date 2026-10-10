-- One queue, one MVCC snapshot and one database clock sample.
WITH sample AS MATERIALIZED (
  SELECT clock_timestamp() AS at
), counts AS MATERIALIZED (
  SELECT
    state,
    count(*)::bigint AS count,
    max(greatest(0, floor(extract(epoch FROM (at - inserted_at)) * 1000)))::bigint AS oldest_job_age_ms,
    count(*) FILTER (
      WHERE state IN ('queued', 'scheduled', 'retryable') AND available_at <= at
    )::bigint AS due_count,
    max(greatest(0, floor(extract(epoch FROM (at - available_at)) * 1000))) FILTER (
      WHERE state IN ('queued', 'scheduled', 'retryable') AND available_at <= at
    )::bigint AS oldest_due_age_ms
  FROM grind_jobs CROSS JOIN sample
  WHERE queue = $1
  GROUP BY state
), state_names AS (
  SELECT state FROM counts
  UNION
  SELECT unnest(ARRAY[
    'queued', 'scheduled', 'retryable', 'executing', 'succeeded',
    'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain',
    'discarded', 'cancelled'
  ]::text[])
)
SELECT
  (SELECT floor(extract(epoch FROM at) * 1000)::bigint FROM sample) AS sampled_at_ms,
  state_names.state,
  coalesce(counts.count, 0)::bigint AS count,
  coalesce(counts.oldest_job_age_ms, 0)::bigint AS oldest_job_age_ms,
  coalesce(counts.due_count, 0)::bigint AS due_count,
  coalesce(counts.oldest_due_age_ms, 0)::bigint AS oldest_due_age_ms
FROM state_names LEFT JOIN counts USING (state)
ORDER BY state_names.state;
