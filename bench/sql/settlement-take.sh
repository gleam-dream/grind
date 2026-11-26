#!/usr/bin/env bash
# Times one settler poll (the settlement take statement) against a queue that
# holds many uncertain jobs, none of them due: the steady state after a burst
# of abandonments whose settlement tries are backing off.
#
# usage: bench/sql/settlement-take.sh <database-url> [rows] [runs] [polls]
# The database must hold Grind's schema. The script adds uncertain jobs to
# the queue `bench-settle` until it holds exactly `rows`, refusing a queue
# that already holds more, and moves every one of them an hour into the
# future, so none is due when timing starts. It then runs `runs` pgbench runs
# of `polls` polls each and prints each run's mean latency in milliseconds.
set -euo pipefail
url=$1
rows=${2:-200000}
runs=${3:-5}
polls=${4:-200}
here="$(cd "$(dirname "$0")" && pwd)"

existing=$(psql "$url" -Atc "SELECT count(*) FROM grind_jobs WHERE queue = 'bench-settle'")
if [[ "$existing" -gt "$rows" ]]; then
  echo "queue bench-settle holds $existing jobs, more than the $rows requested; use a fresh database" >&2
  exit 1
fi
if [[ "$existing" -lt "$rows" ]]; then
  psql "$url" -q -v ON_ERROR_STOP=1 <<SQL
INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, state, available_at, attempt_id, attempt_epoch, attempt_owner, attempt_count, uncertain_at, failure_description, settle_attempt_id, settle_at, settle_tries)
SELECT 'bench-settle', 'bench.settle', 'v1', 'v1', '"x"'::jsonb, 'v1', 'uncertain', clock_timestamp(), g, 1, 'bench-node', 1, clock_timestamp(), 'expired attempt requires outcome reconciliation', g, clock_timestamp() + interval '1 hour', 1
FROM generate_series($existing + 1, $rows) AS g;
ANALYZE grind_jobs;
SQL
fi
# A reused fixture's jobs may have become due; the timed poll must find none.
psql "$url" -q -v ON_ERROR_STOP=1 -c "UPDATE grind_jobs SET settle_at = clock_timestamp() + interval '1 hour' WHERE queue = 'bench-settle'" -c "ANALYZE grind_jobs"

for run in $(seq 1 "$runs"); do
  pgbench -n -c 1 -t "$polls" -f "$here/settlement-take.sql" "$url" 2>/dev/null |
    awk -v run="$run" '/latency average/ { print "run " run ": " $4 " ms" }'
done
