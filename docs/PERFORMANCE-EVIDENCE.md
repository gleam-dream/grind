# Performance evidence

Load, coordinator-bottleneck, and (later) multi-node/soak evidence for
Grind, produced by `bench/` (see `bench/priv/bench.sql`,
`bench/src/grind_bench/audit.gleam`, and `bench/src/grind_bench/load.gleam`)
against a disposable cluster started by `scripts/bench-postgres.sh`.

Every number in this document is **laptop, indicative** unless a row's own
environment header says otherwise (see `docs/RISKS.md` #17 and the bench
planning notes, "User decisions", item 4: laptop runs are the release
evidence, labeled clearly as such — this is not a dedicated benchmarking
server, and the bench harness shares CPU with the PostgreSQL server under
test on the same machine). Raw per-tick JSONL evidence
(`bench/results/<date>-<commit>/raw/*.jsonl`) is never committed; only the
percentile/CSV rollups in `bench/results/<date>-<commit>/*.csv` are.

## Environment template

Fill in one block per run (or per matrix) before recording its numbers:

```
Commit:            <git rev-parse --short HEAD>
Date:              <YYYY-MM-DD>
OS:                <e.g. macOS 15.x / Darwin 24.x, or Linux distro+kernel>
CPU:               <model, core/thread count>
RAM:               <total>
PostgreSQL:        <postgres --version> + non-default GUCs (see
                   scripts/bench-postgres.sh: shared_preload_libraries=
                   pg_stat_statements, log_lock_waits=on,
                   deadlock_timeout=100ms, track_io_timing=on)
OTP / Gleam:       <erl -version> / <gleam --version>
pool_size:         <postgres.Settings.pool_size used for this run>
D (lease/deadline): statement_deadline_ms used
L (lease duration): queue.QueuePolicy.lease_duration_ms used
J (max batch):     queue.QueuePolicy.maximum_batch_jobs (manual-mode only;
                   has no effect on automatic polling — see docs/RISKS.md #6)
I (poll interval): queue.QueuePolicy.polling's PollEvery interval_ms
delay/cost_ms:     per-job artificial handler sleep used (see L1/L7 sections
                   below for why this harness collapses the plan's separate
                   "job cost" and "delay" axes into one lever)
```

## Run: 2026-09-26, af0de18

```
Commit:            af0de18
Date:              2026-09-26
OS:                macOS 26.5.2 (Darwin 25.5.0 arm64)
CPU:               Apple M2 Max, 12 logical cores
RAM:               32 GB
PostgreSQL:        16.15 + scripts/bench-postgres.sh's GUCs
                   (shared_preload_libraries=pg_stat_statements,
                   log_lock_waits=on, deadlock_timeout=100ms,
                   track_io_timing=on)
OTP / Gleam:       28 / 1.18.1
pool_size:         max(consumers * concurrency, 10) (grind_bench.setup)
D (lease/deadline): 4000ms (postgres.settings default, unchanged)
L (lease duration): 30000ms (queue.default_policy default, unchanged)
J (max batch):     1 (default; automatic polling ignores it -- see
                   docs/RISKS.md #6)
I (poll interval): 20ms (L1) / 10ms (L1 multi-consumer) / 5ms (L7)
delay/cost_ms:     0 or 10 (L1, per row); fixed at 1 (L7)
```

This is a laptop, not a dedicated benchmarking host: the bench harness, the
Gleam/Erlang runtime under test, and the PostgreSQL server all share the
same 12 logical cores. Treat every number below as **laptop, indicative**,
not a production ceiling.

## T1-T3 verdict

Per-attempt-storage decision thresholds from the bench planning notes
("Load" section, "Per-attempt storage decision thresholds"). Not yet
evaluated — L6 (renewal starvation under slow acknowledgements) is
implementation-order step 6, out of scope for the increment that produced
this document's first rows (steps 1-3: bench skeleton, samplers, preload +
L1/L7). This section stays empty until L6 exists.

- **T1** (healthy p99 renewal lag bound): not evaluated.
- **T2** (sibling starvation under `K=2` slow acknowledgements): not
  evaluated.
- **T3** (`1×C50` vs `5×C10` coordinator-bottleneck ratio): **informative
  breach, not mechanically gated.** `1×50` reached 744 jobs/s against
  `10×5`'s 3883 jobs/s in the run below — about 19%, well under T3's 70%
  threshold — and the single coordinator's own mailbox sat at p50=45/p99=50
  while both multi-coordinator shapes stayed under 10, consistent with one
  coordinator process serializing every claim being the actual bottleneck at
  this concurrency, not database capacity. `bench/results/2026-09-26-af0de18/samplers.csv`
  has a first, thin DB-side signal for the L1 matrix (`waiting_locks` is 0
  throughout, i.e. no lock contention at all at this scale) but nothing yet
  for L7's own shapes, and no CPU-percent measurement exists at all — T3's
  own "at <50% DB CPU" half is still unconfirmed. This is one laptop run,
  not the load-test evidence campaign T3 was written to be decided from;
  record it as a strong first signal in the same direction as risk 4/5 in
  `docs/RISKS.md`, not as a release decision by itself.

## L1: drain throughput matrix

Scenario: `gleam run -m grind_bench/load -- l1 <job_count> <consumers>
<concurrency> <queues> <cost_ms>` (see `bench/src/grind_bench/load.gleam`'s
own module doc comment). `cost_ms` stands in for both the plan's "job cost"
and "delay" axes — this harness has one lever (an artificial
`process.sleep` inside the bench worker's handler) for simulated per-job
work, not two independent ones; a later increment (proxy-based delay
injection, plan implementation-order step 7) can separate them.

"DB CPU s/1k jobs" and "statements/job" from the plan's own L1 description
are not computed as a single derived number yet; the underlying per-tick
`pg_stat_statements` deltas are captured (see
`bench/results/2026-09-26-af0de18/samplers.csv`'s `*-db,waiting_locks` rows
for the one field this increment rolls up into a percentile — `calls_delta`/
`total_time_delta_ms` per statement are in each run's own gitignored raw
JSONL, not yet summarized). "connections" here is the _configured_ pool
size, not a live measurement.

| consumers | concurrency | queues | cost_ms | job_count | elapsed_ms | jobs/s | pool_size |
| --------- | ----------- | ------ | ------- | --------- | ---------- | ------ | --------- |
| 1         | 10          | 1      | 0       | 3000      | 2980       | 1007   | 10        |
| 2         | 10          | 1      | 0       | 3000      | 1732       | 1732   | 20        |
| 1         | 50          | 1      | 0       | 3000      | 3480       | 862    | 50        |
| 4         | 10          | 2      | 0       | 3000      | 1142       | 2627   | 40        |
| 4         | 10          | 1      | 10      | 1000      | 416        | 2404   | 40        |

Raw values: `bench/results/2026-09-26-af0de18/l1.csv` (per-scenario BEAM/DB
sampler percentiles: `samplers.csv` in the same directory). Two things stand
out, both consistent with `docs/RISKS.md` #4/#6: (1) raising one consumer's
own concurrency from 10 to 50 with zero-cost jobs (`1×10` vs `1×50`) made
throughput _worse_, not better — a single coordinator's own claim round
trip, not free execution slots, is the ceiling here; (2) adding independent
coordinators (`4×10` vs `1×10`) scales roughly linearly (about 2.6x for 4x
the consumers), matching the "one coordinator, one bottleneck" story L7
below measures directly. `waiting_locks` stayed at 0 throughout every L1
row — no lock contention at this scale.

## L7: coordinator bottleneck

Scenario: `gleam run -m grind_bench/load -- l7 <job_count> <consumers>
<concurrency>` — always one queue, `cost_ms` fixed at 1 (0 ms job body +
the plan's "delay 1 ms", collapsed onto the same lever as L1 — see above).
`coordinator_mqlen_p50`/`p99` are percentiles over every consumer's own
coordinator `Pid` `message_queue_len`, sampled every 20ms and pooled across
every coordinator in the shape (not per-coordinator) — see
`grind_bench/load.run_l7`'s own doc comment.

| shape (consumers×concurrency) | job_count | elapsed_ms | jobs/s | coordinator_mqlen_p50 | coordinator_mqlen_p99 |
| ----------------------------- | --------- | ---------- | ------ | --------------------- | --------------------- |
| 1×50                          | 2000      | 2688       | 744    | 45                    | 50                    |
| 5×10                          | 2000      | 728        | 2747   | 9                     | 10                    |
| 10×5                          | 2000      | 515        | 3883   | 5                     | 6                     |

Raw values: `bench/results/2026-09-26-af0de18/l7.csv` (raw per-tick JSONL
under that same directory's `raw/`, gitignored -- L7's own coordinator
mailbox samples are not yet folded through `grind_bench/summarize` into
`samplers.csv`, only L1's BEAM/DB samples are). See the T3 verdict above
for the reading of this table.

## L2-L6, M1-M7, soak

Not yet implemented (implementation-order steps 4-9 in the bench planning
notes). This document's L2-L6/M/soak sections will be filled in as those
scenarios land.
