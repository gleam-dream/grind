# Benchmark harness

Run the disposable-database harness gate with:

```sh
nix develop --command bash scripts/bench-smoke.sh
```

The gate runs the bench tests, requires every database-test marker, then drains
1,000 jobs and audits effects, rows, receipts and observations. Compilation or
plain `gleam test` without the database variables does not prove the DB cases.

For a matrix, select `l1`, `l2`, `l3`, `l4`, `l5`, `l6`, `l7`, or `all`:

```sh
nix develop --command bash scripts/bench-matrix.sh l3
GRIND_BENCH_NETWORK_DELAY_MS=5 nix develop --command bash scripts/bench-matrix.sh l3
```

Each point discards a warm-up and keeps at least three repeats. L6 uses jobs
lasting multiple leases at the real D=4-second ACK deadline. The 26 T2 profiles
cover L=16/24/30 seconds, 0.8D/1.2D ACK delay, K=0 and K=3..8 at L=16 seconds,
and K=0/5/8 at L=24/30 seconds. These and the healthy profiles take hours to run.
A reduced selection is partial evidence, never a full-matrix verdict.

`GRIND_BENCH_T2_STRESS=1` adds four selected C50 profiles with main pools of 50
and 10. To run an explicit subset, set `GRIND_BENCH_T2_PROFILES` to a text file
with one `K D L ACK_delay concurrency main_pool` row per point (milliseconds;
blank lines and `#` comments allowed). The file is copied into the results.
The direct CLI accepts `l6t2 K D L ACK_delay concurrency main_pool repeat`;
older forms remain supported. Every consumer reserves one additional renewal
connection. CSVs name main, reserved-renewal and total Grind connection counts;
the ledger pool is separately `max(total_concurrency, 4)` (L4 uses 8), and the
observer pool has one connection. Keep these resource budgets equal in paired
comparisons; equal main-pool sizes alone do not imply equal connection budgets.

`GRIND_BENCH_NETWORK_DELAY_MS` delays each TCP chunk in each direction between
Grind and PostgreSQL. The ledger and observers connect directly. This is a real
socket-path delay, separate from handler cost. It does not simulate bandwidth,
packet loss, or an Internet topology. A configured 5 ms is per direction, not
a claim that every SQL call takes exactly 10 ms. Include the delay in every
comparison's configuration.

L7 and diagnostic `profile` runs accept `GRIND_BENCH_DRAIN_TIMEOUT_MS`, a
positive decimal integer defaulting to `60000`. For matched zero-delay and
5-ms-delay L7 comparisons, set the same explicit budget (for example `600000`)
on both arms. This changes only how long those runs may wait for completion;
it does not change counts, job costs, pools, leases, statement deadlines, or the
throughput denominator. The budget is checked between drain polls; an in-flight
query can outlast it, so diagnostics report the actual interval. It does not
alter L3's drain or generator-validity rules.

Each L7/profile raw file has a sibling `.jsonl.drain.json` diagnostic, including
successes. It records the resolved budget, time from consumer-startup entry to
drain entry, the actual drain interval, the outcome, sampler first/last times,
tick count, largest observed gap, and a completion snapshot after the observer
has stopped. The sampler takes its first sample before drain entry and its final
sample after drain exit, then acknowledges stop; it has no fixed tick limit.
Coverage is of the drain interval only, not the earlier consumer-startup period.
Cadence remains nominal, and the largest observed gap must be considered when
interpreting samples. These monotonic times are separate from throughput's
handler-start / durable-observation timestamp denominator.

A timeout or instrumentation failure still fails the run. Diagnostic counts are
observed after the drain decision, before consumer shutdown; they do not trigger
another drain or convert partial work to success. On any matrix failure, the
warm-up artifacts are retained under `warmup-results-on-failure/`. They may
include earlier successful warm-ups; logs and diagnostic outcomes identify the
actual failed phase. Warm-up artifacts are discarded when the matrix succeeds.

## Measurements and validity

| Concern              | Current measurement                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Handler work         | `bench_effects.started_at` is recorded before simulated work; `finished_at` is recorded after it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| Durable completion   | An independent observer records the first time the succeeded receipt is visible. Aggregate throughput ends at this observation. It is an upper bound on commit time, including the 10 ms polling interval, observer query time and contention.                                                                                                                                                                                                                                                                                                                                                              |
| SQL timestamps       | Existing `finished_at`/receipt timestamp latency columns remain available; they are written within the transaction and are not exact commit timestamps.                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Open-loop arrival    | A monotonic-clock scheduler uses absolute arrival slots and at most 256 outstanding admissions by default (`GRIND_BENCH_MAX_INFLIGHT`). Admission latency cannot move later slots. A slot without capacity is counted as capacity-limited, never queued for later dispatch.                                                                                                                                                                                                                                                                                                                                 |
| Generator overload   | `arrivals.csv` preserves scheduled, dispatched, admitted, failed/unrecorded, unfinished and capacity-limited counts, the cap, peak outstanding, dispatch duration and p99/max lag. Any exhausted slot or lag over 2% of the window (20 ms floor) marks the run `generator_limited` and fails after writing the row. Failures remain in the denominator.                                                                                                                                                                                                                                                     |
| Renewal headroom     | A separate pool samples every tracked attempt, including expired executing leases. T2 requires coverage of every non-stalled attempt. Successful renewal timing is a separate metric, not the denominator.                                                                                                                                                                                                                                                                                                                                                                                                  |
| Slow ACK activation  | A PostgreSQL sequence increments inside the actual ACK trigger and survives rollback. T2 requires activation of each target and samples proving a slow ACK overlapped a running sibling handler.                                                                                                                                                                                                                                                                                                                                                                                                            |
| T2 result            | Every admitted job has a final row in `l6_t2_outcomes.csv`. Healthy siblings must succeed with exactly one matching receipt and expected output. Fault targets may succeed under the same rule or be uncertain without an ACK receipt. Missing, queued, executing and inconsistent outcomes fail. Summary CSVs retain classification and uncertainty counts. Default validation also rejects non-stalled headroom below L/10 or quarantine. `GRIND_BENCH_ALLOW_T2_FAILURE=1` relaxes the headroom verdict for a historical baseline; it cannot bypass final classification or justify a release pass.       |
| Pruning              | Both L5 arms start with 10,000 pre-aged terminal rows. A durable-prune telemetry observation must arrive within the fixed traffic interval. A later row-count read is reported separately, so admission drain time cannot expand that interval. Fresh workload rows remain in retention so no fast job disappears from the latency sample. The observed durable-completion and handler-completion counts must equal admitted jobs. L5 reports durable p50/p95/p99 separately from the explicitly labelled SQL timestamp columns. Both arms receive a full workload audit after synthetic filler is removed. |
| Polling cost         | L2 compares 0/100k/1M rows, asserts actual table/retained-row counts, and reports time per call separately from calls per second. After timing, a fresh one-connection pool runs three actual empty-queue polls with session-local `auto_explain` ANALYZE/BUFFERS/TIMING JSON logging. Raw plans for the production claim/quarantine SQL and measured counts are retained under `raw/l2-plans-*.log`; missing plans fail. This requires PostgreSQL's `auto_explain` module and the disposable server log.                                                                                                   |
| Admission contention | L4 holds the Grind pool at 80 and runs a C10 consumer. Its separate observer samples lock waits and requires at least 30 samples. CPU and latency are reported independently; rising latency alone is not evidence of advisory-lock contention.                                                                                                                                                                                                                                                                                                                                                             |

Observer traffic is part of harness overhead, and must be held equal between
implementations and measured in overhead controls before publishing comparative
numbers. The idle L2 and admission-only L4 contexts disable the completion
observer. DB samplers never borrow Grind's processing pool. The run owns the completion observer and waits for its stop acknowledgement
before teardown; a query failure or stop timeout invalidates evidence. The
activation gate requires an observer to survive multiple queries and acknowledge
stop. The completion observer and lease sampler share the separate observer pool, so their sample
cadence is nominal rather than a guaranteed deadline.

## Provenance

Every matrix run receives a new timestamped directory and `provenance.json`.
The resolved L7/profile drain budget is recorded even when its environment
variable is unset. CSV rows include commit, dirty flag, source SHA-256 and TCP delay. The digest
covers current repository inputs (including untracked code, excluding results)
and the sibling Sinal source. The matrix refuses to finish successfully if the
source digest changes during the run. `GRIND_BENCH_RELEASE_EVIDENCE=1` rejects a
dirty checkout before collecting evidence. A dirty exploratory run remains
useful for development, but must not be relabelled as clean release evidence.

Old committed CSV files are historical evidence. The repaired composite is now
accepted as exploratory evidence; it does not retroactively validate the
provisional L2–L6 results or change their provenance.

## B1–B10 repair status

The repeated repaired composite passed its full numeric, raw-artifact and
provenance audit. The accepted report is
`bench/results/l7-drain-pair-20260928T091612Z/composite-audit-v4.json`; its retained
parent terminal witness records exit 0. The detailed repair ledger and source
references are in [RELEASE-READINESS.md](../docs/RELEASE-READINESS.md), with current
measurements in [PERFORMANCE-EVIDENCE.md](../docs/PERFORMANCE-EVIDENCE.md).

The final PostgreSQL gate passed 38 tests, its 1,000-job audited smoke and
L2/L3/L5/L7/profile activation checks in `bench/results/repaired-gate-garghm/`.
A separate deliberate 1ms drain timeout retained failure diagnostics, completion
counts and sampler coverage in `bench/results/drain-timeout-negative-2w6b1o/`.
Its corrected checker validates expected failure retention, not performance;
the original stale-literal checker failure remains recorded.

A later formatting incident affected 17 retained JSON files. All formatted
variants are preserved; nine preexisting pinned byte sequences were restored,
and eight unpinned files remain formatted. The unchanged composite v4 re-audit
accepted the evidence with no issues (actual session 66728, exit 0); its report,
empty stderr and terminal result are retained beside the incident record.
Permanent result-folder formatter exclusions are implemented; all 11,177 retained
evidence files remained byte-identical after targeted documentation/configuration formatting. See the incident record in
[PERFORMANCE-EVIDENCE.md](../docs/PERFORMANCE-EVIDENCE.md).

These are dirty, content-pinned exploratory snapshots. Earlier provisional
measurements, the 34-test gate and seven-profile smoke retain their original
scope. The original matrix wrapper failed delayed L7’s 60-second drain bound;
that failure remains retained. A fresh zero/5ms L7 pair uses the same repaired
harness and explicit 600-second soft drain budget. The composite accepts the
unaffected original-source arms and the fresh pair, with three exact L2
setup/cleanup log incidents adjudicated separately.

Every measured T2 fault target activated. All healthy siblings succeeded with
valid receipts and headroom above L/10. The aggregate `t2_triggered` flag can
still be true when selected fault targets become uncertain; it is distinct
from healthy-sibling acceptance. T3 remains triggered.

The owner-approved fresh two-hour resilience run passed all 14 standalone cases
and 266 mixed rounds over 7,202.060719 seconds after warm-up, retaining all
healthy-job, fencing and resource assertions. Actual run session 7230 and audit
session 97027 exited 0. The [final audit](../resilience/results/repaired-7200s-8YvcJq/soak-audit-v5.json)
and [paired M2/M6 comparison](../resilience/results/repaired-7200s-8YvcJq/paired-comparison.json)
are accepted local exploratory evidence; they do not establish a clean release
baseline or remove the documented resource and coverage limits.
The owner approved the prospective duration change on 2026-09-28. The earlier
86,400-second attempt in
`resilience/results/repaired-86400s-o23a23/` remains failed after a 275-second
host software sleep crossed a healthy job's lease expiry. Its elapsed time does
not count toward the fresh run. Day-long endurance remains unverified.
