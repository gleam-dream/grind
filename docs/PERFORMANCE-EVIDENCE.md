# Performance evidence

Load, coordinator-bottleneck, and multi-node/soak evidence for
Grind. Load evidence is produced by `bench/` (see `bench/priv/bench.sql`,
`bench/src/grind_bench/audit.gleam`, and `bench/src/grind_bench/load.gleam`)
against a disposable cluster started by `scripts/bench-postgres.sh` /
`scripts/bench-matrix.sh`.

Every number in this document is **laptop, indicative** unless a row's own
environment header says otherwise (see `docs/RISKS.md` #17 and the bench
planning notes, "User decisions", item 4: laptop runs are the release
evidence, labeled clearly as such — this is not a dedicated benchmarking
server, and the bench harness shares CPU with the PostgreSQL server under
test on the same machine).

Raw per-tick JSONL evidence is retained locally and is not committed. Historical
CSV rollups are committed under their original result directories. The repaired
2026-09-28 composite, its source archives, raw files and reports remain local,
uncommitted exploratory evidence.

The original L1/L7 tables use `bench/results/2026-09-26-25894a6/`; later historical
sections identify their own snapshots and provisional limits. The final
“Repaired benchmark composite — 2026-09-28” appendix records current validation.
It preserves the earlier failed and provisional results rather than treating
them as accepted measurements of the repaired runtime.

## Environment

```
Commit:            25894a6
Date:              2026-09-26
OS:                macOS 26.5.2 (Darwin 25.5.0 arm64)
CPU:               Apple M2 Max, 12 logical cores
RAM:               32 GB
PostgreSQL:        16.15 + scripts/bench-matrix.sh's GUCs
                   (shared_preload_libraries=pg_stat_statements,
                   log_lock_waits=on, deadlock_timeout=100ms,
                   track_io_timing=on, synchronous_commit=local)
OTP / Gleam:       28 / 1.18.1
pool_size:         max(consumers * concurrency, 10) (grind_bench.setup)
D (lease/deadline): 4000ms (postgres.settings default, unchanged)
L (lease duration): 30000ms (queue.default_policy default, unchanged)
J (max batch):     1 (default; automatic polling ignores it -- see
                   docs/RISKS.md #6)
I (poll interval): 10ms (L1) / 5ms (L7)
delay/cost_ms:     0 or 10 (L1, per row); fixed at 1 (L7)
repeats:           3 per point, one discarded warm-up run before them
                   (scripts/bench-matrix.sh)
```

**macOS fsync caveat.** This run's disposable cluster is on macOS. macOS's
own `fsync` is cheaper than Linux's `fdatasync` on real, spinning-committed
disks — a commit that must durably reach the storage device is measurably
more expensive on Linux with default durability settings than the same
commit on this machine. Every latency and throughput number in this
document is understated relative to a durability-equivalent Linux host;
this affects every scenario that commits transactions (all of them), not
one row in particular.

## L1: drain throughput matrix

Scenario: `gleam run -m grind_bench/load -- l1 <job_count> <consumers>
<concurrency> <queues> <cost_ms> <repeat>` (see `bench/src/grind_bench/load.gleam`'s
own module doc comment). `cost_ms` stands in for both the plan's "job cost"
and "delay" axes — this harness has one lever (an artificial
`process.sleep` inside the bench worker's handler) for simulated per-job
work, not two independent ones.

Mean/min/max are over the 3 repeats `scripts/bench-matrix.sh` ran per point
(one discarded warm-up run precedes them). "DB CPU % of one core" is the
mean of each repeat's own `db_cpu_ms / elapsed_ms * 100` (`ps` cputime for
the postmaster + descendants, from `GRIND_BENCH_PG_DATA_DIR`). "CPU-ms per
1k jobs" is the mean `db_cpu_ms` divided by `job_count`, times 1000 —
independent of how long the run took, a rough proxy for the actual
PostgreSQL-side cost per job.

| consumers×concurrency×queues (cost_ms) | job_count | jobs/s mean | jobs/s min | jobs/s max | DB CPU % of 1 core (mean) | CPU-ms / 1k jobs (mean) |
| -------------------------------------- | --------- | ----------- | ---------- | ---------- | ------------------------- | ----------------------- |
| 1×10×1 (c0)                            | 12000     | 973.1       | 964.2      | 984.8      | 85.6%                     | 880.3                   |
| 2×10×1 (c0)                            | 20000     | 1612.6      | 1506.5     | 1749.6     | 147.3%                    | 916.3                   |
| 1×50×1 (c0)                            | 10000     | 725.5       | 606.9      | 806.7      | 79.7%                     | 1114.7                  |
| 4×10×2 (c0)                            | 30000     | 2739.5      | 2711.7     | 2773.4     | 251.7%                    | 919.0                   |
| 4×10×1 (c10)                           | 25000     | 2657.2      | 2619.7     | 2721.8     | 252.4%                    | 950.0                   |

Raw values: `bench/results/2026-09-26-25894a6/l1.csv` (per-scenario BEAM/DB
sampler percentiles: `samplers.csv` in the same directory; statement-split
means: `statements.csv`, see below).

Two things are visible with ranges that do not overlap, so both are safe to
state as real, not noise:

1. **Raising one consumer's own concurrency from 10 to 50 with zero-cost
   jobs made throughput worse, not better.** `1×10`'s range (964.2-984.8
   jobs/s) sits entirely above `1×50`'s range (606.9-806.7 jobs/s) — every
   `1×50` repeat was slower than every `1×10` repeat. A single coordinator's
   own claim/ack round trip, not free execution slots, is the ceiling here
   (see "T3 verdict" below and `docs/RISKS.md` risk 19).
2. **Adding independent coordinators scales close to linearly.** `4×10×2`
   (4 consumers, 2 queues, same total concurrency as `2×10×1` doubled again)
   reached 2739.5 jobs/s mean against `2×10×1`'s 1612.6 — the two ranges do
   not overlap either.

DB CPU stayed under 2.6 core-equivalents (252% of one core) at the busiest
L1 point measured (`4×10×2`, `4×10×1×c10`) — comfortably inside this
12-core machine's own capacity; PostgreSQL was never close to saturated in
this matrix (see "T3 verdict" for the fuller discussion of what "% of one
core" does and does not tell you here).

## L7: coordinator bottleneck

Scenario: `gleam run -m grind_bench/load -- l7 <job_count> <consumers>
<concurrency> <repeat>` — always one queue, `cost_ms` fixed at 1 (0 ms job
body + the plan's "delay 1 ms", collapsed onto the same lever as L1).
`coordinator_mqlen_p50`/`p99` are percentiles over every consumer's own
coordinator `Pid` `message_queue_len`, sampled every 20ms and pooled across
every coordinator in the shape (not per-coordinator).

| shape (consumers×concurrency) | job_count | jobs/s mean | jobs/s min | jobs/s max | mqlen p50 | mqlen p99 | DB CPU % of 1 core (mean) | CPU-ms / 1k jobs (mean) |
| ----------------------------- | --------- | ----------- | ---------- | ---------- | --------- | --------- | ------------------------- | ----------------------- |
| 1×50                          | 9000      | 896.7       | 868.5      | 923.6      | 48        | 50-99     | 85.9%                     | 958.5                   |
| 5×10                          | 33000     | 2799.2      | 2534.4     | 2959.6     | 9-10      | 19-23     | 296.2%                    | 1062.8                  |
| 10×5                          | 46000     | 4080.3      | 3921.2     | 4167.0     | 5         | 10-12     | 476.8%                    | 1168.8                  |

Raw values: `bench/results/2026-09-26-25894a6/l7.csv`. All three ranges are
disjoint: `1×50` never reaches `5×10`'s slowest repeat, and `5×10` never
reaches `10×5`'s slowest repeat — the same total concurrency, split across
more independent coordinators, reliably goes faster. `1×50`'s coordinator
mailbox sits at p50=48/p99 up to 99, an order of magnitude above `5×10`'s
(p50 9-10) and two above `10×5`'s (p50 5) — consistent with one coordinator
process queuing up claim/ack traffic behind itself, not database capacity,
being the ceiling (see "T3 verdict").

## Statement split (per scenario, L1 only)

`bench/src/grind_bench/statement_split.gleam`'s before/after
`pg_stat_statements` rollup, classified into `ack` / `quarantine` /
`claim_or_other_grind` / `ledger` (the bench harness's own writes,
subtracted from Grind's own cost) buckets. **L7 did not capture this split**
this run (`grind_bench/load.run_l7` never calls
`write_statement_split_rows` — only `run_l1` does); this is a gap in the
harness's own instrumentation, not a claim that L7 has no such cost. Means
are over the same 3 repeats as the L1 table above.

| scenario (job_count) | bucket               | mean calls | mean total_exec_time_ms |
| -------------------- | -------------------- | ---------- | ----------------------- |
| 1×10×1×c0 (12000)    | claim_or_other_grind | 24643.7    | 3366.6                  |
| 1×10×1×c0 (12000)    | ack                  | 24000.0    | 469.2                   |
| 1×10×1×c0 (12000)    | quarantine           | 12012.0    | 59.9                    |
| 1×10×1×c0 (12000)    | ledger               | 12001.0    | 67.4                    |
| 2×10×1×c0 (20000)    | claim_or_other_grind | 40417.3    | 4599.7                  |
| 2×10×1×c0 (20000)    | ack                  | 40000.0    | 896.0                   |
| 2×10×1×c0 (20000)    | quarantine           | 20026.0    | 170.1                   |
| 2×10×1×c0 (20000)    | ledger               | 20001.0    | 142.0                   |
| 1×50×1×c0 (10000)    | claim_or_other_grind | 20833.3    | 3471.8                  |
| 1×50×1×c0 (10000)    | ack                  | 20000.0    | 489.8                   |
| 1×50×1×c0 (10000)    | quarantine           | 10052.0    | 168.1                   |
| 1×50×1×c0 (10000)    | ledger               | 10001.0    | 98.4                    |
| 4×10×2×c0 (30000)    | claim_or_other_grind | 60267.3    | 5239.3                  |
| 4×10×2×c0 (30000)    | ack                  | 60000.0    | 1448.6                  |
| 4×10×2×c0 (30000)    | quarantine           | 30053.7    | 450.5                   |
| 4×10×2×c0 (30000)    | ledger               | 30001.0    | 246.1                   |
| 4×10×1×c10 (25000)   | claim_or_other_grind | 50261.0    | 4520.9                  |
| 4×10×1×c10 (25000)   | ack                  | 50000.0    | 1236.5                  |
| 4×10×1×c10 (25000)   | quarantine           | 25050.7    | 388.7                   |
| 4×10×1×c10 (25000)   | ledger               | 25001.0    | 264.1                   |

Two ratios hold steady across every point in this table (within a few
percent), matching the bench planning notes' finding #2 empirically:
**quarantine calls track job count almost 1:1** (e.g. 12012 quarantine calls
for 12000 jobs — one `LIMIT 1` quarantine scan per claim attempt, as
`attempt.claim_one` runs it unconditionally before every claim, not only
when a lease has actually expired); **ack is two statements per job** (e.g.
24000 ack-bucket calls for 12000 jobs); **claim/other-grind is a little
over two statements per job** (24643.7 for 12000 jobs — one claim per job
plus a handful of empty polls that found nothing to claim). The harness's
own `ledger` bucket (`bench_effects`/`bench_submissions` writes) is
excluded from every "Grind's own cost" reading above by construction (its
own separate bucket), and stays a small fraction of total exec time
throughout (under 6% of the `claim_or_other_grind` bucket's own total at
every point).

## Coordinator profile

Statistical sampling of each L7 coordinator's own `current_function` and
total `reductions` every 2ms for the run's own duration
(`bench/results/2026-09-26-25894a6/profile.csv`), for the two shapes where
the coordinator's own serial cost matters most.

| label        | function                                                            | % of samples | reduction rate/s |
| ------------ | ------------------------------------------------------------------- | ------------ | ---------------- |
| profile-1×50 | `prim_inet:recv0/3` (waiting on the network for PostgreSQL's reply) | 76.0%        | 12,409,122       |
| profile-1×50 | `erlang:bif_handle_signals_return/2`                                | 16.0%        | 12,409,122       |
| profile-1×50 | `pgo_pool:checkout_call/4` (checking out a pool connection)         | 4.8%         | 12,409,122       |
| profile-1×50 | `gen:do_call/4`                                                     | 2.8%         | 12,409,122       |
| profile-5×10 | `prim_inet:recv0/3`                                                 | 70.8%        | 33,112,478       |
| profile-5×10 | `erlang:bif_handle_signals_return/2`                                | 23.6%        | 33,112,478       |
| profile-5×10 | `pgo_pool:checkout_call/4`                                          | 2.9%         | 33,112,478       |
| profile-5×10 | `gen:do_call/4`                                                     | 1.9%         | 33,112,478       |

At `1×50`, three-quarters of every sample taken of the (single) coordinator
process is spent blocked on the network waiting for PostgreSQL's reply, not
doing computation — this is what "the coordinator, not the database, is the
ceiling" means concretely: the process has nothing else to do while one
round trip is outstanding, because it never has more than one outstanding
at a time.

## T3 verdict

T3 (bench planning notes, "Per-attempt storage decision thresholds"):
**`1×C50` < 70% of `5×C10`'s throughput, while DB CPU < 50%.**

**Ratio, from the L7 means above:** `896.7 / 2799.2 = 32.0%` — well under
the 70% threshold. `1×C50` delivers less than a third of `5×C10`'s
throughput at identical total concurrency (50) and identical pool size.

**DB CPU, both ways.** The L7 table's own "DB CPU % of 1 core" column reads
85.9% at `1×50` — over the 50% mark if read as a literal percentage. But
that reading conflates "one PostgreSQL backend, serving one connection at a
time, is nearly always busy while a request is outstanding" (which is true
of _any_ single-connection workload, however small the whole server's own
load is) with "the database server itself is short on capacity." This
benchmark machine has 12 logical cores; PostgreSQL can spread across many
backends if given more concurrent connections/queries. Reading the same
number as a **share of the whole 12-core machine** (`85.9% / 12 = 7.2%`)
answers the question T3's own "<50% DB CPU" clause is actually asking —
"does the database have spare capacity, or is it the bottleneck too?" — and
the answer is unambiguous: **7.2%**, nowhere near saturated. The same
division applied to `5×10` (296.2%/12 = 24.7%) and `10×5` (476.8%/12 =
39.7%) tells the same story at every shape in this matrix: PostgreSQL never
used more than 40% of the whole machine even at the highest-throughput
shape measured.

**This document's verdict uses the whole-machine share, not the
per-core percentage**, because T3's own purpose is to distinguish "the
coordinator is the ceiling" from "the database is the ceiling," and only
the whole-machine reading can tell those apart — the per-core reading would
misreport almost every single-connection workload as "database at capacity"
regardless of how idle the rest of the server actually is.

**Verdict: T3 triggered** (32.0% < 70%, and 7.2% < 50% by the
whole-machine reading that the clause is meant to test). The coordinator
profile above corroborates the mechanism directly: at `1×50`, 76% of the
coordinator's own sampled time is spent blocked on the network waiting for
PostgreSQL, not computing, and it never has more than one such round trip
outstanding — consistent with roughly 0.4ms of measured `pg_stat_statements`
exec time per job (from the statement-split table: `claim_or_other_grind`
3471.8ms / 20833.3 calls × ~2.08 calls/job + `ack` 489.8ms / 20000 calls × 2
calls/job + `quarantine` 168.1ms / 10052 calls × 1 call/job ≈ 0.40ms/job)
against roughly 1.1ms of measured wall-clock elapsed time per job at that
same shape (10043ms mean elapsed / 9000 jobs) — the difference is the
coordinator's own serial round-trip overhead, not server-side work.

**Remedy: triggered; remedy deferred by user decision 2026-09-27.** Both
identified remedies are recorded as tracked, post-release performance
optimizations, not implemented in this change and not a release blocker —
see `docs/RISKS.md` risk 19 and `docs/RELEASE-READINESS.md` §4 for the full
write-up:

1. **Batch claim** (Oban's `fetch_jobs` shape): one claim statement claims
   up to the number of free slots at once, plus one expired-lease
   quarantine sweep per poll instead of one per claim.
2. **Ack from the attempt process** (Oban's `executor` shape): each
   attempt's own worker process runs its acknowledgement transaction and
   reports only the outcome to the coordinator, so acks run in parallel,
   bounded by the pool, instead of serially on the coordinator's loop.
   Fencing stays enforced in SQL; the pending-ack retry moves into or beside
   the attempt process, which touches the proven recovery code in
   `grind/internal/attempt` and needs its own design pass with every
   `docs/RECOVERY-EVIDENCE.md` test staying green.

A renewal-only coordinator-adjacent process (the first remedy considered for
`docs/RISKS.md` risk 4) would **not** address T3: it moves lease renewal off
the coordinator, not claim or ack, so the single-coordinator claim/ack
serialization T3 measures would remain unchanged. Where lease renewal itself
should live is being revisited once this document's own L6 (renewal
starvation) T1/T2 results, below, are in. Until either remedy lands, the
mitigation is topological: prefer more consumers (or queues) at a moderate
`maximum_concurrency` (around 10 per consumer, the shape measured above to
scale close to linearly) over one consumer at very high
`maximum_concurrency`.

## Limitations

- **L7 uses a 1ms handler sleep, not network delay.** The bench harness has
  one lever for simulated per-job work (`process.sleep` inside the handler);
  a proxy-based delay-injection mode (plan implementation-order step 7,
  separating "job cost" from "network delay" into two independent axes) has
  not been built yet. Every number in this document runs over loopback,
  which understates real client-database round-trip latency — the
  coordinator-bottleneck effect measured above would very plausibly be
  _more_ pronounced, not less, over a real network, since each blocked
  `prim_inet:recv0` wait would simply be longer.
- **Harness statements (the `ledger` bucket) are included in each row's own
  `db_cpu_ms`/`db_cpu_pct_of_core`** (that measurement is whole-postmaster
  `ps` cputime, which cannot distinguish which connection/role did the
  work), **but their own exec time is split out** in the statement-split
  table above (the separate `ledger` bucket), so a reader who wants
  Grind-only exec time can subtract it from the `pg_stat_statements` total
  directly; the aggregate CPU-time columns themselves are not adjusted for
  it.
- **Laptop numbers are indicative.** This is a single M2 Max laptop sharing
  its 12 cores between the PostgreSQL server under test, the BEAM runtime
  under test, and the bench harness's own driver process — not a dedicated
  benchmarking host. Treat every ratio and threshold verdict above as a
  directionally reliable signal from this one environment, not a production
  capacity plan; a Linux server with dedicated cores for each side would be
  expected to show different absolute numbers (see the macOS fsync caveat
  above for one specific, known-directional difference).

## L2-L6 environment

```
Commit:            548c31e
Date:              2026-09-27
OS/CPU/RAM/PostgreSQL/OTP/Gleam: same machine and versions as the L1/L7
                   run above (see "Environment").
```

Every number in this section is from `bench/results/2026-09-27-548c31e/`
(`l2.csv`, `l3.csv`, `l4.csv`, `l5.csv`, `l6_t1.csv`, `l6_t2.csv`,
`samplers.csv`), produced by `scripts/bench-matrix.sh l2|l3|l4|l5|l6`.
**Correction:** every row of these CSVs carries `dirty=1` — this section
previously (incorrectly) described the run as "a clean, committed tree." The
tree was not clean when this run was captured. This does not by itself
invalidate the numbers below, but it means they are not reproduced against a
verified-clean checkout; treat every result in this section as
**provisional, harness defect tracked** for that reason alone, on top of the
specific per-scenario defects called out below. L2-L5 use
`scripts/bench-matrix.sh`'s own warm-up-plus-3-repeats discipline; **L6's own
points use 2 repeats with no discarded warm-up** — each point is tens of
seconds (L6T2's own points run close to a minute), so a warm-up run would
roughly double an already expensive scenario for a threshold decision that
needs its own real numbers more than cache-priming. Every point below is a
**reduced representative subset** of the bench planning notes' fuller
sweeps, chosen to keep this whole section's own matrix time to about 23
minutes total; each scenario's own reduction is called out in its
subsection.

### Harness defects found in this run (tracked, not fixed here)

An independent review of this evidence (2026-09-27, commits `1ad3f0b..eb41667`)
found the following measurement defects in the bench harness itself, separate
from anything about Grind's own behavior. These are recorded here as tracked
bench-harness fixes (see `docs/RELEASE-READINESS.md` for the checklist) and
are **not fixed in this documentation-only pass**:

- **B1.** The L6T2 setup cannot overlap a stall with a renewal: all jobs are
  claimed together, cost exactly `3L`, and finish together, so the slow acks
  run only after every last renewal has already happened (renewal counts in
  the raw data are exactly `8 × ` the non-stalled sibling count).
- **B2.** The headroom metric is survivor-biased: only a _successful_ renewal
  writes a lease-log row, so a starved renewal is invisible to it and the
  measured minimum headroom can never go below zero — the "min headroom <
  `L/10`" half of T2's own threshold is untestable with this metric as built.
  An ack-headroom / uncertain-transition metric is needed instead.
- **B3.** L5 never prunes during the traffic it measures: the run is 2
  seconds against `max_age_ms=3000`, so `pruned_now = 0` at every point in
  `l5.csv`. The on/off comparison in this run measures idle pruner polling
  overhead only, not pruning-under-load.
- **B4.** L2's "flat" reading is an artifact of the poll interval fixing the
  call counts, not evidence that cost is flat: the real per-call cost column
  (`quarantine_total_ms`) rises 1.5-2.5x with 100k filler rows in the
  reviewer's own reanalysis of the same run (e.g. `1×250ms`: ~0.20 ->
  ~0.52ms; `8×50ms`: ~3.7 -> ~5.4ms), while `claim_total_ms` stays flat. One
  `8×50ms` row has 440 calls in the raw data, not the 448 this document's L2
  table implies. The run also used 100k filler rows, not the 1M the plan
  called for (already noted in "L2: polling cost" below as a wall-clock
  reduction, but worth restating here as a scale caveat on the "flat"
  finding).
- **B5.** L3 is closed-loop per pacer, not open-loop as intended, and runs
  5-12% under its own nominal arrival rate; `poll_interval=10` (not the
  plan's 250) inflates DB CPU; claim-to-start latency is not reported at all.
- **B6.** L4's zero `AdmissionContended` is plausible, but the p99 growth
  the table shows tracks submitter count/pool size/CPU, not lock contention —
  cold keys (no shared lock at all) show the same ~37ms p99 with zero lock
  waits as hot keys do. `waiting_locks` rests on roughly 4 samples, taken by
  a sampler on Grind's own pool. No consumer runs during L4, so
  admission-vs-claim contention specifically is never exercised.
- **B7.** The L6 instrumentation red/green tests are shallower than they
  look: "red" proves only "the trigger is not installed," and the slow-ack
  test does not run inside a genuine acknowledgement. Separately,
  `float_literal_ms` (`bench/src/grind_bench/instrumentation.gleam`
  ~185-199) does not zero-pad a millisecond remainder between 10 and 99
  (e.g. 1050ms renders as `"1.50"`, not `"1.050"`); the two delay values this
  run actually used (1600ms, 3200ms) are unaffected by that bug.
- **B8.** T1 is weakly exercised: jobs run 25 seconds, under `L=30000`,
  giving only 2 renewal ticks each; claim waves are phase-aligned; only 2
  repeats per point. The "not triggered" verdict still stands with a wide
  (~125x) margin regardless. `p99_headroom_ms` in the underlying data is an
  upper tail and is not a meaningful number to read on its own; this
  document correctly uses the minimum instead.
- **B9.** Every row of these CSVs has `dirty=1` (see above), contradicting
  the "clean, committed tree" this section previously claimed.
- **B10.** T2 was never run at the real, undivided scale
  (`D=4000ms`, `L=24000ms`) this project actually ships with — see "L6T2"
  below. Staggered (non-simultaneous) job costs, `K` in `{3..6}`, and the
  plan's own 1.2D acks-that-time-out variant all remain unrun.

Still open, outside this run's scope entirely: multi-node (M1-M7), soak, the
fault-proxy delay/partition modes, and `bench-smoke.sh`.

## L2: polling cost

Idle consumers polling an empty, never-fed queue for a fixed 3-second
window, with `filler_rows` already-`succeeded` rows bulk-inserted directly
first (bypassing `submit`/preload entirely) to simulate a large
finished-jobs table cheaply. Reduced from the plan's `{1,8} x {10,50,250,1000}ms
x {0, 1M}` to `{1,8} x {50,250}ms x {0, 100k}` (8 points) for wall-clock
feasibility. **Relaxes I1/I2/I5**: no bench-tracked job is ever submitted
(filler rows are inserted outside the ledger), so those checks are vacuous
here; I3/I4/I6/I7 are still checked (`reduced_audit_and_report`), and all 8
points x 4 runs (1 warm-up + 3 repeats) passed.

| consumers | interval_ms | filler_rows | claims/s (mean) | quarantine/s (mean) | DB CPU % of 1 core (mean) |
| --------- | ----------- | ----------- | --------------- | ------------------- | ------------------------- |
| 1         | 50          | 0           | 18.67           | 18.67               | 4.11                      |
| 1         | 50          | 100,000     | 18.67           | 18.67               | 4.11                      |
| 1         | 250         | 0           | 4.00            | 4.00                | 2.78                      |
| 1         | 250         | 100,000     | 4.00            | 4.00                | 2.89                      |
| 8         | 50          | 0           | 148.44          | 148.44              | 21.00                     |
| 8         | 50          | 100,000     | 149.33          | 149.33              | 22.11                     |
| 8         | 250         | 0           | 32.00           | 32.00               | 6.67                      |
| 8         | 250         | 100,000     | 32.00           | 32.00               | 6.56                      |

Two things hold across every one of the 24 repeats behind this table: (1)
**claims/s and quarantine/s are identical between `filler_rows=0` and
`filler_rows=100,000`** at every consumers/interval combination (down to the
exact same call counts in the underlying CSV) — the indexes added in
`grind_v12` (`grind_jobs_claim_idx`, `grind_jobs_quarantine_idx`) keep
polling cost flat regardless of how large the finished-jobs table is, exactly
as the plan's own L2 description predicted; (2) **quarantine calls equal
claim calls exactly** at every point (e.g. 448/448 at `8x50ms`) — confirming
the bench planning notes' finding #2 empirically: `attempt.claim_one` runs
its `LIMIT 1` quarantine scan unconditionally before every claim attempt,
including an empty poll that finds no work.

**Provisional, harness defect tracked (B4).** This table's own call counts
are fixed by the poll interval, so "flat across `filler_rows`" is a fact
about call _counts_, not about per-call _cost_: an independent reanalysis of
this same run found `quarantine_total_ms` rising 1.5-2.5x as `filler_rows`
goes from 0 to 100,000 (e.g. `1×250ms`: ~0.20ms -> ~0.52ms; `8×50ms`:
~3.7ms -> ~5.4ms), while `claim_total_ms` stays flat. Read "claims/s and
quarantine/s are identical" above as "throughput is identical" (true, and
expected, since throughput here is poll-interval-bound), not as "the
underlying quarantine scan got no more expensive" (false at this row count).
Also: one `8×50ms` row has 440 calls in the raw data, not 448.

## L3: open-loop latency

Open-loop arrival (via `grind_bench/load.run_open_loop`, several parallel
pacer processes through the real `postgres.submit` path, never bulk
preload) at `{50, 200, 1000}` jobs/s for 5 seconds, against the plan's own
fixed 4-consumer x C10 shape. Full, unrelaxed I1-I7 audit (a healthy,
ordinary run, just paced by arrival instead of preloaded) — all 12 runs (1
warm-up + 3 repeats x 3 rates) passed.

| arrival/s | insert→finish p99 (mean/min/max, ms) | start→ack p99 (mean/min/max, ms) | DB CPU % of 1 core (mean) |
| --------- | ------------------------------------ | -------------------------------- | ------------------------- |
| 50        | 17.25 / 16.23 / 18.26                | 5.73 / 5.29 / 6.38               | 56.96                     |
| 200       | 16.44 / 15.69 / 17.50                | 6.02 / 5.86 / 6.12               | 76.31                     |
| 1000      | 16.85 / 16.16 / 17.78                | 5.28 / 4.86 / 5.75               | 132.84                    |

insert→finish p99 and start→ack p99 both stay essentially flat (~16-17ms /
~5-6ms) all the way from 50 to 1000 jobs/s — this 4xC10 shape has ample
spare capacity at every rate tested (see L1's own `4x10` throughput,
comfortably above 1000/s), so this table is mostly measuring the harness's
own per-job overhead (a `postgres.submit` round trip plus one claim/ack
cycle), not queueing delay. DB CPU scales with rate as expected (57% → 76%
→ 133% of one core) but never approaches this 12-core machine's own
capacity.

**Provisional, harness defect tracked (B5).** This scenario is closed-loop
per pacer process, not open-loop as the plan intends, and runs 5-12% under
its own nominal arrival rate at every point in this table. `poll_interval=10`
(not the plan's 250) also inflates DB CPU relative to a more realistic
polling cadence. Claim-to-start latency — a component the plan asks for
separately from insert-to-finish and start-to-ack — is not reported at all
by this run.

## L4: unique contention

`submit_unique` (`KeepExisting`, `while_retained()`, `IncompleteOrSucceeded`)
from `{4, 16, 64}` parallel submitter processes against either 10
deterministic "hot" keys (`index % 10`) or up to 4,000 deterministic
"cold" keys (`index` directly, never repeated — reduced from the plan's
10,000-key pool, since this run's own `total_submissions` never approaches
that many). No consumer runs: this measures the admission SQL layer alone.
All 36 runs (1 warm-up + 3 repeats x 6 points) passed their own reduced
audit ("one row per key": exactly 10 `grind_jobs` rows under the hot queue
regardless of submitter count or submission count; `row_count == inserted`
for cold).

| submitters | mode | admissions/s (mean) | contended (of total) | latency p50/p99/max (ms, mean) | waiting_locks (mean, from samplers.csv) |
| ---------- | ---- | ------------------- | -------------------- | ------------------------------ | --------------------------------------- |
| 4          | hot  | 4022.0              | 0 / 1500             | 1 / 2 / 16.7                   | 0.0                                     |
| 16         | hot  | 6235.9              | 0 / 1488             | 2 / 17.3 / 20.3                | 1.08                                    |
| 64         | hot  | 5796.4              | 0 / 1472             | 9.7 / 34.0 / 39.7              | 46.33                                   |
| 4          | cold | 3565.1              | 0 / 4000             | 1 / 2 / 15.3                   | 0.0                                     |
| 16         | cold | 5949.9              | 0 / 4000             | 3 / 4 / 18.0                   | 0.0                                     |
| 64         | cold | 5556.9              | 0 / 3968             | 11 / 37.0 / 45.3               | 0.0                                     |

**Zero `AdmissionContended` at every point**, even 64 submitters hammering
just 10 hot keys — the bounded advisory-lock wait (default
`unique_lock_wait_ms=2000`) never actually timed out here; contention shows
up instead as **real lock waiting** (`pg_locks`, sampled every 50ms during
the run): `waiting_locks` climbs to a mean of ~46 at 64 hot submitters
against 0 at every cold-mode point and at 4 hot submitters — the admission
layer serializes correctly under heavy same-key contention rather than
erroring, at the cost of latency (p99 34ms at 64 hot vs 2ms at 4 hot), not
correctness.

**Provisional, harness defect tracked (B6).** Zero `AdmissionContended` is a
real and plausible result, but this run does not establish that the p99
growth above comes from lock contention specifically: cold keys, which share
no lock at all, show the _same_ ~37ms p99 at 64 submitters with zero lock
waits recorded — the growth tracks submitter count, pool size, and CPU, not
contention. `waiting_locks` itself rests on roughly 4 samples per point,
taken by a sampler on Grind's own connection pool (not an independent
observer), so treat its absolute values as indicative only. No consumer runs
during L4 at all, so admission-vs-claim contention — a scope this scenario's
own name suggests but does not cover — is not exercised here.

## L5: pruner concurrent

The same `run_open_loop` at 200 jobs/s for 2 seconds, 4xC10, with
`grind/pruner` either off (control) or on (`interval_ms=1000, limit=10000,
max_age_ms=3000` — `max_age_ms` reduced from the plan's 5s for wall-clock
feasibility within this run's own short duration). Latency is read
immediately after drain, before pruning gets any chance to delete the rows
it joins against, so the on/off comparison is on the same footing both
ways. **`pruner_on=1` relaxes I1/I2/I5** (pruning deletes rows the ledger
still references); `pruner_on=0` runs the full, unrelaxed audit as the
control. All 8 runs (1 warm-up + 3 repeats x 2 conditions) passed their own
audit.

| pruner | insert→finish p99 (mean/min/max, ms) | start→ack p99 (mean/min/max, ms) | dead tuples (mean) | prune_failed |
| ------ | ------------------------------------ | -------------------------------- | ------------------ | ------------ |
| off    | 17.73 / 14.10 / 21.92                | 7.19 / 5.74 / 9.58               | 724.3              | 0            |
| on     | 25.54 / 16.61 / 43.15                | 13.58 / 5.89 / 28.93             | 1130.0             | 0            |

At this load (200/s, well inside 4xC10's own headroom), pruning running
concurrently shows **no consistent p99 regression** — 2 of 3 "on" repeats
sit close to the "off" baseline (~16-17ms / ~6ms), and one repeat spikes to
43ms/29ms, consistent with the plan's own expectation that concurrent
pruning _can_ perturb claim/ack latency under contention, just not on every
tick at this load. `prune_failed` stayed at 0 throughout (no deadlocks, no
`"still waiting for"` lines — I6 held in both conditions). Dead tuples are
**higher with the pruner on** (1130 vs 724 mean) — the pruner's own
`DELETE`s (and their `ON DELETE CASCADE` receipt-table deletes) are
themselves a source of dead tuples, on top of ordinary claim/ack churn; a
real deployment should account for this in its own autovacuum tuning, not
assume pruning is autovacuum-neutral.

**Provisional, harness defect tracked (B3).** This run's own traffic window
(2 seconds against `max_age_ms=3000`) never actually gives the pruner
anything to prune during the measured period: `pruned_now = 0` at every
point in `l5.csv`. The on/off comparison above measures the cost of the
pruner's own idle polling running concurrently with traffic, not the cost of
pruning rows while traffic is live — the scenario this section's own name
promises has not actually been run yet.

## L6 instrumentation: lease-log and slow-ack triggers

`grind_bench/instrumentation` installs two bench-owned PostgreSQL triggers,
scoped to one run's own dynamically-named Grind schema (dropped
automatically with it, never touching `grind`'s own source): a lease-log
trigger (`AFTER UPDATE` on `grind_jobs`, mirroring old/new
`attempt_id`/`state`/`lease_expires_at` into
`grind_bench.bench_lease_log`) and a slow-ack trigger (`BEFORE INSERT` on
`grind_job_acknowledgements`, `pg_sleep`ing for job ids marked in
`grind_bench.bench_slow_ack_targets`). Both are proven by a dedicated
red/green test (`bench/test/grind_bench_instrumentation_test.gleam`, run
against a real disposable cluster by `scripts/bench-postgres.sh`):

- **Lease-log**: an ordinary lease-renewing `UPDATE` produces zero
  `bench_lease_log` rows before the trigger is installed (red), and exactly
  one row with a positive headroom after it is installed (green); a
  transition into `uncertain` is separately logged as a quarantine, not a
  renewal, proving the two are distinguished.
- **Slow-ack**: an acknowledgement insert for an unmarked job stays under
  200ms both before and after the trigger is installed (red/control); once
  installed and the job is marked, its own next acknowledgement insert
  takes at least the configured 300ms (green) — proving the trigger delays
  by (at least) the configured amount, and only for marked jobs.

**Provisional, harness defect tracked (B7).** Both red/green tests are
shallower than a first read suggests. "Red" for the slow-ack test proves
only that the trigger is not yet installed, not that the trigger's own
delay mechanism works correctly once it is; and the slow-ack test's own
"acknowledgement insert" does not run inside a genuine `attempt.acknowledge`
transaction. Separately, `float_literal_ms`
(`bench/src/grind_bench/instrumentation.gleam` ~185-199) has a real bug: it
does not zero-pad a millisecond remainder between 10 and 99, so 1050ms
renders as `"1.50"` instead of `"1.050"`. The two delay values actually used
by every scenario in this document (1600ms and 3200ms) are unaffected by
this bug, since neither has a remainder in that range.

## L6T1: healthy renewal lag (T1)

**T1: healthy p99 renewal lag `(2L/3 - h) > L/6` at any `C <= 50` with
defaults (`L=30000ms`, `D=4000ms`).** One consumer, `job_count =
concurrency * 3` (3 claim waves), `cost_ms=25000` (spans 2 renewal ticks at
`L/3=10000ms`), the lease-log trigger, `K=0` (healthy — no slow acks). Real,
**unmodified** defaults throughout (this threshold's own definition pins
them). Reduced from the plan's `{4,10,50}` x full sweep to `{4,10,50}` x 2
repeats each, no warm-up (documented above). Full, unrelaxed I1-I7 audit —
all 6 runs passed (K=0 is healthy; nothing should be uncertain or
quarantined, and nothing was).

| concurrency | repeat | worst headroom (ms) | worst lag `2L/3-h` (ms) | `L/6` (ms) | triggered |
| ----------- | ------ | ------------------- | ----------------------- | ---------- | --------- |
| 4           | 1      | 19959.34            | 40.66                   | 5000       | false     |
| 4           | 2      | 19983.89            | 16.11                   | 5000       | false     |
| 10          | 1      | 19983.99            | 16.01                   | 5000       | false     |
| 10          | 2      | 19980.12            | 19.88                   | 5000       | false     |
| 50          | 1      | 19987.22            | 12.78                   | 5000       | false     |
| 50          | 2      | 19979.76            | 20.24                   | 5000       | false     |

**T1 verdict: not triggered at any tested concurrency.** The worst observed
renewal lag across all 6 runs is 40.66ms (`C=4`, repeat 1) — 0.81% of the
5000ms `L/6` bound. Every renewal in this healthy matrix landed with
roughly 19,960-19,990ms of headroom on a 30,000ms lease (matching the
expected ~`2L/3` slack a mid-run renewal should have), so under real
defaults, healthy load up to `C=50` on a single consumer shows no
meaningful renewal-lag risk from ordinary coordinator business (claim/ack
traffic for other siblings) alone — the risk risk 4/19 (`docs/RISKS.md`)
and T2 below describe needs an actual stall (a slow acknowledgement), not
merely concurrency.

**Provisional, harness defect tracked (B8).** This matrix exercises T1
weakly: each job runs 25 seconds against a 30,000ms lease, giving only 2
renewal ticks per job; claim waves are phase-aligned rather than staggered;
and each point has only 2 repeats. The "not triggered" verdict above still
stands regardless — the observed margin (0.81% of the bound) is wide enough
(~125x) that a stronger exercise of this scenario is very unlikely to
overturn it, just not proven to the same standard as a fuller sweep would
be. `p99_headroom_ms` in the underlying `l6_t1.csv` is an upper tail and is
not a meaningful number to read in isolation here; this document's own use
of the minimum is the correct reading.

## L6T2: sibling starvation under slow acks (T2)

**T2: at `C=10`, `L=6D`, `K=2` slow acks, any non-stalled sibling min
headroom `< L/10`, or any sibling quarantined.** `C=10` fixed, `L=6*d_ms`,
`cost_ms=3*L` (the plan's own exact relationships), `d_ms=2000` — scaled
down from the real 4000ms default for wall-clock feasibility (noted below);
the geometry is exact, only the absolute unit shrinks. The first `K`
submitted job ids (lowest id, first claimed) are marked as slow-ack
targets, each delayed `0.8*d_ms=1600ms`. A fixed, bounded sleep is used
instead of `wait_for_drain`'s own "every job succeeded" poll (a quarantined
job may never reach `succeeded`). **Relaxes I3 in full** and I1b/I5's
"every job succeeded" shape; still checks I2/I6/I7 (`reduced_l6t2_audit_and_report`)
— all 10 runs (2 repeats x `K in {0,1,2,4,8}`) passed.

| K (slow acks) | repeat | non-stalled min headroom (ms) | `L/10` (ms) | non-stalled quarantined | any quarantined | triggered |
| ------------- | ------ | ----------------------------- | ----------- | ----------------------- | --------------- | --------- |
| 0             | 1      | 7982.75                       | 1200        | 0                       | 0               | false     |
| 0             | 2      | 7989.24                       | 1200        | 0                       | 0               | false     |
| 1             | 1      | 7988.91                       | 1200        | 0                       | 0               | false     |
| 1             | 2      | 7989.39                       | 1200        | 0                       | 0               | false     |
| 2             | 1      | 7988.50                       | 1200        | 0                       | 0               | false     |
| 2             | 2      | 7984.08                       | 1200        | 0                       | 0               | false     |
| 4             | 1      | 7982.93                       | 1200        | 0                       | 0               | false     |
| 4             | 2      | 7983.08                       | 1200        | 0                       | 0               | false     |
| 8             | 1      | 7989.70                       | 1200        | 2                       | 5               | **true**  |
| 8             | 2      | 7984.68                       | 1200        | 2                       | 5               | **true**  |

**Provisional, harness defect tracked (B1, B2, B10) — verdict corrected by
independent review.** This document originally reported "not triggered" for
T2, based on the `K in {0,1,2,4,8}` table above never showing a quarantine
below `K=8` and reading `K=2` as the plan's own literal point. **That
verdict was unsound and is corrected here: T2 is TRIGGERED.** Two defects in
this scenario's own construction hid the effect at low `K`, independently of
whether the mechanism is real:

- **B1: this setup structurally cannot show a stall overlapping a
  renewal.** Every job in one run is claimed together, costs exactly `3L`,
  and finishes together, so every slow ack in a run only ever executes
  _after_ every sibling's last renewal has already succeeded — the raw
  lease-log data confirms renewal counts are exactly `8 ×` the non-stalled
  sibling count at every `K`. A design that can only ever measure "stall
  after last renewal" cannot detect "stall overlapping a renewal," which is
  the actual mechanism T2 is meant to catch.
- **B2: the headroom metric is survivor-biased.** Only a successful renewal
  writes a lease-log row; a renewal that never happens because its sibling
  was starved writes nothing, so it is invisible to the "min headroom"
  column rather than showing up as a very negative or missing value. Minimum
  headroom can therefore never fall below what a _surviving_ renewal
  actually recorded — the "min headroom < `L/10`" half of T2's own
  threshold is untestable with this metric, by construction, not because it
  never happens.

**An independent reviewer (2026-09-27) reran this scenario outside this
repository** (raw data not committed here; cited as reviewer runs) and found
the effect this document's own table missed: at `K=5` slow acks, all 5
non-stalled siblings were quarantined; at `K=6`, all 4 were. This document's
own committed sweep jumped straight from `K=4` (0 quarantined) to `K=8` (2 of
10 quarantined) and never sampled `K=5`/`K=6`, which is exactly where the
reviewer's reproduction shows the transition actually happens at this
`D=2000`/`L=12000` scale.

**The `K=8` result in the table above is real, but its own mechanism was
misdescribed.** It is **ack starvation, not renewal starvation, and not the
plan's own `3D + (N-1)*D` chain**: by the time 8 sequential slow acks are
queued, the 2 non-stalled siblings in question have already _finished_
their handler and are waiting on an ordinary (non-slow) acknowledgement that
is itself stuck behind the 8 slow ones in the same coordinator mailbox — the
same mailbox their own `Renew` messages sit in. No pending-ack retry ever
ran in this scenario (I3 was fully relaxed for this run specifically because
retries were never exercised), so the `3D + (N-1)*D` pending-ack-retry model
does not apply to this result at all; that model describes a different
chain (an ack that itself returns `QueueAckUnknown` after taking longer than
`D`, triggering the coordinator's own bounded retry loop), which this
scenario never reaches.

**The reviewer's fuller reproduction, at real (`D=4000ms`) defaults
(`L=30000ms`):**

- Slow acks that still commit, ~3.2 seconds each: fails around `K≈7`.
- Acks timing out at the `D` bound itself: `K=5` is borderline, `K=6` fails;
  retries make the outcome worse, not better, from `K≥3` onward.
- A slow disk adding ~2 seconds to every commit, at `C=10`: fails with no
  `K` (slow acks) needed at all — ordinary commit latency alone is enough.
- **General limit derived from this reproduction:** `2L/3 ≥ C · D_eff`, i.e.
  `L ≥ 1.5 · C · D`. At `C=10` and `D=4s`, this requires `L ≥ 60s` — twice
  the shipped `L ≥ 6D` (`24s` at these values) validation rule in
  `queue.LeaseTooShortForDeadline`/`minimum_lease_for_deadline`
  (`src/grind/queue.gleam` ~231-272). **The shipped `L ≥ 6D` rule does not
  hold for `C > 2`.**
- **Not run in this repository or by the reviewer:** T2 at the real,
  undivided scale (`D=4000ms`, `L=24000ms`) with staggered (non-simultaneous)
  job costs, `K` in `{3..6}`, and the plan's own 1.2D acks-that-time-out
  variant (B10). **Not evaluated by either party: the 1ms network delay this
  threshold's own definition calls for** (this bench harness's own
  limitation — see "Limitations" above; loopback likely understates how much
  the coordinator's own per-round-trip stall would compound under a
  multi-sibling-starvation shape especially).

**Remedy: owner decision pending, not chosen here.** Three options are on
the table (see `docs/RISKS.md` risk 4 for the full write-up):

1. A per-consumer renewer process that also renews finished-but-unacked and
   pending-ack attempts, with acks staying serialized — the reviewer's
   minimum recommended pre-release fix, and the owner's earlier pre-chosen
   first remedy for this risk.
2. Moving the acknowledgement itself into the attempt process — the
   already-deferred T3 follow-up ("Ack from the attempt process" above) and
   the fuller fix.
3. Documenting this limit and tightening validation to the safe envelope
   `L ≥ 1.5 · C · D` derived above.

**The reviewer recommends against releasing with documentation alone
(option 3 by itself).** That recommendation is recorded here as the
reviewer's own position; which remedy ships, and when, remains an open
owner decision — see `docs/RELEASE-READINESS.md`.

## Per-attempt storage decision (T1/T2/T3 combined)

- **T1**: not triggered. Healthy renewal lag under real defaults has wide
  margin (worst case measured: 40.66ms against a 5000ms bound) up to
  `C=50` (weakly exercised per B8 above, but the margin is wide enough that
  this is not expected to change).
- **T2**: **TRIGGERED** — corrected verdict (see "L6T2" above). The
  committed `K in {0,1,2,4,8}` sweep in this repository missed the effect at
  low `K` because of this scenario's own construction (B1, B2); an
  independent reviewer's reproduction found it directly at `K=5`/`K=6`
  (`D=2000`, `L=6D`, all non-stalled siblings quarantined), and derived a
  general safe-envelope requirement, `L ≥ 1.5 · C · D`, that the shipped
  `L ≥ 6D` validation rule does not meet for `C > 2`. The outcome is silent:
  a job whose handler actually succeeded ends up `uncertain` and needs an
  audited resolution, with a duplicate-effect risk if it is replayed.
  **Remedy is an open owner decision, not implemented here** — see
  `docs/RISKS.md` risk 4 and `docs/RELEASE-READINESS.md`.
- **T3**: triggered (see "T3 verdict" above) — the single coordinator, not
  the database, caps throughput at high per-consumer concurrency.

T2 and T3 are two independent findings pointing at the same underlying
design property: one coordinator process serializing claim, ack, and
renewal traffic. Moving acknowledgement handling off the shared coordinator
loop — the same "ack from the attempt process" remedy T3 already identifies
(`docs/RISKS.md` risk 19) — would also directly address the renewal/ack
starvation mechanism T2 now confirms, since a parallel-ack design has no
single serialized queue for concurrent slow acks to back up behind. T3's own
remedy remains deferred by the user's 2026-09-27 decision (a throughput
optimization, not a release blocker); **T2's remedy is a separate, still-open
decision** or that decision may resolve both at once — see the three options
recorded under "L6T2" above and `docs/RISKS.md` risk 4. Multi-node (M1-M7),
soak, the fault-proxy delay/partition modes, and `bench-smoke.sh` remain
unbuilt and are out of scope for this pass.

## Superseded evidence

The `af0de18` run (`bench/results/2026-09-26-af0de18/`) predates the
hardened measurement wired in by commits `100b4d7`..`25894a6` (DB-CPU
sampling, the statement-split rollup, the coordinator profiler, and the
`grind_a`/`grind_ctl` role split) and its own T3 reading (an unqualified
"19%" ratio with no DB-CPU measurement at all) is superseded by the
25894a6 numbers above. That directory has been removed — nothing in this
document or elsewhere in the repository references it any longer.

## Repaired benchmark composite — 2026-09-28

The repaired composite passed its full numeric, raw-artifact and provenance
audit. The retained CLI witness records session 6734 exiting 0; the report is
[composite-audit-v4.json](../bench/results/l7-drain-pair-20260928T091612Z/composite-audit-v4.json).
This is laptop evidence from frozen dirty trees on 8431b61, not a clean release
baseline. The owner-approved two-hour resilience run and independent audit also passed.

On 2026-09-28, the owner approved a fresh 7,200-second mixed-fault soak as the
acceptance target. The run in `resilience/results/repaired-7200s-8YvcJq/` passed:
actual session 7230 exited 0, followed by independent audit session 97027 exiting 0. The [final audit](../resilience/results/repaired-7200s-8YvcJq/soak-audit-v5.json)
records 7,202.060719 mixed-fault seconds after warm-up; its independent lower
bound is 7,202.058878 seconds. All 14 standalone cases and 266 mixed rounds
passed, with 30 node-kill, worker-kill, request-partition, reply-partition and
full-partition rounds each, and 29 lost-commit-reply, slow-ACK-delay,
connection-loss and database-restart rounds each. Existing healthy-job, receipt,
effect, fencing, audited replay and resource assertions were preserved.
The [paired M2/M6 comparison](../resilience/results/repaired-7200s-8YvcJq/paired-comparison.json)
also passed and records Grind's deliberate replay and pruning differences from
Oban. These remain local exploratory results from dirty, content-pinned inputs.

The audit verified 6,993 primary effects, 520 complete JSONL files and 43,417
records. Sampled database sessions peaked at 8 against a ceiling of 10; retained
primary database size peaked at 245,760 bytes. Both primary VMs returned to 104
processes, one deadline entry and zero sampled mailbox messages. The worker's
atom count grew by 1,596 under the documented linear allowance for fresh consumer
starts; indefinite consumer churn is not proven to use bounded atoms. Resource
samples follow drain/consumer stop and do not directly sample timers or the
forwarder mailbox. Independent cleanup found no owned processes and confirmed
removal of the disposable cluster; PostgreSQL completed an immediate shutdown,
which does not establish graceful BEAM shutdown.

Audit v5 uses pinned causal protocols and controller monotonic order to establish
fault overlap and authorized replay; independent VM wall timestamps are not a
shared clock. It retains all source, runtime, duration, outcome and resource
checks. The final audit SHA-256 is
`de653b1fd4a7fa8054d9a6bf9113ac65697dfc79e1d509f0302b0ff94dc25c1a`.
The paired comparison covers M2 and M6 only: Grind waits for audited replay and
prunes without a leader; Oban's Lifeline rescues automatically and its Peer
transfers leadership. It does not establish equivalence for every fault.

The earlier 86,400-second attempt in
`resilience/results/repaired-86400s-o23a23/` remains failed. A retained power log
confirms a 275-second host software sleep spanning a healthy job's lease expiry;
the job became `uncertain` with one effect, no receipt and no replay. This is
consistent with fencing after suspension, not a passed soak. No time from that
attempt counts toward the fresh run. Day-long endurance remains unverified.

### Scope and provenance

The accepted scope combines the complete baseline and delayed L3/T2 subsets in
`bench/results/exploratory-matrix-LZVevz/` with the fresh zero/5ms L7 pair in
`bench/results/l7-drain-pair-20260928T091612Z/`. Main measured profiles retain three
repeats after discarded warm-up. The original wrapper exited 1 when delayed L7
exceeded its 60,000ms drain budget. That failure and its artifacts remain
retained, and the failed delayed arm remains failed. The fresh pair supplies the
current matched L7 comparison. The original zero-delay L7 rows remain validated
baseline context; unaffected profiles retain their original evidence.

The old source digest is
`4b0c3e229246b329a45ab4b52353954d25d577e72833b3f0fdef8013c0df4cf9`;
the fresh pair uses
`c48eda5d5034c5d62979463f3f030316b9f7af146650db0823c116831139dddc`.
Exactly ten reviewed benchmark files changed for the drain repair. All archived
production source/migrations, root and benchmark manifest/toolchain inputs, and
Sinal source/manifests are byte-identical. Source archives, original/resume/pair
drivers, logs and parent-captured terminal results are hash-linked in the
[composite manifest](../bench/results/l7-drain-pair-20260928T091612Z/composite-manifest.json).
Neither snapshot is relabeled clean.

The machine has 12 logical CPUs and runs Darwin/arm64, Gleam 1.18.1,
OTP 28.5.0.6/ERTS 16.4.0.6 and PostgreSQL 16.15. Injected delay is 5ms per direction
per received TCP chunk, not 5ms round-trip latency. The fresh pair records an
explicit 600,000ms soft drain budget in both arms and 11 sampler/diagnostic pairs
per arm. They cover 9000/33000/46000-job main shapes at three repeats and
9000/33000-job diagnostic shapes. All 306,000 retained jobs per arm have complete
state, receipt, handler and observed durable-completion accounting. The budget
neither cancels SQL nor substitutes drain duration for throughput’s durable
timestamp denominator (`bench/src/grind_bench/load/drain.gleam:68`,
`bench/src/grind_bench/load/report.gleam:18`).

### Subsequent evidence-formatting incident

On 2026-09-28, whole-repository formatting rewrote 17 retained benchmark JSON
files. The parent preserved every formatted variant and reconstruction candidate
under `bench/results/l7-drain-pair-20260928T091612Z/orchestration/formatting-incident-20260928/`.
Only nine byte sequences with preexisting hash pins were restored; all nine match
those pins. The eight unpinned files remain formatted, with equivalent candidate
bytes retained but not applied. The complete incident retention and archived
v3/v4 package hashes were independently verified. No pin, auditor or acceptance
criterion changed.

The unchanged composite v4 auditor completed a fresh post-incident re-audit:
actual session 66728 exited 0, with status `composite_accepted`, no issues and
empty stderr. Its report is semantically identical to the earlier accepted
report. Copies of `composite-reaudit-v4.json`, `composite-reaudit-v4.stderr.log` and
`composite-reaudit-v4-terminal.json`, with their source paths and hashes in
`post-format-reaudit-retention.json`, are retained in the incident directory.
`parent-restoration.json` preserves the original restoration action; neither it
nor the earlier audit result was rewritten.

Permanent formatter exclusions for `bench/results/**`, `oracle/results/**` and
`resilience/results/**` are implemented in `flake.nix`. All 11,177 retained evidence files were
byte-identical after formatting the ten reviewed documentation/configuration files. The benchmark re-audit and the separately accepted
soak audit retain their distinct scopes.

### Repaired harness and liveness evidence

The final gate in `bench/results/repaired-gate-garghm/` passed 38 tests, the
1,000-job audited smoke and L2/L3/L5/L7/profile activation checks. The deliberate
1ms timeout in `bench/results/drain-timeout-negative-2w6b1o/` failed as expected,
retaining three sampler records, acknowledged observer shutdown and final
completion/state counts. Its 26ms observed drain interval demonstrates the
budget is soft. The original checker’s stale-log-literal failure and corrected
`negative-check-v2.json` remain retained; this case is harness validation, not a
performance result.

B1–B10 repairs and source references are listed in
[RELEASE-READINESS.md](RELEASE-READINESS.md). The audit checks 240 main CSV rows:
192 original baseline rows (15 L1, 36 L2, 9 L3, 18 L4, 6 L5, 9 T1, 90 T2 and
9 L7), 9 delayed L3 rows, 21 delayed T2 rows and 18 fresh L7 rows. The 222 old-source
rows include the nine validated original zero-delay L7 rows as baseline context;
the fresh pair's 18 rows supply the current matched L7 comparison.

- L2 verifies actual 0/100k/1M row counts, 18,098 claim calls and 18,098 quarantine
  calls, with 36 retained untimed production-plan files. Call counts do not prove
  constant query cost.
- L3 admits and durably completes 18,750 measured jobs per delay arm at
  50/200/1000 arrivals/s. Every generator profile passes its capacity, lag and
  accounting checks. At 1000/s, median observed durable-ACK p99 is 253.647ms
  without injected delay and 25,644.262ms with delay: valid arrivals do not
  imply low queueing latency.
- L4 retains all 18 repeat files and 3,717 lock samples, at least 130 per repeat,
  with a live consumer and separate sampler connection. These are measurements
  of admission, CPU and waits; latency growth alone does not identify its cause.
- L5 records 12,000 handler and durable completions. Each enabled repeat prunes
  10,000 old rows inside its traffic window; all disabled repeats prune none.
  SQL `finished_at`/receipt timestamps remain distinct from the independent
  observer’s durable-completion timestamps (`bench/src/grind_bench/load/maintenance.gleam:191`).
- T1 observes all 192 attempts and 1,911 renewal samples. Worst renewal lag is
  15.371ms against L/6=5,000ms; minimum all-attempt headroom is 19,984.037ms.
  No negative sample or T1 trigger occurs.
- T2 retains 1,830 classified outcomes across 111 measured rows. All 1,254
  healthy siblings succeed with one valid receipt; none is quarantined or has
  negative sampled headroom. Minimum healthy headroom is 10,638.835ms and every
  row exceeds its own L/10 threshold. All 576 fault targets activate: 288 succeed
  and 288 become uncertain without receipts. The 90 measured fault rows each observe
  renewal during slow ACKs, totaling 1,734 such renewals. D=4s, L=16/24/30s,
  K 0/3–8 and 0.8D/1.2D delays are represented, including selected C50/pool10
  resource stress and a seven-profile 5ms-delay subset.

The legacy `t2_triggered` field is true in 45 rows because it includes quarantine
of selected fault targets (`bench/src/grind_bench/load/maintenance.gleam:464`). It is not the
healthy-sibling verdict. Current acceptance rejects healthy headroom below L/10
or healthy quarantine (`bench/src/grind_bench/load/maintenance.gleam:561`); all measured rows pass. These
results test specific bounded-delay configurations, not unconditional liveness
through arbitrary outages.

### Three adjudicated L2 maintenance incidents

The original frozen auditor rejected six lines in the whole PostgreSQL log:
three lock waits and three associated autovacuum cancellations. The reviewed v4
adjudication retains that rejection and verifies the exact pinned incidents,
statements, waiter/blocker identities, relation/schema, later acquisition and
source phase:

| Profile                          | Phase                                       | Wait / acquisition time |
| -------------------------------- | ------------------------------------------- | ----------------------- |
| C1, 250ms poll, 1M rows, warm-up | `DROP SCHEMA` after emitted result          | 100.604 / 100.685ms     |
| C8, 50ms poll, 1M rows, repeat 2 | `DROP SCHEMA` after emitted result          | 102.070 / 102.203ms     |
| C8, 250ms poll, 1M rows, warm-up | `ANALYZE` before checkpoint and measurement | 101.043 / 101.111ms     |

The schema/plan/row linkage and timestamps place these waits outside the measured
polling/statistics windows (`bench/src/grind_bench/load/open_loop.gleam:146`,
`:163`, `:195`; `bench/src/grind_bench/load/report.gleam:303`). Earlier autovacuum
CPU/cache effects are unquantified; database CPU remains aggregate PostgreSQL
CPU. This is exact phase-specific adjudication, not a general autovacuum
exception. Every other numeric, raw and provenance check remains unchanged;
unclassified errors, waits and deadlocks remain invalid. The adjudication
SHA is `9b187b1f98f887b49ad80aa34d756757c8572fa43b9aa5845ba45d586edc6e15`.

### T3 remains triggered

These are within-pair medians over three measured repeats, with 50 total worker
slots and a main pool of 50 connections. One reserved renewal connection per
consumer makes total Grind connections 51/55/60; total connection budgets are
therefore different. Handler cost is 1ms. The separate harness connections are
not included in those Grind totals.

| Transport delay     | Shape | Jobs/s, median | Total Grind connections |
| ------------------- | ----- | -------------: | ----------------------: |
| 0ms                 | 1×C50 |        1651.07 |                      51 |
| 0ms                 | 5×C10 |        3872.33 |                      55 |
| 0ms                 | 10×C5 |        4329.82 |                      60 |
| 5ms/direction/chunk | 1×C50 |          41.74 |                      51 |
| 5ms/direction/chunk | 5×C10 |         207.81 |                      55 |
| 5ms/direction/chunk | 10×C5 |         414.29 |                      60 |

The 1×C50/5×C10 ratios are 42.64% without delay and 20.09% with delay, both below
T3’s 70% threshold. At 1×C50, median PostgreSQL CPU consumes 14.78% and 3.44% of
the 12-logical-CPU machine respectively. Diagnostic 1×C50 profiles attribute 69.27%
and 98.88% of samples to `prim_inet:recv0/3` respectively. The repaired runtime
still has a coordinator throughput limitation. These are relative topology
measurements; they are not a controlled speedup over the historical 32% result
or hardware-independent capacity claims. Batch claiming remains deferred.
