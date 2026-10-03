# Benchmarks

This unpublished Gleam project measures Grind against disposable PostgreSQL
clusters. Use it to check workload correctness, renewal safety, latency and
throughput on the machine where you intend to run Grind. Historical results
below describe specific development snapshots; they do not qualify today's code
for release. See [release readiness](../docs/RELEASE-READINESS.md) for that decision.

## Run

Run from the repository root with Nix installed and the Sinal path dependency at
`../sinal`. The development shell supplies Gleam, Erlang, PostgreSQL and Python.
Run database gates and benchmarks serially in a checkout, with the host awake
and no competing workload.

```sh
# Harness tests, activation checks and an audited 1,000-job drain.
nix develop --command bash scripts/bench-smoke.sh

# Full representative matrix, including four optional resource-stress points.
GRIND_BENCH_T2_STRESS=1 GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 \
  nix develop --command bash scripts/bench-matrix.sh all

# Matched coordinator comparison: identical drain budgets, separate runs.
GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 \
  nix develop --command bash scripts/bench-matrix.sh l7
GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 GRIND_BENCH_NETWORK_DELAY_MS=5 \
  nix develop --command bash scripts/bench-matrix.sh l7
```

The matrix also accepts `l1`, `l2`, `l3`, `l4`, `l5`, `l6`, `l6t1` and `l6t2`.
Each main point discards one warm-up and measures at least three repeats;
`GRIND_BENCH_REPEATS` can increase that count. L7 also runs two diagnostic
coordinator profiles. The full matrix takes hours. A subset establishes only
its selected coverage. Plain `gleam test` without database configuration skips
DB cases and does not replace the harness gate.

## Scenarios

`C` is concurrency per consumer, `D` is the storage-call deadline, `L` is the
lease duration, and `K` is the number of deliberately slow ACK targets.

| Suite   | Question and representative workload                                                                                                                      |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| L1      | Drain throughput across consumer, concurrency, queue-count and handler-cost shapes.                                                                       |
| L2      | Empty polling cost with 1/8 consumers, 50/250ms polling and 0/100k/1M retained rows. Reports call counts, time per call and untimed production SQL plans. |
| L3      | Open-loop latency at 50/200/1000 arrivals per second, with four C10 consumers.                                                                            |
| L4      | Unique admission under hot/cold keys and 4/16/64 submitters, with a live C10 consumer and a separate lock sampler.                                        |
| L5      | Pruning on/off during traffic, with 10,000 pre-aged terminal rows in each arm.                                                                            |
| L6 / T1 | Healthy renewal lag at C4/C10/C50; staggered handlers run for at least three leases.                                                                      |
| L6 / T2 | Healthy siblings during slow ACKs: D=4s, L=16/24/30s, ACK delay=0.8D/1.2D. The default 26 profiles cover K=0/3–8 at L=16s and K=0/5/8 at L=24/30s.        |
| L7 / T3 | Coordinator throughput at 1×C50, 5×C10 and 10×C5, with 1ms handlers; mailbox and function sampling explain the shape.                                     |

`GRIND_BENCH_T2_STRESS=1` adds four C50 profiles with main pools of 50 and 10.
For a deliberate subset, set `GRIND_BENCH_T2_PROFILES` to a text file containing
`K D L ACK_delay concurrency main_pool` per row, in milliseconds where applicable.
Blank lines and `#` comments are allowed. The matrix copies the profile file into
its output. Point definitions live in [bench-matrix.sh](../scripts/bench-matrix.sh);
the individual-run CLI is documented in [load.gleam](src/grind_bench/load.gleam).

## Harness pools

The harness's own bookkeeping queries (ledger writes, drain polling, the
completion observer and the audits) go through
[`harness_db.execute`](src/grind_bench/harness_db.gleam), not through
Grind. The L3 open-loop scenario once crashed with `noproc` on its drain
pool while nine package gates ran in parallel, and passed when run alone.
The drain pool then had one connection, shared by the 10 ms completion
observer and the drain poller, and every query ran with pog's 5 s default
timeout. pgo arms that deadline at checkout and cancels it asynchronously
at checkin; under heavy machine load a query can run close to it, and a
deadline message that arrives after its connection was checked in or
replaced can crash pgo's pool process. While the pool restarts its name is
unregistered, so a concurrent query exits with `noproc`. The race is in the
harness's plain pog usage under load, not in Grind's storage calls, which
check out through Grind's bounded FFI.

The observer now has its own pool, bookkeeping queries run with a 60 s
timeout so no deadline fires near a slow query's completion, and a query
that exits with `noproc` is retried for up to 5 s. The observer's stop line
reports `harness_pool_restart_retries`; a nonzero count means a pool
restarted during the run and the run should be repeated before its numbers
are used as evidence.

## Comparability and validity

- Match job count, handler cost, arrival pattern, concurrency, queue count,
  storage deadlines, leases, database settings and transport delay. Record the
  machine, operating system, storage and toolchain; results are not portable
  capacity guarantees. Keep observer overhead equal when comparing Grind with Oban.
- Count every connection. Each consumer reserves one renewal connection in
  addition to the main pool; the ledger uses `max(total_concurrency, 4)`
  connections (L4 uses 8), and the observer uses one. Equal worker concurrency
  or equal main pools alone do not establish equal resource budgets.
- `GRIND_BENCH_NETWORK_DELAY_MS` delays each received TCP chunk in each direction
  on Grind's database path; ledger and observer connections remain direct.
  Handler cost is separate. A 5ms setting is neither 5ms round-trip latency nor
  a bandwidth, loss or partition simulation.
- Throughput spans the first handler start to the last independently observed
  durable receipt. The observer polls nominally every 10ms, so completion time
  includes observation delay and contention. Handler completion and SQL
  `finished_at`/receipt timestamps are separate metrics, not exact commit times.
- L3 keeps absolute arrival slots and defaults to 256 outstanding admissions
  (`GRIND_BENCH_MAX_INFLIGHT`). Any capacity-exhausted slot or maximum dispatch
  lag above `max(20ms, 2% of the arrival window)` invalidates the generator.
  Failed, unfinished and undispatched work stays in the accounting denominator.
- Healthy runs must reconcile submissions, effects, final states, outputs,
  receipts and observations. Missing measurements, observer failure, an
  unacknowledged observer stop, or unexplained database errors invalidate evidence.
  The scripts configure PostgreSQL logging and `auto_explain` for these checks.
- L2 verifies actual row counts and captures `ANALYZE/BUFFERS/TIMING` plans outside
  the timed interval. L4 requires at least 30 lock samples from its separate pool.
  L5 must observe pruning inside the traffic window and account for every fresh
  handler and durable completion; idle pruner polls do not prove pruning cost.
- T1's lag threshold is `L/6`. Require `t1_triggered=false`, strictly positive
  all-attempt minimum headroom and no negative samples; the runner records the
  lag trigger without failing on it. T2 requires every slow target to activate
  and a slow ACK to overlap a running sibling. Check the recorded renewals during
  those stalls before claiming renewal-overlap coverage.
- T2 requires every healthy sibling to succeed with one matching receipt, no
  quarantine, and sampled headroom at least `L/10`. Each fault target must succeed
  consistently or finish uncertain without an ACK receipt; incomplete jobs fail.
  The legacy `t2_triggered` flag includes fault-target quarantine and is not the
  healthy-sibling verdict. Never use `GRIND_BENCH_ALLOW_T2_FAILURE=1` for acceptance.
- T3 is triggered when 1×C50 throughput is below 70% of 5×C10 while PostgreSQL
  uses less than 50% of the whole machine's CPU capacity. Report both the ratio
  and CPU denominator; the CSV's per-core percentage needs that normalization.

L7 and diagnostic profiles accept positive `GRIND_BENCH_DRAIN_TIMEOUT_MS`
(default 60000). Use the same explicit value for matched arms; 600000 accommodates
the recorded 5ms-delay shapes. This is a soft polling budget, not SQL cancellation
or the throughput denominator. It does not change L3's drain or validity rules.
Each `.jsonl.drain.json` records the actual interval, final completion counts and
sampler coverage, including first/last samples, count and maximum gap. Timeout
still fails; final diagnostics do not start another drain or turn failure into success.

## Historical results: 2026-09-28

These are laptop measurements from dirty exploratory trees based on `8431b61`,
with Sinal `c8868251a69ecf4fafdba7a7a8f1b419c71c1dfe`. The source SHA-256 values identify
historical inputs; the removed generated snapshots cannot be reconstructed from
these hashes alone:

- Baseline and delayed L3/T2: `4b0c3e229246b329a45ab4b52353954d25d577e72833b3f0fdef8013c0df4cf9`.
- Fresh matched L7 pair: `c48eda5d5034c5d62979463f3f030316b9f7af146650db0823c116831139dddc`.

Environment: Darwin 25.5.0 arm64, 12 logical CPUs, Gleam 1.18.1,
OTP 28.5.0.6 / ERTS 16.4.0.6, PostgreSQL 16.15 on the same host, with
`synchronous_commit=local`. Each main point had three measured repeats after
warm-up. Operating-system and storage differences limit cross-host comparisons.

L7 medians used one queue, 50 total worker slots, main pool 50, 1ms handlers and
a 600-second drain budget. The job counts were 9,000 / 33,000 / 46,000 respectively.
Harness connections are additional to the Grind totals below.

| Shape | Total Grind connections | Jobs/s, no injected delay | Jobs/s, 5ms per direction/chunk |
| ----- | ----------------------: | ------------------------: | ------------------------------: |
| 1×C50 |                      51 |                  1,651.07 |                           41.74 |
| 5×C10 |                      55 |                  3,872.33 |                          207.81 |
| 10×C5 |                      60 |                  4,329.82 |                          414.29 |

The 1×C50 / 5×C10 ratios were 42.64% and 20.09%; median PostgreSQL CPU at 1×C50
was 14.78% and 3.44% of the machine respectively. T3 remained triggered. These
are topology comparisons with different total connection budgets, not a
controlled speedup over an older runtime. Batch claiming remains deferred.

| Other accepted scope  | Recorded result                                                                                                                                                                                        |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| L3 at 1000 arrivals/s | Median observed durable-ACK p99: 253.647ms without injected delay; 25,644.262ms with 5ms delay. Both generators passed validity checks.                                                                |
| T1, 9 measured rows   | 192 attempts; worst renewal lag 15.371ms against a 5,000ms threshold; minimum sampled headroom 19,984.037ms. No T1 trigger.                                                                            |
| T2, 111 measured rows | 1,830 classified jobs; all 1,254 healthy siblings succeeded, with minimum headroom 10,638.835ms and no quarantine. All 576 targets activated: 288 succeeded and 288 became uncertain without receipts. |

The accepted composite contained 240 main rows: the original baseline, delayed
L3/T2 subsets and the fresh L7 pair. Its numeric, raw and provenance audit passed
at the time. Three exact L2 autovacuum-related waits were adjudicated as setup or
cleanup outside measurement; this did not create a general log-error exception.
The original delayed L7 run failed its 60-second drain budget and remains a failed
historical attempt; the fresh pair supplied the matched comparison. Earlier
L2–L6 measurements had harness defects and are not release evidence. The repaired
harness gate passed 38 tests and its audited smoke; a deliberate 1ms drain timeout
also verified failure diagnostics. None of these historical passes qualifies
subsequent source changes.

## Outputs and release use

Each matrix reserves a fresh output directory under `bench/results/`; an existing
`GRIND_BENCH_RESULTS_DIR` is rejected. CSVs, raw samples, plans, logs and provenance
are generated working files. Failed runs preserve warm-up context, which can
include earlier successful warm-ups. Review failures before recording a summary.

Record results with the exact source/dependency versions, environment, shapes,
repeat counts, validity checks and limitations. Routine generated output is
ignored and disposable after that review; keep concise summaries in this file,
not source archives or run-directory histories in Git. The historical raw files
summarized above were removed during repository cleanup and are no longer
available here for re-audit.

Release qualification needs fresh gates and relevant benchmark runs on fixed
source and dependencies. Set `GRIND_BENCH_RELEASE_EVIDENCE=1` to reject a dirty
Grind checkout; separately verify the Sinal revision and cleanliness. The matrix
records both inputs and rejects source-digest changes during a run. A clean run
is a prerequisite, not an automatic release verdict or a replacement for the
behavioural, fault and resilience checks in the release checklist.
