# Benchmarks

This unpublished Gleam project measures Grind against disposable PostgreSQL
clusters. Use it to check workload correctness, renewal safety, latency and
throughput on the machine where you intend to run Grind. Qualification requires
fresh reviewed results for the chosen source and dependencies. See [qualification evidence](../docs/evidence/qualification.md) for that decision.

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

Bookkeeping queries use harness_db.execute rather than Grind. The completion observer has its own pool. Bookkeeping uses a sixty-second timeout and retries noproc for up to five seconds. A nonzero harness_pool_restart_retries count requires repeating the run before its measurements are used. [ADR-0009](../docs/adr/0009-separate-observations-from-qualification-evidence.md) records the failure that motivated this isolation.

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

## Historical measurements

These measurements belong to the dirty exploratory build tested on 2026-09-28.
[ADR-0009](../docs/adr/0009-separate-observations-from-qualification-evidence.md#bounded-measured-conclusions-and-provenance)
records the accepted findings and failed-run limits. The
[original benchmark summary](https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/bench/README.md#historical-results-2026-09-28)
preserves the tables and workload counts. They have not been rerun for this
checkout; the deleted raw outputs cannot be re-audited from these summaries.

L7 used one queue, fifty worker slots, a main pool of fifty, one-millisecond
handlers and a 600-second drain budget. Each value below is the median of three
measured repeats after warm-up. Additional harness pools are excluded from the
Grind connection counts.

| Consumers × slots | Jobs per repeat | Grind connections | Throughput, no injected delay (jobs/s) | Throughput, 5 ms delay (jobs/s) |
| ----------------- | --------------: | ----------------: | -------------------------------------: | ------------------------------: |
| 1 × 50            |           9,000 |                51 |                               1,651.07 |                           41.74 |
| 5 × 10            |          33,000 |                55 |                               3,872.33 |                          207.81 |
| 10 × 5            |          46,000 |                60 |                               4,329.82 |                          414.29 |

L3 used four C10 consumers at 1,000 arrivals/s. Both arrival generators passed
the recorded validity checks. The median of the three per-repeat durable-ACK
p99 measurements was 253.647 ms without injected delay and 25,644.262 ms with
delay. Durable completion includes the observer's nominal ten-millisecond
polling interval. The retained summary does not record L3's per-repeat job
count, arrival-window duration or handler cost.

The delay setting adds five milliseconds to each received TCP chunk in each
direction on Grind's database path. The shapes have different job and connection
counts, so these numbers do not establish a controlled runtime speedup. The
coordinator threshold T3 remained triggered. The earlier delayed L7 run failed
its sixty-second budget; the fresh matched pair does not erase that failure.

Recorded environment: Darwin 25.5.0 arm64, twelve logical CPUs, Gleam 1.18.1,
OTP 28.5.0.6 / ERTS 16.4.0.6, and same-host PostgreSQL 16.15 with
`synchronous_commit=local`. The retained summary does not identify the CPU model,
memory or storage hardware. The dirty Grind trees were based on `8431b61`, with
Sinal `c8868251a69ecf4fafdba7a7a8f1b419c71c1dfe`:

- Baseline and delayed L3/T2 source SHA-256: `4b0c3e229246b329a45ab4b52353954d25d577e72833b3f0fdef8013c0df4cf9`.
- Fresh matched L7 source SHA-256: `c48eda5d5034c5d62979463f3f030316b9f7af146650db0823c116831139dddc`.

These hashes cannot reconstruct the removed source snapshots. Use the L3 and L7
[run commands](#run) with new provenance to measure the chosen build.

## Outputs and release use

Each matrix reserves a fresh output directory under `bench/results/`; an existing
`GRIND_BENCH_RESULTS_DIR` is rejected. CSVs, raw samples, plans, logs and provenance
are generated working files. Failed runs preserve warm-up context, which can
include earlier successful warm-ups. Review failures before recording a summary.

Record results with the exact source/dependency versions, environment, shapes,
repeat counts, validity checks and limitations. Routine generated output is
ignored and disposable after that review; record concise accepted findings with source provenance in the ADR evidence appendix,
not source archives or run-directory histories in Git.

Release qualification needs fresh gates and relevant benchmark runs on fixed
source and dependencies. Set `GRIND_BENCH_RELEASE_EVIDENCE=1` to reject a dirty
Grind checkout; separately verify the Sinal revision and cleanliness. The matrix
records both inputs and rejects source-digest changes during a run. A clean run
is a prerequisite, not an automatic release verdict or a replacement for the
behavioural, fault and resilience checks in the release checklist.
