# Benchmarks

This document states what the benchmark project measures, how to run it, when a result is valid, and which retained results exist. It measures Grind against disposable PostgreSQL clusters on the machine where it runs. A result describes its source, dependencies, hardware and workload only; it is not a capacity guarantee.

- **Pins and comparisons:** [ORACLE.md](ORACLE.md) covers correctness against the pinned Oban reference. Throughput and latency belong here, and correctness belongs there. When a benchmark compares Grind with Oban, both arms follow the matching rules below.
- **Method and qualification rules:** "Benchmark method" and "Qualification evidence" in [the design](design/design.typ), with the decisions in [ADR-0009](adr/0009-separate-observations-from-qualification-evidence.md), [ADR-0018](adr/0018-run-fast-and-full-validation-profiles-and-keep-endurance-weekly.md) and [ADR-0019](adr/0019-keep-usage-as-compiled-examples-and-reference-docs-as-retained-files.md).
- **Commands for every gate:** the [agent instructions](../AGENTS.md). The harness lives in [bench/](../bench/README.md).

## Run

Run from the repository root inside `nix develop`. Run gates and benchmarks serially, with the host awake and no competing workload.

```sh
# Harness tests, activation checks and an audited 1,000-job drain.
nix develop --command bash scripts/bench-smoke.sh

# Full matrix, including four optional resource-stress points. It takes hours.
GRIND_BENCH_T2_STRESS=1 GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 \
  nix develop --command bash scripts/bench-matrix.sh all

# Matched coordinator comparison with identical drain budgets, separate runs.
GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 \
  nix develop --command bash scripts/bench-matrix.sh l7
GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 GRIND_BENCH_NETWORK_DELAY_MS=5 \
  nix develop --command bash scripts/bench-matrix.sh l7
```

The matrix also accepts `l1`, `l2`, `l3`, `l4`, `l5`, `l6`, `l6t1` and `l6t2`. A subset establishes only its selected coverage, and plain `gleam test` without a database skips the database cases and does not replace the harness gate.

| Variable                         | Effect                                                                                                                                                                   |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `GRIND_BENCH_REPEATS`            | Raises the measured repeat count above three. Each main point discards one warm-up.                                                                                      |
| `GRIND_BENCH_T2_STRESS=1`        | Adds four C50 profiles with main pools of 50 and 10.                                                                                                                     |
| `GRIND_BENCH_T2_PROFILES`        | Selects a text file with one `K D L ACK_delay concurrency main_pool` row per line. Blank lines and `#` comments are allowed. The matrix copies the file into its output. |
| `GRIND_BENCH_DRAIN_TIMEOUT_MS`   | The soft drain budget for L7 and the diagnostic profiles, default 60000. Matched arms use one explicit value.                                                            |
| `GRIND_BENCH_NETWORK_DELAY_MS`   | Delays each received TCP chunk, in each direction, on Grind's database path.                                                                                             |
| `GRIND_BENCH_MAX_INFLIGHT`       | Bounds L3 outstanding admissions, default 256.                                                                                                                           |
| `GRIND_BENCH_RELEASE_EVIDENCE=1` | Rejects a dirty Grind checkout. The Sinal revision is verified separately.                                                                                               |

Point definitions live in [bench-matrix.sh](../scripts/bench-matrix.sh) and the single-run CLI in [load.gleam](../bench/src/grind_bench/load.gleam). Each matrix writes a fresh directory under `bench/results/`, which is ignored, and an existing `GRIND_BENCH_RESULTS_DIR` is rejected.

## Scenarios

`C` is concurrency per consumer, `D` the storage-call deadline, `L` the lease duration and `K` the number of deliberately slow acknowledgement targets.

| Suite   | Question and representative workload                                                                                                                                                                              |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| L1      | Drain throughput across consumer, concurrency, queue-count and handler-cost shapes.                                                                                                                               |
| L2      | Empty polling cost with 1 or 8 consumers, 50 or 250 ms polling and 0, 100 thousand or 1 million retained rows. Reports call counts, time per call and untimed production plans.                                   |
| L3      | Open-loop latency at 50, 200 and 1000 arrivals per second, with four C10 consumers.                                                                                                                               |
| L4      | Unique admission under hot and cold keys and 4, 16 or 64 submitters, with a live C10 consumer and a separate lock sampler.                                                                                        |
| L5      | Pruning on and off during traffic, with 10,000 pre-aged terminal rows in each arm.                                                                                                                                |
| L6 / T1 | Healthy renewal lag at C4, C10 and C50; staggered handlers run for at least three leases.                                                                                                                         |
| L6 / T2 | Healthy siblings during slow acknowledgements: D = 4 s, L = 16, 24 or 30 s, acknowledgement delay 0.8D or 1.2D. The default 26 profiles cover K = 0 and 3 to 8 at L = 16 s and K = 0, 5 and 8 at L = 24 and 30 s. |
| L7 / T3 | Coordinator throughput at 1×C50, 5×C10 and 10×C5, with 1 ms handlers; mailbox and function sampling explain the shape.                                                                                            |

## Harness pools

Bookkeeping queries use the harness database path and not Grind, and the completion observer has its own pool. Bookkeeping uses a sixty-second timeout and retries `noproc` for up to five seconds. A nonzero `harness_pool_restart_retries` count requires repeating the run before its measurements are used. [ADR-0009](adr/0009-separate-observations-from-qualification-evidence.md) records the failure that motivated this isolation.

## Comparability and validity

- **Matching.** Match job count, handler cost, arrival pattern, concurrency, queue count, storage deadlines, leases, database settings and transport delay. Record the machine, operating system, storage and toolchain. Keep observer overhead equal when comparing Grind with Oban.
- **Connections.** Count every connection. Each consumer reserves one renewal connection in addition to the main pool, the ledger uses the larger of total concurrency and four connections (L4 uses eight), and the observer uses one. Equal worker concurrency or equal main pools alone do not establish equal resource budgets.
- **Network delay.** The setting is neither a round-trip latency nor a bandwidth, loss or partition simulation. Ledger and observer connections stay direct, and handler cost is separate.
- **Throughput.** It spans the first handler start to the last independently observed durable receipt. The observer polls nominally every 10 ms, so completion time includes observation delay and contention. Handler completion and SQL `finished_at` or receipt timestamps are separate metrics, not exact commit times.
- **L3.** It keeps absolute arrival slots. A capacity-exhausted slot, or a maximum dispatch lag above the larger of 20 ms and 2% of the arrival window, invalidates the generator. Failed, unfinished and undispatched work stays in the accounting denominator.
- **Healthy runs.** They reconcile submissions, effects, final states, outputs, receipts and observations. Missing measurements, observer failure, an unacknowledged observer stop or an unexplained database error invalidate the evidence. The scripts configure PostgreSQL logging and `auto_explain` for these checks.
- **L2, L4 and L5.** L2 verifies actual row counts and captures `ANALYZE/BUFFERS/TIMING` plans outside the timed interval. L4 requires at least 30 lock samples from its separate pool. L5 must observe pruning inside the traffic window and account for every fresh handler and durable completion, because idle pruner polls do not prove pruning cost.
- **T1.** The lag threshold is `L/6`. Require `t1_triggered=false`, strictly positive all-attempt minimum headroom and no negative samples. The runner records the lag trigger without failing on it.
- **T2.** Every slow target must activate and a slow acknowledgement must overlap a running sibling. Every healthy sibling must succeed with one matching receipt, no quarantine and sampled headroom of at least `L/10`. Each fault target must succeed consistently or finish uncertain without an acknowledgement receipt, and an incomplete job fails. The legacy `t2_triggered` flag includes fault-target quarantine and is not the healthy-sibling verdict. Never use `GRIND_BENCH_ALLOW_T2_FAILURE=1` for acceptance.
- **T3.** It triggers when 1×C50 throughput is below 70% of 5×C10 while PostgreSQL uses less than 50% of the whole machine's CPU capacity. A report states the ratio and the CPU denominator, because the CSV's per-core percentage needs that normalization. A crossing is a deferred optimization signal, not a correctness failure.
- **Drain budget.** It is a soft polling budget, neither SQL cancellation nor the throughput denominator, and it does not change L3's drain or validity rules. Each `.jsonl.drain.json` records the actual interval, final completion counts and sampler coverage. A timeout still fails.

## Outputs and release use

CSVs, raw samples, plans, logs and provenance are generated working files, and failed runs preserve warm-up context that can include earlier successful warm-ups. Review failures before recording a summary. Record results with the exact source and dependency versions, environment, shapes, repeat counts, validity checks and limitations. Routine generated output is ignored and disposable after review, and concise accepted findings go into the ADR evidence appendix with source provenance, not source archives or run-directory histories.

Release qualification needs fresh gates and the relevant benchmark runs on fixed source and dependencies. A clean run is a prerequisite and not a release verdict, and it does not replace the behavioral, fault and resilience checks described in the [resilience guide](../resilience/README.md) and in [ORACLE.md](ORACLE.md).

## Retained results

These results belong to the dirty exploratory build tested on **2026-09-28**. They are observational: they describe that build on that machine and were not rerun for the current checkout. The raw outputs were removed, so the summaries cannot be re-audited. [ADR-0009](adr/0009-separate-observations-from-qualification-evidence.md#bounded-measured-conclusions-and-provenance) records the accepted findings and the failed-run limits, and the [original summary](https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/bench/README.md#historical-results-2026-09-28) preserves the full tables.

L7 used one queue, fifty worker slots, a main pool of fifty, 1 ms handlers and a 600-second drain budget. Each value is the median of three measured repeats after warm-up. Additional harness pools are excluded from the Grind connection counts.

| Consumers × slots | Jobs per repeat | Grind connections | Throughput, no injected delay (jobs/s) | Throughput, 5 ms delay (jobs/s) |
| ----------------- | --------------: | ----------------: | -------------------------------------: | ------------------------------: |
| 1 × 50            |           9,000 |                51 |                               1,651.07 |                           41.74 |
| 5 × 10            |          33,000 |                55 |                               3,872.33 |                          207.81 |
| 10 × 5            |          46,000 |                60 |                               4,329.82 |                          414.29 |

| L3 at 1,000 arrivals/s, four C10 consumers | Durable-ACK p99, median of three repeats (ms) |
| ------------------------------------------ | --------------------------------------------: |
| No injected delay                          |                                       253.647 |
| 5 ms delay                                 |                                    25,644.262 |

Both arrival generators passed the recorded validity checks. Durable completion includes the observer's nominal 10 ms polling interval, and the retained summary does not record L3's per-repeat job count, arrival-window duration or handler cost.

**Environment.** Darwin 25.5.0 arm64, twelve logical CPUs, Gleam 1.18.1, OTP 28.5.0.6 / ERTS 16.4.0.6 and same-host PostgreSQL 16.15 with `synchronous_commit=local`. The summary does not identify the CPU model, memory or storage hardware. The dirty Grind trees were based on `8431b61`, with Sinal `c8868251a69ecf4fafdba7a7a8f1b419c71c1dfe`. The baseline and delayed L3/T2 source SHA-256 was `4b0c3e229246b329a45ab4b52353954d25d577e72833b3f0fdef8013c0df4cf9`, and the fresh matched L7 source SHA-256 was `c48eda5d5034c5d62979463f3f030316b9f7af146650db0823c116831139dddc`. These hashes cannot reconstruct the removed source snapshots.

**Limits of the claim.**

- The shapes have different job and connection counts, so the numbers establish no controlled runtime speedup.
- The delayed L7 run failed its sixty-second budget earlier, and the fresh matched pair does not erase that failure. T3 remained triggered.
- A laptop measurement establishes no encrypted-network, connection-pooler or arbitrary-outage behavior.
- To measure a chosen build, run the L3 and L7 commands above with new provenance.
