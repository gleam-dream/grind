# Separate best-effort observations from qualification evidence

<a id="adr-0009"></a>

- **Decision.** Publish commit-proven lifecycle observations and runtime diagnostics through a bounded Sinal forwarder. Keep scenario catalogs, source comparisons, executable fault/audit checks and compact historical evidence. Qualify each chosen source/dependency build independently.

- **Rationale.** Database callbacks must not execute subscribers. Typed event schemas enable safe consumers but drops, duplicates and local ordering prohibit using events as an effect ledger. Historical performance summaries cannot replace fresh reproducible evidence.

- **Alternatives.** Synchronous subscribers could stall storage or lease progression. Counting attempted proposals as committed outcomes would corrupt audit conclusions. Removing failed runs or comparing provisional harness output as a controlled speedup would fabricate evidence.

- **Evidence and history.** 75e50ae0ae95322e0986043b05858d53840cb625 added diagnostics. a1627b5d526cf795517cbc058cdfaf1ae954e5ba removed generated run artifacts at owner request on 2026-10-01 after summaries; deleted raw bytes cannot be re-audited from summaries. The approved 2026-09-28 soak target is two hours after warm-up, not twenty-four. Accepted historical summary: 7202.060719 seconds, 14 standalone cases and 266 mixed rounds. These numbers belong to that older dirty/content-pinned build, not the current package. bench/README.md, resilience/README.md and oracle/ORACLE-LEDGER.md retain evidence and commands.

- **Source revisions.** [75e50ae0](https://github.com/gleam-dream/grind/commit/75e50ae0ae95322e0986043b05858d53840cb625), [a1627b5d](https://github.com/gleam-dream/grind/commit/a1627b5d526cf795517cbc058cdfaf1ae954e5ba).

## Bounded measured conclusions and provenance

The following findings belong to the 2026-09-28 exploratory build, not a future candidate. The full [benchmark tables](https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/bench/README.md#historical-results-2026-09-28) and [resilience summary](https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/resilience/README.md#historical-result-2026-09-28) have immutable source references.

- Source was dirty exploratory trees based on 8431b61, with Sinal c8868251a69ecf4fafdba7a7a8f1b419c71c1dfe. Baseline/L3/T2 content hash: 4b0c3e229246b329a45ab4b52353954d25d577e72833b3f0fdef8013c0df4cf9. Fresh matched L7 hash: c48eda5d5034c5d62979463f3f030316b9f7af146650db0823c116831139dddc. Removed snapshots cannot be reconstructed from hashes alone.
- Environment was Darwin 25.5.0 arm64, twelve logical CPUs, Gleam 1.18.1, OTP 28.5.0.6/ERTS 16.4.0.6, PostgreSQL 16.15 on the same host and synchronous_commit=local. Main points used three measured repeats after warm-up.
- L7 used one queue, fifty worker slots, main pool fifty, one-millisecond handlers and a 600-second drain budget. One C50, five C10 and ten C5 used respectively 51/55/60 total Grind connections, with additional harness pools. Throughput medians were 1651.07/3872.33/4329.82 jobs/s without delay and 41.74/207.81/414.29 with five-millisecond per-direction/chunk delay. T3 remained triggered. These topology comparisons have unequal total connection budgets and cannot establish a runtime speedup.
- L3 at 1000 arrivals/s recorded durable-ACK p99 medians of 253.647ms without delay and 25644.262ms with delay, with valid generators. T1 covered 192 attempts in nine measured rows, worst renewal lag 15.371ms against 5000ms and minimum sampled headroom 19984.037ms; no trigger occurred.
- T2 covered 1830 classified jobs in 111 measured rows. All 1254 healthy siblings succeeded without quarantine; minimum headroom was 10638.835ms. All 576 targets activated, of which 288 succeeded and 288 became uncertain without receipts. This supports those fault configurations only.
- The accepted composite had 240 main rows. Three specific L2 autovacuum waits were adjudicated outside measurement, not a general exception for log errors. The original delayed L7 failed its sixty-second drain budget; the fresh matched pair did not erase that failure. Earlier L2–L6 harness-defective samples are not qualified comparisons. A one-millisecond drain limit separately checked failure diagnostics.
- The two-hour soak had 29–30 executions of each of nine faults, 7182 primary jobs, 11172 receipts and 6993 effects including warm-up. Peak sampled database sessions were eight and retained primary storage 245760 bytes. Worker atoms grew by 1596 across 532 starts, exactly three per start under the declared allowance. This does not prove bounded atoms under indefinite churn. Controller monotonic causal barriers avoided independent-VM wall-clock comparison.
- A preceding day-long attempt failed after a 275-second host sleep and contributed no accepted elapsed time. Day-long endurance, encrypted network-fault paths and poolers remain unverified. The rejected partition adapter discarded bytes and stranded pgo waiting for ReadyForQuery; current directional partitions retain bounded bytes and forward them in order after healing.

The harness bookkeeping pool once exhibited a deadline-message/restart noproc race under heavy parallel package gates. The current observer has a separate pool, bookkeeping uses a sixty-second timeout and bounded noproc recovery, and nonzero harness_pool_restart_retries invalidates measurement use. This rationale belongs here; the current run guide states the operational contract without retaining the failure diary.
