# Performance evidence

The benchmark definitions, commands, configuration and compact results table now
live in [bench/README.md](../bench/README.md). It is the single performance summary.
Independent-node and endurance results live in
[resilience/README.md](../resilience/README.md). Current qualification requirements
live in [RELEASE-READINESS.md](RELEASE-READINESS.md).

## Historical result limits

The 2026-09-28 benchmark composite combined dirty, content-pinned source trees
before the diagnostics commit. It covered a repeated zero-delay baseline,
delayed L3/T2 subsets and a fresh matched L7 pair. The original delayed L7 arm
failed its drain budget; a later pair used a matching larger budget in both
arms. That was a new comparison, not a conversion of the failed run into a pass.

The repaired T2 scenarios observed no healthy sibling quarantine. T3 remained
triggered: claim serialization limits throughput at high concurrency per
consumer. Neither result is an unconditional liveness or capacity guarantee.
The earlier 2026-09-26/27 measurements include provisional harness results and
must not be compared as a controlled before/after speedup. Their detailed tables
and B1–B10 investigation remain in Git history before this documentation cleanup.

On 2026-10-01, the owner requested removal of generated results. Raw CSVs, traces,
logs, source/runtime archives and one-off audit outputs were removed after the
compact summaries were checked. Summaries preserve historical findings; the
deleted raw runs can no longer be independently re-audited. Existing committed
CSV versions remain recoverable from Git history. No new benchmark was run by
this cleanup, and no current release qualification is claimed.

For a new run, record source/dependencies, hardware/toolchain, exact workload and
pool/concurrency shape, delay semantics, measured repeats and validity verdicts
before deleting outputs. Preserve failed-run classifications in the summary.
