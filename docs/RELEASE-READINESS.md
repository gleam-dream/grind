# Release readiness

Status checked on 2026-10-01 against Grind `75e50ae`. No release candidate is
qualified yet. This checklist concerns the first experimental release; the
before-1.0 API work below is separate.

## What remains

| Requirement                        | Current state                                                                                                                                 | Acceptance                                                                                                                                                                                             |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Reproducible dependencies          | Grind uses `sinal = { path = "../sinal" }`. Local Sinal and the CI pin are `5aef827` (the wave 2 API).                                        | Choose the Sinal release, use its published version range, refresh root/consumer/benchmark manifests and make local/CI resolution agree. Run Sinal's own suite on that version.                        |
| Qualification of the final source  | `75e50ae` has passing root, consumer, oracle and benchmark gates with the older Sinal revision. The full matrix and soak predate diagnostics. | Run the commands below on a clean candidate with fixed dependencies. Source or dependency changes invalidate the affected evidence.                                                                    |
| Independent result review          | The runners enforce fault, accounting and resource checks. Historical aggregate auditors were scripts specific to old runs.                   | Review the fresh raw results and provenance independently, or add a reusable post-run auditor. A zero exit status alone is not the complete review.                                                    |
| Package contents and documentation | License/repository metadata exists; publication review remains open.                                                                          | Review Oban-derived notices, version/changelog, generated API docs and the getting-started example. Export the Hex package and build an external consumer from its contents without sibling checkouts. |
| CI and release identity            | CI runs the PostgreSQL gate and Sinal tests against the pinned Sinal. Push CI targets `main`; the local branch is `master`.                   | Align dependency resolution and the intended release branch, obtain green CI, and record the qualified commit and environment.                                                                         |

Publishing dependencies or Grind, pushing and tagging require separate owner
instructions. Changing the release dependency is implementation work; this
cleanup does not select or publish a Sinal version. The benchmark, oracle and resilience
provenance readers currently hash `../sinal`; update them to identify the actual
resolved dependency when switching to Hex. A clean Grind tree alone does not
prove that Sinal is clean or that the recorded source was the one built.

## Qualification commands

Run serially in a fresh checkout. The PostgreSQL gate cleans build artifacts.
Prevent host sleep for long runs; on macOS, prefix them with `caffeinate -i`.
The commands create disposable databases and local, ignored result directories.

```sh
nix flake check
nix develop --command bash scripts/test-postgres.sh
nix develop --command bash scripts/bench-postgres.sh

nix develop --command env GRIND_BENCH_RELEASE_EVIDENCE=1 GRIND_BENCH_T2_STRESS=1 GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 bash scripts/bench-matrix.sh all

nix develop --command env GRIND_BENCH_RELEASE_EVIDENCE=1 GRIND_BENCH_NETWORK_DELAY_MS=5 GRIND_BENCH_T2_PROFILES=bench/profiles/t2-smoke.txt GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 bash scripts/bench-matrix.sh l6t2
nix develop --command env GRIND_BENCH_RELEASE_EVIDENCE=1 GRIND_BENCH_NETWORK_DELAY_MS=5 GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 bash scripts/bench-matrix.sh l3
nix develop --command env GRIND_BENCH_RELEASE_EVIDENCE=1 GRIND_BENCH_NETWORK_DELAY_MS=5 GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 bash scripts/bench-matrix.sh l7

nix develop --command bash scripts/test-resilience.sh --release-evidence --deadline-ms 4000 --lease-ms 30000 --soak-seconds 7200
nix develop --command bash oracle/run-faults.sh
```

After dependency alignment, check packaging with `nix develop --command gleam
export hex-tarball` and `nix develop --command gleam docs build`. These commands
build local artifacts; they do not publish. Inspect the export and exercise an
external consumer before accepting it.

Then run `python3 oracle/fault_compare.py --grind <grind-run> --oban <oban-run>
--output <new-comparison.json>` against those fresh M2/M6 runs. The ordinary
PostgreSQL gate already includes the twelve core Oban pairs, ledger validation,
Squirrel generation checks and the external consumer tests. Plain `gleam test`
can skip PostgreSQL cases and does not replace that gate.

The benchmark matrix requires at least three measured repeats after warm-up.
Use the same source, pool/concurrency shapes, job costs and drain budget for
the zero-delay and delayed comparisons. The stress and delayed subsets are
specified in [the benchmark guide](../bench/README.md).

The soak requirement is **two hours of mixed faults after warm-up**, plus all
fourteen standalone cases. A shorter rehearsal validates the harness; it does
not qualify a release. A 24-hour run is not required. See the
[resilience guide](../resilience/README.md) for scenario and resource assertions.

## What passing means

- Every required test marker and paired scenario ran; no skipped database suite
  is counted as a pass. Claims, receipts, effects and explicit replay agree.
- Healthy jobs survive the tested slow-ACK and pool-pressure configurations.
  Fault targets may become uncertain only where the scenario expects it.
  T1 must report `t1_triggered=false` and strictly positive minimum headroom
  across all attempts; the runner's exit alone does not enforce the lag verdict.
  T2 thresholds, arrival validity and resource bounds remain unchanged.
- The benchmark has no unexplained accounting, generator, SQL or resource
  failure. Report throughput and latency with hardware and configuration.
  T3's coordinator bottleneck remains a documented, deferred optimization;
  crossing its 70% trigger is not itself a release blocker.
- Fresh M2/M6 comparisons classify Grind's audited replay and leaderless pruning
  as deliberate differences from Oban. They do not prove every fault equivalent.
- Review records actual duration, fault coverage, provenance, final outcomes and
  cleanup. Close any new correctness finding before accepting the candidate.

## Release record and output retention

Commit one concise release record: Grind and dependency revisions, toolchain and
machine, configuration, command exits, test counts, benchmark medians/ranges,
soak duration and fault counts, resource peaks, and remaining limits. Record a
failed run as failed; an unchanged retry does not erase it.

Raw logs, traces, CSVs, runtime copies and source archives are temporary outputs.
Keep them through review, then delete them after recording the checked summary.
They are ignored by Git. A summary preserves the finding, not the ability to
re-audit deleted bytes. Historical results below cannot qualify a newer build.

## Historical baseline and remaining scope

- Runtime isolation, first-ACK recovery, startup cleanup, B1–B10 repairs and the
  independent-node harness landed in `1e87d2c`. Historical matrix results and
  their configuration are summarized in [bench/README.md](../bench/README.md).
- Diagnostics landed in `75e50ae`: 261 root tests, 12 consumer tests, twelve
  core pairs, 38 benchmark tests and the 1,000-job smoke audit passed with
  Sinal `858dfa3`. See [OPERATIONAL-DIAGNOSTICS.md](OPERATIONAL-DIAGNOSTICS.md).
- The earlier dirty-source two-hour run passed 14 standalone cases and 266
  mixed rounds. Its summary is in [resilience/README.md](../resilience/README.md).
  It predates diagnostics and today's Sinal changes.
- Before 1.0: transaction-scoped enqueue and stronger public testing helpers.
  Their remaining design constraints are in [RELEASE-EXECUTION.md](RELEASE-EXECUTION.md).
- Deferred: batch claims and broader throughput optimization. Cron, Reindexer,
  automatic Lifeline replay and Peer election are outside the initial scope.
- Documented deployment limits remain: pooler support, encrypted fault paths,
  indefinite consumer-churn atom growth and liveness during arbitrary outages
  are not established by the laptop results. See [RISKS.md](RISKS.md).
