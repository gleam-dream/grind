# Qualification evidence and reproducible runs

Historical results describe their recorded source and dependencies. They do not establish that another candidate is qualified. Material rationale and provenance are in [ADR-0009](../adr/0009-separate-observations-from-qualification-evidence.md) and the original commit mapping is in [ADR-0011](../adr/0011-retain-rewritten-history-as-source-provenance.md).

Use the [benchmark guide](../../bench/README.md), [resilience guide](../../resilience/README.md), and [oracle ledger](../../oracle/ORACLE-LEDGER.md) for executable commands and scenario provenance. The scenario manifests and audit code are retained executable inputs. Historical measured conclusions, failed-run limits and source/dependency attribution have one owner in ADR-0009.

For each new candidate record source and resolved dependency identities, cleanliness/content hashes, hardware and toolchain, exact workload, pool/concurrency and resource shape, delay semantics, warm-up/measured repeats, budgets, command exits, required markers, validity verdicts and failed-run classifications. A new dependency requires evidence for the affected behavior. A clean root tree does not prove the sibling path dependency was clean or built as recorded.

Run serially in a fresh checkout; these harnesses create disposable PostgreSQL clusters and ignored results. Prevent host sleep for long runs. The approved endurance requirement is two hours of mixed faults after warm-up plus fourteen standalone cases; a shorter rehearsal only checks the harness. A twenty-four-hour qualification is not required.

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

The ordinary PostgreSQL gate includes root/consumer suites, twelve paired core scenarios, ledger validation and Squirrel conformance. Plain gleam test may skip database cases and cannot replace it. Run the benchmark matrix with at least three measured repeats after warm-up and equal source, pool/concurrency, job costs and drain budgets for compared arms.

Compare fresh M2/M6 runs with `python3 oracle/fault_compare.py --grind <grind-run> --oban <oban-run> --output <new-comparison.json>`. Audited replay and concurrent leaderless pruning are deliberate differences, not universal fault parity.

Acceptance requires all expected markers and paired cases; no skipped database suite counts. Claims, effects, receipts and authorized replay must agree. Healthy siblings survive the chosen slow-ACK and pressure configurations. T1 must report t1_triggered=false and positive minimum headroom; runner exit alone does not enforce the lag verdict. T2 thresholds, arrival validity and resource checks retain their meaning. T3 crossing its seventy-percent trigger is a deferred optimization signal, not itself proof of a correctness failure. Retain and independently review raw output through acceptance; record failed runs as failed before deleting temporary output.

Package review uses local `nix develop --command gleam export hex-tarball` and `nix develop --command gleam docs build`, then a separate consumer built from the export without sibling checkouts. Dependency publication, tagging and pushing are separate owner actions. Align local and CI resolution and record the accepted commit rather than inferring acceptance from historical counts.

Deployment evidence remains bounded: encrypted network faults, connection poolers and arbitrary outages are not established by laptop measurements. The alternate OTP socket backend is unsupported by the pinned driver probe. Finite-name reuse and long consumer churn need bounded atom/resource review. The upstream rollback-after-crash failure can bypass a typed CommitUnknown through a process crash; lease recovery remains authoritative.
