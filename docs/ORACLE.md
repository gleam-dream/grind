# Oracle

This document names the reference implementation Grind is compared against, what the comparison covers, how outcomes are classified, and where the claim stops. The oracle checks behavior. It is a scoped reference and never a runtime wrapper or a substitute for Grind's own contracts.

- **Throughput and latency:** [BENCHMARKS.md](BENCHMARKS.md). Correctness thresholds here are exact, and performance thresholds belong there. A benchmark that compares Grind with Oban uses the matching rules in that document.
- **Method and qualification rules:** "Oracle method" and "Qualification evidence" in [the design](design/design.typ), with the decisions in [ADR-0001](adr/0001-preserve-full-job-scope-with-scoped-oracles.md), [ADR-0009](adr/0009-separate-observations-from-qualification-evidence.md) and [ADR-0019](adr/0019-keep-usage-as-compiled-examples-and-reference-docs-as-retained-files.md).
- **Commands for every gate:** the [agent instructions](../AGENTS.md). The harness lives in [oracle/](../oracle/README.md).

## Reference

| Reference          | Upstream                          | Pin                                               | Scope                                                                                                                                                           |
| ------------------ | --------------------------------- | ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Oban OSS `v2.24.1` | <https://github.com/oban-bg/oban> | commit `64b8481e5383f6bc46b7a5284e4ced3202dec9d5` | The `Oban.Engines.Basic` engine with ordinary execution, its Postgres notifier and `Oban.Peers.Database`. No Oban Pro capability or Pro equivalence is claimed. |

- **License.** Oban is Apache-2.0, and [OBAN-LICENSE.txt](../oracle/OBAN-LICENSE.txt) preserves its notice.
- **Pin enforcement.** The oracle build takes the commit from `oracle/mix.exs`. The evidence check rejects a checkout whose commit differs from the pin, and a modified checkout is rejected even when its commit matches.
- **Provenance of Grind tests.** [ORACLE-LEDGER.md](../oracle/ORACLE-LEDGER.md) maps each Grind test to the upstream source it was derived from. `scripts/check-oracle-ledger.exs` verifies every local test name, every upstream source path, every catalog source and adapter path and the checkout commit. It needs Elixir and OTP, not a database, and it does not resolve natural-language quotations as upstream test identifiers.

## What is compared

The scenario catalog, [scenarios.json](../oracle/scenarios.json), is a versioned file of common inputs, execution counts, timing parameters, expected observations, source references and classifications. Two adapters read it:

- `test/grind/oracle/paired.gleam` drives Grind's public admission, queue, cancellation, outcome, uniqueness and pruning APIs.
- `oracle/paired.exs` drives Oban Basic through insertion, draining, cancellation and a real Pruner tick.
- `oracle/compare.exs` requires every executable scenario exactly once in each result stream, under the current catalog version and one shared run identity. Each result must equal its declared expectation, and equivalent cases must equal each other. Equal but wrong outputs fail, and results from separate runs cannot be paired.

The twelve paired cases cover successful execution, terminal business failure, first retry, retry exhaustion, snooze, cancellation before execution, future and due scheduling, explicit discard, duplicate unique insertion, deletion of old terminal rows and the completed-row retention clock.

- **Completion** is established from a committed row after processing. The success case also checks Grind's typed outcome and Oban's worker return value. Handler invocation counts never establish durable completion.
- **Normalization** maps only Oban `completed` to `succeeded` and Grind `queued` to `available`. Generated identifiers and absolute timestamps are omitted, and the adapters compare stored attempt and snooze counters and relative due times.
- **Timing** is a relation. Persisted due times are checked against the configured 60-second delay, bracketed by PostgreSQL clock reads before and after the public action, with a 20 ms tolerance and a five-second maximum observation span. This does not claim equal default backoff.
- **Uniqueness** compares one matching scalar key and not the whole uniqueness contract.
- **Pruning** fixtures alter only timestamps after public execution has durably completed. The pruner case uses an isolated Oban peer and says nothing about leader-election correctness.
- **Isolation.** Each scenario starts with an empty installation, and both adapters require empty dedicated databases and fail if configuration is missing.

### Classification

Each catalog entry carries exactly one classification.

| Classification                  | Meaning                                                                        |
| ------------------------------- | ------------------------------------------------------------------------------ |
| `equivalent`                    | The same observed behavior is compared.                                        |
| `intentional-grind-semantics`   | An explained difference is preserved and asserted separately.                  |
| `missing-accidental-divergence` | A capability or required measurement is missing. This is not passing evidence. |
| `deferred`                      | The owner explicitly deferred the capability.                                  |

Gap entries never count toward paired passes. They record remaining fault, multi-node, throughput, uniqueness and API work.

### Deliberate differences

These are asserted and never normalized away:

- Grind's terminal business failure and retry exhaustion remain `business_failed`, while Oban uses `discarded`.
- An old scheduled timestamp with a recent completion is retained by Grind's `finished_at` retention rule and pruned by Oban Basic.
- Grind holds an abandoned attempt `uncertain` and replays only after an attributed `AuthorizeReplay`, while Oban's Lifeline rescues it.
- Grind has durable receipts, live-lease fencing and an audited resolution table, and Oban Basic has none of them. Grind has no leader election, so concurrent pruning uses `SKIP LOCKED`.

Grind's no-automatic-replay policy, live-lease fencing, durable receipts and audited replay are deliberate contracts. An Oban comparison never weakens them to make a case pass.

## Independent-VM fault fixtures

[fault-scenarios.json](../oracle/fault-scenarios.json) specifies M2 and M6 separately for Oban and Grind. Each node is a distinct named BEAM operating-system process. The fixtures do not add to the twelve paired cases, and an Oban-only run does not establish a completed paired run.

- **M2.** The worker appends its synthetic effect and calls `file:sync` before writing a marker and reaching a file barrier. The controller requires the marker and an `executing`, attempt-1 database row before SIGKILL, confirms OS death and waits for the survivor's Lifeline to report the job as rescued. A second independently fsynced effect must occur on the surviving VM before the barrier is released, and only a committed `completed`, attempt-2 row establishes completion. No direct timestamp mutation or operator replay causes the rescue. Grind's M2 instead requires `uncertain`, one effect before attributed `AuthorizeReplay`, and a second effect only afterward.
- **M6.** Two VMs share one Oban instance name and schema. The controller verifies one leader through the public Peer API and `oban_peers`, completes a job and requires that leader's Pruner event to identify the deleted job. After a confirmed SIGKILL the survivor must acquire leadership through normal timed election and prune a second job, with no forced election or plugin tick. Grind's M6 validates concurrent pruning with `SKIP LOCKED`.
- **Timings.** The defaults are a 500 ms Peer interval, a 200 ms plugin interval, a 5,000 ms Lifeline rescue age and a one-second Pruner age, all recorded in provenance. They are accelerated fixture settings, and rescue uses the real `attempted_at` age without inferring that a node died.
- **Evidence.** Effects use the harness fields `event`, `key`, `node`, `beam_node`, `os_pid`, `worker_pid` and `at_ms`, with the job ID and attempt. Retained evidence includes full committed job and Peer rows, actual plugin job IDs and counts, fault activation, death confirmation, the source digest and the pinned Oban commit.

The fixtures must run successfully before their scenarios are cited as validated, and Python evidence checks alone do not establish Oban rescue or failover. After both runs include M2 and M6, the comparator reconstructs normalized observations from committed snapshots, barriers, resolution rows, acknowledgement fences, Peer changes and pruned job IDs. It validates each engine against its own expected contract, retains hashes of its inputs and both runs' provenance, and fails on missing intermediate evidence. The result is two independently recorded runs of one scenario contract with intentional differences, not equal runtime behavior.

## Run

The integrated gate runs the twelve paired scenarios, the comparator regression tests and the ledger check:

```sh
nix develop --command bash scripts/test-postgres.sh
```

It invokes `scripts/run-paired-oracle.sh` with `GRIND_ORACLE_DATABASE_URL`, `GRIND_OBAN_TEST_DATABASE_URL` and `GRIND_ORACLE_RESULTS_ROOT`. Run the script inside `nix develop` after `mix deps.get --check-locked` in `oracle/`, and keep any gate-local `MIX_HOME`, `HEX_HOME` and `MIX_REBAR3`.

The independent-VM fixtures run separately, after the checkout's build slot is obtained:

```sh
nix develop --command bash oracle/run-faults.sh
nix develop --command bash oracle/run-faults.sh --scenarios M2
python3 -m unittest discover -s oracle -p 'test_faults.py' -v
```

The script creates a disposable `oban_resilience` database and compiles the oracle once, and child VMs load those compiled modules without Mix. Compare two completed runs, and recheck a retained paired run from `oracle/`:

```sh
python3 oracle/fault_compare.py --grind /path/to/grind-run --oban /path/to/oban-run --output /path/to/new-comparison.json
mix run --no-start compare.exs <run>/catalog.json <run>/grind.jsonl <run>/oban.jsonl <run>/provenance.json
```

A run directory holds `grind.jsonl`, `oban.jsonl`, adapter logs, the exact `catalog.json` and `provenance.json` with source, dependency, catalog, output and toolchain hashes. Paired results go to `oracle/results/core-<timestamp>-<pid>` and fault results to `oracle/results/<timestamp>-<pid>`, unless `GRIND_ORACLE_FAULT_OUTPUT` selects a new directory. Existing result directories are refused, both adapters read the saved catalog, a source change during the pair fails the evidence check, and dirty Grind source is labeled exploratory. Failed fault runs retain VM logs, controller events and database snapshots when the database is reachable.

`oracle/run.exs` keeps six Oban-only observations (success, discard, first retry, exhaustion, snooze and a Pruner tick) with an automatically polling producer and job-stop telemetry. They remain useful but are not paired evidence.

## Pin upgrades

On a pin upgrade, review the changed upstream source contracts, update the catalog and its classifications, and reproduce both outputs before accepting a new baseline. Changing an expectation only to make a changed implementation pass is not alignment evidence.

## Limits of the claim

- The deterministic suite does not replace the fault-proxy, multi-node or soak evidence in the [resilience guide](../resilience/README.md).
- Correctness thresholds are exact: no unexpected state, attempt count or execution count, no missing observation and no undeclared difference. Throughput thresholds belong to [BENCHMARKS.md](BENCHMARKS.md).
- No Oban Pro behavior is inferred, and Oban Basic's lack of receipts and audited resolution is stated in the artifacts rather than hidden.
