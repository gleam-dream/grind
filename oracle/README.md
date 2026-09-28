# Pinned Oban behavioral oracle

The oracle pins Oban OSS `v2.24.1` at commit
`64b8481e5383f6bc46b7a5284e4ced3202dec9d5` and selects
`Oban.Engines.Basic`. No Oban Pro equivalence is claimed.
`OBAN-LICENSE.txt` preserves the Apache-2.0 notice.

Run the integrated gate with:

```sh
nix develop --command bash scripts/test-postgres.sh
```

## Independent-VM fault fixtures

`fault-scenarios.json` specifies separate M2 and M6 expectations for Oban and
Grind. These fixtures use ordinary Oban execution (`testing: :disabled`), its
Postgres notifier, and `Oban.Peers.Database`. Each node is a distinct named BEAM
operating-system process. They do not add to the twelve deterministic paired
cases below, and an Oban-only run does not establish a completed paired run.

Run after obtaining the checkout's build slot:

```sh
nix develop --command bash oracle/run-faults.sh
nix develop --command bash oracle/run-faults.sh --scenarios M2
python3 -m unittest discover -s oracle -p 'test_faults.py' -v
```

The script creates a disposable local `oban_resilience` database and compiles
the oracle once. Child VMs load those compiled modules without running Mix.
`GRIND_ORACLE_FAULT_OUTPUT` selects a new evidence directory; existing directories
are refused. The default is `oracle/results/<timestamp>-<pid>`. Failures retain
VM logs, controller events, and database snapshots when the database is reachable.

- **M2:** the worker appends its synthetic effect and calls `file:sync` before
  writing a marker and reaching a file barrier. The controller requires that
  marker and an `executing`, attempt-1 database row before SIGKILL. It confirms
  OS death, then waits for the survivor's configured Lifeline to report this job
  as rescued. A second independently fsynced effect must occur on the surviving
  VM before the barrier is released. Only a committed `completed`, attempt-2 row
  establishes completion. No direct timestamp/state mutation or operator replay
  causes the rescue. Grind's corresponding M2 instead requires `uncertain`, one
  effect before attributed `AuthorizeReplay`, and a second effect only afterward.
- **M6:** two VMs use the same Oban instance name and schema. The controller
  verifies one leader through the public Peer API and `oban_peers`, completes a
  real job, and requires that leader's Pruner event to identify the deleted job.
  After confirmed SIGKILL, the survivor must acquire leadership through normal
  timed election and prune a second completed job. No forced election or plugin
  tick is sent. Grind's M6 validates concurrent pruning with `SKIP LOCKED`; Grind
  has no equivalent leader to fail over.

The fixture defaults are a 500 ms Peer interval, 200 ms plugin interval, 5,000 ms
Lifeline rescue age, and a Pruner age of **one second**. They are recorded in
provenance and are configurable through the controller's timing arguments except
for the fixed prune age. They are accelerated fixture settings. Rescue uses the
real `attempted_at` age; it does not infer that a node died.

Effects use the resilience harness fields `event`, `key`, `node`, `beam_node`,
`os_pid`, `worker_pid`, and `at_ms`, plus Oban job ID and attempt. A separate
`handler_finished` record describes callback return. Full committed job and Peer
rows, actual plugin job IDs/counts, fault activation, death confirmation, source
digest and the pinned Oban commit are retained. Oban Basic has no Grind receipt
or audited resolution table; the artifact states that difference explicitly.

The fixtures must be executed successfully before their scenarios are cited as
validated. Python evidence checks alone do not establish Oban rescue or failover.

After both independent-VM runs include M2 and M6, compare their retained evidence:

```sh
python3 oracle/fault_compare.py --grind /path/to/grind-run --oban /path/to/oban-run --output /path/to/new-comparison.json
```

The comparator reconstructs normalized observations from committed snapshots,
fsync/death/replay barriers, attributed resolution rows, acknowledgement fences,
Peer changes and actual pruned job IDs. It validates each engine against its
separate expected contract. A declared passing status alone is insufficient;
missing intermediate evidence fails. The comparison retains hashes of its input
artifacts and both runs' provenance. These are two independently recorded runs
of one scenario contract, with intentional differences, rather than equal
runtime behavior.

## Actual paired execution

`scenarios.json` is a versioned catalog of common inputs, execution counts,
timing parameters, expected observations, source references, and semantic
classifications. Both adapters read it:

- `test/grind/oracle/paired.gleam` drives public Grind admission, queue,
  cancellation, outcome, uniqueness, and pruning APIs.
- `paired.exs` drives Oban Basic through insertion, draining, cancellation,
  and a real Pruner service tick.
- `compare.exs` reads both JSONL result streams. Every executable scenario
  must appear exactly once in each stream, under the current catalog version
  and one shared run identity. Results from separate runs cannot be paired.
  Each result must exactly match its declared expectation. Equivalent cases
  must also match each other. Equal but wrong outputs fail too.

The twelve cases cover successful execution, terminal business failure,
first retry, retry exhaustion, snooze, cancellation before execution, future
and due scheduling, explicit discard, duplicate unique insertion, deletion
of old terminal rows, and the completed-row retention-clock difference.

Completion is established from a committed row after processing. The success
case also checks Grind's public typed outcome and Oban's actual worker return
value. Handler invocation counts alone never establish durable completion.
The adapters query stored attempt/snooze counters and relative due time;
generated IDs and absolute timestamps are omitted. State normalization only
maps Oban `completed` to `succeeded` and Grind `queued` to `available`.

Three cases preserve differences: Grind's terminal business failure and
exhaustion remain `business_failed` while Oban uses `discarded`; an old
scheduled timestamp with a recent completion is retained by Grind's
`finished_at` retention rule but pruned by Oban Basic. These differences are
asserted separately, never normalized away.

Each scenario starts with an empty installation; fixture cleanup prevents a
previous scenario's terminal rows from contaminating a later prune count.
The first-retry, snooze and future-scheduling cases check the persisted due time
against the configured 60-second delay, bracketed by PostgreSQL clock reads
before and after the public action. A 20 ms clock tolerance accommodates Oban's
application-clock scheduling; an observation span above five seconds fails.
This does not claim equal default backoff. Uniqueness compares one matching scalar
key, not the entire uniqueness contract. The pruning fixtures alter only
timestamps after public execution has durably completed, avoiding minute-long
test sleeps. The pruner case uses an isolated Oban peer and makes no claim
about leader-election correctness. Both adapters require empty dedicated
databases and fail if configuration is missing.

The gate invokes `scripts/run-paired-oracle.sh` with these variables:

```sh
GRIND_ORACLE_DATABASE_URL=postgres://.../grind_oracle_test
GRIND_OBAN_TEST_DATABASE_URL=postgres://.../oban_paired_test
GRIND_ORACLE_RESULTS_ROOT=/path/to/run-results
```

That script runs both adapters, the comparator regression tests, and the
comparison. The result directory contains `grind.jsonl`, `oban.jsonl`, adapter
logs, the exact `catalog.json`, and `provenance.json` with source, dependency,
catalog, output and toolchain hashes. The integrated gate retains these under
`oracle/results/core-<timestamp>-<pid>` by default and copies its PostgreSQL log
there on cleanup. Existing result directories are refused. Both adapters read
the saved catalog snapshot; source changes during the pair fail the evidence
check. A modified Oban checkout is rejected even when its HEAD SHA matches.
Dirty Grind source is labeled exploratory. Run this script
inside `nix develop`, after `mix deps.get --check-locked` in `oracle/`, with
any gate-local `MIX_HOME`, `HEX_HOME`, and `MIX_REBAR3` environment retained.

To recheck a completed retained pair from `oracle/`, run
`mix run --no-start compare.exs <run>/catalog.json <run>/grind.jsonl <run>/oban.jsonl <run>/provenance.json`.
That verifies the saved catalog/output hashes and shared run identity before
comparing observations.

## Gaps and alignment over time

Each catalog entry has one explicit classification:

- `equivalent`: compare the same observed behavior.
- `intentional-grind-semantics`: preserve an explained semantic difference.
- `missing-accidental-divergence`: a capability or required measurement is
  missing; this is not passing evidence.
- `deferred`: the owner explicitly deferred the capability.

`gaps` entries never count toward paired passes. They record remaining fault,
multi-node, throughput, uniqueness, and API work. Grind's no-automatic-replay
policy, live-lease fencing, durable receipts, and audited replay are deliberate
contracts. They must not be weakened to make an Oban comparison pass. This
deterministic suite does not replace the fault-proxy, multi-node, or soak
release evidence.

`scripts/check-oracle-ledger.exs` checks every local test name and upstream
source path in `ORACLE-LEDGER.md`, every catalog source/adapter path, and the
actual Oban checkout SHA. It requires only Elixir/OTP, not a running database.
It does not resolve natural-language quotations as upstream test identifiers.
On a pin upgrade, review changed source contracts, update the catalog and
classifications, and reproduce both outputs before accepting a new baseline.
Changing an expectation merely to make a changed implementation pass is not
alignment evidence.

Correctness thresholds are exact: no unexpected state, attempt count,
execution count, missing observation, or undeclared difference. Timing is a
future/due relation with generous setup margins, not an equality of clocks.
Throughput thresholds belong to the separately controlled benchmark suite.

## Legacy observations

`run.exs` remains as six Oban-only observations: success, discard, first
retry, exhaustion, snooze, and a Pruner tick. It includes an automatically
polling producer and job-stop telemetry. Those assertions remain useful but
do not themselves count as paired comparison. The historical ledger retains
its original source mappings and mutation evidence with that distinction.
