# Module map

This map locates the current implementation and its tests. Historical evidence
keeps the names and commit references used when each claim was tested.

## Public modules

| Module                                          | Holds                                                                                          |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| [grind](../src/grind.gleam)                     | Configuration, the supervised runtime, submission, reads, cancellation, errors and their kinds |
| [grind/worker](../src/grind/worker.gleam)       | Workers, codecs, responses, retry, timeout, snooze and abandonment policies, handler context   |
| [grind/job](../src/grind/job.gleam)             | The job builder, handles, states and terminal causes                                           |
| [grind/queue](../src/grind/queue.gleam)         | Per-queue settings                                                                             |
| [grind/unique](../src/grind/unique.gleam)       | Uniqueness policies                                                                            |
| [grind/admin](../src/grind/admin.gleam)         | Listing, audited resolution, quarantine, pruning, acknowledgement receipts                     |
| [grind/telemetry](../src/grind/telemetry.gleam) | Lifecycle and diagnostic Sinal descriptors                                                     |
| [grind/testing](../src/grind/testing.gleam)     | `perform` without a database, bounded `drain`                                                  |

The facade translates engine values to the public vocabulary in
[convert](../src/grind/internal/convert.gleam). The runtime's registered
state, found by name, is in [runtime](../src/grind/internal/runtime.gleam);
the admission request `grind/job` builds is in
[admission](../src/grind/internal/admission.gleam).

## Runtime and storage

| Responsibility                                                         | Implementation                                                                                                                                                                                     | Tests                                                                                                                                                                                           |
| ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Worker definitions and erased execution                                | [worker](../src/grind/internal/worker.gleam), [job](../src/grind/internal/job.gleam), [registry](../src/grind/internal/registry.gleam)                                                             | [Worker policy](../test/grind/worker/), [definitions](../test/grind/facade/definition_test.gleam)                                                                                               |
| Settings, pool lifecycle and storage operations                        | [postgres](../src/grind/internal/postgres.gleam), [pool lifetime](../src/grind/internal/pool.gleam), [owner FFI](../src/grind_pool_ffi.erl)                                                        | [Database](../test/grind/database/), [startup and cache drain](../test/grind/database/startup_test.gleam), [facade lifecycle](../test/grind/facade/lifecycle_test.gleam)                        |
| The supervised runtime                                                 | [grind](../src/grind.gleam), [runtime](../src/grind/internal/runtime.gleam), [runtime FFI](../src/grind_runtime_ffi.erl)                                                                           | [Lifecycle](../test/grind/facade/lifecycle_test.gleam)                                                                                                                                          |
| Audited resolution of uncertain jobs                                   | [Resolution protocol](../src/grind/internal/postgres/resolution.gleam), [resolution queries](../src/grind/internal/postgres/resolution_queries.gleam)                                              | [Resolution](../test/grind/queue/resolution_test.gleam), [resolved observations](../test/grind/observations/resolved_test.gleam)                                                                |
| Handle binding, arguments, state, outcome and acknowledgement receipts | [Job reads](../src/grind/internal/postgres/job_reads.gleam)                                                                                                                                        | [Database identity](../test/grind/database/identity_test.gleam), [queue outcomes](../test/grind/queue/outcomes_test.gleam), [acknowledgements](../test/grind/queue/acknowledgements_test.gleam) |
| Migration execution and schema checks                                  | [Migration runner](../src/grind/internal/postgres/migration.gleam), [schema probe](../src/grind/internal/postgres/schema_probe.gleam), [migration catalog](../src/grind/internal/migrations.gleam) | [Migrations](../test/grind/migrations/)                                                                                                                                                         |
| Admission, receipts, uniqueness and transactional submit               | [unique admission](../src/grind/internal/unique_admission.gleam), [request and query helpers](../src/grind/internal/unique_admission/), [submission](../src/grind/internal/submission.gleam)       | [Submission](../test/grind/submission/), [uniqueness](../test/grind/unique/), [facade submit](../test/grind/facade/submit_test.gleam), [`submit_in`](../test/grind/facade/behavior_test.gleam)  |
| Consumer supervision, capacity, shutdown and drain                     | [consumer](../src/grind/internal/consumer.gleam), [queue helpers](../src/grind/internal/queue/)                                                                                                    | [Queue](../test/grind/queue/), [consumer lifecycle](../test/grind/queue/lifecycle_test.gleam)                                                                                                   |
| Claims and fenced storage operations                                   | [Attempt](../src/grind/internal/attempt.gleam), [lease and replay](../src/grind/internal/lease.gleam), [acknowledgement protocol](../src/grind/internal/attempt/acknowledgement.gleam)             | [Claims](../test/grind/queue/claims_test.gleam), [leases](../test/grind/queue/leases_test.gleam), [ack failures](../test/grind/queue/ack_failure_test.gleam)                                    |
| Handler process, timeout and cancellation; attempt-owned ACK           | [Attempt process](../src/grind/internal/queue/worker.gleam), [active-attempt bookkeeping](../src/grind/internal/queue/active.gleam)                                                                | [Executor regressions](../test/grind/queue/executor_test.gleam), [shutdown](../test/grind/queue/shutdown_test.gleam), [behavior](../test/grind/facade/behavior_test.gleam)                      |
| Independent renewal and cancellation delivery                          | [Renewer](../src/grind/internal/queue/renewer.gleam), [timing](../src/grind/internal/queue/timing.gleam), [batch renewal SQL](../src/grind/internal/attempt.gleam)                                 | [Renewer lifecycle](../test/grind/queue/renewer_test.gleam), [slow ACKs and pool saturation](../test/grind/queue/executor_test.gleam)                                                           |
| Events                                                                 | [telemetry](../src/grind/telemetry.gleam), [event helpers](../src/grind/internal/events.gleam)                                                                                                     | [Observations](../test/grind/observations/)                                                                                                                                                     |
| Pruning and retention                                                  | [Pruner](../src/grind/internal/pruner.gleam), [pruning operation](../src/grind/internal/postgres.gleam)                                                                                            | [Retention](../test/grind/retention/)                                                                                                                                                           |
| PostgreSQL checkout deadlines and connection recovery                  | [Store](../src/grind/internal/store.gleam), [checkout FFI](../src/grind_postgres_ffi.erl)                                                                                                          | [Fault-proxy scenarios](../test/grind_fault_proxy_test.gleam), [stale-holder recovery](../test/grind/database/reconnect_test.gleam)                                                             |

The engine tests under `test/grind/` other than `facade/` drive the internal
modules directly; `test/grind/facade/` and the [consumer package](../consumer/)
exercise the public API.

## Tests, benchmarks and evidence

- [Root tests](../test/grind/) are grouped by behavior. Shared fixtures live in
  [support](../test/grind/support/); the [root runner](../test/grind_test.gleam)
  also retains the PostgreSQL driver shape test.
- [Consumer tests](../consumer/test/grind_consumer/) exercise public imports.
- [Benchmark CLI](../bench/src/grind_bench/load.gleam) dispatches to
  [workloads and reporting](../bench/src/grind_bench/load/).
  [Audit checks](../bench/src/grind_bench/audit.gleam) validate execution evidence.
- [Shared oracle scenarios](../oracle/scenarios.json), [fault scenarios](../oracle/fault-scenarios.json)
  and the [oracle ledger](../oracle/ORACLE-LEDGER.md) define the pinned Oban
  comparison and current test baseline.
- The [resilience harness](../resilience/README.md) runs independent BEAM VMs,
  destructive faults and mixed-workload soak assertions. The approved two-hour
  run passed fourteen standalone cases and 266 mixed fault rounds; its reviewed
  causal audit and final paired M2/M6 comparison pass. The failed first
  86,400-second attempt is summarized as failed; raw artifacts were removed
  during cleanup. Day-long endurance remains unverified. [Recovery evidence](RECOVERY-EVIDENCE.md#topic-index) records
  exact counts, resource limits, cleanup and the auditor correction.
- Run the database gate with `nix develop --command bash scripts/test-postgres.sh`,
  the benchmark gate with `nix develop --command bash scripts/bench-postgres.sh`,
  and resilience with `nix develop --command bash scripts/test-resilience.sh`.
  Run them sequentially: the database gate cleans build artifacts.
