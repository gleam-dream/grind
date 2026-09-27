# Module map

This map locates the current implementation and its tests. Historical evidence
keeps the names and commit references used when each claim was tested.

## Runtime and storage

| Responsibility                                                         | Implementation                                                                                                                                                                                     | Tests                                                                                                                                                                                           |
| ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Typed jobs and workers                                                 | [job](../src/grind/job.gleam), [worker](../src/grind/worker.gleam)                                                                                                                                 | [Worker policy](../test/grind/worker/), [consumer policy](../consumer/test/grind_consumer/policy_test.gleam)                                                                                    |
| Database settings, pool lifecycle and public storage API               | [postgres](../src/grind/postgres.gleam)                                                                                                                                                            | [Database](../test/grind/database/), [consumer storage](../consumer/test/grind_consumer/storage_test.gleam)                                                                                     |
| Audited resolution of uncertain jobs                                   | [Resolution protocol](../src/grind/internal/postgres/resolution.gleam), [resolution queries](../src/grind/internal/postgres/resolution_queries.gleam)                                              | [Resolution](../test/grind/queue/resolution_test.gleam), [resolved observations](../test/grind/observations/resolved_test.gleam)                                                                |
| Handle binding, arguments, state, outcome and acknowledgement receipts | [Job reads](../src/grind/internal/postgres/job_reads.gleam)                                                                                                                                        | [Database identity](../test/grind/database/identity_test.gleam), [queue outcomes](../test/grind/queue/outcomes_test.gleam), [acknowledgements](../test/grind/queue/acknowledgements_test.gleam) |
| Migration execution and schema checks                                  | [Migration runner](../src/grind/internal/postgres/migration.gleam), [schema probe](../src/grind/internal/postgres/schema_probe.gleam), [migration catalog](../src/grind/internal/migrations.gleam) | [Migrations](../test/grind/migrations/)                                                                                                                                                         |
| Admission and uniqueness                                               | [Public storage API](../src/grind/postgres.gleam), [unique admission](../src/grind/internal/unique_admission.gleam), [request and query helpers](../src/grind/internal/unique_admission/)          | [Submission](../test/grind/submission/), [uniqueness](../test/grind/unique/)                                                                                                                    |
| Consumer supervision and coordinator lifecycle                         | [Queue](../src/grind/queue.gleam), [queue helpers](../src/grind/internal/queue/)                                                                                                                   | [Queue](../test/grind/queue/), [consumer execution](../consumer/test/grind_consumer/execution_test.gleam)                                                                                       |
| Claims, execution, leases and acknowledgements                         | [Attempt](../src/grind/internal/attempt.gleam), [lease](../src/grind/internal/lease.gleam), [acknowledgement protocol](../src/grind/internal/attempt/acknowledgement.gleam)                        | [Claims](../test/grind/queue/claims_test.gleam), [leases](../test/grind/queue/leases_test.gleam), [ack failures](../test/grind/queue/ack_failure_test.gleam)                                    |
| Event records and wire codecs                                          | [Observation](../src/grind/observation.gleam), [shared wire codecs](../src/grind/internal/observation/wire.gleam)                                                                                  | [Observations](../test/grind/observations/)                                                                                                                                                     |
| Pruning and retention                                                  | [Pruner](../src/grind/pruner.gleam), [public pruning operation](../src/grind/postgres.gleam)                                                                                                       | [Retention](../test/grind/retention/)                                                                                                                                                           |
| PostgreSQL checkout deadlines                                          | [Store](../src/grind/internal/store.gleam), [checkout FFI](../src/grind_postgres_ffi.erl)                                                                                                          | [Fault-proxy scenarios](../test/grind_fault_proxy_test.gleam)                                                                                                                                   |

Public constructors and entry points remain in their public modules. Internal
PostgreSQL modules own operation implementations; the facade translates their
results where needed to preserve public types. Queue claims, renewals and
acknowledgements still run through one coordinator. Module extraction did not
change that process topology or resolve T2/T3; see
[release readiness](RELEASE-READINESS.md) and
[performance evidence](PERFORMANCE-EVIDENCE.md).

Static SQL comes from [query files](../src/grind/internal/sql/) and is generated
into [sql.gleam](../src/grind/internal/sql.gleam). Follow [AGENTS.md](../AGENTS.md)
for query generation and migration changes.

## Tests, benchmarks and evidence

- [Root tests](../test/grind/) are grouped by behavior. Shared fixtures live in
  [support](../test/grind/support/); the [root runner](../test/grind_test.gleam)
  also retains the version and PostgreSQL driver shape tests.
- [Consumer tests](../consumer/test/grind_consumer/) exercise public imports.
- [Benchmark CLI](../bench/src/grind_bench/load.gleam) dispatches to
  [workloads and reporting](../bench/src/grind_bench/load/).
  [Audit checks](../bench/src/grind_bench/audit.gleam) validate execution evidence.
- [Oracle ledger](../oracle/ORACLE-LEDGER.md) records the pinned Oban comparison
  and current test baseline. [Recovery evidence](RECOVERY-EVIDENCE.md#topic-index)
  records fault mechanisms and historical validation.
- Run the database gate with `nix develop --command bash scripts/test-postgres.sh`
  and the benchmark gate with `nix develop --command bash scripts/bench-postgres.sh`.
  Run them sequentially: the database gate cleans build artifacts.
