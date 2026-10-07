# Public Grind consumer

This separate Gleam package depends on Grind by local path and imports only
Grind's public modules. It checks the public API the way an application
uses it:

- the common task: one runtime with two differently typed workers
  (`Worker(PaymentRequest, String, PaymentError)` and `Worker(Int, Int, Nil)`),
  one `grind.submit` each, and `grind.await` for the typed outcome
  (`execution_test`);
- caller-owned types: `src/grind_consumer.gleam` defines the domain types
  and their codecs as an application would, including a validating codec
  whose rejection reaches `grind.InvalidInput` before anything is written
  and `Failed(RuntimeFailure, ..)` after the handler ran (`codec_test`);
- advanced configuration: queue tuning, uniqueness with an existing
  conflict and a reschedule across queues, idempotent ids, and enqueueing
  inside the application's own transaction with `grind.submit_in`
  (`admission_test`);
- failure handling: a retry policy, cancellation of a running handler
  through `worker.cancellation`, and a handler that crashes after its effect,
  is held `uncertain` and is resolved by an operator through `grind/admin`
  (`recovery_test`), including both cancellation/uncertainty orderings, retained
  evidence, pruning exclusion, replay refusal and attributed terminal resolution; an unreachable database failing `start` with a typed
  error (`storage_test`); pruning (`retention_test`);
- telemetry: handlers on `grind/telemetry` descriptors see each job's
  correlation and committed state (`observation_test`);
- test support: `testing.perform` and `testing.drain` (`testing_test`).

The synthetic payment effect is an ETS table keyed by the caller's
idempotency key (`test/consumer_effect.erl`). Grind does not guarantee
exactly-once effects; the table shows how an application confirms an
uncertain effect before resolving it.

Run it through the repository's database gate,
`nix develop --command bash scripts/test-postgres.sh`, which sets
`GRIND_CONSUMER_DATABASE_URL` and checks that every test's contract marker
was written. Without the variable, database tests return early.

## Transactional resolution

- `resolution_transaction_test` uses only public Grind APIs and application-native values. It commits queue resolution and application acknowledgment in one borrowed transaction, then tests rollback, process death, lost commit reply and pruning.
- It also exercises configured schemas, stricter caller timeouts, exact-command conflicts, cancellation and same-job contention while unrelated work proceeds. The application still owns the outer transaction lifetime.
- `fault_proxy.gleam` and `grind_fault_proxy.erl` link the package's existing test-only TCP proxy. They share fault-injection machinery, not internal Grind APIs. The COMMIT reply-loss test kills the caller after independent commit readback; it does not assert an automatic driver retry result.
