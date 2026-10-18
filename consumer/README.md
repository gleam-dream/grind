# Public Grind consumer

This separate Gleam package depends on Grind by local path and imports only its public modules, so it checks the API the way an application uses it. Its tests cover typed workers and outcomes (`execution_test`), caller-owned codecs (`codec_test`), uniqueness, idempotent IDs and transactional admission (`admission_test`), retry, cancellation and operator recovery (`recovery_test`), transactional resolution (`resolution_transaction_test`), storage failure (`storage_test`), pruning (`retention_test`), observations (`observation_test`) and test support (`testing_test`).

The synthetic payment effect is an ETS table keyed by the caller's idempotency key (`test/consumer_effect.erl`). `fault_proxy.gleam` and `grind_fault_proxy.erl` link the repository's test-only TCP proxy for the lost-commit-reply cases.

The usage examples, one compiled module per topic, are in [examples/usage](../examples/usage/README.md).

## Run

```sh
nix develop --command bash scripts/test-postgres.sh
```

The script sets `GRIND_CONSUMER_DATABASE_URL` and checks that every test wrote its contract marker. Without the variable, database tests return early.
