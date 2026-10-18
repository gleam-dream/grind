# Oban behavioral oracle

This directory pins Oban OSS `v2.24.1` as a scoped behavioral reference. The pin, the compared scenarios, the classification, the deliberate differences and the pin-upgrade procedure are in [docs/ORACLE.md](../docs/ORACLE.md), which links the benchmark rules in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md). [ORACLE-LEDGER.md](ORACLE-LEDGER.md) maps Grind tests to upstream sources, and `OBAN-LICENSE.txt` preserves the Apache-2.0 notice.

```sh
nix develop --command bash scripts/test-postgres.sh   # twelve paired scenarios, comparator tests, ledger check
nix develop --command bash oracle/run-faults.sh       # independent-VM M2 and M6 fixtures
python3 oracle/fault_compare.py --grind <grind-run> --oban <oban-run> --output <new-comparison.json>
```

Results go to `oracle/results/`, and existing result directories are refused. `scripts/check-oracle-ledger.exs` validates the ledger and catalog paths without a database. `run.exs` keeps six Oban-only observations that are not paired evidence.
