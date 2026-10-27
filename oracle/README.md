# Oban behavioral oracle

[![CI](https://github.com/gleam-dream/grind/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/gleam-dream/grind/actions/workflows/ci.yml)
[![grind on Hex](https://img.shields.io/hexpm/v/grind?label=Hex%3A%20grind)](https://hex.pm/packages/grind)
[![Gleam](https://img.shields.io/badge/Gleam-FFAFF3?logo=gleam&logoColor=000000)](https://gleam.run/)

This directory pins Oban OSS `v2.24.1` as a scoped behavioral reference. The pin, the compared scenarios, the classification, the deliberate differences and the pin-upgrade procedure are in [docs/ORACLE.md](../docs/ORACLE.md), which links the benchmark rules in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md). [ORACLE-LEDGER.md](ORACLE-LEDGER.md) maps Grind tests to upstream sources, and `OBAN-LICENSE.txt` preserves the Apache-2.0 notice.

```sh
nix develop --command bash scripts/test-postgres.sh   # twelve paired scenarios, comparator tests, ledger check
nix develop --command bash oracle/run-faults.sh       # independent-VM M2 and M6 fixtures
python3 oracle/fault_compare.py --grind <grind-run> --oban <oban-run> --output <new-comparison.json>
```

Results go to `oracle/results/`, and existing result directories are refused. The ledger is reviewed manually. `run.exs` keeps six Oban-only observations that are not paired evidence.
