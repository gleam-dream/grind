# Benchmarks

[![CI](https://github.com/gleam-dream/grind/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/gleam-dream/grind/actions/workflows/ci.yml)
[![grind on Hex](https://img.shields.io/hexpm/v/grind?label=Hex%3A%20grind)](https://hex.pm/packages/grind)
[![Gleam](https://img.shields.io/badge/Gleam-FFAFF3?logo=gleam&logoColor=000000)](https://gleam.run/)

This unpublished Gleam project is the benchmark harness. The suites, run commands, environment variables, validity rules and retained results are in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md), which links the oracle comparison rules in [docs/ORACLE.md](../docs/ORACLE.md).

```sh
nix develop --command bash scripts/bench-smoke.sh
GRIND_BENCH_DRAIN_TIMEOUT_MS=600000 nix develop --command bash scripts/bench-matrix.sh l7
```

Point definitions live in [bench-matrix.sh](../scripts/bench-matrix.sh) and the single-run CLI in [load.gleam](src/grind_bench/load.gleam). Each matrix writes a fresh, ignored directory under `bench/results/`.
