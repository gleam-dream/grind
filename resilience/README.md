# Resilience harness

This Gleam project launches independent, named BEAM operating-system processes against one disposable PostgreSQL cluster and consumes only Grind's public API. Python controls the processes and the TCP streams. The scenarios (M1 to M7, F1 to F5 and the soak), their required observations and the resource bounds are defined in "Verification method" in [the design](../docs/design/design.typ).

## Run

Serialize runs with other gates in the checkout, inside `nix develop`:

```sh
nix develop --command bash scripts/test-resilience.sh
nix develop --command bash scripts/test-resilience.sh --scenarios M1,M4
nix develop --command bash scripts/test-resilience.sh --scenarios '' --soak-seconds 300
nix develop --command bash scripts/test-resilience.sh --deadline-ms 4000 --lease-ms 30000 --soak-seconds 7200
python3 -m unittest discover -s resilience -p 'test_*.py' -v
```

`GRIND_RESILIENCE_OUTPUT` selects a new output directory; an existing directory is refused. The script creates and removes its own cluster, and loopback socket binding must be permitted. The default suite takes two to four minutes; the full soak takes at least two hours plus the fourteen standalone scenarios.

Compare fresh M2 and M6 runs with the pinned Oban run using [oracle/fault_compare.py](../oracle/fault_compare.py) as described in the [oracle guide](../oracle/README.md).

See [docs/BENCHMARKS.md](../docs/BENCHMARKS.md) for the throughput methods that share its provenance rules, and [docs/ORACLE.md](../docs/ORACLE.md) for the independent comparison of the fault scenarios.
