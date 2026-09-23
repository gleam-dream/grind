# Grind

Grind is a typed background-job library for Gleam on PostgreSQL and Erlang/OTP.
Oban OSS `v2.24.1` is used as a behavioral reference; Grind does not wrap or
embed Oban. The implementation is experimental and has not been published to
Hex.

The current runnable slice includes typed, versioned worker definitions,
heterogeneous registration, PostgreSQL admission and typed result reads,
absolute one-time scheduling, and a supervised serial queue consumer. A queue
policy validates its polling interval and maximum jobs per poll. That batch cap
is a throughput setting; it does not raise the consumer's concurrency above one.
See [implementation scope](docs/IMPLEMENTATION-SCOPE.md) for the delivered
boundary and complete retained backlog.

## Development and integration checks

Run all checks in a fresh local PostgreSQL cluster with separate databases for
Grind, the pinned Oban harness, and the public-import consumer:

```sh
nix develop --command bash scripts/test-postgres.sh
```

The script removes its disposable cluster on exit and refuses to use an
occupied test port. Plain `gleam test` runs pure tests and skips database tests
when their explicit test URL is absent; the script requires database markers so
those skips cannot count as integration passes.

The pinned oracle source, commit, licenses, normalized observations, deliberate
differences, and per-behavior evidence categories are recorded in
[oracle/ORACLE-LEDGER.md](oracle/ORACLE-LEDGER.md). The separate
[consumer package](consumer/README.md) imports only public Grind modules.
