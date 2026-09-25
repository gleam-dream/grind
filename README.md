# Grind

Grind is a typed background-job library for Gleam on PostgreSQL and Erlang/OTP.
Oban OSS `v2.24.1` is used as a behavioral reference; Grind does not wrap or
embed Oban. The implementation is experimental and has not been published to
Hex.

The current runnable slice includes typed, versioned worker definitions,
heterogeneous registration, PostgreSQL admission and typed result reads,
absolute one-time scheduling, and a supervised queue consumer with bounded
per-consumer concurrency. Its validated policy separates local worker capacity
from the maximum jobs claimed per poll. Attempts use database-time leases,
fenced acknowledgement receipts, and conservative uncertainty recovery.
Business failures support a persisted attempt limit, deterministic default or
definition-bound retry policy, and typed terminal causes; queue handlers can
also snooze with a checked delay. Explicit discard, worker uncertainty, and
cooperative cancellation are implemented. Uniqueness admission
(`submit_unique`/`reconcile_unique`) checks a typed full-input or selected
key against a policy's queue scope, occupancy period, and eligible states
inside one locked transaction, returning a typed handle, an existing
conflict, or a rescheduled conflict; this milestone is complete, including
concurrent admission under a forced barrier, lock contention, period-boundary
timing, live rescheduling, uncertain-commit reconciliation, selected keys, and
public-API consumer coverage — see
[docs/UNIQUENESS-CONTRACT.md](docs/UNIQUENESS-CONTRACT.md) for the full
contract and its remaining, explicitly listed gaps (cross-worker uniqueness,
general field replacement, unique bulk insertion, and a few other named
items). The experimental
v11 schema installs only into an empty schema; earlier experimental markers
(including v10) and partial Grind schemas fail closed without repair.
Acknowledgement receipts retain committed attribution and a proposal fingerprint,
not typed historical proposals. Typed outcome reads return the job's current
result.
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
