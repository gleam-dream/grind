# Validate forward migrations and bound receipt retention

<a id="adr-0007"></a>

- **Decision.** The source migration list is authoritative and mirrored byte-for-byte by priv migrations. Each forward step locks, rereads markers, applies DDL and validates exact shape before commit. Terminal pruning cascades admission, ACK and resolution receipts; Uncertain is excluded.

- **Rationale.** Schema marker alone cannot prove physical compatibility. Per-step transactions allow interrupted upgrades to resume without discarding committed earlier work. Cascading receipts makes retained uniqueness and command-recovery lifetimes explicit rather than permanent tombstones.

- **Alternatives.** Automatic destructive reinstall of experimental schemas would risk data. A marker-only probe could accept partial or foreign storage. A global pruner leader is unnecessary when each bounded deletion uses SKIP LOCKED. Permanent deduplication belongs to a business ledger.

- **Evidence and history.** 72fe573db3f83174ea5751ec201ac9dd0b3072e8 closed Cigogne and genuine upgrade lost-reply gaps. Current versions 11–13 and exact-shape probes: src/grind/internal/migrations.gleam, postgres/migration.gleam, postgres/schema_probe.gleam, priv/migrations and test/grind/migrations. Earlier version-11 fresh-install-only narrative is superseded by supported forward upgrade steps; version 10 remains experimental reinstall.

- **Source revisions.** [72fe573d](https://github.com/gleam-dream/grind/commit/72fe573db3f83174ea5751ec201ac9dd0b3072e8).
