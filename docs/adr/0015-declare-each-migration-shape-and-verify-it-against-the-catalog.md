# Declare each migration's complete cumulative shape and verify it against the catalog

<a id="adr-0015"></a>

- **Decision.** Each source migration entry declares the complete cumulative set of grind-prefixed relations its schema version contains, including every implicit object, plus any column-level key columns, and it lists every foreign-key constraint by name. The schema probe compares the declared shape with the actual relation catalog and the declared foreign keys with the constraint catalog, exactly. A relation that exists but is not declared fails closed like a missing one.

- **Rationale.** A schema marker alone cannot prove physical compatibility. An exact comparison rejects partial, foreign and tampered storage. A foreign key backed by no index of its own never appears in the relation catalog, so it needs its own check against the constraint catalog.

- **Alternatives.** Deriving the shape from the DDL text undercounts implicit objects such as the sequence of a bigserial column and the index behind each primary key or unique constraint. Accepting extra relations would adopt a foreign or stale schema. A marker-only probe would accept either.

- **Evidence and history.** The first shape declaration of version 11 undercounted implicit objects, and the exact comparison surfaced it. A new version therefore confirms its relation set on a database where the version was applied. The probe and the declarations are in src/grind/internal/postgres/schema_probe.gleam and src/grind/internal/migrations.gleam.
