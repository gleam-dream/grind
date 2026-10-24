# Configure the installation schema explicitly instead of inferring it from the connection

<a id="adr-0017"></a>

- **Decision.** The installation is identified by an explicit schema: grind.new selects public and with_schema overrides it. Every storage call sets search_path to exactly that schema and restores the caller value before the connection returns to the pool. The uniqueness advisory lock binds the configured schema as a parameter. grind.migrate creates the schema only after confirming that it is absent, and no other call creates a schema.

- **Rationale.** The PostgreSQL default search_path can make current_schema identify the empty personal schema of a role while Grind tables are read from public. Deriving installation identity from that value would give two roles different identities for the same tables. An explicit schema is the same for every role and agrees with the schema every other query reads.

- **Alternatives.** Inferring the schema from current_schema fails as described. Scoping rows by an owner column inside a shared schema was superseded: version 12 removes the storage_owner column, so a schema is the whole unit of isolation. Leaving search_path to the pool breaks application queries that rely on their own search_path. Unconditional schema creation demands database-level CREATE privilege even when the schema exists.

- **Evidence and history.** test/grind/database/isolation_test.gleam exercises the personal-schema case and two schemas in one database. A pooler in transaction mode can attach a client to a connection without its startup parameters, so deployments keep session pooling or qualify their pooler.

- **Example.** The isolation topic in examples/usage runs two schemas in one database and a handler that writes the application tables through the shared pool.
