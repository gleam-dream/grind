# Keep generated static SQL and hand-written dynamic SQL as a permanent split

<a id="adr-0014"></a>

- **Decision.** A query whose SQL text is fixed at compile time lives in its own file under src/grind/internal/sql and is generated into sql.gleam by Squirrel against a disposable database. SQL that varies by call stays hand-written in grind/internal/postgres and its collaborators: shared lease, period and lock predicate fragments spliced into more than one query, per-disposition acknowledgement SQL, nullable-parameter queries whose bound value shape varies, and candidate selection. The generated module is replaced wholesale and never edited by hand.

- **Rationale.** Squirrel generates a binding from one static SQL string. Dynamic predicates depend on scope, period, conflict action and the proposed state, so generating them would require one file per variant or would hide the variation. Generated static queries keep their parameter and row types checked against the real schema.

- **Alternatives.** Writing every query inline gives up generated type checking for the static majority. Enumerating every dynamic variant as a file multiplies near-duplicate SQL. A query-builder dependency adds a second abstraction over one database. The split is a permanent design and not a migration in progress.

- **Evidence and history.** scripts/generate-sql.sh regenerates the module, and scripts/test-postgres.sh checks the generated bindings against a schema built from the migration mirrors. The original adoption date is not established by the current Git history.
