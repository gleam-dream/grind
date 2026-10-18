# Preserve full job scope and qualify each oracle claim

<a id="adr-0001"></a>

- **Decision.** Grind remains an independent typed queue. Retain the complete Oban-inspired core and Pro capability families; label current implementations and unresolved contracts separately. Oban 2.24.1 OSS is the frozen behavioral reference. Pro features need original observable contracts or an available licensed oracle.

- **Rationale.** A finite facade can establish durable admission and fenced execution without discarding richer requirements. General parity would obscure intentional receipt, equality and retry differences and capabilities absent from the comparator.

- **Alternatives.** A thin Oban wrapper would tie the runtime to Elixir and its representations. Claiming only the current facade as the whole intended design would lose priorities, live controls, global/rate/partition limits, schedules, plugins, dependency workflows, batches, chunks, recorder, relay and testing requirements.

- **Evidence and history.** The original design notes, API coverage rows and execution and uniqueness contracts are maintained outside this repository. oracle/ORACLE-LEDGER.md and src/grind.gleam hold the retained evidence. Original scope discussion dates are not established by the current Git history.

- **Retained reference.** docs/ORACLE.md states the pin, the comparison scope, the classification and the deliberate differences, and links this decision and ADR-0019.
