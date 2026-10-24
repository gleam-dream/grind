# State driver deadline and installation limits explicitly

<a id="adr-0008"></a>

- **Decision.** Retain the pinned pog/pgo FFI adapter and its absolute deadline/callback-at-most-once contract, with explicit queue-wait and deployment limits. Installation client tokens prevent accidental misuse; schema/role privileges provide isolation.

- **Rationale.** Grind needs bounded operation ownership and scoped cleanup unavailable through the public driver surface. It therefore depends on private pool/single connection shapes, connection records, cache tables and topology. A narrow dependency upgrade can change these without a compile error.

- **Alternatives.** Unbounded default checkout cannot provide the advertised storage boundary. A universal driver replacement is a separate implementation choice. Treating optional cluster identity fallback as authentication or global equality overstates the token.

- **Evidence and history.** 9ccbfb57dd01fe736632e2463df14225b383928f preserves app search_path and handler pool. Current src/grind_postgres_ffi.erl computes expiry before pgo_pool:checkout but pgo 0.20 has no queued receive timeout; timer arms after transfer. Deadline review is source-derived, not an executed new runtime test. RISKS documented private dependency shapes, upstream rollback-crash window, alternate socket-backend failure, untested poolers and atom growth. Exact original acceptance dates are unknown. pog >=4.1.0 <4.2.0 and pgo >=0.20.0 <0.21.0 are recorded ranges, not evidence of private-API stability.

- **Source revisions.** [9ccbfb57](https://github.com/gleam-dream/grind/commit/9ccbfb57dd01fe736632e2463df14225b383928f).
