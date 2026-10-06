# Changelog

## Unreleased

- Typed, versioned workers and fallible input/output/error JSON codecs behind the grind facade.
- Supervised PostgreSQL runtime, shared application pool, caller-owned transaction admission and optional startup migration.
- Immediate and delayed jobs, durable submission receipts, scoped uniqueness and scheduled-only rescheduling.
- Attempt fencing, independent reserved renewal, retained acknowledgement recovery, retry/snooze accounting and explicit abandonment policies.
- Cooperative cancellation, uncertain-job investigation and idempotent operator resolution.
- Forward schema migrations and bounded terminal retention with dependent receipt cleanup.
- Typed lifecycle observations and operational diagnostics through Sinal.
- Codec round-trip testing helpers, bounded manual drain and independent public consumers.
- Disposable PostgreSQL test and benchmark clusters use their own temporary socket directories.

This checkout has no published release notes to migrate. Material construction
history and original-to-milestone source attribution are recorded in
[the ADRs](docs/adr), especially [ADR-0011](docs/adr/0011-retain-rewritten-history-as-source-provenance.md).
