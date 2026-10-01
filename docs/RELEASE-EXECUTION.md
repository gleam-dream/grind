# Release implementation

Runtime isolation and harness repairs landed in `1e87d2c`; operational diagnostics
landed in `75e50ae`. This document records the accepted design and remaining
scope. [RELEASE-READINESS.md](RELEASE-READINESS.md) is the current qualification
checklist. Superseded execution logs are available in Git history.

## Contracts

The owner approved attempt-owned acknowledgements, independently progressing
renewal protected from ordinary pool saturation, retry of the first known
ACK rollback, and atomic failed-start cleanup. The existing attempt ID,
epoch, owner, executing-state and live database-time lease fences remain
authoritative. Unknown commits reconcile the same command/proposal; no
automatic path may re-execute an uncertain effect. Governing existing
contract: ../oversight/grind-design.md, section 8, "Attempt outcomes and
ownership".

Implementation responsibilities:

- The coordinator owns local capacity, claim/refill and bounded shutdown.
  A finished but unacknowledged attempt still occupies capacity.
- A supervised temporary attempt executes its handler once and owns its
  acknowledgement/reconciliation retries. Manual calls retain their explicit
  unknown-result API; automatic calls retain proposals on known rollback
  and ambiguous results alike.
- One independently progressing renewer per consumer batches live ownership
  updates on a reserved one-connection pool. Claims, admissions and ACKs
  cannot use that pool. Renewal skips rows locked by other transactions,
  so a slow ACK cannot block renewal of unrelated attempts.
- Renewal tracks running and returned/pending attempts. Pending completion
  has a bounded renewal lifetime; after that lifetime, receipt reconciliation
  remains possible but an expired fence never grants a fresh write.
- Consumer supervision owns its reserved pool and renewer, and kills them
  on shutdown/restart. Database startup owns every resource it acquires
  and unwinds them before returning an error.
- Lease validation covers the independently bounded renewal operation and
  its cadence, without multiplying storage deadlines by worker concurrency.
  Database outages, row contention on the same attempt, and unbounded OS
  scheduling remain failures handled by fencing/quarantine, not a claim of
  unconditional liveness.

## Delivered work

| Work                                                | Implementation and validation scope                                                                                                                       |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Attempt-owned ACKs and independent reserved renewal | First-ACK rollback recovery, same-command reconciliation and fenced writes; root regressions and historical slow-ACK matrix.                              |
| Resource lifetime                                   | Failed-start unwind, scoped cache cleanup, tracked-call drain and stale-holder recovery; startup/reconnect regressions.                                   |
| Benchmark repairs B1–B10                            | Validated arrivals, per-attempt observations, durable completion, repeat-specific outputs, explicit budgets and resource counts. See the benchmark guide. |
| Independent-node faults and soak                    | M1–M7, F1–F5, directional partitions and all nine mixed fault types. Historical two-hour run passed; fresh final-dependency qualification remains open.   |
| Oban comparison                                     | Twelve shared core scenarios, ledger checks and separate M2/M6 fault comparison.                                                                          |
| Operational diagnostics                             | Six typed bounded events, native codecs and fault/subscriber isolation tests. See OPERATIONAL-DIAGNOSTICS.md.                                             |

Historical performance is summarized in [the benchmark guide](../bench/README.md)
and resilience in [the resilience guide](../resilience/README.md). On 2026-10-01,
the owner requested removal of generated results after summarization. Raw run
directories and their one-off audit scripts were removed; the runnable harnesses,
scenario catalogs, regression tests and source-controlled comparators remain.
Historical summaries do not establish qualification of today's dependency set.

## Approved soak duration, 2026-09-28

The owner selected a fresh two-hour mixed-fault soak after warm-up, retaining all
fault/accounting/resource assertions. This supersedes the earlier 24-hour target.
The accepted historical run covered 7,202.060719 seconds, 14 standalone cases
and 266 mixed rounds. The preceding long attempt failed after host suspension;
none of its elapsed time counted toward the successful run. Day-long endurance
remains unverified. Future qualification uses the commands and acceptance checks
in [RELEASE-READINESS.md](RELEASE-READINESS.md).

## Lease timing

The minimum is `L >= 4D`, independent of concurrency, where `D` is the configured
storage deadline. Renewal arms its next `L/3` timer before querying. Even allowing
one claim-reply delay, a full interval, and the next renewal operation, bounded
storage time uses at most `2D + L/3`; at the minimum this leaves `2D/3` of margin.
This calculation assumes the reserved connection is established and progressing.
Initial connection/reconnection, actor handoff, mailbox work and OS scheduling
are outside that algebra. In particular, pgo installs its absolute deadline timer
only after checkout transfers the connection; waiting for an unavailable pool is
not unconditionally bounded by D. Fencing and quarantine remain the outage path.

## Before 1.0 follow-ups

Operational diagnostics are complete in `75e50ae`. They explain runtime outcomes
without changing the commit-proven lifecycle observation contract.

Transaction-scoped enqueue must separate admission statements from transaction
ownership: calling the existing submission transaction inside a caller transaction
would issue nested BEGIN/COMMIT and could commit application changes prematurely.
A prepared request must survive a lost COMMIT reply; staging must not emit a
committed observation or claim durable admission. Confirm the admission receipt
on an independent connection after the caller commits. An old matching receipt
proves admission only, never the current surrounding business transaction's commit.
The remaining API choice is a checked borrowed connection versus a Grind-managed
transaction callback that owns the bounded checkout. Public testing helpers should
cover typed enqueue inspection, deadline-based committed-outcome waits, bounded
manual drain and codec/policy execution, with consumer-package acceptance tests.
