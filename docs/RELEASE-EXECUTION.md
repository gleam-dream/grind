# Release execution

Accepted scope: user checklist, 2026-09-27. Starting tree: 8431b61
(after the 6bb2017 module reorganization). The owner authorized a local commit
on 2026-09-28. Pushes, tags and publication remain unauthorized. The Oversight
design remains read-only.

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

## Work and evidence ledger

A focused pass records progress; acceptance still requires the complete gates
and the evidence listed for each item.

| Item | Scope                                                                                                   | Status                                                                                |
| ---- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| 1–3  | Executor ACK, independent reserved renewal, first-ACK rollback recovery                                 | Complete gate and independently audited two-hour soak pass                            |
| 4    | Atomic startup, all failure stages, safe same-settings retry                                            | Nine regressions, independent cache-race probe and full gate pass                     |
| 5    | B1–B10 harness repairs, durable completion and provenance                                               | Repaired composite v4 accepted locally; dirty, uncommitted evidence                   |
| 6    | Independent-node M1–M7, process/node death, DB loss, slow commit, delay/partition, soak, audited replay | 14 standalone cases and 266 mixed rounds pass; final audit and cleanup verified       |
| 7    | Shared paired oracle, cross-result comparator, ledger/source checks and explicit divergences            | Twelve core pairs and fresh-soak M2/M6 fault comparisons pass                         |
| 8    | Operational Sinal diagnostics explaining uncertainty and pressure                                       | Implementation plan prepared; before 1.0                                              |
| 9    | Transaction-scoped enqueue and stronger public testing helpers                                          | API constraints and acceptance cases prepared; before 1.0                             |
| 10   | Rebenchmark after correctness; do not implement batch claims or broader throughput optimization         | Repeated baseline, delayed subsets and matched L7 pair accepted; T3 remains triggered |

Release requires items 1–7. Items 8–9 remain part of the requested trajectory,
but do not block an initial release when their limits are documented.
Cron, Reindexer, automatic Lifeline replay and leader election are excluded
from this release work.

## Verification

Use real PostgreSQL regression/fault witnesses through public operations.
Compile and run focused tests during development; serialize all gates and
builds in this checkout. Full acceptance commands:

- nix develop --command bash scripts/test-postgres.sh
- nix flake check
- nix develop --command bash scripts/bench-postgres.sh
- GRIND_BENCH_T2_STRESS=1 nix develop --command bash scripts/bench-matrix.sh all
- nix develop --command bash scripts/test-resilience.sh --deadline-ms 4000 --lease-ms 30000 --soak-seconds 7200
- nix develop --command bash oracle/run-faults.sh, followed by
  oracle/fault_compare.py against the retained Grind M2/M6 results.

A passing subset is not completion. Record the exact source, configuration,
fault activation, duration, job/effect/receipt outcomes and resource cleanup
for resilience and benchmark results. Preserve historical evidence rather
than replacing it with unverified new claims.

## Current validation record

These are uncommitted exploratory snapshots on 8431b61. No release baseline
or publication is implied. Runtime and harness files changed between the runs;
results apply to their recorded source snapshot, not every later edit.

- The original first-ACK rollback and slow-ACK regressions failed against the
  coordinator-owned path. They pass with attempt-owned ACKs and reserved renewal.
- Nine focused renewal/executor/recovery tests passed in
  `/private/tmp/grind-focused-renewer.log`: the handler ran once across the
  first known ACK rollback; healthy siblings outlived their original leases
  behind slow ACKs; the only ordinary pool connection stayed occupied for eight
  seconds while renewal progressed; coordinator/owner death retained audited
  recovery. The normal-parent-exit regression was red before parent monitoring.
- The first full root gate passed 234 tests and rejected one obsolete 2000ms
  lease fixture under the new minimum. That fixture has been corrected; the
  complete root/consumer/paired gate must pass before acceptance.
- The next full root pass reached 237 passing tests and two failures. A second
  lease fixture used 15 seconds below the new 16-second default minimum. The
  fault-proxy sibling test also measured its retry budget from an earlier point
  now that ACKs progress independently. Both focused regressions now pass. The
  ACK test requires the sibling to finish before the blocked attempt's deadline,
  then observes both typed results through an independent healthy connection
  and rejects a second handler invocation.
- The independent-VM short suite passed all 13 cases in
  `resilience/results/20260928T012210Z-98201/results.json`: M1, M2,
  request-only/reply-only/full M3 partitions, M4–M7, and F1–F4. The artifact
  retains effects, database rows, receipts, node identities and fault witnesses.
  Those original M3 fixtures were later replaced by stream-preserving partitions
  and a required clean drain after healing. The full mixed-fault soak remains
  outstanding.
- M4 initially exposed a proxy defect: cutting COMMIT's Parse response could
  prevent Execute from reaching PostgreSQL. The corrected protocol parser
  waits for Execute; an independent connection then witnessed the durable
  receipt before the paused VM was killed. Its replacement reconciled that
  receipt and produced no second effect.
- Lifecycle churn exposed per-pool caches retained by pinned pg_types/pgo:
  ten consumer starts grew type entries from 557 to 6227. Scoped cleanup after
  pool shutdown held that count at 557. Six focused startup/cache tests passed;
  they preserve live sibling pools and same-settings reopening. The tests now
  witness positive entries in both cache tables before testing cleanup.
- Independent review then reproduced two races that those initial cleanup tests
  missed. A suspended internal pool supervisor let its type server restore 557
  entries after close. Separately, a managed application caller paused at the
  real query-cache insertion wrote an entry after every internal pool process
  had stopped. The reproducible probe is retained in
  `/private/tmp/grind-cache-probe`. The repair must wait for the exact internal
  supervisor and every admitted Grind database call before purging caches.
  The owner rejects new calls while closing, retains its registration and
  deadline during drain, and never kills application callers. A stalled caller
  can make public close return `StopTimedOut`; cleanup then continues until that
  caller finishes or dies. This covers cooperative teardown and failed-start
  unwind. Direct use of the internal raw pog connection must be drained by its
  caller; forcibly killing the lifecycle owner is outside this cleanup guarantee.
- The completed lifetime repair passes all nine startup tests in
  `/private/tmp/grind-startup-lifetime-green.log`, including a real query writer
  held past public close's six-second timeout. The owner remains alive after that
  timeout and cleans up when the caller finishes. The independent green probe
  in `/private/tmp/grind-cache-probe/green-result.log` also proves both original
  races, caller-death cleanup and failure after the registered pool dies before
  its internal supervisor is captured. Every path ends with zero owned cache
  entries while the sibling pool retains its identity, deadline, cache and query
  service. Capture verifies the exact parent PID in pgo's private connection
  supervisor child specification; registered-name ancestry alone is insufficient.
- Churn also found one retained normal EXIT message per stop in callers that
  trap exits. Shutdown now consumes only the stopped supervisor's normal EXIT
  after confirmed termination; timeout keeps the ownership link. The new root
  regression also requires an unrelated child's EXIT to remain available.
- The stronger F5 run in `resilience/results/20260928T014327Z-13412` retained
  exact process/deadline/type/query counts across ten cycles: 104/1/557/60,
  with an empty owner mailbox throughout. The main-pool query cache is warmed
  before this comparison so legitimate first-use statements are not counted as
  leaked reserved-pool entries.
- Strengthening M3 exposed a transport-fixture defect. Discarding bytes and then
  resuming later bytes on the same TCP stream left all five pgo connections
  waiting in `flush_until_ready_for_query`, with PostgreSQL idle and the ACK and
  renewer waiting for checkout. Stacks and database snapshots are retained in
  `resilience/results/20260928T014604Z-14928`. Directional partitions now hold a
  bounded chunk, apply backpressure, and release bytes in order when healed.
  The explicit lost-COMMIT-reply fault remains separate. The stronger three
  partition cases pass with clean drain and no stale receipt in
  `resilience/results/20260928T015041Z-17120`, alongside F5, M2, M4 and M6.
- The repaired benchmark gate passed 32 tests, its 1,000-job audited smoke,
  production query-plan capture, and L3/L5 activation checks. All 50 L3 and 600
  jobs in each L5 arm had independently observed durable completion. Observer
  stop acknowledgements and positive p95 values were checked. Only the enabled
  L5 arm pruned its 10,000 old rows inside the traffic window. Evidence is in
  `/tmp/grind-bench-final.PPZM9r`; this does not establish a matrix verdict.
- Independent Oban M2/Lifeline and M6/Peer-Pruner runs passed in
  `oracle/results/20260928T015525Z-23535`. Its `paired-comparison.json`
  reconstructs and checks both engines' actual effects, death, database,
  receipt/replay and maintenance witnesses. Oban automatically re-executed the
  killed attempt; Grind waited for an attributed replay. The two maintenance
  architectures matched their separate intentional expectations.
- The short soak caught a sampler baseline defect: its first call lazily loads
  runtime introspection code, adding exactly 318 atoms in both long-lived VMs.
  Consecutive samples establish that one-time load before the baseline. The next
  four mixed-fault rounds passed unchanged resource bounds before that rehearsal
  was deliberately interrupted to run the oracle. This is partial evidence,
  not a completed soak (`resilience/results/20260928T015243Z-20520`).
- A later rehearsal reached eight successful fault cases before first-time
  connection-loss handling loaded additional runtime modules and exceeded the
  atom bound. The harness now warms real connection-loss and server-restart
  paths before taking the baseline, with unchanged growth limits. That revision
  still requires a completed rehearsal and the full 24-hour run.
- The complete gate then passed 242 root tests, 11 consumer tests, the pinned
  Oban oracle and all 12 shared core scenarios. It resolved all 55 ledger rows
  and 12 scenario references; comparator and negative-ledger checks passed.
  Core results are in `oracle/results/core-20260928T022327Z-31727`, with
  unchanged source/catalog witnesses and result hashes. Formatting and the
  local platform's flake checks passed. Logs are retained in
  `resilience/results/release-checkpoint-Qnms71` with a source archive. These
  results precede the next checkout repair.
- All 14 standalone resilience cases passed at D4000/L30000 in
  `resilience/results/20260928T022801Z-36374`, but the soak's server-restart
  warmup failed. A confirming trace in
  `resilience/results/20260928T024038Z-53217` showed ACK retries receiving
  already-dead pgo connection owners every ten seconds. Live replacement
  connections existed. The ACK eventually reached one after its lease expired.
  This was a runtime recovery defect, not a reason to relax the soak assertion.
- Two focused regressions reproduce pgo's stale-holder recycling: a dead
  connection owner and a live owner with a closed prior socket. Both returned
  `[false,true,false,false,false,false]` before the repair and pass afterward
  (`/private/tmp/grind-reconnect-red.log`, `/private/tmp/grind-reconnect-green.log`).
  Grind now retires unusable holders before invoking the operation, retaining
  one absolute deadline across candidates. It never retries the operation
  itself or reclassifies an ambiguous commit as uncommitted. Cleanup retires
  sockets that become unusable during a call. TLS and the alternate TCP socket
  backend require bounded local probes because their option lookup can wait on
  another process. Full regression, restart, benchmark and soak acceptance of
  this repair remains pending.
- The extended reconnect module passes all four tests in 2.193 seconds
  (`/private/tmp/grind-reconnect-four-green.log`). Three real stale holders are
  removed before one callback sends SQL; its subsequent `QueryTimeout` remains
  unchanged without another invocation. A debugger pause consumes 1200ms of a
  2000ms checkout budget before a 1500ms statement. The call times out near the
  original deadline, proving that stale-entry removal does not reset the budget.
- The repaired runtime then passed the complete gate: 246 root tests,
  11 consumer tests and 12 paired core scenarios, plus ledger/comparator checks
  (`/private/tmp/grind-full-gate-v4.log`,
  `oracle/results/core-20260928T025504Z-57291`). The paired artifact verifies
  unchanged source and catalog hashes throughout its run.
- An independent real TLS probe passed in `/private/tmp/grind-socket-probe`.
  PostgreSQL's `pg_stat_ssl` confirmed encryption. Suspending the TLS controller
  returned `connection_unavailable` in 801ms with D800, without invoking the
  SQL callback. Killing the caller also removed both probe processes and its
  tracked call. Both cases recovered after controller resume and closed with
  zero owned cache/deadline entries. This proves bounded local TLS preflight,
  not every encrypted network failure. The attempted alternate TCP socket
  backend failed during pgo's type-loader startup: its close path assumes a
  port and calls `unlink` on the socket handle. That dependency limitation is
  retained under the probe's `failed-socket-backend` directory; the default
  backend is the tested configuration.
- A final harness audit found two retention gaps before the long matrix:
  L4 raw filenames omitted the repeat, and matrix cleanup deleted the server
  log. Filenames now include the repeat, cleanup retains `postgres.log`, and
  an explicitly configured unreadable log invalidates the run. The optional
  absence of a log for ad-hoc runs remains distinct. These repairs require the
  next benchmark gate.
- The same audit found that a global slow-ACK activation count could count one
  target repeatedly while another never reached its trigger. Each selected job
  now has its own rollback-proof sequence in the disposable run schema. T2
  requires every target's positive count and records it beside the final job
  outcome. Actual handler/stall overlap remains required. A zero count of
  renewals during a short stall limits renewal-stress evidence; it is not by
  itself a runtime failure. Fault-target uncertainty remains distinct from
  healthy-job uncertainty, including when pool wait consumes part of D.
- The final benchmark gate passed 34 tests, all required database markers,
  its 1,000-job audited smoke, actual L2 plans and L3/L5 activation checks in
  `bench/results/repaired-gate-oy4M9w`. Its source archive and `validation/`
  retain the root gate, format checks, reconnect regressions, lifecycle races
  and independent TLS probes. L3 completed all 50 arrivals; both L5 arms
  completed all 600 jobs, with pruning of 10,000 old rows confined to the
  enabled arm's traffic window.
- Seven T2 profiles passed at D4000/L16000 without network delay in
  `bench/results/t2-smoke-26SU4s`. All 108 healthy jobs succeeded with one
  valid receipt; their minimum observed lease headroom was 10,664ms, above
  the 1,600ms threshold. Every one of the 42 fault targets activated:
  21 below-deadline targets succeeded and 21 above-deadline targets became
  uncertain without receipts. All six fault profiles observed renewal during
  slow ACKs, including C50 with a main pool of ten. Two independent audits
  checked the 150 outcomes, exact profiles, source archive and driver hashes;
  this single-repeat subset does not replace the full matrix.
- The repaired independent-node rehearsal passed all 14 standalone cases and
  18 mixed rounds in `resilience/results/repaired-300s-jBvw19`. Every fault
  type ran twice; mixed traffic continued for 485.61 seconds after warm-up.
  Each round retained 27 terminal jobs, 42 unique receipts and 26 effects
  before pruning; the primary VMs recorded 545 effects including warm-up.
  Both VMs stayed at 104 processes, one deadline entry and 797 type entries;
  query entries remained 21/22 and owner mailboxes stayed empty. Worker atoms
  increased by exactly six per round for two consumer starts. All fixed
  memory, session and storage bounds passed. The source and driver hashes
  remained unchanged, and the independent audit verified all 600 runtime
  files. This rehearsal proves recovery from the previously failing restart
  path; the 24-hour mixed soak remains required.
- The final M2/M6 comparison in that rehearsal's `paired-comparison.json`
  passes against the pinned independent Oban run
  `oracle/results/20260928T015525Z-23535`. It verifies Grind's quarantine,
  attributed replay and new attempt fence against Oban's automatic Lifeline
  rescue, and Grind's concurrent locked-row pruning against Oban's Peer
  failover. These are two classified intentional differences, not equivalent
  rescue timing or a general exactly-once claim.
- A complete artifact audit also found that L1 and L7 raw filenames omitted
  the repeat. Their filenames now include it; L1 sampler and statement labels
  use the same repeat-specific label. The pending full matrix must retain
  every repeat's raw evidence. Documentation now describes separate claim/ACK
  telemetry producers, one absolute checkout deadline, cooperative pool drain
  and the limited scope of private-dependency canaries.

### Repaired benchmark composite — 2026-09-28

- The final benchmark gate passed 38 tests, its 1,000-job audited smoke and
  L2/L3/L5/L7/profile activation checks in `bench/results/repaired-gate-garghm/`;
  the parent terminal witness records session 54069 exiting 0. A deliberate
  1ms drain timeout in `bench/results/drain-timeout-negative-2w6b1o/` exited 1 as
  expected and retained three sampler records, observer shutdown acknowledgement
  and final counts: 9,000 submitted, seven durably observed/succeeded, two
  executing and 8,991 queued. The observed drain interval was 26ms. The original
  checker failed on stale log wording; `negative-check-v2.json` records the
  narrow correction and accepts the retained timeout evidence. Both records
  remain. This is a negative harness check, not a successful performance arm.
- The full composite audit passed with no issues. Its parent-captured terminal
  witness records session 6734 exiting 0. Evidence is retained in
  `bench/results/l7-drain-pair-20260928T091612Z/composite-audit-v4.json` and its
  composite manifest, with the original-source results in
  `bench/results/exploratory-matrix-LZVevz/`. Both source trees are dirty,
  content-pinned exploratory snapshots on 8431b61; no commit or publication is
  implied.
- The original wrapper exited 1 on delayed-L7’s 60-second drain bound. That
  failure remains retained. After the reviewed ten-file benchmark-only drain
  repair, a fresh zero/5ms pair completed every main and diagnostic profile
  with an explicit 600-second soft budget. The composite verifies unchanged
  production code, migrations, manifest/toolchain inputs and archived Sinal
  inputs across the snapshots. The failed delayed arm remains failed; the fresh
  pair replaces the L7 comparison, while unaffected profiles retain their
  original evidence.
- The old baseline, delayed L3 and resumed delayed T2 all pass complete checks.
  Three exact L2 setup/cleanup lock waits and associated autovacuum
  cancellations were separately adjudicated against pinned logs, plans,
  emitted rows and source phases. The original whole-log rejection remains
  visible; no general log exception or claim of zero CPU/cache influence is
  made.
- All 1,254 healthy T2 siblings succeed with valid receipts and headroom above
  L/10. Each of 576 fault targets activates; 288 succeed and 288 become uncertain.
  The legacy aggregate trigger includes these fault-target quarantines and
  must not be read as healthy-sibling starvation. T1 records no trigger.
- T3 remains triggered: fresh 1×C50 throughput is 42.64% of 5×C10 without injected
  delay and 20.09% with 5ms per direction per TCP chunk. Main pools are 50 at
  every shape, but reserved renewal connections make total Grind
  connections 51/55/60. These results do not establish a controlled improvement
  over the historical run or resolve throughput. Batch claiming remains
  deferred. See the current appendix in
  [PERFORMANCE-EVIDENCE.md](PERFORMANCE-EVIDENCE.md).
- B1–B10 and rebenchmark item 10 have accepted local evidence. A subsequent
  formatter pass rewrote 17 retained JSON files. Nine were restored byte-for-byte
  against pre-existing pins; eight unpinned files retain their formatted bytes.
  Both variants and the incident record remain. The unchanged composite auditor
  passed again with no issues (session 66728, exit 0). Result directories are now
  excluded from formatting; the final verification records their byte preservation.

## Approved soak duration, 2026-09-28

The owner approved a fresh two-hour mixed-fault soak (7200 seconds) as the
release acceptance target. This supersedes the earlier 24-hour target in
historical entries above. The original checklist requires a soak but does not
prescribe its duration; the lease protocol does not require a full day.
Day-long endurance remains unverified.

The accepted run passed all 14 standalone cases and repeated all nine mixed
faults at least 29 times. Fault activation, durable completion, effect, receipt,
fencing, audited replay and resource assertions remained unchanged. The timed
interval began after warm-up. Its actual terminal success, source/runtime
provenance and independent final audit are retained below.

The previous 86400-second attempt remains failed in
`resilience/results/repaired-86400s-o23a23`. All 14 standalone cases and 162
complete mixed rounds passed before a 275-second host software sleep spanned
a healthy job's lease expiry. That job became `uncertain` with one effect and
no receipt or replay. This is consistent with fencing after host suspension;
it is not a completed soak or evidence for relaxing the assertions. Its actual
exit status, raw data and independent diagnosis are retained under that root.
The fresh run is recorded separately in
`resilience/results/repaired-7200s-8YvcJq` with its own source snapshot. No elapsed
time from the failed attempt counts toward its accepted duration.

### Accepted two-hour evidence

The [final audit](../resilience/results/repaired-7200s-8YvcJq/soak-audit-v5.json)
passed after the workload's actual session 7230 exited 0. The audit's separate
session 97027 also exited 0. It verifies 278 archived inputs and 600 frozen
runtime files against the completed run. The wrapper source SHA-256 is
`b15a8da75e7213928bcc31fc0ed00efd790107f56e3e5ca04cf8d3e739d6a6f7`.
Subsequent changes affect documentation, formatter exclusions and trailing
whitespace in the resilience package configuration only.

- The mixed interval lasted 7,202.060719 seconds after warm-up. Independent
  controller-monotonic bounds are 7,202.058878–7,202.105858 seconds; the lower
  bound exceeds the approved 7,200 seconds. All 14 standalone cases and 266
  mixed rounds passed. Node kill, worker kill and the three partition modes
  ran 30 times each; lost COMMIT reply, slow ACK, connection loss and database
  restart ran 29 times each.
- The primary rounds account for 7,182 jobs, 11,172 receipts and 6,916 effects,
  plus 77 warm-up effects. Every held healthy primary job completed once across
  its nested fault. Both primary VMs retained their identity for the full run.
  Every nested fault's activation, outcomes, fencing and cleanup marker passed.
- Post-drain samples for both VMs stayed at 104 processes, one deadline entry
  and 797 type-cache entries.
  Query-cache counts stayed at 21/22, and sampled mailbox counts stayed zero.
  Maximum memory above baseline was 421,328 bytes for admin and 569,960 bytes
  for worker. Worker atoms grew by 1,596 across 532 fresh consumer starts,
  exactly the documented three atoms per start; admin atoms did not grow.
  Maximum database sessions were eight; retained primary relation storage was
  245,760 bytes. Sampling occurs after drain; timers and the forwarder mailbox
  are not measured separately, and indefinite atom churn remains a risk.
- The [fresh M2/M6 comparison](../resilience/results/repaired-7200s-8YvcJq/paired-comparison.json)
  passed against pinned Oban 2.24.1. Grind required attributed replay after an
  unknown effect; Oban Lifeline rescued automatically. Concurrent Grind pruning
  and Oban Peer failover/pruning met their separate declared expectations.
- Independent and parent cleanup checks found no owned process or disposable
  cluster directory. PostgreSQL recorded completed immediate shutdown. This
  verifies cleanup, without claiming every BEAM exited gracefully.

Auditor v4 initially rejected round 233 because it compared the worker VM's
wall time with the controller's wall time. Round 234 had the same discrepancy.
The retained release files and frozen code establish that each primary callback
waited until its nested fault and cleanup completed. Two reviewers and the parent
verified v5's narrow replacement: exact release-path and callback identity,
controller-monotonic ordering, clean stop and durable outcomes. Its analogous
replay check uses the pre-replay effect count, attributed resolution and new
attempt fence. No timing tolerance was added. V4, its failure, the exact v4→v5
diff and both audit terminal witnesses remain under the run's `orchestration/`.

Items 1–7 now have accepted local implementation and validation evidence.
These uncommitted dirty snapshots remain exploratory, not a clean release
candidate. Day-long endurance, wider deployment coverage and the before-1.0
items below remain outside this result.

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

Operational diagnostics should be separate from commit-proven lifecycle events.
Use typed, payload-free Sinal descriptors for renewal observations, ACK retries,
claim failures and local capacity transitions. A skipped row proves contention,
not the identity of its locker; a missing live fence does not by itself prove
lease expiry; renewal-budget exhaustion does not prove quarantine. Measure
checkout wait separately from total storage-call duration. Preserve the existing
bounded forwarder and durable `acknowledged(Reconciled)` contract.

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
