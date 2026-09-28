# Resilience acceptance harness

This package launches independent, named BEAM operating-system processes against
one disposable PostgreSQL cluster. It consumes Grind's public API. Python controls
the processes and real TCP streams; it does not simulate node identity inside one
VM. A failed assertion fails the run.

The worker appends and fsyncs each synthetic effect before it reaches a file
barrier. Before destructive faults, the controller also requires an acknowledged
`effect_synced` flag that is set only after `fsync` returns; a visible log line
alone is insufficient. Killing a worker or VM after that witness leaves an effect independently
of Grind's database transaction. `handler_finished` is a separate event. Only a
persisted terminal state and receipt count establish durable completion.

## Run

Serialize this build with other gates in the checkout:

```sh
nix develop --command bash scripts/test-resilience.sh
nix develop --command bash scripts/test-resilience.sh --scenarios M1,M4
nix develop --command bash scripts/test-resilience.sh --scenarios '' --soak-seconds 300
nix develop --command bash scripts/test-resilience.sh --soak-seconds 7200
python3 -m unittest discover -s resilience -p 'test_*.py' -v
```

`GRIND_RESILIENCE_OUTPUT` selects a new output directory. Existing directories are
refused so a new run cannot overwrite earlier evidence. The script creates and
removes its own cluster; it does not touch another database. F4 additionally
checks the disposable cluster path before it can stop PostgreSQL. Loopback socket
binding must be permitted by the execution environment.

The default suite uses D=1500 ms and L=12000 ms and is expected to take roughly
two to four minutes, including waits for genuine database-time lease expiry.
Those are harness development settings. Run the real D=4000/L=30000 profile with
`--deadline-ms 4000 --lease-ms 30000`; expiry-heavy cases will take longer.
The full soak command takes at least two hours plus the fourteen standalone
scenarios. Shorter runs test the harness and cannot establish the two-hour release
requirement. Every soak runs all nine fault types at least twice, even when the
requested duration is shorter. A complete short rehearsal therefore takes roughly
five minutes.

## Scenario contracts

| ID   | Controlled boundary                                                                                                                   | Required observation                                                                                                                                                                                                           |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| M1   | Two BEAM VMs consume one queue with two slots each.                                                                                   | Both VMs execute; local callback concurrency stays at or below two; each effect and durable receipt occurs once.                                                                                                               |
| M2   | SIGKILL a VM after its fsynced effect, before callback return.                                                                        | Survivor quarantines the job; no implicit replay; explicit public `AuthorizeReplay` writes an attributed audit row and permits exactly one additional effect.                                                                  |
| M3   | Request-only, reply-only and bidirectional partitions of one VM while another VM remains healthy.                                     | Fault bytes were held with bounded backpressure; the healthy sibling completes; expired work becomes uncertain; after healing, the old callback returns, public stop confirms clean drain, and no stale receipt was committed. |
| M4   | Forward COMMIT, suppress its reply, pause and then kill the VM.                                                                       | A separate database connection observes the committed receipt while the VM is paused; a fresh VM resolves that receipt with public reconciliation; no duplicate effect.                                                        |
| M5   | v1 work remains active while a v2 worker/codec VM joins.                                                                              | v2 work completes with its own node; killing v1 leaves an old-version row that v2's quarantine scan still sees; no implicit replay.                                                                                            |
| M6   | Two VMs prune concurrently while another connection holds a terminal row lock.                                                        | Both prune calls finish, their total is the unlocked candidate count, the locked row remains, and a later sweep removes it and its receipt.                                                                                    |
| M7   | Graceful drain, grace-expired shutdown, then replacement with more capacity.                                                          | Clean drain finishes durably; forced shutdown reports active work; replacement quarantines the abandoned attempt; audited replay is explicit.                                                                                  |
| F1   | A real PostgreSQL trigger delays ACK updates while a long sibling needs renewal; 5 ms per-direction TCP delay also applies.           | `pg_stat_activity` witnesses `PgSleep`; proxy witnesses delayed bytes; all jobs reach durable success. This is a resilience probe, not the complete B10 parameter matrix.                                                      |
| F2   | Close the proxied connections and terminate real PostgreSQL backends.                                                                 | Connection cuts and successful backend termination are recorded; the long job survives reconnect across a lease interval without another effect.                                                                               |
| F3   | Kill the application worker process inside a live VM after its effect.                                                                | The same quarantine and audited-replay assertions as M2 hold.                                                                                                                                                                  |
| F4   | Stop the disposable PostgreSQL server immediately, then restart it during a live attempt.                                             | Stop/start commands succeed; measured downtime is recorded; the job completes once after reconnect.                                                                                                                            |
| F5   | Ten consumer start/stop cycles while a sibling VM remains live.                                                                       | Exact deadline/type/query cache counts return to baseline, the owner mailbox stays empty, and sibling jobs continue completing.                                                                                                |
| Soak | Repeated normal/retry/snooze batches, queued and running cancellation, lifecycle churn, pruning and all nine destructive fault types. | Exact attempt/effect/receipt accounting; every fault runs at least twice; a long primary job survives each fault without duplicate effects; every round passes the resource bounds below.                                      |

M1–M7 are the concrete scenario slots proposed in the review, not names borrowed
from the unrelated archived-history milestones. M6 covers Grind's concurrent
maintenance design; Grind has no leader election. The retained
[M2/M6 comparison](results/repaired-7200s-8YvcJq/paired-comparison.json) passed
against the pinned independent Oban run in
`../oracle/results/20260928T015525Z-23535`. It checks Grind's audited replay
against Oban Lifeline rescue, and Grind's locked-row concurrent pruning against
Oban Peer failover followed by maintenance. These are classified intentional
differences. No Cron or automatic Lifeline behavior is introduced in Grind.

## Evidence and limits

Each run retains:

- Source commit, dirty flag, source/dependency content digest, frozen compiled runtime
  snapshot and per-file binary hashes, scenario parameters,
  OTP and PostgreSQL versions. Dirty runs are labeled exploratory.
- Every BEAM OS PID and actual distributed node name, configured concurrency and
  main-pool capacity, command replies and logs.
- Fsynced per-VM effects and handler-finish events, explicit fault activation
  witnesses, directional held/released byte totals, COMMIT visibility and OS death confirmation.
- Complete job, acknowledgement receipt and audited resolution rows for each
  scenario, including failed scenarios when the database remains reachable.
- Runtime processes, atoms, total memory, mailbox messages and deadline entries;
  soak relation sizes and database session counts; final scenario pass/fail results.

The initial thirteen-case short run passed in
`results/20260928T012210Z-98201` (about 155 seconds). The stronger M3 drain assertion,
post-fsync acknowledgements, F5 mailbox assertion and mixed soak were added after
that run and require their own execution evidence. Earlier F5 runs reproduced
unbounded dependency type-cache retention and one trapped normal EXIT per stopped
consumer; those findings drove the scoped cache and confirmed-stop cleanup fixes.

The soak keeps its primary admin and consumer VMs alive throughout. Each round
runs eight normal, eight retry-once and eight snooze-once jobs, plus one queued
and one running cancellation. It checks business-attempt/snooze counts and all
41 receipts for that batch. A separate held primary job keeps renewing during an
independent fault subcase. The fault rotation is VM kill, worker-process kill,
request/reply/full partition, lost COMMIT reply, slow ACK with delay, connection
loss, and PostgreSQL restart. Every subcase retains its complete audit before its
disposable rows are truncated. One schema per fault type is migrated before the
long-lived VMs start and reused, keeping relation OIDs and catalog size stable
across reconnects. Each primary batch retains its database rows before
pruning. Effects remain in fsynced per-VM files.

After three warmup batches, asserted recovery from one backend loss and server
restart, and two initial resource samples (the first warms Erlang memory
introspection), every drained round applies these bounds to both
long-lived VMs; a failed bound fails the soak:

- Process count: at most baseline plus four; total memory: at most baseline plus
  64 MiB. These are fixed ceilings, not allowances proportional to duration.
- Deadline entries and PostgreSQL type-cache entries: exact baseline. Query-cache
  entries: at most baseline plus four for reconnect warmup. Synthetic worker ETS
  entries: zero after drain.
- Owner mailbox: empty; total process mailbox lengths: at most baseline plus 32.
  Sampling does not drain messages or erase caches.
- Atom count: baseline plus three per consumer start and 64 warmup atoms. The
  three registration atoms per start are a known runtime lifecycle cost and are
  reported explicitly; this bound does not claim atoms can be reclaimed.
- Database connections: at most the drained baseline plus two; the primary
  schema's retained table/index/TOAST storage after prune and vacuum: at most
  16 MiB. Job and receipt counts must be zero after each prune.

`--release-evidence` requires clean pinned source, every short scenario and a
requested soak of at least 7,200 seconds. Dirty runs remain useful exploratory
evidence. A short rehearsal cannot establish the two-hour requirement. The retained
M2/M6 comparison covers only those two intentional differences; broader
cross-engine fault pairing, TLS and pooler coverage remain separate. The harness
never claims exactly-once effects after an operator has explicitly authorized
replay.

The owner changed the prospective duration requirement from 24 hours to two hours
on 2026-09-28. All scenario assertions, fault counts, fencing checks and resource
bounds remain unchanged. The failed 86,400-second attempt in
`results/repaired-86400s-o23a23/` remains failed and retained; its elapsed time does
not count toward the required fresh run.

The fresh [two-hour audit](results/repaired-7200s-8YvcJq/soak-audit-v5.json)
passed: 7,202.060719 mixed seconds after warm-up, 14 standalone cases and 266
mixed rounds, with each fault repeated 29–30 times. Its source/runtime provenance,
7,182 primary jobs, 11,172 receipts, 6,993 effects including warm-up, resource
bounds and final cleanup were independently checked. The results remain
exploratory because the source tree was dirty. Day-long endurance is unverified.

The first auditor rejected a cross-runtime wall-clock comparison. The retained
v5 correction proves primary overlap through the exact file barrier, matching
callback identity and controller-monotonic order; replay uses explicit
authorization and fence lineage. It does not assume that independent BEAM and
controller wall clocks agree. Both audit versions and the failed first result
remain available under the run's `orchestration/` directory.

Partition modes preserve the reliable byte stream: each direction retains at most
one 64 KiB read and stops reading until recovery, allowing socket backpressure.
Healing forwards those bytes before any later bytes. Only the explicit M4 COMMIT
reply-loss fault blackholes bytes. The earlier byte-discard partition model left
pgo waiting forever for a discarded ReadyForQuery and was rejected after the
stronger M3 drain assertion exposed it; diagnostic artifacts retain that failure.
