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

M1–M7 are the concrete scenario slots proposed in the review. M6 covers
Grind's concurrent maintenance design; Grind has no leader election. Compare
fresh M2/M6 results with the pinned Oban run using
[oracle/fault_compare.py](../oracle/fault_compare.py), as described in the
[oracle guide](../oracle/README.md). These scenarios classify audited replay
versus Lifeline rescue and leaderless pruning versus Peer failover as intentional
differences; they do not establish equivalence for every fault.

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

`--release-evidence` requires a clean Grind checkout, every short scenario and
a requested soak of at least 7,200 seconds. Separately verify the exact resolved
Sinal dependency: this flag does not establish its cleanliness or identity.
A short rehearsal cannot qualify the two-hour requirement. TLS and pooler
coverage remain separate. The harness never claims exactly-once effects after
an operator explicitly authorizes replay.

## Historical result: 2026-09-28

The two-hour run passed 7,202.060719 mixed-fault seconds after warm-up, fourteen
standalone cases and 266 mixed rounds. Each of nine fault types ran 29–30 times.
Independent review checked source/runtime identity, 7,182 primary jobs, 11,172
receipts, 6,993 effects including warm-up, resource bounds and final cleanup.
Peak sampled database sessions were eight; retained primary storage peaked at
245,760 bytes. Worker atoms grew by 1,596 across 532 consumer starts, within the
explicit allowance but not a claim of bounded atoms under indefinite churn.

These results came from dirty development snapshots before `1e87d2c`; diagnostics
and later Sinal changes were not covered. The final audit used causal barriers
and controller monotonic order rather than comparing independent VM wall clocks.
The paired M2/M6 comparison passed within its deliberate semantic differences.
The earlier 24-hour attempt failed after a 275-second host sleep; none of its
elapsed time counted toward this run. Day-long endurance remains unverified.

The historical raw directories and one-off auditors were removed during the
2026-10-01 cleanup after recording these findings. They cannot be re-audited here.
The old independent auditor was tied to snapshot hashes and temporary driver
paths; it is not a portable qualification command. Review fresh source, duration,
fault/accounting/resource results and process cleanup independently before
recording a new release summary. The maintained harness assertions remain.

Future run artifacts are ignored working files. Keep them through investigation
and review, record a concise result with exact source/dependencies and environment,
then delete them. See [release readiness](../docs/RELEASE-READINESS.md).

Partition modes preserve the reliable byte stream: each direction retains at most
one 64 KiB read and stops reading until recovery, allowing socket backpressure.
Healing forwards those bytes before any later bytes. Only the explicit M4 COMMIT
reply-loss fault blackholes bytes. The earlier byte-discard partition model left
pgo waiting forever for a discarded ReadyForQuery and was rejected after the
stronger M3 drain assertion exposed it; the historical failure is recorded in [recovery evidence](../docs/RECOVERY-EVIDENCE.md).
