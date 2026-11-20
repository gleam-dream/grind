# Declare the settlement of abandoned jobs on the worker

<a id="adr-0021"></a>

## Decision

- A worker may declare a settlement with `worker.with_settlement`: a function of an open transaction and the abandoned job that returns ConfirmSuccess, ConfirmFailure, AuthorizeReplay or LeaveUncertain, or an error to ask for a later try.
- When Grind holds a job uncertain because an attempt ended without an acknowledgement and the job is not replayed (lease expiry with no replay left or with cancellation pending, or a handler timeout under HoldUncertain), it writes a settlement mark naming that attempt. A handler's own Uncertain response writes none. The mark is written whether or not any worker declares a settlement.
- The settler of a queue consumer whose registry holds the exact worker version takes a due job in a committed statement that moves its due time later and counts the try. It then locks the job, decodes its input and runs the function in one Grind-owned transaction, under the application's search_path. It applies the decision through the existing resolution protocol, attributed to the worker's settlement, and ends the mark in the same transaction. LeaveUncertain commits the application's writes and ends the mark. A refused decision, or input the deployed codec cannot decode, ends the mark in a separate write and records the reason on the job.
- Schema version 14 adds the mark and marks jobs already held uncertain by lease expiry, so a settlement declared afterwards settles them too.
- This amends ADR-0013, which introduced borrowed resolution with no callback protocol, background process or schema migration. Borrowed resolution stays; the settlement adds all three for this one case.
- Grind still never decides whether uncertain work happened. The application decides, in its own transaction.

## Rationale

- The first production consumer held an agent job uncertain after two node restarts and never reconciled it, because nothing told the application that the job needed a decision. The only signals were a best-effort quarantine observation and a list query an application has to remember to poll. Every application that declares an abandonment policy needed the same sweep: a supervised process, paging, binding, a transaction and a resolution identity scheme.
- The worker already owns the abandonment policy, the codecs that decode the input and the error type a ConfirmFailure must encode. Declaring the settlement there keeps the decision next to the policy that creates the case.
- A durable mark, rather than process memory, makes the hand-over survive restarts: a job abandoned while no node ran is marked by the first quarantine scan and settled by the first settler that registers its worker. The mark names the abandoned attempt, so it has no effect once the job is resolved, claimed again or acknowledged; no other write has to clear it, and a node that predates the mark cannot make a stale one hand over the wrong uncertainty.
- Taking a job in its own committed statement before the function runs means a crash, a node death or a lost connection leaves the job due later rather than at once, so one failing job cannot be retaken ahead of every other. The try count reaches the function, so the application can stop trying with LeaveUncertain; Grind keeps trying with a growing delay, because a fix deployed later should still settle the job.
- One transaction carries the application's writes, the resolution and the ended mark. Whatever commits, commits together. A commit whose reply is lost is unknown; the next take selects only jobs still uncertain under the same mark, so it either finds the job settled or tries again.
- The original attempt cannot contradict the settlement: its acknowledgement requires a live lease, and the job was only marked after that lease expired or after Grind's own timeout acknowledgement.
- Grind's managed checkout runs its statements under its own schema. The function's writes are the application's, so the settlement restores the application's search_path for the function's part of the transaction.

## Alternatives

- **A declarative outcome on the abandonment policy, such as fail when replays are exhausted.** It needs no application function, but it cannot write the application's own rows, so the first consumer would keep its sweep. A settlement that returns ConfirmFailure and writes nothing expresses the same thing.
- **A reconciler registered on the queue or runtime that receives uncertain jobs.** The execution site is right, and the settler is that process. Declaring the function away from the worker splits the decision from the codecs and policy it depends on, and matching by worker id strings drifts.
- **Documenting the sweep and leaving the code unchanged.** That is the status quo that failed in production.
- **Running the function inside the claim-time quarantine statement.** It leaves no durable state for a retry, blocks every later expired row in the queue behind a failing function, and runs application code in the coordinator that also owns claims and capacity.
- **Remembering retries and declined jobs in process memory, without a migration.** A restart would ask again about a job the application already declined, and the declining transaction may have committed writes that the repeat duplicates.
- **Leaving a timed-out HoldUncertain attempt to lease expiry when a settlement is declared, so that one statement writes the mark.** Declaring a settlement would then change the abandonment policy's behaviour, delay the decision by a lease and lose the timeout evidence. Grind's timeout acknowledgement knows it records an abandonment and writes the mark itself.
- **Parking a job after a fixed number of failed tries.** A parked job is as easy to miss as a never-settled one. The function sees the try count and can decline after its own limit.
- **Making a settlement mandatory for workers that replay.** A forgotten case would fail compilation, but every existing consumer would break. The absence of a settlement stays visible at run time through the uncertain listing and the quarantine observation.
- **Handing handler-reported uncertainty to the settlement too.** A handler that returns Uncertain has already decided that an operator must look; settling it automatically would change the meaning of that response.
- **Reusing `admin.Decision` as the function's result.** `grind/admin` depends on `grind`, which depends on `grind/worker`, so the worker cannot name it, and moving it would break every existing `admin.ConfirmSuccess` caller.

## Consequences

- Every consumer applies schema version 14 before running this Grind, including consumers that declare no settlement; Cigogne users add its mirror file. Without a declared settlement, abandoned jobs are marked and wait for an operator: no settler starts and no settlement query runs.
- A node on the previous Grind release keeps running on schema 14 when it requires the current schema at start; one that migrates at start refuses a newer schema, as for any migration. Such a node writes no mark, so a job it holds uncertain waits for an operator.
- The function runs while the job row is locked, in a transaction on the main pool bounded by the storage deadline. It makes short database writes only; an external call in it repeats with every try. On a pool Grind owns alone, the function sees Grind's schema and qualifies its own tables.
- Settlement needs the deployed typed worker. A job of a worker version no consumer registers stays due until one does; a queue no node consumes is never settled. Jobs a handler timeout held uncertain before version 14 carry acknowledgement receipts like a handler's own Uncertain response, so the backfill cannot tell them apart and leaves them with an operator.

## Evidence

- The build adds PostgreSQL tests that drive expired leases by moving `lease_expires_at` to the database clock and stepping consumers manually, as the existing quarantine tests do. They are to cover a settlement that runs once, one for a job abandoned before any consumer started, atomic commit and rollback of application writes, a failing and a crashing function, a late acknowledgement, a declined and a refused decision, a timeout under HoldUncertain, and a worker without a settlement.
