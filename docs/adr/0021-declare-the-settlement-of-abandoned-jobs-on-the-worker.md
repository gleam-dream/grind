# Declare the settlement of abandoned jobs on the worker

<a id="adr-0021"></a>

## Decision

- A worker may declare a settlement with `worker.with_settlement`. When an abandoned attempt leaves its job held uncertain, because its abandonment policy allows no further replay, the job is marked settlement due. A queue consumer whose registry holds that exact worker version hands the job, once committed, to the declared function inside one Grind-owned transaction, together with the job's typed input and the open transaction.
- The function returns ConfirmSuccess, ConfirmFailure, AuthorizeReplay or LeaveUncertain. Grind applies the decision as an attributed resolution through the existing resolution protocol, in the same transaction as the application's own writes, and clears the mark. LeaveUncertain commits the application's writes, clears the mark and keeps the job for an operator.
- A failing function, a crash, a node death or a transaction past the storage deadline rolls everything back and moves the mark one lease later. A decision the resolution protocol refuses parks the job: it stays uncertain, the mark is cleared and the reason is recorded on the job.
- Schema version 14 adds the mark and marks jobs already held uncertain by an abandoned attempt, so a declaration deployed after the fact settles them too.
- Grind still never decides whether uncertain work happened. The application decides, in its own transaction.

## Rationale

- The first production consumer held an agent job uncertain after two node restarts and never reconciled it, because nothing told the application that the job needed a decision. The only signals were a best-effort quarantine observation and a list query an application has to remember to poll. Every application that declares an abandonment policy needed the same sweep: a supervised process, paging, binding, a transaction and a resolution identity scheme.
- The worker already owns the abandonment policy, the codecs that decode the input and the error type a ConfirmFailure must encode. Declaring the settlement there keeps the decision next to the policy that creates the case, so one reading of a worker shows what happens when its attempts are abandoned.
- A durable mark, rather than process memory, makes the hand-over survive restarts: a job abandoned while no node ran is marked by the first quarantine scan and settled by the first settler that registers its worker. Retry after failure and the permanent end of a declined or refused job are both states of that one mark.
- One transaction carries the application's writes, the resolution and the cleared mark. Whatever commits, commits together; anything that fails before the commit leaves the job due, so a repeat cannot duplicate committed writes.
- The original attempt cannot contradict the settlement: its acknowledgement requires a live lease, and the job was only marked after that lease expired.
- ADR-0013 deferred a callback protocol and background process until a concrete consumer established its contract. That consumer now exists; borrowed resolution remains the primitive the settlement is built on.

## Alternatives

- **A declarative outcome on the abandonment policy, such as fail when replays are exhausted.** It needs no callback, but it cannot write the application's own rows, so the first consumer would keep its sweep. A settlement that returns ConfirmFailure and writes nothing expresses the same thing.
- **A reconciler registered on the queue or runtime that receives uncertain jobs.** The execution site is right, and the settler is that process. Declaring the function away from the worker splits the decision from the codecs and policy it depends on, and matching by worker id strings drifts.
- **Documenting the sweep and leaving the code unchanged.** That is the status quo that failed in production.
- **Running the function inside the claim-time quarantine statement.** It leaves no durable state for a retry, blocks every later expired row in the queue behind a failing function, and runs application code in the coordinator that also owns claims and capacity.
- **Remembering retries and declined jobs in process memory, without a migration.** A restart would ask again about a job the application already declined, and the declining transaction may have committed writes that the repeat duplicates.
- **Making a settlement mandatory for workers that replay.** A forgotten case would fail compilation, but every existing consumer would break. The absence of a settlement stays visible at run time through the uncertain listing and the quarantine observation.
- **Handing handler-reported uncertainty to the settlement too.** A handler that returns Uncertain has already decided that an operator must look; settling it automatically would change the meaning of that response.

## Consequences

- Every consumer applies schema version 14 before running this Grind, including consumers that declare no settlement; Cigogne users add its mirror file. A worker without a settlement behaves as before, and a queue whose registry declares none starts no settler and runs no extra query.
- With a declared settlement, a timed-out attempt under HoldUncertain is left to lease expiry instead of being acknowledged uncertain at once, so every abandonment reaches the settlement through the same path. Its description says the lease expired rather than that the handler timed out.
- The function runs in a transaction on the main pool, bounded by the storage deadline, while the job row stays locked. It should make only short database writes; external calls in it may repeat whenever a settlement is retried.
- Settlement needs the deployed typed worker. A job of a worker version no consumer registers stays due until one does; a queue no node consumes is never settled. Jobs held uncertain before version 14 by a handler's own response stay with an operator.
- During a rolling deploy, a node still on version 13 neither marks the jobs it quarantines nor clears the mark when it resolves or claims. A job it marks nothing for waits for an operator as before. A mark it leaves behind is cleared by the next claim on an upgraded node; only a version 13 node that resolves a due job by replay, claims it again and is told by the handler that the effect is uncertain can hand that job to the settlement. Finishing the deploy before declaring a settlement avoids it.

## Evidence

- PostgreSQL tests drive expired leases by moving `lease_expires_at` to the database clock and stepping consumers manually, as the existing quarantine tests do. They cover a settlement that runs once, one that runs for a job abandoned before any consumer started, atomic commit and rollback of application writes, a failing and a crashing function, a late acknowledgement, a declined and a refused decision, and a worker without a settlement.
