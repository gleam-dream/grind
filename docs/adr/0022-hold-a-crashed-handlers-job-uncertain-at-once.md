# Hold a crashed handler's job uncertain at once, with its own cause

<a id="adr-0022"></a>

## Decision

- A handler crash is a third abandonment cause, `HandlerCrashed`, next to `LeaseExpired` and `TimedOut` in the closed cause a declared settlement reads. This amends ADR-0021, whose cause had two variants.
- The attempt actor traps its handler's exit. A crash takes the same path as a timeout: under HoldUncertain the attempt acknowledges the job uncertain at once as an abandonment, which writes the settlement mark, with a bounded description of the crash; under ReplayAfterLeaseExpiry it stops renewing and lease expiry replays the job.
- The abandonment acknowledgement stores its cause in the job's `failure_cause` (`timed_out` or `handler_crashed`). A resolution clears it. A job with no acknowledgement receipt for its retained attempt is `LeaseExpired`; a receipt without a stored cause, written by a timeout before this decision, is `TimedOut`. No migration is needed.
- HoldUncertain stays the default policy.

## Rationale

- Before this decision a crash took the attempt actor down. The job stayed executing for one lease and was then held uncertain as a lease expiry, with no description. A settlement could not tell a defect in the handler from a lost node, and the job waited a lease for nothing: the node that saw the crash knows at once that the attempt ended without a result.
- A crash, like a timeout, does not prove that the handler had no effect, so it is an abandonment and not a failed attempt; the abandonment policy decides what follows, as it does for a timeout.
- The settlement's cause is read in the take's lock statement. The job row already carries an unconstrained `failure_cause` column, used until now only for terminal business failures; using it for the abandonment cause of an uncertain row adds no column and no migration, and the two uses never meet on one row because a resolution clears it.

## Alternatives

- **Keep the crash on lease expiry and only store its description.** It keeps the public type unchanged, but the settlement still reads the wrong cause and the job still waits a lease.
- **A new column for the cause, or a new receipt `failure_cause` value.** Either needs a migration, and the receipt value needs its check constraint widened, for a fact the job row can hold.
- **Retry a crash under the retry policy.** A crash may follow an effect; retrying it would repeat the effect, which the abandonment policy exists to prevent.
