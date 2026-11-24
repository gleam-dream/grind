# Per-job copy cost

## Problem

BEAM processes share no heap. Grind moves each claimed job's execution
closure across process boundaries. It holds the whole worker definition: the
`perform` or `handle` function, the codecs and the policies, with everything
they capture. Consumers whose workers captured whole
environments (agents, catalogues, codecs) paid that size several times per
job, and one service ran out of memory. Grind's documentation did not say
what it copies.

## Outcome

- Grind copies only the selected worker per job. Only the handler process
  keeps it while the job runs; the attempt actor and the lease renewer hold
  none.
- The README and `grind/worker` say what crosses per job and how many times,
  how to keep a worker value small, and how to bound it in a test.
- Codec rules stay in the
  [json_blueprint codec guide](https://github.com/gleam-dream/json_blueprint/blob/main/docs/CODEC-PRACTICE.md).

## Decision: keep three copies per job

The closure is copied three times per job: into the attempt supervisor's
`start_child` call, into the attempt actor's spawn, and into the handler's
spawn. The alternative was to start the attempt actor with a closure-free
request and carry the claim in `StartAttempt`. That cuts the copies to two
(coordinator to actor, actor to handler) and stops passing large terms
through the one factory supervisor that starts every attempt on the queue.
The supervisor copy is transient: temporary children keep no start argument.
On review of grind#10 the owner chose to keep the three copies (2026-11-20),
so the attempt actor's start protocol stays as it is.

## Out of scope

- Changing the public worker API.
