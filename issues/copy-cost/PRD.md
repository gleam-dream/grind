# Per-job copy cost

## Problem

BEAM processes share no heap. Grind moves each claimed job's handler closure,
the worker's `perform` or `handle` function with everything it captures,
across process boundaries. Consumers whose workers captured whole
environments (agents, catalogues, codecs) paid that size several times per
job, and one service ran out of memory. Grind's documentation did not say
what it copies.

## Outcome

- Grind copies only the selected worker per job, and no process copies it
  without needing it.
- The README and `grind/worker` say what crosses per job and how many times,
  how to keep a worker value small, and how to bound it in a test.
- Codec rules stay in the
  [json_blueprint codec guide](https://github.com/lostbean/json_blueprint/blob/main/docs/CODEC-PRACTICE.md).

## Out of scope

- Removing the attempt supervisor hop. It is the supervision boundary for
  attempt actors.
- Changing the public worker API.
