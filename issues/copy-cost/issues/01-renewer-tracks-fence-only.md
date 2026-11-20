# The renewer tracks the lease fence only

Status: in-progress
Category: enhancement

The attempt actor sent the whole claimed job, worker closure included, to the
lease renewer, which kept it for the whole attempt. That was a fourth copy of
the worker per job, retained once per running job. The renewer needs only the
attempt fence and its diagnostic identity.

## Acceptance

- `renewer.Track` carries an `attempt.LeaseFence`, which holds no closure or
  input.
- `test/grind/queue/copy_cost_test.gleam` bounds the fence below 1 kB for a
  worker whose closure exceeds 100 kB.
