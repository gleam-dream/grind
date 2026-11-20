# The renewer and the attempt actor hold no worker

Status: in-progress
Category: enhancement

The attempt actor sent the whole claimed job, worker closure included, to the
lease renewer, which kept it for the whole attempt. That was a fourth copy of
the worker per job, retained once per running job. The renewer needs only the
attempt fence and its diagnostic identity.

## Acceptance

- `renewer.Track` carries an `attempt.LeaseFence`, which holds no closure or
  input.
- After spawning the handler, the attempt actor keeps `attempt.handed_off`,
  the claim without its closure.
- `test/grind/queue/copy_cost_test.gleam` starts the real attempt actor with
  a test-owned renewer subject. For a worker that captures 240 kB, it bounds
  the received `Track` message below 1 kB, the actor's state below 8 kB and
  its heap after collection below half the capture.
