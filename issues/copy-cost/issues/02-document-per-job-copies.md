# Document what each job copies

Status: in-progress
Category: enhancement

State Grind's part of the copy cost for consumers: the closure over the whole worker definition crosses
three process boundaries per job, each copy loses sharing, only the handler
keeps its copy while the job runs, and only the selected worker is copied.
Name the per-consumer registry copies too.

## Acceptance

- README section "What each job copies" and the `grind/worker` module docs.
- The compiled usage topic `copy_cost` holds the README's worker and its
  copy-size test.
- The json_blueprint guide is linked, not restated.
