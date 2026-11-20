# Document what each job copies

Status: in-progress
Category: enhancement

State Grind's part of the copy cost for consumers: the handler closure crosses
three process boundaries per job, the attempt actor and handler each hold a
copy while it runs, and only the selected worker is copied.

## Acceptance

- README section "What each job copies" and the `grind/worker` module docs.
- The compiled usage topic `copy_cost` holds the README's worker and its
  copy-size test.
- The json_blueprint guide is linked, not restated.
