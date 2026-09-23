# Public Grind consumer

This separate Gleam package depends on Grind by local path and imports only
public Grind modules. It registers `Worker(PaymentRequest, String,
PaymentError)` alongside `Worker(Int, Int, Nil)`, admits both through their
definitions, runs them from a supervised automatic queue, and observes committed
typed success and a tagged application error.

The queue validates its polling interval and a three-jobs-per-poll limit before
the queue actor starts. The limit drains jobs serially and does not claim three
concurrent workers. Handler messages and a same-actor request form a bounded
synchronization barrier before the test reads committed outcomes.

The synthetic payment effect uses the caller's idempotency key. The test records
an effect before job admission and then reuses the same key inside the worker;
the app-owned local table returns the original receipt without applying a second
synthetic effect. This demonstrates one way an application can handle a crash
window. Grind itself does not guarantee exactly-once effects, and the test uses
no paid service.

Run the consumer as part of the disposable integration suite from the package
root with `nix develop --command bash scripts/test-postgres.sh`.
