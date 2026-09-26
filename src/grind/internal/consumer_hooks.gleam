import gleam/erlang/process

/// Test-only extension points into one consumer's worker-start sequence.
/// `before_worker_start` runs immediately before `factory_supervisor
/// .start_child`; an `Error(reason)` is treated exactly like that call
/// itself failing (`actor.InitFailed(reason)`), so a test can force the
/// same recovery path a genuine start failure takes without actually
/// breaking child startup. `after_worker_start` runs immediately after a
/// successful start, before the coordinator installs its own monitor, so a
/// test can exercise the "already dead by the time it is monitored" edge
/// (killing the given pid, for instance) that a real crash could otherwise
/// only produce by chance. Neither hook is exposed publicly: `queue.start`
/// always passes `none()`, and only the `@internal` `start_with_hooks`
/// takes a caller-supplied `Hooks`.
pub type Hooks {
  Hooks(
    before_worker_start: fn() -> Result(Nil, String),
    after_worker_start: fn(process.Pid) -> Nil,
  )
}

/// The no-op hooks every publicly-started consumer runs under.
pub fn none() -> Hooks {
  Hooks(before_worker_start: fn() { Ok(Nil) }, after_worker_start: fn(_pid) {
    Nil
  })
}
