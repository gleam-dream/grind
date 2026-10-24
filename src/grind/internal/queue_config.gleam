//// The queue settings `grind/queue` builds.

pub type Queue {
  Queue(
    name: String,
    concurrency: Int,
    poll_interval_ms: Int,
    lease_ms: Int,
    shutdown_grace_ms: Int,
  )
}

pub const default_concurrency = 10

pub const default_poll_interval_ms = 250

pub const default_lease_ms = 30_000

pub const default_shutdown_grace_ms = 15_000

pub fn new(name: String) -> Queue {
  Queue(
    name:,
    concurrency: default_concurrency,
    poll_interval_ms: default_poll_interval_ms,
    lease_ms: default_lease_ms,
    shutdown_grace_ms: default_shutdown_grace_ms,
  )
}
