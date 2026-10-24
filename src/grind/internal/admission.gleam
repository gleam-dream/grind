//// The admission request `grind/job` builds and `grind.submit` admits.

import gleam/option.{type Option}
import grind/internal/unique
import grind/internal/worker.{type Worker}
import sinal/correlation.{type Correlation}

/// When a freshly inserted job may first run.
pub type When {
  Now
  /// At this Unix time, in milliseconds.
  AtUnixMs(Int)
  /// This many milliseconds after the admission's database time.
  AfterMs(Int)
}

/// One job to admit, with its per-job options.
pub type Job(input, output, error) {
  Job(
    worker: Worker(input, output, error),
    input: input,
    id: Option(String),
    when: When,
    unique: Option(unique.Uniqueness(input)),
    correlation: Option(Correlation),
    queue: Option(String),
    max_attempts: Option(Int),
  )
}
