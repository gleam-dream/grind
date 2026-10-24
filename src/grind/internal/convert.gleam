//// Converts the engine's vocabulary to the public one in `grind/job`.

import grind/internal/job as internal
import grind/internal/worker as definition
import grind/job

pub fn state(state: internal.State) -> job.State {
  case state {
    internal.Queued -> job.Queued
    internal.Scheduled -> job.Scheduled
    internal.Retryable -> job.Retryable
    internal.Executing -> job.Executing
    internal.Succeeded -> job.Succeeded
    internal.BusinessFailed -> job.BusinessFailed
    internal.RuntimeFailed -> job.RuntimeFailed
    internal.ContractMismatch -> job.ContractMismatch
    internal.Uncertain -> job.Uncertain
    internal.Discarded -> job.Discarded
    internal.Cancelled -> job.Cancelled
  }
}

pub fn internal_state(state: job.State) -> internal.State {
  case state {
    job.Queued -> internal.Queued
    job.Scheduled -> internal.Scheduled
    job.Retryable -> internal.Retryable
    job.Executing -> internal.Executing
    job.Succeeded -> internal.Succeeded
    job.BusinessFailed -> internal.BusinessFailed
    job.RuntimeFailed -> internal.RuntimeFailed
    job.ContractMismatch -> internal.ContractMismatch
    job.Uncertain -> internal.Uncertain
    job.Discarded -> internal.Discarded
    job.Cancelled -> internal.Cancelled
  }
}

pub fn terminal_cause(
  cause: definition.BusinessFailureCause,
) -> job.TerminalCause {
  case cause {
    definition.BudgetExhausted -> job.BudgetExhausted
    definition.RetryDeclined -> job.RetryDeclined
    definition.SnoozeLimitReached -> job.SnoozeLimitReached
  }
}

pub fn codec_kind(kind: definition.CodecKind) -> job.CodecKind {
  case kind {
    definition.InputCodec -> job.InputCodec
    definition.OutputCodec -> job.OutputCodec
    definition.ErrorCodec -> job.ErrorCodec
  }
}
