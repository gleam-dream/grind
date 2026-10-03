import gleeunit/should
import grind/internal/consumer as queue

pub fn queue_policy_is_checked_before_start_test() {
  queue.default_policy()
  |> queue.with_poll_interval(0)
  |> queue.validate_policy
  |> should.equal(Error(queue.PollIntervalMustBePositive))

  queue.default_policy()
  |> queue.with_maximum_batch_jobs(-1)
  |> queue.validate_policy
  |> should.equal(Error(queue.MaximumBatchJobsMustBePositive))

  queue.default_policy()
  |> queue.with_shutdown_grace(-1)
  |> queue.validate_policy
  |> should.equal(Error(queue.ShutdownGraceMustBeNonNegative))
}
