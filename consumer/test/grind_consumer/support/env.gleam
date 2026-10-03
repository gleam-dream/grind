import gleam/time/timestamp
import grind/internal/consumer as queue

pub fn now_unix_ms() -> Int {
  let #(seconds, nanoseconds) =
    timestamp.system_time() |> timestamp.to_unix_seconds_and_nanoseconds
  seconds * 1000 + nanoseconds / 1_000_000
}

/// The manually-polled `ValidatedPolicy` every consumer in this suite that
/// does not need automatic polling starts under: no `Poll` timer of its own.
pub fn manual_policy() -> queue.ValidatedPolicy {
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_manual_polling
    |> queue.validate_policy
  policy
}

@external(erlang, "consumer_test_env", "database_url")
pub fn database_url() -> Result(String, Nil)

@external(erlang, "consumer_test_env", "storage_failure_url")
pub fn storage_failure_url() -> Result(String, Nil)

@external(erlang, "consumer_test_env", "mark")
pub fn mark(name: String) -> Nil

@external(erlang, "consumer_effect", "reset")
pub fn reset_effects() -> Nil

@external(erlang, "consumer_effect", "apply")
pub fn apply_synthetic_effect(key: String, amount: Int) -> #(String, Int)

@external(erlang, "consumer_effect", "count")
pub fn synthetic_effect_count(key: String) -> Int

/// Reads the application's own dedup record for a key without applying
/// anything: this is how the app inspects its own table during an audited
/// resolution, as opposed to calling `apply_synthetic_effect` again.
@external(erlang, "consumer_effect", "receipt")
pub fn synthetic_effect_receipt(key: String) -> Result(String, Nil)

/// Arms a one-shot crash: the next `apply_synthetic_effect` call for this
/// exact key applies (and retains) its effect first, then raises, killing
/// the calling worker process before Grind can acknowledge anything.
@external(erlang, "consumer_effect", "arm_crash_after_effect")
pub fn arm_crash_after_effect(key: String) -> Nil

/// Reports whether a crash was still armed for this key, consuming it if
/// so. Used here only to prove the fault is genuinely one-shot.
@external(erlang, "consumer_effect", "take_fault")
pub fn take_effect_fault(key: String) -> Bool

@external(erlang, "consumer_counter", "reset")
pub fn reset_counter(key: String) -> Nil

@external(erlang, "consumer_counter", "next")
pub fn next_counter(key: String) -> Int

@external(erlang, "consumer_counter", "value")
pub fn counter_value(key: String) -> Int
