import grind/queue

/// The manually-polled `ValidatedPolicy` almost every test in this suite
/// starts a consumer under: no `Poll` timer of its own, so `process_one`/
/// `process_batch` drives each attempt deterministically.
pub fn manual_policy() -> queue.ValidatedPolicy {
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_manual_polling
    |> queue.validate_policy
  policy
}
