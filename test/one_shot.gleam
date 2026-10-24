/// A one-shot counter a test can hand to a `consumer_hooks.Hooks` closure
/// it builds: `take` reports `True` the first time it is called after the
/// counter is armed, then `False` every time after — deliberately not tied
/// to any Gleam process, since the closure the counter arms runs inside
/// the coordinator under test, not the process that arms it.
pub opaque type OneShot {
  OneShot(ref: CounterRef)
}

/// A disarmed counter — `take` reports `False` until `arm` is called.
pub fn new() -> OneShot {
  OneShot(new_ref())
}

/// A counter armed to fire on the very next `take` — shorthand for `new()`
/// immediately followed by `arm`, for a test that wants its hook to fire on
/// a consumer's first worker start rather than one triggered partway
/// through the test.
pub fn armed() -> OneShot {
  let one_shot = new()
  arm(one_shot)
  one_shot
}

/// Arms (or re-arms, after a previous `take`) `one_shot` to fire once on
/// its next `take` call.
pub fn arm(one_shot: OneShot) -> Nil {
  let OneShot(ref:) = one_shot
  arm_ref(ref)
}

/// `True` the first call after `one_shot` was armed (by `armed()` or
/// `arm`), disarming it in the same call; `False` every other time,
/// including every call before the first arming.
pub fn take(one_shot: OneShot) -> Bool {
  let OneShot(ref:) = one_shot
  take_ref(ref)
}

type CounterRef

@external(erlang, "grind_one_shot", "new")
fn new_ref() -> CounterRef

@external(erlang, "grind_one_shot", "arm")
fn arm_ref(ref: CounterRef) -> Nil

@external(erlang, "grind_one_shot", "take")
fn take_ref(ref: CounterRef) -> Bool
