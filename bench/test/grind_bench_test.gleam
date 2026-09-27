//// `gleam test`'s own required entry point (must be named `<project
//// name>_test`, matching `consumer/test/grind_consumer_test.gleam`'s own
//// convention) -- gleeunit itself discovers every other `*_test.gleam`
//// module's own test functions at runtime.

import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}
