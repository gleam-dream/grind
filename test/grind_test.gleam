import gleam/erlang/process
import gleeunit
import gleeunit/should
import pog

pub fn main() -> Nil {
  gleeunit.main()
}

/// Canary for `grind_postgres_ffi`'s own dependency on pog's private
/// `pog.Connection` shape: a freshly named connection must still be the
/// `{pool, Name}` tuple `grind_postgres_ffi:with_deadline/3` matches on. The
/// exact pog version pin in `gleam.toml` (`>= 4.1.0 and < 4.2.0`) is what
/// actually guards this in practice — this test is the loud failure if that
/// pin is ever widened past a pog release that changes the shape.
pub fn pog_connection_pool_shape_test() {
  let name = process.new_name("grind_pool_shape_probe")
  pog.named_connection(name)
  |> pool_connection_atom()
  |> should.be_ok()
}

@external(erlang, "grind_test_env", "pool_connection_atom")
fn pool_connection_atom(connection: pog.Connection) -> Result(a, Nil)
