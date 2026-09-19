import gleeunit
import gleeunit/should
import grind

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn version_test() {
  grind.version()
  |> should.equal("0.1.0")
}
