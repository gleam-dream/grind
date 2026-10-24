import exception
import gleam/int
import gleeunit/should
import grind_bench/load/report
import grind_bench/load/runtime
import simplifile

@external(erlang, "bench_test_env", "with_postgres_log")
fn with_postgres_log(path: String, run: fn() -> a) -> a

pub fn configured_postgres_log_must_remain_readable_test() {
  let path =
    "/tmp/grind-bench-report-"
    <> int.to_string(runtime.monotonic_ms())
    <> ".log"
  use <- exception.defer(fn() {
    let _ = simplifile.delete(path)
    Nil
  })
  use <- with_postgres_log(path)

  // Neither a missing initial log nor a vanished final log may skip I6.
  exception.rescue(fn() { report.postgres_log_lines_before() })
  |> should.be_error
  exception.rescue(fn() { report.postgres_log_window(0) })
  |> should.be_error

  let assert Ok(Nil) = simplifile.write(path, "before\n")
  let before = report.postgres_log_lines_before()
  before |> should.equal(1)
  let assert Ok(Nil) = simplifile.append(path, "after\n")
  report.postgres_log_window(before) |> should.equal(Ok(["after", ""]))

  let assert Ok(Nil) = simplifile.delete(path)
  exception.rescue(fn() { report.postgres_log_window(before) })
  |> should.be_error
}
