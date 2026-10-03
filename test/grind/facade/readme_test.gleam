//// The README's quick start, compiled and run.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/otp/static_supervisor as supervisor
import gleam/time/duration
import gleeunit/should
import grind
import grind/facade/support.{pool_config}
import grind/job
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/worker
import pog

pub type Email {
  Email(to: String, subject: String)
}

fn encode_email(email: Email) -> json.Json {
  json.object([
    #("to", json.string(email.to)),
    #("subject", json.string(email.subject)),
  ])
}

fn email_decoder() -> decode.Decoder(Email) {
  use to <- decode.field("to", decode.string)
  use subject <- decode.field("subject", decode.string)
  decode.success(Email(to:, subject:))
}

pub fn mailer() -> worker.Worker(Email, String, Nil) {
  worker.new(
    "mailer.send",
    input: worker.codec(worker.infallible(encode_email), email_decoder()),
    output: worker.codec(worker.infallible(json.string), decode.string),
    perform: fn(email) { Ok("msg:" <> email.to) },
  )
  |> worker.with_queue("mailers")
}

pub fn children(pool: pog.Config, name: process.Name(grind.Message)) {
  let config = grind.new(pool) |> grind.with_worker(mailer())
  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(grind.supervised(config, name))
}

pub fn send(name: process.Name(grind.Message), email: Email) {
  let jobs = grind.named(name)
  let assert Ok(grind.Inserted(handle)) =
    grind.submit(jobs, job.new(mailer(), email))
  grind.await(jobs, handle, within: duration.seconds(5))
}

pub fn readme_quick_start_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let name = process.new_name("readme_grind")
      let assert Ok(started) =
        children(pool_config(url), name) |> supervisor.start
      use <- exception.defer(fn() { stop_supervisor(started.pid) })
      let assert Ok(Nil) = grind.migrate(grind.named(name))
      send(name, Email("a@b.c", "hi"))
      |> should.equal(Ok(grind.Succeeded("msg:a@b.c")))
      mark_database_test_executed("facade-readme-quick-start-passed")
    }
  }
}

@external(erlang, "grind_postgres_ffi", "stop_supervisor")
fn stop_supervisor(pid: process.Pid) -> Result(Bool, Nil)
