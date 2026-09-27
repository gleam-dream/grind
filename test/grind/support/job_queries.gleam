import gleam/dynamic/decode
import pog

pub fn count_jobs_in_queue(connection: pog.Connection, queue: String) -> Int {
  let assert Ok(returned) =
    pog.query("SELECT count(*)::bigint FROM grind_jobs WHERE queue = $1")
    |> pog.parameter(pog.text(queue))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection)
  let assert [count] = returned.rows
  count
}
