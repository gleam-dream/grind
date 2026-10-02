# Migrating Grind dependents after wave 2

## Follow-up: fallible codec encoders

A Grind codec's encoder can now reject a value. Before, `worker.codec` took
`fn(value) -> json.Json`, so an app that reused a validating codec (a
json_blueprint codec with a refinement such as `integer_between`) had to
panic on a rejected value or store a `null` that its decoder later refused.
A rejected input now fails submit with a typed error and writes nothing. A
rejected output or error ends the job in a defined terminal state.

### Changed public items

`worker.codec` takes a fallible encoder. Wrap a plain gleam_json encoder with
the new `worker.infallible`:

```gleam
// Before
worker.codec("email-v1", encode_email, email_decoder())

// After
worker.codec("email-v1", worker.infallible(encode_email), email_decoder())
```

A json_blueprint bridge drops its panic or `let assert`:

```gleam
// Before
worker.codec(
  version,
  fn(value) {
    case codec.to_json(c, value) {
      Ok(json) -> json
      Error(error) -> panic as codec.describe_encode_error(error)
    }
  },
  codec.decoder(c),
)

// After
worker.codec(
  version,
  fn(value) {
    codec.to_json(c, value) |> result.map_error(codec.describe_encode_error)
  },
  codec.decoder(c),
)
```

`unique.selected(name, select, codec)` keeps its signature; its codec is the
same `worker.Codec`, so it is built the same way.

### Added public items

| Item                                        | Where                      | When                                                                                      |
| ------------------------------------------- | -------------------------- | ----------------------------------------------------------------------------------------- |
| `worker.infallible(encode)`                 | `grind/worker`             | Adapts `fn(v) -> json.Json` to `fn(v) -> Result(json.Json, String)`.                      |
| `submission.InvalidInput(reason: String)`   | `submission.SubmitError`   | `submit`, `submit_at`, `submit_with_id` or `submit_unique` rejected the input or the key. |
| `postgres.ResolutionInvalidValue(reason:)`  | `postgres.ResolutionError` | `resolve_uncertain` was given an output or error that the job's codec rejects.            |
| `worker.ExecutedUnencodable(codec, reason)` | `worker.Execution`         | The handler's output or error was rejected after the handler ran.                         |

A rejected handler output or error commits the job as `job.RuntimeFailed`,
which is terminal and not retried, even with attempts left.
`postgres.outcome` returns `job.FailedOperationally(description)`, where the
description is `"output codec rejected the handler's output: <reason>"` or
`"error codec rejected the handler's error: <reason>"`. A `selected` key
rejection reads `"unique key <name>: <reason>"`.

A `case` that matches `SubmitError`, `ResolutionError` or `Execution`
exhaustively needs the new variant. No dependent does this today.

### Dependents

Counts come from `grep` over `/code/gleam-dream/*/src`, `*/test`,
`*/integrations`, `*/consumers` and `oversight/apps`. No other gleam-dream
package depends on Grind.

| File                                                          | Calls | Today                                      | Edit                                                                  |
| ------------------------------------------------------------- | ----- | ------------------------------------------ | --------------------------------------------------------------------- |
| `oversight/apps/research_agent/src/research_agent/wire.gleam` | 1     | Blueprint bridge that panics               | `codec.to_json(..) \|> result.map_error(codec.describe_encode_error)` |
| `oversight/apps/extractor/src/extractor/jobs.gleam`           | 1     | Blueprint bridge with `let assert Ok`      | Same                                                                  |
| `oversight/apps/secure_mcp/src/secure_mcp/reports.gleam`      | 1     | Blueprint bridge that stores `json.null()` | Same; a rejected report job then fails submit instead of on read      |
| `oversight/apps/secure_mcp/test/secure_mcp_test.gleam`        | 1     | `json.string`                              | `worker.infallible(json.string)`                                      |
| `oversight/apps/checkout/src/checkout/codecs.gleam`           | 1     | total `JsonCodec.encode`                   | `worker.infallible(c.encode)`                                         |
| `oversight/apps/webhooks/src/webhooks/attempt.gleam`          | 3     | inline total gleam_json encoders           | Wrap each in `worker.infallible(..)`                                  |

An app whose submit path handles `SubmitError` can now report
`InvalidInput(reason)` to its caller instead of crashing.
