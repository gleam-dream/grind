import gleam/dynamic/decode
import gleam/json

pub type LookupFailure {
  AccountMissing(account_id: Int)
}

pub fn encode_lookup_failure(error: LookupFailure) -> json.Json {
  case error {
    AccountMissing(account_id) ->
      json.object([
        #("kind", json.string("account_missing")),
        #("account_id", json.int(account_id)),
      ])
  }
}

pub fn decode_lookup_failure() -> decode.Decoder(LookupFailure) {
  use kind <- decode.field("kind", decode.string)
  use account_id <- decode.field("account_id", decode.int)
  case kind {
    "account_missing" -> decode.success(AccountMissing(account_id))
    _ -> decode.failure(AccountMissing(account_id), "known lookup failure kind")
  }
}
