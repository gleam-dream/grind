import gleam/dynamic/decode
import gleam/int
import gleam/json
import grind/submission
import grind/support/env.{database_url, mark_database_test_executed}
import grind/support/submissions.{submit_keep_existing}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_database}
import grind/support/unique_rows.{
  type RawInput, RawInput, encode_raw_input, raw_input_decoder,
}
import grind/unique
import grind/worker

// -- Uniqueness increment 12 (selected keys) ---------------------------------
//
// `unique.selected` projects part of an admitted input into its own key,
// encoded with its own codec, independently of the rest of the input. These
// tests prove: a non-selected field never enters the key (only the projected
// value matters); the key contract string is `"selected:" <> name <> ":" <>
// codec_version`, so a different name or a different codec version alone
// isolates two selected keys that project the identical value; a full-input
// key and a selected key never collide, even over the exact same input,
// because their contract prefixes differ; and a selected key's equality is
// exact, not containment, the same departure from Oban's own selected-field
// semantics already proven for full-input keys
// (`postgres_submit_unique_json_equality_matches_postgres_jsonb_test`) --
// inspired by `oracle/deps/oban/test/oban/engine_test.exs`, "scoping
// uniqueness to specific argument keys".

/// An input with one field the key projects (`account`, itself a raw JSON
/// value via `RawInput`, reused from the JSON-equality tests above) and one
/// field the key never sees (`other`).
type SelectedInput {
  SelectedInput(account: RawInput, other: Int)
}

fn encode_selected_input(input: SelectedInput) -> json.Json {
  let SelectedInput(account:, other:) = input
  json.object([
    #("account", encode_raw_input(account)),
    #("other", json.int(other)),
  ])
}

fn selected_input_decoder() -> decode.Decoder(SelectedInput) {
  decode.success(SelectedInput(RawInput(json.null()), 0))
}

fn account_projection(input: SelectedInput) -> RawInput {
  let SelectedInput(account:, ..) = input
  account
}

fn selected_input_worker(
  id: String,
) -> worker.Worker(SelectedInput, String, e) {
  let assert Ok(input_codec) =
    worker.codec(
      id <> "-input-v1",
      encode_selected_input,
      selected_input_decoder(),
    )
  let assert Ok(output_codec) =
    worker.codec(id <> "-output-v1", json.string, decode.string)
  let assert Ok(worker_def) =
    worker.define(id, "v1", input_codec, output_codec, fn(input) {
      let SelectedInput(other:, ..) = input
      Ok(int.to_string(other))
    })
  worker_def
}

pub fn postgres_submit_unique_selected_key_scoping_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_unique_selected_key_scoping_test(database_url)
  }
}

fn run_submit_unique_selected_key_scoping_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_selected_pool",
  )
  let worker_def = selected_input_worker("unique.selected-" <> suffix)
  let assert Ok(account_codec) =
    worker.codec(
      "unique-selected-account-" <> suffix <> "-v1",
      encode_raw_input,
      raw_input_decoder(),
    )
  let assert Ok(other_version_codec) =
    worker.codec(
      "unique-selected-account-" <> suffix <> "-v2",
      encode_raw_input,
      raw_input_decoder(),
    )
  let assert Ok(account_key) =
    unique.selected("account", account_projection, account_codec)
  let assert Ok(account_key_other_name) =
    unique.selected("account-alt", account_projection, account_codec)
  let assert Ok(account_key_other_codec_version) =
    unique.selected("account", account_projection, other_version_codec)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let account_policy =
    unique.policy(account_key, unique.WithinQueue, period, unique.Incomplete)
  let account_policy_other_name =
    unique.policy(
      account_key_other_name,
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let account_policy_other_codec_version =
    unique.policy(
      account_key_other_codec_version,
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let full_input_policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      period,
      unique.Incomplete,
    )
  let test_queue = "selected-" <> suffix
  let shared_account = SelectedInput(RawInput(json.int(1)), 10)

  // Same projected key, a different non-selected field: still `Existing` --
  // `other` never enters the key.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-1-" <> suffix,
      worker_def,
      shared_account,
      account_policy,
    )
  let assert Ok(submission.Existing(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-2-" <> suffix,
      worker_def,
      SelectedInput(RawInput(json.int(1)), 20),
      account_policy,
    )

  // A different key name, same projection and codec, same projected value:
  // `Inserted` -- the key contract string ("selected:" <> name <> ":" <>
  // codec_version) differs by name alone.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-3-" <> suffix,
      worker_def,
      shared_account,
      account_policy_other_name,
    )

  // A different key codec version, same name and projection, same projected
  // value: `Inserted` -- the contract string differs by codec version alone.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-4-" <> suffix,
      worker_def,
      shared_account,
      account_policy_other_codec_version,
    )

  // A full-input key and a selected key never collide, even over the exact
  // same input: their contract prefixes ("full-input:" vs "selected:")
  // differ unconditionally.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-5-" <> suffix,
      worker_def,
      shared_account,
      full_input_policy,
    )

  mark_database_test_executed("unique-selected-key-scoping-passed")
}

pub fn postgres_submit_unique_selected_key_equality_not_containment_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_submit_unique_selected_key_containment_test(database_url)
  }
}

fn run_submit_unique_selected_key_containment_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use database, _connection <- with_unique_database(
    database_url,
    "grind_unique_selected_containment_pool",
  )
  let worker_def =
    selected_input_worker("unique.selected-containment-" <> suffix)
  let assert Ok(account_codec) =
    worker.codec(
      "unique-selected-containment-account-" <> suffix <> "-v1",
      encode_raw_input,
      raw_input_decoder(),
    )
  let assert Ok(account_key) =
    unique.selected("account", account_projection, account_codec)
  let assert Ok(period) =
    unique.within_milliseconds(3_600_000, unique.FromInsertion)
  let policy =
    unique.policy(account_key, unique.WithinQueue, period, unique.Incomplete)
  let test_queue = "selected-containment-" <> suffix

  // A subset projected value never conflicts with a stored superset: a
  // selected key compares by exact equality, the same as a full-input key
  // (docs/UNIQUENESS-CONTRACT.md, Decision 2) -- a deliberate departure from
  // Oban's own containment semantics for a selected-field comparison.
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-subset-" <> suffix,
      worker_def,
      SelectedInput(RawInput(json.object([#("id", json.int(1))])), 0),
      policy,
    )
  let assert Ok(submission.Inserted(_)) =
    submit_keep_existing(
      database,
      test_queue,
      "unique-selected-superset-" <> suffix,
      worker_def,
      SelectedInput(
        RawInput(json.object([#("id", json.int(1)), #("extra", json.int(2))])),
        0,
      ),
      policy,
    )

  mark_database_test_executed(
    "unique-selected-key-equality-not-containment-passed",
  )
}
