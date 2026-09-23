import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

/// A versioned JSON boundary for one caller-owned value type.
pub opaque type Codec(value) {
  Codec(
    version: String,
    encode: fn(value) -> json.Json,
    decoder: decode.Decoder(value),
  )
}

pub type CodecError {
  EmptyCodecVersion
}

/// Creates a JSON codec whose version is persisted with each job.
pub fn codec(
  version: String,
  encode: fn(value) -> json.Json,
  decoder: decode.Decoder(value),
) -> Result(Codec(value), CodecError) {
  case version {
    "" -> Error(EmptyCodecVersion)
    _ -> Ok(Codec(version:, encode:, decoder:))
  }
}

pub type DefinitionError {
  EmptyWorkerId
  EmptyWorkerVersion
}

/// A typed worker definition retaining its handler and all persistence codecs.
pub opaque type Worker(input, output, error) {
  Worker(
    id: String,
    version: String,
    input: Codec(input),
    output: Codec(output),
    error: Option(Codec(error)),
    perform: fn(input) -> Result(output, error),
  )
}

/// Defines a worker whose application errors are not retained after failure.
pub fn define(
  id: String,
  version: String,
  input: Codec(input),
  output: Codec(output),
  perform: fn(input) -> Result(output, error),
) -> Result(Worker(input, output, error), DefinitionError) {
  create(id, version, input, output, None, perform)
}

/// Defines a worker whose application errors must survive persistence and typed retrieval.
pub fn define_with_error_codec(
  id: String,
  version: String,
  input: Codec(input),
  output: Codec(output),
  error: Codec(error),
  perform: fn(input) -> Result(output, error),
) -> Result(Worker(input, output, error), DefinitionError) {
  create(id, version, input, output, Some(error), perform)
}

fn create(
  id: String,
  version: String,
  input: Codec(input),
  output: Codec(output),
  error: Option(Codec(error)),
  perform: fn(input) -> Result(output, error),
) -> Result(Worker(input, output, error), DefinitionError) {
  case id, version {
    "", _ -> Error(EmptyWorkerId)
    _, "" -> Error(EmptyWorkerVersion)
    _, _ -> Ok(Worker(id:, version:, input:, output:, error:, perform:))
  }
}

/// Calls the worker handler and preserves its caller-owned output and error types.
pub fn invoke(
  worker: Worker(input, output, error),
  input: input,
) -> Result(output, error) {
  let Worker(perform:, ..) = worker
  perform(input)
}

/// The persistence metadata bound to a worker definition.
pub type Metadata {
  Metadata(
    id: String,
    worker_version: String,
    input_version: String,
    output_version: String,
    error_version: Option(String),
  )
}

/// Internal persistence view; callers should define a worker once and submit it.
@internal
pub fn metadata(worker: Worker(input, output, error)) -> Metadata {
  let Worker(
    id:,
    version: worker_version,
    input: Codec(version: input_version, ..),
    output: Codec(version: output_version, ..),
    error:,
    ..,
  ) = worker
  let error_version = case error {
    Some(Codec(version:, ..)) -> Some(version)
    None -> None
  }
  Metadata(
    id:,
    worker_version:,
    input_version:,
    output_version:,
    error_version:,
  )
}

/// Internal JSON encoding used at admission.
@internal
pub fn encode_input(
  worker: Worker(input, output, error),
  input: input,
) -> String {
  let Worker(input: Codec(encode:, ..), ..) = worker
  encode(input) |> json.to_string
}

/// Internal codec result used by typed job retrieval.
@internal
pub fn input_codec(worker: Worker(input, output, error)) -> Codec(input) {
  let Worker(input:, ..) = worker
  input
}

/// Internal JSON decoding that rejects stored data from another codec version.
@internal
pub fn decode_codec(
  codec: Codec(value),
  stored_version: String,
  encoded: String,
) -> Result(value, StoredCodecError) {
  let Codec(version:, decoder:, ..) = codec
  case version == stored_version {
    False -> Error(CodecVersionMismatch(expected: version, got: stored_version))
    True -> json.parse(encoded, decoder) |> result.map_error(InvalidStoredJson)
  }
}

pub type StoredCodecError {
  CodecVersionMismatch(expected: String, got: String)
  InvalidStoredJson(json.DecodeError)
}

/// The JSON-safe result of one invocation after the typed worker is erased.
pub type Execution {
  ExecutedSuccess(output_version: String, encoded_output: String)
  ExecutedBusinessFailure(
    error_version: Option(String),
    encoded_error: Option(String),
    description: String,
  )
  ExecutedInvalidInput(String)
}

/// Internal erased invocation. The closure remains bound to this worker's types.
@internal
pub fn execute_encoded(
  worker: Worker(input, output, error),
  input_version: String,
  encoded_input: String,
) -> Execution {
  let Worker(
    input: input_codec,
    output: Codec(version: output_version, encode: encode_output, ..),
    error:,
    perform: perform,
    ..,
  ) = worker
  case decode_codec(input_codec, input_version, encoded_input) {
    Error(decode_error) -> ExecutedInvalidInput(string.inspect(decode_error))
    Ok(input) ->
      case perform(input) {
        Ok(output) ->
          ExecutedSuccess(output_version, json.to_string(encode_output(output)))
        Error(application_error) ->
          case error {
            Some(Codec(version:, encode:, ..)) ->
              ExecutedBusinessFailure(
                Some(version),
                Some(json.to_string(encode(application_error))),
                "worker returned an application error",
              )
            None ->
              ExecutedBusinessFailure(
                None,
                None,
                "worker returned an application error",
              )
          }
      }
  }
}

/// Internal typed fields retained by admitted job handles.
@internal
pub fn handle_data(
  worker: Worker(input, output, error),
) -> #(Metadata, Codec(input), Codec(output), Option(Codec(error))) {
  let Worker(input:, output:, error:, ..) = worker
  #(metadata(worker), input, output, error)
}
