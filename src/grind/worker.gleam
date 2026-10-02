//// Defines typed, versioned workers and the JSON codecs that persist their
//// input, output and error.
////
//// A `Worker` binds an id and a version to a handler
//// `fn(input) -> Result(output, error)` and to a `Codec` for each persisted
//// value. Build codecs with `codec`, and workers with `define`, or with
//// `define_with_error_codec` when the error must be stored and read back.
//// `with_queue_handler` lets a handler return a `WorkerResponse` that snoozes,
//// discards, cancels or reports an uncertain outcome. `with_max_attempts` and
//// `with_retry_policy` control retries. Register workers in a `grind/registry`,
//// submit jobs with `grind/postgres` and run them with `grind/queue`.

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

/// A checked relative duration returned by a queue-aware worker.
pub opaque type RetryDelay {
  RetryDelay(milliseconds: Int)
}

pub type RetryDelayError {
  RetryDelayMustNotBeNegative
  RetryDelayExceedsSupportedMaximum
}

/// Maximum millisecond delay supported by Grind's PostgreSQL ACK conversion.
/// The conversion multiplies by a one-millisecond interval at PostgreSQL's
/// microsecond resolution, so this conservative bound keeps the interval value
/// within the exact-integer range of `double precision`.
pub fn retry_delay_maximum_milliseconds() -> Int {
  9_007_199_254_740
}

pub fn retry_delay(milliseconds: Int) -> Result(RetryDelay, RetryDelayError) {
  case milliseconds < 0 {
    True -> Error(RetryDelayMustNotBeNegative)
    False ->
      case milliseconds > retry_delay_maximum_milliseconds() {
        True -> Error(RetryDelayExceedsSupportedMaximum)
        False -> Ok(RetryDelay(milliseconds))
      }
  }
}

pub fn retry_delay_milliseconds(delay: RetryDelay) -> Int {
  let RetryDelay(milliseconds) = delay
  milliseconds
}

/// A typed queue response. String reasons are operational evidence and do not
/// require an application error codec.
pub type WorkerResponse(output, error) {
  WorkerSucceeded(output)
  WorkerFailed(error)
  WorkerSnoozed(RetryDelay, String)
  WorkerDiscarded(String)
  WorkerCancelled(String)
  WorkerUncertain(String)
}

/// A failure presented to the definition-bound retry policy before the
/// caller-owned error type is erased.
pub type RetryFailure(error) {
  BusinessFailure(error)
}

/// Business-attempt accounting for a retry-policy decision. Delivery count is
/// intentionally separate: recovering an expired lease is not a new business
/// attempt.
pub type RetryContext {
  RetryContext(current_attempt: Int, max_attempts: Int, snooze_count: Int)
}

pub type RetryDecision {
  RetryAfter(RetryDelay)
  DoNotRetry
}

pub opaque type RetryPolicy(error) {
  RetryPolicy(fn(RetryFailure(error), RetryContext) -> RetryDecision)
}

pub type RetryPolicyError {
  AttemptLimitMustBePositive
  AttemptLimitExceedsSupportedMaximum
}

/// Largest attempt limit representable by the PostgreSQL `bigint` column.
pub fn max_attempts_supported_maximum() -> Int {
  9_223_372_036_854_775_807
}

/// Creates a pure retry policy callback. It is bound to a worker definition
/// before the worker is registered or any queue resources are acquired.
pub fn retry_policy(
  choose: fn(RetryFailure(error), RetryContext) -> RetryDecision,
) -> RetryPolicy(error) {
  RetryPolicy(choose)
}

/// Sets the persisted business-attempt limit while retaining default backoff.
pub fn with_max_attempts(
  worker: Worker(input, output, error),
  max_attempts: Int,
) -> Result(Worker(input, output, error), RetryPolicyError) {
  case max_attempts > 0 {
    True ->
      case max_attempts <= max_attempts_supported_maximum() {
        True -> Ok(Worker(..worker, max_attempts:))
        False -> Error(AttemptLimitExceedsSupportedMaximum)
      }
    False -> Error(AttemptLimitMustBePositive)
  }
}

pub fn from_result(
  result: Result(output, error),
) -> WorkerResponse(output, error) {
  case result {
    Ok(output) -> WorkerSucceeded(output)
    Error(error) -> WorkerFailed(error)
  }
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
    queue_handler: Option(fn(input) -> WorkerResponse(output, error)),
    max_attempts: Int,
    retry_policy: Option(fn(RetryFailure(error), RetryContext) -> RetryDecision),
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
    _, _ ->
      Ok(Worker(
        id:,
        version:,
        input:,
        output:,
        error:,
        perform:,
        queue_handler: None,
        max_attempts: 20,
        retry_policy: None,
      ))
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

/// Uses a scheduling-aware queue callback without changing ordinary `invoke`.
/// The worker's ID, versions, and codecs remain bound to the original
/// definition.
pub fn with_queue_handler(
  worker: Worker(input, output, error),
  handler: fn(input) -> WorkerResponse(output, error),
) -> Worker(input, output, error) {
  Worker(..worker, queue_handler: Some(handler))
}

/// Replaces the default retry callback on a worker definition. The attempt
/// limit is set separately with `with_max_attempts`. The callback receives a
/// typed business error before encoding, so no error codec is needed merely to
/// choose a retry.
pub fn with_retry_policy(
  worker: Worker(input, output, error),
  policy: RetryPolicy(error),
) -> Worker(input, output, error) {
  let RetryPolicy(choose) = policy
  Worker(..worker, retry_policy: Some(choose))
}

/// Runs the queue-specific callback, adapting ordinary `Result` handlers when
/// no queue callback was supplied. A worker is invoked exactly once.
pub fn respond(
  worker: Worker(input, output, error),
  input: input,
) -> WorkerResponse(output, error) {
  let Worker(perform:, queue_handler:, ..) = worker
  case queue_handler {
    Some(handler) -> handler(input)
    None -> from_result(perform(input))
  }
}

/// Why a worker's business failure is terminal — never retried again, either
/// because its retry budget is spent or because the worker itself declined
/// a retry. Carried by `Execution.ExecutedBusinessFailure.cause` and, from
/// there, by `job.Outcome`'s `BusinessFailedWithCause`/
/// `FailedOperationallyWithCause`; lives here rather than on `grind/job`
/// since `grind/job` already depends on `grind/worker` (for `Codec`/
/// `Worker`), not the other way around.
pub type BusinessFailureCause {
  BudgetExhausted
  RetryDeclined
}

/// The stable, storage-facing string for a `BusinessFailureCause` — the one
/// place this mapping is written. `business_failure_cause_from_string` is
/// its inverse. Internal: only Grind's own storage/observation code needs
/// this mapping; a caller holds a typed `BusinessFailureCause` already.
@internal
pub fn business_failure_cause_to_string(cause: BusinessFailureCause) -> String {
  case cause {
    BudgetExhausted -> "budget_exhausted"
    RetryDeclined -> "retry_declined"
  }
}

@internal
pub fn business_failure_cause_from_string(
  raw: String,
) -> Result(BusinessFailureCause, Nil) {
  case raw {
    "budget_exhausted" -> Ok(BudgetExhausted)
    "retry_declined" -> Ok(RetryDeclined)
    _ -> Error(Nil)
  }
}

@internal
pub type ResolvedResponse(output, error) {
  ResolvedSucceeded(output)
  ResolvedRetryable(error, RetryDelay)
  ResolvedBusinessFailure(error, BusinessFailureCause)
  ResolvedSnoozed(RetryDelay, String)
  ResolvedDiscarded(String)
  ResolvedCancelled(String)
  ResolvedUncertain(String)
}

/// The pure policy resolver used by both queue execution and transition tests.
/// It proposes a disposition; only the fenced PostgreSQL acknowledgement can
/// establish the committed job outcome.
@internal
pub fn resolve_response(
  worker: Worker(input, output, error),
  response: WorkerResponse(output, error),
  context: RetryContext,
) -> ResolvedResponse(output, error) {
  case response {
    WorkerSucceeded(output) -> ResolvedSucceeded(output)
    WorkerFailed(application_error) -> {
      let RetryContext(current_attempt:, max_attempts:, ..) = context
      case current_attempt >= max_attempts {
        True -> ResolvedBusinessFailure(application_error, BudgetExhausted)
        False -> {
          let Worker(retry_policy:, ..) = worker
          let decision = case retry_policy {
            Some(choose) -> choose(BusinessFailure(application_error), context)
            None -> RetryAfter(default_retry_delay(current_attempt))
          }
          case decision {
            RetryAfter(delay) -> ResolvedRetryable(application_error, delay)
            DoNotRetry ->
              ResolvedBusinessFailure(application_error, RetryDeclined)
          }
        }
      }
    }
    WorkerSnoozed(delay, reason) -> ResolvedSnoozed(delay, reason)
    WorkerDiscarded(reason) -> ResolvedDiscarded(reason)
    WorkerCancelled(reason) -> ResolvedCancelled(reason)
    WorkerUncertain(evidence) -> ResolvedUncertain(evidence)
  }
}

/// Which of a worker's three codec contracts (input, output, error)
/// disagreed with what was stored, or with a registered worker's declared
/// version, when a claimed row's worker identity was resolved against the
/// running registry.
pub type CodecKind {
  InputCodec
  OutputCodec
  ErrorCodec
}

/// The persistence metadata bound to a worker definition.
@internal
pub type Metadata {
  Metadata(
    id: String,
    worker_version: String,
    input_version: String,
    output_version: String,
    error_version: Option(String),
    max_attempts: Int,
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
    max_attempts:,
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
    max_attempts:,
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

/// Internal version and JSON encoding view used by audited typed outcomes.
@internal
pub fn encode_value(codec: Codec(value), value: value) -> #(String, String) {
  let Codec(version:, encode:, ..) = codec
  #(version, json.to_string(encode(value)))
}

@internal
pub fn codec_version(codec: Codec(value)) -> String {
  let Codec(version:, ..) = codec
  version
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
    cause: BusinessFailureCause,
  )
  ExecutedRetryable(
    error_version: Option(String),
    encoded_error: Option(String),
    description: String,
    delay_ms: Int,
  )
  ExecutedSnoozed(delay_ms: Int, reason: String)
  ExecutedDiscarded(String)
  ExecutedCancelled(String)
  ExecutedUncertain(String)
  ExecutedInvalidInput(String)
}

/// Internal erased invocation. The closure remains bound to this worker's types.
@internal
pub fn execute_encoded(
  worker: Worker(input, output, error),
  input_version: String,
  encoded_input: String,
  context: RetryContext,
) -> Execution {
  let Worker(
    input: input_codec,
    output: Codec(version: output_version, encode: encode_output, ..),
    ..,
  ) = worker
  case decode_codec(input_codec, input_version, encoded_input) {
    Error(decode_error) -> ExecutedInvalidInput(string.inspect(decode_error))
    Ok(input) -> {
      let response = respond(worker, input)
      case resolve_response(worker, response, context) {
        ResolvedSucceeded(output) ->
          ExecutedSuccess(output_version, json.to_string(encode_output(output)))
        ResolvedRetryable(application_error, delay) -> {
          let #(error_version, encoded_error) =
            encode_error(worker, application_error)
          ExecutedRetryable(
            error_version,
            encoded_error,
            "worker returned an application error",
            retry_delay_milliseconds(delay),
          )
        }
        ResolvedBusinessFailure(application_error, cause) -> {
          let #(error_version, encoded_error) =
            encode_error(worker, application_error)
          ExecutedBusinessFailure(
            error_version,
            encoded_error,
            "worker returned an application error",
            cause,
          )
        }
        ResolvedSnoozed(delay, reason) ->
          ExecutedSnoozed(retry_delay_milliseconds(delay), reason)
        ResolvedDiscarded(reason) -> ExecutedDiscarded(reason)
        ResolvedCancelled(reason) -> ExecutedCancelled(reason)
        ResolvedUncertain(evidence) -> ExecutedUncertain(evidence)
      }
    }
  }
}

fn encode_error(
  worker: Worker(input, output, error),
  application_error: error,
) -> #(Option(String), Option(String)) {
  let Worker(error:, ..) = worker
  case error {
    Some(Codec(version:, encode:, ..)) -> #(
      Some(version),
      Some(json.to_string(encode(application_error))),
    )
    None -> #(None, None)
  }
}

fn default_retry_delay(current_attempt: Int) -> RetryDelay {
  let delay_ms = default_retry_delay_milliseconds(current_attempt)
  let assert Ok(delay) = retry_delay(delay_ms)
  delay
}

/// Deterministic exponential backoff used when a worker has no custom policy.
/// The first failed business attempt waits 15 seconds, capped at one day.
pub fn default_retry_delay_milliseconds(current_attempt: Int) -> Int {
  let exponent = case current_attempt > 1 {
    True -> current_attempt - 1
    False -> 0
  }
  default_retry_delay_loop(exponent, 15_000)
}

fn default_retry_delay_loop(exponent: Int, current_ms: Int) -> Int {
  case exponent <= 0 || current_ms >= 86_400_000 {
    True ->
      case current_ms > 86_400_000 {
        True -> 86_400_000
        False -> current_ms
      }
    False -> default_retry_delay_loop(exponent - 1, current_ms * 2)
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
