//// The engine's worker definition: codecs, the handler, retry, snooze,
//// timeout and abandonment policies, and the erased execution that the
//// consumer runs for one claimed row.
////
//// `grind/worker` builds these values for callers. The test suite also uses
//// the engine-level constructors here (`codec`, `define`,
//// `define_with_error_codec`, `with_queue_handler`), which keep the shapes
//// the storage tests were written against.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/timestamp.{type Timestamp}
import pog
import sinal/correlation.{type Correlation}

/// A versioned JSON boundary for one caller-owned value type.
pub type Codec(value) {
  Codec(
    version: String,
    encode: fn(value) -> Result(json.Json, String),
    decoder: decode.Decoder(value),
  )
}

pub type CodecError {
  EmptyCodecVersion
}

/// Creates a codec, rejecting an empty version.
pub fn codec(
  version: String,
  encode: fn(value) -> Result(json.Json, String),
  decoder: decode.Decoder(value),
) -> Result(Codec(value), CodecError) {
  case version {
    "" -> Error(EmptyCodecVersion)
    _ -> Ok(Codec(version:, encode:, decoder:))
  }
}

/// Adapts an encoder that cannot fail to the encoder shape a codec takes.
pub fn infallible(
  encode: fn(value) -> json.Json,
) -> fn(value) -> Result(json.Json, String) {
  fn(value) { Ok(encode(value)) }
}

pub type DefinitionError {
  EmptyWorkerId
  EmptyWorkerVersion
}

/// A checked relative duration returned by a queue-aware worker.
pub type RetryDelay {
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
pub const retry_delay_maximum = 9_007_199_254_740

pub fn retry_delay_maximum_milliseconds() -> Int {
  retry_delay_maximum
}

pub fn retry_delay(milliseconds: Int) -> Result(RetryDelay, RetryDelayError) {
  case milliseconds < 0 {
    True -> Error(RetryDelayMustNotBeNegative)
    False ->
      case milliseconds > retry_delay_maximum {
        True -> Error(RetryDelayExceedsSupportedMaximum)
        False -> Ok(RetryDelay(milliseconds))
      }
  }
}

/// Clamps a millisecond delay into the supported range.
pub fn clamped_retry_delay(milliseconds: Int) -> RetryDelay {
  RetryDelay(int.clamp(milliseconds, min: 0, max: retry_delay_maximum))
}

pub fn retry_delay_milliseconds(delay: RetryDelay) -> Int {
  delay.milliseconds
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

pub type RetryPolicy(error) {
  RetryPolicy(fn(RetryFailure(error), RetryContext) -> RetryDecision)
}

pub type RetryPolicyError {
  AttemptLimitMustBePositive
  AttemptLimitExceedsSupportedMaximum
}

/// Largest attempt limit representable by the PostgreSQL `bigint` column.
pub const max_attempts_maximum = 9_223_372_036_854_775_807

pub fn max_attempts_supported_maximum() -> Int {
  max_attempts_maximum
}

pub fn retry_policy(
  choose: fn(RetryFailure(error), RetryContext) -> RetryDecision,
) -> RetryPolicy(error) {
  RetryPolicy(choose)
}

/// What happens to an attempt whose node died, whose handler was killed by
/// its timeout, or whose lease otherwise expired before an acknowledgement.
pub type Abandonment {
  /// The job becomes `uncertain` and waits for an audited resolution.
  HoldUncertain
  /// Lease-expiry recovery requeues the job up to `max_replays` times, then
  /// holds it `uncertain`.
  ReplayAfterLeaseExpiry(max_replays: Int)
}

/// What a running handler knows about its job.
pub type Context {
  Context(
    job_id: Int,
    attempt: Int,
    max_attempts: Int,
    snooze_count: Int,
    queue: String,
    correlation: Correlation,
    cancellation: process.Selector(Nil),
    deadline: Option(Timestamp),
    /// The runtime's pool, shared with the application.
    connection: pog.Connection,
  )
}

/// A context for running a handler outside a consumer, as `grind/testing`
/// and the unit tests do. Its cancellation selector never fires, and its
/// connection names a pool that is never started.
pub fn synthetic_context(
  job_id job_id: Int,
  attempt attempt: Int,
  max_attempts max_attempts: Int,
  snooze_count snooze_count: Int,
  queue queue: String,
) -> Context {
  Context(
    job_id:,
    attempt:,
    max_attempts:,
    snooze_count:,
    queue:,
    correlation: correlation.from_key("grind-job-" <> int.to_string(job_id)),
    cancellation: process.new_selector(),
    deadline: None,
    connection: pog.named_connection(no_pool()),
  )
}

@external(erlang, "grind_runtime_ffi", "no_pool")
fn no_pool() -> process.Name(pog.Message)

/// The default handler timeout: 15 minutes.
pub const default_timeout_ms = 900_000

/// The default snooze limit per job.
pub const default_max_snoozes = 100

/// The default business-attempt limit.
pub const default_max_attempts = 20

/// The default queue.
pub const default_queue = "default"

/// A typed worker definition retaining its handler and all persistence codecs.
pub type Worker(input, output, error) {
  Worker(
    id: String,
    version: String,
    queue: String,
    input: Codec(input),
    output: Codec(output),
    error: Option(Codec(error)),
    handle: fn(Context, input) -> WorkerResponse(output, error),
    max_attempts: Int,
    retry_policy: Option(fn(RetryFailure(error), RetryContext) -> RetryDecision),
    timeout_ms: Option(Int),
    max_snoozes: Int,
    abandonment: Abandonment,
  )
}

/// Builds a worker with the engine defaults. The public constructors in
/// `grind/worker` check the strings first.
pub fn new(
  id: String,
  version: String,
  input: Codec(input),
  output: Codec(output),
  error: Option(Codec(error)),
  handle: fn(Context, input) -> WorkerResponse(output, error),
) -> Worker(input, output, error) {
  Worker(
    id:,
    version:,
    queue: default_queue,
    input:,
    output:,
    error:,
    handle:,
    max_attempts: default_max_attempts,
    retry_policy: None,
    timeout_ms: Some(default_timeout_ms),
    max_snoozes: default_max_snoozes,
    abandonment: HoldUncertain,
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
      Ok(
        new(id, version, input, output, error, fn(_context, input) {
          from_result(perform(input))
        }),
      )
  }
}

/// Sets the persisted business-attempt limit while retaining default backoff.
pub fn with_max_attempts(
  worker: Worker(input, output, error),
  max_attempts: Int,
) -> Result(Worker(input, output, error), RetryPolicyError) {
  case max_attempts > 0 {
    True ->
      case max_attempts <= max_attempts_maximum {
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

/// Uses a scheduling-aware handler that ignores the job context.
pub fn with_queue_handler(
  worker: Worker(input, output, error),
  handler: fn(input) -> WorkerResponse(output, error),
) -> Worker(input, output, error) {
  Worker(..worker, handle: fn(_context, input) { handler(input) })
}

/// Uses a handler that receives the job context.
pub fn with_handler(
  worker: Worker(input, output, error),
  handle: fn(Context, input) -> WorkerResponse(output, error),
) -> Worker(input, output, error) {
  Worker(..worker, handle:)
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

/// Runs the handler once.
pub fn respond(
  worker: Worker(input, output, error),
  context: Context,
  input: input,
) -> WorkerResponse(output, error) {
  worker.handle(context, input)
}

/// Why a worker's business failure is terminal.
pub type BusinessFailureCause {
  BudgetExhausted
  RetryDeclined
  SnoozeLimitReached
}

/// The stable, storage-facing string for a `BusinessFailureCause`.
pub fn business_failure_cause_to_string(cause: BusinessFailureCause) -> String {
  case cause {
    BudgetExhausted -> "budget_exhausted"
    RetryDeclined -> "retry_declined"
    SnoozeLimitReached -> "snooze_limit_reached"
  }
}

pub fn business_failure_cause_from_string(
  raw: String,
) -> Result(BusinessFailureCause, Nil) {
  case raw {
    "budget_exhausted" -> Ok(BudgetExhausted)
    "retry_declined" -> Ok(RetryDeclined)
    "snooze_limit_reached" -> Ok(SnoozeLimitReached)
    _ -> Error(Nil)
  }
}

pub type ResolvedResponse(output, error) {
  ResolvedSucceeded(output)
  ResolvedRetryable(error, RetryDelay)
  ResolvedBusinessFailure(error, BusinessFailureCause)
  ResolvedSnoozed(RetryDelay, String)
  ResolvedSnoozeLimitReached(limit: Int, reason: String)
  ResolvedDiscarded(String)
  ResolvedCancelled(String)
  ResolvedUncertain(String)
}

/// The pure policy resolver used by both queue execution and transition tests.
/// It proposes a disposition; only the fenced PostgreSQL acknowledgement can
/// establish the committed job outcome.
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
          let decision = case worker.retry_policy {
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
    WorkerSnoozed(delay, reason) ->
      case context.snooze_count >= worker.max_snoozes {
        True -> ResolvedSnoozeLimitReached(worker.max_snoozes, reason)
        False -> ResolvedSnoozed(delay, reason)
      }
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
pub fn metadata(worker: Worker(input, output, error)) -> Metadata {
  let error_version = case worker.error {
    Some(Codec(version:, ..)) -> Some(version)
    None -> None
  }
  Metadata(
    id: worker.id,
    worker_version: worker.version,
    input_version: worker.input.version,
    output_version: worker.output.version,
    error_version:,
    max_attempts: worker.max_attempts,
  )
}

/// The abandonment replay limit persisted with each admitted job.
pub fn max_replays(worker: Worker(input, output, error)) -> Option(Int) {
  case worker.abandonment {
    HoldUncertain -> None
    ReplayAfterLeaseExpiry(max_replays:) -> Some(max_replays)
  }
}

/// JSON encoding used at admission. `Error` carries the input codec's own
/// rejection reason.
pub fn encode_input(
  worker: Worker(input, output, error),
  input: input,
) -> Result(String, String) {
  worker.input.encode(input) |> result.map(json.to_string)
}

pub fn input_codec(worker: Worker(input, output, error)) -> Codec(input) {
  worker.input
}

/// JSON decoding that rejects stored data from another codec version.
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

/// Version and JSON encoding view used by audited typed outcomes and unique
/// keys. `Error` carries the codec's own rejection reason.
pub fn encode_value(
  codec: Codec(value),
  value: value,
) -> Result(#(String, String), String) {
  let Codec(version:, encode:, ..) = codec
  encode(value)
  |> result.map(fn(encoded) { #(version, json.to_string(encoded)) })
}

pub fn codec_version(codec: Codec(value)) -> String {
  codec.version
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
  /// The handler snoozed once more than its worker's `max_snoozes` allows.
  /// Committed as `business_failed` with cause `snooze_limit_reached` and
  /// no stored error.
  ExecutedSnoozeLimitReached(limit: Int, reason: String)
  ExecutedDiscarded(String)
  ExecutedCancelled(String)
  ExecutedUncertain(String)
  ExecutedInvalidInput(String)
  /// The handler ran, and its output codec (`OutputCodec`) or error codec
  /// (`ErrorCodec`) rejected the value it returned, or the encoded value was
  /// larger than the payload limit. Committed as `runtime_failed`, which is
  /// terminal: the job is not retried, because the handler's effects already
  /// happened.
  ExecutedUnencodable(codec: CodecKind, reason: String)
}

/// The erased invocation. The closure remains bound to this worker's types.
/// `max_payload_bytes` bounds the encoded output and error.
pub fn execute_encoded(
  worker: Worker(input, output, error),
  input_version: String,
  encoded_input: String,
  context: Context,
  max_payload_bytes: Int,
) -> Execution {
  case decode_codec(worker.input, input_version, encoded_input) {
    Error(decode_error) -> ExecutedInvalidInput(string.inspect(decode_error))
    Ok(input) -> {
      let response = respond(worker, context, input)
      let retry_context =
        RetryContext(
          current_attempt: context.attempt,
          max_attempts: context.max_attempts,
          snooze_count: context.snooze_count,
        )
      case resolve_response(worker, response, retry_context) {
        ResolvedSucceeded(output) ->
          case bounded(encode_value(worker.output, output), max_payload_bytes) {
            Ok(#(output_version, encoded_output)) ->
              ExecutedSuccess(output_version, encoded_output)
            Error(reason) -> ExecutedUnencodable(OutputCodec, reason)
          }
        ResolvedRetryable(application_error, delay) ->
          case encode_error(worker, application_error, max_payload_bytes) {
            Ok(#(error_version, encoded_error)) ->
              ExecutedRetryable(
                error_version,
                encoded_error,
                "worker returned an application error",
                retry_delay_milliseconds(delay),
              )
            Error(reason) -> ExecutedUnencodable(ErrorCodec, reason)
          }
        ResolvedBusinessFailure(application_error, cause) ->
          case encode_error(worker, application_error, max_payload_bytes) {
            Ok(#(error_version, encoded_error)) ->
              ExecutedBusinessFailure(
                error_version,
                encoded_error,
                "worker returned an application error",
                cause,
              )
            Error(reason) -> ExecutedUnencodable(ErrorCodec, reason)
          }
        ResolvedSnoozed(delay, reason) ->
          ExecutedSnoozed(retry_delay_milliseconds(delay), reason)
        ResolvedSnoozeLimitReached(limit:, reason:) ->
          ExecutedSnoozeLimitReached(limit:, reason:)
        ResolvedDiscarded(reason) -> ExecutedDiscarded(reason)
        ResolvedCancelled(reason) -> ExecutedCancelled(reason)
        ResolvedUncertain(evidence) -> ExecutedUncertain(evidence)
      }
    }
  }
}

/// The reason a payload of `bytes` bytes is refused under `limit`.
pub fn payload_too_large_reason(bytes: Int, limit: Int) -> String {
  "encoded payload of "
  <> int.to_string(bytes)
  <> " bytes exceeds the limit of "
  <> int.to_string(limit)
  <> " bytes"
}

fn bounded(
  encoded: Result(#(String, String), String),
  max_payload_bytes: Int,
) -> Result(#(String, String), String) {
  use #(version, text) <- result.try(encoded)
  let bytes = string.byte_size(text)
  case bytes > max_payload_bytes {
    True -> Error(payload_too_large_reason(bytes, max_payload_bytes))
    False -> Ok(#(version, text))
  }
}

fn encode_error(
  worker: Worker(input, output, error),
  application_error: error,
  max_payload_bytes: Int,
) -> Result(#(Option(String), Option(String)), String) {
  case worker.error {
    Some(codec) ->
      bounded(encode_value(codec, application_error), max_payload_bytes)
      |> result.map(fn(encoded) {
        let #(version, encoded_error) = encoded
        #(Some(version), Some(encoded_error))
      })
    None -> Ok(#(None, None))
  }
}

/// The stored failure description for an `ExecutedUnencodable` proposal.
pub fn unencodable_description(codec: CodecKind, reason: String) -> String {
  case codec {
    OutputCodec -> "output codec rejected the handler's output: " <> reason
    ErrorCodec -> "error codec rejected the handler's error: " <> reason
    InputCodec -> "input codec rejected the value: " <> reason
  }
}

/// The stored failure description for an `ExecutedSnoozeLimitReached`
/// proposal.
pub fn snooze_limit_description(limit: Int, reason: String) -> String {
  "snooze limit of " <> int.to_string(limit) <> " reached: " <> reason
}

/// The default delay before retry `current_attempt + 1`, with up to 10%
/// random jitter added so that jobs that failed together do not retry
/// together.
fn default_retry_delay(current_attempt: Int) -> RetryDelay {
  let base = default_retry_delay_milliseconds(current_attempt)
  clamped_retry_delay(base + jitter(base / 10))
}

fn jitter(span: Int) -> Int {
  case span > 0 {
    True -> int.random(span + 1)
    False -> 0
  }
}

/// Deterministic exponential backoff used when a worker has no custom policy,
/// before jitter. The first failed business attempt waits 15 seconds, capped
/// at one day.
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

/// Typed fields retained by admitted job handles.
pub fn handle_data(
  worker: Worker(input, output, error),
) -> #(Metadata, Codec(input), Codec(output), Option(Codec(error))) {
  #(metadata(worker), worker.input, worker.output, worker.error)
}
