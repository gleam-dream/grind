import gleam/list
import gleam/option.{type Option}
import grind/worker.{type Worker}

/// Immutable worker selections for one queue name.
pub opaque type Registry {
  Registry(queue: String, workers: List(Selection))
}

type Selection {
  Selection(
    id: String,
    version: String,
    input_version: String,
    output_version: String,
    error_version: Option(String),
    run: fn(String, String, worker.RetryContext) -> worker.Execution,
  )
}

pub type RegisterError {
  EmptyQueueName
  DuplicateWorker(id: String, version: String)
}

pub fn new(queue: String) -> Result(Registry, RegisterError) {
  case queue {
    "" -> Error(EmptyQueueName)
    _ -> Ok(Registry(queue:, workers: []))
  }
}

/// Binds the typed worker before adding it to the heterogeneous registry.
pub fn register(
  registry: Registry,
  worker: Worker(input, output, error),
) -> Result(Registry, RegisterError) {
  let Registry(queue:, workers: workers) = registry
  let metadata = worker.metadata(worker)
  let worker.Metadata(
    id:,
    worker_version: version,
    input_version:,
    output_version:,
    error_version:,
    ..,
  ) = metadata
  case
    list.any(workers, fn(selection) {
      let Selection(id: registered_id, version: registered_version, ..) =
        selection
      registered_id == id && registered_version == version
    })
  {
    True -> Error(DuplicateWorker(id:, version:))
    False ->
      Ok(
        Registry(queue:, workers: [
          Selection(
            id:,
            version:,
            input_version:,
            output_version:,
            error_version:,
            run: fn(input_version, encoded_input, context) {
              worker.execute_encoded(
                worker,
                input_version,
                encoded_input,
                context,
              )
            },
          ),
          ..workers
        ]),
      )
  }
}

pub fn queue(registry: Registry) -> String {
  let Registry(queue:, ..) = registry
  queue
}

@internal
pub fn identities(registry: Registry) -> List(#(String, String)) {
  let Registry(workers:, ..) = registry
  list.map(workers, fn(selection) {
    let Selection(id:, version:, ..) = selection
    #(id, version)
  })
}

pub type SelectionError {
  WrongQueue(expected: String, actual: String)
  WorkerNotRegistered(id: String, version: String)
}

/// Internal exact selection; no worker-version fallback is permitted.
@internal
pub fn select(
  registry: Registry,
  queue: String,
  id: String,
  version: String,
) -> Result(
  #(
    String,
    String,
    Option(String),
    fn(String, String, worker.RetryContext) -> worker.Execution,
  ),
  SelectionError,
) {
  let Registry(queue: registered_queue, workers: workers) = registry
  case registered_queue == queue {
    False -> Error(WrongQueue(expected: registered_queue, actual: queue))
    True -> {
      let matching =
        list.find(workers, fn(selection) {
          let Selection(id: registered_id, version: registered_version, ..) =
            selection
          registered_id == id && registered_version == version
        })
      case matching {
        Ok(Selection(input_version:, output_version:, error_version:, run:, ..)) ->
          Ok(#(input_version, output_version, error_version, run))
        Error(Nil) -> Error(WorkerNotRegistered(id:, version:))
      }
    }
  }
}
