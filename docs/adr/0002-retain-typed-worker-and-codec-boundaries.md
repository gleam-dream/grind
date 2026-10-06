# Retain typed worker and codec boundaries across erasure

<a id="adr-0002"></a>

- **Decision.** The public worker owns caller input/output/error types and versioned fallible JSON codecs. Registration retains typed closures for internal heterogeneous dispatch. Exact worker versions and codec contracts are required; failures never manufacture a business error.

- **Rationale.** Encoding can fail before admission or after a handler effect. These cases need different outcomes. The latter is terminal RuntimeFailed to avoid replaying an effect merely because its representation failed.

- **Alternatives.** Mandatory Blueprint schemas would add an unnecessary dependency to ordinary JSON. Type-erased business errors or runtime-to-business coercion would lose native error handling. Silent version fallback could execute stale data under a different contract.

- **Evidence and history.** c44e4befedf86b21e6f11007640c81f114122b86 made encoders fallible; 7a04831c3c84ffa3be9ebe4d46b847ae16ed259a retained stored error-contract checks; 749c0dcef14500dd05719d3769b871ea64aa380f introduced the unified facade. Current src/grind/worker.gleam, src/grind/internal/registry.gleam and consumer/test/grind_consumer/codec_test.gleam are authoritative. Earlier laboratory full-route registration is superseded by current global worker ID/version duplicate rejection.

- **Source revisions.** [c44e4bef](https://github.com/gleam-dream/grind/commit/c44e4befedf86b21e6f11007640c81f114122b86), [7a04831c](https://github.com/gleam-dream/grind/commit/7a04831c3c84ffa3be9ebe4d46b847ae16ed259a), [749c0dce](https://github.com/gleam-dream/grind/commit/749c0dcef14500dd05719d3769b871ea64aa380f).
