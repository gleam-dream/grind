@external(erlang, "grind_test_env", "database_url")
pub fn database_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "queue_database_url")
pub fn queue_database_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "owner_a_url")
pub fn owner_a_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "user_schema_fallback_url")
pub fn user_schema_fallback_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "migration_collision_submissions_url")
pub fn migration_collision_submissions_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "migration_collision_resolutions_url")
pub fn migration_collision_resolutions_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_bad_url")
pub fn schema_bad_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_fresh_url")
pub fn schema_fresh_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_markers_url")
pub fn schema_markers_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_jobs_url")
pub fn schema_missing_jobs_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_migrations_url")
pub fn schema_missing_migrations_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_resolutions_url")
pub fn schema_missing_resolutions_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_acknowledgements_url")
pub fn schema_missing_acknowledgements_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_attempt_sequence_url")
pub fn schema_missing_attempt_sequence_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_unique_submissions_url")
pub fn schema_missing_unique_submissions_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_missing_fk_url")
pub fn schema_missing_fk_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_atomic_url")
pub fn schema_atomic_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_concurrent_url")
pub fn schema_concurrent_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_partial_url")
pub fn schema_partial_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_upgrade_url")
pub fn schema_upgrade_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_upgrade_fresh_url")
pub fn schema_upgrade_fresh_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_future_foreign_url")
pub fn schema_future_foreign_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_shape_url")
pub fn schema_shape_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_mixed_case_url")
pub fn schema_mixed_case_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "repeatable_read_url")
pub fn repeatable_read_url() -> Result(String, Nil)

/// A database dedicated to the global `quarantine_expired` test: since that
/// operation sweeps every expired `executing` row across the whole schema
/// (not scoped to one queue), running it against the shared
/// `GRIND_TEST_DATABASE_URL` database would make the test's own row counts
/// depend on whatever other tests in this suite happen to run first and
/// leave behind — a dedicated database is a dedicated schema, and therefore
/// a dedicated Grind installation (see README, "Isolation").
@external(erlang, "grind_test_env", "quarantine_url")
pub fn quarantine_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "mark_database_test_executed")
pub fn mark_database_test_executed(contract: String) -> Nil

@external(erlang, "grind_test_env", "monotonic_ms")
pub fn monotonic_ms() -> Int

@external(erlang, "grind_test_env", "unique_test_run_id")
pub fn unique_test_run_id() -> Int

@external(erlang, "grind_test_env", "prune_url")
pub fn prune_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "prune_owner_b_url")
pub fn prune_owner_b_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "postgres_log_path")
pub fn postgres_log_path() -> Result(String, Nil)
