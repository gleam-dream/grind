SELECT
  current_setting('transaction_isolation') AS isolation,
  (SELECT oid::int4 FROM pg_database WHERE datname = current_database()) AS database_oid,
  current_setting('search_path') AS search_path,
  current_setting('lock_timeout') AS lock_timeout,
  current_setting('statement_timeout') AS statement_timeout;
