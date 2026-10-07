SELECT
  set_config('search_path', $1, true) AS search_path,
  set_config('lock_timeout', $2, true) AS lock_timeout,
  set_config('statement_timeout', $3, true) AS statement_timeout;
