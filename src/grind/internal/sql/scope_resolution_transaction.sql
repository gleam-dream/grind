SELECT
  set_config('search_path', $1, true) AS search_path,
  set_config('lock_timeout', LEAST(
    COALESCE(NULLIF(extract(epoch FROM current_setting('lock_timeout')::interval) * 1000, 0), $2::int),
    $2::int)::int::text, true) AS lock_timeout,
  set_config('statement_timeout', LEAST(
    COALESCE(NULLIF(extract(epoch FROM current_setting('statement_timeout')::interval) * 1000, 0), $2::int),
    $2::int)::int::text, true) AS statement_timeout;
