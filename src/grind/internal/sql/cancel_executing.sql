UPDATE grind_jobs SET cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp()) WHERE id = $1 AND state IN ('executing', 'uncertain') RETURNING id
