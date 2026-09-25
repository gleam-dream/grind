UPDATE grind_jobs SET available_at = to_timestamp($1::bigint::double precision / 1000.0) WHERE id = $2 AND storage_owner = $3 AND state = 'scheduled'
