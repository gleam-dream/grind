SELECT storage_owner, queue, worker_id, worker_version, state FROM grind_jobs WHERE id = $1 FOR UPDATE
