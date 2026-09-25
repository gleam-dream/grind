SELECT input::text, input_version, storage_owner, queue, worker_id, worker_version FROM grind_jobs WHERE id = $1
