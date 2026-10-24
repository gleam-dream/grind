SELECT input::text, input_version, queue, worker_id, worker_version FROM grind_jobs WHERE id = $1
