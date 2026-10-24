SELECT queue, worker_id, worker_version, input_version, output_version, error_version FROM grind_jobs WHERE id = $1
