SELECT queue, worker_id, worker_version, state, output, output_version, error, error_version, failure_description, failure_cause FROM grind_jobs WHERE id = $1
