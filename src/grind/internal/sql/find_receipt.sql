SELECT decision, job_id, job_queue, observed_state, worker_id, worker_version, request_sha256 FROM grind_unique_submissions WHERE storage_owner = $1 AND submission_id = $2
