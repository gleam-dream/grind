#!/usr/bin/env bash
set -euo pipefail
# Disposable-cluster harness validation and durable-completion smoke.
exec bash "$(dirname "$0")/bench-postgres.sh" "$@"
