#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"
output="$(mktemp -d "${TMPDIR:-/tmp}/grind-native.XXXXXX")"
trap 'rm -rf "$output"' EXIT
mapfile -d '' erlang_files < <(git ls-files -z -- '*.erl')
[[ ${#erlang_files[@]} -gt 0 ]] || { echo 'No authored Erlang selected' >&2; exit 1; }
erlc -Werror -pa "$repo_root/build/dev/erlang/pgo/ebin" -o "$output" "${erlang_files[@]}"
mapfile -d '' scripts < <(git ls-files -z -- '*.exs' ':!:scripts/check-exs.exs' ':!:oracle/mix.exs')
[[ ${#scripts[@]} -gt 0 ]] || { echo 'No authored Elixir scripts selected' >&2; exit 1; }
for index in "${!scripts[@]}"; do scripts[index]="$repo_root/${scripts[index]}"; done
(
  cd "$repo_root/oracle"
  mix compile --force --warnings-as-errors
  GRIND_OBAN_TEST_DATABASE_URL="postgres://grind@127.0.0.1:1/compile_only" mix run --no-start "$repo_root/scripts/check-exs.exs" "${scripts[@]}"
)
elixir "$repo_root/scripts/check-exs.exs" "$repo_root/oracle/mix.exs"
