#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
mkdir -p results
export BENCH_RUNNER_UID="$(id -u)"
exec docker compose run --rm runner "$@"
