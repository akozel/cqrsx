#!/usr/bin/env bash
# Start the databases and run back-end, sidecar and front-end in the foreground with log prefixes.
# One Ctrl+C stops everything (the databases keep running; `make down` stops them).
set -euo pipefail
cd "$(dirname "$0")/.."
make up

pids=()
prefix() { local tag="$1"; shift; "$@" 2>&1 | sed -u "s/^/[$tag] /" & pids+=($!); }
stop_all() { echo; echo "stopping..."; kill -INT "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap stop_all INT TERM

prefix backend cargo run -p encore-backend
prefix sidecar cargo run -p encore-sidecar
prefix frontend bash -c "cd front-end && npm run dev"

wait_http() {
  local url="$1" deadline=$((SECONDS + 180))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then echo "timeout waiting for $url" >&2; return 1; fi
    sleep 1
  done
}
wait_http http://127.0.0.1:8080/api/health && echo "[dev] back-end ready"
wait_http http://127.0.0.1:8090/inspect/health && echo "[dev] sidecar ready"
echo
echo "Open http://localhost:5173"
echo
wait
