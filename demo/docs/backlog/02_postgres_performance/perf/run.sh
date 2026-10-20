#!/usr/bin/env bash
# One run of the demo/load stand with backlog epic 02's measurement on top: pg_stat_statements
# (compose.perf.yaml) and the counter snapshots before / after the burst and after the drain (hooks.sh),
# turned into work per command and per task in result.json (tools/collect.mjs).
#
# Usage: the parameters of demo/load/run.sh (./run.sh --help), e.g.
#   ./run.sh announce-storm PREFIX=perf-02- NAME=02a-announce-vus100 VUS=100 POOL=20 PROFILE=db
# plus the epic's own, which also go on to run.sh (so result.json records them):
#   GENERATOR=auto|wrk|k6  the load generator; auto (default) = wrk when generator/scenarios/<test>.lua
#                          exists, else k6. wrk costs a small fraction of k6's CPU per request, so the
#                          generator is not the ceiling.
#   PROFILE=full|db|app    CPU budget: full = the stand's (back-end 4, PostgreSQL 4, MongoDB 3, k6 4);
#                          db = PostgreSQL 2 cores, the database is the bottleneck (tickets on the
#                          database side show in req/s); app = back-end 2 cores (tickets in the back-end).
#   BACKEND_IMAGE=…        run this already built back-end image instead of building the tree, e.g. a
#                          baseline of older code; BUILD=1 is ignored then (it would overwrite the image).
# Results land in demo/load/results/ next to the stand's own runs.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD="$(cd "$HERE/../../../../load" && pwd)"

if [[ "${1:-}" == --check || "${1:-}" == -h || "${1:-}" == --help ]]; then
  exec "$LOAD/run.sh" "$@"
fi

SCRIPT="${1:-}"
GENERATOR=auto PROFILE=full BACKEND_IMAGE=""
for arg in "$@"; do
  case "$arg" in
    GENERATOR=*) GENERATOR="${arg#GENERATOR=}" ;;
    PROFILE=*) PROFILE="${arg#PROFILE=}" ;;
    BACKEND_IMAGE=*) BACKEND_IMAGE="${arg#BACKEND_IMAGE=}" ;;
  esac
done
ARGS=()
for arg in "$@"; do
  if [[ -n "$BACKEND_IMAGE" && "$arg" == BUILD=* ]]; then continue; fi
  ARGS+=("$arg")
done
if [[ "$GENERATOR" == auto ]]; then
  if [[ -f "$HERE/generator/scenarios/$SCRIPT.lua" ]]; then GENERATOR=wrk; else GENERATOR=k6; fi
  ARGS+=("GENERATOR=$GENERATOR")
fi

CTR="${CTR:-$(command -v docker >/dev/null 2>&1 && echo docker || echo podman)}"
case "$GENERATOR" in
  wrk)
    # Rebuilt on every run from cached layers (a second or two); the stand runs it as its k6 service.
    $CTR build -q -t encore-perf-wrk:latest "$HERE/generator" >/dev/null
    export LOAD_K6_IMAGE=encore-perf-wrk:latest
    ;;
  k6) ;;
  *) echo "perf: GENERATOR must be auto, wrk or k6 (got '$GENERATOR')" >&2; exit 2 ;;
esac

case "$PROFILE" in
  full) ;;
  db) export LOAD_CPUS_POSTGRES="${LOAD_CPUS_POSTGRES:-2}" LOAD_CPUSET_POSTGRES="${LOAD_CPUSET_POSTGRES:-4-5}" ;;
  app) export LOAD_CPUS_BACKEND="${LOAD_CPUS_BACKEND:-2}" LOAD_CPUSET_BACKEND="${LOAD_CPUSET_BACKEND:-0-1}" ;;
  *) echo "perf: PROFILE must be full, db or app (got '$PROFILE')" >&2; exit 2 ;;
esac

EXTRA="$HERE/compose.perf.yaml"
if [[ -n "$BACKEND_IMAGE" ]]; then
  export PERF_BACKEND_IMAGE="$BACKEND_IMAGE"
  EXTRA="$EXTRA:$HERE/compose.image.yaml"
fi
export LOAD_COMPOSE_EXTRA="$EXTRA${LOAD_COMPOSE_EXTRA:+:$LOAD_COMPOSE_EXTRA}"
export LOAD_HOOKS="$HERE/hooks.sh"
exec "$LOAD/run.sh" "${ARGS[@]}"
