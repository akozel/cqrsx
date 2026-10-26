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
#   NODES=1|2              back-end nodes (default 1). Two nodes split the profile's back-end CPU budget
#                          and the outbox workers (WORKERS, the stand's total, default 2; must be even)
#                          between them, so one node and two run on the same back-end CPU and drain the
#                          outbox with the same number of workers.
#   BALANCE=none|round_robin|key   how requests reach the nodes: none (default) = straight to the one
#                          node, as always; round_robin = through nginx, in turn; key = through nginx,
#                          every request for one aggregate to the same node. NODES=2 needs a balancer;
#                          NODES=1 BALANCE=round_robin is one node behind nginx, the balancer's own cost.
#                          With a balancer the stand runs under a CPU quota (PIN=quota): nginx and the
#                          second node get quotas too. Compare such runs with single-node runs taken
#                          with PIN=quota as well when the machine could pin cores.
# Results land in demo/load/results/ next to the stand's own runs.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD="$(cd "$HERE/../../../../load" && pwd)"

if [[ "${1:-}" == --check || "${1:-}" == -h || "${1:-}" == --help ]]; then
  exec "$LOAD/run.sh" "$@"
fi

SCRIPT="${1:-}"
GENERATOR=auto PROFILE=full BACKEND_IMAGE="" NODES=1 BALANCE=none WORKERS=2
for arg in "$@"; do
  case "$arg" in
    GENERATOR=*) GENERATOR="${arg#GENERATOR=}" ;;
    PROFILE=*) PROFILE="${arg#PROFILE=}" ;;
    BACKEND_IMAGE=*) BACKEND_IMAGE="${arg#BACKEND_IMAGE=}" ;;
    NODES=*) NODES="${arg#NODES=}" ;;
    BALANCE=*) BALANCE="${arg#BALANCE=}" ;;
    WORKERS=*) WORKERS="${arg#WORKERS=}" ;;
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
unset PERF_BACKEND_IMAGE
if [[ -n "$BACKEND_IMAGE" ]]; then
  export PERF_BACKEND_IMAGE="$BACKEND_IMAGE"
  EXTRA="$EXTRA:$HERE/compose.image.yaml"
fi

# Nodes and the balancer (ticket 17). The defaults leave the stand exactly as it is without them.
case "$NODES:$BALANCE" in
  1:none) ;;
  1:round_robin) PERF_UPSTREAM=single ;;
  2:round_robin|2:key) PERF_UPSTREAM="$BALANCE" ;;
  2:none) echo "perf: NODES=2 needs a balancer: BALANCE=round_robin or BALANCE=key" >&2; exit 2 ;;
  1:key) echo "perf: BALANCE=key spreads aggregates over nodes, it needs NODES=2" >&2; exit 2 ;;
  *) echo "perf: NODES must be 1 or 2 and BALANCE none, round_robin or key (got NODES=$NODES BALANCE=$BALANCE)" >&2; exit 2 ;;
esac
if [[ -n "${PERF_UPSTREAM:-}" ]]; then
  # The balancer and the second node get CPU quotas (compose.balancer.yaml, compose.node2.yaml), so the
  # whole stand runs under quotas: pinning some containers and capping others would not compare.
  if [[ "${PIN:-auto}" == cpuset ]]; then
    echo "perf: a balancer runs under a CPU quota; PIN=cpuset is not supported with BALANCE=$BALANCE" >&2
    exit 2
  fi
  if [[ "${PIN:-auto}" == auto || "${PIN:-}" == 1 ]]; then export PIN=quota; fi
  export PERF_UPSTREAM
  EXTRA="$EXTRA:$HERE/compose.balancer.yaml"
  services="balancer"
  if [[ "$NODES" == 2 ]]; then
    # The profile's back-end budget (compose.quota.yaml default: 4 cores), split between the nodes.
    LOAD_CPUS_BACKEND="$(awk -v total="${LOAD_CPUS_BACKEND:-4}" 'BEGIN { printf "%g", total / 2 }')"
    export LOAD_CPUS_BACKEND
    # The outbox workers too: every node polls and drains the queue with its own workers.
    if ! [[ "$WORKERS" =~ ^[0-9]+$ ]] || (( WORKERS < 2 || WORKERS % 2 != 0 )); then
      echo "perf: with NODES=2, WORKERS is the stand's total and must be even, at least 2 (got '$WORKERS')" >&2
      exit 2
    fi
    kept=()
    for arg in "${ARGS[@]}"; do
      if [[ "$arg" != WORKERS=* ]]; then kept+=("$arg"); fi
    done
    ARGS=("${kept[@]}" "WORKERS=$((WORKERS / 2))")
    export PERF_WORKERS_TOTAL="$WORKERS"
    EXTRA="$EXTRA:$HERE/compose.node2.yaml"
    services="backend-2 balancer"
  fi
  export LOAD_SERVICES_EXTRA="$services${LOAD_SERVICES_EXTRA:+ $LOAD_SERVICES_EXTRA}"
  export PERF_CPUS_BALANCER="${PERF_CPUS_BALANCER:-1}"
fi
# The probe that reads MongoDB's operation counters for hooks.sh (compose.perf.yaml).
export LOAD_SERVICES_EXTRA="${LOAD_SERVICES_EXTRA:+$LOAD_SERVICES_EXTRA }mongo-probe"
export LOAD_COMPOSE_EXTRA="$EXTRA${LOAD_COMPOSE_EXTRA:+:$LOAD_COMPOSE_EXTRA}"
export LOAD_HOOKS="$HERE/hooks.sh"
exec "$LOAD/run.sh" "${ARGS[@]}"
