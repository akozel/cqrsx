#!/usr/bin/env bash
# One load-test run on a fresh, isolated stand:
#   down -v, up → wait healthy → k6 (warm-up + measured window) → [outbox drain] → collect → down -v
#
# Usage: ./run.sh <script> [KEY=VALUE ...]        or: ./run.sh --check (tooling and CPU isolation only)
#   <script>        k6/<script>.js: hot-key | announce-storm | spread-bookings
#   NAME=…          results folder suffix (default: the script name)
#   PREFIX=…        prepended to NAME (e.g. c1-): tells one batch of runs from another in the report
#   VUS=…           concurrent virtual users (default 10)
#   DURATION=…      measured window (default 10s); WARMUP=… excluded from the numbers (default 3s)
#   FAN_MODE=…      spread-bookings only: spread (default) | hot (every booking for one fan)
#   WORKERS=…       outbox workers = queue capacity (default 2)
#   DRAIN=1         after the burst, sample transactional_outbox every second until no task is open
#   DRAIN_TIMEOUT=… seconds to wait for the drain (default 900)
#   SNAPSHOT_EVERY=… ENCORE_SNAPSHOT_EVERY (default 10);  RUST_LOG=… back-end log level (default warn)
#   POOL=…          PostgreSQL pool size of the back-end, ENCORE_DATABASE_POOL_SIZE (default 10)
#   PIN=auto|cpuset|quota|0   CPU isolation: pin cores (compose.pin.yaml), quota (compose.quota.yaml)
#                   or none; auto (default) pins when the cpuset controller is available, else quota
#   BUILD=1         rebuild the back-end image first
# Any other KEY=VALUE is passed to k6 as environment (__ENV.KEY).
# Extension points, for measurement tools that live elsewhere (e.g. backlog epic 02, docs/backlog/…/perf/):
#   LOAD_COMPOSE_EXTRA=…  more compose files (`:`-separated), added after the CPU isolation overlay
#   LOAD_SERVICES_EXTRA=… more services (space-separated, defined by those files) started with the stand;
#                         each must be healthy before k6, and its log is saved as <service>.log
#   LOAD_HOOKS=…          a bash file sourced once the run folder exists; each function it defines out of
#                         hook_before_k6, hook_after_k6, hook_after_drain (DRAIN=1 only) and
#                         hook_after_collect is called at that point, with RUN_DIR, compose and log at hand.
#                         A failing hook is logged and does not fail the run.
# Tooling: docker or podman with compose (auto-detected; override with CTR=… COMPOSE=…) and node.
#   LOAD_HOST_DIR=… the host path of this directory when the container engine runs on another machine
#   than this script (a sandbox over the host's podman socket); bind mounts are resolved against it.
# Results: results/<timestamp>-<PREFIX><NAME>/ (result.json, k6.log, stats.tsv, outbox.tsv, backend.log).
# Exit codes: k6's own, 2 = usage or tooling, 3 = the stand did not start.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT=encore-load

usage() { sed -n '2,/^set -euo pipefail/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//' >&2; }

if [[ $# -lt 1 || "$1" == -h || "$1" == --help ]]; then
  usage
  exit 2
fi

# --- tooling --------------------------------------------------------------------------------------
if [[ -z "${CTR:-}" ]]; then
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    CTR=docker
  elif command -v podman >/dev/null 2>&1; then
    CTR=podman
  else
    echo "run: neither docker nor podman found" >&2
    exit 2
  fi
fi
COMPOSE="${COMPOSE:-$CTR compose}"
if ! command -v node >/dev/null 2>&1; then
  echo "run: node is required (tools/collect.mjs assembles result.json)" >&2
  exit 2
fi
if ! $COMPOSE version >/dev/null 2>&1; then
  echo "run: '$COMPOSE version' failed; set COMPOSE=… (e.g. 'podman-compose')" >&2
  exit 2
fi

# --- CPU isolation --------------------------------------------------------------------------------
# Pinning (cpuset) is the better measurement; a quota (cpus) is the fallback when the cpuset
# controller is not delegated to the user, which is the rootless podman default. `info` prints the
# delegated controllers on podman (also behind the docker CLI emulation); real docker prints nothing
# here and runs as root with every controller.
CONTROLLERS="$($CTR info --format '{{.Host.CgroupControllers}}' 2>/dev/null || true)"
# The real docker CLI has no `.Host` in its info template even when it talks to podman; ask the
# libpod API on the socket directly in that case.
LIBPOD_SOCKET=""
if [[ -z "$CONTROLLERS" && "${DOCKER_HOST:-}" == unix://* ]] && command -v curl >/dev/null 2>&1; then
  CONTROLLERS="$(curl -s --max-time 5 --unix-socket "${DOCKER_HOST#unix://}" http://d/v4.0.0/libpod/info 2>/dev/null \
    | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const i=JSON.parse(s);console.log("["+(i.host.cgroupControllers||[]).join(" ")+"]")}catch(_){}})' 2>/dev/null || true)"
  # Remembered for the stats sampler: the docker CLI cannot compute CPU % from podman's compat
  # endpoint (the fields it needs are placeholders), libpod's own stats stream can.
  if [[ -n "$CONTROLLERS" ]]; then LIBPOD_SOCKET="${DOCKER_HOST#unix://}"; fi
fi
has_controller() { [[ -z "$CONTROLLERS" || "$CONTROLLERS" == *"$1"* ]]; }

PIN_MODE="${PIN:-auto}"
if [[ "$PIN_MODE" == 1 ]]; then PIN_MODE=auto; fi
if [[ "$PIN_MODE" == auto ]]; then
  if has_controller cpuset; then
    PIN_MODE=cpuset
  elif has_controller cpu; then
    PIN_MODE=quota
    echo "run: the cpuset controller is not delegated to this user (controllers: $CONTROLLERS): using a CPU quota instead of pinning. See README.md, 'CPU isolation', to enable cpuset." >&2
  else
    PIN_MODE=0
    echo "run: no cpu controller delegated (controllers: $CONTROLLERS): running without CPU isolation" >&2
  fi
fi
if [[ "$PIN_MODE" == cpuset ]] && (( $(nproc) < 15 )) && [[ -z "${LOAD_CPUSET_BACKEND:-}" ]]; then
  echo "run: $(nproc) cores, the default cpusets need 15: using a CPU quota instead (set LOAD_CPUSET_* to pin on fewer cores)" >&2
  PIN_MODE=quota
fi
COMPOSE_FILES=(-f "$HERE/compose.yaml")
case "$PIN_MODE" in
  cpuset) COMPOSE_FILES+=(-f "$HERE/compose.pin.yaml") ;;
  quota) COMPOSE_FILES+=(-f "$HERE/compose.quota.yaml") ;;
  0) ;;
  *) echo "run: PIN must be auto, cpuset, quota or 0 (got '$PIN_MODE')" >&2; exit 2 ;;
esac
IFS=':' read -ra COMPOSE_EXTRA <<<"${LOAD_COMPOSE_EXTRA:-}"
for extra in ${COMPOSE_EXTRA[@]+"${COMPOSE_EXTRA[@]}"}; do
  if [[ -n "$extra" ]]; then COMPOSE_FILES+=(-f "$extra"); fi
done
read -ra SERVICES_EXTRA <<<"${LOAD_SERVICES_EXTRA:-}"
PIN_LABEL="$PIN_MODE"
if [[ "$PIN_LABEL" == 0 ]]; then PIN_LABEL=off; fi

if [[ "$1" == --check ]]; then
  echo "run: tooling ok: $CTR, '$COMPOSE', node $(node --version), $(nproc) cores, cgroup controllers ${CONTROLLERS:-(docker: all)}, cpu isolation: $PIN_LABEL"
  exit 0
fi

SCRIPT="$1"
shift
if [[ ! -f "$HERE/k6/$SCRIPT.js" ]]; then
  echo "run: no k6/$SCRIPT.js" >&2
  exit 2
fi

K6_ENV=()
for pair in "$@"; do
  if [[ "$pair" != *=* ]]; then
    echo "run: expected KEY=VALUE, got '$pair'" >&2
    exit 2
  fi
  export "${pair?}"
  K6_ENV+=(-e "$pair")
done

# shellcheck disable=SC2086  # $COMPOSE is "docker compose" or "podman compose": two words on purpose
compose() { $COMPOSE -p "$PROJECT" "${COMPOSE_FILES[@]}" "$@"; }

export LOAD_OUTBOX_WORKERS="${WORKERS:-2}"
export LOAD_SNAPSHOT_EVERY="${SNAPSHOT_EVERY:-10}"
export LOAD_PG_POOL="${POOL:-10}"
export LOAD_RUST_LOG="${RUST_LOG:-warn}"

RUN_ID="$(date +%Y%m%d-%H%M%S)-${PREFIX:-}${NAME:-$SCRIPT}"
RUN_DIR="$HERE/results/$RUN_ID"
mkdir -p "$RUN_DIR"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$RUN_DIR/run.log" >&2; }
hook() { # point → call hook_<point> from LOAD_HOOKS when it is defined
  if declare -F "hook_$1" >/dev/null; then
    "hook_$1" || log "hook $1 failed (exit $?)"
  fi
}
if [[ -n "${LOAD_HOOKS:-}" ]]; then
  # shellcheck source=/dev/null
  source "$LOAD_HOOKS"
fi

# --- stand ----------------------------------------------------------------------------------------
SAMPLER_PID=
K6_NAME="${PROJECT}-k6"
stop_sampler() {
  if [[ -n "$SAMPLER_PID" ]]; then
    pkill -P "$SAMPLER_PID" 2>/dev/null || true # the curl | node pipeline inside the subshell
    kill "$SAMPLER_PID" 2>/dev/null || true
    SAMPLER_PID=
  fi
}
cleanup() {
  stop_sampler
  $CTR rm -f "$K6_NAME" >/dev/null 2>&1 || true
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

health_of() { # container id → healthy | starting | unhealthy | ""
  $CTR inspect -f '{{with .State.Health}}{{.Status}}{{end}}' "$1" 2>/dev/null || true
}

wait_healthy() {
  local service="$1" timeout="${WAIT_TIMEOUT:-240}" deadline id status
  deadline=$((SECONDS + timeout))
  while true; do
    id="$(compose ps -q "$service" 2>/dev/null | head -n1 || true)"
    status=""
    if [[ -n "$id" ]]; then status="$(health_of "$id")"; fi
    if [[ "$status" == healthy ]]; then return 0; fi
    if (( SECONDS >= deadline )); then
      log "$service is not healthy after ${timeout}s (status: ${status:-no container})"
      compose logs --no-color "$service" 2>/dev/null | tail -n 40 >&2 || true
      return 1
    fi
    sleep 1
  done
}

# One line per container per sample: epoch  name  cpu%  mem. `stats --no-stream` itself takes
# about two seconds (it samples twice for the CPU figure), so the cadence is ~2–3 s, not 1 s.
sample_stats() {
  local ts ids name cpu mem
  if [[ -n "$LIBPOD_SOCKET" ]]; then
    # One JSON document per second with every running container, straight from libpod. CPU is
    # percent of one core (it can exceed 100), MemUsage is bytes; only this project's containers
    # are kept, so a new one (k6) shows up as soon as it starts.
    curl -s -N --unix-socket "$LIBPOD_SOCKET" "http://d/v4.0.0/libpod/containers/stats?stream=true&interval=1&all=true" 2>/dev/null \
      | node -e '
        const rl = require("node:readline").createInterface({ input: process.stdin });
        rl.on("line", (line) => {
          let report;
          try { report = JSON.parse(line); } catch { return; }
          const ts = Math.floor(Date.now() / 1000);
          for (const x of report.Stats || []) {
            if (String(x.Name).startsWith(process.argv[1])) console.log([ts, x.Name, Number(x.CPU).toFixed(2) + "%", String(x.MemUsage)].join("\t"));
          }
        });' "$PROJECT" >> "$RUN_DIR/stats.tsv"
    return
  fi
  while true; do
    ts="$(date +%s)"
    ids="$($CTR ps -q --filter "name=${PROJECT}" 2>/dev/null | tr '\n' ' ')"
    if [[ -n "${ids// /}" ]]; then
      # shellcheck disable=SC2086
      $CTR stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' $ids 2>/dev/null \
        | while IFS=$'\t' read -r name cpu mem; do
            if [[ -n "$name" && -n "$mem" ]]; then printf '%s\t%s\t%s\t%s\n' "$ts" "$name" "$cpu" "$mem"; fi
          done >> "$RUN_DIR/stats.tsv" || true
    fi
    sleep 1
  done
}

log "stand: $CTR, cpu isolation $PIN_LABEL, outbox workers $LOAD_OUTBOX_WORKERS, pg pool $LOAD_PG_POOL, snapshot every $LOAD_SNAPSHOT_EVERY${LOAD_SERVICES_EXTRA:+, also $LOAD_SERVICES_EXTRA}"
compose down -v --remove-orphans >/dev/null 2>&1 || true
$CTR rm -f "$K6_NAME" >/dev/null 2>&1 || true
if [[ "${BUILD:-0}" != 0 ]]; then
  log "building the back-end image"
  compose build backend || { log "the back-end image did not build"; exit 3; }
fi
compose up -d postgres mongo mail backend ${SERVICES_EXTRA[@]+"${SERVICES_EXTRA[@]}"} || { log "the stand did not start"; exit 3; }
for service in backend ${SERVICES_EXTRA[@]+"${SERVICES_EXTRA[@]}"}; do
  wait_healthy "$service" || exit 3
done
: > "$RUN_DIR/stats.tsv"
sample_stats &
SAMPLER_PID=$!
sleep 3 # a baseline sample of the idle stand
hook before_k6

# --- k6 -------------------------------------------------------------------------------------------
RUN_SLUG="$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-' | cut -c1-24)"
log "k6 $SCRIPT: VUS=${VUS:-10} DURATION=${DURATION:-10s} WARMUP=${WARMUP:-3s} FAN_MODE=${FAN_MODE:-spread}"
BURST_START=$(date +%s)
set +e
compose run --name "$K6_NAME" --no-deps ${K6_ENV[@]+"${K6_ENV[@]}"} -e "RUN=$RUN_SLUG" \
  k6 run /scripts/"$SCRIPT".js 2>&1 | tee "$RUN_DIR/k6.log"
K6_EXIT=${PIPESTATUS[0]}
set -e
BURST_END=$(date +%s)
hook after_k6
for f in result.json summary.json; do
  $CTR cp "$K6_NAME:/home/k6/$f" "$RUN_DIR/$f" 2>/dev/null || log "k6 did not produce $f"
done
$CTR rm -f "$K6_NAME" >/dev/null 2>&1 || true
if (( K6_EXIT != 0 )); then log "k6 exited with $K6_EXIT (see k6.log)"; fi

# --- outbox drain ---------------------------------------------------------------------------------
outbox_counts() { # open  completed  failed  total
  compose exec -T postgres psql -U cqrsx -d encore -Atc \
    "select count(*) filter (where status in ('pending','processing')),
            count(*) filter (where status = 'completed'),
            count(*) filter (where status = 'failed'),
            count(*) from transactional_outbox" 2>/dev/null | tr '|' '\t'
}

DRAIN_FIRST="" DRAIN_LAST="" DRAIN_SECONDS=""
if [[ "${DRAIN:-0}" != 0 ]]; then
  DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-900}"
  log "draining the outbox (timeout ${DRAIN_TIMEOUT}s)"
  printf 'epoch\topen\tcompleted\tfailed\ttotal\n' > "$RUN_DIR/outbox.tsv"
  deadline=$((SECONDS + DRAIN_TIMEOUT))
  open=""
  while true; do
    row="$(outbox_counts || true)"
    if [[ -n "$row" ]]; then
      printf '%s\t%s\n' "$(date +%s)" "$row" >> "$RUN_DIR/outbox.tsv"
      if [[ -z "$DRAIN_FIRST" ]]; then DRAIN_FIRST="$row"; fi
      DRAIN_LAST="$row"
      open="${row%%$'\t'*}"
      if [[ "$open" == 0 ]]; then break; fi
    fi
    if (( SECONDS >= deadline )); then
      log "drain timeout, still open: ${open:-?}"
      break
    fi
    sleep 1
  done
  DRAIN_SECONDS=$(( $(date +%s) - BURST_END ))
  hook after_drain
  if [[ -n "$DRAIN_FIRST" ]]; then
    read -r _open0 done0 _failed0 _total0 <<<"$DRAIN_FIRST"
    read -r _open1 done1 failed1 _total1 <<<"$DRAIN_LAST"
    log "drain: ${DRAIN_SECONDS} s, $((done1 - done0)) tasks completed after the burst, ${failed1} failed"
  fi
fi

# --- collect --------------------------------------------------------------------------------------
stop_sampler
compose logs --no-color backend > "$RUN_DIR/backend.log" 2>&1 || true
for service in ${SERVICES_EXTRA[@]+"${SERVICES_EXTRA[@]}"}; do
  compose logs --no-color "$service" > "$RUN_DIR/$service.log" 2>&1 || true
done

# Exact peak memory from cgroup v2, read inside each container; empty on cgroup v1 (then the report
# falls back to the maximum sampled by `stats`).
peak_of() {
  local peak
  peak="$(compose exec -T "$1" cat /sys/fs/cgroup/memory.peak 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "$peak" =~ ^[0-9]+$ ]]; then printf '%s' "$peak"; fi
}

# The CPU budget of each container in cores, from the isolation overlay in use ("0-1,4" → 3 cores).
cores_in() {
  local total=0 part lo hi
  IFS=',' read -ra parts <<<"$1"
  for part in "${parts[@]}"; do
    if [[ "$part" =~ ^([0-9]+)-([0-9]+)$ ]]; then
      lo="${BASH_REMATCH[1]}" hi="${BASH_REMATCH[2]}"
      total=$((total + hi - lo + 1))
    elif [[ "$part" =~ ^[0-9]+$ ]]; then
      total=$((total + 1))
    fi
  done
  printf '%s' "$total"
}
budget_of() { # service → cores, empty when not isolated
  case "$PIN_MODE" in
    cpuset)
      case "$1" in
        backend) cores_in "${LOAD_CPUSET_BACKEND:-0-3}" ;;
        postgres) cores_in "${LOAD_CPUSET_POSTGRES:-4-7}" ;;
        mongo) cores_in "${LOAD_CPUSET_MONGO:-8-10}" ;;
        k6) cores_in "${LOAD_CPUSET_K6:-11-14}" ;;
      esac ;;
    quota)
      case "$1" in
        backend) printf '%s' "${LOAD_CPUS_BACKEND:-4}" ;;
        postgres) printf '%s' "${LOAD_CPUS_POSTGRES:-4}" ;;
        mongo) printf '%s' "${LOAD_CPUS_MONGO:-3}" ;;
        k6) printf '%s' "${LOAD_CPUS_K6:-4}" ;;
      esac ;;
  esac
}

LOADC_RUN="$RUN_ID" LOADC_SCRIPT="$SCRIPT" LOADC_PARAMS="$*" LOADC_CTR="$CTR" LOADC_PIN="$PIN_LABEL" \
  LOADC_BUDGET_BACKEND="$(budget_of backend)" LOADC_BUDGET_POSTGRES="$(budget_of postgres)" LOADC_BUDGET_MONGO="$(budget_of mongo)" LOADC_BUDGET_K6="$(budget_of k6)" \
  LOADC_GIT="$(git -C "$HERE" rev-parse --short HEAD 2>/dev/null || echo unknown)" \
  LOADC_WORKERS="$LOAD_OUTBOX_WORKERS" LOADC_SNAPSHOT="$LOAD_SNAPSHOT_EVERY" LOADC_POOL="$LOAD_PG_POOL" LOADC_K6_EXIT="$K6_EXIT" \
  LOADC_BURST_START="$BURST_START" LOADC_BURST_END="$BURST_END" \
  LOADC_PEAK_BACKEND="$(peak_of backend)" LOADC_PEAK_POSTGRES="$(peak_of postgres)" LOADC_PEAK_MONGO="$(peak_of mongo)" \
  LOADC_DRAIN_FIRST="$DRAIN_FIRST" LOADC_DRAIN_LAST="$DRAIN_LAST" LOADC_DRAIN_SECONDS="$DRAIN_SECONDS" \
  node "$HERE/tools/collect.mjs" "$RUN_DIR" 2>&1 | tee -a "$RUN_DIR/run.log" >&2
hook after_collect

log "results: $RUN_DIR"
exit "$K6_EXIT"
