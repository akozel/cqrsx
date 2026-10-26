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
#   RATE=…          k6's open model: start that many iterations a second, served by VUS users; an iteration
#                   that finds every user busy is dropped (the back-end did not keep up)
#   SOAK=…          a run at a constant rate (needs RATE=…, replaces DURATION): the measured window, e.g. 4m or
#                   290s; with WARMUP at most LOAD_SOAK_MAX_S (default 300) seconds. Every SLICE=… (default 10s)
#                   the outbox by status and, through hook_slice, the database's growth go to soak.tsv; k6
#                   reports requests, accepted commands and latency per slice. Refused when the container
#                   storage has less than LOAD_MIN_FREE_GB (default 10) free: minutes of writes are gigabytes
#   FAN_MODE=…      spread-bookings only: spread (default) | hot (every booking for one fan)
#   WORKERS=…       outbox workers = queue capacity (default 2)
#   DRAIN=1         after the burst, sample transactional_outbox every second until no task is open
#   DRAIN_TIMEOUT=… seconds to wait for the drain (default 900)
#   SNAPSHOT_EVERY=… ENCORE_SNAPSHOT_EVERY (default 10);  RUST_LOG=… back-end log level (default warn)
#   LOCK_MAX_WAIT_MS=… LOCK_MAX_WAITERS=…  the aggregate lock's limits, ENCORE_COMMAND_LOCK_* (default: the back-end's)
#   PROJECTION_BUFFER=… PROJECTION_MAX_BATCH=… PROJECTION_SETTLE_MS=…  its buffer bounds and the pause after a
#                   commit's wake-up, ENCORE_PROJECTION_* (default: the back-end's)
#   VERIFY=1        after the burst (and the drain): wait until no chain task is open and the projections have
#                   caught up with the journal, then compare every document with the history of its aggregate
#                   (GET /api/projections/verify) → verify.json; the run passes with no difference and no
#                   document behind
#   VERIFY_TIMEOUT=… seconds to wait for the chain tasks and the projections first (default 300)
#   VERIFY_SERVICE=… the back-end service verify and the catch-up wait ask (default: the first one running)
#   LOAD_CHAOS=…    a bash file with `chaos` (run in the background when k6 starts), see chaos/ and its
#                   helpers below; its steps are written to chaos.tsv
#   POOL=…          PostgreSQL pool size of the back-end, ENCORE_DATABASE_POOL_SIZE (default 10)
#   MONGO_POOL=…    MongoDB client pool of the back-end, ENCORE_MONGODB_POOL_SIZE (default: the back-end's)
#   PIN=auto|cpuset|quota|0   CPU isolation: pin cores (compose.pin.yaml), quota (compose.quota.yaml)
#                   or none; auto (default) pins when the cpuset controller is available, else quota
#   BUILD=1         rebuild the back-end image first
# Any other KEY=VALUE is passed to k6 as environment (__ENV.KEY).
# Extension points, for measurement tools that live elsewhere (e.g. backlog epic 02, docs/backlog/…/perf/):
#   LOAD_COMPOSE_EXTRA=…  more compose files (`:`-separated), added after the CPU isolation overlay
#   LOAD_SERVICES_EXTRA=… more services (space-separated, defined by those files) started with the stand;
#                         each must be healthy before k6, and its log is saved as <service>.log
#   LOAD_HOOKS=…          a bash file sourced once the run folder exists; each function it defines out of
#                         hook_before_k6, hook_after_k6, hook_after_drain (DRAIN=1 only), hook_slice EPOCH
#                         (SOAK=… only, every slice) and hook_after_collect is called at that point, with
#                         RUN_DIR, compose and log at hand. A failing hook is logged and does not fail the run.
# Tooling: docker or podman with compose (auto-detected; override with CTR=… COMPOSE=…) and node.
#   LOAD_HOST_DIR=… the host path of this directory when the container engine runs on another machine
#   than this script (a sandbox over the host's podman socket); bind mounts are resolved against it.
# The chaos file may use: chaos_at SECONDS (sleep until that many seconds after k6 started), chaos_step TEXT
# (a line in chaos.tsv), api_call CURL_ARGS… (curl inside the first running back-end), compose, log, RUN_DIR,
# WARMUP_S, DURATION_S, and may define chaos_cleanup (called before the stand goes down: undo pauses).
# Results: results/<timestamp>-<PREFIX><NAME>/ (result.json, k6.log, stats.tsv, outbox.tsv, projections.jsonl,
# soak.tsv, chaos.tsv, ledger.tsv, backend.log).
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

# Free space of the container storage in GB: the filesystem of podman's graph root, from its info (the
# podman CLI, or libpod's API behind the docker CLI); empty when the engine does not say (real docker).
free_gb() {
  local info=""
  if [[ "$CTR" == podman ]]; then
    info="$(podman info --format json 2>/dev/null || true)"
  elif [[ "${DOCKER_HOST:-}" == unix://* ]] && command -v curl >/dev/null 2>&1; then
    info="$(curl -s --max-time 5 --unix-socket "${DOCKER_HOST#unix://}" http://d/v4.0.0/libpod/info 2>/dev/null || true)"
  fi
  if [[ -z "$info" ]]; then return 0; fi
  printf '%s' "$info" | node -e '
    let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
      try {
        const store = JSON.parse(s).store;
        const free = (store.graphRootAllocated - store.graphRootUsed) / 1e9;
        if (Number.isFinite(free)) console.log(free.toFixed(1));
      } catch {}
    });' 2>/dev/null || true
}

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
  FREE="$(free_gb)"
  echo "run: tooling ok: $CTR, '$COMPOSE', node $(node --version), $(nproc) cores, cgroup controllers ${CONTROLLERS:-(docker: all)}, cpu isolation: $PIN_LABEL, container storage ${FREE:-?} GB free"
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

seconds_of() { # 290s | 4m | 290 → seconds; nothing for anything else
  if [[ "$1" =~ ^([0-9]+)m$ ]]; then
    printf '%s' $(( BASH_REMATCH[1] * 60 ))
  elif [[ "$1" =~ ^([0-9]+)s?$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
  fi
}

# A soak run: the measured window at a constant rate, sliced for the figures over time.
SLICE_S=""
if [[ -n "${SOAK:-}" ]]; then
  if [[ -z "${RATE:-}" ]]; then echo "run: SOAK=… needs RATE=… (iterations a second)" >&2; exit 2; fi
  if [[ -n "${DURATION:-}" ]]; then echo "run: SOAK=… is the measured window; leave DURATION out" >&2; exit 2; fi
  soak_s="$(seconds_of "$SOAK")" warmup_s="$(seconds_of "${WARMUP:-3s}")" SLICE_S="$(seconds_of "${SLICE:-10s}")"
  if [[ -z "$soak_s" || -z "$warmup_s" || -z "$SLICE_S" || "$SLICE_S" == 0 ]]; then
    echo "run: SOAK, WARMUP and SLICE must be durations such as 290s or 4m (got SOAK=$SOAK WARMUP=${WARMUP:-3s} SLICE=${SLICE:-10s})" >&2
    exit 2
  fi
  if (( warmup_s + soak_s > ${LOAD_SOAK_MAX_S:-300} )); then
    echo "run: WARMUP + SOAK is $((warmup_s + soak_s)) s, more than LOAD_SOAK_MAX_S=${LOAD_SOAK_MAX_S:-300}" >&2
    exit 2
  fi
  FREE="$(free_gb)"
  if [[ -n "$FREE" ]] && awk -v free="$FREE" -v min="${LOAD_MIN_FREE_GB:-10}" 'BEGIN { exit !(free < min) }'; then
    echo "run: the container storage has $FREE GB free; a soak run needs LOAD_MIN_FREE_GB=${LOAD_MIN_FREE_GB:-10}" >&2
    exit 2
  fi
  export DURATION="$SOAK"
  K6_ENV+=(-e "DURATION=$SOAK")
fi

# shellcheck disable=SC2086  # $COMPOSE is "docker compose" or "podman compose": two words on purpose
compose() { $COMPOSE -p "$PROJECT" "${COMPOSE_FILES[@]}" "$@"; }

export LOAD_OUTBOX_WORKERS="${WORKERS:-2}"
export LOAD_SNAPSHOT_EVERY="${SNAPSHOT_EVERY:-10}"
export LOAD_PG_POOL="${POOL:-10}" LOAD_MONGO_POOL="${MONGO_POOL:-}"
export LOAD_RUST_LOG="${RUST_LOG:-warn}"
# Empty = the back-end's own default; an image older than these settings ignores them.
export LOAD_LOCK_MAX_WAIT_MS="${LOCK_MAX_WAIT_MS:-}" LOAD_LOCK_MAX_WAITERS="${LOCK_MAX_WAITERS:-}"
export LOAD_PROJECTION_BUFFER="${PROJECTION_BUFFER:-}" LOAD_PROJECTION_MAX_BATCH="${PROJECTION_MAX_BATCH:-}"
export LOAD_PROJECTION_SETTLE_MS="${PROJECTION_SETTLE_MS:-}"

RUN_ID="$(date +%Y%m%d-%H%M%S)-${PREFIX:-}${NAME:-$SCRIPT}"
RUN_DIR="$HERE/results/$RUN_ID"
mkdir -p "$RUN_DIR"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$RUN_DIR/run.log" >&2; }
hook() { # point [args…] → call hook_<point> from LOAD_HOOKS with the args when it is defined
  if declare -F "hook_$1" >/dev/null; then
    "hook_$1" "${@:2}" || log "hook $1 failed (exit $?)"
  fi
}
if [[ -n "${LOAD_HOOKS:-}" ]]; then
  # shellcheck source=/dev/null
  source "$LOAD_HOOKS"
fi

# --- stand ----------------------------------------------------------------------------------------
SAMPLER_PID= STATUS_PID= CHAOS_PID= SLICES_PID=
K6_NAME="${PROJECT}-k6"
stop_slices() {
  if [[ -n "$SLICES_PID" ]]; then
    pkill -P "$SLICES_PID" 2>/dev/null || true
    kill "$SLICES_PID" 2>/dev/null || true
    wait "$SLICES_PID" 2>/dev/null || true
    SLICES_PID=
  fi
}
stop_sampler() {
  stop_slices
  if [[ -n "$SAMPLER_PID" ]]; then
    pkill -P "$SAMPLER_PID" 2>/dev/null || true # the curl | node pipeline inside the subshell
    kill "$SAMPLER_PID" 2>/dev/null || true
    SAMPLER_PID=
  fi
  if [[ -n "$STATUS_PID" ]]; then
    pkill -P "$STATUS_PID" 2>/dev/null || true
    kill "$STATUS_PID" 2>/dev/null || true
    STATUS_PID=
  fi
}
stop_chaos() {
  if [[ -n "$CHAOS_PID" ]]; then
    pkill -P "$CHAOS_PID" 2>/dev/null || true
    kill "$CHAOS_PID" 2>/dev/null || true
    CHAOS_PID=
  fi
  if declare -F chaos_cleanup >/dev/null; then chaos_cleanup || true; fi
}
cleanup() {
  stop_sampler
  stop_chaos
  $CTR rm -f "$K6_NAME" >/dev/null 2>&1 || true
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 143' TERM INT # a `timeout` around a run still tears the stand down

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

# --- the back-end nodes and the projections' status ------------------------------------------------------
# The back-end services of this stand, the first one first: `backend`, and `backend-2` of a two-node stand.
BACKENDS=(backend)
for service in ${SERVICES_EXTRA[@]+"${SERVICES_EXTRA[@]}"}; do
  if [[ "$service" == backend-* ]]; then BACKENDS+=("$service"); fi
done
running() { # service → 0 when its container is running
  local id
  id="$(compose ps -q "$1" 2>/dev/null | head -n1 || true)"
  [[ -n "$id" && "$($CTR inspect -f '{{.State.Running}}' "$id" 2>/dev/null || true)" == true ]]
}
live_service() { # the service verify and the catch-up wait ask
  if [[ -n "${VERIFY_SERVICE:-}" ]]; then printf '%s' "$VERIFY_SERVICE"; return; fi
  local service
  for service in "${BACKENDS[@]}"; do
    if running "$service"; then printf '%s' "$service"; return; fi
  done
  printf '%s' "${BACKENDS[0]}"
}
api_call() { compose exec -T "$(live_service)" curl -sS --max-time "${API_MAX_TIME:-10}" "$@"; }

# One line per second in projections.jsonl: {"t": epoch, "status": GET /api/projections/status}, or without
# `status` for a second it did not answer. One long-lived `exec` per node (a curl loop inside the container):
# an exec per second would add CPU to the back-end's cgroup only on the side being measured. When the node dies
# (a killed owner), the next node takes over. An image from before ticket 04 answers without `projections`;
# the collector ignores those lines.
STATUS_LOOP='while true; do curl -s --max-time 2 http://127.0.0.1:8080/api/projections/status || true; echo; sleep 1; done'
sample_status() {
  local i=0 service
  while true; do
    service="${BACKENDS[$((i % ${#BACKENDS[@]}))]}"
    compose exec -T "$service" sh -c "$STATUS_LOOP" 2>/dev/null \
      | node -e '
        const rl = require("node:readline").createInterface({ input: process.stdin });
        rl.on("line", (line) => {
          let status = null;
          try { status = JSON.parse(line); } catch { /* a second without an answer */ }
          console.log(JSON.stringify({ t: Math.floor(Date.now() / 1000), status }));
        });' >> "$RUN_DIR/projections.jsonl" || true
    i=$((i + 1))
    sleep 1
  done
}
projections_caught_up() { # 0 when every projection has no lag and is live; also 0 for an image without projections
  api_call http://127.0.0.1:8080/api/projections/status 2>/dev/null | node -e '
    let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
      try {
        const projections = JSON.parse(s).projections;
        if (!Array.isArray(projections)) process.exit(0);
        process.exit(projections.every((p) => (p.lag ?? 0) === 0 && p.state === "live") ? 0 : 1);
      } catch { process.exit(1); }
    });'
}

# --- soak slices ------------------------------------------------------------------------------------
# SOAK=… only: one slice every SLICE_S seconds from k6's start and one more at its end. A slice writes a
# `epoch stand slice <n>` line and the outbox by status (`epoch stand outbox_<status> <tasks>`) to soak.tsv,
# then calls hook_slice EPOCH; a status without tasks has no line.
slice_once() { # number
  local epoch status n
  epoch=$(date +%s)
  printf '%s\tstand\tslice\t%s\n' "$epoch" "$1" >> "$RUN_DIR/soak.tsv"
  compose exec -T -e PGAPPNAME=load-stand postgres psql -U cqrsx -d encore -X -At -F $'\t' \
    -c "select /* load-stand */ status, count(*) from transactional_outbox group by status" 2>/dev/null \
    | while IFS=$'\t' read -r status n; do
        if [[ -n "$status" ]]; then printf '%s\tstand\toutbox_%s\t%s\n' "$epoch" "$status" "$n"; fi
      done >> "$RUN_DIR/soak.tsv" || true
  hook slice "$epoch"
}
sample_slices() {
  local i=0 start target now
  start=$(date +%s)
  while true; do
    target=$((start + i * SLICE_S))
    now=$(date +%s)
    if (( target > now )); then sleep $((target - now)); fi
    slice_once "$i"
    i=$((i + 1))
  done
}

# --- chaos ------------------------------------------------------------------------------------------
WARMUP_S="$(seconds_of "${WARMUP:-3s}")"; WARMUP_S="${WARMUP_S:-3}"
DURATION_S="$(seconds_of "${DURATION:-10s}")"; DURATION_S="${DURATION_S:-10}"
K6_STARTED=0
chaos_at() { # seconds after k6 started
  local now wait
  now=$(date +%s)
  wait=$((K6_STARTED + $1 - now))
  if (( wait > 0 )); then sleep "$wait"; fi
}
chaos_step() { printf '%s\t%s\n' "$(date +%s)" "$*" >> "$RUN_DIR/chaos.tsv"; log "chaos: $*"; }
if [[ -n "${LOAD_CHAOS:-}" ]]; then
  # shellcheck source=/dev/null
  source "$LOAD_CHAOS"
  if ! declare -F chaos >/dev/null; then log "LOAD_CHAOS=$LOAD_CHAOS defines no function 'chaos'"; exit 2; fi
fi

log "stand: $CTR, cpu isolation $PIN_LABEL, outbox workers $LOAD_OUTBOX_WORKERS, pg pool $LOAD_PG_POOL${LOAD_MONGO_POOL:+, mongo pool $LOAD_MONGO_POOL}, snapshot every $LOAD_SNAPSHOT_EVERY${LOAD_PROJECTION_MAX_BATCH:+, projection batch $LOAD_PROJECTION_MAX_BATCH}${LOAD_PROJECTION_SETTLE_MS:+, settle $LOAD_PROJECTION_SETTLE_MS ms}${LOAD_SERVICES_EXTRA:+, also $LOAD_SERVICES_EXTRA}"
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
: > "$RUN_DIR/projections.jsonl"
sample_status &
STATUS_PID=$!
sleep 3 # a baseline sample of the idle stand
hook before_k6

# --- k6 -------------------------------------------------------------------------------------------
RUN_SLUG="$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-' | cut -c1-24)"
log "k6 $SCRIPT: VUS=${VUS:-10} DURATION=${DURATION:-10s} WARMUP=${WARMUP:-3s} FAN_MODE=${FAN_MODE:-spread}${RATE:+ RATE=$RATE}${SOAK:+ SOAK=$SOAK, a slice every ${SLICE_S} s}"
BURST_START=$(date +%s)
K6_STARTED=$BURST_START
if declare -F chaos >/dev/null; then
  chaos &
  CHAOS_PID=$!
fi
if [[ -n "$SLICE_S" ]]; then
  : > "$RUN_DIR/soak.tsv"
  sample_slices &
  SLICES_PID=$!
fi
set +e
compose run --name "$K6_NAME" --no-deps ${K6_ENV[@]+"${K6_ENV[@]}"} -e "RUN=$RUN_SLUG" \
  k6 run /scripts/"$SCRIPT".js 2>&1 | tee "$RUN_DIR/k6.log"
K6_EXIT=${PIPESTATUS[0]}
set -e
BURST_END=$(date +%s)
if [[ -n "$SLICE_S" ]]; then
  stop_slices
  slice_once end
fi
CHAOS_EXIT=0
if [[ -n "$CHAOS_PID" ]]; then
  wait "$CHAOS_PID" || CHAOS_EXIT=$?
  CHAOS_PID=
  if (( CHAOS_EXIT != 0 )); then log "chaos exited with $CHAOS_EXIT"; fi
fi
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

# --- verify ---------------------------------------------------------------------------------------
# Every document against the history of its aggregate. Chain tasks keep changing fan ledgers after the burst and
# the processors trail the journal, so verify waits for both first (a document behind the journal is counted apart
# and does not fail the run, but at the end of a run none should be); mail tasks touch no ledger.
VERIFY_SECONDS=""
if [[ "${VERIFY:-0}" != 0 ]]; then
  chain_open() {
    compose exec -T postgres psql -U cqrsx -d encore -Atc \
      "select count(*) from transactional_outbox where status in ('pending','processing') and task_type like 'encore_record_%'" 2>/dev/null
  }
  deadline=$((SECONDS + ${VERIFY_TIMEOUT:-300}))
  until [[ "$(chain_open || true)" == 0 ]]; do
    if (( SECONDS >= deadline )); then
      log "verify: chain tasks still open after ${VERIFY_TIMEOUT:-300}s, verifying anyway"
      break
    fi
    sleep 1
  done
  until projections_caught_up; do
    if (( SECONDS >= deadline )); then
      log "verify: the projections still trail the journal after ${VERIFY_TIMEOUT:-300}s, verifying anyway"
      break
    fi
    sleep 1
  done
  verify_start=$SECONDS
  if API_MAX_TIME=600 api_call http://127.0.0.1:8080/api/projections/verify > "$RUN_DIR/verify.json" 2>>"$RUN_DIR/run.log"; then
    VERIFY_SECONDS=$((SECONDS - verify_start))
    log "verify: $(head -c 300 "$RUN_DIR/verify.json") (${VERIFY_SECONDS} s)"
  else
    log "verify request failed"
    rm -f "$RUN_DIR/verify.json"
  fi
fi

# --- ledger ---------------------------------------------------------------------------------------
# Events per aggregate type, for the chaos criteria (accepted commands against events written).
for _ in 1 2 3 4 5; do
  if compose exec -T postgres psql -U cqrsx -d encore -Atc \
    "select aggregate_type, count(*) from events group by 1 order by 1" 2>/dev/null | tr '|' '\t' > "$RUN_DIR/ledger.tsv" \
    && [[ -s "$RUN_DIR/ledger.tsv" ]]; then break; fi
  sleep 1
done

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
  LOADC_WORKERS="$LOAD_OUTBOX_WORKERS" LOADC_SNAPSHOT="$LOAD_SNAPSHOT_EVERY" LOADC_POOL="$LOAD_PG_POOL" LOADC_MONGO_POOL="$LOAD_MONGO_POOL" LOADC_K6_EXIT="$K6_EXIT" \
  LOADC_MAX_BATCH="${LOAD_PROJECTION_MAX_BATCH:-default}" LOADC_SETTLE="${LOAD_PROJECTION_SETTLE_MS:-default}" LOADC_VERIFY_SECONDS="$VERIFY_SECONDS" \
  LOADC_BURST_START="$BURST_START" LOADC_BURST_END="$BURST_END" \
  LOADC_PEAK_BACKEND="$(peak_of backend)" LOADC_PEAK_POSTGRES="$(peak_of postgres)" LOADC_PEAK_MONGO="$(peak_of mongo)" \
  LOADC_DRAIN_FIRST="$DRAIN_FIRST" LOADC_DRAIN_LAST="$DRAIN_LAST" LOADC_DRAIN_SECONDS="$DRAIN_SECONDS" \
  node "$HERE/tools/collect.mjs" "$RUN_DIR" 2>&1 | tee -a "$RUN_DIR/run.log" >&2
hook after_collect

log "results: $RUN_DIR"
exit "$K6_EXIT"
