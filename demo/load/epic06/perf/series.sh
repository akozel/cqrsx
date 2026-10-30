#!/usr/bin/env bash
# Backlog epic 06: an interleaved series over a plan file, one perf/run.sh of epic 02 per line and pass (the method of
# ticket 05/13, its tools adapted).
# A plan line: `<prefix> <image tag> <script> NAME=<point> KEY=VALUE …` (blank lines and # comments are skipped). The run
# gets PREFIX=<prefix> and BACKEND_IMAGE=encore-load-backend:<image tag>: every image goes through one stand, one
# run.sh and one set of ENCORE_* / OUTBOX_* variables. The script is run.sh's: a name in demo/load/k6/ (hot-key,
# spread-bookings) or this directory's probe as `../epic06/k6/probe`; NAME is required (run.sh names the results folder
# after the script otherwise). compose.probe.yaml is added to every run (LOAD_COMPOSE_EXTRA): it mounts ../k6 for k6,
# and a line's ENCORE_LOAD_VIEWS=… ENCORE_LOAD_TASKS=… reach the back-end through it; without them the probe is idle.
# - Every pass runs the lines in its own order, Python's random.Random("<SEED>-<pass>"), written to the journal: the
#   host slows down every ~5 minutes and a fixed order would put the same line into every slowdown; `shuf
#   --random-source` with a repeated seed gave one order for every pass (epic 04, ticket 20).
# - NAME gets -r<pass> when REPEAT > 1.
# - Every run under reap.py (the sandbox's PID 1 never reaps the samplers' orphans) and `timeout 295`: no run lasts
#   longer than 5 minutes; on expiry run.sh gets TERM and tears its stand down (exit 124).
# - A stand that did not start (exit 3) or a run cut by the timeout (124) stops the series; any other failure is
#   written to the journal and the series goes on (a failed k6 or verify is a finding, not a reason to rerun).
# - Nothing is built or tested while the series runs: build the images first (build.sh).
# Usage: SEED=<text> series.sh <plan file> <repeat> <journal>
set -uo pipefail
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/podman/podman.sock}"
export PATH=$HOME/.local/bin:$PATH
export LOAD_HOST_DIR=/home/akozel/Documents/projects/event-sourcing-workspace/event-sourcing/demo/load
TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD="$(cd "$TOOLS/../.." && pwd)"
PERF="$(cd "$LOAD/../docs/backlog/02_postgres_performance/perf" && pwd)"
RESULTS="$LOAD/results"
SCRATCH="${SCRATCH:-/tmp/claude-1000/-workspace/0baccf55-8936-4ac7-8c8d-72d2c7689858/scratchpad/e06}"
if [[ $# -ne 3 || -z "${SEED:-}" ]]; then
  echo "usage: SEED=<text> series.sh <plan file> <repeat> <journal>" >&2
  exit 2
fi
PLAN="$1" REPEAT="$2" JOURNAL="$3"
mkdir -p "$SCRATCH/logs" "$(dirname "$JOURNAL")"
export LOAD_COMPOSE_EXTRA="$TOOLS/compose.probe.yaml"

if [[ -n "$(docker ps -aq --filter label=com.docker.compose.project=encore-load 2>/dev/null)" ]]; then
  echo "series: containers of the project encore-load exist: another stand is up, not starting" >&2
  exit 2
fi

# One line of figures from a run's result.json: accepted/s, back-end CPU per 1 000 commands (window, chain) and per
# 1 000 drained tasks, statements per command, the drain, the projections (probe views seen, caught up after the
# burst), tasks per event of the run's aggregate, verify.
summary() {
  node -e '
    const r = JSON.parse(require("node:fs").readFileSync(process.argv[1], "utf8"));
    const e = r.efficiency ?? {}, c = e.chain ?? {}, d = r.drain, p = r.projections;
    const acc = r.requests_measured ? (r.rps * (r.outcomes?.accepted ?? 0)) / r.requests_measured : null;
    const names = Object.keys(p?.owners ?? {});
    const probe = r.ledger?.Probe;
    const parts = [
      `acc/s ${acc?.toFixed(0)}`,
      `backend ms/1k cmd ${e.cpu_ms_per_1k_accepted?.backend ?? "-"} (chain ${c.cpu_ms_per_1k_accepted?.backend ?? "-"})`,
      `ms/1k tasks ${d?.per_task?.cpu_ms_per_1k_tasks?.backend ?? "-"}`,
      `stmt/cmd ${e.statements_per_command ?? "-"}`,
      `drain ${d ? `${d.seconds} s, ${d.tasks_per_second} tasks/s, total ${d.tasks_total}, failed ${d.failed}, open ${d.open_at_end}` : "-"}`,
      `views ${names.length} (probe ${names.filter((n) => n.startsWith("probe_")).length}), caught up ${p?.caught_up_s ?? "-"} s`,
      probe ? `Probe events ${probe}, tasks/event ${d ? (d.tasks_total / probe).toFixed(2) : "-"}` : `ledger ${JSON.stringify(r.ledger ?? null)}`,
      `verify ${r.verify ? `ok=${r.verify.ok} behind=${r.verify.behind_documents} diff=${r.verify.differences}` : "-"}`,
    ];
    console.log(parts.join(" · "));' "$1" 2>/dev/null || echo "no result.json"
}

mapfile -t LINES < <(grep -v '^\s*#' "$PLAN" | grep -v '^\s*$')
for line in "${LINES[@]}"; do
  if [[ " $line " != *" NAME="?* ]]; then
    echo "series: a plan line without NAME=…: $line" >&2
    exit 2
  fi
done
total=$(( ${#LINES[@]} * REPEAT )) n=0 failed=0
echo "[$(date +%H:%M:%S)] series: $PLAN, $REPEAT pass(es), $total runs, seed $SEED; zombies before: $(ps -eo stat | grep -c Z)" | tee -a "$JOURNAL"
for ((pass = 1; pass <= REPEAT; pass++)); do
  mapfile -t order < <(printf '%s\n' "${LINES[@]}" | python3 -I -c 'import random, sys; lines = sys.stdin.read().splitlines(); random.Random(sys.argv[1]).shuffle(lines); print("\n".join(lines))' "$SEED-$pass")
  echo "pass $pass order ($SEED-$pass): $(printf '%s\n' "${order[@]}" | awk '{ for (i = 4; i <= NF; i++) if ($i ~ /^NAME=/) print $1 substr($i, 6) }' | tr '\n' ' ')" | tee -a "$JOURNAL"
  for line in "${order[@]}"; do
    read -r prefix image script rest <<<"$line"
    args=() name=""
    for kv in $rest; do
      if [[ "$kv" == NAME=* ]]; then
        if (( REPEAT > 1 )); then kv="$kv-r$pass"; fi
        name="${kv#NAME=}"
      fi
      args+=("$kv")
    done
    n=$((n + 1))
    start=$(date +%s)
    out="$SCRATCH/logs/run-$(date +%Y%m%d-%H%M%S)-$n.log"
    (cd "$PERF" && python3 -I "$TOOLS/reap.py" timeout -k 20 295 ./run.sh "$script" PREFIX="$prefix" \
      BACKEND_IMAGE="encore-load-backend:$image" "${args[@]}") > "$out" 2>&1
    rc=$?
    dir="$(ls -1d "$RESULTS"/*-"$prefix$name" 2>/dev/null | tail -n 1)"
    echo "[$(date +%H:%M:%S)] $n/$total pass $pass $prefix$name ($image) → exit $rc, $(( $(date +%s) - start )) s · $(summary "$dir/result.json")" | tee -a "$JOURNAL"
    if (( rc == 3 || rc == 124 )); then
      echo "[$(date +%H:%M:%S)] series: stopped (exit $rc; console: $out)" | tee -a "$JOURNAL"
      exit "$rc"
    fi
    if (( rc != 0 )); then failed=$((failed + 1)); fi
  done
done
echo "[$(date +%H:%M:%S)] series: done, $failed failed; zombies after: $(ps -eo stat | grep -c Z)" | tee -a "$JOURNAL"
