#!/usr/bin/env bash
# The chaos scenes of the demo's read side (D-21/D-22 in DESIGN.md), one fresh stand each, at most 5 minutes a
# run (`timeout 300`). Each scene injects a fault while k6 hammers the back-end (chaos/<scene>.sh, run.sh
# LOAD_CHAOS), then chaos/check.mjs holds the result to its criteria: commands accepted = events written
# (or the bounds when an answer can be lost), the projections caught up, verify ok with nothing behind.
#
# Usage: ./chaos.sh [scene …] [KEY=VALUE …]     scenes: mongo-pause kill-9 kill-term postgres-restart
#                                                 encore-0 encore-80 wipe   (default: all)
#   REPEAT=n      runs per scene (default 1)
#   VUS=…         virtual users (default 40);  DURATION=… measured window (default 40s)
# Other KEY=VALUE pairs go to run.sh. kill-9 and kill-term run two nodes behind nginx through
# perf/run.sh (BALANCE=round_robin); the others run on the one-node stand. Exit 0 = every run passed.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PERF="$HERE/perf/run.sh"
REPEAT=1 VUS=40 DURATION=40s
SCENES=() EXTRA=()
for arg in "$@"; do
  case "$arg" in
    REPEAT=*) REPEAT="${arg#REPEAT=}" ;;
    VUS=*) VUS="${arg#VUS=}" ;;
    DURATION=*) DURATION="${arg#DURATION=}" ;;
    *=*) EXTRA+=("$arg") ;;
    *) SCENES+=("$arg") ;;
  esac
done
if [[ ${#SCENES[@]} -eq 0 ]]; then SCENES=(mongo-pause kill-9 kill-term postgres-restart encore-0 encore-80 wipe); fi

FAILED=()
run_scene() { # scene round
  local scene="$1" round="$2" runner="$HERE/run.sh" runner_args=() script=hot-key file mode=equal flags=() env=() name vus="$VUS" duration="$DURATION"
  case "$scene" in
    mongo-pause) file=mongo-pause ;;
    kill-9) file=kill-owner env=(SIGNAL=KILL) mode=bounds flags=(handover) runner="$PERF" runner_args=(NODES=2 BALANCE=round_robin) ;;
    kill-term) file=kill-owner env=(SIGNAL=TERM) mode=bounds flags=(handover) runner="$PERF" runner_args=(NODES=2 BALANCE=round_robin) ;;
    postgres-restart) file=postgres-restart mode=bounds ;;
    encore-0) file=encore env=(PACE_MS=0) ;;
    # Few, short histories: a pace of 80 ms on the hot key's thirty thousand events would take forty minutes, and
    # a full-speed spread run leaves thousands of halls of forty events each.
    encore-80) file=encore env=(PACE_MS=80 ROUNDS=2 ROUND_TIMEOUT_S=120) script=spread-bookings vus=2 duration=15s ;;
    wipe) file=wipe ;;
    *) echo "chaos: unknown scene '$scene'" >&2; return 2 ;;
  esac
  name="chaos-$scene-r$round"
  echo
  echo "################  $scene (run $round)"
  env LOAD_CHAOS="$HERE/chaos/$file.sh" ${env[@]+"${env[@]}"} timeout 300 \
    "$runner" "$script" NAME="$name" VUS="$vus" DURATION="$duration" WARMUP=3s VERIFY=1 VERIFY_TIMEOUT=120 \
    ${runner_args[@]+"${runner_args[@]}"} ${EXTRA[@]+"${EXTRA[@]}"}
  local rc=$? result
  result="$(ls -d "$HERE"/results/*-"$name" 2>/dev/null | tail -n1)/result.json"
  if [[ ! -f "$result" ]]; then echo "  ✘ no result.json (exit $rc)"; return 1; fi
  node "$HERE/chaos/check.mjs" "$result" "$mode" ${flags[@]+"${flags[@]}"}
}

for scene in "${SCENES[@]}"; do
  for round in $(seq 1 "$REPEAT"); do
    run_scene "$scene" "$round" || FAILED+=("$scene (run $round)")
  done
done
echo
if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo "chaos: failed: ${FAILED[*]}" >&2
  exit 1
fi
echo "chaos: every run passed"
