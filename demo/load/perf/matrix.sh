#!/usr/bin/env bash
# Backlog epic 02's matrix: the points of demo/load's matrix the epic measures (01, 02a, 02b, 03a), each
# run through perf/run.sh, i.e. with pg_stat_statements and the work-per-command counters. Every line is
# a fresh stand, 3 s warm-up + 10 s measured. The epic README ("Что мерить") lists the points per ticket.
# Tooling is checked once up front. A run whose stand did not start (exit 3) aborts the matrix: the
# next runs would fail the same way. A run that failed inside k6 does not stop the others; the table
# at the end shows the gaps.
# Pass KEY=VALUE pairs to add to every run, e.g. ./matrix.sh PIN=0 DURATION=10s. Two of them belong to
# the matrix itself and are not passed on:
#   REPEAT=N   run the whole matrix N times, one pass after another, NAME suffixed -r1 … -rN. Passes
#              rather than N runs in a row spread the repeats of a point over time, so a drift of the
#              machine widens the spread instead of shifting one point. The report shows the median and
#              the min–max of the repeats.
#   ONLY=a,b   only these lines: a series tag (the part of NAME before the first `-`: 01, 02a, 02b, 03a)
#              or a whole NAME (e.g. 02a-announce-vus100).
# The first run that actually starts rebuilds the back-end image (BUILD=1); pass BUILD=0 to skip that.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTRA=()
REPEAT=1
ONLY=""
BUILD_FIRST=1
for pair in "$@"; do
  case "$pair" in
    REPEAT=*) REPEAT="${pair#REPEAT=}" ;;
    ONLY=*) ONLY="${pair#ONLY=}" ;;
    BUILD=0) BUILD_FIRST=0 ;;
    *) EXTRA+=("$pair") ;;
  esac
done
if [[ ! "$REPEAT" =~ ^[1-9][0-9]*$ ]]; then
  echo "matrix: REPEAT must be a positive number (got '$REPEAT')" >&2
  exit 2
fi
FAILED=()
PASS=1

"$HERE/run.sh" --check || exit 2

selected() { # NAME → whether ONLY lets it through: the whole NAME or its series tag
  [[ -z "$ONLY" ]] && return 0
  local wanted want
  IFS=',' read -ra wanted <<<"$ONLY"
  for want in "${wanted[@]}"; do
    if [[ "$1" == "$want" || "$1" == "$want"-* ]]; then return 0; fi
  done
  return 1
}

run() { # script NAME=… [KEY=VALUE …]
  local script="$1" args=() name="" arg
  shift
  for arg in "$@"; do
    case "$arg" in
      NAME=*) name="${arg#NAME=}" ;;
      *) args+=("$arg") ;;
    esac
  done
  selected "$name" || return 0
  if (( REPEAT > 1 )); then name="$name-r$PASS"; fi
  if (( BUILD_FIRST )); then
    args+=(BUILD=1)
    BUILD_FIRST=0
  fi
  echo
  echo "################  $script NAME=$name ${args[*]}"
  local rc=0
  "$HERE/run.sh" "$script" "NAME=$name" ${args[@]+"${args[@]}"} ${EXTRA[@]+"${EXTRA[@]}"} || rc=$?
  if (( rc == 3 )); then
    echo "matrix: the stand did not start; aborting (the remaining runs would fail the same way)" >&2
    exit 3
  fi
  if (( rc != 0 )); then
    FAILED+=("$script $name (exit $rc)")
  fi
}

matrix() {
  # 1. Hot key: one stream, up to 40 writers with a seat each (more than 40 adds 422 noise).
  run hot-key NAME=01-hot-key-vus5 VUS=5
  run hot-key NAME=01-hot-key-vus10 VUS=10
  run hot-key NAME=01-hot-key-vus20 VUS=20
  run hot-key NAME=01-hot-key-vus40 VUS=40

  # 2a. Announce storm: a new aggregate per request, the lightest command.
  run announce-storm NAME=02a-announce-vus10 VUS=10
  run announce-storm NAME=02a-announce-vus50 VUS=50
  run announce-storm NAME=02a-announce-vus100 VUS=100

  # 2b. Spread bookings: full write path, no contention.
  run spread-bookings NAME=02b-spread-vus10 VUS=10
  run spread-bookings NAME=02b-spread-vus50 VUS=50
  run spread-bookings NAME=02b-spread-vus100 VUS=100

  # 3a. Outbox drain after a burst, by worker count (queue capacity follows the worker count).
  run spread-bookings NAME=03a-drain-w2 VUS=50 DRAIN=1 WORKERS=2
  run spread-bookings NAME=03a-drain-w8 VUS=50 DRAIN=1 WORKERS=8
  run spread-bookings NAME=03a-drain-w32 VUS=50 DRAIN=1 WORKERS=32

}

for ((PASS = 1; PASS <= REPEAT; PASS++)); do
  if (( REPEAT > 1 )); then echo; echo "================  pass $PASS of $REPEAT"; fi
  matrix
done

echo
"$HERE/report.sh"
if (( ${#FAILED[@]} > 0 )); then
  echo
  echo "matrix: ${#FAILED[@]} run(s) failed:" >&2
  printf '  %s\n' "${FAILED[@]}" >&2
  exit 1
fi
