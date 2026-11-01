#!/usr/bin/env bash
# The first-iteration matrix (README.md): every line is a fresh stand, 3 s warm-up + 10 s measured.
# Tooling is checked once up front. A run whose stand did not start (exit 3) aborts the matrix: the
# next runs would fail the same way. A run that failed inside k6 does not stop the others; the table
# at the end shows the gaps.
# Pass KEY=VALUE pairs to add to every run, e.g. ./matrix.sh PIN=0 DURATION=10s
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTRA=("$@")
FAILED=()

"$HERE/run.sh" --check || exit 2

run() {
  echo
  echo "################  $*"
  local rc=0
  "$HERE/run.sh" "$@" ${EXTRA[@]+"${EXTRA[@]}"} || rc=$?
  if (( rc == 3 )); then
    echo "matrix: the stand did not start; aborting (the remaining runs would fail the same way)" >&2
    exit 3
  fi
  if (( rc != 0 )); then
    FAILED+=("$* (exit $rc)")
  fi
}

# 1. Hot key: one stream, up to 40 writers with a seat each (more than 40 adds 422 noise).
run hot-key NAME=01-hot-key-vus5 VUS=5 BUILD=1
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

# 3b. Hot fan: every booking for one fan. The fan has a ledger per concert (D-14), so nothing grows with the
#     fan's history: the regression check against 3a.
run spread-bookings NAME=03b-drain-hot-fan-w8 VUS=50 DRAIN=1 WORKERS=8 FAN_MODE=hot

echo
"$HERE/report.sh"
if (( ${#FAILED[@]} > 0 )); then
  echo
  echo "matrix: ${#FAILED[@]} run(s) failed:" >&2
  printf '  %s\n' "${FAILED[@]}" >&2
  exit 1
fi
