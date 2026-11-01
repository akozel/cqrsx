#!/usr/bin/env bash
# The epic's report over demo/load/results: the runs named <prefix>* (default perf-) as a terminal table,
# one line per point with the median of its repeats, and demo/load/results/perf-report.html with charts.
# Usage: ./report.sh [prefix]          e.g. ./report.sh perf-base-
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS="$(cd "$HERE/.." && pwd)/results"
PREFIX="${1:-perf-}"
node "$HERE/tools/report.mjs" "$RESULTS" "$PREFIX"
node "$HERE/tools/report-html.mjs" "$RESULTS" "$RESULTS/perf-report.html" "$PREFIX"
