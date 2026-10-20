#!/usr/bin/env bash
# One run of the demo/load stand with backlog epic 02's measurement on top: pg_stat_statements
# (compose.perf.yaml) and the counter snapshots before / after the burst and after the drain (hooks.sh),
# turned into work per command and per task in result.json (tools/collect.mjs).
#
# Usage and parameters: exactly those of demo/load/run.sh (./run.sh --help), e.g.
#   ./run.sh announce-storm PREFIX=perf-02- NAME=02a-announce-vus100 VUS=100 POOL=20
# Results land in demo/load/results/ next to the stand's own runs.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD="$(cd "$HERE/../../../../load" && pwd)"
export LOAD_COMPOSE_EXTRA="$HERE/compose.perf.yaml${LOAD_COMPOSE_EXTRA:+:$LOAD_COMPOSE_EXTRA}"
export LOAD_HOOKS="$HERE/hooks.sh"
exec "$LOAD/run.sh" "$@"
