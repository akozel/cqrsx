#!/usr/bin/env bash
# The report over results/: a table in the terminal (tools/report.mjs) and results/report.html with
# charts (tools/report-html.mjs). Open the HTML in a browser; it needs no network.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
node "$HERE/tools/report.mjs" "$HERE/results"
node "$HERE/tools/report-html.mjs" "$HERE/results"
