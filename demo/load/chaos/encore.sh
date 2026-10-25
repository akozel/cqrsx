# shellcheck shell=bash
# Scene: ⏪ Encore! during the burst, ROUNDS times one after the other, PACE_MS between the events of one key.
# The box office stays open (D-16): commands keep being accepted while every document is rebuilt in place.
# Each round waits for the status to say the rebuild is over (at most ROUND_TIMEOUT_S).
# Sourced by run.sh (LOAD_CHAOS=chaos/encore.sh); see its header for the helpers.
PACE_MS="${PACE_MS:-0}"
ROUNDS="${ROUNDS:-3}"
ROUND_TIMEOUT_S="${ROUND_TIMEOUT_S:-60}"
rebuild_over() { # 0 when no rebuild runs on any projection
  api_call http://127.0.0.1:8080/api/projections/status 2>/dev/null | node -e '
    let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
      try {
        const status = JSON.parse(s);
        const busy = status.rebuild != null || (status.projections ?? []).some((p) => p.rebuild?.requested || p.rebuild?.cursor);
        process.exit(busy ? 1 : 0);
      } catch { process.exit(1); }
    });'
}
chaos() {
  local round deadline
  chaos_at $((WARMUP_S + 2))
  for round in $(seq 1 "$ROUNDS"); do
    chaos_step "encore round $round, pace_ms=$PACE_MS"
    api_call -X POST -H 'Content-Type: application/json' -d "{\"pace_ms\":$PACE_MS}" \
      http://127.0.0.1:8080/api/projections/rebuild >> "$RUN_DIR/chaos.log" || chaos_step "encore request failed"
    sleep 1
    deadline=$((SECONDS + ROUND_TIMEOUT_S))
    until rebuild_over; do
      if (( SECONDS >= deadline )); then chaos_step "encore round $round did not end in ${ROUND_TIMEOUT_S}s"; return 1; fi
      sleep 1
    done
    chaos_step "encore round $round over"
  done
}
