# shellcheck shell=bash
# Scene: the node that owns the seat maps dies. SIGNAL=KILL (default) leaves its lease to expire (the other node
# takes the checkpoint after ENCORE_PROJECTION_LEASE_MS plus a poll), SIGNAL=TERM lets it hand the checkpoint
# over on its way out. Needs two nodes: perf/run.sh NODES=2 BALANCE=round_robin (the owner is named like its
# service: `backend` or `backend-2`). The dead node stays dead: verify and the status sampler use the survivor.
# Sourced by run.sh (LOAD_CHAOS=chaos/kill-owner.sh); see its header for the helpers.
SIGNAL="${SIGNAL:-KILL}"
KILL_AT_S="${KILL_AT_S:-10}"
chaos() {
  chaos_at $((WARMUP_S + KILL_AT_S))
  local owner
  owner="$(api_call http://127.0.0.1:8080/api/projections/status | node -e '
    let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
      const halls = (JSON.parse(s).projections ?? []).find((p) => p.projection === "concert_halls");
      console.log(halls?.owner ?? "");
    });')"
  if [[ -z "$owner" ]]; then chaos_step "no owner of concert_halls to kill"; return 1; fi
  chaos_step "kill -$SIGNAL $owner (the owner of concert_halls)"
  "$CTR" kill --signal "$SIGNAL" "$(compose ps -q "$owner" | head -n1)" >/dev/null
}
