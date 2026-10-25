# shellcheck shell=bash
# Scene: MongoDB stops answering (`pause`, not a crash: connections stay open and hang) for PAUSE_S seconds in
# the middle of the burst. Commands never touch MongoDB, so every one is still accepted; the projections lose
# their lease and the lag grows by the last known position; after the unpause they catch up in a page or two.
# Sourced by run.sh (LOAD_CHAOS=chaos/mongo-pause.sh); see its header for the helpers.
PAUSE_S="${PAUSE_S:-10}"
# The container id is read before the pause (`compose ps` may not answer while it is paused) and kept in a file
# for chaos_cleanup, which runs in the parent shell.
chaos() {
  chaos_at $((WARMUP_S + 3))
  local id
  id="$(compose ps -q mongo | head -n1)"
  printf '%s' "$id" > "$RUN_DIR/.mongo-id"
  chaos_step "pause mongo for ${PAUSE_S}s"
  "$CTR" pause "$id" >/dev/null
  sleep "$PAUSE_S"
  "$CTR" unpause "$id" >/dev/null
  chaos_step "unpause mongo"
}
chaos_cleanup() {
  if [[ -s "$RUN_DIR/.mongo-id" ]]; then "$CTR" unpause "$(cat "$RUN_DIR/.mongo-id")" >/dev/null 2>&1 || true; fi
}
