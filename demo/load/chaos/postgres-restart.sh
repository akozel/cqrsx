# shellcheck shell=bash
# Scene: PostgreSQL restarts in the middle of the burst. Commands answer 503 while it is away and are accepted
# again after; the processors fail their rounds, pause with backoff and resume from their checkpoints.
# Sourced by run.sh (LOAD_CHAOS=chaos/postgres-restart.sh); see its header for the helpers.
chaos() {
  chaos_at $((WARMUP_S + 3))
  chaos_step "restart postgres"
  compose restart postgres >/dev/null 2>&1
  chaos_step "postgres restarted"
}
