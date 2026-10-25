# shellcheck shell=bash
# Scene: 🧨 wipe in the middle of the burst, as the demo's sidecar does it: the read models first, their
# checkpoints second. The processors register their checkpoints again and read the journal from the start, so
# every hall and wallet comes back without Encore!. One node only (a stale owner's write that began before the
# wipe can land on the recreated document, see the ticket's risks).
# Sourced by run.sh (LOAD_CHAOS=chaos/wipe.sh); see its header for the helpers.
chaos() {
  chaos_at $((WARMUP_S + 3))
  chaos_step "wipe concert_halls, fan_wallets, then projection_checkpoints"
  compose exec -T mongo mongosh --quiet encore_read --eval \
    'db.concert_halls.drop(); db.fan_wallets.drop(); db.projection_checkpoints.drop()' >/dev/null
  chaos_step "wiped"
}
