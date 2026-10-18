#!/usr/bin/env bash
# Functional smoke test of the fan ledgers (D-14) on a fresh stand, no k6: announce two concerts, book
# a seat for one fan in each, cancel one, and check what the API and the databases say.
#   - GET /api/fans/{fan}/wallet assembles both ledgers (two entries in `ledgers`, two tickets);
#   - the refund chain runs inside the right ledger (the ticket turns `cancelled` with a `refund`);
#   - every outbox task completes; GET /api/projections/verify answers ok: true.
# Uses the same compose files, tooling detection and host-path mapping as run.sh (PIN, LOAD_HOST_DIR,
# CTR/COMPOSE). The back-end's own curl runs the requests inside the network: no published ports.
# Usage: ./smoke.sh            Exit 0 = every check passed; the stand is torn down either way.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT=encore-load

if [[ -z "${CTR:-}" ]]; then
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then CTR=docker; else CTR=podman; fi
fi
COMPOSE="${COMPOSE:-$CTR compose}"
COMPOSE_FILES=(-f "$HERE/compose.yaml")
case "${PIN:-quota}" in
  cpuset) COMPOSE_FILES+=(-f "$HERE/compose.pin.yaml") ;;
  quota) COMPOSE_FILES+=(-f "$HERE/compose.quota.yaml") ;;
esac
# shellcheck disable=SC2086
compose() { $COMPOSE -p "$PROJECT" "${COMPOSE_FILES[@]}" "$@"; }
export LOAD_OUTBOX_WORKERS="${WORKERS:-2}" LOAD_RUST_LOG="${RUST_LOG:-info}"

FAILED=0
pass() { echo "  ✔ $*"; }
fail() { echo "  ✘ $*" >&2; FAILED=$((FAILED + 1)); }
cleanup() { compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

# --- stand ------------------------------------------------------------------------------------------
echo "smoke: fresh stand"
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d postgres mongo mail backend
for _ in $(seq 1 120); do
  id="$(compose ps -q backend 2>/dev/null | head -n1 || true)"
  [[ -n "$id" && "$($CTR inspect -f '{{with .State.Health}}{{.Status}}{{end}}' "$id" 2>/dev/null)" == healthy ]] && break
  sleep 1
done

# HTTP through the back-end container's curl: `api METHOD PATH [JSON]` prints the body.
api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    compose exec -T backend curl -sS -X "$method" -H 'Content-Type: application/json' -d "$body" "http://127.0.0.1:8080$path"
  else
    compose exec -T backend curl -sS -X "$method" "http://127.0.0.1:8080$path"
  fi
}
field() { node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const v=JSON.parse(s);console.log(process.argv[1].split(".").reduce((o,k)=>o==null?o:o[k],v)??"")})' "$1"; }
sql() { compose exec -T postgres psql -U cqrsx -d encore -Atc "$1" | tr -d '[:space:]'; }

FAN="fox"
C1="smoke-radiohead-2026-11-21"
C2="smoke-muse-2027-01-16"
echo "smoke: two concerts, one seat each for $FAN"
for c in "$C1" "$C2"; do
  out="$(api POST "/api/concerts/$c/commands" '{"type":"AnnounceConcert","artist":"Smoke","city":"Loadville","date":"2026-12-31"}')"
  [[ "$(field outcome <<<"$out")" == accepted ]] && pass "announced $c" || fail "announce $c: $out"
done
book1="$(api POST "/api/concerts/$C1/commands" "{\"type\":\"BookSeat\",\"seat\":\"B4\",\"fan_id\":\"$FAN\",\"fan_name\":\"Fox\"}")"
book2="$(api POST "/api/concerts/$C2/commands" "{\"type\":\"BookSeat\",\"seat\":\"A1\",\"fan_id\":\"$FAN\",\"fan_name\":\"Fox\"}")"
[[ "$(field outcome <<<"$book1")" == accepted && "$(field outcome <<<"$book2")" == accepted ]] && pass "two bookings accepted" || fail "bookings: $book1 / $book2"
booking2="$(field command_id <<<"$book2")"

echo "smoke: the wallet is assembled from two ledgers"
wallet=""
for _ in $(seq 1 30); do
  wallet="$(api GET "/api/fans/$FAN/wallet" || true)"
  [[ "$(field view.active_count <<<"$wallet" 2>/dev/null)" == 2 ]] && break
  sleep 1
done
[[ "$(field view.active_count <<<"$wallet")" == 2 ]] && pass "active_count = 2" || fail "wallet: $wallet"
ledgers="$(node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const w=JSON.parse(s);console.log(w.ledgers.map(l=>l.concert_id).sort().join(","))})' <<<"$wallet")"
[[ "$ledgers" == "$C2,$C1" ]] && pass "ledgers = $ledgers" || fail "ledgers: $ledgers"
[[ "$(field view.total_spent <<<"$wallet")" == 200 ]] && pass "total_spent = 200 (80 + 120)" || fail "total_spent: $(field view.total_spent <<<"$wallet")"
[[ "$(field fan_id <<<"$wallet")" == "$FAN" && "$(field view.fan_id <<<"$wallet")" == "$FAN" ]] && pass "fan_id = $FAN at both levels" || fail "fan_id in $wallet"
ids="$(sql "select string_agg(distinct aggregate_id, ',' order by aggregate_id) from events where aggregate_type='Fan'")"
[[ "$ids" == "$FAN:$C2,$FAN:$C1" ]] && pass "Fan streams: $ids" || fail "Fan streams: $ids"

echo "smoke: cancel the $C2 ticket → refund inside its ledger"
cancel="$(api POST "/api/concerts/$C2/commands" "{\"type\":\"CancelBooking\",\"seat\":\"A1\",\"fan_id\":\"$FAN\"}")"
[[ "$(field outcome <<<"$cancel")" == accepted ]] && pass "cancellation accepted" || fail "cancel: $cancel"
state=""
for _ in $(seq 1 30); do
  wallet="$(api GET "/api/fans/$FAN/wallet" || true)"
  state="$(field "view.tickets.$booking2.state" <<<"$wallet" 2>/dev/null || true)"
  [[ "$state" == cancelled ]] && break
  sleep 1
done
[[ "$state" == cancelled ]] && pass "ticket $booking2 is cancelled" || fail "ticket state: $state"
[[ "$(field "view.tickets.$booking2.refund" <<<"$wallet")" == 120 ]] && pass "refund = 120" || fail "refund: $(field "view.tickets.$booking2.refund" <<<"$wallet")"
[[ "$(field view.active_count <<<"$wallet")" == 1 && "$(field view.total_spent <<<"$wallet")" == 80 ]] && pass "active_count = 1, total_spent = 80" || fail "counters: $wallet"

echo "smoke: every task completes, verify is green"
open=1
for _ in $(seq 1 30); do
  open="$(sql "select count(*) from transactional_outbox where status in ('pending','processing')")"
  [[ "$open" == 0 ]] && break
  sleep 1
done
counts="$(sql "select string_agg(status || '=' || n, ',' order by status) from (select status, count(*) n from transactional_outbox group by status) t")"
[[ "$open" == 0 && "$counts" == "completed=6" ]] && pass "outbox: $counts" || fail "outbox: $counts (open=$open)"
verify="$(api GET /api/projections/verify)"
[[ "$(field ok <<<"$verify")" == true ]] && pass "verify ok" || fail "verify: $verify"
errors="$(compose logs --no-color backend 2>/dev/null | grep -c -E 'ERROR|WARN' || true)"
[[ "$errors" == 0 ]] && pass "no WARN/ERROR in the back-end log" || { fail "$errors WARN/ERROR lines in the back-end log"; compose logs --no-color backend | grep -E 'ERROR|WARN' | head -5 >&2; }

if (( FAILED > 0 )); then
  echo "smoke: $FAILED check(s) failed" >&2
  exit 1
fi
echo "smoke: all checks passed"
