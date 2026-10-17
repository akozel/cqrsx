#!/usr/bin/env bash
# Snapshot or compare the read models through the back-end API, ignoring the optimistic `version`.
#   scripts/compare-projections.sh snapshot out.json      # dump concert_halls + fan_wallets (via /api)
#   scripts/compare-projections.sh compare a.json b.json  # exit 0 when equal
set -euo pipefail
API="${ENCORE_API:-http://127.0.0.1:8080}"
INSPECT="${ENCORE_INSPECT:-http://127.0.0.1:8090}"
case "${1:-}" in
  snapshot)
    # Wallets are listed through the sidecar (the back-end has no "all wallets" endpoint).
    python3 - "$API" "$INSPECT" "$2" <<'PY'
import json, sys, urllib.request
api, inspect, out = sys.argv[1:4]
def get(url):
    with urllib.request.urlopen(url, timeout=10) as r: return json.load(r)
def strip(doc):
    doc = dict(doc); doc.pop("version", None); return doc
halls = {c["concert_id"]: strip(get(f"{api}/api/concerts/{c['concert_id']}")) for c in get(f"{api}/api/concerts")}
try:
    wallets = {d["_id"]: strip(d) for d in get(f"{inspect}/inspect/mongo/encore_read/fan_wallets?limit=1000")["docs"]}
except Exception as e:  # sidecar may be absent (Phase 6 runs before Phase 7)
    wallets = {"_unavailable": str(e)}
json.dump({"concert_halls": halls, "fan_wallets": wallets}, open(out, "w"), indent=1, sort_keys=True)
print(f"snapshot: {len(halls)} halls, {len(wallets)} wallets -> {out}")
PY
    ;;
  compare)
    python3 - "$2" "$3" <<'PY'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
if a == b:
    print("projections are identical (ignoring version)"); sys.exit(0)
for coll in ("concert_halls", "fan_wallets"):
    for key in sorted(set(a[coll]) | set(b[coll])):
        if a[coll].get(key) != b[coll].get(key): print(f"DIFF {coll}/{key}")
sys.exit(1)
PY
    ;;
  *) echo "usage: $0 snapshot <out.json> | compare <a.json> <b.json>" >&2; exit 2 ;;
esac
