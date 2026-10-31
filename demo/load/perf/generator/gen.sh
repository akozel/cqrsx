#!/bin/sh
# Entry point of the epic's wrk image. demo/load/run.sh starts the k6 service as `run /scripts/<test>.js`
# with the run's parameters in the environment (VUS, WARMUP, DURATION, RUN, BASE_URL); this runs wrk for
# the warm-up and then for the measured window with scenarios/<test>.lua and writes /home/k6/result.json
# in the shape of demo/load/k6/lib/summary.js (k6's summary.json does not exist here: an empty object).
# Only the scenarios in /gen exist; the others stay with k6 (GENERATOR=k6).
set -eu
[ "${1:-}" = run ] && shift
test="$(basename "${1:-}" .js)"
script="/gen/$test.lua"
if [ ! -f "$script" ]; then
  echo "gen: no wrk scenario for '$test' (have: $(ls /gen | sed 's/\.lua$//' | tr '\n' ' ')); run it with GENERATOR=k6" >&2
  exit 2
fi
case "$test" in
  announce-storm) cmd=AnnounceConcert ;;
  *) cmd="$test" ;;
esac
vus="${VUS:-10}"
warmup="${WARMUP:-3s}"
duration="${DURATION:-10s}"
threads="${GEN_THREADS:-4}"
if [ "$threads" -gt "$vus" ]; then threads="$vus"; fi
url="${BASE_URL:-http://backend:8080}"

# Closed loop like k6's constant VUs: VUS connections, each sends the next request when the answer comes.
PHASE=warmup wrk -t "$threads" -c "$vus" -d "$warmup" --timeout 10s -s "$script" "$url" > /tmp/warmup.txt
PHASE=measured wrk -t "$threads" -c "$vus" -d "$duration" --timeout 10s -s "$script" "$url" > /tmp/measured.txt
cat /tmp/measured.txt

warm="$(sed -n 's/^GEN_RESULT //p' /tmp/warmup.txt)"
meas="$(sed -n 's/^GEN_RESULT //p' /tmp/measured.txt)"
jq -n --argjson w "$warm" --argjson m "$meas" --arg test "$test" --arg cmd "$cmd" --arg run "${RUN:-run}" \
  --argjson vus "$vus" --arg warmup "$warmup" --arg duration "$duration" '
  def ms: . / 1000 | (. * 100 | round) / 100;
  def secs: (capture("^(?<n>[0-9.]+)(?<u>ms|s|m)?$") | (.n | tonumber) * ({"ms": 0.001, "s": 1, "m": 60}[.u // "s"]));
  def st($r; $code): ($r.statuses[$code] // 0);
  ($m.duration_us / 1e6) as $seconds
  | ($m.latency_us | {avg: (.mean | ms), p50: (.p50 | ms), p95: (.p95 | ms), p99: (.p99 | ms), max: (.max | ms)}) as $lat
  | ([$m.errors[]] | add) as $errors
  | {
      test: $test, run: $run, vus: $vus, duration_s: ($duration | secs), warmup_s: ($warmup | secs),
      generator: "wrk",
      requests_measured: $m.requests,
      rps: (($m.requests / $seconds) * 100 | round / 100),
      latency_ms: {all: $lat, by_cmd: {($cmd): ({requests: $m.requests} + $lat)}},
      status: $m.statuses,
      outcomes: {
        accepted: st($m; "200"), conflict: st($m; "409"),
        unavailable: st($m; "503"), rejected_total: st($m; "422"), rejected: {},
        other: ($m.requests - ([$m.statuses[]] | add // 0) + $errors)
      },
      accepted_total_all_phases: (st($w; "200") + st($m; "200")),
      generator_errors: {warmup: $w.errors, measured: $m.errors}
    }' > /home/k6/result.json
echo '{}' > /home/k6/summary.json
jq -c '{test, rps, requests_measured, latency_ms: .latency_ms.all, status, accepted_total_all_phases}' /home/k6/result.json
