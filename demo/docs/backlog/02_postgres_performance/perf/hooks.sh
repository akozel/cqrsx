# shellcheck shell=bash
# Backlog epic 02 measurement hooks for demo/load/run.sh (LOAD_HOOKS, set by perf/run.sh). Sourced by
# run.sh once the run folder exists; uses its RUN_DIR, compose, log and the run parameters (DRAIN).
#
# A snapshot appends `point  service  key  value` lines to counters.tsv; tools/collect.mjs turns the
# differences between points into figures per command (before → after the burst) and per task
# (after → drained). Points: `before` (statement statistics reset just after), `after` (statements
# dumped to pg_statements.tsv, and reset again with DRAIN=1), `drained` (dumped to
# pg_statements.drain.tsv). The containers are probed in parallel, everything inside one container in
# one go, so a snapshot is a single moment to within a few milliseconds per container.
# - cpu_usec: the container's cgroup CPU time, exact; for the back-end, PostgreSQL, MongoDB and every
#   service of LOAD_SERVICES_EXTRA (the second node and the balancer of ticket 17).
# - net_rx_packets / net_tx_packets: every interface of the PostgreSQL container but `lo`, i.e. its
#   traffic with the back-end (psql below uses the unix socket).
# - xact_commit / xact_rollback: pg_stat_database. PostgreSQL publishes them per backend at most once
#   a second, so the last second of a burst may be missing from `after`.
# - wal_lsn: the WAL insert position, exact (the difference is the WAL written).
# - outbox_completed: completed outbox tasks, the denominator of the drain figures.
# Our own queries carry `/* load-stand */`; they and run.sh's drain probe (`count(*) filter (where
# status …`) are left out of the statement files.
# Nothing here fails a run: a value that could not be read is logged and left out.

PERF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

read -r -d '' PG_PROBE <<'SH' || true
mode="$1"
tab="$(printf '\t')"
cpu="$(sed -n 's/^usage_usec //p' /sys/fs/cgroup/cpu.stat 2>/dev/null)"
if [ -n "$cpu" ]; then printf 'cpu_usec\t%s\n' "$cpu"; fi
rx=0; tx=0; n=0
for dir in /sys/class/net/*; do
  [ "${dir##*/}" = lo ] && continue
  [ -r "$dir/statistics/rx_packets" ] || continue
  rx=$((rx + $(cat "$dir/statistics/rx_packets"))); tx=$((tx + $(cat "$dir/statistics/tx_packets"))); n=$((n + 1))
done
if [ "$n" -gt 0 ]; then printf 'net_rx_packets\t%s\nnet_tx_packets\t%s\n' "$rx" "$tx"; fi
psql -U cqrsx -d encore -X -At -F "$tab" -c "
  select /* load-stand */ 'xact_commit', xact_commit::text from pg_stat_database where datname = current_database()
  union all select 'xact_rollback', xact_rollback::text from pg_stat_database where datname = current_database()
  union all select 'wal_lsn', pg_current_wal_insert_lsn()::text
  union all select 'outbox_completed', count(*)::text from transactional_outbox where status = 'completed'"
case "$mode" in
  dump|dump-reset)
    psql -U cqrsx -d encore -X -At -F "$tab" -c "
      select /* load-stand */ 'stmt', queryid, calls, round(total_exec_time::numeric, 3), round(mean_exec_time::numeric, 4),
             rows, shared_blks_hit, shared_blks_read, round(shared_blk_read_time::numeric, 3), regexp_replace(query, '\s+', ' ', 'g')
      from pg_stat_statements
      where toplevel and dbid = (select oid from pg_database where datname = current_database())
        and query !~ 'load-stand' and query !~* 'count\(\*\) filter \(where status'
      order by total_exec_time desc" ;;
esac
case "$mode" in
  reset|dump-reset) psql -U cqrsx -d encore -X -Atq -c "select /* load-stand */ pg_stat_statements_reset()" >/dev/null ;;
esac
SH

cpu_probe() { # service → `cpu_usec <n>` from the container's own cgroup (cgroup v2)
  local cpu
  cpu="$(compose exec -T "$1" cat /sys/fs/cgroup/cpu.stat 2>/dev/null | sed -n 's/^usage_usec //p' | tr -d '[:space:]' || true)"
  if [[ "$cpu" =~ ^[0-9]+$ ]]; then printf 'cpu_usec\t%s\n' "$cpu"; fi
}

STATEMENTS_HEADER=$'queryid\tcalls\ttotal_exec_time_ms\tmean_exec_time_ms\trows\tshared_blks_hit\tshared_blks_read\tshared_blk_read_time_ms\tquery'

# The containers whose CPU time a snapshot reads besides PostgreSQL's (which its own probe reads).
read -ra CPU_SERVICES <<<"backend mongo ${LOAD_SERVICES_EXTRA:-}"

snapshot() { # point  mode (none | reset | dump | dump-reset)  [statements file]
  local point="$1" mode="$2" statements="${3:-}" tmp service line pids=()
  tmp="$(mktemp -d "$RUN_DIR/.snapshot.XXXXXX")"
  printf '%s\tstand\tepoch_ms\t%s\n' "$point" "$(date +%s%3N)" >> "$RUN_DIR/counters.tsv"
  compose exec -T postgres sh -c "$PG_PROBE" sh "$mode" > "$tmp/postgres" 2>"$tmp/postgres.err" &
  pids+=($!)
  for service in "${CPU_SERVICES[@]}"; do
    cpu_probe "$service" > "$tmp/$service" &
    pids+=($!)
  done
  wait "${pids[@]}" || true # never a bare `wait`: the stats sampler is a background job too
  if [[ -n "$statements" ]]; then printf '%s\n' "$STATEMENTS_HEADER" > "$statements"; fi
  for service in postgres "${CPU_SERVICES[@]}"; do
    while IFS= read -r line; do
      case "$line" in
        stmt$'\t'*) if [[ -n "$statements" ]]; then printf '%s\n' "${line#stmt$'\t'}" >> "$statements"; fi ;;
        *$'\t'*) printf '%s\t%s\t%s\n' "$point" "$service" "$line" >> "$RUN_DIR/counters.tsv" ;;
      esac
    done < "$tmp/$service"
  done
  local missing=()
  grep -q '^cpu_usec' "$tmp/postgres" || missing+=("postgres cpu")
  grep -q '^xact_commit' "$tmp/postgres" || missing+=("postgres statistics ($(head -c 200 "$tmp/postgres.err" | tr '\n' ' '))")
  grep -q '^net_rx_packets' "$tmp/postgres" || missing+=("postgres network packets (no interface but lo)")
  for service in "${CPU_SERVICES[@]}"; do
    grep -q '^cpu_usec' "$tmp/$service" || missing+=("$service cpu")
  done
  if (( ${#missing[@]} > 0 )); then log "snapshot $point: could not read: $(IFS=';'; echo "${missing[*]}")"; fi
  rm -rf "$tmp"
}

hook_before_k6() {
  : > "$RUN_DIR/counters.tsv"
  snapshot before reset
}

hook_after_k6() {
  if [[ "${DRAIN:-0}" != 0 ]]; then
    snapshot after dump-reset "$RUN_DIR/pg_statements.tsv"
  else
    snapshot after dump "$RUN_DIR/pg_statements.tsv"
  fi
}

hook_after_drain() {
  snapshot drained dump "$RUN_DIR/pg_statements.drain.tsv"
}

hook_after_collect() {
  # PostgreSQL's server log of the whole run: how many statements failed (ERROR lines, each with its
  # STATEMENT) and how many bytes the server wrote, e.g. for write conflicts raised as errors.
  local errors bytes
  errors="$(compose logs --no-color postgres 2>/dev/null | grep -c 'ERROR:' || true)"
  bytes="$(compose logs --no-color postgres 2>/dev/null | wc -c | tr -d '[:space:]' || true)"
  if [[ "$errors" =~ ^[0-9]+$ ]]; then printf 'final\tpostgres\tlog_errors\t%s\n' "$errors" >> "$RUN_DIR/counters.tsv"; fi
  if [[ "$bytes" =~ ^[0-9]+$ ]]; then printf 'final\tpostgres\tlog_bytes\t%s\n' "$bytes" >> "$RUN_DIR/counters.tsv"; fi
  PERF_GIT="$(git -C "$PERF_DIR" describe --always --dirty --abbrev=7 2>/dev/null || echo unknown)" \
    node "$PERF_DIR/tools/collect.mjs" "$RUN_DIR" 2>&1 | tee -a "$RUN_DIR/run.log" >&2
}
