# shellcheck shell=bash
# Backlog epic 02 measurement hooks for demo/load/run.sh (LOAD_HOOKS, set by perf/run.sh). Sourced by
# run.sh once the run folder exists; uses its RUN_DIR, compose, log, projections_caught_up and the run
# parameters (DRAIN, WARMUP_S, DURATION_S).
#
# A snapshot appends `point  service  key  value` lines to counters.tsv; tools/collect.mjs turns the
# differences between points into figures per command (before → after the burst, and before → drained:
# the whole outbox chain of the accepted commands) and per task (after → drained). Points: `before`
# (statement statistics reset just after), `after` (statements dumped to pg_statements.tsv, and reset
# again with DRAIN=1), `drained` (once the projections have caught up too; statements dumped to
# pg_statements.drain.tsv). The containers are probed in parallel, everything inside one container in
# one go, so a snapshot is a single moment to within a few milliseconds per container; MongoDB's
# operation counters come within the start of mongosh (about half a second) of it.
# - cpu_usec, cpu_throttled_usec: the container's cgroup CPU time and the time its CPU quota held it
#   back, exact; for the back-end, PostgreSQL, MongoDB and every service of LOAD_SERVICES_EXTRA (the
#   second node and the balancer of ticket 17) but the probe.
# - mem_anon_bytes / mem_file_bytes: the container's memory from its cgroup: the processes' own memory
#   and the page cache of the files it reads and writes.
# - net_rx_packets / net_tx_packets: every interface of the PostgreSQL container but `lo`, i.e. its
#   traffic with the back-end (psql below uses the unix socket).
# - xact_commit / xact_rollback: pg_stat_database. PostgreSQL publishes them per backend at most once
#   a second, so the last second of a burst may be missing from `after`.
# - wal_lsn: the WAL insert position, exact (the difference is the WAL written).
# - wal_records / wal_fpi / wal_bytes: pg_stat_wal; wal_fpi counts the full-page images, the WAL that
#   the first change of a page after a checkpoint costs.
# - outbox_completed: completed outbox tasks, the denominator of the drain figures.
# - mongo_insert … mongo_command: db.serverStatus().opcounters, read through the mongo-probe service
#   (compose.perf.yaml). mongosh is a Node program: started inside the mongo container, its own CPU
#   would land in MongoDB's.
# While k6 runs, one psql session samples every 100 ms what PostgreSQL's client sessions are doing
# (pg_stat_activity: state and wait event, `CPU` for an active session without one) into
# wait_events.tsv. A SOAK run of demo/load/run.sh also calls hook_slice every slice: how the database
# grows over the run, into soak.tsv.
# Our own queries carry `/* load-stand */` and an application name `load-stand…`; they and run.sh's
# drain probe (`count(*) filter (where status …`) are left out of the statement files and the samples.
# Nothing here fails a run: a value that could not be read is logged and left out.

PERF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

read -r -d '' PG_PROBE <<'SH' || true
mode="$1"
tab="$(printf '\t')"
cat /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.stat 2>/dev/null | awk '
  $1 == "usage_usec" { print "cpu_usec\t" $2 }
  $1 == "throttled_usec" { print "cpu_throttled_usec\t" $2 }
  $1 == "anon" { print "mem_anon_bytes\t" $2 }
  $1 == "file" { print "mem_file_bytes\t" $2 }'
rx=0; tx=0; n=0
for dir in /sys/class/net/*; do
  [ "${dir##*/}" = lo ] && continue
  [ -r "$dir/statistics/rx_packets" ] || continue
  rx=$((rx + $(cat "$dir/statistics/rx_packets"))); tx=$((tx + $(cat "$dir/statistics/tx_packets"))); n=$((n + 1))
done
if [ "$n" -gt 0 ]; then printf 'net_rx_packets\t%s\nnet_tx_packets\t%s\n' "$rx" "$tx"; fi
export PGAPPNAME=load-stand
psql -U cqrsx -d encore -X -At -F "$tab" -c "
  select /* load-stand */ 'xact_commit', xact_commit::text from pg_stat_database where datname = current_database()
  union all select 'xact_rollback', xact_rollback::text from pg_stat_database where datname = current_database()
  union all select 'wal_lsn', pg_current_wal_insert_lsn()::text
  union all select 'wal_records', wal_records::text from pg_stat_wal
  union all select 'wal_fpi', wal_fpi::text from pg_stat_wal
  union all select 'wal_bytes', wal_bytes::text from pg_stat_wal
  union all select 'outbox_completed', count(*)::text from transactional_outbox where status = 'completed'"
case "$mode" in
  dump|dump-reset)
    psql -U cqrsx -d encore -X -At -F "$tab" -c "
      select /* load-stand */ 'stmt', queryid, calls, round(total_exec_time::numeric, 3), round(mean_exec_time::numeric, 4),
             rows, shared_blks_hit, shared_blks_read, round(shared_blk_read_time::numeric, 3), wal_records, wal_fpi, wal_bytes,
             regexp_replace(query, '\s+', ' ', 'g')
      from pg_stat_statements
      where toplevel and dbid = (select oid from pg_database where datname = current_database())
        and query !~ 'load-stand' and query !~* 'count\(\*\) filter \(where status'
      order by total_exec_time desc" ;;
esac
case "$mode" in
  reset|dump-reset) psql -U cqrsx -d encore -X -Atq -c "select /* load-stand */ pg_stat_statements_reset()" >/dev/null ;;
esac
SH

cgroup_probe() { # service → cpu_usec, cpu_throttled_usec, mem_anon_bytes, mem_file_bytes from its own cgroup (cgroup v2)
  compose exec -T "$1" cat /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.stat 2>/dev/null | awk '
    $1 == "usage_usec" && $2 ~ /^[0-9]+$/ { print "cpu_usec\t" $2 }
    $1 == "throttled_usec" && $2 ~ /^[0-9]+$/ { print "cpu_throttled_usec\t" $2 }
    $1 == "anon" && $2 ~ /^[0-9]+$/ { print "mem_anon_bytes\t" $2 }
    $1 == "file" && $2 ~ /^[0-9]+$/ { print "mem_file_bytes\t" $2 }' || true
}

mongo_probe() { # → `mongo_<operation>  <count>` lines from db.serverStatus().opcounters, through the probe service
  compose exec -T mongo-probe mongosh --quiet --host mongo --eval '
    const counters = db.serverStatus().opcounters;
    for (const op of ["insert", "query", "update", "delete", "getmore", "command"]) print(`mongo_${op}\t${String(counters[op])}`);' 2>/dev/null || true
}

STATEMENTS_HEADER=$'queryid\tcalls\ttotal_exec_time_ms\tmean_exec_time_ms\trows\tshared_blks_hit\tshared_blks_read\tshared_blk_read_time_ms\twal_records\twal_fpi\twal_bytes\tquery'

# The containers whose cgroup a snapshot reads besides PostgreSQL's (which its own probe reads): not the
# probe, whose mongosh is no part of the stand's work.
CPU_SERVICES=()
MONGO_PROBE=0
for service in backend mongo ${LOAD_SERVICES_EXTRA:-}; do
  if [[ "$service" == mongo-probe ]]; then
    MONGO_PROBE=1
  else
    CPU_SERVICES+=("$service")
  fi
done
MONGO_OPS_LINE=$'^mongo_[a-z]+\t[0-9]+$'

snapshot() { # point  mode (none | reset | dump | dump-reset)  [statements file]
  local point="$1" mode="$2" statements="${3:-}" tmp service line pids=()
  tmp="$(mktemp -d "$RUN_DIR/.snapshot.XXXXXX")"
  printf '%s\tstand\tepoch_ms\t%s\n' "$point" "$(date +%s%3N)" >> "$RUN_DIR/counters.tsv"
  compose exec -T postgres sh -c "$PG_PROBE" sh "$mode" > "$tmp/postgres" 2>"$tmp/postgres.err" &
  pids+=($!)
  for service in "${CPU_SERVICES[@]}"; do
    cgroup_probe "$service" > "$tmp/$service" &
    pids+=($!)
  done
  if (( MONGO_PROBE )); then
    mongo_probe > "$tmp/mongo.ops" &
    pids+=($!)
  fi
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
  if [[ -f "$tmp/mongo.ops" ]]; then
    while IFS= read -r line; do
      if [[ "$line" =~ $MONGO_OPS_LINE ]]; then printf '%s\tmongo\t%s\n' "$point" "$line" >> "$RUN_DIR/counters.tsv"; fi
    done < "$tmp/mongo.ops"
  fi
  local missing=()
  grep -q '^cpu_usec' "$tmp/postgres" || missing+=("postgres cpu")
  grep -q '^xact_commit' "$tmp/postgres" || missing+=("postgres statistics ($(head -c 200 "$tmp/postgres.err" | tr '\n' ' '))")
  grep -q '^wal_fpi' "$tmp/postgres" || missing+=("postgres pg_stat_wal")
  grep -q '^net_rx_packets' "$tmp/postgres" || missing+=("postgres network packets (no interface but lo)")
  for service in "${CPU_SERVICES[@]}"; do
    grep -q '^cpu_usec' "$tmp/$service" || missing+=("$service cpu")
  done
  if (( MONGO_PROBE )) && ! grep -q '^mongo_insert' "$tmp/mongo.ops"; then missing+=("mongo operation counters"); fi
  if (( ${#missing[@]} > 0 )); then log "snapshot $point: could not read: $(IFS=';'; echo "${missing[*]}")"; fi
  rm -rf "$tmp"
}

# --- what the sessions wait for ---------------------------------------------------------------------
# One `sample` line per sample (so that a sample with no busy session still counts), then one line per
# state and wait event with the number of client sessions in it. `\watch` stops by itself after the
# window and a little more, should nobody stop it.
read -r -d '' WAITS_SQL <<'SQL' || true
with /* load-stand waits */ now as (select (extract(epoch from clock_timestamp()) * 1000)::bigint as ms)
select now.ms, 'sample', '-', 0 from now
union all
select now.ms, a.state, coalesce(a.wait_event_type || ':' || a.wait_event, 'CPU'), count(*)
from now, pg_stat_activity a
where a.backend_type = 'client backend' and a.state <> 'idle' and a.pid <> pg_backend_pid()
  and a.application_name not like 'load-stand%'
group by now.ms, a.state, 3
SQL
WAITS_INTERVAL_S="${PERF_WAITS_INTERVAL_S:-0.1}"
WAITS_PID=

start_wait_sampler() {
  local seconds count
  seconds=$(( ${WARMUP_S:-3} + ${DURATION_S:-10} + 10 ))
  count=$(awk -v s="$seconds" -v i="$WAITS_INTERVAL_S" 'BEGIN { printf "%d", s / i }')
  printf 'epoch_ms\tstate\twait\tsessions\n' > "$RUN_DIR/wait_events.tsv"
  printf '%s \\watch interval=%s count=%s\n' "$WAITS_SQL" "$WAITS_INTERVAL_S" "$count" \
    | compose exec -T -e PGAPPNAME=load-stand-waits postgres psql -U cqrsx -d encore -X -At -F $'\t' -f - \
      >> "$RUN_DIR/wait_events.tsv" 2>/dev/null &
  WAITS_PID=$!
}

stop_wait_sampler() {
  [[ -n "$WAITS_PID" ]] || return 0
  compose exec -T postgres psql -U cqrsx -d encore -X -Atq -c \
    "select /* load-stand */ pg_terminate_backend(pid) from pg_stat_activity where application_name = 'load-stand-waits'" >/dev/null 2>&1 || true
  local _
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "$WAITS_PID" 2>/dev/null || break
    sleep 0.2
  done
  kill "$WAITS_PID" 2>/dev/null || true
  wait "$WAITS_PID" 2>/dev/null || true
  WAITS_PID=
}

# --- the database over a SOAK run -------------------------------------------------------------------
read -r -d '' SLICE_SQL <<'SQL' || true
select /* load-stand */ 'size_' || relname, pg_total_relation_size(relid)::text from pg_stat_user_tables where relname in ('events', 'snapshots', 'transactional_outbox')
union all select 'index_size_' || relname, pg_indexes_size(relid)::text from pg_stat_user_tables where relname in ('events', 'snapshots', 'transactional_outbox')
union all select 'live_tup_' || relname, n_live_tup::text from pg_stat_user_tables where relname in ('events', 'snapshots', 'transactional_outbox')
union all select 'dead_tup_' || relname, n_dead_tup::text from pg_stat_user_tables where relname in ('events', 'snapshots', 'transactional_outbox')
union all select 'autovacuum_' || relname, autovacuum_count::text from pg_stat_user_tables where relname in ('events', 'snapshots', 'transactional_outbox')
union all select 'upd_' || relname, n_tup_upd::text from pg_stat_user_tables where relname in ('events', 'snapshots', 'transactional_outbox')
union all select 'hot_upd_' || relname, n_tup_hot_upd::text from pg_stat_user_tables where relname in ('events', 'snapshots', 'transactional_outbox')
union all select 'checkpoints_timed', num_timed::text from pg_stat_checkpointer
union all select 'checkpoints_requested', num_requested::text from pg_stat_checkpointer
union all select 'checkpoint_buffers', buffers_written::text from pg_stat_checkpointer
union all select 'wal_bytes', wal_bytes::text from pg_stat_wal
union all select 'wal_fpi', wal_fpi::text from pg_stat_wal
union all select 'temp_bytes', temp_bytes::text from pg_stat_database where datname = current_database()
SQL

hook_slice() { # epoch → `epoch  postgres  key  value` lines in soak.tsv
  local epoch="$1" key value
  compose exec -T -e PGAPPNAME=load-stand postgres psql -U cqrsx -d encore -X -At -F $'\t' -c "$SLICE_SQL" 2>/dev/null \
    | while IFS=$'\t' read -r key value; do
        if [[ -n "$key" && -n "$value" ]]; then printf '%s\tpostgres\t%s\t%s\n' "$epoch" "$key" "$value"; fi
      done >> "$RUN_DIR/soak.tsv" || true
}

# --- the points -------------------------------------------------------------------------------------
hook_before_k6() {
  : > "$RUN_DIR/counters.tsv"
  snapshot before reset
  start_wait_sampler
}

hook_after_k6() {
  stop_wait_sampler
  if [[ "${DRAIN:-0}" != 0 ]]; then
    snapshot after dump-reset "$RUN_DIR/pg_statements.tsv"
  else
    snapshot after dump "$RUN_DIR/pg_statements.tsv"
  fi
}

hook_after_drain() {
  # The chain ends in the read models: the last tasks' events are still on their way to MongoDB when the
  # outbox is empty.
  local deadline=$((SECONDS + ${PERF_CATCH_UP_S:-30}))
  until projections_caught_up || (( SECONDS >= deadline )); do sleep 1; done
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
