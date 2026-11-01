#!/usr/bin/env bash
# Wait until PostgreSQL and MongoDB accept connections. Uses only bash and the container tools,
# so it works without psql/mongosh on the host.
set -euo pipefail
PG_PORT="${ENCORE_POSTGRES_PORT:-5434}"
MONGO_PORT="${ENCORE_MONGODB_PORT:-27018}"
TIMEOUT="${WAIT_DB_TIMEOUT:-90}"

wait_tcp() {
  local name="$1" port="$2" deadline=$((SECONDS + TIMEOUT))
  until (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      echo "wait-db: ${name} on 127.0.0.1:${port} is not reachable after ${TIMEOUT}s" >&2
      exit 1
    fi
    sleep 1
  done
  exec 3>&- 2>/dev/null || true
  echo "wait-db: ${name} is listening on 127.0.0.1:${port}"
}

wait_tcp PostgreSQL "$PG_PORT"
wait_tcp MongoDB "$MONGO_PORT"

# PostgreSQL may accept TCP before init scripts finish; give the healthcheck a moment.
COMPOSE="${COMPOSE:-podman compose}"
if command -v podman >/dev/null 2>&1 || command -v docker >/dev/null 2>&1; then
  deadline=$((SECONDS + TIMEOUT))
  while true; do
    status="$($COMPOSE ps --format json 2>/dev/null | tr -d '\n' | grep -o '"Health":"[a-z]*"' | sort -u | tr '\n' ' ' || true)"
    if [[ -z "$status" || "$status" == *healthy* && "$status" != *unhealthy* && "$status" != *starting* ]]; then
      break
    fi
    if (( SECONDS >= deadline )); then
      echo "wait-db: containers are not healthy: $status" >&2
      exit 1
    fi
    sleep 1
  done
fi
echo "wait-db: databases are ready"
