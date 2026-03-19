#!/usr/bin/env sh
set -eu

MAIN_APP_PORT="${MAIN_APP_PORT:-8080}"
ROUTING_APP_PORT="${ROUTING_APP_PORT:-3931}"
FILE_APP_PORT="${FILE_APP_PORT:-9443}"

export ROUTING_APP_PORT FILE_APP_PORT
export ROUTING_ENGINE_BASE_URL="${ROUTING_ENGINE_BASE_URL:-http://localhost:${ROUTING_APP_PORT}}"

PIDS=""

start_process() {
  name="$1"
  shift
  echo "[start] ${name}"
  "$@" &
  pid=$!
  PIDS="$PIDS $pid"
  echo "[pid] ${name}=${pid}"
}

stop_all() {
  echo "[stop] shutting down child processes"
  for pid in $PIDS; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait || true
}

trap stop_all INT TERM

start_process etl-main-app java -Dserver.port="$MAIN_APP_PORT" -jar /app/etl-main-app.jar
start_process etl-routing-engine java -jar /app/etl-routing-engine.jar
start_process etl-file-engine java -jar /app/etl-file-engine.jar

wait -n || true
status=$?
echo "[exit] a child process exited with status ${status}"
stop_all
exit "$status"
