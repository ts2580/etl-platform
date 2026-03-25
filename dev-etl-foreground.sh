#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$SCRIPT_DIR"
cd "$BASE_DIR"

ENV_DIR="$BASE_DIR/env"
LOG_DIR="$BASE_DIR/.logs"
COMMON_ENV_FILE="$BASE_DIR/.env"
MAIN_ENV_FILE="$ENV_DIR/etl-main-app.env"
ROUTING_ENV_FILE="$ENV_DIR/etl-routing-engine.env"
FILE_ENV_FILE="$ENV_DIR/etl-file-engine.env"

MAIN_PORT="${MAIN_PORT:-8080}"
ROUTING_PORT="${ROUTING_PORT:-3931}"
FILE_PORT="${FILE_PORT:-9443}"

MAIN_JAR="$BASE_DIR/etl-main-app/build/libs/etl-main-app.jar"
ROUTING_JAR="$BASE_DIR/etl-routing-engine/build/libs/etl-routing-engine.jar"
FILE_JAR="$BASE_DIR/etl-file-engine/build/libs/etl-file-engine.jar"

DEBUG_ENV_KEYS=(APP_DB_ENABLED DB_URL DB_USERNAME DB_PASSWORD ROUTING_APP_PORT FILE_APP_PORT SALESFORCE_TOKEN_URL SALESFORCE_ALLOWED_ORIGINS SALESFORCE_LOGIN_URL SALESFORCE_API_VERSION SALESFORCE_INSTANCE_URL REDIS_URL REDIS_PORT REDIS_PASSWORD)

MAIN_PID=""
ROUTING_PID=""
FILE_PID=""
TAIL_PID=""

MAIN_LOG="$LOG_DIR/etl-main-app.foreground.log"
ROUTING_LOG="$LOG_DIR/etl-routing-engine.foreground.log"
FILE_LOG="$LOG_DIR/etl-file-engine.foreground.log"

mkdir -p "$LOG_DIR" "$ENV_DIR"

get_env_value() {
  local file="$1"
  local key="$2"
  [[ -f "$file" ]] || return 0
  awk -F= -v key="$key" '$1 == key {print substr($0, index($0, "=") + 1)}' "$file" | tail -n 1
}

print_env_debug() {
  local name="$1"
  local env_file="$2"
  echo "[ENVDBG] $name effective env summary"
  for key in "${DEBUG_ENV_KEYS[@]}"; do
    local common_value module_value
    common_value=$(get_env_value "$COMMON_ENV_FILE" "$key")
    module_value=$(get_env_value "$env_file" "$key")
    [[ -n "$common_value" ]] && echo "[ENVDBG] $name common $key=$common_value"
    [[ -n "$module_value" ]] && echo "[ENVDBG] $name module $key=$module_value"
    if [[ -n "$common_value" && -n "$module_value" && "$common_value" != "$module_value" ]]; then
      echo "[WARN] $name module env overrides common env for $key"
    fi
  done
}

show_recent_log() {
  local name="$1"
  local log_file="$2"
  echo "[DEBUG] last 120 lines from $name log:"
  if [[ -f "$log_file" ]]; then
    tail -n 120 "$log_file" || true
  else
    echo "[DEBUG] log file not found: $log_file"
  fi
}

kill_port() {
  local port="$1"
  local pids
  pids=$(lsof -ti tcp:"$port" 2>/dev/null || true)
  if [[ -z "$pids" ]]; then
    echo "[SKIP] no process using port $port"
    return
  fi
  echo "[PORT] killing processes on port $port -> $pids"
  for pid in $pids; do kill "$pid" 2>/dev/null || true; done
  sleep 2
  local remain
  remain=$(lsof -ti tcp:"$port" 2>/dev/null || true)
  if [[ -n "$remain" ]]; then
    echo "[PORT] force killing remaining processes on port $port -> $remain"
    for pid in $remain; do kill -9 "$pid" 2>/dev/null || true; done
  fi
}

stop_gradle_daemons() {
  ./gradlew --stop >/dev/null 2>&1 || true
}

stop_children() {
  echo
  echo "[STOP] shutting down child processes..."

  [[ -n "$TAIL_PID" ]] && kill "$TAIL_PID" 2>/dev/null || true

  for pid in "$MAIN_PID" "$ROUTING_PID" "$FILE_PID"; do
    if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
      kill "$pid" 2>/dev/null || true
    fi
  done

  sleep 2

  for pid in "$MAIN_PID" "$ROUTING_PID" "$FILE_PID"; do
    if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done

  kill_port "$MAIN_PORT"
  kill_port "$ROUTING_PORT"
  kill_port "$FILE_PORT"
  stop_gradle_daemons
}

trap 'stop_children; exit 130' INT TERM

build_tailwind_css() {
  echo "[BUILD] building tailwind css..."
  (
    cd "$BASE_DIR/etl-main-app"

    if [[ ! -f "package.json" ]]; then
      echo "[ERROR] package.json not found in etl-main-app. skipped tailwind build"
      exit 1
    fi

    if ! command -v npm >/dev/null 2>&1; then
      echo "[ERROR] npm not found. Install Node.js (npm) for tailwind build."
      exit 1
    fi

    if [[ ! -d node_modules ]]; then
      echo "[BUILD] npm ci (node_modules not found)"
      npm ci --no-audit --no-fund --silent
    else
      echo "[BUILD] using existing node_modules (skip npm ci)"
    fi

    npm run build:css
  )

  if [[ ! -f "$BASE_DIR/etl-main-app/src/main/resources/static/css/tailwind.css" ]]; then
    echo "[ERROR] expected tailwind output not found after build"
    exit 1
  fi

  cp "$BASE_DIR/etl-main-app/src/main/resources/static/css/tailwind.css" "$BASE_DIR/etl-file-engine/src/main/resources/static/css/tailwind.css"
  echo "[OK] tailwind build done and synced"
}

build_all() {
  build_tailwind_css
  echo "[BUILD] building jars..."
  ./gradlew clean \
    :etl-main-app:bootJar \
    :etl-routing-engine:bootJar \
    :etl-file-engine:bootJar
  echo "[OK] build completed"
}

check_jars() {
  [[ -f "$MAIN_JAR" ]] || { echo "[ERROR] missing jar: $MAIN_JAR"; exit 1; }
  [[ -f "$ROUTING_JAR" ]] || { echo "[ERROR] missing jar: $ROUTING_JAR"; exit 1; }
  [[ -f "$FILE_JAR" ]] || { echo "[ERROR] missing jar: $FILE_JAR"; exit 1; }
}

start_one() {
  local name="$1"
  local jar_path="$2"
  local env_file="$3"
  local log_file="$4"
  local pid_var="$5"

  [[ -f "$COMMON_ENV_FILE" ]] && echo "[ENV] $name load common env $COMMON_ENV_FILE"
  [[ -f "$env_file" ]] && echo "[ENV] $name load module env $env_file"
  print_env_debug "$name" "$env_file"

  : > "$log_file"

  (
    set -a
    [[ -f "$COMMON_ENV_FILE" ]] && source "$COMMON_ENV_FILE"
    [[ -f "$env_file" ]] && source "$env_file"
    set +a
    exec java -jar "$jar_path"
  ) >> "$log_file" 2>&1 &

  printf -v "$pid_var" '%s' "$!"
}

stream_logs() {
  touch "$MAIN_LOG" "$ROUTING_LOG" "$FILE_LOG"
  tail -n +1 -F "$MAIN_LOG" "$ROUTING_LOG" "$FILE_LOG" 2>/dev/null | \
    while IFS= read -r line; do
      case "$line" in
        "==> $MAIN_LOG <==") continue ;;
        "==> $ROUTING_LOG <==") continue ;;
        "==> $FILE_LOG <==") continue ;;
      esac
      if grep -Fq "$line" "$MAIN_LOG" 2>/dev/null; then
        echo "[main] $line"
      elif grep -Fq "$line" "$ROUTING_LOG" 2>/dev/null; then
        echo "[routing] $line"
      else
        echo "[file] $line"
      fi
    done &
  TAIL_PID=$!
}

start_all() {
  echo "[START] etl-main-app"
  start_one "etl-main-app" "$MAIN_JAR" "$MAIN_ENV_FILE" "$MAIN_LOG" MAIN_PID

  echo "[START] etl-routing-engine"
  start_one "etl-routing-engine" "$ROUTING_JAR" "$ROUTING_ENV_FILE" "$ROUTING_LOG" ROUTING_PID

  echo "[START] etl-file-engine"
  start_one "etl-file-engine" "$FILE_JAR" "$FILE_ENV_FILE" "$FILE_LOG" FILE_PID

  echo "[OK] all processes started"
  echo "[INFO] press Ctrl+C to stop all safely"
  echo "[INFO] foreground logs:"
  echo "       - $MAIN_LOG"
  echo "       - $ROUTING_LOG"
  echo "       - $FILE_LOG"
  echo "[PID] main=$MAIN_PID routing=$ROUTING_PID file=$FILE_PID"
  stream_logs
}

wait_all() {
  local exit_code=0
  set +e

  wait "$MAIN_PID"
  local main_exit=$?
  if [[ $main_exit -ne 0 ]]; then
    echo "[ERROR] etl-main-app exited with code $main_exit"
    show_recent_log "etl-main-app" "$MAIN_LOG"
    exit_code=$main_exit
  fi

  wait "$ROUTING_PID"
  local routing_exit=$?
  if [[ $routing_exit -ne 0 ]]; then
    echo "[ERROR] etl-routing-engine exited with code $routing_exit"
    show_recent_log "etl-routing-engine" "$ROUTING_LOG"
    [[ $exit_code -eq 0 ]] && exit_code=$routing_exit
  fi

  wait "$FILE_PID"
  local file_exit=$?
  if [[ $file_exit -ne 0 ]]; then
    echo "[ERROR] etl-file-engine exited with code $file_exit"
    show_recent_log "etl-file-engine" "$FILE_LOG"
    [[ $exit_code -eq 0 ]] && exit_code=$file_exit
  fi

  set -e
  stop_children
  return "$exit_code"
}

case "${1:-run}" in
  run)
    kill_port "$MAIN_PORT"
    kill_port "$ROUTING_PORT"
    kill_port "$FILE_PORT"
    build_all
    check_jars
    start_all
    wait_all
    ;;
  nobuild)
    kill_port "$MAIN_PORT"
    kill_port "$ROUTING_PORT"
    kill_port "$FILE_PORT"
    check_jars
    start_all
    wait_all
    ;;
  build)
    build_all
    ;;
  *)
    echo "Usage: $0 {run|nobuild|build}"
    echo "  run    : kill ports -> build css & jars -> foreground run"
    echo "  nobuild: kill ports -> foreground run"
    echo "  build  : build css + jars"
    exit 1
    ;;

esac
