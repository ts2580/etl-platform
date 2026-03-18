#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$HOME/code/etl-platform"
PID_DIR="$BASE_DIR/.pids"
LOG_DIR="$BASE_DIR/.logs"
ENV_DIR="$BASE_DIR/env"
COMMON_ENV_FILE="$BASE_DIR/.env"

MAIN_PID="$PID_DIR/etl-main-app.pid"
ROUTING_PID="$PID_DIR/etl-routing-engine.pid"
FILE_PID="$PID_DIR/etl-file-engine.pid"

STARTUP_WAIT_SECONDS="${STARTUP_WAIT_SECONDS:-5}"
GRADLE_RUN_ARGS=(--stacktrace --warning-mode all)
DEBUG_ENV_KEYS=(APP_DB_ENABLED DB_URL DB_USERNAME DB_PASSWORD ROUTING_APP_PORT FILE_APP_PORT SALESFORCE_TOKEN_URL SALESFORCE_ALLOWED_ORIGINS SALESFORCE_LOGIN_URL SALESFORCE_API_VERSION SALESFORCE_INSTANCE_URL REDIS_URL REDIS_PORT REDIS_PASSWORD)

# 필요하면 프로젝트 환경에 맞게 수정
MAIN_PORT="${MAIN_PORT:-8080}"
ROUTING_PORT="${ROUTING_PORT:-3931}"
FILE_PORT="${FILE_PORT:-9443}"

# 모듈별 env 파일 경로 (없어도 실행됨)
MAIN_ENV_FILE="$ENV_DIR/etl-main-app.env"
ROUTING_ENV_FILE="$ENV_DIR/etl-routing-engine.env"
FILE_ENV_FILE="$ENV_DIR/etl-file-engine.env"

mkdir -p "$PID_DIR" "$LOG_DIR" "$ENV_DIR"

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

    if [[ -n "$common_value" ]]; then
      echo "[ENVDBG] $name common $key=$common_value"
    fi
    if [[ -n "$module_value" ]]; then
      echo "[ENVDBG] $name module $key=$module_value"
    fi
    if [[ -n "$common_value" && -n "$module_value" && "$common_value" != "$module_value" ]]; then
      echo "[WARN] $name module env overrides common env for $key"
    fi
  done
}

show_recent_log() {
  local name="$1"
  local log_file="$2"
  if [[ -f "$log_file" ]]; then
    echo "[DEBUG] last 80 lines from $name log:"
    tail -n 80 "$log_file" || true
  else
    echo "[DEBUG] log file not found for $name: $log_file"
  fi
}

start_one() {
  local name="$1"
  local pid_file="$2"
  local log_file="$3"
  local gradle_task="$4"
  local env_file="$5"

  if [[ -f "$pid_file" ]]; then
    local pid
    pid=$(cat "$pid_file")
    if ps -p "$pid" >/dev/null 2>&1; then
      echo "[SKIP] $name already running (pid=$pid)"
      return
    else
      rm -f "$pid_file"
    fi
  fi

  if [[ -f "$COMMON_ENV_FILE" ]]; then
    echo "[ENV] $name load common env $COMMON_ENV_FILE"
  fi
  if [[ -f "$env_file" ]]; then
    echo "[ENV] $name load module env $env_file"
  fi
  print_env_debug "$name" "$env_file"

  echo "[START] $name"
  (
    set -a
    [[ -f "$COMMON_ENV_FILE" ]] && source "$COMMON_ENV_FILE"
    [[ -f "$env_file" ]] && source "$env_file"
    set +a
    nohup ./gradlew "$gradle_task" "${GRADLE_RUN_ARGS[@]}" > "$log_file" 2>&1 &
    echo $! > "$pid_file"
  )

  local pid
  pid=$(cat "$pid_file")
  sleep "$STARTUP_WAIT_SECONDS"

  if ps -p "$pid" >/dev/null 2>&1; then
    echo "[OK] $name started (pid=$pid)"
  else
    echo "[ERROR] $name exited during startup (pid=$pid)"
    show_recent_log "$name" "$log_file"
    return 1
  fi
}

stop_one() {
  local name="$1"
  local pid_file="$2"

  if [[ ! -f "$pid_file" ]]; then
    echo "[SKIP] $name pid file not found"
    return
  fi

  local pid
  pid=$(cat "$pid_file")

  if ps -p "$pid" >/dev/null 2>&1; then
    echo "[STOP] $name (pid=$pid)"
    kill "$pid" || true
    sleep 2

    if ps -p "$pid" >/dev/null 2>&1; then
      echo "[KILL] $name (pid=$pid)"
      kill -9 "$pid" || true
    fi
  else
    echo "[SKIP] $name already stopped"
  fi

  rm -f "$pid_file"
}

status_one() {
  local name="$1"
  local pid_file="$2"

  if [[ -f "$pid_file" ]]; then
    local pid
    pid=$(cat "$pid_file")
    if ps -p "$pid" >/dev/null 2>&1; then
      echo "[RUNNING] $name (pid=$pid)"
      return
    fi
  fi

  echo "[STOPPED] $name"
}

kill_port() {
  local port="$1"
  if [[ -z "$port" ]]; then
    return
  fi

  local pids
  pids=$(lsof -ti tcp:"$port" 2>/dev/null || true)

  if [[ -z "$pids" ]]; then
    echo "[SKIP] no process using port $port"
    return
  fi

  echo "[PORT] killing processes on port $port -> $pids"
  for pid in $pids; do
    kill "$pid" 2>/dev/null || true
  done

  sleep 2

  local remain
  remain=$(lsof -ti tcp:"$port" 2>/dev/null || true)
  if [[ -n "$remain" ]]; then
    echo "[PORT] force killing remaining processes on port $port -> $remain"
    for pid in $remain; do
      kill -9 "$pid" 2>/dev/null || true
    done
  fi
}

kill_ports() {
  kill_port "$MAIN_PORT"
  kill_port "$ROUTING_PORT"
  kill_port "$FILE_PORT"
}

build_all() {
  echo "[BUILD] building 3 modules..."
  ./gradlew clean \
    :etl-main-app:bootJar \
    :etl-routing-engine:bootJar \
    :etl-file-engine:bootJar
  echo "[OK] build completed"
}

start_all() {
  start_one "etl-main-app" "$MAIN_PID" "$LOG_DIR/etl-main-app.log" ":etl-main-app:bootRun" "$MAIN_ENV_FILE"
  start_one "etl-routing-engine" "$ROUTING_PID" "$LOG_DIR/etl-routing-engine.log" ":etl-routing-engine:bootRun" "$ROUTING_ENV_FILE"
  start_one "etl-file-engine" "$FILE_PID" "$LOG_DIR/etl-file-engine.log" ":etl-file-engine:bootRun" "$FILE_ENV_FILE"
}

stop_all() {
  stop_one "etl-main-app" "$MAIN_PID"
  stop_one "etl-routing-engine" "$ROUTING_PID"
  stop_one "etl-file-engine" "$FILE_PID"
}

cd "$BASE_DIR"

case "${1:-}" in
  start)
    start_all
    ;;
  stop)
    stop_all
    ;;
  restart)
    stop_all
    sleep 1
    start_all
    ;;
  rebuild)
    stop_all
    build_all
    sleep 1
    start_all
    ;;
  build)
    build_all
    ;;
  killports)
    kill_ports
    ;;
  status)
    status_one "etl-main-app" "$MAIN_PID"
    status_one "etl-routing-engine" "$ROUTING_PID"
    status_one "etl-file-engine" "$FILE_PID"
    ;;
  logs)
    tail -f \
      "$LOG_DIR/etl-main-app.log" \
      "$LOG_DIR/etl-routing-engine.log" \
      "$LOG_DIR/etl-file-engine.log"
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|build|rebuild|killports|status|logs}"
    echo "Ports: MAIN_PORT=$MAIN_PORT ROUTING_PORT=$ROUTING_PORT FILE_PORT=$FILE_PORT"
    echo "Common env: $COMMON_ENV_FILE"
    echo "Module env: $MAIN_ENV_FILE, $ROUTING_ENV_FILE, $FILE_ENV_FILE"
    exit 1
    ;;
esac
