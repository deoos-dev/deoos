#!/bin/sh
# Local deoos: RustFS from compose.yaml, the dev bucket, and deoos-server.
#   ./scripts/dev.sh        start (foreground) and print env to export
#   ./scripts/dev.sh down   stop the server and RustFS; keep the volume
set -eu
cd "$(dirname "$0")/.."
ROOT=$(pwd)
PYTHON=${PYTHON:-python3}
ACTION=${1:-up}

# Local RustFS always uses the compose.yaml credentials. Ambient AWS keys and
# session tokens are for cloud accounts and would fail against this endpoint.
ENDPOINT=${AWS_ENDPOINT:-http://127.0.0.1:19000}
if [ "$ENDPOINT" = "http://127.0.0.1:19000" ]; then
  AWS_ACCESS_KEY_ID=local-development
  AWS_SECRET_ACCESS_KEY=local-development-only-secret
  AWS_REGION=us-east-1
  AWS_ALLOW_HTTP=true
  DEOOS_STORAGE_PROVIDER=s3
  unset AWS_SESSION_TOKEN || true
else
  AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required when AWS_ENDPOINT is not local RustFS}
  AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required when AWS_ENDPOINT is not local RustFS}
  AWS_REGION=${AWS_REGION:-us-east-1}
  AWS_ALLOW_HTTP=${AWS_ALLOW_HTTP:-true}
  DEOOS_STORAGE_PROVIDER=${DEOOS_STORAGE_PROVIDER:-s3}
fi

DEOOS_STORAGE_BUCKET=${DEOOS_STORAGE_BUCKET:-deoos-dev}
EXECUTION_PREFIX=${EXECUTION_PREFIX:-deoos}
ENGINE_BIND=${ENGINE_BIND:-127.0.0.1:7331}
ENGINE_URL=${ENGINE_URL:-http://127.0.0.1:7331}
LEASE_MS=${LEASE_MS:-30000}
PIDFILE=$ROOT/outputs/dev-server.pid
ENVFILE=$ROOT/outputs/dev.env
BIN=$ROOT/engine/target/release/deoos-server

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_ENDPOINT="$ENDPOINT"
export AWS_ALLOW_HTTP DEOOS_STORAGE_PROVIDER DEOOS_STORAGE_BUCKET EXECUTION_PREFIX
export ENGINE_BIND ENGINE_URL LEASE_MS

sq() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

print_env() {
  {
    printf 'export ENGINE_URL=%s\n' "$(sq "$ENGINE_URL")"
    printf 'export PYTHONPATH=%s\n' "$(sq "$ROOT/clients/python")"
    printf 'export DEOOS_STORAGE_PROVIDER=%s\n' "$(sq "$DEOOS_STORAGE_PROVIDER")"
    printf 'export DEOOS_STORAGE_BUCKET=%s\n' "$(sq "$DEOOS_STORAGE_BUCKET")"
    printf 'export AWS_ACCESS_KEY_ID=%s\n' "$(sq "$AWS_ACCESS_KEY_ID")"
    printf 'export AWS_SECRET_ACCESS_KEY=%s\n' "$(sq "$AWS_SECRET_ACCESS_KEY")"
    printf 'export AWS_REGION=%s\n' "$(sq "$AWS_REGION")"
    printf 'export AWS_ENDPOINT=%s\n' "$(sq "$ENDPOINT")"
    printf 'export AWS_ALLOW_HTTP=%s\n' "$(sq "$AWS_ALLOW_HTTP")"
    printf 'export EXECUTION_PREFIX=%s\n' "$(sq "$EXECUTION_PREFIX")"
    printf 'export ENGINE_BIND=%s\n' "$(sq "$ENGINE_BIND")"
    printf 'export LEASE_MS=%s\n' "$(sq "$LEASE_MS")"
    if [ -n "${ENGINE_TOKEN:-}" ]; then
      printf 'export ENGINE_TOKEN=%s\n' "$(sq "$ENGINE_TOKEN")"
    fi
  } | tee "$ENVFILE"
}

healthy() {
  "$PYTHON" - "$ENGINE_URL" <<'PY'
import sys, urllib.request
try:
    with urllib.request.urlopen(sys.argv[1] + "/health", timeout=1) as response:
        raise SystemExit(0 if response.status == 200 else 1)
except Exception:
    raise SystemExit(1)
PY
}

stop_pid() {
  if [ ! -f "$PIDFILE" ]; then
    return
  fi
  pid=$(cat "$PIDFILE" 2>/dev/null || true)
  case "$pid" in
    ''|*[!0-9]*) ;;
    *)
      kill "$pid" 2>/dev/null || true
      i=0
      while [ "$i" -lt 30 ] && kill -0 "$pid" 2>/dev/null; do
        i=$((i + 1))
        sleep 0.1
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
      ;;
  esac
  rm -f "$PIDFILE"
}

down() {
  stop_pid
  docker compose stop rustfs >/dev/null 2>&1 || true
  echo "stopped deoos-server and RustFS (volume kept)" >&2
}

up() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required" >&2
    exit 1
  fi
  if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo is required to build deoos-server" >&2
    exit 1
  fi
  mkdir -p "$ROOT/outputs"
  if healthy; then
    echo "deoos-server is already listening at $ENGINE_URL (not restarted). make dev-down stops it." >&2
    if [ -f "$ENVFILE" ]; then
      cat "$ENVFILE"
    else
      print_env
    fi
    exit 0
  fi
  docker compose up -d rustfs >&2
  if [ "$ENDPOINT" = "http://127.0.0.1:19000" ]; then
    "$PYTHON" "$ROOT/scripts/ensure_bucket.py"
  else
    echo "AWS_ENDPOINT is not local RustFS; not creating a bucket" >&2
  fi
  cargo build --locked --release --manifest-path "$ROOT/engine/Cargo.toml" --bin deoos-server >&2
  echo "starting deoos-server at $ENGINE_URL (bucket $DEOOS_STORAGE_BUCKET). make dev-down stops it." >&2
  "$BIN" &
  pid=$!
  echo "$pid" > "$PIDFILE"
  trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; rm -f "$PIDFILE"; exit 130' INT
  trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; rm -f "$PIDFILE"; exit 143' TERM
  trap 'rm -f "$PIDFILE"' EXIT
  i=0
  while [ "$i" -lt 100 ]; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "deoos-server exited before it was ready" >&2
      wait "$pid" || true
      exit 1
    fi
    if healthy; then
      print_env
      set +e
      wait "$pid"
      status=$?
      set -e
      if [ "$status" -gt 128 ]; then
        exit 0
      fi
      exit "$status"
    fi
    i=$((i + 1))
    sleep 0.1
  done
  echo "deoos-server did not become ready at $ENGINE_URL" >&2
  kill "$pid" 2>/dev/null || true
  exit 1
}

case "$ACTION" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 [up|down]" >&2; exit 2 ;;
esac
