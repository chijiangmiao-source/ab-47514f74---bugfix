#!/bin/sh
# Local smoke without docker: server + 2 supervised workers; runs the
# pre-restart acceptance phase, restarts app + workers, then runs the
# post-restart phase against the same stable export ids.
set -u
cd "$(dirname "$0")/.."

DATA="${DATA_DIR:-/tmp/track-export-smoke}"
PORT="${PORT:-8080}"
RUN="${VERIFY_RUN:-$(python3 -c 'import uuid;print(uuid.uuid4().hex[:6])')}"
rm -rf "$DATA"
mkdir -p "$DATA"

start_app() {
  DATA_DIR="$DATA" PORT="$PORT" TEST_HOOKS=1 python3 -m app.server &
  APP_PID=$!
}

start_workers() {
  # restart loops mimic compose `restart: on-failure` for crash-injection scenarios
  DATA_DIR="$DATA" TEST_HOOKS=1 LEASE_TTL_SECONDS=8 POLL_INTERVAL_SECONDS=0.3 \
    sh -c 'while true; do python3 -m app.worker; sleep 1; done' &
  W1_PID=$!
  DATA_DIR="$DATA" TEST_HOOKS=1 LEASE_TTL_SECONDS=8 POLL_INTERVAL_SECONDS=0.3 \
    sh -c 'while true; do python3 -m app.worker; sleep 1; done' &
  W2_PID=$!
}

stop_all() {
  kill "$APP_PID" "$W1_PID" "$W2_PID" 2>/dev/null
  pkill -f 'app.worker' 2>/dev/null
  sleep 1
}

cleanup() { stop_all 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

echo "verify run id: $RUN"
start_app
start_workers
sleep 1

API_BASE="http://localhost:$PORT" DATA_DIR="$DATA" VERIFY_PHASE=pre VERIFY_RUN="$RUN" \
  python3 -m verify.verify
pre_code=$?
if [ "$pre_code" -ne 0 ]; then
  exit "$pre_code"
fi

# Restart the service and both background workers (startup convergence path).
echo "restarting app + workers before post-restart acceptance..."
stop_all
start_app
start_workers

API_BASE="http://localhost:$PORT" DATA_DIR="$DATA" VERIFY_PHASE=post VERIFY_RUN="$RUN" \
  python3 -m verify.verify
exit $?
