#!/bin/sh
# One-shot acceptance run: build, start app + 2 workers, run verify (full),
# restart app + workers, run verify (recheck), report via exit code.
set -u
cd "$(dirname "$0")/.."

docker compose up -d --build --wait app worker
docker compose run --rm verify
code=$?
if [ "$code" -eq 0 ]; then
  echo "verify.sh: restarting app + workers; published state must survive"
  docker compose restart app worker
  docker compose run --rm -e VERIFY_MODE=recheck verify
  code=$?
fi
docker compose down
exit $code
