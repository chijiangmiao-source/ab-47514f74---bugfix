#!/bin/sh
# One-shot acceptance run: build, start app + 2 workers, run the pre-restart
# acceptance phase, RESTART app + workers (startup convergence), then run the
# post-restart phase against the same stable export ids. Reports via exit code.
set -u
cd "$(dirname "$0")/.."

# Shared run id so both phases reference the same stable export identifiers.
RUN="${VERIFY_RUN:-$(python3 -c 'import uuid;print(uuid.uuid4().hex[:6])')}"
echo "verify run id: $RUN"

docker compose up -d --build --wait app worker || exit 1

docker compose run --rm -e VERIFY_PHASE=pre -e VERIFY_RUN="$RUN" verify
pre_code=$?
if [ "$pre_code" -ne 0 ]; then
  echo "pre-restart acceptance failed ($pre_code)"
  docker compose down
  exit "$pre_code"
fi

# Restart the service and both background workers: the erroneous PUBLISHED
# state must converge to each export's own verifiable artifact on startup.
echo "restarting app + workers before post-restart acceptance..."
docker compose restart app worker
docker compose up -d --wait app worker

docker compose run --rm -e VERIFY_PHASE=post -e VERIFY_RUN="$RUN" verify
post_code=$?

docker compose down
exit "$post_code"
