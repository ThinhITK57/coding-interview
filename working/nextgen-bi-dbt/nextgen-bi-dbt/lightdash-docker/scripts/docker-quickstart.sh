#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${1:-$ROOT_DIR/.env.lightdash}"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker command not found. Please install Docker Desktop first." >&2
  exit 127
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: env file not found at $ENV_FILE" >&2
  echo "Hint: copy .env.lightdash.example to .env.lightdash and update secrets." >&2
  exit 1
fi

if [[ ! -f "$SCRIPT_DIR/docker-build.sh" || ! -f "$SCRIPT_DIR/docker-run.sh" ]]; then
  echo "Error: required scripts docker-build.sh or docker-run.sh are missing." >&2
  exit 1
fi

echo "[quickstart] Step 1/3: Build and pull images"
"$SCRIPT_DIR/docker-build.sh" "$ENV_FILE"

echo "[quickstart] Step 2/3: Start services"
"$SCRIPT_DIR/docker-run.sh" "$ENV_FILE"

echo "[quickstart] Step 3/3: Health check"
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" ps

if command -v curl >/dev/null 2>&1; then
  if curl -fsS --max-time 10 "http://localhost:8080" >/dev/null; then
    echo "[quickstart] Lightdash is reachable at http://localhost:8080"
  else
    echo "[quickstart] Warning: Lightdash endpoint is not reachable yet. Check logs:" >&2
    echo "docker compose --env-file $ENV_FILE -f $COMPOSE_FILE logs -f lightdash" >&2
    exit 1
  fi
else
  echo "[quickstart] curl not found, skipped HTTP check."
  echo "Open http://localhost:8080 to verify manually."
fi

echo "[quickstart] Completed successfully."
