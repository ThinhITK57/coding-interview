#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
ENV_FILE="${2:-$ROOT_DIR/.env.lightdash}"
MODE="${1:-keep-data}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker command not found. Please install Docker Desktop first." >&2
  exit 127
fi

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Error: docker-compose.yml not found at $COMPOSE_FILE" >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: env file not found at $ENV_FILE" >&2
  echo "Hint: copy .env.lightdash.example to .env.lightdash and update secrets." >&2
  exit 1
fi

case "$MODE" in
  keep-data)
    echo "[docker-remove] Stopping and removing containers/network only (volumes kept)."
    docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" down --remove-orphans
    ;;
  purge)
    echo "[docker-remove] Purge mode: removing containers, networks, volumes, and local images."
    docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" down -v --remove-orphans --rmi local
    ;;
  *)
    echo "Usage: $0 [keep-data|purge] [ENV_FILE]" >&2
    exit 1
    ;;
esac

echo "[docker-remove] Done."
