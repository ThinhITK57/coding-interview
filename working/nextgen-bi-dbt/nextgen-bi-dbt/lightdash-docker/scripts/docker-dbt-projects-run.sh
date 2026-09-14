#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

ENV_FILE="${1:-$ROOT_DIR/.env.lightdash}"
IMAGE_NAME="${2:-vcs-lightdash-dbt-projects:latest}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker command not found." >&2
  exit 127
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: env file not found at $ENV_FILE" >&2
  exit 1
fi

echo "[dbt-projects-run] Running deploy container with env file: $ENV_FILE"
docker run --rm \
  --env-file "$ENV_FILE" \
  --network host \
  "$IMAGE_NAME"

echo "[dbt-projects-run] Done."
