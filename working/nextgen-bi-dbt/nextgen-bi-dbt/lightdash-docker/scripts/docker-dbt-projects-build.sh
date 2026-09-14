#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE_NAME="${1:-vcs-lightdash-dbt-projects:latest}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker command not found." >&2
  exit 127
fi

echo "[dbt-projects-build] Building image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" "$ROOT_DIR/dbt_projects"
echo "[dbt-projects-build] Done."
