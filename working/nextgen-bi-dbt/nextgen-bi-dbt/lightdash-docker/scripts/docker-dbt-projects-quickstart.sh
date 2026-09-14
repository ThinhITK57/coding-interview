#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${1:-$ROOT_DIR/.env.lightdash}"
IMAGE_NAME="${2:-vcs-lightdash-dbt-projects:latest}"

if [[ ! -f "$SCRIPT_DIR/docker-dbt-projects-build.sh" || ! -f "$SCRIPT_DIR/docker-dbt-projects-run.sh" ]]; then
  echo "Error: required scripts are missing." >&2
  exit 1
fi

echo "[dbt-projects-quickstart] Step 1/2: Build image"
"$SCRIPT_DIR/docker-dbt-projects-build.sh" "$IMAGE_NAME"

echo "[dbt-projects-quickstart] Step 2/2: Deploy projects"
"$SCRIPT_DIR/docker-dbt-projects-run.sh" "$ENV_FILE" "$IMAGE_NAME"

echo "[dbt-projects-quickstart] Done."
