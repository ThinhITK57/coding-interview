#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE="${1:-redeploy}"
ENV_FILE="${2:-$ROOT_DIR/.env.lightdash}"
IMAGE_NAME="${3:-vcs-lightdash-dbt-projects:latest}"
DOMAINS="${4:-}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker command not found." >&2
  exit 127
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: env file not found at $ENV_FILE" >&2
  exit 1
fi

case "$MODE" in
  create|redeploy)
    ;;
  *)
    echo "Usage: $0 [create|redeploy] [ENV_FILE] [IMAGE_NAME] [DOMAINS_CSV]" >&2
    echo "Example 1 (create all): $0 create ./.env.lightdash" >&2
    echo "Example 2 (redeploy selected): $0 redeploy ./.env.lightdash vcs-lightdash-dbt-projects:latest crm,cx,finance" >&2
    exit 1
    ;;
esac

echo "[dbt-projects-deploy] Mode: $MODE"
echo "[dbt-projects-deploy] Env file: $ENV_FILE"
echo "[dbt-projects-deploy] Image: $IMAGE_NAME"
if [[ -n "$DOMAINS" ]]; then
  echo "[dbt-projects-deploy] Domains: $DOMAINS"
else
  echo "[dbt-projects-deploy] Domains: all default domains"
fi

docker run --rm \
  --env-file "$ENV_FILE" \
  --env DEPLOY_MODE="$MODE" \
  --env DEPLOY_DOMAINS="$DOMAINS" \
  --network host \
  "$IMAGE_NAME"

echo "[dbt-projects-deploy] Done."
