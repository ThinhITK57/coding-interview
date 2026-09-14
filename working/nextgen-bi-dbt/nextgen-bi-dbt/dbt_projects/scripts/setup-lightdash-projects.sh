#!/bin/bash

set -euo pipefail

LIGHTDASH_URL="${LIGHTDASH_URL:-http://localhost:8080}"
LIGHTDASH_API_TOKEN="${LIGHTDASH_API_TOKEN:-}"
DEFAULT_DOMAINS=(crm cx finance hr jira noc investment other nextgen_bi)
DEPLOY_MODE="${DEPLOY_MODE:-redeploy}"
DEPLOY_DOMAINS="${DEPLOY_DOMAINS:-}"

if [[ -z "$LIGHTDASH_API_TOKEN" ]]; then
  echo "Error: LIGHTDASH_API_TOKEN is required" >&2
  exit 1
fi

if ! command -v lightdash >/dev/null 2>&1; then
  echo "Error: lightdash CLI is not installed in this container" >&2
  exit 1
fi

if ! command -v dbt >/dev/null 2>&1; then
  echo "Error: dbt is not installed in this container" >&2
  exit 1
fi

case "$DEPLOY_MODE" in
  create|redeploy)
    ;;
  *)
    echo "Error: DEPLOY_MODE must be 'create' or 'redeploy' (got: $DEPLOY_MODE)" >&2
    exit 1
    ;;
esac

if [[ -n "$DEPLOY_DOMAINS" ]]; then
  IFS=',' read -r -a DOMAINS <<< "$DEPLOY_DOMAINS"
else
  DOMAINS=("${DEFAULT_DOMAINS[@]}")
fi

echo "[setup] Login Lightdash at: $LIGHTDASH_URL"
lightdash login "$LIGHTDASH_URL" --token "$LIGHTDASH_API_TOKEN"

for domain in "${DOMAINS[@]}"; do
  domain="$(echo "$domain" | xargs)"
  if [[ -z "$domain" ]]; then
    continue
  fi

  project_dir="/workspace/$domain"
  if [[ ! -f "$project_dir/dbt_project.yml" ]]; then
    echo "[setup] Skip $domain (dbt_project.yml not found)"
    continue
  fi

  # pwd
  # cd $project_dir
  # echo "\n=> dbt build"
  # dbt build
  # echo "\n=> dbt docs generate"
  # dbt docs generate
  # # echo "\n=> lightdash dbt run"
  # # lightdash dbt run
  # echo "\n=> lightdash compile"
  # lightdash compile
  # cd ..
  # pwd

  if [[ "$DEPLOY_MODE" == "create" ]]; then
        echo "[setup-local] Create project if missing and deploy: $domain"

        lightdash deploy \
        -y \
        --verbose \
        --use-dbt-list true \
        --gzip \
        --parallel-batches 5 \
        --use-batched-deploy \
        --threads 5 \
        --ignore-errors \
        --create "$domain" \
        --project-dir "$project_dir" \
        --profiles-dir "$project_dir"

    else
        echo "[setup-local] Redeploy project: $domain"

        lightdash deploy \
        -y \
        --verbose \
        --use-dbt-list true \
        --gzip \
        --parallel-batches 5 \
        --use-batched-deploy \
        --threads 5 \
        --ignore-errors \
        --project-dir "$project_dir" \
        --profiles-dir "$project_dir"
    fi
done

echo "[setup] Completed mode '$DEPLOY_MODE' for selected domain projects."
