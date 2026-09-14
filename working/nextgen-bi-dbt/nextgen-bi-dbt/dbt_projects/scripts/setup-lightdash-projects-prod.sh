#!/bin/bash

set -euo pipefail

# Base folder containing domain dbt projects (crm, cx, finance, ...).
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DBT_PROJECTS_ROOT="${DBT_PROJECTS_ROOT:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"

ENV_FILE="${ENV_FILE:-$DBT_PROJECTS_ROOT/.env.prod}"
if [[ -f "$ENV_FILE" ]]; then
  echo "[setup-local] Loading env file: $ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "Warning: Env file not found: $ENV_FILE (proceeding with existing env vars)"
  exit 1
fi

LIGHTDASH_URL="${LIGHTDASH_URL:-http://localhost:8080}"
LIGHTDASH_API_TOKEN="${LIGHTDASH_API_TOKEN:-}"
DEFAULT_DOMAINS=(crm cx finance hr jira noc investment other nextgen_bi)
DEPLOY_MODE="${DEPLOY_MODE:-redeploy}"
DEPLOY_DOMAINS="${DEPLOY_DOMAINS:-}"


echo "[setup-local] Using DBT_PROJECTS_ROOT: $DBT_PROJECTS_ROOT"
echo "[setup-local] Using LIGHTDASH_URL: $LIGHTDASH_URL"
echo "[setup-local] Using DEPLOY_MODE: $DEPLOY_MODE"
if [[ -n "$DEPLOY_DOMAINS" ]]; then
  echo "[setup-local] Using DEPLOY_DOMAINS: $DEPLOY_DOMAINS"
else
  echo "[setup-local] Using default domains: ${DEFAULT_DOMAINS[*]}"
fi  

if [[ -z "$LIGHTDASH_API_TOKEN" ]]; then
  echo "Error: LIGHTDASH_API_TOKEN is required" >&2
  exit 1
fi

if ! command -v lightdash >/dev/null 2>&1; then
  echo "Error: lightdash CLI is not installed" >&2
  exit 1
fi

if ! command -v dbt >/dev/null 2>&1; then
  echo "Error: dbt is not installed" >&2
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

echo "[setup-local] Login Lightdash at: $LIGHTDASH_URL"
login_args=(--verbose "$LIGHTDASH_URL" --token "$LIGHTDASH_API_TOKEN")
if [[ -n "${CRM_PROJECT_UUID:-}" ]]; then
  login_args+=(--project "$CRM_PROJECT_UUID")
fi
lightdash login "${login_args[@]}"
echo "[setup-local] Successfully logged in to Lightdash"

for domain in "${DOMAINS[@]}"; do
  domain="$(echo "$domain" | xargs)"
  if [[ -z "$domain" ]]; then
    continue
  fi

  echo "[setup-local] Processing domain: $domain"

  project_args=()
  if [[ "$DEPLOY_MODE" == "redeploy" ]]; then
    domain_upper="$(echo "$domain" | tr '[:lower:]' '[:upper:]')"
    project_uuid_env_name="${domain_upper}_PROJECT_UUID"
    project_uuid="${!project_uuid_env_name:-}"
    if [[ -n "$project_uuid" ]]; then
      # project_args=(--project "$project_uuid")
      export LIGHTDASH_PROJECT="$project_uuid"
      lightdash config set-project --uuid "$project_uuid"
      echo "[setup-local] Use project UUID from env $project_uuid_env_name for $domain"
    fi
  fi

  project_dir="$DBT_PROJECTS_ROOT/$domain"
  if [[ ! -f "$project_dir/dbt_project.yml" ]]; then
    echo "[setup-local] Skip $domain (dbt_project.yml not found in $project_dir)"
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
        --ignore-errors \
        "${project_args[@]}" \
        --create "$domain" \
        --project-dir "$project_dir" \
        --profiles-dir "$project_dir"

    else
        echo "[setup-local] Redeploy project: $domain"

        lightdash deploy \
        -y \
        --verbose \
        --use-dbt-list true \
        --ignore-errors \
        "${project_args[@]}" \
        --project-dir "$project_dir" \
        --profiles-dir "$project_dir"
    fi
    
    echo "[setup-local] Completed domain: $domain"

done

echo "[setup-local] Completed mode '$DEPLOY_MODE' for selected domain projects."
