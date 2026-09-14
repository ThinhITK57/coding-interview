#!/usr/bin/env bash

set -euo pipefail

is_truthy() {
  case "${1,,}" in
    1|true|yes|y|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

DBT_SCRIPT="${DBT_SCRIPT:-/workspace/dbt_projects/scripts/s3-build-upload-openmetadata-local.sh}"
DBT_DOMAINS="${DBT_DOMAINS:-crm}"
DBT_RUN_ARGS="${DBT_RUN_ARGS:-}"
DBT_PROJECTS_ROOT="${DBT_PROJECTS_ROOT:-/workspace/dbt_projects}"
ENV_FILE="${ENV_FILE:-/workspace/dbt_projects/.env.s3.local}"
ENABLE_GIT_PULL="${ENABLE_GIT_PULL:-false}"
REPO_DIR="${REPO_DIR:-/workspace/repo}"
GIT_BRANCH="${GIT_BRANCH:-master}"
GIT_LAST_SHA_FILE="${GIT_LAST_SHA_FILE:-/var/lib/metadata-agent/last_${GIT_BRANCH}.sha}"
ENABLE_OM_DBT_TRIGGER="${ENABLE_OM_DBT_TRIGGER:-false}"

SHOULD_RUN_JOB="true"
CURRENT_REMOTE_SHA=""

export DBT_PROJECTS_ROOT
export ENV_FILE

echo "[metadata-agent] ===== $(date -u +'%Y-%m-%dT%H:%M:%SZ') ====="

if is_truthy "$ENABLE_GIT_PULL"; then
  if [[ -d "$REPO_DIR/.git" ]]; then
    echo "[metadata-agent] git fetch/pull: $REPO_DIR ($GIT_BRANCH)"
    git -C "$REPO_DIR" fetch origin "$GIT_BRANCH"

    CURRENT_REMOTE_SHA="$(git -C "$REPO_DIR" rev-parse "origin/$GIT_BRANCH")"

    mkdir -p "$(dirname "$GIT_LAST_SHA_FILE")"
    LAST_SUCCESS_SHA=""
    if [[ -f "$GIT_LAST_SHA_FILE" ]]; then
      LAST_SUCCESS_SHA="$(tr -d '[:space:]' < "$GIT_LAST_SHA_FILE")"
    fi

    if [[ -n "$LAST_SUCCESS_SHA" && "$LAST_SUCCESS_SHA" == "$CURRENT_REMOTE_SHA" ]]; then
      echo "[metadata-agent] no new commit since last successful run ($CURRENT_REMOTE_SHA). Skip job."
      SHOULD_RUN_JOB="false"
    fi

    local_sha="$(git -C "$REPO_DIR" rev-parse HEAD)"
    if [[ "$local_sha" != "$CURRENT_REMOTE_SHA" ]]; then
      git -C "$REPO_DIR" checkout "$GIT_BRANCH"
      git -C "$REPO_DIR" pull --ff-only origin "$GIT_BRANCH"
      echo "[metadata-agent] updated to $(git -C "$REPO_DIR" rev-parse --short HEAD)"
    else
      echo "[metadata-agent] local HEAD already at latest origin/$GIT_BRANCH"
    fi
  else
    echo "[metadata-agent] no .git at $REPO_DIR while ENABLE_GIT_PULL=true. Skip job."
    SHOULD_RUN_JOB="false"
  fi
fi

if [[ "$SHOULD_RUN_JOB" != "true" ]]; then
  exit 0
fi

if [[ ! -f "$DBT_SCRIPT" ]]; then
  echo "[metadata-agent] Error: script not found: $DBT_SCRIPT" >&2
  exit 1
fi

IFS=',' read -r -a domains <<<"$DBT_DOMAINS"
trimmed_domains=()
for item in "${domains[@]}"; do
  value="$(echo "$item" | xargs)"
  [[ -n "$value" ]] && trimmed_domains+=("$value")
done

if [[ "${#trimmed_domains[@]}" -eq 0 ]]; then
  echo "[metadata-agent] Error: DBT_DOMAINS is empty" >&2
  exit 1
fi

extra_args=()
if [[ -n "$DBT_RUN_ARGS" ]]; then
  # shellcheck disable=SC2206
  extra_args=($DBT_RUN_ARGS)
fi

echo "[metadata-agent] run: bash $DBT_SCRIPT ${trimmed_domains[*]} ${extra_args[*]}"
bash "$DBT_SCRIPT" "${trimmed_domains[@]}" "${extra_args[@]}"

if is_truthy "$ENABLE_OM_DBT_TRIGGER"; then
  echo "[metadata-agent] trigger OpenMetadata DBT ingestion pipeline(s)"
  python3 /workspace/metadata-agent/python/main.py
fi

if is_truthy "$ENABLE_GIT_PULL" && [[ -n "$CURRENT_REMOTE_SHA" ]]; then
  printf "%s\n" "$CURRENT_REMOTE_SHA" > "$GIT_LAST_SHA_FILE"
  echo "[metadata-agent] saved last successful sha: $CURRENT_REMOTE_SHA"
fi

echo "[metadata-agent] done"
