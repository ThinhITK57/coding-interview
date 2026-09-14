#!/usr/bin/env bash

set -euo pipefail

CRON_SCHEDULE="${CRON_SCHEDULE:-*/30 * * * *}"
RUN_ON_STARTUP="${RUN_ON_STARTUP:-true}"
LOG_FILE="${LOG_FILE:-/var/log/metadata-agent/cron.log}"
RUNTIME_ENV_FILE="/workspace/metadata-agent/.runtime.env"

mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"

cat >"$RUNTIME_ENV_FILE" <<EOF
export DBT_SCRIPT=$(printf '%q' "${DBT_SCRIPT:-/workspace/dbt_projects/scripts/s3-build-upload-openmetadata-local.sh}")
export DBT_DOMAINS=$(printf '%q' "${DBT_DOMAINS:-crm}")
export DBT_RUN_ARGS=$(printf '%q' "${DBT_RUN_ARGS:-}")
export DBT_PROJECTS_ROOT=$(printf '%q' "${DBT_PROJECTS_ROOT:-/workspace/dbt_projects}")
export ENV_FILE=$(printf '%q' "${ENV_FILE:-/workspace/dbt_projects/.env.s3.local}")
export ENABLE_GIT_PULL=$(printf '%q' "${ENABLE_GIT_PULL:-false}")
export REPO_DIR=$(printf '%q' "${REPO_DIR:-/workspace/repo}")
export GIT_BRANCH=$(printf '%q' "${GIT_BRANCH:-master}")
export GIT_LAST_SHA_FILE=$(printf '%q' "${GIT_LAST_SHA_FILE:-/var/lib/metadata-agent/last_${GIT_BRANCH:-master}.sha}")
export ENABLE_OM_DBT_TRIGGER=$(printf '%q' "${ENABLE_OM_DBT_TRIGGER:-false}")
export OM_HOST=$(printf '%q' "${OM_HOST:-}")
export OM_API_HOSTPORT=$(printf '%q' "${OM_API_HOSTPORT:-}")
export OM_JWT=$(printf '%q' "${OM_JWT:-}")
export OM_VERIFY_SSL=$(printf '%q' "${OM_VERIFY_SSL:-true}")
export OM_DATABASE_SERVICE_NAME=$(printf '%q' "${OM_DATABASE_SERVICE_NAME:-}")
export OM_DBT_TRIGGER_MODE=$(printf '%q' "${OM_DBT_TRIGGER_MODE:-all}")
export OM_DBT_PIPELINE_NAME=$(printf '%q' "${OM_DBT_PIPELINE_NAME:-}")
export OM_REQUEST_TIMEOUT=$(printf '%q' "${OM_REQUEST_TIMEOUT:-30}")
export OM_RETRY_MAX_ATTEMPTS=$(printf '%q' "${OM_RETRY_MAX_ATTEMPTS:-4}")
export OM_RETRY_INITIAL_DELAY_SECONDS=$(printf '%q' "${OM_RETRY_INITIAL_DELAY_SECONDS:-1}")
export OM_RETRY_MAX_DELAY_SECONDS=$(printf '%q' "${OM_RETRY_MAX_DELAY_SECONDS:-8}")
EOF

cat >/etc/cron.d/metadata-agent <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
${CRON_SCHEDULE} root bash -lc 'source /workspace/metadata-agent/.runtime.env && /workspace/metadata-agent/scripts/run-once.sh' >> ${LOG_FILE} 2>&1
EOF

chmod 0644 /etc/cron.d/metadata-agent

if [[ "${RUN_ON_STARTUP,,}" =~ ^(1|true|yes|y|on)$ ]]; then
  echo "[metadata-agent] run once on startup"
  /workspace/metadata-agent/scripts/run-once.sh || true
fi

echo "[metadata-agent] cron schedule: ${CRON_SCHEDULE}"
echo "[metadata-agent] log file: ${LOG_FILE}"

exec cron -f
