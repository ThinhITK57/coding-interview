#!/bin/bash

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DBT_PROJECTS_ROOT="${DBT_PROJECTS_ROOT:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"

ENV_FILE="${ENV_FILE:-$DBT_PROJECTS_ROOT/.env.openmetadata.local}"
export ENV_FILE

"$SCRIPT_DIR/dbt-build-upload-openmetadata.sh" "$@"
