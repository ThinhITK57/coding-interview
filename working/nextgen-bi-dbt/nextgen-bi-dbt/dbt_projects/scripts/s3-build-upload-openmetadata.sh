#!/bin/bash

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DBT_PROJECTS_ROOT="${DBT_PROJECTS_ROOT:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"

ENV_FILE="${ENV_FILE:-$DBT_PROJECTS_ROOT/.env.s3.local}"
if [[ -f "$ENV_FILE" ]]; then
  echo "[dbt-s3] Loading env file: $ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "Error: Env file not found: $ENV_FILE" >&2
  exit 1
fi

S3_DBT_PREFIX="${S3_DBT_PREFIX:-s3://metadata/dbt}"

if ! command -v dbt >/dev/null 2>&1; then
  echo "Error: dbt is not installed" >&2
  exit 1
fi

PYTHON_BIN=""
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "Error: python is not installed" >&2
  exit 1
fi

if ! "$PYTHON_BIN" -c "import boto3" >/dev/null 2>&1; then
  echo "Error: boto3 is not installed for $PYTHON_BIN. Please install boto3 before running this script." >&2
  exit 1
fi

# Ignore noisy urllib3 TLS warning for environments using self-signed/unverified HTTPS.
PYTHONWARN_FILTER="ignore:Unverified HTTPS request"
if [[ -n "${PYTHONWARNINGS:-}" ]]; then
  export PYTHONWARNINGS="${PYTHONWARNINGS},${PYTHONWARN_FILTER}"
else
  export PYTHONWARNINGS="$PYTHONWARN_FILTER"
fi

normalize_prefix() {
  local prefix="$1"
  echo "${prefix%/}"
}

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

resolve_project_dir() {
  local input="$1"

  if [[ -d "$input" && -f "$input/dbt_project.yml" ]]; then
    cd -- "$input" >/dev/null
    pwd
    return 0
  fi

  if [[ -d "$DBT_PROJECTS_ROOT/$input" && -f "$DBT_PROJECTS_ROOT/$input/dbt_project.yml" ]]; then
    cd -- "$DBT_PROJECTS_ROOT/$input" >/dev/null
    pwd
    return 0
  fi

  return 1
}

collect_projects() {
  local -a resolved=()
  local -a raw_inputs=()

  if [[ "$#" -gt 0 ]]; then
    raw_inputs=("$@")
  elif [[ -n "${DBT_PROJECTS:-}" ]]; then
    local projects_from_env="${DBT_PROJECTS//,/ }"
    # shellcheck disable=SC2206
    raw_inputs=($projects_from_env)
  else
    local dir
    for dir in "$DBT_PROJECTS_ROOT"/*; do
      [[ -d "$dir" ]] || continue
      [[ -f "$dir/dbt_project.yml" ]] || continue
      resolved+=("$dir")
    done
  fi

  if [[ "${#raw_inputs[@]}" -gt 0 ]]; then
    local input
    local project_dir
    for input in "${raw_inputs[@]}"; do
      project_dir="$(resolve_project_dir "$input")" || {
        echo "Error: Cannot resolve dbt project from input: $input" >&2
        exit 1
      }
      resolved+=("$project_dir")
    done
  fi

  if [[ "${#resolved[@]}" -eq 0 ]]; then
    echo "Error: No dbt project found. Pass project names/paths, or set DBT_PROJECTS." >&2
    exit 1
  fi

  printf "%s\n" "${resolved[@]}"
}

BASE_S3_DBT_PREFIX="$(normalize_prefix "$S3_DBT_PREFIX")"

SKIP_TESTS="${DBT_SKIP_TESTS:-false}"
PROJECT_INPUTS=()
for arg in "$@"; do
  case "$arg" in
    --skip-test|--skip-tests)
      SKIP_TESTS="true"
      ;;
    *)
      PROJECT_INPUTS+=("$arg")
      ;;
  esac
done

echo "[dbt-s3] Using DBT_PROJECTS_ROOT: $DBT_PROJECTS_ROOT"
echo "[dbt-s3] Using base S3_DBT_PREFIX: $BASE_S3_DBT_PREFIX"

mapfile -t PROJECT_DIRS < <(collect_projects "${PROJECT_INPUTS[@]}")

echo "[dbt-s3] Projects to process: ${#PROJECT_DIRS[@]}"
if is_truthy "$SKIP_TESTS"; then
  echo "[dbt-s3] dbt build tests: disabled (--skip-test or DBT_SKIP_TESTS=true)"
else
  echo "[dbt-s3] dbt build tests: enabled (default)"
fi

required_artifacts=(
  "target/manifest.json"
  "target/catalog.json"
  "target/run_results.json"
)

for DBT_PROJECT_DIR in "${PROJECT_DIRS[@]}"; do
  DBT_PROFILES_DIR="$DBT_PROJECT_DIR"
  PROJECT_KEY="$(basename -- "$DBT_PROJECT_DIR")"
  PROJECT_S3_DBT_PREFIX="$BASE_S3_DBT_PREFIX/$PROJECT_KEY"

  echo "[dbt-s3] ------------------------------"
  echo "[dbt-s3] Using DBT_PROJECT_DIR: $DBT_PROJECT_DIR"
  echo "[dbt-s3] Auto DBT_PROFILES_DIR: $DBT_PROFILES_DIR"
  echo "[dbt-s3] Upload prefix for project: $PROJECT_S3_DBT_PREFIX"

  pushd "$DBT_PROJECT_DIR" >/dev/null

  echo "[dbt-s3] Running: dbt deps"
  dbt deps --profiles-dir "$DBT_PROFILES_DIR"

  if is_truthy "$SKIP_TESTS"; then
    echo "[dbt-s3] Running: dbt build (skip tests)"
    dbt build --exclude-resource-type test unit_test --profiles-dir "$DBT_PROFILES_DIR"
  else
    echo "[dbt-s3] Running: dbt build (with tests)"
    dbt build --profiles-dir "$DBT_PROFILES_DIR"
  fi

  echo "[dbt-s3] Running: dbt docs generate"
  dbt docs generate --profiles-dir "$DBT_PROFILES_DIR"

  for artifact in "${required_artifacts[@]}"; do
    if [[ ! -f "$artifact" ]]; then
      echo "Error: Missing artifact: $artifact" >&2
      popd >/dev/null
      exit 1
    fi
  done

  echo "[dbt-s3] Reformatting target/*.json with Python"
  "$PYTHON_BIN" - "target" <<'PY'
import glob
import json
import os
import sys


def main() -> int:
  if len(sys.argv) != 2:
    print("Error: Missing target directory argument", file=sys.stderr)
    return 1

  target_dir = sys.argv[1]
  json_paths = sorted(glob.glob(os.path.join(target_dir, "*.json")))

  if not json_paths:
    print(f"Error: No JSON files found in {target_dir}", file=sys.stderr)
    return 1

  reformatted_count = 0

  for json_path in json_paths:
    with open(json_path, "r", encoding="utf-8") as json_file:
      payload = json.load(json_file)

    with open(json_path, "w", encoding="utf-8") as json_file:
      json.dump(payload, json_file, ensure_ascii=False, indent=2)
      json_file.write("\n")

    reformatted_count += 1

  print(f"[dbt-s3] Reformatted {reformatted_count} JSON files in target/")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
PY

  echo "[dbt-s3] Normalizing OpenMetadata FQN fields and syncing catalog comments"
  "$PYTHON_BIN" - "target/manifest.json" "target/catalog.json" "$PROJECT_KEY" <<'PY'
import json
import os
import sys
from typing import Dict, Optional


def find_column(columns: Dict[str, dict], column_name: str) -> Optional[dict]:
  if column_name in columns:
    return columns[column_name]

  lowered = column_name.lower()
  for key, value in columns.items():
    if key.lower() == lowered:
      return value

  return None


def to_bool(value: str) -> bool:
  return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def resolve_template(value: str, project_key: str) -> str:
  return value.replace("{project}", project_key)


def first_non_empty(*values: Optional[str]) -> Optional[str]:
  for value in values:
    if isinstance(value, str):
      candidate = value.strip()
      if candidate:
        return candidate
  return None


def normalize_item_fqn(item: dict, database: Optional[str], schema: Optional[str], lowercase: bool) -> bool:
  changed = False

  metadata = item.get("metadata")
  if not isinstance(metadata, dict):
    metadata = None

  effective_database = first_non_empty(database, item.get("database"), metadata.get("database") if metadata else None)
  effective_schema = first_non_empty(schema, item.get("schema"), metadata.get("schema") if metadata else None)

  table_name = first_non_empty(
    item.get("identifier"),
    item.get("alias"),
    item.get("name"),
    metadata.get("name") if metadata else None,
  )

  if effective_database:
    if item.get("database") != effective_database:
      item["database"] = effective_database
      changed = True

    if metadata is not None and metadata.get("database") != effective_database:
      metadata["database"] = effective_database
      changed = True

  if effective_schema:
    if item.get("schema") != effective_schema:
      item["schema"] = effective_schema
      changed = True

    if metadata is not None and metadata.get("schema") != effective_schema:
      metadata["schema"] = effective_schema
      changed = True

  if table_name:
    if item.get("name") != table_name:
      item["name"] = table_name
      changed = True

    if item.get("identifier") != table_name:
      item["identifier"] = table_name
      changed = True

    if metadata is not None and metadata.get("name") != table_name:
      metadata["name"] = table_name
      changed = True

  if effective_database and effective_schema and table_name:
    relation_name = f'"{effective_database}"."{effective_schema}"."{table_name}"'
    if item.get("relation_name") != relation_name:
      item["relation_name"] = relation_name
      changed = True

  if lowercase:
    for key in ("database", "schema", "name", "alias", "identifier", "relation_name"):
      value = item.get(key)
      if isinstance(value, str):
        lowered = value.lower()
        if value != lowered:
          item[key] = lowered
          changed = True

    if metadata is not None:
      for key in ("database", "schema", "name"):
        value = metadata.get(key)
        if isinstance(value, str):
          lowered = value.lower()
          if value != lowered:
            metadata[key] = lowered
            changed = True

  return changed


def main() -> int:
  if len(sys.argv) != 4:
    print("Error: Missing manifest/catalog/project arguments", file=sys.stderr)
    return 1

  manifest_path = sys.argv[1]
  catalog_path = sys.argv[2]
  project_key = sys.argv[3]

  fqn_database = os.getenv("OM_FQN_DATABASE", "").strip() or None
  fqn_schema = os.getenv("OM_FQN_SCHEMA", "").strip() or None
  lowercase_fqn = to_bool(os.getenv("OM_FQN_LOWERCASE", "false"))
  coerce_ephemeral_to_view = to_bool(os.getenv("OM_COERCE_EPHEMERAL_TO_VIEW", "true"))

  if fqn_database:
    fqn_database = resolve_template(fqn_database, project_key)
  if fqn_schema:
    fqn_schema = resolve_template(fqn_schema, project_key)

  with open(manifest_path, "r", encoding="utf-8") as manifest_file:
    manifest = json.load(manifest_file)

  with open(catalog_path, "r", encoding="utf-8") as catalog_file:
    catalog = json.load(catalog_file)

  normalized_count = 0
  updated_count = 0
  coerced_materialized_count = 0

  for section in ("nodes", "sources"):
    manifest_items = manifest.get(section) or {}
    catalog_items = catalog.get(section) or {}

    if not isinstance(manifest_items, dict) or not isinstance(catalog_items, dict):
      continue

    for unique_id, manifest_item in manifest_items.items():
      if not isinstance(manifest_item, dict):
        continue

      if normalize_item_fqn(manifest_item, fqn_database, fqn_schema, lowercase_fqn):
        normalized_count += 1

      # Preserve dbt runtime behavior, but normalize manifest model materialization
      # for OpenMetadata dbt ingestion compatibility.
      if coerce_ephemeral_to_view and section == "nodes" and manifest_item.get("resource_type") == "model":
        config = manifest_item.get("config")
        if isinstance(config, dict) and config.get("materialized") == "ephemeral":
          config["materialized"] = "view"
          coerced_materialized_count += 1

        unrendered_config = manifest_item.get("unrendered_config")
        if isinstance(unrendered_config, dict) and unrendered_config.get("materialized") == "ephemeral":
          unrendered_config["materialized"] = "view"

      catalog_item = catalog_items.get(unique_id)
      if not isinstance(catalog_item, dict):
        continue

      if normalize_item_fqn(catalog_item, fqn_database, fqn_schema, lowercase_fqn):
        normalized_count += 1

      manifest_columns = manifest_item.get("columns") or {}
      catalog_columns = catalog_item.get("columns") or {}

      if not isinstance(manifest_columns, dict) or not isinstance(catalog_columns, dict):
        continue

      for column_name, manifest_column in manifest_columns.items():
        if not isinstance(manifest_column, dict):
          continue

        description = manifest_column.get("description")
        if not description:
          continue

        catalog_column = find_column(catalog_columns, column_name)
        if not isinstance(catalog_column, dict):
          continue

        if catalog_column.get("comment") != description:
          catalog_column["comment"] = description
          updated_count += 1

  with open(catalog_path, "w", encoding="utf-8") as catalog_file:
    json.dump(catalog, catalog_file, ensure_ascii=False, indent=2)
    catalog_file.write("\n")

  with open(manifest_path, "w", encoding="utf-8") as manifest_file:
    json.dump(manifest, manifest_file, ensure_ascii=False, indent=2)
    manifest_file.write("\n")

  print(f"[dbt-s3] Normalized {normalized_count} node/source FQN records")
  if coerced_materialized_count:
    print(f"[dbt-s3] Coerced {coerced_materialized_count} model materializations from ephemeral to view")
  elif not coerce_ephemeral_to_view:
    print("[dbt-s3] Skipped model materialization coercion (OM_COERCE_EPHEMERAL_TO_VIEW=false)")
  print(f"[dbt-s3] Updated {updated_count} catalog column comments")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
PY

  echo "[dbt-s3] Uploading dbt artifacts to: $PROJECT_S3_DBT_PREFIX"
  "$PYTHON_BIN" - "$PROJECT_S3_DBT_PREFIX" "target/manifest.json" "target/catalog.json" "target/run_results.json" <<'PY'
import os
import sys
from urllib.parse import urlparse
import urllib3
urllib3.disable_warnings()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import boto3
from botocore.config import Config


def to_bool(value: str) -> bool:
  return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> int:
  if len(sys.argv) < 3:
    print("Error: Missing arguments for Python uploader", file=sys.stderr)
    return 1

  s3_prefix = sys.argv[1]
  artifacts = sys.argv[2:]

  parsed = urlparse(s3_prefix)
  if parsed.scheme != "s3" or not parsed.netloc:
    print(f"Error: Invalid S3_DBT_PREFIX: {s3_prefix}. Expected format: s3://bucket/prefix", file=sys.stderr)
    return 1

  bucket = parsed.netloc
  base_prefix = parsed.path.lstrip("/").rstrip("/")

  endpoint_url = os.getenv("AWS_ENDPOINT_URL") or None
  profile_name = os.getenv("AWS_PROFILE") or None
  region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or None
  force_path_style = to_bool(os.getenv("AWS_S3_FORCE_PATH_STYLE", "false"))

  session = boto3.session.Session(profile_name=profile_name, region_name=region_name)

  client_kwargs = {}
  if endpoint_url:
    client_kwargs["endpoint_url"] = endpoint_url
  if force_path_style:
    client_kwargs["config"] = Config(s3={"addressing_style": "path"})

  s3_client = session.client("s3", verify=False, **client_kwargs)

  for artifact in artifacts:
    filename = os.path.basename(artifact)
    object_key = f"{base_prefix}/{filename}" if base_prefix else filename
    print(f"[dbt-s3] Upload: {artifact} -> s3://{bucket}/{object_key}")
    s3_client.upload_file(artifact, bucket, object_key)

  return 0


if __name__ == "__main__":
  raise SystemExit(main())
PY

  popd >/dev/null
done

echo "[dbt-s3] Done."
