#!/bin/bash

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DBT_PROJECTS_ROOT="${DBT_PROJECTS_ROOT:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"

ENV_FILE="${ENV_FILE:-$DBT_PROJECTS_ROOT/.env.openmetadata.local}"
if [[ -f "$ENV_FILE" ]]; then
  echo "[dbt-om] Loading env file: $ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "Error: Env file not found: $ENV_FILE" >&2
  exit 1
fi

if ! command -v dbt >/dev/null 2>&1; then
  echo "Error: dbt is not installed" >&2
  exit 1
fi

METADATA_BIN=""
if command -v metadata >/dev/null 2>&1; then
  METADATA_BIN="$(command -v metadata)"
elif [[ -x "$DBT_PROJECTS_ROOT/.venv/bin/metadata" ]]; then
  METADATA_BIN="$DBT_PROJECTS_ROOT/.venv/bin/metadata"
elif [[ -x "$DBT_PROJECTS_ROOT/../.venv/bin/metadata" ]]; then
  METADATA_BIN="$DBT_PROJECTS_ROOT/../.venv/bin/metadata"
else
  echo "Error: metadata CLI is not installed or not found in PATH/.venv." >&2
  echo "Install with: pip install \"openmetadata-ingestion[dbt]\"" >&2
  exit 1
fi

PYTHON_BIN=""
if [[ -x "/media/namtv/data/projects/vc/vcs-lightdash/.venv/bin/python" ]]; then
  PYTHON_BIN="/media/namtv/data/projects/vc/vcs-lightdash/.venv/bin/python"
elif [[ -x "$DBT_PROJECTS_ROOT/../.venv/bin/python" ]]; then
  PYTHON_BIN="$DBT_PROJECTS_ROOT/../.venv/bin/python"
elif [[ -x "$DBT_PROJECTS_ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$DBT_PROJECTS_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "Error: python is not installed" >&2
  exit 1
fi

# Ignore noisy urllib3 TLS warning for environments using self-signed/unverified HTTPS.
PYTHONWARN_FILTER="ignore:Unverified HTTPS request"
if [[ -n "${PYTHONWARNINGS:-}" ]]; then
  export PYTHONWARNINGS="${PYTHONWARNINGS},${PYTHONWARN_FILTER}"
else
  export PYTHONWARNINGS="$PYTHONWARN_FILTER"
fi

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

resolve_om_jwt() {
  if [[ -n "${OM_JWT:-}" ]]; then
    return 0
  fi

  echo "Error: OM_JWT is required. This script no longer logs in via OM_USER/OM_PASS." >&2
  echo "Set OM_JWT in your env file or export it before running." >&2
  exit 1
}

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

resolve_om_jwt

OM_HOST="${OM_HOST:-}"
if [[ -z "$OM_HOST" ]]; then
  echo "Error: OM_HOST is required" >&2
  exit 1
fi

OM_API_HOSTPORT="${OM_API_HOSTPORT:-${OM_HOST%/}/api}"
OM_AUTH_PROVIDER="${OM_AUTH_PROVIDER:-openmetadata}"
OM_LOG_LEVEL="${OM_LOG_LEVEL:-INFO}"
OM_STORE_SERVICE_CONNECTION="${OM_STORE_SERVICE_CONNECTION:-false}"

echo "[dbt-om] Using DBT_PROJECTS_ROOT: $DBT_PROJECTS_ROOT"
echo "[dbt-om] OpenMetadata hostPort: $OM_API_HOSTPORT"

mapfile -t PROJECT_DIRS < <(collect_projects "${PROJECT_INPUTS[@]}")

echo "[dbt-om] Projects to process: ${#PROJECT_DIRS[@]}"
if is_truthy "$SKIP_TESTS"; then
  echo "[dbt-om] dbt build tests: disabled (--skip-test or DBT_SKIP_TESTS=true)"
else
  echo "[dbt-om] dbt build tests: enabled (default)"
fi

required_artifacts=(
  "target/manifest.json"
  "target/catalog.json"
  "target/run_results.json"
)

for DBT_PROJECT_DIR in "${PROJECT_DIRS[@]}"; do
  DBT_PROFILES_DIR="$DBT_PROJECT_DIR"
  PROJECT_KEY="$(basename -- "$DBT_PROJECT_DIR")"

  echo "[dbt-om] ------------------------------"
  echo "[dbt-om] Using DBT_PROJECT_DIR: $DBT_PROJECT_DIR"
  echo "[dbt-om] Auto DBT_PROFILES_DIR: $DBT_PROFILES_DIR"

  pushd "$DBT_PROJECT_DIR" >/dev/null

  echo "[dbt-om] Running: dbt deps"
  dbt deps --profiles-dir "$DBT_PROFILES_DIR"

  if is_truthy "$SKIP_TESTS"; then
    echo "[dbt-om] Running: dbt build (skip tests)"
    dbt build --exclude-resource-type test unit_test --profiles-dir "$DBT_PROFILES_DIR"
  else
    echo "[dbt-om] Running: dbt build (with tests)"
    dbt build --profiles-dir "$DBT_PROFILES_DIR"
  fi

  echo "[dbt-om] Running: dbt docs generate"
  dbt docs generate --profiles-dir "$DBT_PROFILES_DIR"

  for artifact in "${required_artifacts[@]}"; do
    if [[ ! -f "$artifact" ]]; then
      echo "Error: Missing artifact: $artifact" >&2
      popd >/dev/null
      exit 1
    fi
  done

  echo "[dbt-om] Reformatting target/*.json with Python"
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

  for json_path in json_paths:
    with open(json_path, "r", encoding="utf-8") as json_file:
      payload = json.load(json_file)

    with open(json_path, "w", encoding="utf-8") as json_file:
      json.dump(payload, json_file, ensure_ascii=False, indent=2)
      json_file.write("\n")

  print(f"[dbt-om] Reformatted {len(json_paths)} JSON files in target/")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
PY

  echo "[dbt-om] Normalizing OpenMetadata FQN fields and syncing catalog comments"
  "$PYTHON_BIN" - "target/manifest.json" "target/catalog.json" "$PROJECT_KEY" <<'PY'
import json
import os
import sys
import ssl
import urllib.error
import urllib.parse
import urllib.request
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


def parse_manifest_service(manifest: dict) -> Optional[str]:
  for section in ("nodes", "sources"):
    items = manifest.get(section)
    if not isinstance(items, dict):
      continue

    for value in items.values():
      if not isinstance(value, dict):
        continue

      meta = value.get("meta")
      if not isinstance(meta, dict):
        continue

      openmeta = meta.get("openmetadata")
      if not isinstance(openmeta, dict):
        continue

      service = openmeta.get("service")
      if isinstance(service, str) and service.strip():
        return service.strip()

  return None


def build_item_fqn(item: dict, service: str, database: Optional[str], schema: Optional[str], lowercase: bool) -> Optional[str]:
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

  if not service or not effective_database or not effective_schema or not table_name:
    return None

  fqn = f"{service}.{effective_database}.{effective_schema}.{table_name}"
  if lowercase:
    return fqn.lower()
  return fqn


def create_ssl_context(verify_ssl: str):
  raw = (verify_ssl or "").strip().lower()
  if raw in {"ignore", "false", "0", "no", "off"}:
    return ssl._create_unverified_context()
  return ssl.create_default_context()


def table_exists(api_hostport: str, jwt_token: str, table_fqn: str, ssl_context, cache: Dict[str, bool]) -> bool:
  cached = cache.get(table_fqn)
  if cached is not None:
    return cached

  encoded_fqn = urllib.parse.quote(table_fqn, safe="")
  endpoint = f"{api_hostport.rstrip('/')}/v1/tables/name/{encoded_fqn}"
  request = urllib.request.Request(endpoint, headers={"Authorization": f"Bearer {jwt_token}", "Accept": "application/json"})

  try:
    with urllib.request.urlopen(request, context=ssl_context, timeout=15) as response:
      exists = response.status == 200
  except urllib.error.HTTPError as exc:
    if exc.code == 404:
      exists = False
    else:
      print(f"[dbt-om] Warning: cannot verify table FQN {table_fqn}: HTTP {exc.code}", file=sys.stderr)
      exists = True
  except Exception as exc:
    print(f"[dbt-om] Warning: cannot verify table FQN {table_fqn}: {exc}", file=sys.stderr)
    exists = True

  cache[table_fqn] = exists
  return exists


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

  if fqn_database:
    fqn_database = resolve_template(fqn_database, project_key)
  if fqn_schema:
    fqn_schema = resolve_template(fqn_schema, project_key)

  with open(manifest_path, "r", encoding="utf-8") as manifest_file:
    manifest = json.load(manifest_file)

  with open(catalog_path, "r", encoding="utf-8") as catalog_file:
    catalog = json.load(catalog_file)

  om_api_hostport = (os.getenv("OM_API_HOSTPORT", "") or "").strip()
  om_jwt = (os.getenv("OM_JWT", "") or "").strip()
  om_verify_ssl = (os.getenv("OM_VERIFY_SSL", "") or "").strip()
  om_filter_existing = to_bool(os.getenv("OM_FILTER_EXISTING_TABLES", "true"))

  service_name = first_non_empty(os.getenv("OM_SERVICE_NAME", ""), parse_manifest_service(manifest))
  if isinstance(service_name, str):
    service_name = service_name.strip()

  ssl_context = None
  if om_filter_existing and om_api_hostport and om_jwt and service_name:
    ssl_context = create_ssl_context(om_verify_ssl)

  normalized_count = 0
  updated_count = 0
  coerced_materialized_count = 0
  dropped_missing_catalog_count = 0
  dropped_missing_table_count = 0
  exists_cache: Dict[str, bool] = {}

  for section in ("nodes", "sources"):
    manifest_items = manifest.get(section) or {}
    catalog_items = catalog.get(section) or {}

    if not isinstance(manifest_items, dict):
      continue

    if not isinstance(catalog_items, dict):
      catalog_items = {}

    kept_manifest_items = {}
    kept_catalog_items = {}

    for unique_id, manifest_item in manifest_items.items():
      if not isinstance(manifest_item, dict):
        continue

      if normalize_item_fqn(manifest_item, fqn_database, fqn_schema, lowercase_fqn):
        normalized_count += 1

      catalog_item = catalog_items.get(unique_id)
      if not isinstance(catalog_item, dict):
        dropped_missing_catalog_count += 1
        continue

      if normalize_item_fqn(catalog_item, fqn_database, fqn_schema, lowercase_fqn):
        normalized_count += 1

      # Preserve runtime behavior (e.g. DBT_MATERIALIZED=ephemeral) but coerce
      # manifest materialization in artifacts so OM dbt ingestion maps models
      # to existing physical entities and can apply metadata updates.
      if section == "nodes":
        resource_type = manifest_item.get("resource_type")
        if resource_type == "model":
          catalog_meta = catalog_item.get("metadata")
          catalog_type = ""
          if isinstance(catalog_meta, dict):
            raw_type = catalog_meta.get("type")
            if isinstance(raw_type, str):
              catalog_type = raw_type.strip().upper()

          forced_materialized: Optional[str] = None
          if "VIEW" in catalog_type:
            forced_materialized = "view"
          elif "TABLE" in catalog_type:
            forced_materialized = "table"
          elif isinstance(manifest_item.get("config"), dict) and manifest_item["config"].get("materialized") == "ephemeral":
            forced_materialized = "table"

          if forced_materialized:
            config = manifest_item.get("config")
            if isinstance(config, dict) and config.get("materialized") != forced_materialized:
              config["materialized"] = forced_materialized
              coerced_materialized_count += 1

            unrendered_config = manifest_item.get("unrendered_config")
            if isinstance(unrendered_config, dict) and unrendered_config.get("materialized") != forced_materialized:
              unrendered_config["materialized"] = forced_materialized

      manifest_columns = manifest_item.get("columns") or {}
      catalog_columns = catalog_item.get("columns") or {}

      if not isinstance(manifest_columns, dict) or not isinstance(catalog_columns, dict) or not catalog_columns:
        dropped_missing_catalog_count += 1
        continue

      if ssl_context is not None and service_name:
        item_fqn = build_item_fqn(manifest_item, service_name, fqn_database, fqn_schema, lowercase_fqn)
        if item_fqn and not table_exists(om_api_hostport, om_jwt, item_fqn, ssl_context, exists_cache):
          dropped_missing_table_count += 1
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

      kept_manifest_items[unique_id] = manifest_item
      kept_catalog_items[unique_id] = catalog_item

    manifest[section] = kept_manifest_items
    catalog[section] = kept_catalog_items

  with open(catalog_path, "w", encoding="utf-8") as catalog_file:
    json.dump(catalog, catalog_file, ensure_ascii=False, indent=2)
    catalog_file.write("\n")

  with open(manifest_path, "w", encoding="utf-8") as manifest_file:
    json.dump(manifest, manifest_file, ensure_ascii=False, indent=2)
    manifest_file.write("\n")

  print(f"[dbt-om] Normalized {normalized_count} node/source FQN records")
  if coerced_materialized_count:
    print(f"[dbt-om] Coerced {coerced_materialized_count} model materializations for ingestion")
  print(f"[dbt-om] Updated {updated_count} catalog column comments")
  if dropped_missing_catalog_count:
    print(f"[dbt-om] Dropped {dropped_missing_catalog_count} nodes/sources missing catalog columns")
  if dropped_missing_table_count:
    print(f"[dbt-om] Dropped {dropped_missing_table_count} nodes/sources not found in OpenMetadata")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
PY

  OM_SERVICE_NAME="$($PYTHON_BIN - "dbt_project.yml" "target/manifest.json" <<'PY'
import json
import sys


def parse_yaml_value(path: str):
  try:
    import yaml  # type: ignore
  except Exception:
    return None

  try:
    with open(path, "r", encoding="utf-8") as f:
      data = yaml.safe_load(f) or {}
  except Exception:
    return None

  if not isinstance(data, dict):
    return None

  project_name = data.get("name")
  models = data.get("models")
  if not isinstance(models, dict):
    return None

  candidates = []
  if isinstance(project_name, str):
    candidates.append(project_name)
  candidates.extend([key for key in models.keys() if isinstance(key, str)])

  for key in candidates:
    model_cfg = models.get(key)
    if not isinstance(model_cfg, dict):
      continue

    meta = model_cfg.get("+meta")
    if not isinstance(meta, dict):
      continue

    openmeta = meta.get("openmetadata")
    if not isinstance(openmeta, dict):
      continue

    service = openmeta.get("service")
    if isinstance(service, str) and service.strip():
      return service.strip()

  return None


def parse_manifest_value(path: str):
  try:
    with open(path, "r", encoding="utf-8") as f:
      manifest = json.load(f)
  except Exception:
    return None

  for section in ("nodes", "sources"):
    items = manifest.get(section)
    if not isinstance(items, dict):
      continue

    for value in items.values():
      if not isinstance(value, dict):
        continue

      meta = value.get("meta")
      if not isinstance(meta, dict):
        continue

      openmeta = meta.get("openmetadata")
      if not isinstance(openmeta, dict):
        continue

      service = openmeta.get("service")
      if isinstance(service, str) and service.strip():
        return service.strip()

  return None


def main() -> int:
  if len(sys.argv) != 3:
    print("", end="")
    return 0

  yaml_path = sys.argv[1]
  manifest_path = sys.argv[2]

  service = parse_yaml_value(yaml_path) or parse_manifest_value(manifest_path)
  if service:
    print(service)
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
PY
)"

  if [[ -z "$OM_SERVICE_NAME" ]]; then
    echo "Error: Cannot resolve openmetadata.service from dbt_project.yml or manifest meta" >&2
    popd >/dev/null
    exit 1
  fi

  echo "[dbt-om] OpenMetadata service for $PROJECT_KEY: $OM_SERVICE_NAME"

  echo "[dbt-om] Analyzing artifacts and updating OpenMetadata directly"
  emit_ingest_config() {
    echo "source:"
    echo "  type: dbt"
    echo "  serviceName: $OM_SERVICE_NAME"
    echo "  sourceConfig:"
    echo "    config:"
    echo "      type: DBT"
    echo "      dbtConfigSource:"
    echo "        dbtConfigType: local"
    echo "        dbtManifestFilePath: $DBT_PROJECT_DIR/target/manifest.json"
    echo "        dbtCatalogFilePath: $DBT_PROJECT_DIR/target/catalog.json"
    echo "        dbtRunResultsFilePath: $DBT_PROJECT_DIR/target/run_results.json"

    [[ -n "${OM_DBT_UPDATE_DESCRIPTIONS:-}" ]] && echo "      dbtUpdateDescriptions: ${OM_DBT_UPDATE_DESCRIPTIONS}"
    [[ -n "${OM_DBT_UPDATE_OWNERS:-}" ]] && echo "      dbtUpdateOwners: ${OM_DBT_UPDATE_OWNERS}"
    [[ -n "${OM_DBT_INCLUDE_TAGS:-}" ]] && echo "      includeTags: ${OM_DBT_INCLUDE_TAGS}"
    [[ -n "${OM_DBT_CLASSIFICATION_NAME:-}" ]] && echo "      dbtClassificationName: ${OM_DBT_CLASSIFICATION_NAME}"

    echo "sink:"
    echo "  type: metadata-rest"
    echo "  config: {}"
    echo "workflowConfig:"
    echo "  loggerLevel: $OM_LOG_LEVEL"
    echo "  openMetadataServerConfig:"
    echo "    hostPort: $OM_API_HOSTPORT"
    echo "    authProvider: $OM_AUTH_PROVIDER"
    echo "    securityConfig:"
    echo "      jwtToken: $OM_JWT"
    echo "    storeServiceConnection: $OM_STORE_SERVICE_CONNECTION"

    [[ -n "${OM_VERIFY_SSL:-}" ]] && echo "    verifySSL: ${OM_VERIFY_SSL}"
    if [[ -n "${OM_SSL_CA_CERT_PATH:-}" ]]; then
      echo "    sslConfig:"
      echo "      caCertificate: ${OM_SSL_CA_CERT_PATH}"
    fi

    [[ -n "${OM_INGESTION_PIPELINE_FQN:-}" ]] && echo "  ingestionPipelineFQN: ${OM_INGESTION_PIPELINE_FQN}"

    return 0
  }

  CFG_FILE="$(mktemp -t dbt-om-config-XXXXXX.yaml)"
  trap 'rm -f "$CFG_FILE"' EXIT

  emit_ingest_config > "$CFG_FILE"
  "$METADATA_BIN" ingest -c "$CFG_FILE"

  rm -f "$CFG_FILE"
  trap - EXIT

  unset -f emit_ingest_config

  popd >/dev/null
done

echo "[dbt-om] Done."
