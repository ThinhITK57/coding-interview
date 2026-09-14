#!/usr/bin/env python3
"""Upsert OpenMetadata lineage edges for Trino/Hive ETL flows.

This script reads a YAML/JSON config file and creates lineage edges such as:
raw -> silver -> gold

Usage:
  python lineage/lineage_agent_trino_hive.py \
    --config lineage/lineage_agent_config.example.yaml
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict


def load_env_file(env_path: Path) -> int:
    if not env_path.exists():
        return 0

    loaded = 0
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue

        if key not in os.environ:
            os.environ[key] = value
            loaded += 1

    return loaded


def load_default_env_candidates(explicit_env_file: str | None) -> None:
    if explicit_env_file:
        env_path = Path(explicit_env_file)
        loaded = load_env_file(env_path)
        print(f"[lineage-agent] Loaded {loaded} vars from {env_path}")
        return

    repo_root = Path(__file__).resolve().parents[1]
    candidates = [
        repo_root / "lineage" / ".env.lineage",
        repo_root / ".env",
    ]

    for env_path in candidates:
        if env_path.exists():
            loaded = load_env_file(env_path)
            print(f"[lineage-agent] Loaded {loaded} vars from {env_path}")
            break


def load_config(config_path: Path) -> Dict[str, Any]:
    raw = config_path.read_text(encoding="utf-8")

    if config_path.suffix.lower() == ".json":
        return json.loads(raw)

    try:
        import yaml  # type: ignore
    except Exception as exc:  # pragma: no cover - best effort import
        raise RuntimeError(
            "YAML config requires PyYAML. Install with: pip install pyyaml"
        ) from exc

    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise RuntimeError("Config root must be a JSON/YAML object")
    return data


def make_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}{path}"


def http_json(
    method: str,
    url: str,
    jwt_token: str,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    data = None
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
    }

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, data=data, method=method, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {method} {url}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error {method} {url}: {exc}") from exc


def build_fqn(service: str, item: Dict[str, Any]) -> str:
    if "fqn" in item and str(item["fqn"]).strip():
        return str(item["fqn"]).strip().lower()

    database = str(item.get("database", "")).strip()
    schema = str(item.get("schema", "")).strip()
    table = str(item.get("table", "")).strip()

    if not database or not schema or not table:
        raise RuntimeError(
            "Each endpoint must define either 'fqn' or all of 'database', 'schema', 'table'"
        )

    return f"{service}.{database}.{schema}.{table}".lower()


def resolve_table_ref(host_port: str, jwt_token: str, fqn: str) -> Dict[str, str]:
    encoded_fqn = urllib.parse.quote(fqn, safe="")
    url = make_url(host_port, f"/v1/tables/name/{encoded_fqn}")
    payload = http_json("GET", url, jwt_token)

    entity_id = str(payload.get("id", "")).strip()
    if not entity_id:
        raise RuntimeError(f"Table not found or missing id for FQN: {fqn}")

    return {
        "id": entity_id,
        "type": "table",
    }


def upsert_lineage_edge(
    host_port: str,
    jwt_token: str,
    from_ref: Dict[str, str],
    to_ref: Dict[str, str],
    description: str | None,
) -> Dict[str, Any]:
    edge: Dict[str, Any] = {
        "fromEntity": from_ref,
        "toEntity": to_ref,
    }
    if description:
        edge["description"] = description

    payload = {"edge": edge}
    url = make_url(host_port, "/v1/lineage")
    return http_json("PUT", url, jwt_token, payload)


def get_token(config: Dict[str, Any]) -> str:
    om_cfg = config.get("openmetadata", {})
    if not isinstance(om_cfg, dict):
        raise RuntimeError("Config.openmetadata must be an object")

    explicit = str(om_cfg.get("jwtToken", "")).strip()
    if explicit:
        return explicit

    token_env = str(om_cfg.get("jwtTokenEnv", "OM_JWT")).strip() or "OM_JWT"
    env_val = os.getenv(token_env, "").strip()
    if env_val:
        return env_val

    raise RuntimeError(
        f"Missing JWT token. Set openmetadata.jwtToken or environment variable {token_env}"
    )


def get_host_port(config: Dict[str, Any]) -> str:
    om_cfg = config.get("openmetadata", {})
    if not isinstance(om_cfg, dict):
        raise RuntimeError("Config.openmetadata must be an object")

    host_port = str(om_cfg.get("hostPort", "")).strip()
    if host_port:
        return host_port

    env_host_port = os.getenv("OM_API_HOSTPORT", "").strip()
    if env_host_port:
        return env_host_port

    env_host = os.getenv("OM_HOST", "").strip()
    if env_host:
        return f"{env_host.rstrip('/')}/api"

    raise RuntimeError(
        "Missing hostPort. Set config.openmetadata.hostPort or environment OM_API_HOSTPORT/OM_HOST"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create/update OpenMetadata lineage edges for Trino/Hive ETL"
    )
    parser.add_argument("--config", required=True, help="Path to YAML/JSON config file")
    parser.add_argument(
        "--env-file",
        default="",
        help="Optional env file path (.env style). If omitted, auto-load lineage/.env.lineage or .env",
    )
    args = parser.parse_args()

    load_default_env_candidates(args.env_file or None)

    config_path = Path(args.config)
    if not config_path.exists():
        raise RuntimeError(f"Config file not found: {config_path}")

    config = load_config(config_path)

    om_cfg = config.get("openmetadata", {})
    if not isinstance(om_cfg, dict):
        raise RuntimeError("Config.openmetadata must be an object")

    host_port = get_host_port(config)

    default_service = str(config.get("service", "")).strip()

    jwt_token = get_token(config)

    edges = config.get("edges", [])
    if not isinstance(edges, list) or not edges:
        raise RuntimeError("Config.edges must be a non-empty list")

    success_count = 0

    for idx, edge in enumerate(edges, start=1):
        if not isinstance(edge, dict):
            raise RuntimeError(f"edges[{idx}] must be an object")

        from_cfg = edge.get("from", {})
        to_cfg = edge.get("to", {})
        if not isinstance(from_cfg, dict) or not isinstance(to_cfg, dict):
            raise RuntimeError(f"edges[{idx}] requires 'from' and 'to' objects")

        from_service = str(from_cfg.get("service", default_service)).strip()
        to_service = str(to_cfg.get("service", default_service)).strip()
        if not from_service or not to_service:
            raise RuntimeError(
                f"edges[{idx}] missing service. Set root 'service' as default or specify from.service/to.service"
            )

        from_fqn = build_fqn(from_service, from_cfg)
        to_fqn = build_fqn(to_service, to_cfg)

        from_ref = resolve_table_ref(host_port, jwt_token, from_fqn)
        to_ref = resolve_table_ref(host_port, jwt_token, to_fqn)

        description = str(edge.get("description", "")).strip() or None
        upsert_lineage_edge(host_port, jwt_token, from_ref, to_ref, description)

        success_count += 1
        print(f"[{idx}/{len(edges)}] Upserted lineage: {from_fqn} -> {to_fqn}")

    print(f"Done. Upserted {success_count} lineage edges.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
