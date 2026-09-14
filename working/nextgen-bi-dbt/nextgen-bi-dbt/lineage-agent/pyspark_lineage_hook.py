#!/usr/bin/env python3
"""PySpark lineage hook for OpenMetadata lineage agent.

Use this module inside ETL jobs to collect edges and run lineage sync
at the end of each successful pipeline run.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


def _normalize_table_fqn(value: str) -> Dict[str, str]:
    """Parse table string in the form catalog.schema.table."""
    parts = [p.strip() for p in value.split(".") if p.strip()]
    if len(parts) != 3:
        raise ValueError(
            f"Table must be catalog.schema.table, got: {value}"
        )
    return {"database": parts[0], "schema": parts[1], "table": parts[2]}


@dataclass
class TableNode:
    service: str
    database: str
    schema: str
    table: str

    @classmethod
    def from_table_name(cls, service: str, table_name: str) -> "TableNode":
        parsed = _normalize_table_fqn(table_name)
        return cls(
            service=service,
            database=parsed["database"],
            schema=parsed["schema"],
            table=parsed["table"],
        )

    def as_config(self) -> Dict[str, str]:
        return {
            "service": self.service,
            "database": self.database,
            "schema": self.schema,
            "table": self.table,
        }


class LineageCollector:
    """Collect edges and call lineage_agent_trino_hive.py."""

    def __init__(self, host_port: str, jwt_token_env: str = "OM_JWT"):
        self.host_port = host_port
        self.jwt_token_env = jwt_token_env
        self.edges: List[Dict[str, object]] = []

    def add_edge(self, source: TableNode, target: TableNode, description: str = "") -> None:
        self.edges.append(
            {
                "from": source.as_config(),
                "to": target.as_config(),
                "description": description,
            }
        )

    def build_config(self) -> Dict[str, object]:
        return {
            "openmetadata": {
                "hostPort": self.host_port,
                "jwtTokenEnv": self.jwt_token_env,
            },
            "edges": self.edges,
        }

    def write_config(self, output_path: str) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = self.build_config()

        if path.suffix.lower() == ".json":
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            return path

        try:
            import yaml  # type: ignore
        except Exception as exc:
            raise RuntimeError("PyYAML is required for YAML output: pip install pyyaml") from exc

        path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return path

    def run_agent(self, config_path: str, agent_script_path: str = "lineage/lineage_agent_trino_hive.py") -> None:
        if not self.edges:
            print("[lineage-hook] No edges collected, skip lineage sync.")
            return

        cmd = ["python3", agent_script_path, "--config", config_path]
        subprocess.run(cmd, check=True)
        print(f"[lineage-hook] Synced {len(self.edges)} lineage edges.")
