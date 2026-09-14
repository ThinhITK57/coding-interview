import yaml
from dataclasses import dataclass, field
import re


def normalize_schema(schema_expr: str) -> str:
    """
    "{{ env_var('DBT_TRINO_SCHEMA', 'bi_silver') }}"
    -> bi_silver
    """

    if not schema_expr:
        return ""

    m = re.search(r"'([^']+)'\s*\)\s*\}\}", schema_expr)

    if m:
        return m.group(1)

    return schema_expr


@dataclass
class TableMetadata:
    name: str
    description: str = ""
    owner: str | None = None
    tags: list[str] = field(default_factory=list)
    columns: dict[str, str] = field(default_factory=dict)


def parse_schema_yml(path) -> list[TableMetadata]:

    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    tables = []
    for model in doc.get("models", []):
        if isinstance(model, str):
            continue
        metadata = TableMetadata(
            name=model["name"],
            description=model.get("description", ""),
        )

        meta = model.get("meta", {})

        metadata.owner = meta.get("owner")

        metadata.tags.extend(
            model.get("tags", [])
        )

        for col in model.get("columns", []):

            metadata.columns[
                col["name"]
            ] = col.get("description", "")

        tables.append(metadata)

    return tables


def parse_sources_yml(path)  -> list[TableMetadata]:
    
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    tables = []

    for source in doc.get("sources", []):

        for tbl in source.get("tables", []):

            metadata = TableMetadata(
                name=tbl["name"],
                description=tbl.get("description", ""),
            )

            for col in tbl.get("columns", []):

                metadata.columns[
                    col["name"]
                ] = col.get("description", "")

            tables.append(metadata)

    return tables