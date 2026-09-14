import yaml
from .om_client import OpenMetadataSDK
from pathlib import Path
from .schema import *


def build_column_patches(
    om_columns: list,
    metadata_columns: dict[str, str],
    patches: list,
    prefix_path="/columns"
):
    for idx, col in enumerate(om_columns):

        col_name = col["name"]

        if col_name in metadata_columns:

            patches.append({
                "op": "add",
                "path": f"{prefix_path}/{idx}/description",
                "value": metadata_columns[col_name]
            })

        children = col.get("children") or []

        if children:
            build_column_patches(
                children,
                metadata_columns,
                patches,
                f"{prefix_path}/{idx}/children"
            )
            
            
def sync_table_metadata(
    om: OpenMetadataSDK,
    fqn: str,
    description: str,
    columns: dict[str, str]
):
    table = om.tables.get_by_fqn(fqn)

    if not table:
        print(f"Table not found: {fqn}")
        return

    patches = []

    # table description
    if description:
        patches.append({
            "op": "add",
            "path": "/description",
            "value": description
        })

    # column descriptions
    build_column_patches(
        table.get("columns", []),
        columns,
        patches
    )

    if not patches:
        print(f"No changes: {fqn}")
        return

    om.tables.update_by_name(
        fqn,
        patches
    )

    print(f"Updated: {fqn}")




def sync_dbt_metadata(
    folder,
    service,
    database,
    schema,
):
    om = OpenMetadataSDK()
    for file in Path(folder).rglob("*.yml"):
        print(f"Processing file {file}")
        doc = yaml.safe_load(
            file.read_text(
                encoding="utf-8"
            )
        )
        if not doc:
            continue

        if "models" in doc:
            tables = parse_schema_yml(file)
        elif "sources" in doc:
            tables = parse_sources_yml(file)
        else:
            continue

        for table in tables:
            print(f"Processing table {table.name}")
            fqn = (
                f"{service}."
                f"{database}."
                f"{schema}."
                f"{table.name}"
            )
            try:
                sync_table_metadata(om, fqn, table.description, table.columns)
            except Exception as e:
                print(e)
                print(f"Failed file {file}")
                continue
        print(f"Done file {file}")
     