import re

def split_fields(s: str) -> list[str]:
    """
    Split top-level fields by ',' ignoring nested <> and ().
    """
    fields = []
    level = 0
    start = 0

    for i, c in enumerate(s):
        if c in "<(":
            level += 1

        elif c in ">)":
            level -= 1

        elif c == "," and level == 0:
            fields.append(s[start:i].strip())
            start = i + 1

    fields.append(s[start:].strip())

    return fields


def ddl_type_to_json_type(hive_type: str):
    hive_type = hive_type.strip().lower()

    if hive_type.startswith("varchar") or \
       hive_type.startswith("string"):
        return {"type": "string"}

    if hive_type.startswith("bigint") or \
       hive_type.startswith("int"):
        return {"type": "long"}

    if hive_type.startswith("boolean"):
        return {"type": "boolean"}

    if hive_type.startswith("double") or \
       hive_type.startswith("float"):
        return {"type": "double"}

    if hive_type.startswith("array<"):
        inner = hive_type[6:-1]

        return {
            "type": "array",
            "elementType": ddl_type_to_json_type(inner),
        }

    if hive_type.startswith("row(") or \
       hive_type.startswith("row<"):

        inner = (
            hive_type[hive_type.find("(") + 1:-1]
            if "(" in hive_type
            else hive_type[4:-1]
        )

        fields = []

        for part in split_fields(inner):
            k, v = part.split(":", 1)

            fields.append({
                "name": k.strip(),
                "nullable": True,
                "dataType": ddl_type_to_json_type(v.strip()),
            })

        return {
            "type": "struct",
            "fields": fields,
        }

    return {"type": "string"}


def ddl_to_json_schema(ddl: str):
    match = re.search(r"\((.*)\)", ddl, re.S)

    if not match:
        raise ValueError("Không parse được DDL columns")

    cols_str = match.group(1)

    fields = []

    for col_def in split_fields(cols_str):

        if not col_def:
            continue

        if col_def.lower().startswith("primary"):
            continue

        if col_def.lower().startswith("constraint"):
            continue

        parts = col_def.strip().split(None, 1)

        if len(parts) < 2:
            continue

        name, typ = parts

        fields.append({
            "name": name.strip(),
            "nullable": True,
            "dataType": ddl_type_to_json_type(
                typ.strip()
            ),
        })

    return {
        "type": "struct",
        "fields": fields,
    }


def extract_s3_location(ddl: str) -> str:
    match = re.search(
        r"(external_location|location)\s*=\s*'([^']+)'",
        ddl,
        re.I,
    )

    if not match:
        raise ValueError(
            "❌ Không tìm thấy external_location trong DDL"
        )

    return match.group(2)


def normalize_ddl(
    ddl: str,
    tgt_schema: str,
    table: str,
) -> str:

    ddl = re.sub(
        r"CREATE TABLE\s+\S+",
        f"CREATE TABLE {tgt_schema}.{table}",
        ddl,
        flags=re.IGNORECASE,
    )

    ddl = re.sub(
        r"\s*external_location\s*=\s*'[^']*'\s*,?",
        "",
        ddl,
        flags=re.IGNORECASE,
    )

    ddl = re.sub(r",\s*\)", ")", ddl)
    ddl = re.sub(r"\(\s*,", "(", ddl)

    return ddl