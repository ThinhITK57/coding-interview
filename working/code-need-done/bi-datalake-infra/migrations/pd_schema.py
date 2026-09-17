from datetime import datetime, timezone, date, timedelta
import pytz
import re
import pyarrow as pa
import pyarrow.parquet as pq
import decimal
import traceback

# -------------------- helper parse nested --------------------
def split_fields(s:str):
    """Split top-level fields by ',' ignoring nested <>"""
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

# -------------------- DDL type -> JSON type --------------------
def ddl_type_to_json_type(hive_type: str):
    hive_type = hive_type.strip().lower()
    if hive_type.startswith("varchar") or hive_type.startswith("string"):
        return {"type": "string"}
    if hive_type.startswith("bigint") or hive_type.startswith("int"):
        return {"type": "long"}
    if hive_type.startswith("boolean"):
        return {"type": "boolean"}
    if hive_type.startswith("double") or hive_type.startswith("float"):
        return {"type": "double"}
    if hive_type.startswith("array<"):
        inner = hive_type[6:-1]
        return {"type": "array", "elementType": ddl_type_to_json_type(inner)}
    if hive_type.startswith("row(") or hive_type.startswith("row<"):
        inner = hive_type[hive_type.find("(")+1:-1] if "(" in hive_type else hive_type[4:-1]
        fields = []
        for part in split_fields(inner):
            k, v = part.split(":", 1)
            fields.append({
                "name": k.strip(),
                "nullable": True,
                "dataType": ddl_type_to_json_type(v.strip())
            })
        return {"type": "struct", "fields": fields}
    return {"type": "string"}


# -------------------- DDL type -> PyArrow type --------------------
def extract_inner(t: str) -> str:
    t = t.strip()

    # tìm vị trí mở đầu
    start = None
    open_char = None
    close_char = None

    for i, c in enumerate(t):
        if c in ("(", "<"):
            start = i
            open_char = c
            close_char = ")" if c == "(" else ">"
            break

    if start is None:
        return ""

    level = 0
    for i in range(start, len(t)):
        c = t[i]

        if c == open_char:
            level += 1
        elif c == close_char:
            level -= 1

            if level == 0:
                return t[start + 1:i]

    raise ValueError(f"Unmatched bracket in: {t}")


def split_top_level(s: str, delimiter: str):
    parts = []
    level = 0
    current = []

    for c in s:
        if c in "<(":
            level += 1
        elif c in ">)":
            level -= 1

        if c == delimiter and level == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(c)

    if current:
        parts.append("".join(current))

    return parts


def ddl_type_to_arrow_type(hive_type: str):
    t = hive_type.strip().lower()

    # -------------------- STRING --------------------
    if t.startswith(("varchar", "char", "string")):
        return pa.string()

    # -------------------- INTEGER --------------------
    if t.startswith("tinyint"):
        return pa.int8()
    if t.startswith("smallint"):
        return pa.int16()
    if t.startswith(("int", "integer")):
        return pa.int32()
    if t.startswith("bigint"):
        return pa.int64()

    # -------------------- FLOAT --------------------
    if t.startswith("float"):
        return pa.float32()
    if t.startswith("double"):
        return pa.float64()

    # -------------------- DECIMAL --------------------
    if t.startswith("decimal"):
        m = re.match(r"decimal\((\d+),\s*(\d+)\)", t)
        if m:
            precision, scale = int(m.group(1)), int(m.group(2))
            return pa.decimal128(precision, scale)
        return pa.decimal128(38, 10)  # default fallback

    # -------------------- BOOLEAN --------------------
    if t.startswith("boolean"):
        return pa.bool_()

    # -------------------- DATE/TIME --------------------
    if t == "date":
        return pa.date32()

    if t.startswith("timestamp") or t.startswith("datetime"):
        return pa.timestamp("ms")  # hoặc "us" tùy hệ thống

    # -------------------- BINARY --------------------
    if t.startswith("varbinary") or t.startswith("binary"):
        return pa.binary()

    # -------------------- ARRAY --------------------
    if t.startswith("array<") or t.startswith("array("):
        inner = extract_inner(t)
        return pa.list_(ddl_type_to_arrow_type(inner))

    # -------------------- MAP --------------------
    if t.startswith("map<") or t.startswith("map("):
        inner = extract_inner(t)
        k, v = split_top_level(inner, ",")
        return pa.map_(
            ddl_type_to_arrow_type(k.strip()),
            ddl_type_to_arrow_type(v.strip())
        )

    # -------------------- STRUCT / ROW --------------------
    if t.startswith("row(") or t.startswith("struct<") or t.startswith("row<"):
        inner = extract_inner(t)
        fields = []

        for part in split_top_level(inner, ","):
            # support cả "a int" và "a:int"
            if ":" in part:
                k, v = part.split(":", 1)
            else:
                k, v = part.split(" ", 1)

            fields.append(
                pa.field(k.strip(), ddl_type_to_arrow_type(v.strip()))
            )

        return pa.struct(fields)

    # -------------------- UNKNOWN --------------------
    return pa.string()


# -------------------- DDL -> PyArrow schema --------------------
def unwrap_outer_parentheses(s: str) -> str:
    s = s.strip()

    while s.startswith("(") and s.endswith(")"):
        level = 0
        balanced = True

        for i, c in enumerate(s):
            if c == "(":
                level += 1
            elif c == ")":
                level -= 1

            # Nếu đóng ngoặc trước cuối chuỗi thì không phải ngoặc bao ngoài
            if level == 0 and i != len(s) - 1:
                balanced = False
                break

        if not balanced:
            break

        s = s[1:-1].strip()

    return s

def ddl_to_arrow_schema(ddl: str):
    match = re.search(r"CREATE TABLE.*?\((.*?)\)\s*(?:WITH|;|$)", ddl, re.S | re.I)
    if not match:
        raise ValueError("Không parse được DDL columns")

    cols_str = match.group(1).strip()
    
    print(cols_str)
    fields = []
    for col_def in split_fields(cols_str):
        if not col_def or col_def.lower().startswith("primary") or col_def.lower().startswith("constraint"):
            continue
        parts = col_def.strip().split(None, 1)
        if len(parts) < 2:
            continue
        name, typ = parts
        name = name.replace('"', '')
        # typ = typ.strip("(").strip(")").strip()
        # name = name.strip("(").strip(")").strip()
        name = unwrap_outer_parentheses(name)
        typ = unwrap_outer_parentheses(typ)
        print(name, f"[{typ}]")
        fields.append(pa.field(name.strip(), ddl_type_to_arrow_type(typ.strip())))
    return pa.schema(fields)

# ================= CAST =================

# ================= Convert values =================
DATETIME_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
]

def parse_datetime_to_epoch(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        return None
    for fmt in DATETIME_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)
            return int(dt.timestamp() * 1000)
        except Exception:
            continue
    return None

from collections.abc import Mapping, Sequence

def normalize_to_arrow(value, arrow_type=None):

    if value is None:
        return None

    # scalar
    if isinstance(value, (str, bytes, int, float, bool)):
        return value

    asdict = getattr(value, "_asdict", None)

    if callable(asdict):
        return {
            k: normalize_to_arrow(v)
            for k, v in asdict().items()
        }

    fields = getattr(value, "_fields", None)

    if fields is not None:
        return {
            k: normalize_to_arrow(v)
            for k, v in zip(fields, value)
        }

    # Mapping
    if isinstance(value, Mapping):
        return {
            normalize_to_arrow(k): normalize_to_arrow(v)
            for k, v in value.items()
        }

    # Sequence
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):

        # ROW -> dict
        if arrow_type is not None and pa.types.is_struct(arrow_type):
            fields = list(arrow_type)

            return {
                field.name: normalize_to_arrow(
                    value[i] if i < len(value) else None,
                    field.type
                )
                for i, field in enumerate(fields)
            }

        # ARRAY
        value_type = arrow_type.value_type if (
            arrow_type is not None and pa.types.is_list(arrow_type)
        ) else None

        return [
            normalize_to_arrow(v, value_type)
            for v in value
        ]

    return value

def convert_value_by_arrow_type(value, arrow_type: pa.DataType, col_name=None):
    if value is None:
        return None

    try:
        # -------------------- INTEGER --------------------
        if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) \
           or pa.types.is_int32(arrow_type) or pa.types.is_int64(arrow_type):
            return int(value)

        # -------------------- FLOAT --------------------
        if pa.types.is_float16(arrow_type) or pa.types.is_float32(arrow_type) \
           or pa.types.is_float64(arrow_type):
            return float(value)

        # -------------------- BOOLEAN --------------------
        if pa.types.is_boolean(arrow_type):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ("true", "1", "t", "yes")
            return bool(value)

        # -------------------- STRING --------------------
        if pa.types.is_string(arrow_type):
            return str(value)

        # -------------------- DECIMAL --------------------
        if pa.types.is_decimal(arrow_type):
            return decimal.Decimal(str(value))

        # -------------------- DATE --------------------
        if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
            if isinstance(value, date):
                return value
            if isinstance(value, str):
                return datetime.strptime(value[:10], "%Y-%m-%d").date()
            if isinstance(value, (int, float)):
                # unix days
                return date(1970, 1, 1) + timedelta(days=int(value))
            print(f"Warning {col_name} is not date - arrow_type={arrow_type}")
            return None

        # -------------------- TIMESTAMP --------------------
        if pa.types.is_timestamp(arrow_type):
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                # handle multiple formats
                try:
                    return datetime.fromisoformat(value)
                except:
                    return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value)
            print(f"Warning {col_name} is not timestamp - arrow_type={arrow_type}")
            return None

        # -------------------- BINARY --------------------
        if pa.types.is_binary(arrow_type):
            if isinstance(value, bytes):
                return value
            return str(value).encode()

        # -------------------- STRUCT --------------------
        if pa.types.is_struct(arrow_type):
            value = normalize_to_arrow(value, arrow_type)
            if not isinstance(value, dict):
                print(f"Warning {col_name} is not struct - arrow_type={arrow_type} {type(value)}")
                return None
            return {
                f.name: convert_value_by_arrow_type(value.get(f.name), f.type, col_name)
                for f in arrow_type
            }

        # -------------------- LIST --------------------
        if pa.types.is_list(arrow_type):
            if not isinstance(value, list):
                print(f"Warning {col_name} is not list")
                return None
            return [
                convert_value_by_arrow_type(v, arrow_type.value_type, col_name)
                for v in value
            ]

        # -------------------- MAP --------------------
        if pa.types.is_map(arrow_type):
            if not isinstance(value, dict):
                print(f"Warning {col_name} is not map - arrow_type={arrow_type}")
                return None
            key_type = arrow_type.key_type
            item_type = arrow_type.item_type
            return {
                convert_value_by_arrow_type(k, key_type, col_name):
                convert_value_by_arrow_type(v, item_type, col_name)
                for k, v in value.items()
            }

        # -------------------- FALLBACK --------------------
        return value

    except Exception as e:
        # tránh crash pipeline
        print(f"Convert failed: value={value}, type={arrow_type}, err={e}")
        traceback.print_exc()
        # import sys
        # sys.exit(0)
        return None
    

def convert_record_by_arrow_schema(record: dict, schema: pa.Schema):
    return {f.name: convert_value_by_arrow_type(record.get(f.name), f.type) for f in schema}


# ================= Write Parquet =================
def write_parquet_file_df(rows, schema, file_path, columns):
    print(f"columns={columns}")
    records = []
    for row in rows:
        rec = {}
        for _, col_name in enumerate(columns):
            value = row.get(col_name, None)
            rec[col_name] = convert_value_by_arrow_type(value, schema.field(col_name).type, col_name)
        records.append(rec)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, file_path, compression="snappy", row_group_size=100_000)


# ================= Write Parquet =================
def write_parquet_file(rows, schema, file_path, columns, row_group_size=100_000):
    records = []
    for row in rows:
        rec = {}
        for idx, col_name in enumerate(columns):
            value = row[idx] if idx < len(row) else None
            arrow_type = schema.field(col_name).type
            rec[col_name] = convert_value_by_arrow_type(value, arrow_type)
        records.append(rec)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, file_path, compression="snappy", row_group_size=row_group_size)