import os
import sys
import json
import time
import requests
import pandas as pd
import pyarrow as pa
from dateutil import parser
import pyarrow.parquet as pq
import pytz
from datetime import datetime, timedelta
import base64


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
TIME_FIELD_NAMES = ["created", "created_at", "updated", "updated_at"]


def build_basic_auth_header(username: str, password: str) -> str:
    auth_str = f"{username}:{password}"
    auth_bytes = auth_str.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")
    return f"Basic {auth_base64}"


def get_max_updated_at_str(df: pd.DataFrame, col: str = "updated_at") -> str | None:
    if col not in df.columns:
        return None
    # 2025-12-31T15:18:33+07:00
    # 2025-12-31T15:18:33Z
    ts = pd.to_datetime(df[col], errors="coerce", utc=True)

    max_ts = ts.max()

    if pd.isna(max_ts):
        return None

    return max_ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def subtract_minutes(iso_time: str, minutes: int) -> str:
    if iso_time is None:
        return iso_time
    dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt - timedelta(minutes=minutes)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fix_empty_object_in_json(data, dummy_field="_dummy_"):
    """
    Fix empty object {} in JSON recursively
    """
    # dict
    if isinstance(data, dict):
        # object rỗng
        if len(data) == 0:
            return {dummy_field: 1}

        fixed = {}
        for k, v in data.items():
            fixed[k] = fix_empty_object_in_json(v, dummy_field)
        return fixed

    # list
    if isinstance(data, list):
        return [fix_empty_object_in_json(item, dummy_field) for item in data]

    # primitive
    return data


def to_epoch_millis(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        # assume already epoch
        return int(value)

    if not isinstance(value, str):
        return None

    for fmt in DATETIME_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)

            # normalize timezone
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)

            return int(dt.timestamp() * 1000)
        except Exception:
            pass

    return None


def is_datetime_string(value):
    if not isinstance(value, str):
        return False

    for fmt in DATETIME_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return True
        except Exception:
            pass

    return False


def convert_json_list_time_to_long(records):
    """
    records: List[dict]
    return: List[dict]
    """
    output = []

    for r in records:
        new_r = {}

        for k, v in r.items():
            key_lower = k.lower()

            if key_lower in TIME_FIELD_NAMES or is_datetime_string(v):
                new_r[k] = to_epoch_millis(v)
            else:
                new_r[k] = v

        output.append(new_r)

    return output


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
            pass

    return None


def convert_value_by_arrow_type(value, arrow_type: pa.DataType):
    if value is None:
        return None

    # primitive
    if pa.types.is_int64(arrow_type):
        return int(value) if value is not None else None

    if pa.types.is_float64(arrow_type):
        return float(value) if value is not None else None

    if pa.types.is_boolean(arrow_type):
        return bool(value)

    if pa.types.is_string(arrow_type):
        return str(value)

    if pa.types.is_timestamp(arrow_type):
        return parse_datetime_to_epoch(value)

    # complex
    if pa.types.is_struct(arrow_type):
        if not isinstance(value, dict):
            return None
        return {
            f.name: convert_value_by_arrow_type(value.get(f.name), f.type)
            for f in arrow_type
        }

    if pa.types.is_list(arrow_type):
        if not isinstance(value, list):
            return None
        return [convert_value_by_arrow_type(v, arrow_type.value_type) for v in value]

    return value


def convert_record_by_arrow_schema(record: dict, schema: pa.Schema):
    out = {}
    for field in schema:
        out[field.name] = convert_value_by_arrow_type(
            record.get(field.name), field.type
        )
    return out


def convert_json_list_by_arrow_schema(records, schema: pa.Schema):
    return [convert_record_by_arrow_schema(r, schema) for r in records]


def parse_datetime_to_epoch_ms(value):
    if not isinstance(value, str):
        return None

    for fmt in DATETIME_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)
            return int(dt.timestamp() * 1000)
        except Exception:
            pass

    return None


def convert_json_add_ts_columns(records, suffix="_ts"):
    """
    records: List[dict]
    return: List[dict]
    """
    out = []
    now = int(datetime.now().timestamp())
    for r in records:
        r = fix_empty_object_in_json(r)
        new_r = dict(r)
        new_r["crawled_at_ts"] = now

        for k, v in r.items():
            epoch = parse_datetime_to_epoch_ms(v)
            if epoch is not None:
                new_r[k + suffix] = epoch

        out.append(new_r)

    return out


def infer_arrow_type(values):
    values = [v for v in values if v is not None]
    if not values:
        return pa.string()

    if all(isinstance(v, bool) for v in values):
        return pa.bool_()

    if all(isinstance(v, int) for v in values):
        return pa.int64()

    if all(isinstance(v, (int, float)) for v in values):
        return pa.float64()

    if all(isinstance(v, str) for v in values):
        return pa.string()
        # detect timestamp
        try:
            for v in values[:5]:
                parser.isoparse(v)
            return pa.timestamp("ms")
        except Exception:
            return pa.string()

    if all(isinstance(v, dict) for v in values):
        return infer_struct(values)

    if all(isinstance(v, list) for v in values):
        elem_type = infer_arrow_type([e for v in values for e in v if e is not None])
        return pa.list_(elem_type)

    return pa.string()


def infer_struct(dicts):
    fields = {}
    for d in dicts:
        for k, v in d.items():
            fields.setdefault(k, []).append(v)

    return pa.struct([pa.field(k, infer_arrow_type(v)) for k, v in fields.items()])


def infer_schema_from_json(records):
    cols = {}
    for r in records:
        for k, v in r.items():
            cols.setdefault(k, []).append(v)

    return pa.schema([pa.field(k, infer_arrow_type(v)) for k, v in cols.items()])


def arrow_to_hive_type(t: pa.DataType) -> str:
    if pa.types.is_int64(t):
        return "BIGINT"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP"
    if pa.types.is_string(t):
        return "STRING"
    if pa.types.is_struct(t):
        fields = [f"{f.name}:{arrow_to_hive_type(f.type)}" for f in t]
        return f"STRUCT<{', '.join(fields)}>"
    if pa.types.is_list(t):
        return f"ARRAY<{arrow_to_hive_type(t.value_type)}>"
    return "STRING"


def gen_spark_create_table(schema, db, table, location):
    cols = []
    for f in schema:
        cols.append(
            "  {name} {dtype}".format(name=f.name, dtype=arrow_to_hive_type(f.type))
        )

    sql = """
        CREATE TABLE IF NOT EXISTS {db}.{table} (
        {cols}
        )
        USING PARQUET
        LOCATION '{location}'
    """.format(
        db=db, table=table, cols=",\n".join(cols), location=location
    )

    return sql.strip()


def json_type_to_pyarrow(jt):
    t = jt["type"]

    # ===== PRIMITIVE =====
    if t == "string":
        return pa.string()

    if t == "boolean":
        return pa.bool_()

    if t == "long":
        return pa.int64()

    if t == "double":
        return pa.float64()

    if t == "timestamp":
        return pa.timestamp("ms")

    if t == "date":
        return pa.date32()

    # ===== ARRAY =====
    if t == "array":
        return pa.list_(json_type_to_pyarrow(jt["elementType"]))

    # ===== MAP =====
    if t == "map":
        return pa.map_(
            json_type_to_pyarrow(jt["keyType"]), json_type_to_pyarrow(jt["valueType"])
        )

    # ===== STRUCT =====
    if t == "struct":
        return pa.struct(
            [
                pa.field(
                    f["name"],
                    json_type_to_pyarrow(f["dataType"]),
                    f.get("nullable", True),
                )
                for f in jt["fields"]
            ]
        )

    # ===== FALLBACK =====
    return pa.string()


def load_pyarrow_schema_from_json(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        schema_json = json.load(f)

    assert schema_json["type"] == "struct", "Root schema must be struct"

    fields = []
    for f in schema_json["fields"]:
        fields.append(
            pa.field(
                f["name"], json_type_to_pyarrow(f["dataType"]), f.get("nullable", True)
            )
        )

    return pa.schema(fields)


def pyarrow_type_to_json(dt: pa.DataType):
    # ===== PRIMITIVE =====
    if pa.types.is_string(dt):
        return {"type": "string"}

    if pa.types.is_boolean(dt):
        return {"type": "boolean"}

    # ByteType, ShortType, IntegerType, LongType
    if (
        pa.types.is_int64(dt)
        or pa.types.is_int8(dt)
        or pa.types.is_int16(dt)
        or pa.types.is_int32(dt)
        or pa.types.is_integer(dt)
        or pa.types.is_uint8(dt)
        or pa.types.is_uint16(dt)
        or pa.types.is_uint32(dt)
        or pa.types.is_unsigned_integer(dt)
    ):
        return {"type": "long"}

    # (FloatType, DoubleType, DecimalType)
    if (
        pa.types.is_float16(dt)
        or pa.types.is_float32(dt)
        or pa.types.is_float64(dt)
        or pa.types.is_decimal(dt)
        or pa.types.is_decimal32(dt)
        or pa.types.is_decimal64(dt)
        or pa.types.is_decimal128(dt)
        or pa.types.is_decimal256(dt)
    ):
        return {"type": "double"}

    if pa.types.is_timestamp(dt):
        return {"type": "timestamp"}

    if pa.types.is_date(dt):
        return {"type": "date"}

    # ===== ARRAY =====
    if pa.types.is_list(dt):
        return {"type": "array", "elementType": pyarrow_type_to_json(dt.value_type)}

    # ===== MAP =====
    if pa.types.is_map(dt) or pa.types.is_dictionary(dt):
        if not isinstance(dt, dict):
            return {"type": "string"}
        return [
            {
                "type": "map",
                "keyType": pyarrow_type_to_json(f.key_type),
                "valueType": pyarrow_type_to_json(f.value_type),
            }
            for f in dt
        ]

    # ===== STRUCT =====
    if pa.types.is_struct(dt):
        return {
            "type": "struct",
            "fields": [
                {
                    "name": f.name,
                    "nullable": f.nullable,
                    "dataType": pyarrow_type_to_json(f.type),
                }
                for f in dt
            ],
        }

    # ===== FALLBACK =====
    return {"type": "string"}


def save_pyarrow_type_to_json(schema):
    return {
        "type": "struct",
        "fields": [
            {
                "name": f.name,
                "nullable": f.nullable,
                "dataType": pyarrow_type_to_json(f.type),
            }
            for f in schema
        ],
    }


def write_file_json(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=4))


def read_file_json(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


def write_file_text(filename, data: str):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(data)


def read_file_text(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return f.read()

def remove_file(file_path):
    try:
        # os.remove(file_path)
        from pathlib import Path
        Path(file_path).unlink(missing_ok=True)
        print(f"Đã xoá file: {file_path}")
    except FileNotFoundError:
        print(f"File không tồn tại: {file_path}")
    except PermissionError:
        print(f"Không có quyền xoá file: {file_path}")
    except Exception as e:
        print(f"Lỗi khi xoá file: {e}")
    