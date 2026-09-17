import os
import re
import uuid
import argparse
import warnings
import json
from urllib.parse import urlparse
from datetime import datetime
import time 
from crawler_util import load_pyarrow_schema_from_json

import trino
from trino.auth import BasicAuthentication
from urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv

load_dotenv()
warnings.simplefilter('ignore', InsecureRequestWarning)

from pd_schema import *

# ================= CLI =================
parser = argparse.ArgumentParser()
parser.add_argument("--schemas", required=True, help="comma separated schemas")
parser.add_argument("--prefix", default=None)
parser.add_argument("--truncate", action="store_true", help="truncate target table before insert")
parser.add_argument("--schema", action="store_true", help="drop current schema and copy src schema")
args = parser.parse_args()
SCHEMAS = [s.strip() for s in args.schemas.split(",")]

BATCH_SIZE = 50_000
TMP_DIR = "./tmp/trino_parquet"
os.makedirs(TMP_DIR, exist_ok=True)

# ================= CONFIG =================
SRC = {
    "host": "trino.viettelcyber.com",
    "port": 443,
    "user": os.getenv("TRINO_SRC_USERNAME"),
    "catalog": "hive",
    "http_scheme": "https",
    "password": os.getenv("TRINO_SRC_PASSWORD")
}

TGT = {
    "host": "10.255.245.150",
    "port": 8443,
    "user": os.getenv("TRINO_DST_USERNAME"),
    "catalog": "hive",
    "http_scheme": "https",
    "password": os.getenv("TRINO_DST_PASSWORD")
}

# ================= MINIO =================

from minio_file_client import MinioS3Client
minio_client = MinioS3Client()


# ================= TRINO =================
def get_conn(cfg, schema):
    return trino.dbapi.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        catalog=cfg["catalog"],
        schema=schema,
        http_scheme=cfg["http_scheme"],
        auth=BasicAuthentication(cfg['user'], cfg['password']),
        verify=False
    )

def get_tables(cursor, catalog, schema):
    cursor.execute(f"""
        SELECT table_name FROM {catalog}.information_schema.tables
        WHERE table_schema='{schema}' AND table_type='BASE TABLE'
    """)
    return [row[0] for row in cursor.fetchall()]

def get_create_table_ddl(cursor, full_table_name):
    cursor.execute(f"SHOW CREATE TABLE {full_table_name}")
    return cursor.fetchone()[0]

def normalize_ddl(ddl, tgt_schema, table):

    # replace table name
    ddl = re.sub(
        r"CREATE TABLE\s+\S+",
        f"CREATE TABLE {tgt_schema}.{table}",
        ddl,
        flags=re.IGNORECASE
    )

    # remove external_location
    ddl = re.sub(
        r"\s*external_location\s*=\s*'[^']*'\s*,?",
        "",
        ddl,
        flags=re.IGNORECASE
    )

    # cleanup syntax
    ddl = re.sub(r",\s*\)", ")", ddl)
    ddl = re.sub(r"\(\s*,", "(", ddl)

    return ddl

def create_table_if_not_exists(cursor, ddl):
    try:
        cursor.execute(ddl)
    except Exception as e:
        if "already exists" in str(e).lower():
            print("⚠️ Table exists, skip create")
        else:
            raise

def table_exists(cursor, catalog: str, schema: str, table: str) -> bool:
    query = f"""
        SELECT 1
        FROM information_schema.tables
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table}'
        LIMIT 1
    """
    cursor.execute(query)
    return cursor.fetchone() is not None


def extract_s3_location(ddl):
    match = re.search(r"(external_location|location)\s*=\s*'([^']+)'", ddl, re.I)
    if not match:
        raise ValueError("❌ Không tìm thấy external_location trong DDL")
    return match.group(2)

# ================= DDL → JSON schema & PyArrow schema =================

import re
import pyarrow as pa

# -------------------- helper parse nested --------------------
def split_fields(s):
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

# -------------------- DDL -> JSON schema --------------------
def ddl_to_json_schema(ddl: str):
    m = re.search(r"\((.*)\)", ddl, re.S)
    if not m:
        raise ValueError("Không parse được DDL columns")
    cols_str = m.group(1)
    fields = []
    for col_def in split_fields(cols_str):
        if not col_def or col_def.lower().startswith("primary") or col_def.lower().startswith("constraint"):
            continue
        parts = col_def.strip().split(None, 1)
        if len(parts) < 2:
            continue
        name, typ = parts
        fields.append({
            "name": name.strip(),
            "nullable": True,
            "dataType": ddl_type_to_json_type(typ.strip())
        })
    return {"type": "struct", "fields": fields}


# ================= Clone =================
def clone_table_via_parquet(src_cursor, tgt_cursor,schema, table, truncate=False, is_clone_schema=False):
    full_src = f"hive.{schema}.{table}"
    full_tgt = f"hive.{schema}.{table}"
    ddl_src = get_create_table_ddl(src_cursor, full_src)
    
    schema_tm_path = f"./resources/parquet_schema/{schema}/{table}.json"
    schema_pd = None
    if os.path.exists(schema_tm_path):
        schema_pd = load_pyarrow_schema_from_json(schema_tm_path)
    
    if not schema_pd:
        schema_pd = ddl_to_arrow_schema(ddl_src)

    if is_clone_schema:
        print(f"🧨 Recreate table {full_tgt}")
        existed = table_exists(tgt_cursor, 'hive', schema, table)
        if existed:
            ddl_tgt = get_create_table_ddl(tgt_cursor, full_tgt)
            s3_location = extract_s3_location(ddl_tgt)
            print(f"\n🚀 Remove Folder {s3_location}")
            minio_client.remove_dir(s3_location)
            
        tgt_cursor.execute(f"DROP TABLE IF EXISTS {full_tgt}")
            
        print(tgt_cursor.fetchone())
        
        ddl_tgt = normalize_ddl(ddl_src, tgt_schema=f"hive.{schema}", table=table)
        ddl_tgt = ddl_tgt.replace("csv_escape = '\\',", "")
        ddl_tgt = ddl_tgt.replace("csv_quote = '\"',", "")
        ddl_tgt = ddl_tgt.replace("csv_separator = ',',", "")
        ddl_tgt = ddl_tgt.replace("format = 'CSV',", "")
        ddl_tgt = ddl_tgt.replace("skip_header_line_count = 1", "format = 'PARQUET'")
        
        print(ddl_tgt)
        print(f"Try to create table -  {full_tgt}")
        create_table_if_not_exists(tgt_cursor, ddl_tgt)
        for field in schema_pd:
            print(f"{field.name}: {field.type}")
        
        time.sleep(2)
        
    ddl_tgt = get_create_table_ddl(tgt_cursor, full_tgt)
    s3_location = extract_s3_location(ddl_tgt)
    print(f"\n🚀 Cloning {full_src} → {s3_location}")
    
    if truncate:
        minio_client.remove(s3_location)

    src_cursor.execute(f"SELECT * FROM {full_src}")
    columns = [col[0] for col in src_cursor.description]

    total_rows = 0
    file_count = 0
    while True:
        rows = src_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        local_file = os.path.join(TMP_DIR, f"{full_src}_{uuid.uuid4().hex}.parquet")
        write_parquet_file(rows, schema_pd, local_file, columns)
        minio_client.upload(s3_location, local_file)
        os.remove(local_file)
        total_rows += len(rows)
        file_count += 1
        print(f"   {total_rows} rows, {file_count} files uploaded")
    print(f"✅ DONE {full_src}: {total_rows} rows, {file_count} files")

# ================= MAIN =================
for schema in SCHEMAS:
    print(f"\n==============================\n🚀 PROCESS SCHEMA: {schema}\n==============================")
    src_conn = get_conn(SRC, schema)
    tgt_conn = get_conn(TGT, schema)
    src_cursor = src_conn.cursor()
    tgt_cursor = tgt_conn.cursor()

    tables = get_tables(src_cursor, SRC["catalog"], schema)
    if args.prefix:
        tables = [t for t in tables if t.startswith(args.prefix)]
    print(f"📦 {len(tables)} tables to process")

    tgt_cursor.execute("SET SESSION task_max_writer_count = 1")
    tgt_cursor.execute("SET SESSION hive.target_max_file_size = '64MB'")

    for table in tables:
        if table == "noc_metrics":
            continue
        
        if table not in ["cx_mixpanel_product_feature"]:
            continue
        # if table not in ["role_table_permissions", "user_role", "user_table_permissions"]:
        #     continue
        existed = table_exists(src_cursor, 'hive', schema, table)
        if not existed:
            print('hive', schema, table, "is not exists")
            continue
        existed = table_exists(tgt_cursor, 'hive', schema, table)
        if not existed:
            print(f"{schema}.{table} is not exists, need to create")
        
        try:
            clone_table_via_parquet(src_cursor, tgt_cursor,schema, table, truncate=args.truncate, is_clone_schema=args.schema or not existed)
        except Exception as e:
            print(e)
        print("\n\n")