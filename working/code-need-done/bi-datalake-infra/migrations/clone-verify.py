import os
import re
import uuid
import argparse
import warnings
import json
from urllib.parse import urlparse
from datetime import datetime
import pytz

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
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
args = parser.parse_args()
SCHEMAS = [s.strip() for s in args.schemas.split(",")]

# ================= CONFIG =================

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

def verify_table(cursor, full_table_name: str) -> bool:
    try:
        query = f"SELECT * FROM {full_table_name} LIMIT 1"
        cursor.execute(query)
        cursor.fetchone()  # chỉ cần chạy được là OK
        return True
    except Exception as e:
        print(f"❌ Verify failed: {full_table_name} | {e}")
        return False
    
# ================= MAIN =================
for schema in SCHEMAS:
    print(f"\n==============================\n🚀 PROCESS SCHEMA: {schema}\n==============================")
    tgt_conn = get_conn(TGT, schema)
    tgt_cursor = tgt_conn.cursor()

    tables = get_tables(tgt_cursor, TGT["catalog"], schema)
    # if args.prefix:
    #     tables = [t for t in tables if t.startswith(args.prefix)]
    print(f"📦 {len(tables)} tables to process")

    tgt_cursor.execute("SET SESSION task_max_writer_count = 1")
    tgt_cursor.execute("SET SESSION hive.target_max_file_size = '64MB'")

    for table in tables:
        if table == "noc_metrics":
            continue
        full_tgt = f"hive.{schema}.{table}"
        verify_table(tgt_cursor, full_tgt)
        