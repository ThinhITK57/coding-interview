from dotenv import load_dotenv
load_dotenv()
import os
import warnings
from urllib3.exceptions import InsecureRequestWarning
from trino.auth import BasicAuthentication
from datetime import datetime, timezone
warnings.simplefilter('ignore', InsecureRequestWarning)

import os
import re
import pyarrow as pa
import trino
from trino.auth import BasicAuthentication


# ==== MinIO config ====
from minio_file_client import MinioS3Client

s3_client = MinioS3Client()

# ==== Trino connection ====
def get_trino_cursor(schema="bi_silver"):
    return trino.dbapi.connect(
        host="10.255.245.150",
        port=8443,
        user=os.getenv("TRINO_DST_USERNAME"),
        catalog="hive",
        schema=schema,
        http_scheme="https",
        auth=BasicAuthentication(
            os.getenv("TRINO_DST_USERNAME"), os.getenv("TRINO_DST_PASSWORD")
        ),
        verify=False,
    ).cursor()

# ==== Process table full ====
def process_table(cursor, schema_name, table_name):
    print(f"\n🚀 Processing {schema_name}.{table_name}")
    # Get DDL
    cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table_name}")
    ddl = cursor.fetchone()[0]
    
    # Get location
    m = re.search(r"(external_location|location)\s*=\s*'([^']+)'", ddl, re.I)
    if not m:
        raise ValueError(f"{schema_name}.{table_name} không có location")
    s3_location = m.group(2)
    print(f"s3_location={s3_location}")

    # Download parquet files
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    local_dir = f"./tmp/{schema_name}/{table_name}_{suffix}"
    os.makedirs(local_dir, exist_ok=True)
    bucket, parent_key = s3_client.parse_s3_path(s3_location)
    
    # Upload overwrite
    parent_key = parent_key.rstrip("/")
    new_key = parent_key+"_"+suffix
    print(f"Backup {parent_key} -> {new_key}")
    dst_backup_bucket = "vcs-raw-backup"
    s3_client.clone_prefix(bucket, parent_key, dst_backup_bucket, new_key)
    
    print(f"✅ Done Clone {schema_name}.{table_name} to {dst_backup_bucket}")

# ==== Example usage ====
schema = "cx_cso_raw"
table = "fact_cso_tickets"
cursor = get_trino_cursor(schema)
process_table(cursor, schema, table)