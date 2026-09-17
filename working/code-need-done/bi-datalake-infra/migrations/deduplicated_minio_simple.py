from dotenv import load_dotenv
load_dotenv()
import os
import warnings
from urllib3.exceptions import InsecureRequestWarning
from trino.auth import BasicAuthentication
import sys
from datetime import datetime, timezone
import pytz
warnings.simplefilter('ignore', InsecureRequestWarning)


import os
import re
import uuid
from urllib.parse import urlparse
from crawler_util import load_pyarrow_schema_from_json

import pyarrow as pa
import pyarrow.parquet as pq
import trino
from trino.auth import BasicAuthentication
from pd_schema import write_parquet_file_df, ddl_to_arrow_schema


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

# ==== Helpers ====



# ==== Cast values theo schema ====
def cast_value(value, pa_type):
    if value is None:
        return None
    if pa.types.is_string(pa_type):
        return str(value)
    if pa.types.is_int64(pa_type) or pa.types.is_int32(pa_type):
        return int(value)
    if pa.types.is_boolean(pa_type):
        return bool(value)
    if pa.types.is_list(pa_type):
        return [cast_value(v, pa_type.value_type) for v in value]
    if pa.types.is_struct(pa_type):
        return {k: cast_value(value.get(k), pa_type[k].type) for k in pa_type}
    return value

# ==== Deduplicate records ====
def deduplicate_records(records, key_col, ts_col=None):
    print(f"Deduplicated mode  {key_col}, {ts_col}")
    if ts_col:
        records_sorted = sorted(records, key=lambda x: x.get(ts_col, 0), reverse=True)
        seen = set()
        dedup = []
        for r in records_sorted:
            k = r.get(key_col)
            if k not in seen:
                dedup.append(r)
                seen.add(k)
        return dedup
    else:
        seen = set()
        dedup = []
        for r in records:
            k = r.get(key_col)
            if k not in seen:
                dedup.append(r)
                seen.add(k)
        return dedup

    
# ==== Process table full ====
def process_table(cursor, schema_name, table_name, key_col="id", time_col="updated_at_ts"):
    print(f"\n🚀 Processing {schema_name}.{table_name}")
    # Get DDL
    cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table_name}")
    ddl = cursor.fetchone()[0]
    
    schema_tm_path = f"./resources/parquet_schema/{schema_name}/{table_name}.json"
    pa_schema = None
    if os.path.exists(schema_tm_path):
        pa_schema = load_pyarrow_schema_from_json(schema_tm_path)
    
    if not pa_schema:
        pa_schema = ddl_to_arrow_schema(ddl)

    # Get location
    m = re.search(r"(external_location|location)\s*=\s*'([^']+)'", ddl, re.I)
    if not m:
        raise ValueError(f"{schema_name}.{table_name} không có location")
    s3_location = m.group(2)

    # Download parquet files
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    local_dir = f"./tmp/{schema_name}/{table_name}_{suffix}"
    os.makedirs(local_dir, exist_ok=True)
    bucket, keys, parent_key = s3_client.list_s3_objects(s3_location)
    local_files = s3_client.download_s3_objects(bucket, keys, local_dir)

    all_records = []
    for f in local_files:
        table = pq.read_table(f)
        all_records.extend(table.to_pylist())

    print(f"   rows before dedup: {len(all_records)}")

    # Deduplicate
    # key_col = "id"
    ts_col = time_col if time_col in pa_schema.names else None
    records_clean = deduplicate_records(all_records, key_col, ts_col)

    print(f"   rows after dedup: {len(records_clean)}")

    # Write parquet mới
    out_file = os.path.join(local_dir, f"{table_name}_clean.parquet")
    
    cursor.execute(f"SELECT * FROM hive.{schema_name}.{table_name} limit 1")
    columns = [col[0] for col in cursor.description]
    write_parquet_file_df(records_clean, pa_schema, out_file, columns)
    
    # Upload overwrite
    parent_key = parent_key.rstrip("/")
    new_key = parent_key+"_"+suffix
    print(f"Backup {parent_key} -> {new_key}")
    s3_client.rename_prefix(bucket, parent_key, new_key)
    
    s3_client.upload_s3_file(out_file, bucket, keys[0])

    # Cleanup
    os.remove(out_file)
    for f in local_files:
        os.remove(f)

    print(f"✅ Done {schema_name}.{table_name}")

# ==== Example usage ====
schema = "cx_cso_raw"
table = "fact_cso_tickets"
key_col_name = "id"
time_col = "updated_at_ts"
cursor = get_trino_cursor(schema)
process_table(cursor, schema, table, key_col_name, time_col)