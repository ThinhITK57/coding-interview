from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR

from prefect import flow, get_run_logger, task
import warnings
from urllib3.exceptions import InsecureRequestWarning
from trino.auth import BasicAuthentication
import re
from datetime import datetime, timezone, timedelta

warnings.simplefilter('ignore', InsecureRequestWarning)

import re
from common.crawler_util import load_pyarrow_schema_from_json
import pyarrow as pa
import pyarrow.parquet as pq
import trino
from trino.auth import BasicAuthentication
from common.pd_schema import write_parquet_file_df, ddl_to_arrow_schema
from prefect.cache_policies import NO_CACHE


# ==== MinIO config ====
from common.minio_file_client import MinioS3Client



# ==== Trino connection ====
def get_trino_cursor(schema="bi_silver"):
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST"),
        port= int(os.getenv("TRINO_PORT", 8443)),
        user=os.getenv("TRINO_USERNAME"),
        catalog=os.getenv("TRINO_CATALOG"),
        schema=schema,
        http_scheme=os.getenv("TRINO_HTTP_SCHEMA"),
        auth=BasicAuthentication(
            os.getenv("TRINO_USERNAME"), os.getenv("TRINO_PASSWORD")
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
    if ts_col:
        records_sorted = sorted(records, key=lambda x: x.get(ts_col, 0) or 0, reverse=True)
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


def cleanup_old_backups(s3_client: MinioS3Client, bucket, prefix_base, max_keep=30, max_days=30, dry_run=True):
    """
    prefix_base: ví dụ 'table_name'
    """
    logger = get_run_logger()
    logger.info(f"cleanup_old_backups start")
    # List tất cả backup prefix
    keys = s3_client.list_objects(bucket, prefix_base)
    
    # Extract prefix unique (folder level)
    prefixes = set()
    for key in keys:
        m = re.match(rf"({prefix_base}_(\d{{8}}_\d{{6}}))", key)
        if m:
            prefixes.add(m.group(1))

    backups = []
    for p in prefixes:
        try:
            ts_str = p.split("_")[-2] + "_" + p.split("_")[-1]
            ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
            backups.append((p, ts))
        except Exception:
            continue

    # Sort newest -> oldest
    backups.sort(key=lambda x: x[1], reverse=True)

    logger.info(f"Found {len(backups)} backups")

    now = datetime.now(timezone.utc)
    cutoff_time = now - timedelta(days=max_days)

    to_delete = []

    # chỉ xét nếu > max_keep
    if len(backups) > max_keep:
        for i, (prefix, ts) in enumerate(backups):
            if i < max_keep:
                continue  # giữ lại 30 cái mới nhất

            if ts < cutoff_time:
                to_delete.append(prefix)

    logger.info(f"Delete candidates: {to_delete}")

    # Delete
    for prefix in to_delete:
        logger.info(f"Deleting backup {prefix}")
        if dry_run:
                logger.info(f"[DRY RUN] Would delete {prefix}")
        else:
            s3_client.delete_prefix(bucket, prefix)
    logger.info(f"cleanup_old_backups end")
        
        

# ==== Process table full ====
@task(name="process_table", cache_policy=NO_CACHE)
def process_table(cursor, schema_name, table_name, s3_client: MinioS3Client, is_deduplicated=True, key_col="id", time_col="updated_at_ts"):
    logger = get_run_logger()
    logger.info(f"\n🚀 Processing {schema_name}.{table_name} , is_deduplicated={is_deduplicated}")
    # Get DDL
    cursor.execute(f"SHOW CREATE TABLE hive.{schema_name}.{table_name}")
    ddl = cursor.fetchone()[0]

    # Get location
    m = re.search(r"(external_location|location)\s*=\s*'([^']+)'", ddl, re.I)
    if not m:
        raise ValueError(f"{schema_name}.{table_name} không có location")
    s3_location = m.group(2)

    # Download parquet files
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    local_dir = f"./tmp/{schema_name}/{table_name}_{suffix}"
    os.makedirs(local_dir, exist_ok=True)
    bucket, parent_key = s3_client.parse_s3_path(s3_location)
    
    # Upload overwrite
    parent_key = parent_key.rstrip("/")
    new_key = parent_key+"_"+suffix
    logger.info(f"Backup {parent_key} -> {new_key}")
    dst_backup_bucket = "vcs-raw-backup"
    s3_client.clone_prefix(bucket, parent_key, dst_backup_bucket, new_key)
    
    cleanup_old_backups(
        s3_client=s3_client,
        bucket=dst_backup_bucket,
        prefix_base=parent_key,
        max_keep=30,
        max_days=30
    )
    
    if not is_deduplicated:
        logger.info(f"Only backup , no deduplicate table {schema_name}, {table_name} -> {s3_location}")
        return
    
    schema_tm_path = f"{RESOURCE_BASE_DIR}/parquet_schema/{schema_name}/{table_name}.json"
    pa_schema = None
    if os.path.exists(schema_tm_path):
        pa_schema = load_pyarrow_schema_from_json(schema_tm_path)
    
    if not pa_schema:
        pa_schema = ddl_to_arrow_schema(ddl)
    
    
    bucket, keys, parent_key = s3_client.list_s3_objects(s3_location)
    if not keys:
        return
    
    logger.info(f"Download s3 file to local {bucket}/{keys} -> {local_dir}")
    local_files = s3_client.download_s3_objects(bucket, keys, local_dir)

    all_records = []
    for f in local_files:
        table = pq.read_table(f)
        all_records.extend(table.to_pylist())

    logger.info(f"rows before dedup: {len(all_records)}")

    # Deduplicate
    # key_col = "id"
    ts_col = time_col if time_col in pa_schema.names else None
    logger.info(f"Deduplicated mode  {key_col}, {ts_col}")
    
    records_clean = deduplicate_records(all_records, key_col, ts_col)

    logger.info(f"   rows after dedup: {len(records_clean)}")

    # Write parquet mới
    out_file = os.path.join(local_dir, f"{table_name}_clean.parquet")
    
    cursor.execute(f"SELECT * FROM hive.{schema_name}.{table_name} limit 1")
    columns = [col[0] for col in cursor.description]
    write_parquet_file_df(records_clean, pa_schema, out_file, columns)
    
    s3_client.remove_files(bucket, keys)
    
    s3_client.upload_s3_file(out_file, bucket, keys[0])

    # Cleanup
    logger.info(f"Clean local OUT temp file {out_file}")
    try:
        os.remove(out_file)
    except Exception as e:
        logger.error(f"{e}")
    for f in local_files:
        try:
            os.remove(f)
            logger.info(f"Remove local temp file {f}")
        except Exception as e:
            logger.error(f"{e}")

    logger.info(f"✅ Done {schema_name}.{table_name}")


@flow(log_prints=True, name="[D] Daily Data Lake Backup and Deduplicate")
def run_daily_backup_and_deduplicate_table():
    logger = get_run_logger()
    s3_client = MinioS3Client()
    # ==== Example usage ====
    candidates = [
        ("crm_raw", "sales_accounts", "id", "updated_at_ts"),
        # ("crm_raw", "deals", "id", "updated_at_ts"),
        ("cx_cso_raw", "fact_cso_tickets", "id", "updated_at_ts"),
        ("cx_cso_raw", "cx_company", "id", "updated_at_ts"),
        ("cx_cso_raw", "dim_cso_contacts", "id", "updated_at_ts"),
        # ("cx_cso_raw", "dim_survicate_surveys", "id", "updated_at_ts"),
        ("cx_cso_raw", "fact_cso_tickets_sla", "ticket_id", "crawled_at_ts"),
    ]
    for schema, table, key_col, time_col in candidates:
        cursor = get_trino_cursor(schema)
        process_table(cursor, schema, table, s3_client, key_col=key_col, time_col=time_col)



@flow(name="[Ma] Trigger Data Lake Backup and Deduplicate for single table")
def run_single_backup_and_deduplicate_table(schema: str, table : str, is_deduplicated: bool, key_col="id", time_col="updated_at_ts" ):
    s3_client = MinioS3Client()
    cursor = get_trino_cursor(schema)
    process_table(cursor, schema, table, s3_client, is_deduplicated, key_col=key_col, time_col=time_col)

    

if __name__ == "__main__":
    d1 = run_daily_backup_and_deduplicate_table.to_deployment(
        name="[D] Daily Data Lake Backup and Deduplicate",
        cron="10 20 * * *",
        tags=["production", "Datalake", "Backup", "Deduplicate"],
    )
    d2 = run_single_backup_and_deduplicate_table.to_deployment(
        name="[Ma] Trigger Data Lake Backup and Deduplicate for single table",
        tags=["production", "spark", "etl"],
    )
    
    from prefect import serve
    serve(d1, d2)