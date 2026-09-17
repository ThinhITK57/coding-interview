import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime
from pathlib import Path

from prefect import task, get_run_logger

from common.config import *
from common.http_util import *
from common.crawler_util import *
from common.ambari_util import *
from common.flat.reader import *

AMBARI_SOURCE = ["excel_surveys", "excel_survey_questions"]

# ---------------------------------------------------------------------------
# Task 1 -- Read & normalise  (supports single file and multi-source)
# ---------------------------------------------------------------------------


@task(name="read_and_normalize")
def read_and_normalize(resource: dict) -> list:
    """
    Reads one or more source files for a resource, normalises columns,
    concatenates them, applies an optional top-level post_process, and
    returns JSON records.

    Handles both shapes:
      resource["object_key"]  -> single-source shorthand
      resource["sources"]     -> explicit list of source descriptors
    """
    logger = get_run_logger()
    resource_name = resource["resource_name"]
    domain = resource.get("domain", "cx_cso_raw")

    mapping = load_mapping(domain, resource_name)
    logger.info(f"[{resource_name}] Mapping loaded ({len(mapping)} entries)")

    s3 = build_s3_client()
    in_bucket = os.getenv("MINIO_INPUT_BUCKET")

    if "sources" in resource:
        sources = resource["sources"]
    else:
        sources = [
            {
                "object_key": resource["object_key"],
                "file_type": resource.get("file_type", "excel"),
                "sheet_name": resource.get("sheet_name", 0),
                "header_row": resource.get("header_row", 0),
                "drop_rows": resource.get("drop_rows", 1),
                "encoding": resource.get("encoding", "utf-8"),
                "add_columns": resource.get("add_columns", {}),
            }
        ]

    dfs = [
        read_source(s3, in_bucket, src, mapping, resource_name, logger)
        for src in sources
    ]
    df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

    logger.info(f"[{resource_name}] Total rows after concat: {len(df)}")

    if resource.get("post_process"):
        logger.info(f"[{resource_name}] Applying top-level post_process ...")
        df = resource["post_process"](df)
        logger.info(f"[{resource_name}] After post_process: {len(df)} rows")

    df = df.fillna("")
    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Task 2 -- Convert & upload to output bucket
# ---------------------------------------------------------------------------


@task(name="upload_data")
def upload_data(
    resource_name: str,
    records: list,
    domain: str,
    crawl_mode: str = "static",
    enable_state: bool = False,
    schema_local_path: str = None,
) -> bool:
    """
    1. Load / infer / cache the PyArrow schema.
    2. Write a Snappy-compressed Parquet file locally.
    3. Upload to the MinIO output bucket.
       - static           -> delete all existing objects first (full-replace)
       - modified_and_new -> append alongside existing files
    4. Persist a CREATE TABLE SQL stub for Spark / Hive.
    5. Optionally update last-seen state timestamp.

    Returns True when there are effectively no new records (< 100).
    """
    logger = get_run_logger()

    if not domain:
        logger.info("need to set domain param!")
        return

    hive_db = domain

    if not records:
        logger.warning(f"[{resource_name}] No records -- skipping upload")
        return True

    s3_client = build_s3_client()

    out_bucket_s3 = os.getenv("MINIO_OUTPUT_BUCKET")
    out_bucket_ambari = os.getenv("AMBARI_OUTPUT_BUCKET")
    start_time = time.time()

    last_state = read_last_state(resource_name) if enable_state else None
    logger.info(f"[{resource_name}] Last state: {last_state}")

    now = datetime.now()
    partition_folder = f"{domain}/{resource_name}"
    filename = f"data_{resource_name}_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    local_parquet = f"./tmp/data/{domain}/{resource_name}/{filename}"
    os.makedirs(os.path.dirname(local_parquet), exist_ok=True)

    records = convert_json_add_ts_columns(records)
    schema_tm_path = f"./resources/parquet_schema/{domain}/{resource_name}.json"
    schema = None

    if schema_local_path and Path(schema_local_path).exists():
        schema = load_pyarrow_schema_from_json(schema_local_path)
    elif Path(schema_tm_path).exists():
        schema = load_pyarrow_schema_from_json(schema_tm_path)

    if schema is None:
        logger.info(f"[{resource_name}] No cached schema -- inferring from data")
        schema = infer_schema_from_json(records)
        os.makedirs(os.path.dirname(schema_tm_path), exist_ok=True)
        write_file_json(schema_tm_path, save_pyarrow_type_to_json(schema))

    records = convert_json_list_by_arrow_schema(records, schema)
    df = pd.DataFrame(records)

    data_table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(data_table, local_parquet, compression="snappy")
    logger.info(f"[{resource_name}] Wrote local parquet: {local_parquet}")

    target_key = f"{partition_folder}/{filename}"

    common_params = {
        "partition_folder": partition_folder,
        "resource_name": resource_name,
        "logger": logger,
    }
    if resource_name in AMBARI_SOURCE:
        _ambari_writer(
            local_file=local_parquet,
            bucket=out_bucket_ambari,
            crawl_mode=crawl_mode,
            **common_params,
        )
    else:
        _s3_writer(
            client=s3_client,
            local_file=local_parquet,
            bucket=out_bucket_s3,
            target_key=target_key,
            crawl_mode=crawl_mode,
            **common_params,
        )

    sql = gen_spark_create_table(
        schema=schema,
        db=hive_db,
        table=resource_name,
        location=f"s3a://{out_bucket_s3}/{domain}/{resource_name}",
    )
    local_sql = f"./tmp/data/{domain}/{resource_name}/create_table_{resource_name}.sql"
    os.makedirs(os.path.dirname(local_sql), exist_ok=True)
    Path(local_sql).write_text(sql)
    logger.info(f"[{resource_name}] SQL stub written: {local_sql}")

    if enable_state and "updated_at" in df.columns:
        max_ts = subtract_minutes(get_max_updated_at_str(df), 30)
        write_last_state(max_ts, resource_name)
        logger.info(f"[{resource_name}] State saved: {max_ts}")

    elapsed = time.time() - start_time
    logger.info(f"[{resource_name}] Done in {elapsed:.3f}s | rows={len(records)}")
    return len(records) < 100


def _s3_writer(client, local_file, bucket, target_key, crawl_mode, **kwargs):
    partition_folder = kwargs["partition_folder"]
    resource_name = kwargs.get("resource_name")
    logger = kwargs.get("logger")
    if crawl_mode == "static":
        existing = client.list_objects_v2(Bucket=bucket, Prefix=partition_folder)
        for obj in existing.get("Contents", []):
            client.delete_object(Bucket=bucket, Key=obj["Key"])
            logger.debug(f"[{resource_name}] Deleted old object: {obj['Key']}")

    client.upload_file(local_file, bucket, target_key)
    logger.info(f"[{resource_name}] Uploaded -> s3://{bucket}/{target_key}")


def _ambari_writer(local_file, bucket, crawl_mode, **kwargs):
    hdfs_client = build_hdfs_client()
    partition_folder = kwargs["partition_folder"]
    resource_name = kwargs.get("resource_name")
    logger = kwargs.get("logger")
    hdfs_path = f"{bucket}/{partition_folder}"
    try:
        if crawl_mode == "static":
                hdfs_client.replace(hdfs_path, local_file)
        else:    
            hdfs_client.upload(hdfs_path, local_file)
    except Exception as e:
        print(e)
        raise e
    logger.info("Uploaded parquet to %s", partition_folder)
