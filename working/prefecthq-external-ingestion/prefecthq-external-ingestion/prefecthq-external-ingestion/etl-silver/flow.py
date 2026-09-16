from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger
import requests
import subprocess
import time
from datetime import datetime, timezone
import uuid


LIVY_ENDPOINT = os.getenv("LIVY_ENDPOINT")
HDFS_SOURCE_CODE_DIR = os.getenv("HDFS_SOURCE_CODE_DIR")


@task
def upload_to_hdfs(local_path: str, hdfs_dir="/jobs/silver"):
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
        check=True
    )

    hdfs_path = f"{hdfs_dir}/{os.path.basename(local_path)}"

    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
        check=True
    )

    return f"hdfs://{hdfs_path}"


@task(retries=3, retry_delay_seconds=30)
def submit_livy_job(hdfs_file):
    payload = {
        "file": hdfs_file,
        "conf": {
            "spark.executor.memory": "1g",
            "spark.executor.cores": "2",
            "spark.sql.sources.partitionOverwriteMode": "dynamic", # Done overwrite partition
            "spark.sql.hive.convertMetastoreParquet": "false", # Dont use Hive metadata
        },
        "args": ["2026-02-01"]
    }
    r = requests.post(LIVY_ENDPOINT + "/batches", json=payload)
    r.raise_for_status()
    return r.json()["id"]

@task
def wait_job(batch_id):
    while True:
        r = requests.get(LIVY_ENDPOINT + f"/batches/{batch_id}")
        state = r.json()["state"]
        if state in ("success", "dead", "killed"):
            return state
        time.sleep(10)

@task
def build_spark_job(sql_file: str, template_path: str, output_dir: str):
    logger = get_run_logger()
    
    filename = os.path.basename(sql_file)
    schema, table = filename.replace(".sql", "").split(".")
    if not schema or not table or not schema.endswith("_silver"):
        return None, None, None, None

    silver_batch_id = str(uuid.uuid4())
    snapshot_ts = datetime.now(timezone.utc).isoformat()

    with open(sql_file, "r", encoding="utf-8") as f:
        sql_query = f.read().strip()

    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    pyspark_code = (
        template
        .replace("#SCHEMA#", schema)
        .replace("#TABLE_NAME#", table)
        .replace("#SQL_QUERY#", sql_query)
        .replace("#SNAPSHOT_TS#", snapshot_ts)
        .replace("#SILVER_BATCH_ID#", silver_batch_id)
    )
    
    logger.info(f"Build sql {schema}.{table} at {snapshot_ts}  -  {silver_batch_id}")
    
    os.makedirs(output_dir, exist_ok=True)

    output_file = f"{schema}__{table}_create_silver_data.py"
    output_path = os.path.join(output_dir, output_file)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(pyspark_code)

    return schema, table, output_path, silver_batch_id


@task
def process_sql(sql_file):
    logger = get_run_logger()
    TEMPLATE = "templates/create_silver_data_from_sql_template.py"
    OUTPUT_DIR = "generated_jobs"
    
    schema, table, py_file, silver_batch_id = build_spark_job(
        sql_file,
        TEMPLATE,
        OUTPUT_DIR
    )
    
    if not silver_batch_id:
        logger.error(f"Cannot process {sql_file}")
        return
    
    hdfs_file = upload_to_hdfs(py_file, hdfs_dir=HDFS_SOURCE_CODE_DIR)
    batch_id = submit_livy_job(hdfs_file)
    
    logger.info(f"Submit job - {batch_id} - {schema}.{table} - {silver_batch_id} - {py_file}")
    return {
        "schema": schema,
        "table": table,
        "batch_id": batch_id
    }


@flow(name="silver-data-pipeline")
def silver_pipeline():
    SQL_FILES = [
        "sql/crm_silver.deals.sql",
        "sql/crm_silver.kpis.sql",
        "sql/crm_silver.partners.sql",
    ]
    results = []
    for f in SQL_FILES:
        results.append(process_sql(f))
    return results


if __name__ == "__main__":
    d1 = silver_pipeline.to_deployment(
        name="[Hourly] [Silver] ETL Data Lake",
        cron="0 * * * *",
        tags=["production", "DataLake", "ETL"],
    )
    from prefect import serve
    serve(d1)