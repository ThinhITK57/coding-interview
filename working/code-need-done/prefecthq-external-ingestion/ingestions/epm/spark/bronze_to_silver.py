"""
Spark ETL Pipeline: Bronze (Raw Conformed) -> Silver (Deduplicated & Cleaned)
Author: Data Engineering Team
Target Environment: Python 3.7.1 | Apache Spark 2.3.2 | Java 8

Purpose:
  1. Ingest Bronze layer Parquet data produced by API Extractor.
  2. Enforce strict Schema Contracts (from data_type/<table_name>_dataType.sql).
  3. Execute primary-key deduplication (sysid + last_updated_on) using DedupEngine.
  4. Write clean, partitioned Silver Parquet tables into MinIO / S3 Lakehouse.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime

# Add root directory to sys.path for module resolution
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.env_loader import load_env_file
load_env_file()

from transform.spark_session import SparkSessionFactory
from transform.schema_contract import SchemaContract
from transform.contract_conformer import read_conformed, assert_matches_contract
from transform.dedup_engine import DedupEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("bronze_to_silver")

SUPPORTED_TABLES = [
    "tasks",
    "projects",
    "targets",
    "objectives",
    "assignments"
]


def process_bronze_to_silver(
    spark,
    table_name: str,
    bronze_base_path: str = "s3a://lakehouse/bronze/clarizen",
    silver_base_path: str = "s3a://lakehouse/silver/epm",
    contract_dir: str = "./data_type"
) -> dict:
    """
    Transforms a single table from Bronze to Silver layer.
    """
    table_lower = table_name.lower()
    logger.info(f"=== Starting Bronze to Silver transformation for: {table_lower} ===")

    # 1. Load Schema Contract
    contract = SchemaContract.load(table_lower, contract_dir=contract_dir)
    logger.info(f"Loaded schema contract with {len(contract.columns)} columns for {table_lower}")

    # 2. Determine paths
    bronze_input = f"{bronze_base_path.rstrip('/')}/{table_lower}"
    silver_output = f"{silver_base_path.rstrip('/')}/epm_{table_lower}"

    # Fallback to local data directories if S3 path does not exist
    if bronze_base_path.startswith("./") or not os.getenv("MINIO_ENDPOINT"):
        bronze_input = os.path.join(ROOT_DIR, "data", "bronze", table_lower)
        silver_output = os.path.join(ROOT_DIR, "data", "silver", "epm", f"epm_{table_lower}")

    logger.info(f"Input Bronze Path:  {bronze_input}")
    logger.info(f"Output Silver Path: {silver_output}")

    # 3. Read Bronze Conformed Data
    try:
        # Check if directory exists before reading
        raw_df = spark.read.parquet(bronze_input)
        raw_count = raw_df.count()
        logger.info(f"Read {raw_count} raw records from {bronze_input}")
    except Exception as e:
        logger.warning(f"Could not read from primary path {bronze_input}: {e}")
        # Try local fallback
        local_path = os.path.join(ROOT_DIR, "data", "bronze", table_lower)
        if os.path.exists(local_path):
            logger.info(f"Falling back to local path: {local_path}")
            raw_df = spark.read.parquet(local_path)
            raw_count = raw_df.count()
        else:
            logger.error(f"Bronze path for {table_lower} not found. Skipping.")
            return {"table": table_lower, "status": "NOT_FOUND", "raw_count": 0, "silver_count": 0}

    if raw_count == 0:
        logger.warning(f"Table {table_lower} has 0 records in Bronze. Skipping dedup.")
        return {"table": table_lower, "status": "EMPTY", "raw_count": 0, "silver_count": 0}

    # 4. Conform columns according to contract
    conformed_cols = []
    for col_spec in contract.columns:
        if col_spec.name in raw_df.columns:
            conformed_cols.append(raw_df[col_spec.name].cast(col_spec.spark_type()).alias(col_spec.name))
        elif col_spec.src_name in raw_df.columns:
            conformed_cols.append(raw_df[col_spec.src_name].cast(col_spec.spark_type()).alias(col_spec.name))
        else:
            from pyspark.sql.functions import lit
            conformed_cols.append(lit(None).cast(col_spec.spark_type()).alias(col_spec.name))

    # Keep technical audit columns
    technical_cols = ["_raw_payload", "_batch_id", "_ingest_timestamp", "ingest_date"]
    for tech_col in technical_cols:
        if tech_col in raw_df.columns:
            conformed_cols.append(raw_df[tech_col])

    conformed_df = raw_df.select(*conformed_cols)

    # 5. Deduplication using DedupEngine
    logger.info(f"Executing deduplication on table {table_lower}...")
    dedup_engine = DedupEngine(primary_key="sysid", watermark_field="last_updated_on")
    cleaned_df, duplicates_df = dedup_engine.deduplicate_with_audit(conformed_df)

    silver_count = cleaned_df.count()
    dup_count = duplicates_df.count()
    logger.info(f"Deduplication complete: {silver_count} unique records kept, {dup_count} duplicate records discarded.")

    # 6. Partition column handling: ingest_date
    from pyspark.sql import functions as F
    if "ingest_date" not in cleaned_df.columns:
        if "last_updated_on" in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                "ingest_date",
                F.coalesce(F.to_date("last_updated_on"), F.current_date())
            )
        else:
            cleaned_df = cleaned_df.withColumn("ingest_date", F.current_date())

    # 7. Write to Silver Layer
    logger.info(f"Writing {silver_count} records to Silver Parquet: {silver_output}")
    (
        cleaned_df.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .format("parquet")
        .option("compression", "snappy")
        .save(silver_output)
    )

    logger.info(f"Successfully processed Bronze -> Silver for {table_lower}.")
    return {
        "table": table_lower,
        "status": "SUCCESS",
        "raw_count": raw_count,
        "silver_count": silver_count,
        "duplicates_count": dup_count,
        "output_path": silver_output
    }


def main():
    parser = argparse.ArgumentParser(description="Bronze to Silver Spark Transformation Job")
    parser.add_argument("--table", default="all", help="Target table name or 'all'")
    parser.add_argument("--bronze-path", default="s3a://lakehouse/bronze/clarizen", help="Base Bronze Parquet path")
    parser.add_argument("--silver-path", default="s3a://lakehouse/silver/epm", help="Base Silver Parquet path")
    parser.add_argument("--contract-dir", default=os.path.join(ROOT_DIR, "data_type"), help="Path to schema contracts")
    parser.add_argument("--spark-master", default="local[4]", help="Spark Master URL")
    args = parser.parse_args()

    spark = SparkSessionFactory.create(
        app_name="BronzeToSilverETL",
        master=args.spark_master,
        enable_hive=False
    )

    tables_to_run = SUPPORTED_TABLES if args.table.lower() == "all" else [args.table.lower()]
    results = {}

    for table in tables_to_run:
        res = process_bronze_to_silver(
            spark=spark,
            table_name=table,
            bronze_base_path=args.bronze_path,
            silver_base_path=args.silver_path,
            contract_dir=args.contract_dir
        )
        results[table] = res

    logger.info("=== Bronze to Silver Processing Summary ===")
    for t, summary in results.items():
        logger.info(f"  {t}: {summary.get('status')} (Bronze: {summary.get('raw_count', 0)} -> Silver: {summary.get('silver_count', 0)})")

    spark.stop()


if __name__ == "__main__":
    main()
