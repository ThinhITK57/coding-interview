import os
import gzip
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from storage.trino_ddl_generator import TrinoDDLGenerator

logger = logging.getLogger(__name__)


class TriStorageSink:
    """
    Tri-Storage Persistence Engine for Enterprise Data Lake.

    Sinks data to 3 distinct locations:
        1. MinIO Raw Backup: Immutable gzip JSON files (Disaster Recovery & Replay source).
        2. Trino DB 1: hive.personal_raw.<table_name> (Raw sandbox for DE/DS algorithm development).
        3. Trino DB 2: hive.global_clean.<table_name> (Curated, flattened, validated production tables).

    Deep module design:
        Interface: persist_all(table_name, raw_records, clean_df, ...)
        Implementation: Handles atomic compression, S3A/local path resolution,
        idempotent partition overwrite in Spark 2.3.2, and auto-generates Trino DDL scripts.
    """

    def __init__(
        self,
        spark_session=None,
        base_s3_uri: str = "s3a://lakehouse",
        local_base_dir: str = "./data",
        trino_catalog: str = "hive",
        personal_schema: str = "personal_raw",
        clean_schema: str = "global_clean",
        ddl_output_dir: str = "./generated_ddl",
    ):
        """Initialize Tri-Storage Sink.

        Args:
            spark_session: Active SparkSession (compatible with Spark 2.3.2 / Zeppelin Livy).
            base_s3_uri: Base S3A URI for MinIO/Cloud lake (e.g. s3a://lakehouse).
            local_base_dir: Local filesystem fallback path.
            trino_catalog: Trino catalog name (default: "hive").
            personal_schema: Schema for DB 1 sandbox (default: "personal_raw").
            clean_schema: Schema for DB 2 clean warehouse (default: "global_clean").
            ddl_output_dir: Directory to export Trino .sql DDL scripts.
        """
        self.spark = spark_session
        self.base_s3_uri = base_s3_uri.rstrip("/")
        self.local_base_dir = local_base_dir.rstrip("/")
        self.trino_catalog = trino_catalog
        self.personal_schema = personal_schema
        self.clean_schema = clean_schema
        self.ddl_output_dir = ddl_output_dir

        self.ddl_generator = TrinoDDLGenerator(
            catalog=trino_catalog,
            personal_schema=personal_schema,
            clean_schema=clean_schema,
            base_s3_location=f"{self.base_s3_uri}/warehouse",
        )

    def sink_minio_raw_backup(
        self,
        table_name: str,
        raw_records: List[Dict[str, Any]],
        batch_id: str,
        partition_date: Optional[str] = None,
    ) -> str:
        """Sink 1: Write immutable raw JSON.gz backup file.

        Args:
            table_name: Name of the entity/table.
            raw_records: List of raw JSON dictionaries directly from API.
            batch_id: Unique batch execution ID.
            partition_date: Date string YYYY-MM-DD. Defaults to today.

        Returns:
            str: Path to the generated raw backup file.
        """
        if partition_date is None:
            partition_date = datetime.utcnow().strftime("%Y-%m-%d")

        dt = datetime.strptime(partition_date, "%Y-%m-%d")
        year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

        # Local directory path for raw backup
        backup_dir = os.path.join(
            self.local_base_dir,
            "raw_backup",
            table_name.lower(),
            f"year={year}",
            f"month={month}",
            f"day={day}",
        )
        os.makedirs(backup_dir, exist_ok=True)

        backup_file = os.path.join(backup_dir, f"{batch_id}.json.gz")

        # Compress and write records
        with gzip.open(backup_file, "wt", encoding="utf-8") as gz_file:
            json.dump({
                "table_name": table_name,
                "batch_id": batch_id,
                "ingest_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "record_count": len(raw_records),
                "records": raw_records,
            }, gz_file, ensure_ascii=False)

        logger.info(json.dumps({
            "event": "minio_raw_backup_written",
            "table": table_name,
            "file": backup_file,
            "record_count": len(raw_records),
            "size_bytes": os.path.getsize(backup_file),
        }))

        return backup_file

    def sink_trino_personal_raw(
        self,
        table_name: str,
        raw_df,
        partition_col: str = "ingest_date",
        partition_val: Optional[str] = None,
    ) -> str:
        """Sink 2: Write Parquet into Trino DB 1 (personal_raw sandbox).

        Args:
            table_name: Table name.
            raw_df: Spark DataFrame containing raw records.
            partition_col: Partition column name.
            partition_val: Partition value (e.g. current date).

        Returns:
            str: Target storage path.
        """
        import pyspark.sql.functions as F

        target_path = os.path.join(
            self.local_base_dir,
            "warehouse",
            self.personal_schema,
            table_name.lower(),
        )

        df_to_write = raw_df
        if partition_val and partition_col not in df_to_write.columns:
            df_to_write = df_to_write.withColumn(partition_col, F.lit(partition_val))

        (
            df_to_write.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .partitionBy(partition_col)
            .save(target_path)
        )

        logger.info(json.dumps({
            "event": "trino_personal_raw_written",
            "table": table_name,
            "schema": self.personal_schema,
            "target_path": target_path,
        }))

        return target_path

    def sink_trino_global_clean(
        self,
        table_name: str,
        clean_df,
        partition_col: str = "ingest_date",
        partition_val: Optional[str] = None,
    ) -> str:
        """Sink 3: Write Parquet into Trino DB 2 (global_clean curated warehouse).

        Args:
            table_name: Table name.
            clean_df: Flattened and validated Spark DataFrame.
            partition_col: Partition column name.
            partition_val: Partition value.

        Returns:
            str: Target storage path.
        """
        import pyspark.sql.functions as F

        target_path = os.path.join(
            self.local_base_dir,
            "warehouse",
            self.clean_schema,
            table_name.lower(),
        )

        df_to_write = clean_df
        if partition_val and partition_col not in df_to_write.columns:
            df_to_write = df_to_write.withColumn(partition_col, F.lit(partition_val))

        (
            df_to_write.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .partitionBy(partition_col)
            .save(target_path)
        )

        logger.info(json.dumps({
            "event": "trino_global_clean_written",
            "table": table_name,
            "schema": self.clean_schema,
            "target_path": target_path,
        }))

        return target_path

    def persist_all(
        self,
        table_name: str,
        raw_records: List[Dict[str, Any]],
        clean_df,
        batch_id: str,
        raw_df=None,
        partition_date: Optional[str] = None,
        partition_col: str = "ingest_date",
    ) -> Dict[str, str]:
        """Execute all 3 sinks simultaneously and generate Trino DDL.

        Args:
            table_name: Target table/entity name.
            raw_records: List of raw JSON dicts from API.
            clean_df: Cleaned Spark DataFrame.
            batch_id: Batch identifier.
            raw_df: Optional raw Spark DataFrame. If None, clean_df is used as base for DB 1.
            partition_date: Date string YYYY-MM-DD.
            partition_col: Partition column name.

        Returns:
            dict: Summary of paths for backup, DB 1, DB 2, and the Trino DDL file.
        """
        if partition_date is None:
            partition_date = datetime.utcnow().strftime("%Y-%m-%d")

        # 1. Sink 1: MinIO Raw Backup (JSON.gz)
        backup_file = self.sink_minio_raw_backup(
            table_name=table_name,
            raw_records=raw_records,
            batch_id=batch_id,
            partition_date=partition_date,
        )

        # 2. Sink 2: Trino DB 1 Personal Raw (Parquet)
        target_raw_df = raw_df if raw_df is not None else clean_df
        db1_path = self.sink_trino_personal_raw(
            table_name=table_name,
            raw_df=target_raw_df,
            partition_col=partition_col,
            partition_val=partition_date,
        )

        # 3. Sink 3: Trino DB 2 Global Clean (Parquet)
        db2_path = self.sink_trino_global_clean(
            table_name=table_name,
            clean_df=clean_df,
            partition_col=partition_col,
            partition_val=partition_date,
        )

        # 4. Generate Trino DDL script (.sql) ready for trino.exe
        ddl_file = self.ddl_generator.export_sql_file(
            output_path=self.ddl_output_dir,
            table_name=table_name,
            schema_or_fields=clean_df.schema,
            partition_col=partition_col,
        )

        return {
            "table_name": table_name,
            "minio_backup_file": backup_file,
            "trino_personal_raw_path": db1_path,
            "trino_global_clean_path": db2_path,
            "trino_ddl_sql_file": ddl_file,
        }
