import os
import gzip
import json

import logging

from datetime import datetime
from typing import Any, Dict, List, Optional

from storage.trino_ddl_generator import TrinoDDLGenerator
from storage.spark_sql_ddl_generator import SparkSQLDDLGenerator


logger = logging.getLogger(__name__)

class TriStorageSink:
    def __init__(
            self,
            spark_session=None,
            base_s3_uri: str = "s3a://lakehouse",
            local_base_dir: str = "./data",
            trino_catalog: str = "hive",
            personal_schema: str = "personal_raw",
            clean_schema: str = "global_clean",
            ddl_output_dir: str = "./generated_ddl",
            warehouse_uri: Optional[str] = None,
            ddl_location_base: Optional[str] = None,
            partitioned: bool = False
            ):
        self.spark = spark_session
        self.base_s3_uri = base_s3_uri.rstrip("/")
        self.local_base_dir = local_base_dir.rstrip("/")
        self.trino_catalog = trino_catalog
        self.personal_schema = personal_schema
        self.clean_schema = clean_schema
        self.ddl_output_dir = ddl_output_dir

        # He thong dich chua ho tro partition. Khi tat, ingest_date duoc ghi
        # thanh cot DATE binh thuong trong file parquet thay vi thanh thu muc
        # ingest_date=... - neu khong, bang khong khai partition se khong nhin
        # thay file nao.
        self.partitioned = partitioned

        # Noi Spark GHI parquet. Mac dinh la local; dat s3a://... de day thang
        # len MinIO (roi tai ve va upload tay len Ambari).
        self.warehouse_uri = (warehouse_uri or f"{self.local_base_dir}/warehouse").rstrip("/")

        # LOCATION ghi trong DDL. Tach rieng khoi warehouse_uri vi file duoc
        # chuyen tay: Spark ghi len MinIO, con bang lai tro vao duong dan tren
        # Ambari. Khong dat thi coi nhu hai noi trung nhau.
        self.ddl_location_base = (ddl_location_base or self.warehouse_uri).rstrip("/")

        self.ddl_generator = TrinoDDLGenerator(
            catalog=trino_catalog,
            personal_schema=personal_schema,
            clean_schema=clean_schema,
            base_s3_location=self.ddl_location_base,
            partitioned=self.partitioned
        )

        # Bang duoc tao bang Spark SQL tren Zeppelin, sau do co job dong bo sang
        # Trino. Sinh ca hai dialect tu cung mot contract nen chung khong the
        # mo ta hai schema khac nhau.
        self.spark_ddl_generator = SparkSQLDDLGenerator(
            personal_schema=personal_schema,
            clean_schema=clean_schema,
            base_location=self.ddl_location_base,
            partitioned=self.partitioned
        )

        logger.info(json.dumps({
            "event": "tri_storage_sink_initialized",
            "warehouse_uri": self.warehouse_uri,
            "ddl_location_base": self.ddl_location_base
        }))

    def _target_path(self, schema_name: str, table_name: str) -> str:
        return f"{self.warehouse_uri}/{schema_name}/{table_name.lower()}"

    def _ddl_source(self, table_name: str, fallback_df):
        """Contract tu data_type/*.sql neu co, khong thi quay ve schema cua DataFrame.

        Uu tien contract vi schema suy luan tu DataFrame doi theo tung batch
        (cot toan null -> void, so nguyen -> bigint), khien DDL Trino lech dan
        khoi file Parquet da ghi.
        """
        from transform.schema_contract import SchemaContract

        if SchemaContract.has_contract(table_name):
            return SchemaContract.load(table_name)

        logger.warning(json.dumps({
            "event": "ddl_from_inferred_schema",
            "table": table_name,
            "detail": "khong co data_type contract - DDL sinh tu schema Spark suy luan"
        }))
        return fallback_df.schema

    def export_ddl(
            self,
            table_name: str,
            fallback_df=None,
            partition_col: str = "ingest_date"
    ) -> Dict[str, Optional[str]]:
        """Sinh ca DDL Spark SQL (chay tren Zeppelin) lan DDL Trino."""
        source = self._ddl_source(table_name, fallback_df)

        trino_path = self.ddl_generator.export_sql_file(
            output_path=self.ddl_output_dir,
            table_name=table_name,
            schema_or_fields=source,
            partition_col=partition_col
        )

        spark_path = None
        if hasattr(source, "trino_columns"):
            spark_path = self.spark_ddl_generator.export_sql_file(
                output_path=self.ddl_output_dir,
                table_name=table_name,
                contract=source,
                partition_col=partition_col
            )
        else:
            logger.warning(json.dumps({
                "event": "spark_ddl_skipped",
                "table": table_name,
                "detail": "khong co contract - chi sinh duoc DDL Trino"
            }))

        return {"trino_ddl": trino_path, "spark_ddl": spark_path}

    def sink_minio_raw_backup(
            self,
            table_name: str,
            raw_records: List[Dict[str, Any]],
            batch_id: str,
            partition_date: Optional[str] = None
    ) -> str:
        if partition_date is None:
            partition_date = datetime.utcnow().strftime("%Y-%m-%d")

        dt = datetime.strptime(partition_date, "%Y-%m-%d")
        year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

        backup_dir = os.path.join(
            self.local_base_dir,
            "raw_backup",
            table_name.lower(),
            f"year={year}",
            f"month={month}",
            f"day={day}"
        )
        os.makedirs(backup_dir, exist_ok=True)

        backup_file = os.path.join(backup_dir, f"{batch_id}.json.gz")

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
            "size_bytes": os.path.getsize(backup_file)
        }))

        return backup_file

    def sink_trino_personal_raw(
            self,
            table_name: str,
            raw_df,
            partition_col: str = "ingest_date",
            partition_val: Optional[str] = None,
    )-> str:
        import pyspark.sql.functions as F
        target_path = self._target_path(self.personal_schema, table_name)
        df_to_write = raw_df
        if partition_val and partition_col not in df_to_write.columns:
            # Phai cast DATE: khi khong partition, cot nay nam THAT trong file
            # parquet, con DDL khai DATE. F.lit() tra ve string -> lech kieu.
            df_to_write = df_to_write.withColumn(
                partition_col, F.lit(partition_val).cast("date")
            )

        writer = (
            df_to_write.write
            .mode("append")
            .format("parquet")
            .option("compression", "snappy")
        )
        if self.partitioned:
            writer = writer.partitionBy(partition_col)
        writer.save(target_path)
        logger.info(json.dumps({
            "event": "trino_personal_raw_written",
            "table": table_name,
            "schema": self.personal_schema,
            "target_path": target_path
        }))
        return target_path

    def sink_trino_global_clean(
            self,
            table_name: str,
            clean_df,
            partition_col: str = "ingest_date",
            partition_val: Optional[str] = None,
    )-> str:
        import pyspark.sql.functions as F

        target_path = self._target_path(self.clean_schema, table_name)

        df_to_write = clean_df

        if partition_val and partition_col not in df_to_write.columns:
            # Phai cast DATE: khi khong partition, cot nay nam THAT trong file
            # parquet, con DDL khai DATE. F.lit() tra ve string -> lech kieu.
            df_to_write = df_to_write.withColumn(
                partition_col, F.lit(partition_val).cast("date")
            )

        writer = (
            df_to_write.write
            .mode("append")
            .format("parquet")
            .option("compression", "snappy")
        )
        if self.partitioned:
            writer = writer.partitionBy(partition_col)
        writer.save(target_path)

        logger.info(json.dumps({
            "event": "trino_global_clean_written",
            "table": table_name,
            "schema": self.clean_schema,
            "target_path": target_path
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
    )-> Dict[str, str]:
        if partition_date is None:
            partition_date = datetime.utcnow().strftime("%Y-%m-%d")

        backup_file = self.sink_minio_raw_backup(
            table_name=table_name,
            raw_records=raw_records,
            batch_id=batch_id,
            partition_date=partition_date
        )

        target_raw_df = raw_df if raw_df is not None else clean_df

        # DDL duoi day mo ta ca hai bang, nen personal_raw va global_clean phai
        # co cung schema. Neu lech thi Trino se doc duoc mot bang va vo bang kia.
        if raw_df is not None and raw_df.schema != clean_df.schema:
            logger.warning(json.dumps({
                "event": "raw_clean_schema_mismatch",
                "table": table_name,
                "detail": "raw_df va clean_df khac schema - DDL sinh ra chi dung cho mot trong hai",
                "raw_columns": len(raw_df.columns),
                "clean_columns": len(clean_df.columns)
            }))

        db1_path = self.sink_trino_personal_raw(
            table_name=table_name,
            raw_df=target_raw_df,
            partition_col=partition_col,
            partition_val=partition_date
        )

        db2_path = self.sink_trino_global_clean(
            table_name=table_name,
            clean_df=clean_df,
            partition_col=partition_col,
            partition_val=partition_date
        )


        ddl_file = self.ddl_generator.export_sql_file(
            output_path=self.ddl_output_dir,
            table_name=table_name,
            schema_or_fields=self._ddl_source(table_name, clean_df),
            partition_col=partition_col
        )

        return {
            "table_name": table_name,
            "minio_backup_file": backup_file,
            "trino_personal_raw_path": db1_path,
            "trino_global_clean_path": db2_path,
            "trino_ddl_sql_file": ddl_file
        }
