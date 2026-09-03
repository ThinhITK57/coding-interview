import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from enterprise_elt.config.pipeline_config import PipelineConfig
from enterprise_elt.clients.api_client import ResilientAPIIngestor
from enterprise_elt.processors.json_flattener import SparkJSONFlattener
from enterprise_elt.processors.quality_engine import DataQualityEngine
from enterprise_elt.processors.version_manager import VersionDeduplicator
from enterprise_elt.sinks.warehouse_sink import MultiTargetWarehouseSink

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("EnterpriseELT")

def init_spark_2_3(config: PipelineConfig) -> SparkSession:
    """Khởi tạo SparkSession tối ưu cho Spark 2.3.2 kết nối MinIO S3A"""
    builder = SparkSession.builder \
        .appName(f"ELT_Pipeline_{config.get('domain_target')}") \
        .config("spark.sql.parquet.writeLegacyFormat", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.fs.s3a.endpoint", config.get("minio_endpoint")) \
        .config("spark.hadoop.fs.s3a.access.key", config.get("minio_access_key")) \
        .config("spark.hadoop.fs.s3a.secret.key", config.get("minio_secret_key")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    builder = builder.enableHiveSupport()
    return builder.getOrCreate()

def main():
    config = PipelineConfig()
    logger.info("Running ELT for target [%s] in [%s] mode.", config.get("domain_target"), config.get("env"))

    spark = init_spark_2_3(config)

    try:
        api_client = ResilientAPIIngestor(rate_limit_rps=config.get("rate_limit_rps"))
        sample_api_url = f"https://api.internal.company.com/{config.get('domain_target')}"
        
        payload = api_client.fetch_endpoint(sample_api_url)
        api_client.close()

        raw_json_str = json.dumps(payload.get("data", []))
        df_raw_text = spark.createDataFrame([(raw_json_str,)], ["value"])

        sink = MultiTargetWarehouseSink(spark, config)

        # 1. MinIO Raw Backup
        sink.save_raw_backup_to_minio(df_raw_text)

        # 2. Làm phẳng JSON phi cấu trúc
        df_parsed = spark.read.json(df_raw_text.rdd.map(lambda r: r.value))
        df_flattened = SparkJSONFlattener.flatten(df_parsed)

        df_flattened = df_flattened \
            .withColumn("_ingested_at", F.current_timestamp()) \
            .withColumn("_batch_id", F.lit(config.get("batch_id")))

        df_flattened.persist()

        # 3. Trino Dev Sandbox
        sink.save_to_trino_dev_sandbox(df_flattened)

        # 4. Data Quality Rules & DLQ
        rules = [
            {"code": "ERR_NULL_PK", "cond": F.col("id").isNull()},
            {"code": "ERR_NEGATIVE_AMOUNT", "cond": F.col("amount") < 0}
        ]
        quality_engine = DataQualityEngine(rules)
        df_valid, df_dlq = quality_engine.validate_and_split(df_flattened)

        sink.save_to_dlq(df_dlq)

        dlq_count = df_dlq.count()
        valid_count = df_valid.count()
        total_count = dlq_count + valid_count

        if total_count > 0:
            error_ratio = float(dlq_count) / total_count
            if error_ratio > config.get("dlq_threshold_ratio"):
                logger.error("DLQ Error ratio %.2f%% exceeded threshold %.2f%%!",
                             error_ratio * 100, config.get("dlq_threshold_ratio") * 100)
                raise ValueError(f"Pipeline breached DLQ threshold: {error_ratio:.2%}")

        # 5. Khử trùng lặp & Upsert Trino Prod
        df_clean_dedup = VersionDeduplicator.deduplicate(
            df_valid, 
            business_key="id", 
            timestamp_col="_ingested_at"
        )
        
        sink.upsert_to_trino_prod(df_clean_dedup, business_pk="id")
        logger.info("Pipeline executed successfully across all 3 targets.")

    finally:
        if 'df_flattened' in locals():
            df_flattened.unpersist()
        spark.stop()

if __name__ == "__main__":
    main()
