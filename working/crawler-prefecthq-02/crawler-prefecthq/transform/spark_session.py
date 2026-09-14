import logging
import json
import os
from config.env_loader import load_env_file


logger = logging.getLogger(__name__)


class SparkSessionFactory:
    @staticmethod
    def create(
        app_name="api_ingestion",
        master="local[*]",
        extra_config=None,
        minio_endpoint=None,
        minio_access_key=None,
        minio_secret_key=None,
        enable_hive=False
    ):
        load_env_file()
        
        from pyspark.sql import SparkSession
        minio_endpoint = (
                minio_endpoint
                or os.getenv(
            "MINIO_ENDPOINT",
            "http://127.0.0.1:9000"
        )
        )
        minio_access_key = (
                minio_access_key
                or os.getenv("MINIO_ACCESS_KEY")
        )

        minio_secret_key = (
                minio_secret_key
                or os.getenv("MINIO_SECRET_KEY")
        )

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.writeLegacyFormat", "true")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                minio_endpoint
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                minio_access_key
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                minio_secret_key
            )
            .config(
                "spark.hadoop.fs.s3a.path.style.access",
                "true"
            )
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "false"
            )
        )

        if enable_hive:
            try:
                builder = builder.enableHiveSupport()
            except Exception as e:
                logger.warning("enableHiveSupport failed (running without Hive metastore): %s", str(e))

        if extra_config:
            for key, value in extra_config.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()
        logger.info(json.dumps({
            "event": "spark_session_created",
            "app_name": app_name,
            "master": master,
            "spark_version": getattr(spark, "version", "2.3.2")
        }))
        return spark
    