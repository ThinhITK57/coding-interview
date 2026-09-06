import logging
import json

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """
    Factory for creating SparkSession instances compatible with Spark 2.3.2 and Zeppelin Livy.

    Deep module design:
        Interface (small): create(...) -> SparkSession
        Implementation (deep): Configures Spark properties for Parquet/Snappy,
        timezone handling, dynamic partition overwrite, S3A MinIO filesystem properties,
        and optional Hive Metastore support.
    """

    @staticmethod
    def create(
        app_name="api_ingestion",
        master="local[*]",
        extra_config=None,
        minio_endpoint=None,
        minio_access_key=None,
        minio_secret_key=None,
        enable_hive=False,
    ):
        """Create or get a configured SparkSession.

        Args:
            app_name: Spark application name.
            master: Spark master URL (local[*], yarn, spark://host:port).
            extra_config: Optional dict of additional Spark config key-value pairs.
            minio_endpoint: S3A endpoint (e.g. "http://minio:9000").
            minio_access_key: S3A access key.
            minio_secret_key: S3A secret key.
            enable_hive: If True, calls enableHiveSupport().

        Returns:
            SparkSession instance.
        """
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.writeLegacyFormat", "true")
        )

        # MinIO S3A configuration if provided
        if minio_endpoint:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
                .config("spark.hadoop.fs.s3a.access.key", minio_access_key or "")
                .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key or "")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            )

        if enable_hive:
            try:
                builder = builder.enableHiveSupport()
            except Exception as e:
                logger.warning("enableHiveSupport failed (running without Hive Metastore): %s", e)

        if extra_config:
            for key, value in extra_config.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()

        logger.info(json.dumps({
            "event": "spark_session_created",
            "app_name": app_name,
            "master": master,
            "spark_version": getattr(spark, "version", "2.3.2"),
        }))

        return spark
