import logging
import json

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """
    Factory for creating SparkSession instances compatible with Spark 2.3.2.

    Deep module design:
        Interface (small): create(app_name, master) -> SparkSession
        Implementation (deep): Configures Spark properties for Parquet/Snappy,
        timezone handling, and memory tuning.
    """

    @staticmethod
    def create(app_name="api_ingestion", master="local[*]", extra_config=None):
        """Create a configured SparkSession.

        Args:
            app_name: Spark application name.
            master: Spark master URL (local[*], yarn, spark://host:port).
            extra_config: Optional dict of additional Spark config key-value pairs.

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

        if extra_config:
            for key, value in extra_config.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()

        logger.info(json.dumps({
            "event": "spark_session_created",
            "app_name": app_name,
            "master": master,
            "spark_version": spark.version,
        }))

        return spark
