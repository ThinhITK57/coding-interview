import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

class MultiTargetWarehouseSink:
    """
    Quản trị ghi đồng thời 3 đích đến trên nền tảng Spark 2.3.2 và Trino.
    """
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config

    def save_raw_backup_to_minio(self, df_raw_text: DataFrame):
        """Đích 1: Lưu trữ bất biến (Immutable Bronze) trên MinIO S3A"""
        logger.info("Writing raw backup to: %s", self.config.raw_backup_path)
        df_raw_text.write.mode("overwrite").text(self.config.raw_backup_path)

    def save_to_trino_dev_sandbox(self, df_flattened: DataFrame):
        """Đích 2: Lưu bảng raw đã làm phẳng vào Sandbox của bạn trên Trino"""
        logger.info("Writing to Dev Sandbox table: %s", self.config.dev_table_name)
        df_flattened.write.format("parquet").mode("overwrite").saveAsTable(self.config.dev_table_name)

    def save_to_dlq(self, df_dlq: DataFrame):
        """Ghi nhận dữ liệu lỗi vào Dead Letter Queue"""
        if not df_dlq.rdd.isEmpty():
            logger.warning("Writing invalid records to DLQ: %s", self.config.dlq_table_name)
            df_dlq.write.format("parquet").mode("append").saveAsTable(self.config.dlq_table_name)

    def upsert_to_trino_prod(self, df_clean_updates: DataFrame, business_pk: str):
        """
        Đích 3: Upsert vào Production Table khi Spark 2.3.2 KHÔNG có MERGE INTO.
        Áp dụng kỹ thuật: Left Anti Join + unionByName
        """
        prod_table = self.config.prod_table_name
        logger.info("Performing Upsert into Prod Table: %s", self.config.prod_table_name)

        table_exists = self.spark._jsparkSession.catalog().tableExists(prod_table)

        if not table_exists:
            df_clean_updates.write.format("parquet").mode("overwrite").saveAsTable(prod_table)
            return

        df_current_prod = self.spark.table(prod_table)

        # 1. Giữ lại những bản ghi cũ KHÔNG bị update trong đợt này
        df_unaffected = df_current_prod.join(
            df_clean_updates,
            on=business_pk,
            how="left_anti"
        )

        # 2. Hợp nhất dữ liệu mới nhất (Spark 2.3.0+ hỗ trợ unionByName)
        df_final_prod = df_unaffected.unionByName(df_clean_updates)

        # 3. Ghi đè vào bảng Prod
        df_final_prod.write.format("parquet").mode("overwrite").saveAsTable(prod_table)
