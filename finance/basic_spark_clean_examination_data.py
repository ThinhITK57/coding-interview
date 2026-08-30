from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

# Thiết lập logger chuẩn cho Production
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LogisticsETL")


def create_spark_session():
    return SparkSession.builder \
        .appName("Logistics-Data-Cleaning-Level1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def clean_logistics_data(spark, input_path, clean_output_path, dlq_output_path,
                         dq_threshold: float = 0.95):
    # ĐỊnh nghĩa Schema chặt chẽ
    schema = StructType([
        StructField("tracking_id", StringType(), False),
        StructField("sender_city", StringType(), True),
        StructField("receiver_city", StringType(), True),
        StructField("weight_kg", DoubleType(), True),
        StructField("ship_fee", DoubleType(), True),
        StructField("created_at", StringType(), True)
    ])

    logger.info(f"Reading data from Input path : {input_path}")
    raw_df = spark.read.schema(schema).json(input_path)  ## File input la json file
    # total_count = raw_df.count()

    # 2 . Chuẩn hóa chuỗi -- Trimming , Uppercase & Parse Date
    processed_df = (
        raw_df.withColumn("tracking_id", F.trim(F.upper(F.col("tracking_id")))) \
            .withColumn("sender_city", F.initcap(F.trim(F.col("sender_city")))) \
            .withColumn("receiver_city", F.initcap(F.trim(F.col("receiver_city")))) \
            .withColumn("created_timestamp",
                        F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn(
            "error_reasons",
            F.array_remove(
                F.array(
                    F.when(
                        F.col("tracking_id").isNull()
                        | (F.length(F.col("tracking_id")) == 0),
                        "MISSING_TRACKING_ID"
                    ),
                    F.when(
                        F.col("weight_kg").isNull() | (F.col("weight_kg") <= 0),
                        "INVALID_WEIGHT",
                    ),
                    F.when(
                        F.col("ship_fee").isNull() | (F.col("ship_fee") < 0),
                        "INVALID_SHIP_FEE",
                    ),
                    F.when(
                        F.col("created_timestamp").isNull(),
                        "INVALID_TIMESTAMP"
                    ),
                ),
                None
            ),
        )
            .withColumn("is_valid", F.size(F.col("error_reasons")) == 0)
    )
    ## Caching DataFrame để tránh tính toán lại DAG khi chia luồng Clean / DLQ
    processed_df.cache()
    try:
        # 3 Tính toán Metric Data quality trong 1 action duy nhất
        metrics = processed_df.groupBy("is_valid").count().collect()
        counts = {row["is_valid"]: row["count"] for row in metrics}

        clean_count = counts.get(True, 0)
        dlq_count = counts.get(False, 0)
        total_count = clean_count + dlq_count

        if total_count == 0:
            logger.warning("Input path is empty. Existing pipeline")
            return

        dq_score = clean_count / total_count
        logger.info(
            f"Metrics --> Total: {total_count} | Clean: {clean_count} {dq_score:.2%} | DLQ: {dlq_count}"
        )

        # 3. LUÔN GHI DLQ TRƯỚC (Dù DQ đạt hay không đạt ngưỡng)
        if dlq_count > 0:
            logger.warning(
                f"Writing {dlq_count} bad records to DLQ: {dlq_output_path}"
            )

            # Phân luồng DLQ Data
            dlq_df = (
                processed_df.filter(~F.col("is_valid"))
                .withColumn("error_reason", F.array_join(F.col("error_reasons"), ";"))
                .drop("is_valid", "error_reasons")
            )
            dlq_df.write.mode("append").json(dlq_output_path)

        # Cảnh báo / Chặn nếu Data Quality không đạt ngưỡng 95%
        if dq_score < dq_threshold:
            logger.error(
                f"CRITICAL: Data Quality ({dq_score:.2%}) below threshold ({dq_threshold:.2%})!"
            )
            raise ValueError(
                f"CRITICAL: Data Quality ({dq_score:.2%}) is below threshold ({dq_threshold:.2%})! Aborting pipeline."
            )

        # 4 . Phân luồng Clean Data
        clean_df = (
            processed_df.filter(F.col("is_valid"))
            .select(
                "tracking_id",
                "sender_city",
                "receiver_city",
                "weight_kg",
                "ship_fee",
                "created_timestamp",
            )
            .withColumn(
                "weight_category",
                F.when(F.col("weight_kg") < 1.0, "LIGHT")
                .when(
                    (F.col("weight_kg") >= 1.0) & (F.col("weight_kg") <= 5.0), "MEDIUM"
                )
                .otherwise("HEAVY"),
            )
            .withColumn("processed_at", F.current_timestamp())
        )

        # Ghi dữ liệu
        # Dùng repartition or Coalesce tránh lỗi Small Files khi ghi Parquet
        logger.info(f"Writing clean data to : {clean_output_path}")
        clean_df.repartition("receiver_city").write.mode("overwrite").partitionBy(
            "receiver_city"
        ).parquet(clean_output_path)
    finally:
        # Luôn luôn thu hồi bộ nhớ Cache bất kể code chạy thành công hay tung Exception
        logger.info("Releasing cached DataFrame memory...")
        processed_df.unpersist()


if __name__ == "__main__":
    spark = create_spark_session()
    clean_logistics_data(
        spark,
        "/tmp/raw_tms_logs.json",
        "/tmp/clean_tms_parquet",
        "/tmp/tms_dlq",
        dq_threshold=0.95
    )