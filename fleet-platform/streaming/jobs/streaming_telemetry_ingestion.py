"""
Fleet Platform — Spark Structured Streaming Telemetry & Repair Ingestion
========================================================================
Job này chạy 24/7 tiêu thụ dữ liệu từ 2 Kafka topics:
  1. `truck-telemetry`  → Ghi Parquet xuống HDFS `/fleet-datalake/raw/telemetry/year=YYYY/month=MM/day=DD/`
  2. `repair-request`   → Ghi Parquet xuống HDFS `/fleet-datalake/raw/repair_requests/year=YYYY/month=MM/day=DD/`

Tính năng quan trọng:
  - Explicit Schema Parsing (`from_json`) chống Schema Drift & Malformed JSON.
  - Extracted partition columns (`year`, `month`, `day`) từ ISO-8601 timestamp.
  - HDFS Checkpointing đảm bảo Exactly-Once / At-Least-Once delivery semantics khi Spark job restart.
  - OutputMode: `append` cho immutable event stream.

Chạy job:
  spark-submit \
    --master spark://master:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /path/to/streaming_telemetry_ingestion.py
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    year,
    month,
    dayofmonth,
    date_format,
    current_timestamp,
)

# Import schemas (có thể import trực tiếp nếu đặt chung thư mục hoặc PYTHONPATH)
try:
    from schemas.telemetry_schemas import TELEMETRY_SCHEMA, REPAIR_REQUEST_SCHEMA
except ImportError:
    # Inline fallback schemas nếu module không nằm trên python path
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
    )
    TELEMETRY_SCHEMA = StructType([
        StructField("truck_id", StringType(), True),
        StructField("truck_plate", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("altitude_m", DoubleType(), True),
            StructField("heading_deg", DoubleType(), True),
        ]), True),
        StructField("metrics", StructType([
            StructField("speed_kmh", DoubleType(), True),
            StructField("engine_rpm", IntegerType(), True),
            StructField("engine_temp_c", DoubleType(), True),
            StructField("fuel_level_pct", DoubleType(), True),
            StructField("oil_pressure_psi", DoubleType(), True),
            StructField("battery_voltage", DoubleType(), True),
            StructField("odometer_km", DoubleType(), True),
        ]), True),
        StructField("dtc_codes", ArrayType(StringType()), True),
        StructField("status", StringType(), True),
    ])

    REPAIR_REQUEST_SCHEMA = StructType([
        StructField("request_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("truck_plate", StringType(), True),
        StructField("requested_at", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]), True),
        StructField("issue_category", StringType(), True),
        StructField("issue_description", StringType(), True),
        StructField("urgency", StringType(), True),
        StructField("dtc_codes", ArrayType(StringType()), True),
    ])


# =============================================================
# Config Defaults
# =============================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "master:9092,slave1:9092,slave2:9092")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://master:9000")
BASE_DATALAKE_PATH = f"{HDFS_NAMENODE}/fleet-datalake"

TELEMETRY_CHECKPOINT = f"{BASE_DATALAKE_PATH}/checkpoints/telemetry"
TELEMETRY_OUTPUT = f"{BASE_DATALAKE_PATH}/raw/telemetry"

REPAIR_CHECKPOINT = f"{BASE_DATALAKE_PATH}/checkpoints/repair_requests"
REPAIR_OUTPUT = f"{BASE_DATALAKE_PATH}/raw/repair_requests"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("FleetPlatform-StreamingTelemetryIngestion")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def process_telemetry_stream(spark):
    """Stream 1: Tiêu thụ truck-telemetry → Ghi HDFS Parquet."""
    print(f"[Streaming 1] Reading from Kafka topic: truck-telemetry...")

    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "truck-telemetry")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Cast Kafka binary value to String and parse JSON
    parsed_df = (
        raw_kafka_df
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
        .select(from_json(col("json_str"), TELEMETRY_SCHEMA).alias("data"), col("kafka_timestamp"))
        .select("data.*", "kafka_timestamp")
    )

    # Transform timestamp and add partition columns
    transformed_df = (
        parsed_df
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("event_time", col("event_time").cast("timestamp"))
        # Parse partition columns from event_time, fallback to kafka_timestamp if null
        .withColumn("effective_time", col("event_time"))
        .withColumn("year", date_format(col("effective_time"), "yyyy"))
        .withColumn("month", date_format(col("effective_time"), "MM"))
        .withColumn("day", date_format(col("effective_time"), "dd"))
        .withColumn("ingested_at", current_timestamp())
    )

    # Write to HDFS Data Lake (Parquet)
    telemetry_query = (
        transformed_df.writeStream
        .format("parquet")
        .option("path", TELEMETRY_OUTPUT)
        .option("checkpointLocation", TELEMETRY_CHECKPOINT)
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return telemetry_query


def process_repair_stream(spark):
    """Stream 2: Tiêu thụ repair-request → Ghi HDFS Parquet."""
    print(f"[Streaming 2] Reading from Kafka topic: repair-request...")

    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "repair-request")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_kafka_df
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
        .select(from_json(col("json_str"), REPAIR_REQUEST_SCHEMA).alias("data"), col("kafka_timestamp"))
        .select("data.*", "kafka_timestamp")
    )

    transformed_df = (
        parsed_df
        .withColumn("requested_timestamp", to_timestamp(col("requested_at")))
        .withColumn("year", date_format(col("requested_timestamp"), "yyyy"))
        .withColumn("month", date_format(col("requested_timestamp"), "MM"))
        .withColumn("day", date_format(col("requested_timestamp"), "dd"))
        .withColumn("ingested_at", current_timestamp())
    )

    repair_query = (
        transformed_df.writeStream
        .format("parquet")
        .option("path", REPAIR_OUTPUT)
        .option("checkpointLocation", REPAIR_CHECKPOINT)
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return repair_query


def main():
    print("=" * 70)
    print(" Fleet Platform — Spark Structured Streaming Ingestion")
    print(f" Kafka Brokers : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f" HDFS DataLake : {BASE_DATALAKE_PATH}")
    print("=" * 70)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    q1 = process_telemetry_stream(spark)
    q2 = process_repair_stream(spark)

    print("\n✅ Streaming queries started successfully. Listening for Kafka events...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
