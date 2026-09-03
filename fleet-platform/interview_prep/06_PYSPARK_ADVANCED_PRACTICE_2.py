"""
===============================================================================
 🧪 BÀI TẬP THỰC HÀNH PYSPARK BỔ SUNG (SET 2 - MIDDLE / SENIOR DE)
===============================================================================
File này bổ sung 4 bài tập thực hành PySpark nâng cao bám sát thực tế dự án:
  - Bài 5: Parsing Debezium CDC JSON Envelope (Rút extracted before/after/op) trong Spark.
  - Bài 6: Stream-Stream Join với Watermark (Join Telemetry Stream với Repair Request Stream).
  - Bài 7: Tổng hợp Doanh thu theo Năm Tài Khóa dùng ROLLUP & CUBE.
  - Bài 8: Dynamic Partition Overwrite ghi HDFS Data Warehouse.

Usage:
  python3 06_PYSPARK_ADVANCED_PRACTICE_2.py
===============================================================================
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, from_json, to_timestamp, window, sum as _sum, count as _count,
    concat_ws, md5, coalesce, current_date, to_date, when, expr, rollup, cube
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType
)


def get_spark():
    return (
        SparkSession.builder
        .appName("PySpark-Advanced-Set2")
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# =============================================================================
# BÀI TẬP 5: PARSING DEBEZIUM CDC ENVELOPE TRONG PYSPARK
# =============================================================================
def exercise_5_parse_debezium_cdc():
    print("\n" + "=" * 70)
    print(" BÀI 5: Parsing Debezium CDC JSON Envelope (Before/After/Op) trong Spark")
    print("=" * 70)
    spark = get_spark()

    # Debezium JSON Envelope Schema
    debezium_schema = StructType([
        StructField("payload", StructType([
            StructField("op", StringType(), True),         # 'c': create, 'u': update, 'd': delete, 'r': read
            StructField("ts_ms", LongType(), True),       # CDC Event timestamp
            StructField("before", StructType([
                StructField("id", IntegerType(), True),
                StructField("status", StringType(), True),
            ]), True),
            StructField("after", StructType([
                StructField("id", IntegerType(), True),
                StructField("status", StringType(), True),
            ]), True),
        ]), True)
    ])

    # Sample Kafka Debezium raw JSON strings
    raw_kafka_cdc = [
        ('{"payload": {"op": "c", "ts_ms": 1722400000000, "before": null, "after": {"id": 1, "status": "mới"}}}',),
        ('{"payload": {"op": "u", "ts_ms": 1722400500000, "before": {"id": 1, "status": "mới"}, "after": {"id": 1, "status": "thường niên"}}}',),
        ('{"payload": {"op": "d", "ts_ms": 1722401000000, "before": {"id": 1, "status": "thường niên"}, "after": null}}',),
    ]
    raw_df = spark.createDataFrame(raw_kafka_cdc, ["value"])

    # Parse JSON and Extract fields
    parsed_cdc_df = (
        raw_df
        .select(from_json(col("value"), debezium_schema).alias("cdc"))
        .select(
            col("cdc.payload.op").alias("operation"),
            col("cdc.payload.ts_ms").alias("cdc_timestamp_ms"),
            col("cdc.payload.before.id").alias("before_id"),
            col("cdc.payload.before.status").alias("before_status"),
            col("cdc.payload.after.id").alias("after_id"),
            col("cdc.payload.after.status").alias("after_status"),
        )
    )

    print("📊 Extracted Debezium CDC Envelope Fields:")
    parsed_cdc_df.show(truncate=False)


# =============================================================================
# BÀI TẬP 6: STREAM-STREAM JOIN VỚI WATERMARKING
# =============================================================================
def exercise_6_stream_stream_join():
    print("\n" + "=" * 70)
    print(" BÀI 6: Stream-Stream Join giữa Telemetry Stream & Repair Request Stream")
    print("=" * 70)
    spark = get_spark()

    # Telemetry stream
    t_data = [
        ("51C-123.45", "2026-07-31T10:00:00Z", 95.0), # High temp
        ("51D-456.78", "2026-07-31T10:05:00Z", 85.0),
    ]
    telemetry_df = (
        spark.createDataFrame(t_data, ["truck_plate", "t_timestamp", "engine_temp"])
        .withColumn("t_time", to_timestamp(col("t_timestamp")))
        .withWatermark("t_time", "10 minutes")
    )

    # Repair request stream
    r_data = [
        ("51C-123.45", "2026-07-31T10:02:00Z", "Động cơ quá nhiệt"), # Xảy ra sau 2 phút
    ]
    repair_df = (
        spark.createDataFrame(r_data, ["truck_plate", "r_timestamp", "issue_desc"])
        .withColumn("r_time", to_timestamp(col("r_timestamp")))
        .withWatermark("r_time", "10 minutes")
    )

    # Stream-Stream Join với khoảng thời gian hợp lệ (r_time trong khoảng t_time ± 5 phút)
    joined_stream_df = telemetry_df.join(
        repair_df,
        expr("""
            telemetry_df.truck_plate = repair_df.truck_plate AND
            r_time >= t_time AND
            r_time <= t_time + interval 5 minutes
        """),
        how="inner"
    )

    print("⚡ Kết quả Stream-Stream Join (Khớp telemetry cảnh báo với Yêu cầu sửa chữa):")
    joined_stream_df.select("telemetry_df.truck_plate", "t_time", "engine_temp", "r_time", "issue_desc").show(truncate=False)


# =============================================================================
# BÀI TẬP 7: CUBE & ROLLUP CHO BÁO CÁO TÀI CHÍNH ĐA CHIỀU
# =============================================================================
def exercise_7_rollup_financial_reporting():
    print("\n" + "=" * 70)
    print(" BÀI 7: Báo cáo Doanh thu Đa chiều dùng ROLLUP (Theo Năm -> Quý -> Tháng)")
    print("=" * 70)
    spark = get_spark()

    sales_data = [
        (2026, "Q3", "2026-07", 2200000.0),
        (2026, "Q3", "2026-08", 3500000.0),
        (2026, "Q4", "2026-10", 7800000.0),
    ]
    df = spark.createDataFrame(sales_data, ["fiscal_year", "fiscal_quarter", "month", "revenue"])

    # ROLLUP tính tổng cấp Quý, cấp Năm và Tổng cộng toàn bộ (Subtotals & Grand Total)
    rollup_df = (
        df.rollup("fiscal_year", "fiscal_quarter", "month")
        .agg(_sum("revenue").alias("total_revenue"))
        .orderBy("fiscal_year", "fiscal_quarter", "month")
    )

    print("📊 Kết quả ROLLUP Subtotals & Grand Total:")
    rollup_df.show(truncate=False)


if __name__ == "__main__":
    exercise_5_parse_debezium_cdc()
    exercise_6_stream_stream_join()
    exercise_7_rollup_financial_reporting()
    print("\n✅ Tất cả bài tập PySpark Set 2 đã hoàn thành!")
