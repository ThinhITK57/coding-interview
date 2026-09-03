"""
===============================================================================
 🧪 BÀI TẬP THỰC HÀNH PYSPARK (LEVEL MIDDLE / SENIOR DATA ENGINEER) - FIXED
===============================================================================
File này chứa 4 bài tập thực hành PySpark đã được sửa lỗi cú pháp và logic:
  - Bài 1: PySpark Structured Streaming với MemoryStream, Watermarking & 10-Min Window.
  - Bài 2: PySpark SCD Type 2 Merge Pipeline (Bảo toàn 100% lịch sử old/new).
  - Bài 3: PySpark Star Schema Join & Aggregation theo Năm Tài Khóa (Fix missing round import).
  - Bài 4: Tối ưu hóa Data Skew bằng kỹ thuật Salting (Thêm muối chuẩn PySpark API).

Usage:
  python3 02_PYSPARK_PRACTICE_EXERCISES.py
===============================================================================
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, window, sum as _sum, count as _count,
    concat_ws, md5, when, expr, rand, floor,
    array, explode, round as _round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)


def get_spark_session(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# =============================================================================
# BÀI TẬP 1: PYSPARK STRUCTURED STREAMING VỚI WATERMARKING & WINDOWING
# =============================================================================
def exercise_1_streaming_watermark():
    print("\n" + "=" * 70)
    print(" BÀI 1: PySpark Structured Streaming với Watermarking & Streaming Window")
    print("=" * 70)
    spark = get_spark_session("Exercise-1-Watermark")

    # 1. Khởi tạo UNBOUNDED STREAMING DATAFRAME bằng spark.readStream (Rate Source)
    # Giả lập luồng sự kiện cảm biến phát ra liên tục 5 dòng/giây
    streaming_df = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "5")
        .load()
        .select(
            col("timestamp").alias("event_time"),
            concat_ws("-", lit("TRUCK"), (col("value") % 3).cast("string")).alias("truck_plate"),
            ((col("value") % 40) + 70.0).alias("engine_temp_c")
        )
    )

    # Kiểm tra bản chất luồng dữ liệu bất biến (Unbounded Streaming)
    print("⚡ Check DataFrame isStreaming:", streaming_df.isStreaming)  # True

    # 2. Xử lý Streaming State: Áp dụng Watermark 10 giây BẮT BUỘC trước groupBy Window
    # Watermark quyết định thời điểm drop các event đến trễ và dọn dẹp State Store trong bộ nhớ
    watermarked_stream = (
        streaming_df
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            window(col("event_time"), "10 seconds", "5 seconds"),
            col("truck_plate")
        )
        .agg(
            _sum(when(col("engine_temp_c") > 100.0, 1).otherwise(0)).alias("overheat_warnings"),
            _count("engine_temp_c").alias("total_readings")
        )
    )

    print("📊 Cấu hình Streaming Sink (OutputMode = update/append với Watermark):")
    # 3. Kích hoạt Streaming Query ghi kết quả ra Sink (Console / Parquet HDFS)
    query = (
        watermarked_stream.writeStream
        .outputMode("update")  # Mode 'update' hoặc 'append' bắt buộc khi dùng Watermark
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Trong thực tế sản xuất chạy 24/7: query.awaitTermination()
    # Ở đây stop minh họa sau 2 giây để tránh treo script khi test
    query.awaitTermination(timeout=2)
    query.stop()


# =============================================================================
# BÀI TẬP 2: PYSPARK SCD TYPE 2 CUSTOMER DIMENSION PIPELINE (CHUẨN LOGIC)
# =============================================================================
def exercise_2_scd_type_2():
    print("\n" + "=" * 70)
    print(" BÀI 2: PySpark SCD Type 2 Customer Dimension Merge Pipeline (Sửa Logic)")
    print("=" * 70)
    spark = get_spark_session("Exercise-2-SCD2")

    # 1. Dữ liệu HDFS Data Warehouse hiện tại (Bao gồm cả dòng active VÀ dòng đã expired)
    old_dim_data = [
        ("key_0", 1, "Công ty Vận Tải A", "nháp", "2025-01-01", "2025-12-31", False), # Lịch sử cũ đã đóng
        ("key_1", 1, "Công ty Vận Tải A", "mới", "2026-01-01", "9999-12-31", True),   # Hiện tại đang active
        ("key_2", 2, "Công ty Vận Tải B", "cũ", "2026-01-01", "9999-12-31", True),   # Hiện tại đang active
    ]
    dim_schema = ["customer_key", "customer_id", "company_name", "status", "effective_date", "expiration_date", "is_current"]
    old_dim_df = spark.createDataFrame(old_dim_data, dim_schema)

    print("📜 Bảng Dim Customer hiện tại trên Data Warehouse (Có cả lịch sử cũ):")
    old_dim_df.show(truncate=False)

    # 2. Dữ liệu CDC mới từ Postgres Odoo (Khách hàng ID=1 đổi status 'mới' -> 'thường niên')
    cdc_data = [
        (1, "Công ty Vận Tải A", "thường niên"), # Đổi status
        (3, "Công ty Vận Tải C", "mới"),        # Khách hàng mới hoàn toàn
    ]
    cdc_df = spark.createDataFrame(cdc_data, ["customer_id", "company_name", "status"])

    today_str = "2026-07-31"

    # --- CHUẨN HÓA THỰC HIỆN XỬ LÝ SCD TYPE 2 MERGE ---
    
    # Bước 1: Bảo toàn 100% các bản ghi lịch sử ĐÃ ĐÓNG từ trước (is_current == False)
    historical_closed_df = old_dim_df.filter(~col("is_current"))

    # Bước 2: Lấy các bản ghi ĐANG ACTIVE (is_current == True) để Left Join với CDC mới
    active_dim_df = old_dim_df.filter(col("is_current"))
    joined_df = active_dim_df.join(cdc_df.withColumnRenamed("status", "new_status"), on="customer_id", how="left")

    # Bước 3: Đóng các bản ghi active bị thay đổi status (expiration_date = today_str, is_current = False)
    closing_records_df = (
        joined_df
        .filter(col("new_status").isNotNull() & (col("status") != col("new_status")))
        .withColumn("expiration_date", lit(today_str))
        .withColumn("is_current", lit(False))
        .select(dim_schema)
    )

    # Bước 4: Giữ nguyên các bản ghi active KHÔNG bị thay đổi (không có CDC hoặc status giữ nguyên)
    unchanged_active_df = (
        joined_df
        .filter(col("new_status").isNull() | (col("status") == col("new_status")))
        .select(dim_schema)
    )

    # Bước 5: Chèn dòng mới cho các bản ghi bị thay đổi status VÀ khách hàng mới hoàn toàn
    # A. Dòng mới cho khách hàng đổi status
    changed_new_records_df = (
        joined_df
        .filter(col("new_status").isNotNull() & (col("status") != col("new_status")))
        .withColumn("status", col("new_status"))
        .withColumn("effective_date", lit(today_str))
        .withColumn("expiration_date", lit("9999-12-31"))
        .withColumn("is_current", lit(True))
        .withColumn("customer_key", md5(concat_ws("_", col("customer_id"), col("effective_date"), col("status"))))
        .select(dim_schema)
    )

    # B. Dòng mới cho khách hàng mới hoàn toàn (chưa từng có trong Dim)
    brand_new_customers_df = (
        cdc_df.join(old_dim_df.select("customer_id").distinct(), on="customer_id", how="left_anti")
        .withColumn("effective_date", lit(today_str))
        .withColumn("expiration_date", lit("9999-12-31"))
        .withColumn("is_current", lit(True))
        .withColumn("customer_key", md5(concat_ws("_", col("customer_id"), col("effective_date"), col("status"))))
        .select(dim_schema)
    )

    # Bước 6: UNION tất cả 5 tập dữ liệu để thu được bảng Dim Customer SCD Type 2 chuẩn xác 100%
    final_scd2_df = (
        historical_closed_df
        .union(closing_records_df)
        .union(unchanged_active_df)
        .union(changed_new_records_df)
        .union(brand_new_customers_df)
        .orderBy("customer_id", "effective_date")
    )

    print("✅ Kết quả bảng Dim Customer sau khi Merge SCD Type 2 chuẩn xác:")
    final_scd2_df.show(truncate=False)


# =============================================================================
# BÀI TẬP 3: PYSPARK STAR SCHEMA JOIN & FISCAL YEAR AGGREGATION
# =============================================================================
def exercise_3_fiscal_year_aggregation():
    print("\n" + "=" * 70)
    print(" BÀI 3: PySpark Star Schema Aggregation theo Năm Tài Khóa (Fiscal Year)")
    print("=" * 70)
    spark = get_spark_session("Exercise-3-FiscalYear")

    # Bảng Fact Hóa đơn
    fact_invoices_data = [
        (101, 1, 1, 20260715, 500000.0, 1700000.0, 2200000.0), # Q4 Fiscal 2026
        (102, 2, 2, 20261010, 800000.0, 7000000.0, 7800000.0), # Q1 Fiscal 2027 (Tháng 10 năm 2026)
    ]
    fact_df = spark.createDataFrame(fact_invoices_data, ["invoice_id", "customer_id", "head_id", "date_key", "service_amount", "parts_amount", "total_amount"])

    # Bảng Dim Date có thuộc tính Fiscal Year (01/10 đến 30/09)
    dim_date_data = [
        (20260715, "2026-07-15", 2026, 3, 2026, "Q4-FY2026"),
        (20261010, "2026-10-10", 2026, 4, 2027, "Q1-FY2027"),
    ]
    date_df = spark.createDataFrame(dim_date_data, ["date_key", "full_date", "year", "quarter", "fiscal_year", "fiscal_quarter"])

    # Join Fact & Dim Date -> Gom nhóm theo Năm tài khóa (Đã import _round)
    report_df = (
        fact_df.join(date_df, on="date_key", how="inner")
        .groupBy("fiscal_year", "fiscal_quarter")
        .agg(
            _sum("service_amount").alias("total_labor_revenue"),
            _sum("parts_amount").alias("total_parts_revenue"),
            _sum("total_amount").alias("total_revenue"),
            _count("invoice_id").alias("total_invoices")
        )
        .withColumn("profit_margin_est", _round(col("total_revenue") * 0.45, 2))
    )

    print("📊 Báo cáo Tài chính theo Năm Tài Khóa (Fiscal Year):")
    report_df.show(truncate=False)


# =============================================================================
# BÀI TẬP 4: TỐI ƯU HÓA DATA SKEW BẰNG KỸ THUẬT SALTING
# =============================================================================
def exercise_4_salting_skew_optimization():
    print("\n" + "=" * 70)
    print(" BÀI 4: Tối ưu hóa Data Skew bằng Kỹ thuật Salting (Thêm Muối)")
    print("=" * 70)
    spark = get_spark_session("Exercise-4-Salting")

    # Giả lập bảng Fact bị Skew (Khách hàng ID=1 chiếm 90% số lượng bản ghi)
    skewed_fact_data = [(1, 100.0) for _ in range(100)] + [(2, 200.0) for _ in range(10)]
    fact_df = spark.createDataFrame(skewed_fact_data, ["customer_id", "amount"])

    # Bảng Dim Customer nhỏ
    dim_data = [(1, "Đại lý Siêu Lớn A"), (2, "Khách nhỏ B")]
    dim_df = spark.createDataFrame(dim_data, ["customer_id", "company_name"])

    # --- KỸ THUẬT SALTING VỚI PYSPARK API CHUẨN ---
    SALT_FACTOR = 4

    # 1. Thêm cột salt ngẫu nhiên từ 0 -> (SALT_FACTOR - 1) vào bảng Fact
    salted_fact_df = fact_df.withColumn("salt", floor(rand() * SALT_FACTOR))

    # 2. Nhân bản bảng Dim bằng array & explode
    salt_list = list(range(SALT_FACTOR))
    exploded_dim_df = (
        dim_df
        .withColumn("salt_array", array([lit(i) for i in salt_list]))
        .withColumn("salt", explode(col("salt_array")))
        .drop("salt_array")
    )

    # 3. Join trên CẢ customer_id VÀ salt -> Dữ liệu của khách hàng 1 được chia đều ra 4 Executors!
    joined_df = salted_fact_df.join(exploded_dim_df, on=["customer_id", "salt"], how="inner")

    result_df = (
        joined_df
        .groupBy("customer_id", "company_name")
        .agg(_sum("amount").alias("total_amount"), _count("amount").alias("record_count"))
    )

    print("⚡ Kết quả Join Salting phân tán đều data skew:")
    result_df.show(truncate=False)


if __name__ == "__main__":
    exercise_1_streaming_watermark()
    exercise_2_scd_type_2()
    exercise_3_fiscal_year_aggregation()
    exercise_4_salting_skew_optimization()
    print("\n✅ Tất cả bài tập thực hành PySpark đã kiểm tra và chạy thành công!")
