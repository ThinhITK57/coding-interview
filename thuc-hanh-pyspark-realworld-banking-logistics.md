# BỘ BÀI TẬP & THỰC HÀNH CODE PYSPARK CHUYÊN SÂU (BANKING & LOGISTICS)

> **Mục tiêu:** Chuyển hóa kiến thức lý thuyết Spark thành kỹ năng lập trình production thực tế. Các bài tập được phân cấp độ từ Cơ bản đến Nâng cao, dựa trên bài toán thực tế của các Ngân hàng (VPBank, MB Bank) và Công ty Logistics (Vietnam Post, E-commerce).
> Mỗi bài tập đều có **Yêu cầu nghiệp vụ (Business Requirements)**, **Kịch bản dữ liệu (Data Scenario)**, **Code mẫu Production-Ready (Có logging, Schema validation, Error Handling, Performance Tuning)** và **Giải thích kỹ thuật**.

---

## MỤC LỤC
* [CẤP ĐỘ 1: XỬ LÝ & LÀM SẠCH DỮ LIỆU CƠ BẢN (FOUNDATION ETL)](#cấp-độ-1-xử-lý--làm-sạch-dữ-liệu-cơ-bản)
  * [Bài 1 (Logistics): Làm sạch & Kiểm tra Data Quality cho luồng vận đơn (TMS/WMS)](#bài-1-logistics-làm-sạch--kiểm-tra-data-quality-cho-luồng-vận-đơn)
  * [Bài 2 (Banking): Mã hóa dữ liệu nhạy cảm (PII Masking) & Phân loại giao dịch đáng ngờ](#bài-2-banking-mã-hóa-dữ-liệu-nhạy-cảm-pii-masking--phân-loại-giao-dịch-đáng-ngờ)
* [CẤP ĐỘ 2: MÔ HÌNH HÓA DỮ LIỆU & PIPELINE TRUNG CẤP (INTERMEDIATE ETL & DWH)](#cấp-độ-2-mô-hình-hóa-dữ-liệu--pipeline-trung-cấp)
  * [Bài 3 (Banking): Xây dựng Pipeline SCD Type 2 cho biến động số dư tài khoản dùng Delta Lake](#bài-3-banking-xây- dựng-pipeline-scd-type-2-dùng-delta-lake)
  * [Bài 4 (Logistics): Đo lường SLA giao hàng & Thống kê doanh thu trượt 7 ngày (Moving Average Window)](#bài-4-logistics-đo-lường-sla-giao-hàng--thống-kê-doanh-thu-trượt-7-ngày)
* [CẤP ĐỘ 3: TỐI ƯU HỆ THỐNG & XỬ LÝ QUY MÔ CỰC HẠN (ADVANCED TUNING & STREAMING)](#cấp-độ-3-tối-ưu-hệ-thống--xử-lý-quy-mô-cực-hạn)
  * [Bài 5 (Banking): Kỹ thuật Salting giải quyết Data Skew khi Join tài khoản Merchant doanh nghiệp lớn](#bài-5-banking-kỹ-thuật-salting-giải-quyết-data-skew)
  * [Bài 6 (Logistics/Streaming): Processing CDC Event Stream cập nhật trạng thái đơn hàng thời gian thực](#bài-6-logisticsstreaming-processing-cdc-event-stream-thời-gian-thực)

---

## CẤP ĐỘ 1: XỬ LÝ & LÀM SẠCH DỮ LIỆU CƠ BẢN

### Bài 1 (Logistics): Làm sạch & Kiểm tra Data Quality cho luồng vận đơn

#### 1. Yêu cầu Nghiệp vụ
Hệ thống Quản lý Vận tải (TMS - Transport Management System) của công ty Logistics nhận dữ liệu vận đơn thô từ nhiều bưu cục gửi về. Dữ liệu gặp các vấn đề:
* Định dạng mã bưu cục/địa chỉ bị dư thừa khoảng trắng, chữ hoa/thường không đồng nhất.
* Cột trọng lượng (`weight_kg`) và phí vận chuyển (`shipping_fee`) chứa giá trị `NULL` hoặc âm do lỗi nhập liệu.
* Yêu cầu: Làm sạch dữ liệu, lọc bỏ bản ghi lỗi vào thư mục Dead Letter Queue (DLQ), tính toán tổng phí và gắn nhãn phân loại kích thước đơn hàng (`LIGHT`, `MEDIUM`, `HEAVY`). Đảm bảo Data Quality > 95%.

#### 2. PySpark Production Code
```python
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
```

---

### Bài 2 (Banking): Mã hóa dữ liệu nhạy cảm (PII Masking) & Phân loại giao dịch đáng ngờ

#### 1. Yêu cầu Nghiệp vụ
Ngân hàng yêu cầu xử lý dữ liệu giao dịch tài khoản (Core Banking Transactions) trước khi đưa vào Data Lake phân tích:
* Mã hóa thông tin cá nhân (PII - Personally Identifiable Information): Tên khách hàng phải giữ lại chữ cái đầu và cuộn bằng dấu `*` (ví dụ `Nguyen Van A` -> `N****** V** A`), Số tài khoản chỉ giữ 4 số cuối (ví dụ `1903456789` -> `******789`).
* Gắn nhãn giao dịch có dấu hiệu bất thường (Fraud/Anomaly Flag): Giao dịch chuyển tiền trên 500,000,000 VND diễn ra trong khoảng từ 00:00:00 đến 04:00:00 sáng.

#### 2. PySpark Production Code
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, TimestampType, StructType, StructField

def create_bank_spark_session():
    return SparkSession.builder \
        .appName("Banking-PII-Masking-Level1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# Native Expression Masking (Tốc độ cao hơn UDF)
def mask_account_number_col(col_name):
    # Lấy 4 ký tự cuối, chèn '******' vào phía trước
    return F.concat(F.lit("******"), F.substring(F.col(col_name), -4, 4))

def process_banking_transactions(df):
    # 1. Masking PII
    masked_df = df \
        .withColumn("masked_account_no", mask_account_number_col("account_number")) \
        .withColumn("masked_customer_name", 
                    F.regexp_replace(F.col("customer_name"), r"(?<=\w)\w", "*"))
    
    # 2. Xử lý thời gian & Gắn nhãn Anomaly (Cảnh báo rửa tiền / Giao dịch đêm)
    transformed_df = masked_df \
        .withColumn("txn_hour", F.hour(F.col("transaction_time"))) \
        .withColumn(
            "is_suspicious_night_txn",
            F.when(
                (F.col("amount") >= 500000000.0) & 
                (F.col("txn_hour") >= 0) & 
                (F.col("txn_hour") < 4),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    
    # Loại bỏ cột PII thô trước khi ghi ra Data Lake
    final_df = transformed_df.drop("account_number", "customer_name")
    return final_df

# Unit Test đơn giản
if __name__ == "__main__":
    spark = create_bank_spark_session()
    
    data = [
        ("TXN1001", "190333444555", "Nguyen Van An", 600000000.0, "2026-07-23 01:30:00"),
        ("TXN1002", "970412345678", "Tran Thi Binh", 50000000.0, "2026-07-23 14:20:00")
    ]
    schema = StructType([
        StructField("txn_id", StringType()),
        StructField("account_number", StringType()),
        StructField("customer_name", StringType()),
        StructField("amount", DoubleType()),
        StructField("transaction_time", StringType())
    ])
    
    df = spark.createDataFrame(data, schema).withColumn("transaction_time", F.to_timestamp("transaction_time"))
    res = process_banking_transactions(df)
    res.show(truncate=False)
```

---

## CẤP ĐỘ 2: MÔ HÌNH HÓA DỮ LIỆU & PIPELINE TRUNG CẤP

### Bài 3 (Banking): Xây dựng Pipeline SCD Type 2 dùng Delta Lake

#### 1. Yêu cầu Nghiệp vụ
Quản lý thông tin trạng thái tài khoản Ngân hàng (Account Status Dimension) trong Data Warehouse. Khi khách hàng thay đổi hạn mức tín dụng (`credit_limit`) hoặc nâng hạng thẻ (`tier`), hệ thống cần lưu lại lịch sử (Slowly Changing Dimension - SCD Type 2):
* Cập nhật bản ghi cũ: gán `is_current = False`, `effective_end_date = CURRENT_DATE()`.
* Thêm bản ghi mới: gán `is_current = True`, `effective_start_date = CURRENT_DATE()`, `effective_end_date = NULL`.
* Sử dụng cú pháp `MERGE INTO` của **Delta Lake** để đảm bảo tính nguyên tử (ACID Transaction).

#### 2. PySpark Production Code (Delta Lake Merge)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import os

def create_delta_spark():
    return SparkSession.builder \
        .appName("Banking-SCD2-DeltaLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def upsert_scd_type2_delta(spark, updates_df, delta_table_path):
    """
    Thực hiện SCD Type 2 Merge sử dụng Delta Lake.
    """
    # Hash cột cần theo dõi thay đổi để so sánh nhanh
    track_cols = ["credit_limit", "customer_tier"]
    
    stg_df = updates_df.withColumn(
        "row_hash", 
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in track_cols]), 256)
    )

    # Kiểm tra nếu bảng Delta chưa tồn tại thì khởi tạo lần đầu
    if not DeltaTable.isDeltaTable(spark, delta_path=delta_table_path):
        print("Delta Table does not exist. Initializing Table...")
        init_df = stg_df \
            .withColumn("effective_start_date", F.current_date()) \
            .withColumn("effective_end_date", F.lit(None).cast("date")) \
            .withColumn("is_current", F.lit(True))
        
        init_df.write.format("delta").mode("overwrite").save(delta_path)
        return

    # Nếu bảng Delta đã tồn tại -> Thực hiện thuật toán Merge 2 giai đoạn (Two-pass Merge)
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    
    # 1. Giai đoạn 1: Merge để CLOSE các bản ghi cũ bị thay đổi (is_current = False)
    # Join staging với target hiện tại theo natural key (account_id)
    target_df = delta_table.toDF().filter("is_current = true")
    
    # Tìm các account_id vừa có trong target vừa có trong staging nhưng hash khác nhau
    rows_to_update = stg_df.join(
        target_df,
        on="account_id",
        how="inner"
    ).filter(stg_df.row_hash != target_df.row_hash) \
     .select(stg_df.account_id)

    # 2. Chuẩn bị tập dữ liệu nạp mới bao gồm:
    # - Bản ghi hoàn toàn mới chưa từng có trong Target
    # - Bản ghi mới tạo ra để thay thế bản ghi vừa bị Close ở bước trên
    stg_df_to_insert = stg_df.alias("stg")

    # Thực thi Merge trên Delta Table
    delta_table.alias("target").merge(
        source=stg_df.alias("src"),
        condition="target.account_id = src.account_id AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.row_hash != src.row_hash",
        set={
            "is_current": "false",
            "effective_end_date": "current_date()"
        }
    ).execute()

    # Insert các bản ghi mới (gồm bản ghi hoàn toàn mới + bản ghi mới cho dòng vừa expired)
    new_records_df = stg_df \
        .withColumn("effective_start_date", F.current_date()) \
        .withColumn("effective_end_date", F.lit(None).cast("date")) \
        .withColumn("is_current", F.lit(True))
    
    # Chỉ insert những bản ghi chưa tồn tại dưới dạng is_current=true trong Target
    new_records_to_append = new_records_df.join(
        delta_table.toDF().filter("is_current = true"),
        on=["account_id", "row_hash"],
        how="left_anti"
    )

    new_records_to_append.write.format("delta").mode("append").save(delta_table_path)
    print("SCD Type 2 Merge Completed Successfully.")

```

---

### Bài 4 (Logistics): Đo lường SLA giao hàng & Thống kê doanh thu trượt 7 ngày

#### 1. Yêu cầu Nghiệp vụ
Phòng Vận hành Vietnam Post cần báo cáo hiệu năng bưu cục (Hub Performance):
1. **Thời gian hoàn thành đơn hàng (Order Lead Time & SLA):** Tính số giờ từ lúc nhận đơn (`created_at`) đến khi giao thành công (`delivered_at`). Nếu > 48 giờ đối với đơn nội thành thì đánh dấu `SLA_Breached = True`.
2. **Doanh thu trượt 7 ngày (7-Day Moving Average Revenue):** Tính doanh thu trung bình trượt 7 ngày gần nhất cho từng bưu cục (`hub_id`) tại mỗi ngày ghi nhận.

#### 2. PySpark Production Code (Window Functions)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_logistics_analytics_spark():
    return SparkSession.builder \
        .appName("Logistics-SLA-MovingAverage-Level2") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

def process_hub_performance(df_orders):
    # 1. Tính SLA Lead Time (Số giờ giao hàng)
    # Chuyển đổi timestamp sang Unix Epoch (seconds) để tính chênh lệch giờ
    df_lead_time = df_orders.withColumn(
        "lead_time_hours",
        (F.unix_timestamp("delivered_at") - F.unix_timestamp("created_at")) / 3600.0
    ).withColumn(
        "is_sla_breached",
        F.when((F.col("order_type") == "URBAN") & (F.col("lead_time_hours") > 48.0), True)
         .when((F.col("order_type") == "REGIONAL") & (F.col("lead_time_hours") > 120.0), True)
         .otherwise(False)
    )

    # 2. Tổng hợp Doanh thu theo Hub và Ngày (Daily Hub Revenue)
    df_daily_hub = df_lead_time \
        .withColumn("delivery_date", F.to_date("delivered_at")) \
        .groupBy("hub_id", "delivery_date") \
        .agg(
            F.sum("shipping_fee").alias("daily_revenue"),
            F.count("order_id").alias("total_orders"),
            F.sum(F.when(F.col("is_sla_breached"), 1).otherwise(0)).alias("breached_orders")
        )

    # 3. Định nghĩa Window Cửa sổ trượt 7 ngày (7-Day Moving Window)
    # Chuyển delivery_date thành số ngày kể từ Epoch để dùng rangeBetween chuẩn xác
    df_daily_hub = df_daily_hub.withColumn("date_days", F.datediff("delivery_date", F.lit("1970-01-01")))

    # 7 ngày = 6 ngày trước đó + ngày hiện tại (rangeBetween -6 đến 0)
    window_7d = Window.partitionBy("hub_id") \
        .orderBy("date_days") \
        .rangeBetween(-6, 0)

    df_final = df_daily_hub.withColumn(
        "moving_avg_revenue_7d",
        F.round(F.avg("daily_revenue").over(window_7d), 2)
    ).withColumn(
        "moving_total_orders_7d",
        F.sum("total_orders").over(window_7d)
    ).drop("date_days")

    return df_final
```

---

## CẤP ĐỘ 3: TỐI ƯU HỆ THỐNG & XỬ LÝ QUY MÔ CỰC HẠN

### Bài 5 (Banking): Kỹ thuật Salting giải quyết Data Skew khi Join

#### 1. Kịch bản Thực tế & Nghẽn Cổ Chai
Ngân hàng có bảng `Transactions` (500 triệu dòng) cần Join với bảng `Accounts` (10 triệu dòng).
* **Vấn đề Data Skew:** Cột `account_id` của các tài khoản doanh nghiệp lớn (như Ví điện tử, Cổng thanh toán Lazada/Shopee, Tập đoàn) có tới 50 triệu bản ghi giao dịch trùng khóa `account_id`.
* Khi Spark thực hiện Shuffle Hash Join hoặc Sort-Merge Join, **toàn bộ 50 triệu dòng này bị đẩy về duy nhất 1 Executor (Single Partition)**. Task đó chạy 3 tiếng không xong (Straggler Task) hoặc bị crash OOM, trong khi 99% Executor khác đã hoàn thành và ngồi chờ.

#### 2. Kỹ thuật Salting (Thêm Muối)
* **Bước 1:** Thêm một số ngẫu nhiên từ `0` đến `N-1` (ví dụ `N=10`) vào khóa join của bảng bị lệch (Bảng Transactions) $\rightarrow$ Khóa mới: `account_id_salted = account_id + "_" + random(0, 9)`.
* **Bước 2:** Nhân bản (Explode) bảng Dimension (Bảng Accounts) lên `N` lần với các muối từ `0` đến `N-1` tương ứng $\rightarrow$ Khóa mới: `account_id_salted`.
* **Bước 3:** Thực hiện Join trên `account_id_salted`. Dữ liệu 50 triệu dòng bị lệch giờ được phân tán đều ra 10 Executor khác nhau xử lý song song!

#### 3. PySpark Salting Production Code
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_high_perf_spark():
    return SparkSession.builder \
        .appName("Banking-Salting-DataSkew-Level3") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()

def join_skewed_transactions(spark, df_transactions, df_accounts, num_salts=10):
    """
    Thực hiện Salting Join giữa Transactions (Skewed) và Accounts.
    """
    print(f"Applying Salting Technique with Salt Factor = {num_salts}...")

    # 1. Bảng Skewed (Transactions): Thêm Salt ngẫu nhiên từ 0 đến num_salts - 1
    df_txn_salted = df_transactions.withColumn(
        "salt_key", 
        F.floor(F.rand() * num_salts)
    ).withColumn(
        "salted_account_id",
        F.concat_ws("_", F.col("account_id"), F.col("salt_key"))
    )

    # 2. Bảng Dimension (Accounts): Nhân bản (Explode) lên num_salts lần
    # Tạo array [0, 1, 2, ..., num_salts - 1]
    salts_array = F.array([F.lit(i) for i in range(num_salts)])
    
    df_acc_exploded = df_accounts \
        .withColumn("salt_key", F.explode(salts_array)) \
        .withColumn(
            "salted_account_id",
            F.concat_ws("_", F.col("account_id"), F.col("salt_key"))
        )

    # 3. Perform Join trên salted_account_id
    # Dữ liệu bị lệch được chia nhỏ đều ra 10 partitions khác nhau!
    joined_df = df_txn_salted.join(
        df_acc_exploded,
        on="salted_account_id",
        how="inner"
    )

    # 4. Clean up các cột phụ muối sau khi Join
    final_df = joined_df.drop("salted_account_id", "salt_key")
    return final_df
```

---

### Bài 6 (Logistics/Streaming): Processing CDC Event Stream cập nhật trạng thái đơn hàng thời gian thực

#### 1. Yêu cầu Nghiệp vụ
Ứng dụng di động shipper gửi các sự kiện cập nhật trạng thái đơn hàng (CDC events từ Kafka topic `shipment_events`).
* Dữ liệu JSON gửi về dưới dạng chuỗi nén byte trong Kafka `value`.
* Cần parse JSON schema, xử lý sự kiện đến muộn (Late-arriving events) bằng **Watermarking (10 phút)**.
* Tính tổng số đơn hàng giao thành công (`DELIVERED`) theo từng Bưu cục (`hub_id`) trong cửa sổ thời gian 5 phút (Tumbling Window).
* Ghi kết quả vào Delta Lake / Sink với checkpointing an toàn.

#### 2. PySpark Structured Streaming Code
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

def start_kafka_cdc_streaming():
    spark = SparkSession.builder \
        .appName("Logistics-CDC-StructuredStreaming-Level3") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    # 1. Khai báo JSON Schema của CDC Event từ Kafka
    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("order_id", StringType()),
        StructField("hub_id", StringType()),
        StructField("status", StringType()), # CREATED, PICKED_UP, IN_TRANSIT, DELIVERED
        StructField("event_time", StringType())
    ])

    # 2. Đọc Stream từ Apache Kafka
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "shipment_events") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parse JSON & Chuyển đổi Timestamp
    parsed_stream = kafka_stream_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(F.from_json("json_str", event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))

    # 4. Áp dụng Watermark 10 phút để xử lý Late-Arriving Data & Windowing 5 phút
    windowed_counts = parsed_stream \
        .withWatermark("event_timestamp", "10 minutes") \
        .filter(F.col("status") == "DELIVERED") \
        .groupBy(
            F.window(F.col("event_timestamp"), "5 minutes"),
            F.col("hub_id")
        ) \
        .agg(F.count("order_id").alias("delivered_count")) \
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("hub_id"),
            F.col("delivered_count")
        )

    # 5. Ghi Stream ra Console hoặc Delta Lake với Checkpoint
    query = windowed_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", "/tmp/spark_streaming_checkpoint") \
        .trigger(processingTime="30 seconds") \
        .start()

    print("Streaming Query Started. Awaiting Termination...")
    query.awaitTermination()

if __name__ == "__main__":
    # Để chạy bài này cần có cụm Kafka đang hoạt động ở localhost:9092
    pass
```

---

## TỔNG KẾT BẢNG KỸ NĂNG CẦN CHUẨN BỊ KHI ĐI PHỎNG VẤN

| Vùng Kiến Thức | Mức Độ Cần Đạt | Lỗi Thường Gặp Cần Tránh |
| :--- | :--- | :--- |
| **Data Cleaning & Quality** | Thành thạo Schema validation, Regex, DLQ splitting, Null handling bằng native Spark. | Dùng Python `apply()` hoặc UDF thay vì hàm native của Spark SQL. |
| **Window Functions** | Làm chủ `moving average`, `lead/lag`, `dense_rank`, `rangeBetween` vs `rowsBetween`. | Quên `partitionBy` gây gom toàn bộ dữ liệu vào 1 partition duy nhất (OOM). |
| **Delta Lake & DWH** | Hiểu bản chất `MERGE INTO`, `SCD Type 2`, `ACID Transactions`, Transaction Log JSON. | Ghi đè thủ công file Parquet gây mất nhất quán dữ liệu concurrent read. |
| **Optimization & Skew** | Nắm vững kỹ thuật **Salting**, Broadcast Join threshold, AQE, `repartition` vs `coalesce`. | Đổi `shuffle.partitions` bừa bãi mà không tính toán dung lượng dữ liệu. |
| **Structured Streaming** | Nắm vững `Watermarking`, `Tumbling/Sliding Window`, `Output modes` (append, update, complete). | Không khai báo `checkpointLocation` dẫn đến mất vị trí offset khi stream bị crash. |
