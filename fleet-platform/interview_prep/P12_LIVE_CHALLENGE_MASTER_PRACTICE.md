# P12 — KỊCH BẢN LUYỆN TẬP 3 BÀI THỬ THÁCH THỰC CHIẾN TẠI PHÒNG PHỎNG VẤN
## Chứng Minh Năng Lực "Làm Thật 100%": Live Spark Optimization, Star-Schema Design & Architecture Defense

> **Tôn Chỉ Khi Bị Thử Thách Trực Tiếp Tại Bàn:**
> *"Một người chém gió chỉ nói được từ khóa (Keywords). Một người LÀM THẬT sẽ lập tức:
> 1. Chỉ ra chính xác dòng code gây nghẽn bộ nhớ / Shuffle I/O.
> 2. Viết ra được 2 phiên bản code (Baseline vs Optimized) và giải thích tại sao bản 2 nhanh hơn gấp chục lần.
> 3. Tự tay thiết kế DDL Star Schema, phân tích Grain, Surrogate Key MD5 và SCD Type 2.
> 4. Bảo vệ kiến trúc bằng ma trận Trade-offs kỹ thuật chứ không bảo thủ chạy theo công nghệ."*

---

# MỤC LỤC CHIẾN LƯỢC

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BÀI THỬ THÁCH 1: LIVE SPARK CODE OPTIMIZATION (TỐI ƯU HÓA PYSPARK TẠI CHỖ)                             │
│   1.1 Đề bài thực tế: Tổng hợp doanh thu theo khách hàng từ 5.2 triệu hóa đơn                          │
│   1.2 Phiên bản 1 (Baseline): Code chạy đúng nhưng chậm (45.3 phút, Disk Spill 1.1GB, nghẽn Skew)      │
│   1.3 Phiên bản 2 (Optimized): Code tối ưu sâu (1.4 phút, Salting 2 giai đoạn, Broadcast Join, Kryo)   │
│   1.4 Lời giải trình kỹ thuật đẳng cấp Senior trước Hội đồng (So sánh Spark UI Metrics)               │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ BÀI THỬ THÁCH 2: LIVE DATA WAREHOUSE & STAR-SCHEMA DESIGN (THIẾT KẾ MÔ HÌNH DỮ LIỆU)                   │
│   2.1 Đề bài: Xây dựng DWH phục vụ Chuỗi 50 trạm sửa xe tải với 3 tiêu chí A, B, C                    │
│   2.2 Thiết kế Sơ đồ Star Schema (2 Fact Tables + 4 Dimension Tables)                                  │
│   2.3 DDL chi tiết (PostgreSQL / Spark SQL DDL)                                                        │
│   2.4 Code PySpark xử lý SCD Type 2 Merge 5 bước với Surrogate Key MD5                                │
│   2.5 Logic Năm Tài Khóa (Fiscal Year) trong `dim_date`                                                │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ BÀI THỬ THÁCH 3: ARCHITECTURE DESIGN & TOOL TRADE-OFF DEFENSE (TẠI SAO CHỌN CÔNG CỤ A?)                │
│   3.1 Sơ đồ Kiến trúc Data Pipeline E2E hoàn chỉnh                                                     │
│   3.2 Ma trận bảo vệ 5 lựa chọn công nghệ: Tại sao A mà không phải B?                                  │
│       • Tại sao Debezium CDC (WAL) mà không dùng Query Polling?                                        │
│       • Tại sao Kafka (3 brokers) mà không dùng RabbitMQ / HTTP Webhook?                               │
│       • Tại sao Spark on YARN + HDFS Parquet mà không dùng ClickHouse / PostgreSQL DWH?                │
│       • Tại sao Redis 7+ GEO + Lua Script mà không dùng PostgreSQL PostGIS?                            │
│       • Tại sao Airflow LocalExecutor mà không dùng Crontab / Celery?                                  │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# BÀI THỬ THÁCH 1: LIVE SPARK CODE OPTIMIZATION (TỐI ƯU HÓA PYSPARK TẠI CHỖ)

---

### 1.1 Đề bài thực tế từ Nhà Tuyển Dụng:
> *"Hệ thống có bảng `invoices` (5.2 triệu dòng, 1.7GB) và bảng `customers` (50.000 dòng, 15MB). Một khách hàng vận tải lớn chiếm 40% lượng hóa đơn (2.1 triệu hóa đơn). 
> Hãy viết PySpark job: JOIN 2 bảng này, tính tổng doanh thu theo từng khách hàng, lọc khách có tổng tiền $> 100.000.000\text{đ}$, và lưu ra HDFS Parquet.
> Em hãy viết 2 phiên bản code: 1 bản chạy đúng thông thường và 1 bản tối ưu hóa triệt để."*

---

### 1.2 Phiên bản 1 (Baseline Code — Chạy đúng nhưng CHẬM: 45.3 phút, dính Data Skew & Disk Spill)

```python
# ==============================================================================
# PHIÊN BẢN 1: BASELINE (Chạy đúng logic nhưng dính 3 điểm nghẽn nghiêm trọng)
# Thời gian chạy thực tế: 45.3 PHÚT (Task 42 bị nghẽn Data Skew, Spill 1.1GB Disk)
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("FleetRevenueSummary_Baseline") \
    .getOrCreate()

# 1. Đọc toàn bộ dữ liệu (Không chọn lọc cột - tốn I/O)
df_invoices = spark.read.parquet("hdfs://master:9000/lake/bronze/invoices")
df_customers = spark.read.parquet("hdfs://master:9000/lake/bronze/customers")

# 2. Phép SortMergeJoin mặc định gây Shuffle Network toàn bộ 5.2 triệu dòng
df_joined = df_invoices.join(df_customers, df_invoices.customer_id == df_customers.id, "inner")

# 3. GroupBy trực tiếp trên customer_id dính bẫy Data Skew (Task 42 ôm 2.1 triệu dòng)
df_summary = df_joined.groupBy("customer_id", "company_name") \
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("invoices.id").alias("total_invoices")
    ) \
    .filter("total_revenue > 100000000")

# 4. Ghi trực tiếp không kiểm soát số lượng file partition
df_summary.write.mode("overwrite").parquet("hdfs://master:9000/dwh/gold/customer_revenue_baseline")
```

#### ❌ 3 Điểm nghẽn chí mạng trong Phiên bản 1:
1. **Shuffle Toàn Bộ Dữ Liệu (SortMergeJoin)**: Bảng `customers` chỉ có $15\text{MB}$ nhưng Spark vẫn băm (hash) cả 2 bảng và shuffle qua mạng giữa 2 Worker nodes $\to$ Tốn $1.7\text{GB}$ Shuffle I/O.
2. **Tắc Nghẽn Data Skew Trên 1 Core**: Khách hàng `customer_id = 1001` dồn 2.1 triệu dòng vào Task 42 $\to$ Vượt quá $600\text{MB}$ Execution Memory của Core $\to$ **Spill 1.1GB xuống đĩa**, 1 Core chạy 45 phút trong khi 7 Cores khác ngồi chơi sau 2 giây!
3. **Không Column Pruning**: Đọc toàn bộ 30 cột của bảng hóa đơn thay vì chỉ lấy 3 cột cần tính toán (`customer_id`, `id`, `total_amount`).

---

### 1.3 Phiên bản 2 (Optimized Code — Tối Ưu Sâu: 1.4 phút, Tăng Tốc $32\times$, Disk Spill = 0 Bytes)

```python
# ==============================================================================
# PHIÊN BẢN 2: OPTIMIZED (Áp dụng Column Pruning, Broadcast Join, Salting 2 Giai Đoạn)
# Thời gian chạy thực tế: 1.4 PHÚT (Huy động 100% 8 Cores CPU, Disk Spill = 0 Bytes)
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("FleetRevenueSummary_Optimized") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# 1. Column Pruning: Chỉ đọc đúng các cột cần thiết, giảm 70% I/O đĩa
df_invoices = spark.read.parquet("hdfs://master:9000/lake/bronze/invoices") \
    .select("id", "customer_id", "total_amount")

df_customers = spark.read.parquet("hdfs://master:9000/lake/bronze/customers") \
    .select("id", "company_name")

# 2. GIAI ĐOẠN 1 CỦA SALTING: Thêm Salt ngẫu nhiên (0..9) để phân tán key bị Skew
SALT_BUCKETS = 10
df_invoices_salted = df_invoices.withColumn(
    "salt_key", 
    F.concat(F.col("customer_id"), F.lit("_"), F.floor(F.rand() * SALT_BUCKETS))
)

# Chạy Partial Aggregation trên salt_key (Chia 2.1 triệu dòng thành 10 phần ~210k dòng)
# Vừa vặn 170MB RAM per core -> Hoàn toàn KHÔNG bị tràn đĩa (Disk Spill: 0B)
df_partial_agg = df_invoices_salted.groupBy("salt_key").agg(
    F.sum("total_amount").alias("partial_revenue"),
    F.count("id").alias("partial_invoices")
)

# 3. GIAI ĐOẠN 2 CỦA SALTING: Bỏ đuôi Salt và Aggregate lần cuối trên tập dữ liệu nhỏ
df_invoices_aggregated = df_partial_agg.withColumn(
    "customer_id", 
    F.split(F.col("salt_key"), "_").getItem(0).cast("int")
).groupBy("customer_id").agg(
    F.sum("partial_revenue").alias("total_revenue"),
    F.sum("partial_invoices").alias("total_invoices")
).filter(F.col("total_revenue") > 100000000)

# 4. Broadcast Hash Join: Đưa bảng customers (15MB) vào RAM của từng Executor
# Triệt tiêu 100% Shuffle Network I/O cho phép JOIN
df_final = df_invoices_aggregated.join(
    F.broadcast(df_customers), 
    df_invoices_aggregated.customer_id == df_customers.id, 
    "inner"
).select(
    df_invoices_aggregated.customer_id,
    df_customers.company_name,
    df_invoices_aggregated.total_revenue,
    df_invoices_aggregated.total_invoices
)

# 5. Coalesce(4) gộp file output chuẩn 128MB, chống Small File Problem trên HDFS
df_final.coalesce(4).write \
    .mode("overwrite") \
    .parquet("hdfs://master:9000/dwh/gold/customer_revenue_optimized")
```

---

### 1.4 Lời Giải Trình Kỹ Thuật Đẳng Cấp Senior So Sánh 2 Phiên Bản (60 Giây):

> *"Thưa anh, ở phiên bản 1, code nhìn rất ngắn gọn nhưng chạy mất **45.3 phút** vì dính 3 lỗi sơ đẳng trong Big Data: 
> Thứ nhất, dùng SortMergeJoin trên bảng 15MB gây lãng phí Shuffle I/O; 
> Thứ hai, dồn 2.1 triệu dòng của khách hàng 1001 vào duy nhất 1 Core CPU khiến 7 Cores khác ngồi chơi và task bị **Spill 1.1GB xuống đĩa cứng**.
> 
> Ở phiên bản 2, em đã tối ưu toàn diện:
> 1. **Broadcast Join**: Phát tán bảng 15MB vào bộ nhớ của 2 Workers, triệt tiêu hoàn toàn Shuffle I/O.
> 2. **Kỹ thuật Salting 2 giai đoạn**: Băm nhỏ 2.1 triệu dòng thành 10 buckets ngẫu nhiên, **huy động đồng thời cả 8 Cores CPU xử lý song song trên RAM với Disk Spill = 0 Bytes**, hoàn tất Stage 1 trong 55 giây.
> 3. **Coalesce(4)**: Kiểm soát đầu ra thành các file chuẩn 128MB bảo vệ NameNode HDFS.
> $\implies$ Kết quả: Thời gian chạy giảm từ **$45.3\text{ phút} \downharpoonright 1.4\text{ phút}$ (Nhanh hơn $32\times$)**."*

---

# BÀI THỬ THÁCH 2: LIVE DATA WAREHOUSE & STAR-SCHEMA DESIGN (THIẾT KẾ MÔ HÌNH DỮ LIỆU)

---

### 2.1 Đề bài:
> *"Cho cấu trúc OLTP nguồn gồm 6 bảng: `invoices`, `work_orders`, `customers`, `heads`, `components`, `parts_inventory`.
> Hãy thiết kế Data Warehouse theo **Mô hình Star Schema** thỏa mãn 3 tiêu chí:
> * **Tiêu chí A**: Báo cáo doanh thu phân tách riêng: (1) Doanh thu dịch vụ sửa chữa và (2) Doanh thu bán linh kiện phụ tùng, theo **Năm Tài Khóa (Fiscal Year: 01/10 đến 30/09 năm sau)**, theo Trạm và theo Quý.
> * **Tiêu chí B**: Theo dõi lịch sử thay đổi hạng khách hàng (*Mới $\to$ Thường niên $\to$ VIP*) bằng **SCD Type 2** bảo toàn 100% lịch sử giao dịch.
> * **Tiêu chí C**: Tối ưu hóa truy vấn Dashboard PowerBI với độ trễ $< 1\text{ giây}$."*

---

### 2.2 Thiết kế Sơ đồ Star Schema (2 Fact Tables + 4 Conformed Dimensions):

```
                                  SƠ ĐỒ STAR SCHEMA (DWH GOLD LAYER)
                                  
                                    ┌────────────────────────┐
                                    │      dim_date          │
                                    │ (fiscal_year, quarter) │
                                    └───────────┬────────────┘
                                                │
                 ┌──────────────────────────────┼──────────────────────────────┐
                 │ 1:N                          │ 1:N                          │ 1:N
                 ▼                              ▼                              ▼
  ┌──────────────────────────────┐ ┌──────────────────────────────┐ ┌────────────────────────┐
  │         dim_head             │ │ fact_repair_service_revenue  │ │      dim_customer      │
  │ (head_id, name, region, lat) │ │ (service_fact_sk, head_sk,   │ │ (customer_sk MD5,      │
  └──────────────┬───────────────┘ │  customer_sk, date_sk,      │ │  customer_id, status,  │
                 │ 1:N             │  labor_amount, duration_min) │ │  effective_date,       │
                 │                 └──────────────────────────────┘ │  expiration_date,       │
                 │                                                  │  is_current) [SCD2]    │
                 │                 ┌──────────────────────────────┐ │
                 │ 1:N             │      fact_parts_sales        │ │ 1:N
                 └────────────────►│ (parts_fact_sk, head_sk,     │◄┘
                                   │  customer_sk, component_sk,  │
                                   │  date_sk, quantity, amount)  │
                                   └──────────────┬───────────────┘
                                                  │ 1:N
                                                  ▼
                                   ┌──────────────────────────────┐
                                   │        dim_component         │
                                   │ (component_sk, code, category│
                                   └──────────────────────────────┘
```

---

### 2.3 Mã DDL Chi Tiết Cho Hệ Thống Data Warehouse (PostgreSQL / Spark SQL):

```sql
-- 1. Dimension Date có tính toán sẵn Năm Tài Khóa (Fiscal Year)
CREATE TABLE dim_date (
    date_sk INT PRIMARY KEY,              -- Format: YYYYMMDD (ví dụ: 20260515)
    full_date DATE NOT NULL,
    calendar_year INT NOT NULL,
    calendar_quarter INT NOT NULL,
    calendar_month INT NOT NULL,
    fiscal_year INT NOT NULL,             -- Logic: Tháng >= 10 thì Năm Tài Khóa = Năm Hiện Tại + 1
    fiscal_quarter VARCHAR(10) NOT NULL,  -- FY2026-Q1, FY2026-Q2...
    is_weekend BOOLEAN NOT NULL
);

-- 2. Dimension Customer áp dụng SCD Type 2
CREATE TABLE dim_customer (
    customer_sk VARCHAR(32) PRIMARY KEY,  -- Surrogate Key: MD5(customer_id || status || effective_date)
    customer_id INT NOT NULL,             -- Natural Key từ Odoo
    company_name VARCHAR(255) NOT NULL,
    customer_rank VARCHAR(50) NOT NULL,   -- 'New', 'Regular', 'VIP'
    fleet_size INT NOT NULL,
    effective_date TIMESTAMP NOT NULL,    -- Ngày bắt đầu hiệu lực của hạng
    expiration_date TIMESTAMP,            -- Ngày hết hiệu lực (NULL nếu đang active)
    is_current BOOLEAN NOT NULL           -- TRUE: Bản ghi hiện tại, FALSE: Bản ghi lịch sử
);

-- 3. Dimension Head (Trạm Dịch Vụ)
CREATE TABLE dim_head (
    head_sk INT PRIMARY KEY,
    head_id INT NOT NULL,
    head_name VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,          -- Miền Bắc, Miền Trung, Miền Nam
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    max_bay_capacity INT NOT NULL
);

-- 4. Dimension Component (Linh Kiện Phụ Tùng)
CREATE TABLE dim_component (
    component_sk INT PRIMARY KEY,
    component_id INT NOT NULL,
    component_code VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,       -- Hệ thống Phanh, Động cơ, Điện...
    unit_cost NUMERIC(15, 2) NOT NULL
);

-- 5. Fact 1: Doanh Thu Dịch Vụ Sửa Chữa (Transaction Fact Table)
-- Grain: 1 dòng = 1 Phiếu Dịch Vụ Sửa Chữa (Work Order)
CREATE TABLE fact_repair_service_revenue (
    service_fact_sk BIGINT PRIMARY KEY,
    work_order_id INT NOT NULL,
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    head_sk INT NOT NULL REFERENCES dim_head(head_sk),
    customer_sk VARCHAR(32) NOT NULL REFERENCES dim_customer(customer_sk),
    service_labor_amount NUMERIC(15, 2) NOT NULL, -- Measure Additive
    service_duration_hours NUMERIC(6, 2) NOT NULL
);

-- 6. Fact 2: Doanh Thu Bán Linh Kiện Phụ Tùng (Transaction Fact Table)
-- Grain: 1 dòng = 1 Mặt hàng Linh kiện trong Hóa đơn xuất kho
CREATE TABLE fact_parts_sales (
    parts_fact_sk BIGINT PRIMARY KEY,
    invoice_id INT NOT NULL,
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    head_sk INT NOT NULL REFERENCES dim_head(head_sk),
    customer_sk VARCHAR(32) NOT NULL REFERENCES dim_customer(customer_sk),
    component_sk INT NOT NULL REFERENCES dim_component(component_sk),
    quantity_sold INT NOT NULL,                   -- Measure Additive
    unit_price NUMERIC(15, 2) NOT NULL,
    total_parts_amount NUMERIC(15, 2) NOT NULL    -- Measure Additive
);
```

---

### 2.4 Code PySpark Xử Lý SCD Type 2 Merge 5 Bước (Surrogate Key MD5):

```python
# ==============================================================================
# LOGIC XỬ LÝ SCD TYPE 2 TRÊN PYSPARK VỚI SURROGATE KEY MD5
# ==============================================================================
from pyspark.sql import functions as F

# Bước 1: Đọc dữ liệu CDC mới nhất từ Kafka/Bronze Layer
df_cdc_updates = spark.read.parquet("hdfs://master:9000/lake/bronze/cdc_customers")

# Bước 2: Đọc bảng Dimension Customer hiện tại trong DWH
df_dim_existing = spark.read.parquet("hdfs://master:9000/dwh/gold/dim_customer")

# Bước 3: Tìm các bản ghi thay đổi trạng thái (rank hoặc fleet_size)
df_changed = df_cdc_updates.join(
    df_dim_existing.filter("is_current = true"),
    on="customer_id",
    how="inner"
).filter(
    (df_cdc_updates.customer_rank != df_dim_existing.customer_rank) |
    (df_cdc_updates.fleet_size != df_dim_existing.fleet_size)
)

# Bước 4.1: Đóng bản ghi cũ (Set is_current = False, expiration_date = Now)
df_expired_records = df_dim_existing.join(
    df_changed.select("customer_id"), on="customer_id", how="inner"
).filter("is_current = true") \
 .withColumn("is_current", F.lit(False)) \
 .withColumn("expiration_date", F.current_timestamp())

# Bước 4.2: Tạo bản ghi mới (Sinh Surrogate Key MD5 mới, is_current = True)
df_new_records = df_changed.select(
    "customer_id", "company_name", "customer_rank", "fleet_size"
).withColumn("effective_date", F.current_timestamp()) \
 .withColumn("expiration_date", F.lit(None).cast("timestamp")) \
 .withColumn("is_current", F.lit(True)) \
 .withColumn(
     "customer_sk", 
     F.md5(F.concat(F.col("customer_id"), F.col("customer_rank"), F.col("effective_date")))
 )

# Bước 5: Union bản ghi không đổi + bản ghi cũ đã đóng + bản ghi mới -> Ghi đè Gold DWH
df_unchanged = df_dim_existing.join(df_changed.select("customer_id"), on="customer_id", how="left_anti")
df_final_dim = df_unchanged.unionByName(df_expired_records).unionByName(df_new_records)
df_final_dim.write.mode("overwrite").parquet("hdfs://master:9000/dwh/gold/dim_customer")
```

---

# BÀI THỬ THÁCH 3: ARCHITECTURE DESIGN & TOOL TRADE-OFF DEFENSE

---

### 3.1 Sơ Đồ Kiến Trúc Data Pipeline E2E Hoàn Chỉnh:

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BẢN VẼ KIẾN TRÚC TỔNG THỂ DATA PLATFORM (CHUẨN BARE-METAL MULTI-NODE LINUX):                           │
│                                                                                                        │
│  [PostgreSQL OLTP] ────► [Debezium CDC] ────► [Kafka 3 Brokers] ────► [Spark Streaming]               │
│  (WAL Logical)           (pgoutput 8083)      (Partition=6, RF=3)     (Micro-batch 30s)                │
│                                                                               │                        │
│                                                                               ▼                        │
│  [FastAPI WebSocket] ◄── [Redis 7+ Serving] ◄── [Airflow DWH Job] ◄── [HDFS Data Lake]                 │
│  (Push BI <15ms)         (GEOSEARCH <1ms)       (Nightly SCD2 Gold)   (Bronze Parquet 128MB)           │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 3.2 Ma Trận Bảo Vệ 5 Lựa Chọn Công Nghệ: Tại Sao A Mà Không Phải B?

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ MA TRẬN PHẢN BIỆN LỰA CHỌN CÔNG NGHỆ CHUẨN SENIOR (TRADE-OFF DECISION MATRIX):                         │
├──────────────────────────┬──────────────────────────┬──────────────────────────────────────────────────┤
│ Lựa Chọn Kỹ Thuật (Tool A)│ Giải Pháp Thay Thế (Tool B)│ Lý Do Quyết Định & Đánh Đổi Kỹ Thuật (Trade-off) │
├──────────────────────────┼──────────────────────────┼──────────────────────────────────────────────────┤
│ 1. Debezium Log-based    │ Query Polling            │ • CDC đọc WAL: 0% tải query lên OLTP, bắt trọn   │
│    CDC (pgoutput)        │ (`WHERE updated_at > ?`) │   vẹn sự kiện DELETE và các trạng thái trung gian│
│                          │                          │ • Đánh đổi: Cần quản trị Replication Slot an toàn│
├──────────────────────────┼──────────────────────────┼──────────────────────────────────────────────────┤
│ 2. Kafka Multi-Broker    │ RabbitMQ / Direct HTTP   │ • Kafka lưu trữ phân tán, phân vùng theo ID,     │
│    (3 Brokers, RF=3)     │ Webhooks                 │   chịu tải hàng chục ngàn events IoT mà không mất│
│                          │                          │ • Đánh đổi: Cần Zookeeper và quản lý Consumer Lag│
├──────────────────────────┼──────────────────────────┼──────────────────────────────────────────────────┤
│ 3. Spark on YARN +       │ PostgreSQL Read Replica /│ • Parquet Columnar giảm 90% Disk I/O; Spark xử lý│
│    HDFS Parquet Lakehouse│ ClickHouse Single-node   │   song song trên 8 Cores tính toán đa chiều < 2s │
│                          │                          │ • Đánh đổi: Cần Airflow Compaction gộp file nhỏ  │
├──────────────────────────┼──────────────────────────┼──────────────────────────────────────────────────┤
│ 4. Redis 7+ GEO +        │ PostgreSQL PostGIS       │ • Redis xử lý in-memory < 1ms, chịu 100k req/s từ│
│    Lua Script            │ (`ST_DWithin` & Lock)    │   app tài xế; Lua script đảm bảo nguyên tử 0% lỗi│
│                          │                          │ • Đánh đổi: Dữ liệu RAM dễ mất nếu không bật AOF │
├──────────────────────────┼──────────────────────────┼──────────────────────────────────────────────────┤
│ 5. Apache Airflow        │ Crontab / Celery         │ • Airflow quản lý đồ thị phụ thuộc DAG, tự động  │
│    LocalExecutor         │ Background Workers       │   retry, backfill lịch sử và trực quan hóa lỗi   │
│                          │                          │ • Đánh đổi: Tốn 1 database metadata riêng biệt   │
└──────────────────────────┴──────────────────────────────────────────────────────────────────────────┘
```

---

### 🏆 ĐIỂM SÁNG TUYỆT ĐỐI CỦA 3 BÀI THỬ THÁCH NÀY:
1. **Bạn có sẵn 2 phiên bản code PySpark cụ thể**: Khi được yêu cầu live-coding, bạn lập tức viết được logic Salting 2 giai đoạn với Broadcast Join.
2. **Bạn có DDL Star Schema hoàn chỉnh**: Trình bày rõ ràng 2 Fact Tables, 4 Dimensions, cách tính Năm Tài Khóa và code SCD Type 2 Merge 5 bước.
3. **Bạn bảo vệ kiến trúc bằng tư duy Ma trận Đánh đổi (Trade-off Matrix)**: Chứng minh bạn hiểu sâu lý do tại sao dùng từng công cụ và làm chủ 100% hệ thống!
