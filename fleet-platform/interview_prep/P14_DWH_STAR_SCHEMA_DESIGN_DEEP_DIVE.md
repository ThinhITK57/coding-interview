# P14 — CẨM NANG THIẾT KẾ DATA WAREHOUSE & STAR SCHEMA TỪNG BƯỚC (THỰC CHIẾN CHUẨN KIMBALL)
## Hướng Dẫn Chi Tiết: Từ Khảo Sát Nghiệp Vụ, Bus Matrix, Xác Định Grain Đến DDL & PySpark SCD Type 2

> **Tôn Chỉ Thiết Kế Data Warehouse Cấp Cao:**
> *"Thiết kế Data Warehouse không phải là vẽ vài bảng Fact và Dimension một cách ngẫu hứng. Một Kỹ sư Dữ liệu cấp cao phải tuân thủ nghiêm ngặt **Quy trình 4 Bước của Ralph Kimball**, xây dựng **Enterprise Bus Matrix** để đảm bảo tính nhất quán (Conformed Dimensions), xác định chính xác **Grain (Hạt dữ liệu)**, và xử lý triệt để bài toán **SCD Type 2** để bảo toàn lịch sử số liệu tài chính."*

---

# MỤC LỤC CHI TIẾT

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PHẦN 1: QUY TRÌNH 4 BƯỚC THIẾT KẾ CHIỀU CHUẨN KIMBALL (THE 4-STEP DIMENSIONAL DESIGN)                  │
│   1.1 Bước 1: Chọn Quy trình Nghiệp vụ (Select the Business Process)                                  │
│   1.2 Bước 2: Tuyên bố Mức độ Chi tiết (Declare the Grain) — Trọng tâm của thiết kế DWH               │
│   1.3 Bước 3: Xác định các Chiều Phân tích (Identify the Dimensions) & Conformed Dimensions           │
│   1.4 Bước 4: Xác định các Chỉ số Đo lường (Identify the Facts/Measures) — Additive / Semi / Non      │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 2: MA TRẬN DOANH NGHIỆP (ENTERPRISE DATA WAREHOUSE BUS MATRIX)                                    │
│   • Bảng Ma trận kết nối giữa các Fact Tables và Conformed Dimensions                                  │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 3: THIẾT KẾ CHI TIẾT TỪNG BẢNG (DATA DICTIONARY, GRAIN & DDL STATEMENTS)                          │
│   3.1 Dimension Date: Logic Năm Tài Khóa (Fiscal Year: 01/10 - 30/09) & Cờ Thời Gian                  │
│   3.2 Dimension Customer: Cơ chế SCD Type 2 với Surrogate Key MD5                                     │
│   3.3 Dimension Head (Trạm dịch vụ) & Dimension Component (Linh kiện phụ tùng)                         │
│   3.4 Fact 1: `fact_repair_service_revenue` (Transaction Fact: Doanh thu Dịch vụ)                     │
│   3.5 Fact 2: `fact_parts_sales` (Transaction Fact: Doanh thu Bán Linh Kiện)                          │
│   3.6 Fact 3: `fact_daily_inventory_snapshot` (Periodic Snapshot Fact: Tồn kho cuối ngày)              │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 4: PIPELINE PYSPARK THỰC THI SCD TYPE 2 NĂM BƯỚC (END-TO-END CODE IMPLEMENTATION)                 │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 5: CHIẾN LƯỢC TỐI ƯU HÓA TRUY VẤN DWH (PARTITIONING, Z-ORDER, BUCKETING & SHUFFLE REDUCTION)     │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 6: KỊCH BẢN TRÌNH BÀY THIẾT KẾ TRÊN BẢNG TRẮNG KHI PHỎNG VẤN (WHITEBOARD INTERVIEW SCRIPT)       │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 1: QUY TRÌNH 4 BƯỚC THIẾT KẾ CHIỀU CHUẨN KIMBALL

Khi Hội đồng phỏng vấn yêu cầu: *"Hãy trình bày cách em thiết kế Data Warehouse cho một bài toán cụ thể?"*, bạn phải trình bày ngay **Khung 4 bước chuẩn mực của Ralph Kimball**:

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ KHUNG 4 BƯỚC THIẾT KẾ DIMENSIONAL MODELING CỦA RALPH KIMBALL:                                          │
│                                                                                                        │
│  [Bước 1: Chọn Business Process] ──► [Bước 2: Tuyên bố Grain] ──► [Bước 3: Xác định Dims] ──► [Bước 4: Xác định Facts]│
│  (Quy trình tạo doanh thu)           (1 dòng = 1 giao dịch)       (Ai, Ở đâu, Khi nào, Cái gì) (Số tiền, Số lượng, Thời gian)│
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 1.1 Bước 1: Chọn Quy Trình Nghiệp Vụ (Select the Business Process)
* Một Data Warehouse không bao giờ mô hình hóa "toàn bộ công ty cùng lúc", mà đi từ từng **Quy trình nghiệp vụ cụ thể phát sinh sự kiện (Event-driven Business Process)**.
* Với chuỗi 50 trạm sửa xe Fleet, ta có 3 quy trình kinh doanh cốt lõi:
  1. *Quy trình Sửa chữa xe*: Khi một phiếu sửa chữa (Work Order) hoàn thành, thợ sửa xe nghiệm thu và xuất hóa đơn dịch vụ công.
  2. *Quy trình Bán linh kiện thay thế*: Khi phụ tùng (má phanh, lốp, dầu nhớt) được xuất kho bán cho xe.
  3. *Quy trình Quản lý Tồn kho trạm*: Theo dõi biến động nhập/xuất/tồn linh kiện tại từng trạm mỗi ngày.

---

### 1.2 Bước 2: Tuyên Bố Mức Độ Chi Tiết (Declare the Grain) — Trọng Tâm Cốt Tử
> ⚠️ **Quy tắc vàng của Kimball**: *"Grain là định nghĩa chính xác về mặt vật lý xem MỘT DÒNG DUY NHẤT trong bảng Fact đại diện cho cái gì trong thế giới thực."*
> Nếu không khai báo Grain rõ ràng, bạn sẽ bị nhầm lẫn giữa các mức tổng hợp khác nhau (Summary vs Transaction), dẫn đến hiện tượng tính trùng số liệu (Double-counting).

* **Grain của Fact 1 (`fact_repair_service_revenue`)**: 
  * *Tuyên bố*: **Một dòng là MỘT PHIẾU DỊCH VỤ SỬA CHỮA (Work Order) đã hoàn tất nghiệm thu và xuất hóa đơn.**
* **Grain của Fact 2 (`fact_parts_sales`)**: 
  * *Tuyên bố*: **Một dòng là MỘT MẶT HÀNG LINH KIỆN trong một Hóa đơn xuất bán phụ tùng.**
* **Grain của Fact 3 (`fact_daily_inventory_snapshot`)**: 
  * *Tuyên bố*: **Một dòng là MỨC TỒN KHO CUỐI NGÀY của MỘT MÃ LINH KIỆN tại MỘT TRẠM DỊCH VỤ vào MỘT NGÀY CỤ THỂ.**

---

### 1.3 Bước 3: Xác Định Các Chiều Phân Tích (Identify the Dimensions)
Trả lời các câu hỏi: *Ai tham gia? Ở đâu? Khi nào? Mặt hàng gì? Thiết bị nào?*
1. **`dim_date`** (*Khi nào?*): Ngày xuất hóa đơn, Quý tài khóa, Năm tài khóa (01/10 - 30/09).
2. **`dim_customer`** (*Ai mua?*): Khách hàng, Doanh nghiệp vận tải, Hạng khách hàng (SCD Type 2).
3. **`dim_head`** (*Ở đâu?*): Trạm dịch vụ, Khu vực địa lý (Bắc/Trung/Nam), Tọa độ Lat/Lng.
4. **`dim_component`** (*Cái gì?*): Mã phụ tùng, Nhóm linh kiện (Phanh, Động cơ, Điện), Nhà sản xuất.

---

### 1.4 Bước 4: Xác Định Các Chỉ Số Đo Lường (Identify Facts / Measures)
Phân loại chính xác 3 kiểu Measure trong Fact Table:
* **Additive Measures (Cộng dồn hoàn toàn)**: Có thể `SUM()` theo mọi chiều phân tích.
  * *Ví dụ*: `service_labor_amount` (Tiền công), `parts_quantity` (Số lượng linh kiện), `total_parts_amount` (Tiền phụ tùng).
* **Semi-Additive Measures (Cộng dồn bán phần)**: Có thể `SUM()` theo không gian, trạm, khách hàng, nhưng **KHÔNG ĐƯỢC `SUM()` theo trục thời gian**.
  * *Ví dụ*: `ending_stock_quantity` (Số lượng tồn kho cuối ngày) $\implies$ Tổng tồn của 50 trạm ngày hôm nay = SUM được; nhưng tồn của 30 ngày trong tháng = KHÔNG ĐƯỢC SUM (phải lấy ngày cuối kỳ).
* **Non-Additive Measures (Không thể cộng dồn)**: Tỉ lệ phần trăm, đơn giá.
  * *Ví dụ*: `discount_rate_%`, `unit_cost` $\implies$ Phải lưu `numerator` (tử số) và `denominator` (mẫu số) để tính toán động trên BI.

---

# PHẦN 2: MA TRẬN DOANH NGHIỆP (ENTERPRISE DWH BUS MATRIX)

Một kiến trúc sư giỏi luôn sử dụng **Enterprise Bus Matrix** để chứng minh các bảng Fact khác nhau chia sẻ chung các Dimension chuẩn hóa (**Conformed Dimensions**), giúp loại bỏ hoàn toàn các "ốc đảo dữ liệu" (Data Silos):

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ MA TRẬN ENTERPRISE DWH BUS MATRIX (FLEET PLATFORM):                                                    │
├──────────────────────────────────┬──────────┬──────────────┬──────────┬───────────────┬────────────────┤
│ Fact Table (Business Process)    │ dim_date │ dim_customer │ dim_head │ dim_component │ Grain Chi Tiết │
├──────────────────────────────────┼──────────┼──────────────┼──────────┼───────────────┼────────────────┤
│ 1. fact_repair_service_revenue   │    ●     │      ●       │    ●     │               │ 1 Work Order   │
│ 2. fact_parts_sales              │    ●     │      ●       │    ●     │       ●       │ 1 Line Item    │
│ 3. fact_daily_inventory_snapshot │    ●     │              │    ●     │       ●       │ 1 Head/Part/Day│
└──────────────────────────────────┴──────────┴──────────────┴──────────┴───────────────┴────────────────┘
```
*(Dấu ● thể hiện bảng Dimension được dùng chung - Conformed Dimension).*

---

# PHẦN 3: THIẾT KẾ CHI TIẾT TỪNG BẢNG & DDL CHUẨN POSTGRESQL / SPARK SQL

---

### 3.1 Dimension Date: Logic Năm Tài Khóa (Fiscal Year)
Doanh nghiệp có Năm Tài Khóa đặc thù: **Bắt đầu từ ngày 01/10 và kết thúc vào ngày 30/09 năm sau**.

```sql
CREATE TABLE dim_date (
    date_sk INT PRIMARY KEY,              -- Format: YYYYMMDD (ví dụ: 20260515)
    full_date DATE NOT NULL UNIQUE,
    calendar_year INT NOT NULL,
    calendar_quarter INT NOT NULL,        -- 1, 2, 3, 4
    calendar_month INT NOT NULL,          -- 1..12
    day_of_month INT NOT NULL,            -- 1..31
    day_of_week_name VARCHAR(10) NOT NULL,-- 'Monday', 'Tuesday'...
    -- LOGIC NĂM TÀI KHÓA (FISCAL YEAR CALCULATION):
    -- Nếu tháng >= 10: Fiscal Year = Calendar Year + 1. Ví dụ: Tháng 11/2025 -> FY2026
    fiscal_year INT NOT NULL,
    fiscal_quarter VARCHAR(10) NOT NULL,  -- 'FY2026-Q1', 'FY2026-Q2'...
    fiscal_month_number INT NOT NULL,     -- Tháng 10 là tháng 1 của Năm Tài Khóa
    is_weekend BOOLEAN NOT NULL,
    is_national_holiday BOOLEAN NOT NULL
);
```

#### Code nạp dữ liệu mẫu cho `dim_date` (Tính toán tự động cho 10 năm):
```sql
INSERT INTO dim_date
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_sk,
    datum AS full_date,
    EXTRACT(YEAR FROM datum) AS calendar_year,
    EXTRACT(QUARTER FROM datum) AS calendar_quarter,
    EXTRACT(MONTH FROM datum) AS calendar_month,
    EXTRACT(DAY FROM datum) AS day_of_month,
    TO_CHAR(datum, 'FMDay') AS day_of_week_name,
    -- Tính Fiscal Year:
    CASE 
        WHEN EXTRACT(MONTH FROM datum) >= 10 THEN EXTRACT(YEAR FROM datum) + 1 
        ELSE EXTRACT(YEAR FROM datum) 
    END AS fiscal_year,
    -- Tính Fiscal Quarter:
    CASE 
        WHEN EXTRACT(MONTH FROM datum) IN (10, 11, 12) THEN 'Q1'
        WHEN EXTRACT(MONTH FROM datum) IN (1, 2, 3)    THEN 'Q2'
        WHEN EXTRACT(MONTH FROM datum) IN (4, 5, 6)    THEN 'Q3'
        ELSE 'Q4'
    END AS fiscal_quarter,
    -- Tính Fiscal Month:
    CASE 
        WHEN EXTRACT(MONTH FROM datum) >= 10 THEN EXTRACT(MONTH FROM datum) - 9
        ELSE EXTRACT(MONTH FROM datum) + 3
    END AS fiscal_month_number,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_national_holiday
FROM GENERATE_SERIES('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) datum;
```

---

### 3.2 Dimension Customer: Cơ Chế SCD Type 2 với Surrogate Key MD5

```sql
CREATE TABLE dim_customer (
    customer_sk VARCHAR(32) PRIMARY KEY,  -- Surrogate Key: MD5(customer_id || status || effective_date)
    customer_id INT NOT NULL,             -- Natural Key từ Odoo ERP
    company_name VARCHAR(255) NOT NULL,
    customer_rank VARCHAR(50) NOT NULL,   -- 'New', 'Regular', 'VIP'
    fleet_size INT NOT NULL,              -- Số lượng xe sở hữu
    contact_phone_masked VARCHAR(20),     -- PII Data Masking: '0912***678'
    effective_date TIMESTAMP NOT NULL,    -- Mốc bắt đầu có hiệu lực của phiên bản
    expiration_date TIMESTAMP,            -- Mốc hết hiệu lực (NULL nếu là bản ghi hiện hành)
    is_current BOOLEAN NOT NULL           -- TRUE: Bản ghi đang active, FALSE: Bản ghi lịch sử
);

CREATE INDEX idx_dim_customer_lookup ON dim_customer (customer_id, is_current);
```

---

### 3.3 Dimension Head & Dimension Component

```sql
-- Dimension Head (50 Trạm Dịch Vụ)
CREATE TABLE dim_head (
    head_sk INT PRIMARY KEY,
    head_id INT NOT NULL,                 -- Natural Key
    head_name VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,          -- 'Miền Bắc', 'Miền Trung', 'Miền Nam'
    city VARCHAR(50) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    max_service_bays INT NOT NULL,        -- Số lượng buồng sửa chữa tối đa
    opened_date DATE NOT NULL
);

-- Dimension Component (Danh Mục Linh Kiện)
CREATE TABLE dim_component (
    component_sk INT PRIMARY KEY,
    component_id INT NOT NULL,             -- Natural Key
    component_code VARCHAR(50) NOT NULL,  -- 'BRK-001', 'ENG-99'
    component_name VARCHAR(150) NOT NULL,
    category VARCHAR(100) NOT NULL,       -- 'Hệ Thống Phanh', 'Động Cơ', 'Hệ Thống Điện'
    unit_of_measure VARCHAR(20) NOT NULL, -- 'Cái', 'Bộ', 'Lít'
    standard_cost NUMERIC(15, 2) NOT NULL
);
```

---

### 3.4 Fact Tables Chi Tiết:

```sql
-- FACT 1: Doanh Thu Dịch Vụ Sửa Chữa (Transaction Fact Table)
CREATE TABLE fact_repair_service_revenue (
    service_fact_sk BIGINT PRIMARY KEY,
    work_order_id INT NOT NULL,           -- Degenerate Dimension (Mã đơn gốc)
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    head_sk INT NOT NULL REFERENCES dim_head(head_sk),
    customer_sk VARCHAR(32) NOT NULL REFERENCES dim_customer(customer_sk),
    -- Measures:
    labor_amount NUMERIC(15, 2) NOT NULL, -- Tiền công dịch vụ (Additive)
    service_duration_hours NUMERIC(6, 2), -- Thời gian sửa chữa (Additive)
    discount_amount NUMERIC(15, 2) NOT NULL
);

-- FACT 2: Doanh Thu Bán Linh Kiện Phụ Tùng (Transaction Fact Table)
CREATE TABLE fact_parts_sales (
    parts_fact_sk BIGINT PRIMARY KEY,
    invoice_id INT NOT NULL,              -- Degenerate Dimension
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    head_sk INT NOT NULL REFERENCES dim_head(head_sk),
    customer_sk VARCHAR(32) NOT NULL REFERENCES dim_customer(customer_sk),
    component_sk INT NOT NULL REFERENCES dim_component(component_sk),
    -- Measures:
    quantity_sold INT NOT NULL,           -- Số lượng bán (Additive)
    unit_selling_price NUMERIC(15, 2) NOT NULL,
    total_parts_amount NUMERIC(15, 2) NOT NULL, -- Doanh thu phụ tùng (Additive)
    cost_of_goods_sold NUMERIC(15, 2) NOT NULL  -- Giá vốn FIFO (Additive)
);

-- FACT 3: Báo Cáo Tồn Kho Cuối Ngày (Periodic Snapshot Fact Table)
CREATE TABLE fact_daily_inventory_snapshot (
    inventory_snapshot_sk BIGINT PRIMARY KEY,
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    head_sk INT NOT NULL REFERENCES dim_head(head_sk),
    component_sk INT NOT NULL REFERENCES dim_component(component_sk),
    -- Measures:
    ending_quantity INT NOT NULL,         -- Tồn kho cuối ngày (Semi-Additive!)
    total_inventory_value NUMERIC(15, 2) NOT NULL, -- Giá trị tồn kho (Semi-Additive!)
    days_of_supply_remaining INT          -- Số ngày tồn kho ước tính (Non-Additive)
);
```

---

# PHẦN 4: PIPELINE PYSPARK THỰC THI SCD TYPE 2 NĂM BƯỚC

Dưới đây là mã nguồn PySpark chuẩn production xử lý trọn vẹn **5 bước của giải thuật SCD Type 2**:

```python
# ==============================================================================
# PIPELINE PYSPARK XỬ LÝ SCD TYPE 2 MERGE CHO DIM_CUSTOMER
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SCD2_Customer_Pipeline").getOrCreate()

# 1. Đọc dữ liệu CDC mới nhất từ Bronze Layer
df_cdc = spark.read.parquet("hdfs://master:9000/lake/bronze/cdc_customers")

# 2. Đọc bảng dim_customer hiện hành từ Gold Layer DWH
df_dim = spark.read.parquet("hdfs://master:9000/dwh/gold/dim_customer")

# 3. Lấy ra các bản ghi đang active (is_current = True)
df_dim_active = df_dim.filter("is_current = true")

# 4. Phép JOIN để phát hiện những bản ghi có sự thay đổi (customer_rank hoặc fleet_size)
df_joined = df_cdc.join(
    df_dim_active,
    on="customer_id",
    how="inner"
)

# Điều kiện phát hiện thay đổi trạng thái
df_changed = df_joined.filter(
    (df_cdc.customer_rank != df_dim_active.customer_rank) |
    (df_cdc.fleet_size != df_dim_active.fleet_size)
)

# BƯỚC 5.1: Đóng các bản ghi cũ (Set is_current = False, expiration_date = Timestamp hiện tại)
df_expired_old = df_dim_active.join(
    df_changed.select("customer_id"),
    on="customer_id",
    how="inner"
).withColumn("is_current", F.lit(False)) \
 .withColumn("expiration_date", F.current_timestamp())

# BƯỚC 5.2: Tạo bản ghi mới (Sinh Surrogate Key MD5 mới, is_current = True)
df_inserted_new = df_changed.select(
    df_cdc.customer_id,
    df_cdc.company_name,
    df_cdc.customer_rank,
    df_cdc.fleet_size,
    df_cdc.contact_phone_masked
).withColumn("effective_date", F.current_timestamp()) \
 .withColumn("expiration_date", F.lit(None).cast("timestamp")) \
 .withColumn("is_current", F.lit(True)) \
 .withColumn(
     "customer_sk", 
     F.md5(F.concat(
         F.col("customer_id"), 
         F.col("customer_rank"), 
         F.col("effective_date").cast("string")
     ))
 )

# BƯỚC 5.3: Lấy các bản ghi không thay đổi (Unchanged Records)
df_unchanged = df_dim.join(
    df_changed.select("customer_id"),
    on="customer_id",
    how="left_anti"
)

# BƯỚC 5.4: UNION toàn bộ 3 tập dữ liệu -> Ghi đè vào Gold Layer
df_final_dim_customer = df_unchanged \
    .unionByName(df_expired_old) \
    .unionByName(df_inserted_new)

df_final_dim_customer.write \
    .mode("overwrite") \
    .parquet("hdfs://master:9000/dwh/gold/dim_customer")
```

---

# PHẦN 5: CHIẾN LƯỢC TỐI ƯU HÓA TRUY VẤN DATA WAREHOUSE

Để đảm bảo các Dashboard PowerBI load dữ liệu trong **$< 1\text{ giây}$**, ta áp dụng 3 kỹ thuật tối ưu hóa vật lý:

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 3 KỸ THUẬT TỐI ƯU HÓA HIỆU NĂNG DATA WAREHOUSE (PHYSICAL OPTIMIZATION):                                │
├──────────────────────────┬─────────────────────────────────────────────────────────────────────────────┤
│ 1. Partition Pruning     │ • Phân vùng Fact Table theo Năm/Tháng: `/year=2026/month=08/`               │
│    (Cắt tỉa phân vùng)   │ • Khi BI query theo quý, Spark bỏ qua 75% các thư mục phân vùng khác.      │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 2. Parquet Column Pruning│ • Định dạng Columnar lưu metadata Min/Max tại Footer của mỗi Row Group.     │
│    & Pushdown Predicate  │ • Chỉ đọc đúng cột `labor_amount`, bỏ qua 90% các cột không dùng.           │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 3. Broadcast Hash Join   │ • Các bảng `dim_head` (50 dòng), `dim_component` (500 dòng), `dim_date`     │
│    (Triệt tiêu Shuffle)  │   (3650 dòng) có dung lượng siêu nhỏ (< 5MB).                               │
│                          │ • Spark tự động Broadcast toàn bộ Dimensions vào RAM của Executors,        │
│                          │   biến phép JOIN 1-hop thành phép In-Memory HashMap Lookup $O(1)$!         │
└──────────────────────────┴─────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 6: KỊCH BẢN TRÌNH BÀY THIẾT KẾ TRÊN BẢNG TRẮNG KHI PHỎNG VẤN (3-5 PHÚT)

Khi được yêu cầu thiết kế DWH trực tiếp, bạn hãy cầm bút vẽ lên bảng và trình bày dõng dạc theo kịch bản 4 bước:

> **[Bước 1: Khẳng định phương pháp luận Kimball]**
> *"Thưa các anh, để thiết kế Data Warehouse cho chuỗi 50 trạm dịch vụ, em áp dụng **Quy trình 4 bước chuẩn của Ralph Kimball**:
> Đầu tiên, em tách bạch 2 quy trình nghiệp vụ tạo doanh thu: **Dịch vụ Sửa chữa** và **Bán Linh kiện Phụ tùng**."*
>
> **[Bước 2: Tuyên bố Grain rõ ràng]**
> *"Điểm quan trọng nhất trong DWH là tuyên bố **Grain (Hạt dữ liệu)** để chống tính trùng lặp:
> Với Dịch vụ sửa chữa: Grain là **1 dòng = 1 Phiếu Work Order hoàn tất**.
> Với Bán linh kiện: Grain là **1 dòng = 1 Mặt hàng trong Hóa đơn xuất kho**."*
>
> **[Bước 3: Vẽ Star Schema & Conformed Dimensions]**
> *(Vừa nói vừa vẽ 2 hình chữ nhật Fact ở giữa và 4 hình chữ nhật Dimensions xung quanh)*
> *"Em thiết kế mô hình Star Schema gồm 2 Transaction Fact liên kết với 4 Conformed Dimensions:
> 1. `dim_date`: Được tính toán sẵn logic **Năm Tài Khóa (Fiscal Year)** từ 01/10 đến 30/09 để phục vụ báo cáo kế toán.
> 2. `dim_customer`: Được áp dụng **SCD Type 2 với Surrogate Key MD5**, `effective_date`, `expiration_date` để lưu vết lịch sử khi khách hàng lên hạng VIP mà không làm sai lệch báo cáo tài chính của các quý cũ.
> 3. `dim_head` và `dim_component`: Chuẩn hóa danh mục trạm và danh mục phụ tùng."*
>
> **[Bước 4: Giải thích tối ưu hiệu năng]**
> *"Tại sao mô hình này giúp Dashboard chạy dưới 1 giây?
> Vì trên hệ thống tính toán phân tán Spark, mô hình Star Schema đưa mọi câu truy vấn phức tạp về dạng **1-hop join giữa Fact và Dimension**. Do các bảng Dimension có dung lượng nhỏ ($< 10\text{MB}$), Spark sẽ dùng **Broadcast Hash Join** nạp trọn vẹn Dimension vào RAM, triệt tiêu $100\%$ Shuffle Network I/O và quét dữ liệu Parquet với tốc độ tối đa."*

---

### 🏆 ĐIỂM SÁNG CỦA PHẦN THIẾT KẾ NÀY:
* Bạn nói chuẩn ngôn ngữ của một **Chuyên gia Data Modeling** (Grain, Conformed Dimension, Additive Measure, Non-equi Join, Surrogate Key MD5, Bus Matrix).
* Bạn có sẵn toàn bộ mã **DDL SQL** và mã **PySpark 5 bước SCD2** để chứng minh mình làm thật 100%!
