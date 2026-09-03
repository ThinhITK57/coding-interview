# P13 — BỘ ĐỀ THỰC CHIẾN CHUYÊN SÂU: LIVE SQL & PYSPARK EXTREME CHALLENGES
## Đa Dạng Hóa Tình Huống: Nâng Cao Tư Duy Thuật Toán Dữ Liệu, Tối Ưu Bộ Nhớ & Xử Lý Dòng

> **Mục Tiêu Chiến Lược Cho Vòng Final:**
> *"Hội đồng phỏng vấn Senior/Lead tại Viettel Cyber Security không hỏi các câu lý thuyết suông. Họ sẽ đưa ra các **bài toán nghiệp vụ hóc búa, dữ liệu bị lệch (Skew), bùng nổ bộ nhớ (OOM), hoặc yêu cầu tính toán logic phức tạp** để xem ứng viên có viết được code chuẩn, hiểu rõ cơ chế thực thi bên dưới (Execution Plan / Spark Physical Plan) và giải thích mạch lạc hay không."*

---

# MỤC LỤC 6 BÀI THỬ THÁCH THỰC CHIẾN

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BÀI 1: PHÂN TÍCH PHIÊN HOẠT ĐỘNG (SESSIONIZATION) TRÊN DỮ LIỆU IOT TELEMETRY XE TẢI                   │
│   • Kỹ thuật: Cửa sổ không hoạt động 30 phút (Inactivity Timeout), Island Identification              │
│   • Thực thi: PostgreSQL Window Functions (LAG, Running Sum) vs PySpark Distributed Windowing         │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ BÀI 2: THUẬT TOÁN GAPS & ISLANDS — PHÁT HIỆN CHUỖI SỰ CỐ & SỤT GIẢM DOANH THU LIÊN TIẾP                │
│   • Kỹ thuật: Classic Difference of Row Numbers (`ROW_NUMBER() - DENSE_RANK()`), Island Grouping      │
│   • Thực thi: Advanced SQL CTEs & PySpark Window Aggregation                                           │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ BÀI 3: CƠN ÁC MỘNG ARRAY EXPLODE SKEW — XỬ LÝ MẢNG DỮ LIỆU KHỔNG LỒ KHÔNG GÂY OOM                     │
│   • Kỹ thuật: Bùng nổ dòng (Row Explosion) khi 1 phiếu đại tu chứa 5.000 linh kiện                    │
│   • Thực thi: PySpark Baseline (OOM) vs PySpark Optimized (Batched Explode + Projection Pruning)       │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ BÀI 4: KHỬ TRÙNG LẶP NÂNG CAO VỚI LATE-ARRIVING DATA & TIE-BREAKING MULTI-CRITERIA                     │
│   • Kỹ thuật: Lấy trạng thái cuối cùng (Final State) khi nhiều bản ghi trùng lặp đến muộn              │
│   • Thực thi: SQL `DISTINCT ON` vs `ROW_NUMBER()` vs PySpark `max_by()` (Tối ưu gấp 3x)               │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ BÀI 5: TÍNH TOÁN XUẤT KHO LINH KIỆN THEO NGUYÊN TẮC FIFO (GIÁ VỐN HÀNG BÁN - COGS)                     │
│   • Kỹ thuật: Cumulative Running Sum Matching giữa Lô Hàng Nhập và Đơn Xuất Kho                       │
│   • Thực thi: Pure PostgreSQL Window SQL với Non-equi Interval Overlap                                 │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ BÀI 6: PYSPARK STRUCTURED STREAMING — STATEFUL WATERMARKING & ROCKSDB STATESTORE TUNING                │
│   • Kỹ thuật: Deduplication trong cửa sổ 1 giờ + Tính trung bình trượt 10 phút, Watermark 5 phút       │
│   • Thực thi: PySpark Streaming + Cấu hình RocksDB StateStore thay thế HDFS StateStore chống OOM       │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# BÀI 1: PHÂN TÍCH PHIÊN HOẠT ĐỘNG (SESSIONIZATION) TRÊN DỮ LIỆU IOT TELEMETRY XE TẢI

---

### 1.1 Bối cảnh nghiệp vụ:
$10.000$ xe tải liên tục gửi gói tin cảm biến (vận tốc, tọa độ, trạng thái máy). Doanh nghiệp cần xác định **từng chuyến hành trình (Trip / Session)** của xe:
* Một chuyến đi bắt đầu khi xe nổ máy chạy.
* Nếu xe tắt máy hoặc mất tín hiệu quá **30 phút (Inactivity Gap $> 30\text{ phút}$)** $\implies$ Chuyến đi đó kết thúc, gói tin tiếp theo sẽ bắt đầu một Session mới.
* **Yêu cầu**: Tính thời gian bắt đầu, thời gian kết thúc, tổng thời lượng (phút), và quãng đường di chuyển của từng chuyến đi.

---

### 1.2 Giải pháp bằng ADVANCED SQL (PostgreSQL):

```sql
-- ==============================================================================
-- BÀI 1: SESSIONIZATION TRÊN SQL (KỸ THUẬT LAG + RUNNING SUM ISLAND)
-- ==============================================================================
WITH step1_lag AS (
    -- Bước 1: Lấy thời điểm của sự kiện liền trước bằng LAG()
    SELECT 
        truck_id,
        event_time,
        odometer_km,
        LAG(event_time) OVER (
            PARTITION BY truck_id 
            ORDER BY event_time
        ) AS prev_event_time
    FROM truck_telemetry
),
step2_new_session_flag AS (
    -- Bước 2: Đánh dấu cờ 1 nếu khoảng cách > 30 phút (hoặc sự kiện đầu tiên)
    SELECT 
        truck_id,
        event_time,
        odometer_km,
        CASE 
            WHEN prev_event_time IS NULL THEN 1
            WHEN EXTRACT(EPOCH FROM (event_time - prev_event_time)) / 60 > 30 THEN 1
            ELSE 0 
        END AS is_new_session
    FROM step1_lag
),
step3_session_id AS (
    -- Bước 3: Tạo Session ID duy nhất bằng phép cộng dồn tích lũy SUM() OVER()
    SELECT 
        truck_id,
        event_time,
        odometer_km,
        SUM(is_new_session) OVER (
            PARTITION BY truck_id 
            ORDER BY event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM step2_new_session_flag
)
-- Bước 4: Tổng hợp thống kê từng chuyến đi
SELECT 
    truck_id,
    session_id,
    MIN(event_time) AS trip_start_time,
    MAX(event_time) AS trip_end_time,
    ROUND(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) / 60, 2) AS duration_minutes,
    MAX(odometer_km) - MIN(odometer_km) AS distance_km,
    COUNT(*) AS total_pings
FROM step3_session_id
GROUP BY truck_id, session_id
ORDER BY truck_id, trip_start_time;
```

---

### 1.3 Giải pháp bằng PYSPARK (Xử lý phân tán trên hàng trăm triệu dòng):

```python
# ==============================================================================
# BÀI 1: SESSIONIZATION TRÊN PYSPARK (WINDOW SPEC & LAG CỘNG DỒN)
# ==============================================================================
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("TelemetrySessionization").getOrCreate()

df_telemetry = spark.read.parquet("hdfs://master:9000/lake/bronze/truck_telemetry")

# 1. Định nghĩa Window Spec theo từng xe tải, sắp xếp theo thời gian
truck_window = Window.partitionBy("truck_id").orderBy("event_time")

# 2. Tính khoảng cách thời gian giữa 2 ping liền kề (đơn vị: giây)
df_with_prev = df_telemetry.withColumn("prev_time", F.lag("event_time").over(truck_window)) \
                           .withColumn("time_diff_sec", 
                               F.coalesce(F.unix_timestamp("event_time") - F.unix_timestamp("prev_time"), F.lit(999999))
                           )

# 3. Đánh cờ New Session (Nếu khoảng cách > 1800 giây = 30 phút)
df_with_flag = df_with_prev.withColumn(
    "is_new_session", 
    F.when(F.col("time_diff_sec") > 1800, 1).otherwise(0)
)

# 4. Tạo session_id tích lũy (Running Sum)
running_session_window = Window.partitionBy("truck_id").orderBy("event_time") \
                               .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_with_session = df_with_flag.withColumn(
    "session_id", 
    F.sum("is_new_session").over(running_session_window)
)

# 5. Tổng hợp thông số từng chuyến đi
df_trip_summary = df_with_session.groupBy("truck_id", "session_id").agg(
    F.min("event_time").alias("trip_start_time"),
    F.max("event_time").alias("trip_end_time"),
    ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_minutes"),
    (F.max("odometer_km") - F.min("odometer_km")).alias("distance_km"),
    F.count("event_time").alias("total_pings")
)

df_trip_summary.write.mode("overwrite").parquet("hdfs://master:9000/dwh/gold/fact_truck_trips")
```

---

# BÀI 2: THUẬT TOÁN GAPS & ISLANDS — PHÁT HIỆN CHUỖI SỰ CỐ & SỤT GIẢM DOANH THU LIÊN TIẾP

---

### 2.1 Bối cảnh nghiệp vụ:
Ban Giám đốc cần phát hiện **các trạm dịch vụ (Heads) có dấu hiệu khủng hoảng**:
* Tiêu chí: Trạm có **chuỗi từ 3 ngày liên tiếp trở lên** bị sụt giảm doanh thu dưới mức chỉ tiêu ($< 50.000.000\text{đ/ngày}$).
* Cần xuất ra: `head_id`, `start_date`, `end_date`, `consecutive_days` (số ngày liên tiếp bị sụt giảm), và `total_lost_revenue`.

---

### 2.2 Giải pháp bằng ADVANCED SQL (Kỹ thuật Difference of Row Numbers):

```sql
-- ==============================================================================
-- BÀI 2: GAPS & ISLANDS (THUẬT TOÁN HIỆU HAI ROW_NUMBER ĐẲNG CẤP)
-- ==============================================================================
WITH daily_revenue AS (
    -- Bước 1: Tổng hợp doanh thu từng ngày của từng trạm
    SELECT 
        head_id,
        invoice_date::DATE AS report_date,
        SUM(total_amount) AS daily_rev
    FROM invoices
    GROUP BY head_id, invoice_date::DATE
),
low_revenue_days AS (
    -- Bước 2: Lọc các ngày dưới chỉ tiêu 50 triệu
    SELECT 
        head_id,
        report_date,
        daily_rev
    FROM daily_revenue
    WHERE daily_rev < 50000000
),
islands_grouped AS (
    -- Bước 3: THUẬT TOÁN CỐT LÕI: Lấy (Ngày thực tế - ROW_NUMBER ngày) để tạo Group ID bất biến
    SELECT 
        head_id,
        report_date,
        daily_rev,
        report_date - (ROW_NUMBER() OVER (PARTITION BY head_id ORDER BY report_date) * INTERVAL '1 day') AS island_group
    FROM low_revenue_days
),
island_aggregations AS (
    -- Bước 4: Gom nhóm theo island_group để tính độ dài chuỗi liên tiếp
    SELECT 
        head_id,
        MIN(report_date) AS streak_start_date,
        MAX(report_date) AS streak_end_date,
        COUNT(*) AS consecutive_days,
        SUM(daily_rev) AS total_actual_revenue,
        COUNT(*) * 50000000 - SUM(daily_rev) AS revenue_deficit
    FROM islands_grouped
    GROUP BY head_id, island_group
)
-- Bước 5: Lọc các chuỗi >= 3 ngày liên tiếp
SELECT 
    head_id,
    streak_start_date,
    streak_end_date,
    consecutive_days,
    total_actual_revenue,
    revenue_deficit
FROM island_aggregations
WHERE consecutive_days >= 3
ORDER BY consecutive_days DESC, revenue_deficit DESC;
```

#### 💡 Tại sao công thức `report_date - ROW_NUMBER()` lại tạo ra Group ID?
* Giả sử trạm 101 có các ngày lỗi: `2026-08-01` (Row 1), `2026-08-02` (Row 2), `2026-08-03` (Row 3).
  * `2026-08-01 - 1 day` = `2026-07-31`.
  * `2026-08-02 - 2 days` = `2026-07-31`.
  * `2026-08-03 - 3 days` = `2026-07-31`.
  $\implies$ **Tất cả các ngày liên tiếp đều cho ra cùng một mốc `2026-07-31`!** Khi chuỗi bị đứt quãng (Gap), hiệu số này sẽ nhảy sang một giá trị mới ngay lập tức. Đây là thuật toán kinh điển mà mọi Senior SQL đều phải thành thạo!

---

# BÀI 3: CƠN ÁC MỘNG ARRAY EXPLODE SKEW — XỬ LÝ MẢNG DỮ LIỆU KHỔNG LỒ TRÊN SPARK

---

### 3.1 Bối cảnh nghiệp vụ:
Bảng `work_orders` có trường `repair_items` là một `ARRAY<STRUCT<component_id: int, quantity: int, unit_price: double>>`.
* $99\%$ phiếu sửa chữa thông thường chỉ có 3 đến 5 linh kiện.
* Tuy nhiên, có **$0.1\%$ phiếu đại tu xe container dài hạn chứa tới $5.000$ linh kiện**.
* **Vấn đề**: Khi chạy `F.explode("repair_items")`, 1 dòng đại tu bùng nổ thành $5.000$ dòng trong RAM. Kèm theo toàn bộ 40 cột thông tin khác của work order $\implies$ **Gây lỗi Java Heap OOM (Out of Memory) trên Spark Executor!**

---

### 3.2 So sánh 2 Phiên bản Code PySpark:

#### ❌ Phiên bản 1: Baseline (Gây OOM Crash Executor)
```python
# BASELINE: Bùng nổ dòng trên toàn bộ các cột -> Executor OOM
df_orders = spark.read.parquet("hdfs://master:9000/lake/bronze/work_orders")

# Sai lầm: Giữ nguyên 40 cột rồi mới explode -> RAM nhân lên 5000 lần cho mỗi dòng lớn!
df_exploded_bad = df_orders.withColumn("item", F.explode("repair_items")) \
                           .select("id", "head_id", "created_at", "customer_id", "item.*", "driver_notes", "mechanic_notes")
```

#### ✅ Phiên bản 2: Optimized (Projection Pruning + Batched Explode + Broadcast Join)
```python
# ==============================================================================
# BÀI 3: OPTIMIZED ARRAY EXPLODE (CẮT GIẢM 90% BỘ NHỚ TRƯỚC KHI BÙNG NỔ DÒNG)
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SafeArrayExplode").getOrCreate()

df_orders = spark.read.parquet("hdfs://master:9000/lake/bronze/work_orders")

# Bước 1: Tách riêng ID và Cột Array ra để Explode (Tuyệt đối không kéo 40 cột khác theo)
df_items_only = df_orders.select("id", "repair_items") \
                         .filter(F.col("repair_items").isNotNull())

# Bước 2: Explode trên tập dữ liệu siêu hẹp (Chỉ có id + item)
df_items_exploded = df_items_only.select(
    F.col("id").alias("order_id"),
    F.explode_outer("repair_items").alias("item")
).select(
    "order_id",
    F.col("item.component_id").cast("int"),
    F.col("item.quantity").cast("int"),
    F.col("item.unit_price").cast("double"),
    (F.col("item.quantity") * F.col("item.unit_price")).alias("item_total_amount")
)

# Bước 3: Đọc bảng metadata của Work Order (Chỉ lấy các cột cần thiết cho Fact Table)
df_order_metadata = df_orders.select("id", "head_id", "customer_id", "created_at")

# Bước 4: JOIN lại với Metadata bằng Broadcast Join (Nếu bảng metadata nhỏ) hoặc Partitioned Join
df_fact_parts = df_items_exploded.join(
    df_order_metadata, 
    df_items_exploded.order_id == df_order_metadata.id, 
    "inner"
).drop(df_order_metadata.id)

# Bước 5: Ghi ra Fact Table với Coalesce
df_fact_parts.coalesce(4).write.mode("overwrite").parquet("hdfs://master:9000/dwh/gold/fact_parts_exploded")
```

---

# BÀI 4: KHỬ TRÙNG LẶP NÂNG CAO VỚI LATE-ARRIVING DATA & TIE-BREAKING MULTI-CRITERIA

---

### 4.1 Bối cảnh nghiệp vụ:
Do mạng 4G tại 50 trạm chập chờn, Kafka nhận về hàng ngàn bản ghi hóa đơn bị trùng lặp `invoice_id`. 
* Quy tắc Tie-breaking để chọn **Bản Ghi Cuối Cùng Chính Xác Nhất (Golden Record)**:
  1. Ưu tiên bản ghi có `updated_at` mới nhất.
  2. Nếu `updated_at` bằng nhau, ưu tiên bản ghi có `sync_version` lớn nhất.
  3. Nếu `sync_version` vẫn bằng nhau, ưu tiên bản ghi có `is_deleted = false`.

---

### 4.2 Giải pháp SQL (PostgreSQL `DISTINCT ON` vs Window `ROW_NUMBER`):

```sql
-- Cách 1: Sử dụng DISTINCT ON (Cực kỳ tối ưu trong PostgreSQL - Index Scan siêu nhanh)
SELECT DISTINCT ON (invoice_id)
    invoice_id,
    customer_id,
    head_id,
    total_amount,
    status,
    updated_at,
    sync_version,
    is_deleted
FROM raw_cdc_invoices
ORDER BY 
    invoice_id, 
    updated_at DESC, 
    sync_version DESC, 
    is_deleted ASC; -- is_deleted = false (0) xếp trước true (1)
```

---

### 4.3 Giải pháp PySpark (Tại sao `max_by()` nhanh gấp 3 lần `row_number()`?):

```python
# ==============================================================================
# BÀI 4: DEDUPLICATION TRÊN PYSPARK DÙNG MAX_BY (SPARK 3.3+)
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DeduplicationMaxBy").getOrCreate()

df_raw = spark.read.parquet("hdfs://master:9000/lake/bronze/raw_invoices")

# CÁCH 1: Dùng Window row_number() (Chậm vì phải Sort toàn bộ Partition trên Disk)
# window_spec = Window.partitionBy("invoice_id").orderBy(col("updated_at").desc(), col("sync_version").desc())
# df_dedup = df_raw.withColumn("rn", row_number().over(window_spec)).filter("rn = 1")

# CÁCH 2: DÙNG struct() + max_by() (TỐI ƯU GẤP 3X VÌ KHÔNG CẦN FULL SORT)
# Đóng gói các trường cần lấy vào struct, tìm struct có updated_at và sync_version lớn nhất
df_dedup_fast = df_raw.groupBy("invoice_id").agg(
    F.max_by(
        F.struct("customer_id", "head_id", "total_amount", "status", "updated_at", "sync_version", "is_deleted"),
        F.struct("updated_at", "sync_version", F.when(F.col("is_deleted") == False, 1).otherwise(0))
    ).alias("golden_record")
).select(
    "invoice_id",
    "golden_record.*"
)

df_dedup_fast.write.mode("overwrite").parquet("hdfs://master:9000/lake/silver/clean_invoices")
```

---

# BÀI 5: TÍNH TOÁN XUẤT KHO LINH KIỆN THEO NGUYÊN TẮC FIFO (GIÁ VỐN HÀNG BÁN - COGS)

---

### 5.1 Bối cảnh nghiệp vụ:
Khi xuất kho $15$ chiếc má phanh cho hóa đơn, kế toán yêu cầu tính **Giá vốn hàng bán (COGS - Cost of Goods Sold)** chính xác theo nguyên tắc **Nhập trước Xuất trước (FIFO)**:
* Lô nhập 1 (01/01): Nhập 10 chiếc giá $500.000\text{đ}$.
* Lô nhập 2 (15/01): Nhập 10 chiếc giá $550.000\text{đ}$.
* Đơn xuất ngày 20/01 cần $15$ chiếc $\implies$ Phải lấy **10 chiếc lô 1 ($5.000.000\text{đ}$) + 5 chiếc lô 2 ($2.750.000\text{đ}$)** $\to$ Tổng giá vốn = **$7.750.000\text{đ}$**.

---

### 5.2 Giải pháp PURE ADVANCED SQL (PostgreSQL):

```sql
-- ==============================================================================
-- BÀI 5: TÍNH GIÁ VỐN FIFO BẰNG RUNNING SUM INTERVAL MATCHING TRÊN SQL
-- ==============================================================================
WITH stock_in AS (
    -- Lô hàng nhập kho: Tính khoảng cộng dồn [start_qty, end_qty]
    SELECT 
        component_id,
        batch_id,
        receive_date,
        unit_cost,
        quantity AS in_qty,
        SUM(quantity) OVER (PARTITION BY component_id ORDER BY receive_date, batch_id) - quantity AS start_qty,
        SUM(quantity) OVER (PARTITION BY component_id ORDER BY receive_date, batch_id) AS end_qty
    FROM inventory_receipts
),
stock_out AS (
    -- Đơn hàng xuất kho: Tính khoảng cộng dồn [start_qty, end_qty]
    SELECT 
        component_id,
        issue_id,
        issue_date,
        quantity AS out_qty,
        SUM(quantity) OVER (PARTITION BY component_id ORDER BY issue_date, issue_id) - quantity AS start_qty,
        SUM(quantity) OVER (PARTITION BY component_id ORDER BY issue_date, issue_id) AS end_qty
    FROM inventory_issues
),
fifo_matching AS (
    -- Khớp các khoảng giao nhau (Interval Overlap) giữa Lô Nhập và Đơn Xuất
    SELECT 
        o.issue_id,
        o.issue_date,
        o.component_id,
        i.batch_id,
        i.unit_cost,
        -- Số lượng lấy từ lô = Min(end) - Max(start)
        LEAST(o.end_qty, i.end_qty) - GREATEST(o.start_qty, i.start_qty) AS allocated_qty
    FROM stock_out o
    JOIN stock_in i 
      ON o.component_id = i.component_id
     AND o.start_qty < i.end_qty 
     AND o.end_qty > i.start_qty
)
-- Tổng hợp Giá Vốn Hàng Bán (COGS) cho từng Đơn xuất kho
SELECT 
    issue_id,
    issue_date,
    component_id,
    SUM(allocated_qty) AS total_qty_issued,
    SUM(allocated_qty * unit_cost) AS total_cogs_amount,
    ROUND(SUM(allocated_qty * unit_cost) / SUM(allocated_qty), 2) AS weighted_unit_cost
FROM fifo_matching
GROUP BY issue_id, issue_date, component_id
ORDER BY issue_id;
```

---

# BÀI 6: PYSPARK STRUCTURED STREAMING — STATEFUL WATERMARKING & ROCKSDB STATESTORE TUNING

---

### 6.1 Bối cảnh nghiệp vụ:
Ingest luồng cảm biến nhiệt độ phanh từ $10.000$ xe tải qua Kafka.
* **Yêu cầu 1**: Khử trùng lặp tin nhắn dựa trên `event_id` trong vòng $1\text{ giờ}$.
* **Yêu cầu 2**: Tính nhiệt độ trung bình trượt trên **Cửa sổ 10 phút (Sliding Window 10m, slide 2m)**.
* **Yêu cầu 3**: Cho phép dữ liệu trễ tối đa $5\text{ phút}$ (**Watermark 5m**).
* **Vấn đề**: Bộ StateStore mặc định của Spark lưu trên JVM Heap bị tràn RAM (OOM) khi giữ trạng thái của $10.000$ xe tải trong 1 giờ.

---

### 6.2 Giải pháp PySpark Streaming với Cấu Hình RocksDB StateStore:

```python
# ==============================================================================
# BÀI 6: STATEFUL STREAMING VỚI ROCKSDB STATESTORE CHỐNG TRÀN RAM JVM
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Khởi tạo Spark Session kích hoạt RocksDB StateStore (Lưu state trên đĩa NVMe cục bộ)
spark = SparkSession.builder \
    .appName("FleetTelemetryStreaming_RocksDB") \
    .config("spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .config("spark.sql.shuffle.partitions", "12") \
    .getOrCreate()

# 2. Định nghĩa Schema gói tin Telemetry
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("truck_id", StringType(), False),
    StructField("brake_temp_celsius", DoubleType(), False),
    StructField("event_time", TimestampType(), False)
])

# 3. Đọc dòng dữ liệu từ Kafka Cluster
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "master:9092,slave1:9092,slave2:9092") \
    .option("subscribe", "iot.telemetry.sensors") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parse JSON và gán Watermark 5 phút
df_parsed = df_kafka.select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
                    .select("data.*") \
                    .withWatermark("event_time", "5 minutes")

# 5. Khử trùng lặp tin nhắn trong cửa sổ Watermark
df_dedup = df_parsed.dropDuplicates(["event_id", "event_time"])

# 6. Tính toán nhiệt độ trung bình trên Cửa sổ Trượt (Window 10m, Slide 2m)
df_windowed_avg = df_dedup.groupBy(
    F.window(F.col("event_time"), "10 minutes", "2 minutes"),
    F.col("truck_id")
).agg(
    F.avg("brake_temp_celsius").alias("avg_brake_temp"),
    F.max("brake_temp_celsius").alias("max_brake_temp"),
    F.count("event_id").alias("ping_count")
)

# 7. Phát hiện Cảnh Báo Khẩn Cấp (Nhiệt độ phanh > 300 độ C)
df_alerts = df_windowed_avg.filter("max_brake_temp > 300.0") \
                           .select(
                               F.col("window.start").alias("window_start"),
                               F.col("window.end").alias("window_end"),
                               "truck_id",
                               "avg_brake_temp",
                               "max_brake_temp"
                           )

# 8. Ghi dòng cảnh báo ra Redis / Kafka Topic để App Tài xế nhận tức thì
query = df_alerts.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://master:9000/checkpoints/telemetry_alerts") \
    .option("path", "hdfs://master:9000/lake/gold/realtime_brake_alerts") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```

#### 💡 Tại sao phải dùng `RocksDBStateStoreProvider`?
* Bộ lưu trữ trạng thái mặc định của Spark (`HDFSBackedStateStoreProvider`) lưu toàn bộ State trên bộ nhớ **JVM Heap Memory**. Khi theo dõi $10.000$ xe tải trong 1 giờ với hàng triệu key, Heap Memory sẽ nhanh chóng cạn kiệt, gây **Java Garbage Collection Pause hàng chục giây và làm sập Streaming Query**.
* **RocksDB StateStore** lưu trữ State trên bộ nhớ Off-heap và file C++ nhị phân cục bộ trên đĩa SSD, chỉ giữ dữ liệu nóng trong block cache $\implies$ **Cho phép State mở rộng lên hàng chục Gigabytes mà RAM JVM vẫn hoàn toàn ổn định!**

---

### 🏆 TỔNG KẾT BẢN LĨNH 6 BÀI THỬ THÁCH:
1. **Làm chủ Advanced SQL**: Thành thạo Sessionization, Gaps & Islands, Interval Overlap Matching tính FIFO COGS, và `DISTINCT ON` tie-breaking.
2. **Làm chủ PySpark Optimization**: Thành thạo Salting 2 giai đoạn, Projection Pruning cho mảng Explode, `max_by()` struct deduplication, và RocksDB Stateful Streaming.
3. **Nắm chắc nguyên lý hệ thống bên dưới**: Hiểu rõ sự khác biệt giữa Sort-based vs Hash-based aggregation, JVM Heap vs Off-heap RocksDB, và cách tránh tràn đĩa (Disk Spill).
