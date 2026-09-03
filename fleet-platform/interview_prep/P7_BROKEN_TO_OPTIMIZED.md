# P7 — BROKEN → OPTIMIZED (Trụ cột 7 — Ngày 12)
*5 bài: Code/Config KÉM → Liệt kê vấn đề → Code/Config TỐT → Giải thích metrics improvement*

---

## Bài 1: SPARK JOB KÉM → TỐI ƯU

### ❌ Phiên bản BROKEN

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("broken_revenue_report") \
    .config("spark.sql.shuffle.partitions", 200)  # ❌ Mặc định 200 cho mọi job
    .getOrCreate()

# ❌ Đọc TOÀN BỘ HDFS không filter partition
df_invoices = spark.read.parquet("hdfs:///data/invoices/")

# ❌ Filter SAU khi đã đọc hết data
df_2026 = df_invoices.filter("year = 2026 AND month = 6")

# ❌ Broadcast join bảng 10GB (quá lớn cho broadcast)
df_heads = spark.read.parquet("hdfs:///data/heads/")  # 10GB!
df_joined = df_2026.join(
    spark.sparkContext.broadcast(df_heads),  # ❌ OOM risk
    "head_id"
)

# ❌ Collect toàn bộ kết quả về driver
result = df_joined.groupBy("head_name") \
    .sum("total_amount") \
    .collect()  # ❌ Driver OOM nếu result lớn

print(result)
```

### 🔍 Vấn đề phát hiện (5 lỗi)

| # | Vấn đề | Hậu quả |
|---|---|---|
| 1 | **Không partition pruning** — đọc toàn bộ HDFS rồi mới filter | Đọc 100GB thay vì 2GB (50x thừa) |
| 2 | **shuffle.partitions = 200** cố định | Dữ liệu nhỏ: 200 partitions quá nhiều → overhead. Dữ liệu lớn: 200 có thể quá ít |
| 3 | **Broadcast 10GB** | Broadcast threshold mặc định 10MB. 10GB → serialization chậm, OOM driver/executor |
| 4 | **collect() toàn bộ** | Kéo hết data về driver → OOM nếu kết quả lớn |
| 5 | **Không AQE** | Không tự điều chỉnh shuffle partitions, skew join |

### ✅ Phiên bản OPTIMIZED

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("optimized_revenue_report") \
    .config("spark.sql.adaptive.enabled", "true")            # ✅ AQE
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")  # ✅ Auto-coalesce
    .config("spark.sql.adaptive.skewJoin.enabled", "true")    # ✅ Auto skew handling
    .getOrCreate()

# ✅ Partition pruning — chỉ đọc partition cần thiết
df_invoices = spark.read.parquet("hdfs:///data/invoices/year=2026/month=6/")
# Hoặc: spark.read.parquet("hdfs:///data/invoices/").filter("year = 2026 AND month = 6")
# → Parquet predicate pushdown tự động skip partitions không match

# ✅ Sort-Merge Join cho bảng lớn (10GB không broadcast được)
df_heads = spark.read.parquet("hdfs:///data/heads/")
df_joined = df_invoices.join(df_heads, "head_id")  # Spark tự chọn Sort-Merge Join

# ✅ Ghi kết quả ra HDFS thay vì collect
df_result = df_joined.groupBy("head_name").sum("total_amount")
df_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs:///output/revenue_report/year=2026/month=6/"
)
```

### 📊 Metrics Improvement

| Metric | Broken | Optimized | Cải thiện |
|---|---|---|---|
| Data scanned | 100 GB | **2 GB** | **50x ít hơn** |
| Shuffle partitions | 200 (cố định) | **Auto** (AQE) | Tự điều chỉnh |
| Join strategy | Broadcast 10GB (OOM) | **Sort-Merge** (ổn định) | Không OOM |
| Runtime | ~45 phút | **~3 phút** | **15x nhanh hơn** |

---

## Bài 2: SQL QUERY KÉM → TỐI ƯU VỚI EXPLAIN ANALYZE

### ❌ Phiên bản BROKEN

```sql
-- Query: Tìm Top 10 khách hàng theo doanh thu tháng 6/2026
SELECT *
FROM invoices i
JOIN customers c ON c.id = i.customer_id
WHERE CAST(i.invoice_date AS TEXT) LIKE '2026-06%'  -- ❌ Implicit cast → Seq Scan
  AND i.status = 'paid'
ORDER BY i.total_amount DESC
LIMIT 10;
```

### EXPLAIN ANALYZE kết quả (Broken)

```
Limit  (actual time=45230.12..45230.15 rows=10)
  -> Sort  (actual time=45230.10..45230.12 rows=10)
      -> Nested Loop  (actual time=120.50..44980.30 rows=85000)       ← ❌ Nested Loop
          -> Seq Scan on invoices  (actual time=0.05..38000.20 rows=1200000)  ← ❌ SEQ SCAN!
               Filter: (status = 'paid' AND (invoice_date::text) LIKE '2026-06%')
               Rows Removed by Filter: 1100000
          -> Index Scan on customers  (actual time=0.005..0.008 rows=1)
Planning Time: 0.5 ms
Execution Time: 45230.50 ms   ← ❌ 45 GIÂY!
```

### 🔍 Vấn đề phát hiện

| # | Vấn đề | Trong EXPLAIN |
|---|---|---|
| 1 | **`CAST(invoice_date AS TEXT)`** → index trên invoice_date bị vô hiệu | `Seq Scan` thay vì Index Scan |
| 2 | **Nested Loop Join** trên 1.2M rows | `Nested Loop` — O(n × m) |
| 3 | **SELECT *** — đọc hết cột | Disk I/O thừa |
| 4 | **Không có composite index** cho (status, invoice_date) | Filter loại 1.1M/1.2M rows = 92% scan thừa |

### ✅ Phiên bản OPTIMIZED

```sql
-- Bước 1: Tạo composite partial index
CREATE INDEX idx_invoices_paid_date 
ON invoices (invoice_date DESC, customer_id, total_amount)
WHERE status = 'paid';
-- Partial index: chỉ index rows có status='paid' → nhỏ hơn 50%

-- Bước 2: Query tối ưu
SELECT i.invoice_date, i.total_amount, i.customer_id,
       c.company_name
FROM invoices i
JOIN customers c ON c.id = i.customer_id
WHERE i.invoice_date >= '2026-06-01'        -- ✅ Dùng date comparison trực tiếp
  AND i.invoice_date < '2026-07-01'         -- ✅ Range scan trên index
  AND i.status = 'paid'
ORDER BY i.total_amount DESC
LIMIT 10;
```

### EXPLAIN ANALYZE kết quả (Optimized)

```
Limit  (actual time=0.15..0.20 rows=10)
  -> Nested Loop  (actual time=0.15..0.19 rows=10)
      -> Index Scan using idx_invoices_paid_date on invoices  ← ✅ INDEX SCAN
            (actual time=0.10..0.13 rows=10)
            Index Cond: (invoice_date >= '2026-06-01' AND invoice_date < '2026-07-01')
      -> Index Scan on customers_pkey  (actual time=0.003..0.003 rows=1)
Planning Time: 0.3 ms
Execution Time: 0.22 ms   ← ✅ 0.2 MILI-GIÂY!
```

### 📊 Metrics: **45s → 0.2ms (225,000x nhanh hơn)**

---

## Bài 3: PIPELINE KÉM → TỐI ƯU

### ❌ Phiên bản BROKEN

```python
# Pipeline xử lý CDC events — KHÔNG CÓ error handling
def process_cdc_events(events):
    for event in events:
        data = json.loads(event.value)       # ❌ Crash nếu malformed JSON
        
        # ❌ Không retry — network error = data loss
        db.execute(
            "INSERT INTO dim_customer VALUES (%s, %s, %s)",
            (data['id'], data['name'], data['status'])
        )  # ❌ Không idempotent — retry = duplicate!
        
        consumer.commit()  # ❌ Commit NGAY sau mỗi record (chậm)
```

### 🔍 Vấn đề

| # | Vấn đề | Hậu quả |
|---|---|---|
| 1 | **Không try/except** | 1 malformed JSON → crash toàn bộ pipeline |
| 2 | **Không retry** | Network glitch → mất data vĩnh viễn |
| 3 | **INSERT (không idempotent)** | Retry → duplicate records |
| 4 | **Không DLQ** | Poison message → block pipeline mãi mãi |
| 5 | **Commit từng record** | 1000 commits/s thay vì 1 commit/batch |

### ✅ Phiên bản OPTIMIZED

```python
import json
import time
from tenacity import retry, stop_after_attempt, wait_exponential

DLQ_TOPIC = "cdc-dead-letter-queue"

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10)  # 1s, 2s, 4s
)
def upsert_customer(data):
    """✅ Idempotent UPSERT — retry an toàn, không duplicate"""
    db.execute("""
        INSERT INTO dim_customer (id, name, status, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at
        WHERE dim_customer.name <> EXCLUDED.name
           OR dim_customer.status <> EXCLUDED.status
    """, (data['id'], data['name'], data['status']))

def process_cdc_events(events):
    batch_count = 0
    for event in events:
        try:
            data = json.loads(event.value)
        except json.JSONDecodeError:
            # ✅ Malformed JSON → DLQ (không block pipeline)
            producer.send(DLQ_TOPIC, event.value)
            continue
        
        try:
            upsert_customer(data)  # ✅ Retry 3 lần với exponential backoff
            batch_count += 1
        except Exception as e:
            # ✅ Retry exhausted → DLQ
            producer.send(DLQ_TOPIC, event.value)
            log.error(f"Failed after 3 retries: {e}")
    
    # ✅ Batch commit — 1 commit per batch thay vì per record
    consumer.commit()
    log.info(f"Committed batch of {batch_count} records")
```

### 📊 Improvement

| Metric | Broken | Optimized |
|---|---|---|
| Crash on malformed data | Pipeline stops | DLQ routing, pipeline continues |
| Network error handling | Data loss | 3 retries + exponential backoff |
| Duplicate on retry | ✅ Có | ❌ Không (idempotent UPSERT) |
| Commit overhead | 1000 commits/s | 1 commit/batch |

---

## Bài 4: STREAMING KÉM → TỐI ƯU

### ❌ Phiên bản BROKEN

```python
# Spark Structured Streaming — BROKEN config
df_stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "fleet-cdc-invoices") \
    .load()

# ❌ Không watermark → state vô hạn → OOM sau vài ngày
windowed = df_stream \
    .groupBy(
        window(col("event_time"), "1 hour"),  # ❌ Giữ state MỌI window vĩnh viễn
        col("head_id")
    ) \
    .sum("amount")

# ❌ Trigger quá nhanh → hàng nghìn small files/ngày
query = windowed.writeStream \
    .format("parquet") \
    .trigger(processingTime="100 milliseconds")  # ❌ 36,000 files/giờ!
    .option("path", "hdfs:///streaming_output/")
    # ❌ Không checkpoint → restart = reprocess ALL
    .start()
```

### 🔍 Vấn đề

| # | Vấn đề | Hậu quả |
|---|---|---|
| 1 | **Không watermark** | State tăng vô hạn → OOM sau 2-3 ngày |
| 2 | **Trigger 100ms** | 36,000 files/giờ → HDFS NameNode áp lực, small file problem |
| 3 | **Không checkpoint** | Restart → reprocess từ đầu (mất progress) |
| 4 | **Không monitoring** | Không biết bao nhiêu rows bị drop bởi watermark |

### ✅ Phiên bản OPTIMIZED

```python
df_stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "fleet-cdc-invoices") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_time")
).select("data.*", "kafka_time")

# ✅ Watermark 10 phút — state chỉ giữ windows trong 10 phút gần nhất
windowed = parsed \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("head_id")
    ) \
    .sum("amount")

# ✅ Trigger 30 giây — cân bằng latency vs file count
query = windowed.writeStream \
    .format("parquet") \
    .trigger(processingTime="30 seconds") \
    .option("path", "hdfs:///streaming_output/") \
    .option("checkpointLocation", "hdfs:///checkpoints/invoices_stream/")  # ✅ Checkpoint
    .outputMode("append") \
    .start()

# ✅ Monitor trong Spark UI hoặc custom metrics
# query.lastProgress["numRowsDroppedByWatermark"]
```

```python
# ✅ Airflow nightly compaction job — gộp small files
def compact_streaming_output(ds):
    spark.read.parquet(f"hdfs:///streaming_output/date={ds}/") \
        .coalesce(4) \
        .write.mode("overwrite") \
        .parquet(f"hdfs:///streaming_output_compacted/date={ds}/")
```

### 📊 Improvement

| Metric | Broken | Optimized |
|---|---|---|
| State growth | Vô hạn (OOM 2-3 ngày) | Giới hạn ~10 phút |
| Files/ngày | ~864,000 (trigger 100ms) | **~2,880** (trigger 30s) → compaction → **~4 files** |
| Restart behavior | Reprocess từ đầu | Resume từ checkpoint |
| Late data | Giữ mãi mãi | Drop sau 10 min (configurable) |

---

## Bài 5: DATA SKEW → TỐI ƯU BẰNG SALTING 🔥

### ❌ Phiên bản BROKEN — GroupBy trên key bị skew

```python
# Tính tổng doanh thu theo customer_id
# Vấn đề: 1 customer (doanh nghiệp lớn) chiếm 40% dữ liệu
df_invoices = spark.read.parquet("hdfs:///data/invoices/")

# ❌ GroupBy trực tiếp → 1 partition chứa 40% data
result = df_invoices.groupBy("customer_id").agg(
    sum("total_amount").alias("total_revenue")
)
result.write.parquet("hdfs:///output/customer_revenue/")
```

**Spark UI cho thấy:**
```
Task  0: 1 min   (    5,000 records)
Task  1: 1 min   (    5,000 records)
...
Task 99: 1 min   (    5,000 records)
Task  5: 30 min  (2,000,000 records)  ← ❌ SKEW! 1 task chạy 30x lâu hơn
```

### 🔍 Vấn đề
- **Data skew**: Customer "CORP_001" có 2 triệu invoices, 99 customers còn lại mỗi cái chỉ 5,000
- Spark chia data theo hash(customer_id) → tất cả records của CORP_001 vào **1 partition duy nhất**
- 99 tasks xong trong 1 phút, nhưng job phải **đợi 30 phút** cho task cuối cùng

### ✅ Phiên bản OPTIMIZED — Salting Technique (2-Stage Aggregation)

```python
from pyspark.sql.functions import lit, rand, floor, concat, col, sum as _sum

df_invoices = spark.read.parquet("hdfs:///data/invoices/")

# ====== STAGE 1: Aggregate theo SALTED KEY ======
NUM_SALT = 10  # Chia customer skewed thành 10 phần

# Thêm random salt 0-9 vào customer_id
df_salted = df_invoices.withColumn(
    "salted_key",
    concat(col("customer_id"), lit("_"), floor(rand() * NUM_SALT).cast("int"))
)
# customer_id "CORP_001" → "CORP_001_0", "CORP_001_1", ..., "CORP_001_9"
# → 2 triệu records chia đều thành 10 partitions, mỗi partition ~200K records

# Partial aggregation theo salted key
df_partial = df_salted.groupBy("salted_key").agg(
    _sum("total_amount").alias("partial_revenue")
)

# ====== STAGE 2: Final aggregation — bỏ salt, gộp lại ======
df_final = df_partial.withColumn(
    "customer_id",
    # Bỏ phần "_0", "_1", ... để lấy lại customer_id gốc
    regexp_extract(col("salted_key"), r"^(.+)_\d+$", 1)
).groupBy("customer_id").agg(
    _sum("partial_revenue").alias("total_revenue")
)

df_final.write.parquet("hdfs:///output/customer_revenue/")
```

**Spark UI SAU tối ưu:**
```
Stage 1 (Salted GroupBy):
  Task  0-99: ~1 min each  (mỗi task ~20K-200K records, KHÔNG SKEW)

Stage 2 (Final GroupBy):
  Task  0-99: ~10 sec each (chỉ gộp partial sums, rất nhẹ)

TỔNG: ~1.5 min  ← thay vì 30 min!
```

### Giải pháp thay thế: AQE Skew Join (Spark 3.0+)

```python
# Spark 3.0+ tự động detect và xử lý skew trong JOIN
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)  # partition > 5x median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# AQE tự phát hiện partition skewed và split thành nhiều phần nhỏ hơn
# → Không cần manual salting cho JOIN operations
```

### Khi nào dùng Salting vs AQE?

| | Manual Salting | AQE Skew Join |
|---|---|---|
| Spark version | Mọi version | **3.0+** |
| Operation | **GroupBy / Aggregate** | **JOIN** |
| Control | Toàn quyền kiểm soát | Tự động |
| Overhead | 2-stage aggregation | Minimal |
| Fleet dùng gì | Salting cho GroupBy revenue, AQE cho JOIN fact×dim |

### 📊 Metrics

| Metric | Broken | Optimized (Salting) |
|---|---|---|
| Longest task | 30 phút | **1.5 phút** |
| Total runtime | 30 phút (đợi task chậm nhất) | **1.5 phút** |
| Data per partition | 5K - 2M (skewed 400x) | 20K - 200K (**balanced**) |
| Cải thiện | | **20x nhanh hơn** |

---

## 📋 CHECKLIST ÔN TẬP TRỤ CỘT 7

- [ ] Xem Spark code broken → chỉ ra 5 lỗi trong 2 phút
- [ ] Viết EXPLAIN ANALYZE cho SQL query kém, chỉ ra Seq Scan và giải pháp index
- [ ] Giải thích DLQ + idempotent UPSERT + exponential backoff retry
- [ ] Giải thích watermark + checkpoint + trigger interval trade-off
- [ ] Code Salting technique từ đầu (2-stage aggregation) không nhìn mẫu
- [ ] Phân biệt khi nào dùng Salting (GroupBy) vs AQE (JOIN)
