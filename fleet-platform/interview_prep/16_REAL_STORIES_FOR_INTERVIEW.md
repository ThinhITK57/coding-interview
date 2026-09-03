# 🎯 2 CÂU CHUYỆN THỰC TẾ TỪ DỰ ÁN FLEET — DÙNG CHO PHỎNG VẤN
*Chuẩn bị theo framework STAR (Situation → Task → Action → Result). Mỗi câu chuyện kết nối 2-3 pattern kỹ thuật trong cùng 1 kịch bản thực tế, thể hiện tư duy hệ thống của Senior DE.*

---

## CÂU CHUYỆN 1: "Khi khách hàng đổi trạng thái nhưng báo cáo phải giữ nguyên lịch sử"
**Pattern kỹ thuật**: SCD Type 2 + Debezium CDC + Incremental Load (MERGE)

---

### 🔍 SITUATION — Bối cảnh

Tôi tham gia xây dựng Data Platform cho hệ thống chuỗi trạm dịch vụ sửa chữa xe tải. Hệ thống OLTP chạy trên PostgreSQL (mô phỏng Odoo ERP) quản lý khoảng 50 trạm dịch vụ trên cả nước, với hàng trăm khách hàng là các công ty vận tải.

Trong nghiệp vụ, khách hàng có vòng đời trạng thái: `mới` → `cũ` → `thường niên`. Mỗi trạng thái tương ứng với chính sách giá dịch vụ và chiết khấu linh kiện khác nhau. Đội ngũ kinh doanh cập nhật trạng thái khách hàng trực tiếp trên Odoo.

### 🎯 TASK — Vấn đề cần giải quyết

Ban Giám đốc yêu cầu báo cáo phân tích doanh thu theo **trạng thái khách hàng tại thời điểm phát sinh hóa đơn**, không phải trạng thái hiện tại. Cụ thể:

> *"Tháng 7, Công ty Vận Tải A còn là khách 'mới' nên được giảm 20%. Tháng 8 họ chuyển thành 'thường niên'. Nếu báo cáo ghi nhận tất cả hóa đơn của A đều ở trạng thái 'thường niên' thì sai hoàn toàn — ta không thể phân tích được hiệu quả chương trình ưu đãi khách mới."*

Nếu dùng cách thông thường (`UPDATE dim_customer SET status = 'thường niên'`), toàn bộ lịch sử trạng thái bị ghi đè → **mất truy vết**.

### ⚙️ ACTION — Giải pháp kỹ thuật

Tôi thiết kế pipeline 3 tầng:

**Tầng 1: CDC (Change Data Capture) — Bắt mọi thay đổi từ nguồn**

Thay vì viết cronjob `SELECT * FROM customers WHERE updated_at > last_run` (polling), tôi dùng **Debezium Log-Based CDC** đọc trực tiếp PostgreSQL WAL log:

```sql
-- Cấu hình BẮT BUỘC trên PostgreSQL cho SCD2:
ALTER TABLE customers REPLICA IDENTITY FULL;
-- → Debezium gửi ĐẦY ĐỦ giá trị cũ + mới trong mỗi CDC event
```

Tại sao phải `REPLICA IDENTITY FULL`? Vì nếu để `DEFAULT`, khi khách hàng đổi status:
```json
// REPLICA IDENTITY DEFAULT → before chỉ có Primary Key:
{"before": {"id": 1}, "after": {"id": 1, "status": "thường niên"}}
// ❌ Không biết status CŨ là gì → không thể so sánh để quyết định đóng/mở SCD2 record

// REPLICA IDENTITY FULL → before có ĐẦY ĐỦ tất cả cột:
{"before": {"id": 1, "company_name": "VT A", "status": "mới"}, 
 "after":  {"id": 1, "company_name": "VT A", "status": "thường niên"}}
// ✅ Biết status đổi từ "mới" → "thường niên" → trigger SCD2 merge
```

Lý do chọn CDC thay vì Polling:
- **0% query load** lên Odoo DB (đọc WAL file, không chạy SELECT)
- **Bắt 100% DELETE events** (polling không thấy row đã xóa)
- **Bắt trạng thái trung gian**: Nếu khách đổi `mới → cũ → thường niên` trong 5 phút, polling chỉ thấy `thường niên`, CDC bắt cả 2 transition

**Tầng 2: SCD Type 2 Merge — Giữ nguyên lịch sử trên Data Warehouse**

CDC events từ Kafka được Spark Batch job xử lý theo thuật toán 5 bước:

```sql
-- Bảng dim_customer SCD2 trên Data Warehouse (HDFS Parquet):
-- customer_key (MD5 surrogate), customer_id, company_name, status,
-- effective_date, expiration_date, is_current

-- BƯỚC 1: Giữ nguyên 100% bản ghi lịch sử đã đóng (KHÔNG ĐỤNG VÀO)
SELECT * FROM dim_customer WHERE is_current = FALSE;

-- BƯỚC 2: Join bản ghi ĐANG ACTIVE với CDC mới
SELECT old.*, cdc.status AS new_status
FROM dim_customer old
LEFT JOIN cdc_batch cdc ON old.customer_id = cdc.customer_id
WHERE old.is_current = TRUE;

-- BƯỚC 3: ĐÓNG bản ghi active bị thay đổi status
UPDATE dim_customer
SET expiration_date = CURRENT_DATE, is_current = FALSE
WHERE customer_id = 1 AND is_current = TRUE
  AND status <> 'thường niên';

-- BƯỚC 4: Giữ nguyên active records KHÔNG đổi (skip)

-- BƯỚC 5: TẠO bản ghi mới cho khách đổi status + khách mới hoàn toàn
INSERT INTO dim_customer (customer_key, customer_id, company_name, status,
                          effective_date, expiration_date, is_current)
VALUES (
    MD5(CONCAT('1', '_', '2026-08-01', '_', 'thường niên')),
    1, 'Công ty Vận Tải A', 'thường niên',
    '2026-08-01', '9999-12-31', TRUE
);
-- Khách hàng mới chưa từng có → dùng LEFT ANTI JOIN để tìm và INSERT
```

Tại sao Surrogate Key dùng `MD5(customer_id + effective_date + status)` thay vì auto-increment?
→ Vì 1 customer_id có NHIỀU records trong SCD2 (mỗi phiên bản 1 record). MD5 hash tạo ra key duy nhất cho từng phiên bản, và deterministic — chạy lại pipeline cho cùng dữ liệu sẽ tạo ra cùng key (idempotent).

**Tầng 3: Incremental Load — Chỉ xử lý phần thay đổi**

Pipeline không load lại toàn bộ dim_customer mỗi đêm. Spark job chỉ đọc CDC events từ Kafka topic trong khoảng thời gian kể từ lần chạy cuối:

```python
# PySpark: Đọc incremental từ Kafka (offset-based)
cdc_df = (
    spark.read.format("kafka")
    .option("startingOffsets", last_committed_offset)  # Watermark
    .option("endingOffsets", "latest")
    .load()
)
# Chỉ xử lý delta, không quét lại toàn bộ bảng nguồn
```

Idempotency đảm bảo bằng `spark.sql.sources.partitionOverwriteMode=dynamic` — dù job chạy lại 2 lần cũng không bị duplicate records.

### ✅ RESULT — Kết quả

```sql
-- Query báo cáo CHÍNH XÁC trạng thái khách hàng tại thời điểm hóa đơn:
SELECT 
    i.invoice_number, 
    i.invoice_date,
    dc.status AS customer_status_at_invoice_time,
    i.total_amount,
    CASE dc.status
        WHEN 'mới' THEN i.total_amount * 0.80       -- Giảm 20%
        WHEN 'cũ'  THEN i.total_amount * 0.90       -- Giảm 10%
        ELSE i.total_amount                           -- Giá gốc
    END AS actual_revenue_after_discount
FROM invoices i
JOIN dim_customer dc
  ON i.customer_id = dc.customer_id
 AND i.invoice_date BETWEEN dc.effective_date 
     AND COALESCE(dc.expiration_date, '9999-12-31');
```

Kết quả cụ thể:
- Báo cáo **chính xác 100%** trạng thái khách hàng tại từng thời điểm phát sinh hóa đơn
- Ban Giám đốc phân tích được hiệu quả chương trình ưu đãi khách mới (mới → thường niên conversion rate)
- CDC giảm **0% query load** lên Odoo DB so với giải pháp polling trước đó
- Pipeline chạy incremental — xử lý ~vài trăm CDC events mỗi batch thay vì quét lại toàn bộ bảng customers

### 🎤 Cách kể trong phỏng vấn (< 3 phút):

> *"Trong dự án Fleet Platform, Ban Giám đốc cần báo cáo doanh thu theo trạng thái khách hàng **tại thời điểm phát sinh hóa đơn**, không phải trạng thái hiện tại. Nếu chỉ ghi đè (SCD1), toàn bộ lịch sử bị mất.*
>
> *Tôi thiết kế pipeline 3 tầng: Debezium CDC đọc WAL log từ PostgreSQL — cấu hình REPLICA IDENTITY FULL để có đầy đủ before/after image. CDC events được Kafka buffer rồi Spark Batch job merge vào bảng dim_customer theo thuật toán SCD Type 2 — 5 bước: giữ lịch sử cũ, join active với CDC mới, đóng records bị đổi, giữ records không đổi, tạo records mới với surrogate key MD5.*
>
> *Kết quả: báo cáo chính xác 100%, pipeline idempotent nhờ Dynamic Partition Overwrite, và CDC giảm query load lên OLTP DB xuống 0%."*

---

## CÂU CHUYỆN 2: "Khi pipeline âm thầm chết 2 ngày mà không ai biết"
**Pattern kỹ thuật**: Data Quality Gate + Freshness Check + WAL Monitoring + Reconciliation

---

### 🔍 SITUATION — Bối cảnh

Hệ thống Fleet Platform chạy ổn định được 2 tuần sau khi go-live. Pipeline: Debezium CDC → Kafka → Spark Streaming → HDFS Data Lake → Spark Batch → HDFS Data Warehouse → Redis Serving → WebSocket Dashboard.

Lúc đó, hệ thống chưa có monitoring đầy đủ — chỉ có Airflow ghi log mỗi lần DAG chạy.

### 🎯 TASK — Sự cố

Sáng thứ Hai, Ban Giám đốc mở Dashboard và thấy **số liệu doanh thu cuối tuần bằng 0**. Nhưng thực tế, các trạm dịch vụ vẫn hoạt động bình thường, có hàng chục hóa đơn được tạo trên Odoo.

Sau khi điều tra, phát hiện **Debezium connector bị crash vào tối thứ Sáu** (do Java OutOfMemoryError trên Kafka Connect worker). Hậu quả:
1. CDC events ngừng phát sinh → Kafka không có data mới
2. Spark Streaming job vẫn chạy nhưng micro-batch rỗng (0 records) → không có gì để ghi HDFS
3. Spark Batch DWH job chạy bình thường nhưng tổng hợp trên dữ liệu cũ → báo cáo **sai nhưng không báo lỗi**
4. PostgreSQL tích tụ WAL files suốt 2 ngày → **ổ đĩa gần đầy 95%** (nguy cơ crash DB OLTP)

Vấn đề nghiêm trọng nhất: **Không có cơ chế nào phát hiện dữ liệu bị trễ/thiếu**. Pipeline chạy "thành công" (status=success) nhưng output sai.

### ⚙️ ACTION — Xây dựng hệ thống Data Quality Gate 5 tầng

Sau sự cố, tôi thiết kế 5 lớp kiểm tra tự động chạy qua Airflow:

**Tầng 1: PostgreSQL Replication Slot Monitoring (Phát hiện CDC chết)**

```sql
-- Check 1: Debezium connector còn hoạt động không?
SELECT 
    slot_name,
    active,                                          -- FALSE = connector đã chết!
    pg_wal_lsn_diff(
        pg_current_wal_lsn(), 
        confirmed_flush_lsn
    ) AS wal_lag_bytes,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
    ) AS wal_lag_human,
    CASE
        WHEN NOT active 
            THEN '🚨 CRITICAL: Debezium connector INACTIVE — WAL đang tích tụ!'
        WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) > 5368709120
            THEN '🚨 CRITICAL: WAL lag > 5GB — nguy cơ đầy đĩa PostgreSQL!'
        WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) > 1073741824
            THEN '⚠️ WARNING: WAL lag > 1GB — Debezium đang chậm'
        ELSE '✅ HEALTHY'
    END AS health_status
FROM pg_replication_slots
WHERE slot_name = 'debezium';
```

Đồng thời cấu hình phòng thủ trong `postgresql.conf`:
```
max_slot_wal_keep_size = 10GB
# → Nếu Debezium lag > 10GB WAL, Postgres TỰ ĐỘNG invalidated slot
# → Ưu tiên bảo vệ sự sống còn của OLTP DB
```

**Tầng 2: Kafka Consumer Lag Check**

```bash
# Chạy qua Airflow BashOperator mỗi 5 phút:
kafka-consumer-groups.sh --bootstrap-server master:9092 \
  --group fleet-spark-streaming --describe

# Parse output, alert nếu LAG > 100,000 messages
```

**Tầng 3: Freshness Check — Dữ liệu có bị trễ so với kỳ vọng không?**

```sql
-- Chạy sau mỗi Batch ETL job:
SELECT
    'fact_repair_service_revenue' AS table_name,
    MAX(loaded_at) AS last_loaded_at,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(loaded_at))) / 3600 AS hours_since_load,
    CASE
        WHEN CURRENT_TIMESTAMP - MAX(loaded_at) > INTERVAL '4 hours'
        THEN '🚨 STALE DATA: Không có dữ liệu mới trong 4 giờ!'
        ELSE '✅ FRESH'
    END AS freshness_status
FROM fact_repair_service_revenue;
-- Nếu status = STALE → Airflow trigger alert, CHẶN downstream job
```

**Tầng 4: Volume Anomaly Detection — Số dòng có bất thường không?**

```sql
WITH daily_volumes AS (
    SELECT 
        DATE(loaded_at) AS load_date,
        COUNT(*) AS row_count
    FROM fact_repair_service_revenue
    WHERE loaded_at >= CURRENT_DATE - INTERVAL '8 days'
    GROUP BY DATE(loaded_at)
),
stats AS (
    SELECT
        AVG(row_count) FILTER (WHERE load_date < CURRENT_DATE) AS avg_7d,
        STDDEV(row_count) FILTER (WHERE load_date < CURRENT_DATE) AS stddev_7d,
        MAX(row_count) FILTER (WHERE load_date = CURRENT_DATE) AS today_count
    FROM daily_volumes
)
SELECT 
    today_count, 
    ROUND(avg_7d, 0) AS avg_7d,
    CASE
        -- Hôm nay = 0 rows hoặc giảm > 2 độ lệch chuẩn → bất thường
        WHEN today_count = 0 
            THEN '🚨 CRITICAL: 0 rows hôm nay — pipeline có thể đã chết!'
        WHEN today_count < avg_7d - 2 * stddev_7d 
            THEN '⚠️ WARNING: Số rows giảm bất thường so với 7 ngày qua'
        ELSE '✅ NORMAL'
    END AS volume_status
FROM stats;
```

**Tầng 5: Reconciliation — Đối soát nguồn/đích sau ETL**

```sql
-- Đếm số hóa đơn trên OLTP source (PostgreSQL)
-- vs số hóa đơn trên DWH (HDFS fact table) cho cùng ngày
WITH source_count AS (
    SELECT COUNT(*) AS cnt 
    FROM invoices 
    WHERE invoice_date = CURRENT_DATE AND status = 'paid'
),
target_count AS (
    SELECT COUNT(*) AS cnt 
    FROM fact_repair_service_revenue 
    WHERE date_key = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INT
)
SELECT 
    s.cnt AS source_rows, 
    t.cnt AS target_rows,
    s.cnt - t.cnt AS difference,
    CASE 
        WHEN s.cnt <> t.cnt 
        THEN '🚨 MISMATCH: Mất ' || (s.cnt - t.cnt) || ' rows trong quá trình ETL!'
        ELSE '✅ MATCH'
    END AS reconciliation_status
FROM source_count s, target_count t;
```

**Tích hợp thành Data Quality Gate trong Airflow:**

```python
# Airflow DAG: data_quality_gate_dag.py
from airflow.operators.python import ShortCircuitOperator

def check_all_quality_gates(**context):
    """Chạy 5 checks. Nếu BẤT KỲ check nào FAIL → return False → chặn downstream."""
    results = {
        'replication_slot': check_pg_replication_slot(),   # Tầng 1
        'kafka_lag': check_kafka_consumer_lag(),            # Tầng 2
        'freshness': check_data_freshness(),                # Tầng 3
        'volume': check_volume_anomaly(),                   # Tầng 4
        'reconciliation': check_row_count_match(),          # Tầng 5
    }
    
    failed = [k for k, v in results.items() if v != 'OK']
    if failed:
        send_alert(f"🚨 Data Quality Gate FAILED: {failed}")
        return False   # ShortCircuitOperator sẽ SKIP tất cả downstream tasks
    return True

quality_gate = ShortCircuitOperator(
    task_id='data_quality_gate',
    python_callable=check_all_quality_gates,
)

# Task flow: etl_job >> quality_gate >> publish_to_redis >> notify_dashboard
# Nếu quality_gate FAIL → Redis KHÔNG được update → Dashboard giữ data cũ (đúng)
# → Tránh trường hợp Dashboard hiển thị doanh thu = 0 (sai)
```

### ✅ RESULT — Kết quả

Sau khi triển khai 5 tầng monitoring:

1. **Phát hiện sự cố trong < 5 phút** thay vì 2 ngày:
   - Tầng 1 (replication slot) phát hiện Debezium chết ngay lập tức
   - Tầng 3 (freshness) phát hiện dữ liệu trễ sau 4 giờ
   - Tầng 4 (volume) phát hiện 0 rows ngay trong batch đêm đầu tiên

2. **Ngăn dữ liệu sai lan xuống downstream**:
   - Data Quality Gate chặn publish dữ liệu lỗi ra Redis/Dashboard
   - Dashboard giữ data cũ (đúng) thay vì hiển thị 0 (sai)

3. **Phòng ngừa sự cố đĩa đầy**:
   - `max_slot_wal_keep_size = 10GB` tự động invalidate slot nếu Debezium lag quá lâu
   - Heartbeat mỗi 10 giây (`heartbeat.interval.ms = 10000`) giúp advance LSN cho bảng ít giao dịch

4. **Dashboard audit cho Ban Giám đốc**:
   - Pipeline Run Log ghi lại mỗi lần chạy: thời gian, số rows, status, error message
   - Query SLA: "Pipeline nào chạy quá 30 phút?" → phát hiện bottleneck sớm

### 🎤 Cách kể trong phỏng vấn (< 3 phút):

> *"Sau khi go-live hệ thống Fleet, Debezium connector bị crash vào cuối tuần mà không ai biết. Hậu quả: Dashboard báo cáo doanh thu = 0, và PostgreSQL tích tụ WAL files gần đầy đĩa 95%.*
>
> *Vấn đề cốt lõi: pipeline chạy 'thành công' nhưng output sai — không có cơ chế phát hiện dữ liệu bị thiếu.*
>
> *Tôi xây dựng hệ thống Data Quality Gate 5 tầng chạy qua Airflow: (1) monitor replication slot trên PostgreSQL bằng `pg_wal_lsn_diff`, (2) check Kafka consumer lag, (3) freshness check — bảng fact không có data mới quá 4 giờ thì alert, (4) volume anomaly — 0 rows hoặc giảm > 2 độ lệch chuẩn thì alert, (5) reconciliation — đối soát row count giữa source và target.*
>
> *Quan trọng nhất: dùng Airflow `ShortCircuitOperator` làm quality gate — nếu bất kỳ check nào fail, pipeline CHẶN publish data xuống Redis/Dashboard. Giữ data cũ đúng thay vì data mới sai.*
>
> *Kết quả: phát hiện sự cố trong 5 phút thay vì 2 ngày, và cấu hình `max_slot_wal_keep_size = 10GB` trên PostgreSQL để tự động bảo vệ OLTP DB khi CDC lag quá lâu."*

---

## 💡 MẸO PHỎNG VẤN: LIÊN KẾT 2 CÂU CHUYỆN

Khi interviewer hỏi tiếp "Còn ví dụ nào khác không?", hãy nối 2 câu chuyện lại:

> *"Câu chuyện 1 về SCD2 và câu chuyện 2 về Data Quality Gate thực ra liên kết chặt chẽ: SCD2 pipeline phụ thuộc vào CDC data đầy đủ. Nếu CDC chết (câu chuyện 2), SCD2 merge sẽ không phát hiện thay đổi status → dim_customer không được cập nhật → báo cáo lịch sử SAI ở mức subtler (không phải 0, mà là trạng thái cũ).*
>
> *Đó là lý do freshness check + reconciliation phải chạy TRƯỚC SCD2 merge — đây chính là khái niệm Data Quality Gate: chặn input xấu trước khi nó gây hỏng output."*

Cách nối này cho thấy bạn **tư duy hệ thống** — đúng chuẩn Senior DE mà JD tìm kiếm.

---

## 📋 CHECKLIST: Trước khi vào phỏng vấn

- [ ] Thuộc 5 bước SCD2 Merge (kể được trong 60 giây)
- [ ] Biết giải thích REPLICA IDENTITY FULL vs DEFAULT (1 câu)
- [ ] Viết được `pg_wal_lsn_diff` query không cần xem tài liệu
- [ ] Kể được lý do chọn CDC over Polling (3 lý do: load, delete, intermediate)
- [ ] Biết 5 tầng Data Quality Gate (replication → kafka → freshness → volume → reconciliation)
- [ ] Giải thích ShortCircuitOperator pattern (gate trước publish)
- [ ] Biết `max_slot_wal_keep_size` và `heartbeat.interval.ms` là gì

