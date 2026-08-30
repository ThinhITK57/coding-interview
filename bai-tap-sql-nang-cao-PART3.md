# BÀI TẬP SQL NÂNG CAO — PHẦN 3: PERFORMANCE OPTIMIZATION & KỊCH BẢN FLEET PROJECT
*Bổ sung 2 nhóm hoàn toàn mới: SQL Tuning cho Production + 10 kịch bản phỏng vấn tổng hợp liên hệ dự án Fleet Platform.*

---

## NHÓM 7 (MỚI): PERFORMANCE OPTIMIZATION — SQL TUNING CHO PRODUCTION

### Bài 7.1 — Đọc hiểu EXPLAIN ANALYZE trên PostgreSQL
*Bối cảnh JD*: "Tối ưu hiệu năng hệ thống" — Senior DE phải biết đọc execution plan để tìm bottleneck.

*Yêu cầu*: Phân tích query báo cáo doanh thu theo Head — tìm nguyên nhân chậm.
```sql
-- Query cần tối ưu: Tổng doanh thu theo trạm dịch vụ, lọc 3 tháng gần nhất
EXPLAIN ANALYZE
SELECT
    h.name AS head_name,
    SUM(i.service_amount) AS total_labor,
    SUM(i.parts_amount) AS total_parts,
    SUM(i.total_amount) AS total_revenue,
    COUNT(i.id) AS invoice_count
FROM invoices i
JOIN work_orders w ON i.work_order_id = w.id
JOIN heads h ON w.head_id = h.id
WHERE i.invoice_date >= CURRENT_DATE - INTERVAL '3 months'
  AND i.status = 'paid'
GROUP BY h.name
ORDER BY total_revenue DESC;
```

**Cách đọc EXPLAIN ANALYZE output:**

| Thành phần | Ý nghĩa | Dấu hiệu cần tối ưu |
|---|---|---|
| `Seq Scan on invoices` | Quét toàn bộ bảng, không dùng index | ⚠️ Nếu bảng > 100K rows → cần index |
| `cost=0.00..12345.67` | Ước tính chi phí (đơn vị trừu tượng) | So sánh cost trước/sau tối ưu |
| `actual time=0.012..145.678` | Thời gian thực tế (ms) | > 100ms trên 1 node = cần xem xét |
| `rows=50000` vs `rows=500` (estimated) | Ước tính sai lệch lớn = statistics outdated | Chạy `ANALYZE invoices;` |
| `Hash Join` vs `Nested Loop` | Hash Join tốt cho bảng lớn; Nested Loop tốt khi 1 bên rất nhỏ | Nested Loop trên 2 bảng lớn = thảm họa |
| `Bitmap Index Scan` | Kết hợp nhiều index conditions | Tốt hơn Seq Scan, nhưng chậm hơn Index Only Scan |

```sql
-- Tối ưu: Tạo composite index cho filter thường dùng
CREATE INDEX idx_invoices_date_status ON invoices (invoice_date, status)
WHERE status = 'paid';  -- Partial index: chỉ index các record đã thanh toán

-- Verify: chạy lại EXPLAIN ANALYZE → Seq Scan biến thành Index Scan
```

*🎤 Khi phỏng vấn hỏi*: "Query báo cáo chạy 45 giây, bạn xử lý thế nào?"
→ Trả lời theo 4 bước: EXPLAIN ANALYZE → Xác định bottleneck (Seq Scan / Hash Join trên bảng lớn) → Tạo index phù hợp → Verify lại với EXPLAIN.

---

### Bài 7.2 — Index Strategy cho Fact Tables
*Bối cảnh JD*: Fact tables có hàng triệu rows — index strategy quyết định tốc độ BI query.

*Yêu cầu*: Thiết kế index cho bảng `fact_repair_service_revenue` phục vụ các pattern query khác nhau.
```sql
-- Pattern 1: Filter theo date_key (BI filter theo ngày/tháng/năm)
-- → Single Column Index
CREATE INDEX idx_fact_revenue_date ON fact_repair_service_revenue (date_key);

-- Pattern 2: Filter theo date_key + head_id (báo cáo theo trạm theo tháng)
-- → Composite Index (thứ tự cột quan trọng: cột selectivity cao hơn đặt trước)
CREATE INDEX idx_fact_revenue_date_head
ON fact_repair_service_revenue (date_key, head_id);

-- Pattern 3: Chỉ cần đếm invoices đã paid — Partial Index
CREATE INDEX idx_fact_revenue_paid
ON fact_repair_service_revenue (date_key)
WHERE status = 'paid';

-- Pattern 4: Covering Index — bao gồm cả cột cần SELECT để tránh table lookup
CREATE INDEX idx_fact_revenue_covering
ON fact_repair_service_revenue (date_key, head_id)
INCLUDE (service_amount, parts_amount, total_amount);
-- → PostgreSQL dùng Index Only Scan, không cần quay lại heap table
```

**So sánh các loại Index:**

| Loại Index | Ưu điểm | Nhược điểm | Khi nào dùng |
|---|---|---|---|
| Single Column | Đơn giản, tối ưu cho 1 filter | Không tối ưu multi-filter | WHERE chỉ có 1 điều kiện |
| Composite | Tối ưu multi-filter queries | Chỉ hiệu quả theo thứ tự cột trái → phải | WHERE có 2-3 cột thường xuyên đi cùng |
| Partial | Nhỏ hơn, nhanh hơn (ít rows) | Chỉ dùng cho query matching WHERE clause | Bảng lớn nhưng chỉ query subset (vd: `status='paid'`) |
| Covering (INCLUDE) | Tránh table lookup hoàn toàn | Index lớn hơn do chứa thêm data | SELECT luôn lấy cùng bộ cột |

*🚚 Liên hệ Fleet*: Trên HDFS, "index" tương đương với **Parquet column pruning** (chỉ đọc cột cần thiết) + **partition pruning** (chỉ đọc thư mục year/month/day cần thiết). Cả hai đều giảm I/O đĩa giống index trên RDBMS.

---

### Bài 7.3 — Partition Pruning Verification
*Bối cảnh JD*: Table partitioning là kỹ thuật cốt lõi cho DWH quy mô lớn.

*Yêu cầu*: Tạo bảng invoices phân vùng theo năm, verify query chỉ quét partition cần thiết.
```sql
-- Tạo bảng partitioned
CREATE TABLE invoices_partitioned (
    id              SERIAL,
    work_order_id   INTEGER NOT NULL,
    customer_id     INTEGER NOT NULL,
    invoice_date    DATE NOT NULL,
    total_amount    DECIMAL(15, 2) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    PRIMARY KEY (id, invoice_date)  -- PK phải chứa partition key
) PARTITION BY RANGE (invoice_date);

-- Tạo partitions
CREATE TABLE invoices_p2025 PARTITION OF invoices_partitioned
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE invoices_p2026 PARTITION OF invoices_partitioned
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE invoices_p2027 PARTITION OF invoices_partitioned
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

-- Verify partition pruning
EXPLAIN (COSTS OFF)
SELECT * FROM invoices_partitioned
WHERE invoice_date = '2026-07-31';
-- Output kỳ vọng: chỉ scan invoices_p2026, KHÔNG scan p2025 hay p2027
-- →  Seq Scan on invoices_p2026
--      Filter: (invoice_date = '2026-07-31'::date)

-- ⚠️ CẢNH BÁO: Query KHÔNG có filter trên partition key → full scan TẤT CẢ partitions
EXPLAIN (COSTS OFF)
SELECT * FROM invoices_partitioned
WHERE total_amount > 1000000;
-- → Append
--     → Seq Scan on invoices_p2025
--     → Seq Scan on invoices_p2026
--     → Seq Scan on invoices_p2027
```

*🚚 Liên hệ Fleet*: Trên HDFS, partition pruning tương đương với cấu trúc thư mục `year=2026/month=07/day=31/`. Spark chỉ đọc thư mục matching filter condition, bỏ qua hàng nghìn thư mục khác. Config `spark.sql.sources.partitionOverwriteMode=dynamic` cho phép ghi đè chỉ partition bị ảnh hưởng.

---

### Bài 7.4 — Materialized View với Concurrent Refresh cho Dashboard
*Bối cảnh JD*: Dashboard doanh thu real-time phải nhanh, không được lock bảng khi refresh.

*Yêu cầu*: Tạo Materialized View tổng hợp doanh thu theo Head theo Tháng, refresh không khóa đọc.
```sql
-- 1. Tạo Materialized View
CREATE MATERIALIZED VIEW mv_head_monthly_revenue AS
SELECT
    h.id AS head_id,
    h.name AS head_name,
    DATE_TRUNC('month', i.invoice_date) AS invoice_month,
    SUM(i.service_amount) AS total_labor_revenue,
    SUM(i.parts_amount) AS total_parts_revenue,
    SUM(i.total_amount) AS total_revenue,
    COUNT(i.id) AS invoice_count
FROM heads h
JOIN work_orders w ON h.id = w.head_id
JOIN invoices i ON w.id = i.work_order_id
WHERE i.status = 'paid'
GROUP BY h.id, h.name, DATE_TRUNC('month', i.invoice_date);

-- 2. Tạo UNIQUE INDEX (BẮT BUỘC cho REFRESH CONCURRENTLY)
CREATE UNIQUE INDEX idx_mv_head_monthly_pk
ON mv_head_monthly_revenue (head_id, invoice_month);

-- 3. Refresh KHÔNG khóa đọc (non-blocking)
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_head_monthly_revenue;

-- 4. Dashboard query chỉ cần quét view nhỏ gọn (~vài nghìn rows)
SELECT head_name, invoice_month, total_revenue, invoice_count
FROM mv_head_monthly_revenue
WHERE invoice_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months')
ORDER BY invoice_month DESC, total_revenue DESC;
```

**Blocking vs Non-Blocking Refresh:**

| Kiểu Refresh | Lệnh | Lock | Khi nào dùng |
|---|---|---|---|
| Blocking | `REFRESH MATERIALIZED VIEW mv_x` | **ACCESS EXCLUSIVE** — chặn tất cả SELECT | Lần đầu tạo hoặc off-peak hours |
| Non-Blocking | `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_x` | Không chặn SELECT | Production 24/7, cần UNIQUE INDEX |

*🎤 Khi phỏng vấn hỏi*: "Tại sao CONCURRENTLY cần UNIQUE INDEX?"
→ Vì PostgreSQL phải xác định row nào thay đổi (diff) giữa data cũ và data mới để update in-place thay vì replace toàn bộ. UNIQUE INDEX cho phép PostgreSQL so khớp từng row một cách chính xác.

---

### Bài 7.5 — CTE vs Subquery vs Temporary Table — Performance Trade-offs
*Yêu cầu*: Cùng 1 logic báo cáo, viết 3 cách và so sánh hiệu năng.

```sql
-- Cách 1: CTE (Common Table Expression)
WITH monthly_revenue AS (
    SELECT head_id, DATE_TRUNC('month', invoice_date) AS month,
           SUM(total_amount) AS revenue
    FROM invoices WHERE status = 'paid'
    GROUP BY head_id, DATE_TRUNC('month', invoice_date)
)
SELECT h.name, mr.month, mr.revenue,
       LAG(mr.revenue) OVER (PARTITION BY mr.head_id ORDER BY mr.month) AS prev_month
FROM monthly_revenue mr
JOIN heads h ON mr.head_id = h.id;

-- Cách 2: Subquery
SELECT h.name, sub.month, sub.revenue,
       LAG(sub.revenue) OVER (PARTITION BY sub.head_id ORDER BY sub.month) AS prev_month
FROM (
    SELECT head_id, DATE_TRUNC('month', invoice_date) AS month,
           SUM(total_amount) AS revenue
    FROM invoices WHERE status = 'paid'
    GROUP BY head_id, DATE_TRUNC('month', invoice_date)
) sub
JOIN heads h ON sub.head_id = h.id;

-- Cách 3: Temporary Table (dùng khi CTE chạy chậm do materialize không cần thiết)
CREATE TEMP TABLE tmp_monthly_revenue AS
SELECT head_id, DATE_TRUNC('month', invoice_date) AS month,
       SUM(total_amount) AS revenue
FROM invoices WHERE status = 'paid'
GROUP BY head_id, DATE_TRUNC('month', invoice_date);

CREATE INDEX idx_tmp_head ON tmp_monthly_revenue (head_id);

SELECT h.name, t.month, t.revenue,
       LAG(t.revenue) OVER (PARTITION BY t.head_id ORDER BY t.month) AS prev_month
FROM tmp_monthly_revenue t
JOIN heads h ON t.head_id = h.id;
```

**So sánh:**

| Tiêu chí | CTE | Subquery | Temporary Table |
|---|---|---|---|
| Readability | ⭐⭐⭐ Tốt nhất | ⭐⭐ Trung bình | ⭐ Cần nhiều statement |
| Performance (PG 12+) | Tương đương Subquery (inline by default) | Tương đương CTE | Tốt nhất cho complex pipeline |
| Index support | Không | Không | ✅ Có thể CREATE INDEX |
| Reuse trong query | ✅ Dùng nhiều lần | ❌ Phải copy paste | ✅ Dùng nhiều lần |
| Khi nào dùng | Default cho mọi trường hợp | Khi chỉ dùng 1 lần | Khi CTE quá phức tạp hoặc cần index |

*Giải thích*: Từ PostgreSQL 12, CTE không còn là "optimization fence" mặc định — optimizer có thể inline CTE vào query chính. Dùng `MATERIALIZED` keyword nếu muốn ép CTE phải tính trước: `WITH monthly_revenue AS MATERIALIZED (...)`.

---

### Bài 7.6 — Pivoting Data với FILTER (WHERE) vs CASE WHEN vs crosstab()
*Bối cảnh JD*: Báo cáo inventory matrix — hàng = trạm, cột = loại linh kiện, giá trị = tồn kho.

```sql
-- Cách 1: FILTER (WHERE) — PostgreSQL-native, đọc hiểu tốt nhất
SELECT
    h.name AS head_name,
    COALESCE(SUM(p.quantity) FILTER (WHERE c.category = 'Phanh'), 0) AS qty_brake,
    COALESCE(SUM(p.quantity) FILTER (WHERE c.category = 'Lốp'), 0) AS qty_tire,
    COALESCE(SUM(p.quantity) FILTER (WHERE c.category = 'Dầu'), 0) AS qty_oil,
    COALESCE(SUM(p.quantity) FILTER (WHERE c.category = 'Điện'), 0) AS qty_electrical
FROM heads h
LEFT JOIN parts_inventory p ON h.id = p.head_id
LEFT JOIN components c ON p.component_id = c.id
GROUP BY h.id, h.name
ORDER BY h.name;

-- Cách 2: CASE WHEN — ANSI SQL chuẩn, chạy trên mọi database
SELECT
    h.name AS head_name,
    COALESCE(SUM(CASE WHEN c.category = 'Phanh' THEN p.quantity END), 0) AS qty_brake,
    COALESCE(SUM(CASE WHEN c.category = 'Lốp' THEN p.quantity END), 0) AS qty_tire,
    COALESCE(SUM(CASE WHEN c.category = 'Dầu' THEN p.quantity END), 0) AS qty_oil,
    COALESCE(SUM(CASE WHEN c.category = 'Điện' THEN p.quantity END), 0) AS qty_electrical
FROM heads h
LEFT JOIN parts_inventory p ON h.id = p.head_id
LEFT JOIN components c ON p.component_id = c.id
GROUP BY h.id, h.name;

-- Cách 3: crosstab() — cần extension tablefunc, linh hoạt nhất cho dynamic columns
-- CREATE EXTENSION IF NOT EXISTS tablefunc;
SELECT * FROM crosstab(
    'SELECT h.name, c.category, SUM(p.quantity)
     FROM heads h
     LEFT JOIN parts_inventory p ON h.id = p.head_id
     LEFT JOIN components c ON p.component_id = c.id
     GROUP BY h.name, c.category
     ORDER BY h.name, c.category',
    'SELECT DISTINCT category FROM components ORDER BY category'
) AS ct(head_name TEXT, "Dầu" BIGINT, "Lốp" BIGINT, "Phanh" BIGINT, "Điện" BIGINT);
```

**So sánh:**

| Tiêu chí | `FILTER (WHERE)` | `CASE WHEN` | `crosstab()` |
|---|---|---|---|
| Cú pháp | PostgreSQL-native | ANSI SQL (mọi DB) | Cần extension `tablefunc` |
| Đọc hiểu | ⭐⭐⭐ Rõ ràng nhất | ⭐⭐ Verbose | ⭐ Phức tạp |
| Dynamic columns | ❌ Hard-code | ❌ Hard-code | ✅ Linh hoạt hơn |
| Performance | Nhanh nhất | Tương đương | Chậm hơn do function call |

*🚚 Liên hệ Fleet*: File `08_POSTGRES_ADVANCED_SQL_2.sql` trong dự án dùng `FILTER (WHERE)` cho ma trận tồn kho linh kiện theo trạm dịch vụ.

---

### 🚚 Kịch bản Phỏng vấn Fleet — Nhóm 7

**Kịch bản 7.1**: "Query báo cáo tháng chạy chậm 45 giây, bạn tối ưu thế nào?"

**💡 Cấu trúc trả lời (4 bước):**
1. **Diagnosis**: Chạy `EXPLAIN ANALYZE` → phát hiện `Seq Scan on invoices` (1.2 triệu rows)
2. **Quick Fix**: Tạo composite partial index `CREATE INDEX idx_inv ON invoices (invoice_date, status) WHERE status = 'paid'`
3. **Medium Fix**: Nếu vẫn chậm → Partition bảng invoices theo năm → partition pruning giảm scan 3x
4. **Long-term**: Tạo Materialized View `mv_head_monthly_revenue` với CONCURRENT refresh hàng đêm → dashboard query < 50ms

**Kịch bản 7.2**: "Hàng triệu file nhỏ trên HDFS gây chậm batch job, xử lý sao?"

**💡 Trả lời:**
- **Nguyên nhân**: Spark Streaming trigger mỗi 10 giây tạo 1 file Parquet nhỏ vài KB. Sau 1 ngày = 8,640 files. Mỗi file ngốn ~150 bytes RAM NameNode.
- **Giải pháp**: Airflow trigger Compaction job hàng đêm:
  ```python
  # Đọc các file nhỏ trong ngày
  df = spark.read.parquet("hdfs://master:9000/fleet-datalake/year=2026/month=07/day=31/")
  # Gộp thành ít file lớn (coalesce không gây shuffle)
  df.coalesce(4).write.mode("overwrite").parquet("hdfs://...same path...")
  ```
- **Phân biệt**: `coalesce(n)` = gộp KHÔNG shuffle (nhanh) vs `repartition(n)` = chia đều CÓ shuffle (chậm hơn nhưng file kích thước đều)

---

## NHÓM 8 (MỚI): 🚚 FLEET PROJECT — KỊCH BẢN PHỎNG VẤN TỔNG HỢP

*Đây là phần quan trọng nhất — 10 kịch bản end-to-end kết nối kiến thức SQL với kiến trúc thực tế dự án Fleet.*

---

### 8.1 — "Hãy mô tả end-to-end data pipeline của dự án bạn đã làm"

**🎤 Câu hỏi**: Interviewer muốn hiểu bạn có nắm được bức tranh tổng thể hay chỉ biết phần mình code.

**💡 Cấu trúc trả lời:**

```
PostgreSQL (Odoo ERP)
    ↓ Debezium Log-Based CDC (wal_level=logical)
Kafka Multi-Broker (3 nodes, RF=3)
    ↓ PySpark Structured Streaming
HDFS Data Lake (Parquet, partitioned year/month/day)
    ↓ PySpark Batch + Airflow Orchestration
HDFS Data Warehouse (Star Schema: 2 Fact + 4 Dimension tables)
    ↓ Redis Pub/Sub
Async Python WebSocket → Executive Dashboard
```

**📝 SQL minh họa — từ OLTP schema đến DWH query:**
```sql
-- 1. OLTP (nguồn): 3NF normalized
SELECT i.id, i.invoice_date, i.total_amount, c.company_name, h.name
FROM invoices i
JOIN work_orders w ON i.work_order_id = w.id
JOIN customers c ON w.customer_id = c.id
JOIN heads h ON w.head_id = h.id;
-- → 4 JOINs trên OLTP = chậm cho BI reporting

-- 2. DWH (đích): Star Schema denormalized
SELECT f.total_amount, dd.fiscal_quarter, dh.head_name, dc.company_name
FROM fact_repair_service_revenue f
JOIN dim_date dd ON f.date_key = dd.date_key
JOIN dim_head dh ON f.head_key = dh.head_key
JOIN dim_customer dc ON f.customer_key = dc.customer_key;
-- → Cùng dữ liệu, nhưng JOIN trên surrogate keys = nhanh hơn 10-50x
```

**⚡ Điểm ghi bonus**: "Kiến trúc này triệt tiêu 100% query load lên OLTP DB — Debezium đọc WAL log trực tiếp, không cần SELECT polling."

---

### 8.2 — "Tại sao chọn Debezium CDC thay vì Query Polling?"

**🎤 Câu hỏi**: Interviewer kiểm tra bạn hiểu trade-off lựa chọn công nghệ.

**📝 SQL minh họa — Query Polling (cách cũ):**
```sql
-- Polling pattern: chạy mỗi 5 phút
SELECT * FROM customers
WHERE updated_at > '2026-08-09 23:00:00'  -- last_run_timestamp
ORDER BY updated_at;

-- ❌ VẤN ĐỀ 1: Mất DELETE events
-- Khi khách hàng bị xóa, query SELECT không thấy row đã xóa → downstream không biết

-- ❌ VẤN ĐỀ 2: Mất trạng thái trung gian
-- Khách hàng đổi: 'mới' → 'cũ' → 'thường niên' trong 5 phút
-- Polling chỉ thấy trạng thái cuối 'thường niên', mất transition 'mới→cũ'

-- ❌ VẤN ĐỀ 3: Tải I/O lên OLTP DB
-- Mỗi 5 phút quét index → CPU + I/O trên Odoo DB → ảnh hưởng end-user
```

**📝 Debezium CDC (cách Fleet dùng):**
```sql
-- Debezium đọc WAL log → 0% query load lên DB
-- CDC Event cho UPDATE: op='u'
-- before: {"id": 1, "status": "mới"}      ← Có nhờ REPLICA IDENTITY FULL
-- after:  {"id": 1, "status": "thường niên"}

-- CDC Event cho DELETE: op='d'
-- before: {"id": 1, "status": "thường niên"}  ← Bắt được DELETE
-- after: null

-- Query verify CDC hoạt động (chạy trên PostgreSQL):
SELECT slot_name, active, confirmed_flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots
WHERE slot_name = 'debezium';
```

| Tiêu chí | Query Polling | Debezium CDC |
|---|---|---|
| Tải lên OLTP DB | ⚠️ Cao (quét index liên tục) | ✅ 0% (đọc WAL file) |
| Bắt DELETE | ❌ Mất hoàn toàn | ✅ 100% (op='d') |
| Trạng thái trung gian | ❌ Chỉ thấy snapshot cuối | ✅ Mọi transaction commit |
| Latency | 5 phút (poll interval) | < 1 giây (streaming) |
| Đánh đổi | Đơn giản | Cần quản lý WAL disk + Replication Slot |

---

### 8.3 — "Kafka bị lag 5 triệu messages, bạn xử lý thế nào?"

**🎤 Câu hỏi**: Production incident — kiểm tra khả năng xử lý sự cố.

**💡 Quy trình 4 bước:**

```bash
# Bước 1: Kiểm tra Consumer Lag
kafka-consumer-groups.sh --bootstrap-server master:9092 \
  --group fleet-spark-streaming --describe

# Output:
# TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# fleet-cdc       0          1000000         6000000         5000000  ← 5M lag!

# Bước 2: Xác định nguyên nhân — Spark job crashed? Network issue?
# → Check Spark UI: http://master:4040

# Bước 3: Fix root cause và restart consumer
# → Consumer tự động resume từ last committed offset

# Bước 4: Nếu cần replay từ thời điểm cụ thể:
kafka-consumer-groups.sh --bootstrap-server master:9092 \
  --group fleet-spark-streaming \
  --reset-offsets --to-datetime 2026-08-09T20:00:00.000 \
  --execute --topic fleet-cdc
```

**📝 SQL minh họa — Tại sao replay an toàn (Idempotent Sink):**
```sql
-- Trên HDFS: Dynamic Partition Overwrite
-- spark.sql.sources.partitionOverwriteMode = dynamic
-- Ghi lại partition year=2026/month=08/day=09 → ghi đè nguyên tử
-- Dù replay 1 lần hay 100 lần → kết quả giống nhau

-- Trên Redis: Key-based overwrite
-- HSET head:info:1 name "Bình Tân" lat 10.75 lng 106.62
-- Ghi lại cùng key → overwrite → idempotent
```

**⚡ Điểm ghi bonus**: "Kafka replay an toàn nhờ 2 yếu tố: Replayable Source (Kafka lưu offset) + Idempotent Sink (overwrite pattern ở cả HDFS và Redis)."

---

### 8.4 — "SCD Type 2 trong project được implement thế nào?"

**🎤 Câu hỏi**: Interviewer muốn xem bạn hiểu SCD2 ở mức production, không chỉ lý thuyết.

**📝 SQL minh họa — 5 bước Merge SCD2:**
```sql
-- BƯỚC 1: Bảo toàn 100% bản ghi lịch sử đã đóng
SELECT * FROM dim_customer WHERE is_current = FALSE;

-- BƯỚC 2: LEFT JOIN active records với CDC data mới
SELECT old.*, new.status AS new_status
FROM dim_customer old
LEFT JOIN cdc_customers new ON old.customer_id = new.customer_id
WHERE old.is_current = TRUE;

-- BƯỚC 3: Đóng bản ghi active bị thay đổi
UPDATE dim_customer
SET expiration_date = '2026-08-09', is_current = FALSE
WHERE customer_id = 1 AND is_current = TRUE
  AND status <> 'thường niên';  -- status mới từ CDC

-- BƯỚC 4: Giữ nguyên bản ghi active không đổi
-- (Không cần SQL — skip records where status unchanged)

-- BƯỚC 5: Tạo bản ghi mới (active version mới + khách hàng mới)
INSERT INTO dim_customer (customer_key, customer_id, company_name, status,
                          effective_date, expiration_date, is_current)
VALUES (
    MD5('1_2026-08-09_thường niên'),  -- Surrogate Key = MD5(id + date + status)
    1, 'Công ty Vận Tải A', 'thường niên',
    '2026-08-09', '9999-12-31', TRUE
);

-- Query lịch sử: "Khách hàng ở trạng thái gì tại thời điểm hóa đơn X?"
SELECT i.invoice_number, i.invoice_date, dc.status AS customer_status_at_invoice_time
FROM invoices i
JOIN dim_customer dc
  ON i.customer_id = dc.customer_id
 AND i.invoice_date BETWEEN dc.effective_date
     AND COALESCE(dc.expiration_date, '9999-12-31');
```

**⚡ Điểm ghi bonus**: "REPLICA IDENTITY FULL là non-negotiable cho SCD2 — nếu để DEFAULT, CDC chỉ gửi Primary Key trong `before` image, Spark không biết trạng thái cũ để so sánh."

---

### 8.5 — "Năm Tài Khóa được xử lý thế nào trong pipeline?"

**📝 SQL minh họa:**
```sql
-- dim_date chứa fiscal_year pre-computed
-- Fiscal Year Fleet: 01/10 → 30/09 (FY2027 = Oct 2026 → Sep 2027)

-- Logic tính fiscal_year:
SELECT
    full_date,
    EXTRACT(YEAR FROM full_date) AS calendar_year,
    CASE
        WHEN EXTRACT(MONTH FROM full_date) >= 10
        THEN EXTRACT(YEAR FROM full_date) + 1
        ELSE EXTRACT(YEAR FROM full_date)
    END AS fiscal_year,
    CASE
        WHEN EXTRACT(MONTH FROM full_date) IN (10,11,12) THEN 'Q1'
        WHEN EXTRACT(MONTH FROM full_date) IN (1,2,3) THEN 'Q2'
        WHEN EXTRACT(MONTH FROM full_date) IN (4,5,6) THEN 'Q3'
        WHEN EXTRACT(MONTH FROM full_date) IN (7,8,9) THEN 'Q4'
    END AS fiscal_quarter
FROM generate_series('2025-01-01'::date, '2027-12-31'::date, '1 day') AS full_date;

-- Báo cáo doanh thu theo năm tài khóa:
SELECT dd.fiscal_year, dd.fiscal_quarter, dh.head_name,
       SUM(f.total_amount) AS total_revenue
FROM fact_repair_service_revenue f
JOIN dim_date dd ON f.date_key = dd.date_key
JOIN dim_head dh ON f.head_key = dh.head_key
WHERE dd.fiscal_year = 2027
GROUP BY dd.fiscal_year, dd.fiscal_quarter, dh.head_name
ORDER BY dd.fiscal_quarter, total_revenue DESC;
```

**⚡ Điểm ghi bonus**: "Airflow DAG `dag_fiscal_year_report.py` schedule chạy vào ngày 01/10 hàng năm. dim_date pre-compute sẵn fiscal columns → JOIN đơn giản, không cần CASE WHEN runtime."

---

### 8.6 — "Hệ thống đảm bảo Exactly-Once Semantics bằng cách nào?"

**💡 Trả lời:**

| Tầng | Cơ chế Exactly-Once |
|---|---|
| Kafka → Spark | Checkpoint lưu offset: `checkpoint/offsets/v1/batchId` |
| Spark → HDFS | FileStreamSinkLog: `_spark_metadata/batchId` commit log |
| Spark → Redis | `foreachBatch()` + idempotent `HSET` overwrite |
| HDFS Batch | `partitionOverwriteMode=dynamic` — ghi đè nguyên tử theo partition |

```sql
-- SQL tương đương pattern idempotent write:
-- ❌ Non-idempotent (INSERT trùng nếu retry):
INSERT INTO fact_daily_sales VALUES ('2026-08-09', 101, 500000);

-- ✅ Idempotent (overwrite nếu retry):
-- Cách 1: DELETE + INSERT trong transaction
BEGIN;
DELETE FROM fact_daily_sales WHERE sales_date = '2026-08-09';
INSERT INTO fact_daily_sales
SELECT sales_date, product_id, SUM(amount) FROM staging WHERE sales_date = '2026-08-09'
GROUP BY sales_date, product_id;
COMMIT;

-- Cách 2: MERGE/UPSERT
INSERT INTO fact_daily_sales (sales_date, product_id, total_amount)
VALUES ('2026-08-09', 101, 500000)
ON CONFLICT (sales_date, product_id)
DO UPDATE SET total_amount = EXCLUDED.total_amount;
```

---

### 8.7 — "Redis serving layer được thiết kế ra sao? Tại sao không dùng PostgreSQL trực tiếp?"

**📝 So sánh bằng SQL/Redis:**
```sql
-- PostgreSQL PostGIS: Tìm trạm gần nhất trong bán kính 10km
SELECT h.name, h.capacity,
       ST_Distance(h.geom, ST_SetSRID(ST_MakePoint(106.65, 10.78), 4326)) AS distance_m
FROM heads h
WHERE ST_DWithin(h.geom, ST_SetSRID(ST_MakePoint(106.65, 10.78), 4326), 10000)
  AND h.slots_available > 0
ORDER BY distance_m
LIMIT 5;
-- Latency: 20-100ms (disk I/O)
```

```
# Redis GEOSEARCH: Cùng logic, sub-millisecond
GEOSEARCH geo:heads FROMLONLAT 106.65 10.78 BYRADIUS 10 km ASC COUNT 5
# Latency: < 1ms (in-memory)
```

```
# Redis Lua Script: Atomic slot reservation (zero overbooking)
-- KEYS[1] = "head:slots:1"
-- ARGV[1] = truck_plate
local avail = tonumber(redis.call('HGET', KEYS[1], 'slots_available'))
if avail > 0 then
    redis.call('HINCRBY', KEYS[1], 'slots_available', -1)
    redis.call('SADD', KEYS[1]..':reserved', ARGV[1])
    return 1  -- SUCCESS
else
    return 0  -- NO SLOTS
end
-- Single-threaded execution = 100% atomic, no race condition
```

| Tiêu chí | PostgreSQL (PostGIS) | Redis 7+ (GEOSEARCH) |
|---|---|---|
| Latency | 20-100ms | < 1ms |
| Concurrency Control | `SELECT FOR UPDATE` → row lock → potential deadlock | Lua Script single-threaded → 100% atomic |
| Scalability | Limited by disk I/O | Limited by RAM |
| Data Persistence | ✅ Durable (WAL) | ⚠️ RDB/AOF (periodic) |

---

### 8.8 — "Nếu Master node chết, hệ thống có hoạt động được không?"

**💡 Phân tích từng component:**

| Component | Master chết → Hậu quả | Giải pháp HA |
|---|---|---|
| HDFS NameNode | ❌ Mất metadata → HDFS không đọc/ghi được | Active/Standby NameNode + JournalNodes (3 nodes) |
| Kafka | Leader partition migrate tự động | `min.insync.replicas=2` + `RF=3` → 1 broker chết vẫn OK |
| PostgreSQL | ❌ OLTP DB unavailable | Streaming Replication + Patroni auto-failover |
| Airflow | ❌ Scheduler/Webserver down | Metadata DB trên PostgreSQL replica |
| Redis | ❌ Serving layer down | Redis Sentinel hoặc Redis Cluster |

```sql
-- Kiểm tra HDFS replication status:
-- hdfs dfsadmin -report
-- → Under-replicated blocks: 0 (healthy)
-- → Under-replicated blocks: 150 (cần repair)

-- Kiểm tra Kafka leader election sau broker crash:
-- kafka-topics.sh --describe --topic fleet-cdc --bootstrap-server slave1:9092
-- → Partition 0: Leader: 2 (was 1) ← tự động election
```

**⚡ Điểm ghi bonus**: "Trên bare-metal cluster 3 nodes, HDFS RF=3 và Kafka RF=3 đều tolerate 1 node failure. Nhưng nếu 2 nodes chết cùng lúc → Kafka quăng NotEnoughReplicasException (min.insync.replicas=2 không đủ)."

---

### 8.9 — "Dashboard executive dùng WebSocket push thay vì REST polling — tại sao?"

**📝 So sánh bằng số:**
```
# REST Polling:
30 executives × 1 request / 5 seconds = 360 requests/phút
Mỗi request query DB → CPU + I/O liên tục dù data KHÔNG thay đổi

# WebSocket Push:
30 persistent connections × 0 requests = 0 requests/phút
Data chỉ push KHI CÓ event mới (Airflow job chạy xong → Redis Pub/Sub → WebSocket broadcast)
Latency: < 15ms từ lúc job xong đến dashboard hiển thị
```

```python
# Pattern: Spark foreachBatch → Redis Pub/Sub → WebSocket
def write_to_redis_and_notify(batch_df, batch_id):
    # 1. Ghi aggregation results vào Redis
    for row in batch_df.collect():
        redis_client.hset(f"report:agg:monthly", mapping=row.asDict())

    # 2. Publish event để WebSocket broadcast
    redis_client.publish("channel:report-updates", json.dumps({"event": "REPORT_UPDATED"}))

# WebSocket server lắng nghe Redis Pub/Sub → broadcast tới tất cả clients
```

---

### 8.10 — "Bạn monitor production system thế nào? Phát hiện lỗi bằng cách nào?"

**💡 Layer-by-layer diagnosis (từ nguồn → đích):**

```sql
-- Tầng 1: PostgreSQL CDC Health
SELECT slot_name, active,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS wal_lag_bytes,
       CASE
           WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) > 5368709120  -- 5GB
           THEN '🚨 CRITICAL: WAL BLOAT RISK'
           WHEN NOT active THEN '🚨 CRITICAL: SLOT INACTIVE'
           ELSE '✅ HEALTHY'
       END AS health_status
FROM pg_replication_slots;
```

```bash
# Tầng 2: Kafka Consumer Lag
kafka-consumer-groups.sh --bootstrap-server master:9092 \
  --group fleet-spark-streaming --describe
# LAG > 100000 → ⚠️ WARNING
# LAG > 1000000 → 🚨 CRITICAL
```

```
# Tầng 3: Spark UI (http://master:4040)
# Jobs → Stages → Tasks timeline
# Dấu hiệu bất thường:
# - 1 task chạy 10x lâu hơn trung bình → Data Skew
# - Tất cả tasks pending → Executor thiếu memory
```

```bash
# Tầng 4: Redis Health
redis-cli INFO memory
# used_memory_human: 1.2G (OK nếu < 75% maxmemory)
redis-cli INFO clients
# connected_clients: 32 (WebSocket connections)
```

```sql
-- Tầng 5: Data Quality Checks (chạy tự động sau mỗi ETL)
-- Freshness: Bảng fact có data mới trong 2h không?
SELECT MAX(loaded_at), CURRENT_TIMESTAMP - MAX(loaded_at) AS staleness
FROM fact_repair_service_revenue;

-- Volume: Số rows hôm nay có bất thường không?
-- (Dùng pattern Bài 3.2 từ tài liệu gốc)
```

**⚡ Điểm ghi bonus**: "5 tầng monitoring này chạy tự động qua Airflow Sensor DAGs + Prometheus/Grafana dashboards. Khi bất kỳ tầng nào alert → PagerDuty thông báo on-call engineer."

---

## 📋 TỔNG KẾT: BẢN ĐỒ KIẾN THỨC SQL ↔ FLEET PROJECT ↔ JD

| Yêu cầu JD | SQL Pattern | Liên hệ Fleet |
|---|---|---|
| Batch & Streaming Pipeline | MERGE, SCD2, Window Agg, Dedup | Spark Streaming + Batch DWH |
| Orchestration | Pipeline audit table, backfill, idempotent | Airflow DAGs + partition-level tracking |
| Data Quality | Freshness, volume, null-rate, reconciliation | CDC verification + DQ gates |
| Data Modeling | Star Schema, ROLLUP/CUBE, Fiscal Year | fact_repair + dim_date + fiscal calendar |
| Data Governance | RLS, masking, audit trail, CDC | REPLICA IDENTITY FULL + Debezium |
| AI/Chatbot | RAG metadata, PII filtering | Redis serving + data sanitization |
| Performance | EXPLAIN, index, partition, MatView | HDFS partition pruning + Parquet column pruning |
| System Design | End-to-end architecture | PostgreSQL→Debezium→Kafka→Spark→HDFS→Redis→WS |
