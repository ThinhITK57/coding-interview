# BÀI TẬP SQL NÂNG CAO - PHẦN 2 (MÔ HÌNH DỮ LIỆU, BI, DATA GOVERNANCE & AI)

## NHÓM 4 BỔ SUNG: MÔ HÌNH DỮ LIỆU & BI — NÂNG CAO

### Bài 4.3 — ROLLUP & CUBE cho báo cáo đa chiều (Multi-Dimensional Reporting)

**Bối cảnh JD:** Ban giám đốc cần báo cáo tổng doanh thu theo nhiều chiều: theo Head (trạm), theo Quý tài khóa, theo loại dịch vụ — kèm subtotals (tổng phụ) và grand total (tổng cộng).

**Yêu cầu:** 
Viết truy vấn SQL sử dụng `ROLLUP` để tính subtotals theo cấp bậc (fiscal_year → fiscal_quarter → head_name) và `CUBE` cho tất cả các tổ hợp có thể.

**SQL Code (PostgreSQL):**

```sql
-- Dùng ROLLUP cho Hierarchical Subtotals
SELECT 
    d.fiscal_year,
    d.fiscal_quarter,
    h.name AS head_name,
    SUM(f.total_amount) AS total_revenue
FROM fact_repair_service_revenue f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_head h ON f.head_id = h.id
GROUP BY ROLLUP(d.fiscal_year, d.fiscal_quarter, h.name)
ORDER BY d.fiscal_year, d.fiscal_quarter, h.name;

-- Dùng CUBE cho All Combinations
SELECT 
    d.fiscal_quarter,
    h.name AS head_name,
    c.category AS component_category,
    SUM(f.parts_amount) AS total_parts_revenue
FROM fact_parts_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_head h ON f.head_id = h.id
JOIN dim_component c ON f.component_id = c.id
GROUP BY CUBE(d.fiscal_quarter, h.name, c.category);
```

**Giải thích:**
- `ROLLUP`: Tạo subtotals theo thứ tự phân cấp. Nếu `ROLLUP(A, B, C)`, nó sẽ nhóm theo (A, B, C), (A, B), (A), và (). Rất hợp với dữ liệu phân cấp thời gian hoặc địa lý.
- `CUBE`: Tạo subtotals cho *tất cả* các tổ hợp có thể của các cột. Nếu `CUBE(A, B)`, nó sẽ nhóm theo (A, B), (A), (B), ().

**🚚 Liên hệ dự án Fleet:**
Mẫu query này được sử dụng trong Spark job `batch_dwh_aggregation.py` để tạo các pre-aggregated views trên HDFS Parquet Data Lake, phục vụ BI dashboard. Spark SQL cũng hỗ trợ hoàn toàn cú pháp `ROLLUP` và `CUBE`.

> 🎤 **Tips:** Interivewer thường hỏi khi nào dùng ROLLUP vs CUBE. ROLLUP dùng cho data có tính thứ bậc (Năm > Tháng > Ngày), CUBE dùng khi các chiều độc lập và user muốn filter theo bất kỳ chiều nào.

---

### Bài 4.4 — Thiết kế Dim_Date với Fiscal Year Calendar

**Bối cảnh JD:** Năm tài khóa (Fiscal Year) của hệ thống Fleet bắt đầu từ 01/10 và kết thúc vào 30/09 năm sau. Bảng `dim_date` trong Data Warehouse cần pre-compute các thuộc tính thời gian để tránh tính toán lại mỗi khi truy vấn.

**Yêu cầu:** 
Viết DDL cho `dim_date` và truy vấn INSERT tạo ra 3 năm dữ liệu sử dụng hàm `generate_series`. Thể hiện logic tính Fiscal Year.

**SQL Code:**

```sql
-- DDL tạo bảng Dim_Date
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- Định dạng YYYYMMDD
    full_date DATE NOT NULL,
    calendar_year INT NOT NULL,
    calendar_quarter INT NOT NULL,
    calendar_month INT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- Sinh dữ liệu 3 năm bằng generate_series
INSERT INTO dim_date
SELECT 
    TO_CHAR(d.dt, 'YYYYMMDD')::INT AS date_key,
    d.dt::DATE AS full_date,
    EXTRACT(YEAR FROM d.dt) AS calendar_year,
    EXTRACT(QUARTER FROM d.dt) AS calendar_quarter,
    EXTRACT(MONTH FROM d.dt) AS calendar_month,
    -- Logic tính Fiscal Year: Nếu tháng >= 10, năm tài khóa là năm sau
    CASE WHEN EXTRACT(MONTH FROM d.dt) >= 10 
         THEN EXTRACT(YEAR FROM d.dt) + 1 
         ELSE EXTRACT(YEAR FROM d.dt) 
    END AS fiscal_year,
    -- Logic tính Fiscal Quarter
    CASE WHEN EXTRACT(MONTH FROM d.dt) IN (10, 11, 12) THEN 1
         WHEN EXTRACT(MONTH FROM d.dt) IN (1, 2, 3) THEN 2
         WHEN EXTRACT(MONTH FROM d.dt) IN (4, 5, 6) THEN 3
         ELSE 4 
    END AS fiscal_quarter,
    EXTRACT(ISODOW FROM d.dt) AS day_of_week,
    CASE WHEN EXTRACT(ISODOW FROM d.dt) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT generate_series(
        '2023-01-01'::DATE, 
        '2025-12-31'::DATE, 
        '1 day'::interval
    ) AS dt
) d;
```

**Giải thích:**
- `date_key`: Thường dùng kiểu INT (YYYYMMDD) trong Star Schema vì tốn ít dung lượng và lookup nhanh hơn kiểu DATE.
- Logic Fiscal Year: Cực kỳ quan trọng để xử lý lệch năm. Thay vì dùng `CASE WHEN` lúc SELECT báo cáo, pre-compute vào `dim_date` giúp query BI nhanh hơn gấp nhiều lần.

---

### Bài 4.5 — Semi-Additive Measures: Inventory Snapshot

**Bối cảnh JD:** Tồn kho linh kiện (`parts_inventory.quantity`) là một *semi-additive measure* — không thể cộng dồn theo chiều thời gian (tồn kho ngày 1 + ngày 2 không ra tổng tồn kho 2 ngày), nhưng có thể cộng theo chiều trạm (tồn kho trạm A + trạm B trong 1 ngày = tổng tồn kho). Phải dùng periodic snapshot.

**Yêu cầu:** 
Thiết kế bảng `fact_inventory_snapshot` và viết truy vấn lấy tồn kho cuối tháng của mỗi linh kiện tại từng trạm.

**SQL Code:**

```sql
-- DDL Fact Inventory Snapshot
CREATE TABLE fact_inventory_snapshot (
    snapshot_date_key INT,
    head_id INT,
    component_id INT,
    quantity_on_hand INT,
    total_value DECIMAL(12,2),
    PRIMARY KEY (snapshot_date_key, head_id, component_id)
);

-- Lấy Tồn kho cuối kỳ (End of Month) cho mỗi trạm và linh kiện
WITH EndOfMonthDates AS (
    SELECT MAX(date_key) AS eom_date_key, calendar_year, calendar_month
    FROM dim_date
    GROUP BY calendar_year, calendar_month
)
SELECT 
    e.calendar_year,
    e.calendar_month,
    f.head_id,
    f.component_id,
    f.quantity_on_hand
FROM fact_inventory_snapshot f
JOIN EndOfMonthDates e ON f.snapshot_date_key = e.eom_date_key;
```

**Giải thích:**
- **Additive**: Doanh thu (Revenue) có thể SUM qua tất cả các chiều (Time, Head, Customer).
- **Semi-Additive**: Tồn kho (Inventory) có thể SUM qua các chiều không phải thời gian (Cộng tồn kho của nhiều trạm được, nhưng không thể cộng theo thời gian).
- **Non-Additive**: Tỷ lệ lợi nhuận (Profit Margin) không thể SUM theo bất kỳ chiều nào (phải tính lại từ tử số / mẫu số).

---

### Bài 4.6 — Slowly Changing Dimension Type 6 (Hybrid 1+2+3)

**Bối cảnh JD:** Bảng `dim_customer` cần duy trì cả lịch sử để tái hiện báo cáo quá khứ (SCD2) VÀ giá trị hiện tại trên mỗi dòng để báo cáo current-state nhanh (SCD1 overwrite + SCD3 previous column). Mô hình này gọi là SCD Type 6 (1+2+3=6).

**Yêu cầu:** 
Viết DDL cho `dim_customer` SCD6 và câu lệnh UPDATE/INSERT (merge logic) khi có thay đổi status.

**SQL Code:**

```sql
-- DDL SCD Type 6
CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY, -- Surrogate Key
    customer_id INT, -- Natural/Business Key
    company_name VARCHAR(100),
    
    -- SCD 2 fields (Lịch sử)
    status VARCHAR(50), 
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN,
    
    -- SCD 3 field (Giá trị cũ)
    previous_status VARCHAR(50),
    
    -- SCD 1 field (Giá trị hiện tại, overwrite trên MỌI dòng của KH này)
    current_status VARCHAR(50)
);

-- Logical Flow khi update (ví dụ dùng MERGE hoặc Transaction)
-- Giả sử KH 101 đổi status từ 'Active' -> 'Inactive' vào ngày '2024-05-01'

BEGIN;

-- 1. Expire dòng cũ (SCD 2) và cập nhật SCD1 & SCD3
UPDATE dim_customer
SET expiration_date = '2024-04-30',
    is_current = FALSE,
    previous_status = status,        -- SCD 3 logic
    current_status = 'Inactive'      -- SCD 1 logic (cập nhật mọi bản ghi lịch sử)
WHERE customer_id = 101 AND is_current = TRUE;

-- Cũng phải update current_status cho các dòng lịch sử cũ hơn nữa
UPDATE dim_customer
SET current_status = 'Inactive'
WHERE customer_id = 101;

-- 2. Insert dòng mới (SCD 2)
INSERT INTO dim_customer (
    customer_id, company_name, status, effective_date, expiration_date, is_current, previous_status, current_status
)
VALUES (
    101, 'ABC Corp', 'Inactive', '2024-05-01', '9999-12-31', TRUE, 'Active', 'Inactive'
);

COMMIT;
```

---

### Bài 4.7 — Tổng hợp Doanh thu theo Năm Tài Khóa với Profit Margin

**Bối cảnh JD:** Báo cáo chi tiết kết hợp Star Schema.

**Yêu cầu:** 
Viết truy vấn JOIN `fact_repair_service_revenue` với `dim_date` và `dim_head` để tính: tổng doanh thu nhân công, phụ tùng, tổng doanh thu theo quý tài khóa. Tính Profit Margin và xếp hạng trạm.

**SQL Code:**

```sql
WITH QuarterRevenue AS (
    SELECT 
        d.fiscal_year,
        d.fiscal_quarter,
        h.name AS head_name,
        SUM(f.service_amount) AS total_labor_rev,
        SUM(f.parts_amount) AS total_parts_rev,
        SUM(f.total_amount) AS total_revenue,
        -- Giả sử COGS (Giá vốn) ước tính là 60% cho linh kiện và 40% cho nhân công
        SUM(f.parts_amount * 0.6 + f.service_amount * 0.4) AS estimated_cogs
    FROM fact_repair_service_revenue f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_head h ON f.head_id = h.id
    GROUP BY d.fiscal_year, d.fiscal_quarter, h.name
)
SELECT 
    fiscal_year,
    fiscal_quarter,
    head_name,
    total_labor_rev,
    total_parts_rev,
    total_revenue,
    -- Profit Margin: Non-additive measure
    ROUND(((total_revenue - estimated_cogs) / NULLIF(total_revenue, 0)) * 100, 2) AS profit_margin_pct,
    -- Rank trạm theo doanh thu mỗi quý
    DENSE_RANK() OVER (
        PARTITION BY fiscal_year, fiscal_quarter 
        ORDER BY total_revenue DESC
    ) AS revenue_rank
FROM QuarterRevenue
ORDER BY fiscal_year, fiscal_quarter, revenue_rank;
```

---

### 🚚 Kịch bản Phỏng vấn Fleet — Nhóm 4

1. **"Tại sao dự án chọn Star Schema thay vì Snowflake Schema cho Data Warehouse?"**
   > *Trả lời:* Star Schema được chọn vì nó tối ưu cho read performance. Snowflake chuẩn hóa dữ liệu, tiết kiệm không gian nhưng đòi hỏi nhiều JOIN phức tạp. Hầu hết các công cụ BI (Tableau, PowerBI) tương thích và hoạt động nhanh nhất trên Star Schema. Trong Data Lake/Data Warehouse hiện đại, storage rẻ nên ta ưu tiên performance hơn là normalization.

2. **"Năm tài khóa (Fiscal Year) được xử lý thế nào trong pipeline để không bị chậm báo cáo?"**
   > *Trả lời:* Thay vì dùng câu lệnh `CASE WHEN` phức tạp tại thời điểm chạy báo cáo, logic Fiscal Year được pre-computed sẵn vào bảng `dim_date` (sinh sẵn dữ liệu 10-20 năm). Khi truy vấn, chỉ cần JOIN trên `date_key` là có ngay `fiscal_year` và `fiscal_quarter`.

3. **"Giải thích sự khác biệt giữa các Measure: Additive, Semi-additive, Non-additive?"**
   > *Trả lời:* Dựa trên dự án Fleet: Additive là Doanh thu (có thể SUM mọi chiều như Thời gian, Trạm). Semi-additive là Tồn kho phụ tùng (chỉ có thể SUM theo Trạm, nhưng không thể SUM theo Ngày). Non-additive là Profit Margin (tỷ lệ phần trăm, không thể SUM theo bất kỳ chiều nào mà phải tự tính lại `(Tổng doanh thu - Tổng chi phí) / Tổng doanh thu`).

---

## NHÓM 5 BỔ SUNG: DATA GOVERNANCE — NÂNG CAO

### Bài 5.5 — CDC Audit Trail: Lịch sử biến động dữ liệu qua Debezium

**Bối cảnh JD:** Thay vì tự build audit log bằng Database Triggers (làm chậm CSDL OLTP), hệ thống Fleet dùng Debezium CDC (Change Data Capture) để tự động capture mọi thay đổi ở tầng WAL của PostgreSQL.

**Yêu cầu:** 
Truy vấn bảng CDC audit trên Data Lake. So sánh Trigger-based và WAL-based CDC.

**SQL Code (Spark SQL / Athena pattern for CDC Lake):**

```sql
-- Dữ liệu Debezium đẩy xuống có format: before_struct, after_struct, op (c, u, d)
SELECT 
    payload.after.id AS customer_id,
    payload.before.status AS old_status,
    payload.after.status AS new_status,
    payload.source.ts_ms AS changed_at,
    payload.op AS operation_type
FROM debezium_raw_customers
WHERE payload.after.id = 1001 
  AND payload.before.status IS DISTINCT FROM payload.after.status
ORDER BY payload.source.ts_ms DESC;
```

**Giải thích:**
| Tính năng | Trigger-based Audit (Bài 5.3) | WAL-based CDC (Debezium) |
|---|---|---|
| **Performance ảnh hưởng** | Cao (Chạy logic ngay trong transaction OLTP) | Cực thấp (Đọc file WAL chạy nền) |
| **Bảo trì** | Phức tạp (Cần viết DDL Trigger cho từng bảng) | Đơn giản (Chỉ cấu hình connector) |
| **Tính toàn vẹn** | Phụ thuộc vào code Trigger | Bắt được 100% thay đổi (kể cả truncate, bulk update) |

**🚚 Liên hệ Fleet:** Để Debezium capture được ảnh trước khi update (`payload.before`), bảng PostgreSQL phải được cấu hình `ALTER TABLE customers REPLICA IDENTITY FULL;`.

---

### Bài 5.6 — Data Retention Policy Implementation

**Bối cảnh JD:** Tuân thủ GDPR/Compliance yêu cầu xóa dữ liệu định danh khách hàng cá nhân sau 5 năm (Retention Policy).

**Yêu cầu:** 
Viết cấu trúc bảng Policy và query tìm dữ liệu hết hạn.

**SQL Code:**

```sql
CREATE TABLE data_retention_policies (
    table_name VARCHAR(100),
    retention_period_months INT,
    retention_column VARCHAR(100)
);

INSERT INTO data_retention_policies VALUES ('customers', 60, 'created_at');

-- Query tìm các KH cần purge (Xóa / Masking)
SELECT id, company_name 
FROM customers 
WHERE created_at < CURRENT_DATE - INTERVAL '60 months'
AND status = 'Inactive';
```

---

### Bài 5.7 — Replication Slot Monitoring (WAL Bloat Prevention)

**Bối cảnh JD:** Vấn đề Critical Production — Nếu Debezium/Kafka Connect bị crash mà không ai biết, Logical Replication Slot trên PostgreSQL sẽ giữ lại toàn bộ WAL files. Nếu để lâu, ổ cứng máy chủ Postgres sẽ đầy (WAL Bloat) và gây sập Database.

**Yêu cầu:** 
Viết truy vấn giám sát lag của replication slot.

**SQL Code:**

```sql
-- Query cảnh báo WAL Lag > 5GB
SELECT 
    slot_name,
    plugin,
    active,
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / (1024 * 1024 * 1024.0) AS lag_gb
FROM pg_replication_slots
WHERE active = false 
   OR pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) > (5 * 1024 * 1024 * 1024); -- > 5GB
```

**Giải thích:**
- `pg_wal_lsn_diff` tính khoảng cách byte giữa WAL sinh ra hiện tại và vị trí slot đang đọc.
- **Safety net**: Configure `max_slot_wal_keep_size = '20GB'` trong postgresql.conf. Nếu lag quá 20GB, Postgres sẽ tự động hy sinh slot (xóa WAL) để bảo vệ ổ cứng, dẫn đến Debezium cần chạy snapshot lại, nhưng DB không bị sập.

---

### 🚚 Kịch bản Phỏng vấn Fleet — Nhóm 5

1. **"Hệ thống Fleet có cơ chế audit trail theo dõi lịch sử chỉnh sửa dữ liệu như thế nào?"**
   > *Trả lời:* Chúng tôi không dùng DB Triggers vì gây tải lên OLTP. Fleet dùng Debezium kết nối vào PostgreSQL Logical Replication. CDC bắt mọi thay đổi ở mức độ WAL và stream qua Kafka xuống Data Lake. Chúng tôi set `REPLICA IDENTITY FULL` để lấy được trọn vẹn record before-image & sau khi update.

2. **"Nếu Debezium connector chết, điều gì xảy ra với PostgreSQL?"**
   > *Trả lời:* Đây là rủi ro WAL bloat. Postgres sẽ giữ WAL lại cho Replication Slot, làm đầy ổ đĩa. Cách giải quyết: 1) Monitor bằng view `pg_replication_slots` và hàm `pg_wal_lsn_diff` để alert khi lag lớn. 2) Cấu hình `max_slot_wal_keep_size` để Postgres drop slot bảo vệ disk.

---

## NHÓM 6 BỔ SUNG: CHUẨN HOÁ DỮ LIỆU CHO AI — NÂNG CAO

### Bài 6.5 — Text Chunking Strategy cho tài liệu kỹ thuật (Adaptive Chunking)

**Bối cảnh JD:** Xây dựng RAG pipeline. Tài liệu kỹ thuật (`SYSTEM-DESIGN-SPEC.md`) cần được chia chunk thông minh theo section header, thay vì fixed 500 tokens, để giữ trọn vẹn context.

**Yêu cầu:** 
Thiết kế bảng metadata quản lý chunk với parent-child relationship.

**SQL Code:**

```sql
CREATE TABLE document_chunks (
    chunk_id UUID PRIMARY KEY,
    document_id INT REFERENCES documents(id),
    parent_chunk_id UUID REFERENCES document_chunks(chunk_id),
    heading_level INT, -- e.g., 1 cho H1, 2 cho H2
    heading_text VARCHAR(255),
    content TEXT,
    token_count INT,
    embedding VECTOR(1536) -- Sử dụng pgvector
);

-- Truy vấn lấy context: Lấy chunk hiện tại và chunk cha của nó để bổ sung context cho LLM
SELECT 
    c.content AS current_chunk,
    p.heading_text AS parent_context,
    p.content AS parent_summary
FROM document_chunks c
LEFT JOIN document_chunks p ON c.parent_chunk_id = p.chunk_id
WHERE c.chunk_id = 'a1b2-c3d4-e5f6';
```

---

### Bài 6.6 — Embedding Freshness Dashboard Query

**Bối cảnh JD:** Tài liệu liên tục được cập nhật. Cần theo dõi chunk nào đã "cũ" (stale) chưa được re-embed và ưu tiên xử lý trong queue.

**Yêu cầu:** 
Viết truy vấn chấm điểm ưu tiên re-embedding: `days_since_update * document_importance_weight`.

**SQL Code:**

```sql
SELECT 
    d.id AS document_id,
    d.title,
    d.importance_weight,
    EXTRACT(DAY FROM CURRENT_TIMESTAMP - d.last_updated_at) AS days_since_update,
    (EXTRACT(DAY FROM CURRENT_TIMESTAMP - d.last_updated_at) * d.importance_weight) AS priority_score
FROM documents d
JOIN document_chunks c ON d.id = c.document_id
WHERE c.last_embedded_at < d.last_updated_at -- Chunk cũ hơn nội dung gốc
GROUP BY d.id, d.title, d.importance_weight, d.last_updated_at
ORDER BY priority_score DESC;
```

---

### 🚚 Kịch bản Phỏng vấn Fleet — Nhóm 6

1. **"Nếu cần xây dựng chatbot hỏi đáp về tình trạng sửa chữa xe cho khách hàng, dữ liệu sẽ lấy từ đâu và architecture như thế nào?"**
   > *Trả lời:* Sẽ là kiến trúc kết hợp. Data RAG (tài liệu quy trình) có thể lấy từ Vector DB. Nhưng data Transactional (tình trạng xe real-time) sẽ được đồng bộ CDC từ PostgreSQL lên Redis Serving Layer. Khi User query, hệ thống sẽ chèn status từ Redis vào LLM Context Prompt. Đồng thời cần có filter chặn PII (thông tin định danh) trước khi đẩy context cho LLM.
