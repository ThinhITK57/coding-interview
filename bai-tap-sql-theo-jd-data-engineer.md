

# BỘ BÀI TẬP SQL "MAY ĐO" THEO JD DATA ENGINEER (BIG DATA / LAKEHOUSE)
*Thiết kế bám sát từng gạch đầu dòng trong JD — dùng để tự luyện trước phỏng vấn và làm "bằng chứng" trong buổi phỏng vấn khi được hỏi "cho ví dụ cụ thể".*

---

## CÁCH DÙNG TÀI LIỆU NÀY

JD này **không** kiểm tra bạn viết `SELECT` đẹp — nó kiểm tra bạn có tư duy của người **vận hành hệ thống dữ liệu ở production** hay không. Mỗi bài tập bên dưới được ánh xạ trực tiếp tới 1 gạch đầu dòng trong JD, kèm câu hỏi phỏng vấn khả năng sẽ được hỏi kèm theo (mục "🎤 Khi phỏng vấn hỏi..."). Học theo đúng thứ tự này để câu trả lời của bạn luôn nối được vào ngôn ngữ của JD.

| Nhóm JD | Chủ đề SQL tương ứng |
|---|---|
| Data Platform / Lakehouse, batch & streaming | MERGE/UPSERT, SCD Type 2, Windowed Streaming Aggregation, Incremental Load |
| Orchestration, đúng hạn, có thể theo dõi | Idempotent SQL, Pipeline Run Audit Table, Backfill, Watermark tracking |
| Data Quality, giám sát, cảnh báo | Anomaly detection, Null-rate check, Row-count reconciliation, Freshness check |
| Mô hình dữ liệu & truy vấn downstream | Star schema (Fact/Dimension), tối ưu JOIN cho BI |
| Data Governance | Row-level security qua VIEW, Column masking, Audit trail, Metadata catalog |
| Chuẩn hoá dữ liệu cho AI/Chatbot | RAG context table, Chunk metadata, PII filtering trước khi đưa vào LLM |

---

## NHÓM 1: DATA PLATFORM / LAKEHOUSE — BATCH & STREAMING PIPELINE

### Bài 1.1 — Incremental Load bằng MERGE (thay vì load lại toàn bộ bảng mỗi ngày)
*Bối cảnh JD*: "Tham gia thiết kế, phát triển và vận hành các luồng xử lý dữ liệu (batch & streaming) trên nền tảng Big Data."

*Yêu cầu*: Đồng bộ bảng `dim_customers` (Lakehouse) từ nguồn `staging_customers` mỗi đêm — chỉ cập nhật bản ghi thay đổi, chèn bản ghi mới, không load lại toàn bộ.
```sql
MERGE INTO dim_customers AS target
USING staging_customers AS source
   ON target.customer_id = source.customer_id
WHEN MATCHED AND target.updated_at < source.updated_at THEN
    UPDATE SET
        target.name        = source.name,
        target.email       = source.email,
        target.city        = source.city,
        target.updated_at  = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, updated_at)
    VALUES (source.customer_id, source.name, source.email, source.city, source.updated_at);
```
*Giải thích*: `MERGE` (hỗ trợ trên Databricks Delta Lake, Snowflake, BigQuery) là công cụ trung tâm của kiến trúc Lakehouse — thay vì `TRUNCATE + INSERT` toàn bộ (tốn tài nguyên, downtime), chỉ xử lý phần **thay đổi**. Điều kiện `updated_at < source.updated_at` tránh ghi đè bằng dữ liệu cũ hơn nếu batch chạy trễ thứ tự.

### Bài 1.2 — Slowly Changing Dimension Type 2 (SCD2) — giữ lịch sử thay đổi dữ liệu
*Bối cảnh JD*: "Phối hợp thiết kế mô hình dữ liệu... phục vụ nhu cầu phân tích, báo cáo."

*Yêu cầu*: Khi khách hàng đổi địa chỉ, hệ thống báo cáo vẫn cần biết "khách hàng ở đâu tại thời điểm mua hàng X" — không được ghi đè mất lịch sử.
```sql
-- Bảng dim_customer_scd2: customer_id, city, valid_from, valid_to, is_current

-- Bước 1: đóng bản ghi cũ khi phát hiện thay đổi
UPDATE dim_customer_scd2
SET valid_to = CURRENT_TIMESTAMP, is_current = FALSE
WHERE customer_id = 501
  AND is_current = TRUE
  AND city <> 'Đà Nẵng';   -- giá trị mới từ nguồn

-- Bước 2: chèn bản ghi mới đại diện trạng thái hiện tại
INSERT INTO dim_customer_scd2 (customer_id, city, valid_from, valid_to, is_current)
VALUES (501, 'Đà Nẵng', CURRENT_TIMESTAMP, NULL, TRUE);

-- Truy vấn "khách hàng ở đâu tại thời điểm đơn hàng được tạo" (dùng cho báo cáo lịch sử chính xác):
SELECT o.order_id, o.order_date, d.city
FROM orders o
JOIN dim_customer_scd2 d
  ON o.customer_id = d.customer_id
 AND o.order_date BETWEEN d.valid_from AND COALESCE(d.valid_to, '9999-12-31');
```
*Giải thích*: SCD2 là kỹ thuật bắt buộc phải biết khi thiết kế Data Warehouse/Lakehouse cho báo cáo lịch sử. `COALESCE(d.valid_to, '9999-12-31')` xử lý bản ghi hiện tại (chưa đóng, `valid_to IS NULL`) để JOIN theo khoảng thời gian không bị lỗi.

### Bài 1.3 — Windowed Aggregation cho dữ liệu Streaming (Tumbling Window)
*Bối cảnh JD*: "Phối hợp xây dựng và tối ưu hệ thống xử lý dữ liệu thời gian thực."

*Yêu cầu*: Từ bảng sự kiện streaming `clickstream_events(event_time, user_id, event_type)` (giả lập bằng SQL trên Spark Structured Streaming / Flink SQL), tính số lượt click mỗi cửa sổ 5 phút.
```sql
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE)   AS window_end,
    COUNT(*) AS click_count
FROM clickstream_events
WHERE event_type = 'click'
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);
```
*Giải thích*: Cú pháp `TUMBLE` là chuẩn Flink SQL/streaming SQL (Spark dùng `window(event_time, "5 minutes")` tương đương). Điểm mấu chốt cần hiểu là khái niệm **watermark** — hệ thống cần biết "chờ dữ liệu trễ bao lâu" trước khi chốt kết quả 1 cửa sổ, tránh window bị đóng quá sớm khi dữ liệu đến muộn (late data).

### Bài 1.4 — Deduplication dữ liệu streaming khi có retry/at-least-once delivery
*Bối cảnh JD*: "đảm bảo độ ổn định và hiệu năng" của hệ thống streaming.

*Yêu cầu*: Kafka/message queue thường đảm bảo "at-least-once", nghĩa là 1 message có thể được xử lý trùng. Loại bỏ event trùng dựa trên `event_id` trước khi ghi vào Lakehouse.
```sql
WITH deduped_batch AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingestion_time DESC) AS rn
    FROM raw_events_batch
)
INSERT INTO lakehouse_events
SELECT event_id, user_id, event_type, event_time, ingestion_time
FROM deduped_batch
WHERE rn = 1;
```
*Giải thích*: Đây là pattern **exactly-once semantics ở tầng ghi** — kể cả khi tầng ingest (Kafka Connect, Kinesis...) gửi trùng message do retry mạng, bước dedupe trước khi `INSERT` đảm bảo bảng đích không có event trùng lặp.

---

## NHÓM 2: ORCHESTRATION — ĐÚNG HẠN & CÓ THỂ THEO DÕI

### Bài 2.1 — Bảng Audit theo dõi trạng thái từng lần chạy pipeline (Pipeline Run Metadata)
*Bối cảnh JD*: "Xây dựng và quản lý quy trình điều phối dữ liệu (orchestration), đảm bảo dữ liệu được xử lý đúng hạn và có thể theo dõi."

*Yêu cầu*: Thiết kế bảng log để Airflow/Dagster ghi lại mỗi lần chạy job, phục vụ theo dõi SLA và debug khi có lỗi.
```sql
CREATE TABLE pipeline_run_log (
    run_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    run_date      DATE NOT NULL,
    status        VARCHAR(20) NOT NULL CHECK (status IN ('running','success','failed')),
    started_at    TIMESTAMP NOT NULL,
    finished_at   TIMESTAMP,
    rows_processed BIGINT,
    error_message TEXT,
    UNIQUE (pipeline_name, run_date)   -- chặn 2 lần chạy trùng ngày (idempotency ở tầng orchestration)
);

-- Truy vấn kiểm tra SLA: pipeline nào chạy quá 30 phút mà chưa xong hôm nay?
SELECT pipeline_name, started_at,
       EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) / 60 AS running_minutes
FROM pipeline_run_log
WHERE run_date = CURRENT_DATE
  AND status = 'running'
  AND started_at < CURRENT_TIMESTAMP - INTERVAL '30 minutes';
```
*Giải thích*: Ràng buộc `UNIQUE (pipeline_name, run_date)` là cơ chế chặn chạy trùng ở tầng database — bổ trợ cho retry logic của orchestrator. Truy vấn SLA phía dưới chính là nền tảng cho một dashboard/cảnh báo "pipeline bị treo".

### Bài 2.2 — Backfill dữ liệu lịch sử một cách an toàn, không phá dữ liệu đang chạy
*Bối cảnh JD*: "đảm bảo dữ liệu được xử lý đúng hạn."

*Yêu cầu*: Phát hiện job batch bị lỗi 3 ngày trước, cần backfill lại 3 ngày đó mà không ảnh hưởng dữ liệu của các ngày khác đang chạy song song.
```sql
BEGIN;

DELETE FROM fact_daily_sales
WHERE sales_date BETWEEN '2026-08-01' AND '2026-08-03';

INSERT INTO fact_daily_sales
SELECT sales_date, product_id, SUM(amount) AS total_amount
FROM staging_sales_raw
WHERE sales_date BETWEEN '2026-08-01' AND '2026-08-03'
GROUP BY sales_date, product_id;

COMMIT;
```
*Giải thích*: Mẫu "xóa theo partition rồi ghi lại" (`DELETE + INSERT` trong 1 transaction) là pattern backfill an toàn, được dùng phổ biến với bảng partition theo ngày trên Lakehouse. Bọc trong `BEGIN/COMMIT` đảm bảo không có trạng thái "dữ liệu đã xóa nhưng chưa ghi lại" nếu job bị crash giữa chừng.

### Bài 2.3 — Kiểm tra tính idempotent trước khi retry một batch job
*Bối cảnh JD*: "có thể theo dõi" — orchestrator cần biết khi nào an toàn để retry.

*Yêu cầu*: Trước khi Airflow retry task load dữ liệu ngày hôm nay, kiểm tra xem batch đó đã từng chạy thành công chưa để tránh insert trùng.
```sql
SELECT COUNT(*) AS already_succeeded
FROM pipeline_run_log
WHERE pipeline_name = 'load_daily_sales'
  AND run_date = CURRENT_DATE
  AND status = 'success';
-- Nếu already_succeeded > 0 → orchestrator nên skip thay vì chạy lại.
```
*Giải thích*: Câu hỏi "pipeline của bạn có idempotent không?" gần như chắc chắn sẽ xuất hiện trong phỏng vấn Data Engineer — chuẩn bị sẵn cách trả lời có kèm ví dụ SQL kiểm tra trạng thái trước khi retry sẽ ghi điểm rất mạnh.

---

## NHÓM 3: DATA QUALITY, GIÁM SÁT & CẢNH BÁO

### Bài 3.1 — Kiểm tra "Freshness" — dữ liệu có bị trễ so với kỳ vọng không?
*Bối cảnh JD*: "Thực hiện các hoạt động đảm bảo chất lượng dữ liệu, giám sát và cảnh báo khi có sự cố."

*Yêu cầu*: Cảnh báo nếu bảng `fact_orders` không có dữ liệu mới trong 2 giờ qua (dấu hiệu pipeline upstream bị đứng).
```sql
SELECT
    MAX(loaded_at) AS last_loaded_at,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(loaded_at))) / 3600 AS hours_since_last_load,
    CASE
        WHEN CURRENT_TIMESTAMP - MAX(loaded_at) > INTERVAL '2 hours' THEN 'ALERT: STALE DATA'
        ELSE 'OK'
    END AS freshness_status
FROM fact_orders;
```
*Giải thích*: Đây là "SQL check" nền tảng cho mọi hệ thống Data Observability (Monkey Wrench, Great Expectations, Monte Carlo...). Nên biết trình bày: freshness check, volume check, schema check, distribution check — là 4 trụ cột của Data Quality mà JD đang nhắc tới.

### Bài 3.2 — Kiểm tra "Volume Anomaly" — số dòng load hôm nay có bất thường so với trung bình lịch sử?
*Yêu cầu*: Phát hiện nếu số đơn hàng load hôm nay giảm/tăng bất thường so với 7 ngày gần nhất (dấu hiệu lỗi ở nguồn hoặc lỗi pipeline).
```sql
WITH daily_counts AS (
    SELECT run_date, rows_processed
    FROM pipeline_run_log
    WHERE pipeline_name = 'load_daily_orders'
      AND run_date >= CURRENT_DATE - INTERVAL '8 days'
),
stats AS (
    SELECT
        AVG(rows_processed) FILTER (WHERE run_date < CURRENT_DATE) AS avg_last_7d,
        STDDEV(rows_processed) FILTER (WHERE run_date < CURRENT_DATE) AS stddev_last_7d,
        MAX(rows_processed) FILTER (WHERE run_date = CURRENT_DATE) AS today_count
    FROM daily_counts
)
SELECT today_count, avg_last_7d, stddev_last_7d,
       CASE
           WHEN ABS(today_count - avg_last_7d) > 2 * stddev_last_7d
           THEN 'ALERT: VOLUME ANOMALY'
           ELSE 'OK'
       END AS volume_check_status
FROM stats;
```
*Giải thích*: Ngưỡng "2 độ lệch chuẩn" là quy tắc phổ biến cho volume anomaly detection tự động — không cần hard-code ngưỡng cố định (vd "phải > 10.000 dòng"), giúp check tự thích nghi khi hệ thống tăng trưởng theo thời gian.

### Bài 3.3 — Reconciliation: đối soát số dòng giữa nguồn và đích sau mỗi lần ETL
*Yêu cầu*: Sau khi load từ `staging` sang `Lakehouse`, xác minh không bị mất dữ liệu (row count phải khớp, hoặc lệch trong ngưỡng cho phép do dedupe).
```sql
WITH source_count AS (
    SELECT COUNT(*) AS cnt FROM staging_orders WHERE load_date = CURRENT_DATE
),
target_count AS (
    SELECT COUNT(*) AS cnt FROM lakehouse.fact_orders WHERE load_date = CURRENT_DATE
)
SELECT s.cnt AS source_rows, t.cnt AS target_rows,
       s.cnt - t.cnt AS row_diff,
       CASE WHEN s.cnt <> t.cnt THEN 'ALERT: ROW COUNT MISMATCH' ELSE 'OK' END AS reconciliation_status
FROM source_count s, target_count t;
```
*Giải thích*: Reconciliation check là bước bắt buộc trong mọi pipeline production nghiêm túc — nếu không có bước này, dữ liệu có thể "âm thầm" bị mất (do lỗi filter sai, join làm nhân bản dòng, hay lỗi network) mà không ai phát hiện cho tới khi báo cáo sai số.

### Bài 3.4 — Kiểm tra tỷ lệ NULL bất thường trên các cột quan trọng (Schema/Completeness Check)
*Yêu cầu*: Cảnh báo nếu tỷ lệ NULL của cột `customer_id` trong bảng `fact_orders` hôm nay vượt 1% (dấu hiệu lỗi join/lỗi mapping ở tầng ETL).
```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_id,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE customer_id IS NULL) / NULLIF(COUNT(*), 0),
        2
    ) AS null_rate_pct,
    CASE
        WHEN COUNT(*) FILTER (WHERE customer_id IS NULL) * 1.0 / NULLIF(COUNT(*), 0) > 0.01
        THEN 'ALERT: NULL RATE TOO HIGH'
        ELSE 'OK'
    END AS null_check_status
FROM fact_orders
WHERE load_date = CURRENT_DATE;
```
*Giải thích*: `NULLIF(COUNT(*), 0)` chặn lỗi chia cho 0 nếu bảng rỗng ngày đó (chính bản thân đây cũng là 1 tín hiệu bất thường cần bắt riêng). Loại check này nên được chạy tự động **ngay sau mỗi lần ETL**, không chờ đến khi BI/downstream báo lỗi.

### Bài 3.5 — Phát hiện Duplicate Key vi phạm giả định "1 dòng = 1 thực thể" trước khi publish dữ liệu
*Yêu cầu*: Trước khi công bố bảng `dim_customers` cho các team downstream (BI, AI) dùng, kiểm tra không có `customer_id` bị trùng.
```sql
SELECT customer_id, COUNT(*) AS duplicate_count
FROM dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
-- Nếu truy vấn này trả về > 0 dòng → CHẶN pipeline publish (data quality gate),
-- không cho downstream (BI/AI) dùng dữ liệu sai.
```
*Giải thích*: Đây chính là khái niệm **"Data Quality Gate"** — một bước kiểm tra tự động chặn dữ liệu lỗi lan xuống downstream, thay vì chỉ ghi log cảnh báo rồi vẫn cho đi tiếp. Nên nhấn mạnh khái niệm này khi phỏng vấn vì JD nói rõ "phục vụ... các hệ thống downstream."

---

## NHÓM 4: MÔ HÌNH DỮ LIỆU & TRUY VẤN PHỤC VỤ BI/DOWNSTREAM

### Bài 4.1 — Thiết kế và truy vấn mô hình Star Schema (Fact + Dimension)
*Bối cảnh JD*: "Phối hợp thiết kế mô hình dữ liệu và truy vấn dữ liệu, phục vụ nhu cầu phân tích, báo cáo."

*Yêu cầu*: Từ mô hình `fact_sales(order_id, date_key, customer_key, product_key, amount)` + các bảng dimension, tạo báo cáo doanh thu theo Quý, theo Danh mục sản phẩm, theo Vùng khách hàng.
```sql
SELECT
    d.quarter,
    p.category,
    c.region,
    SUM(f.amount) AS total_revenue
FROM fact_sales f
JOIN dim_date d      ON f.date_key = d.date_key
JOIN dim_product p   ON f.product_key = p.product_key
JOIN dim_customer c  ON f.customer_key = c.customer_key
GROUP BY d.quarter, p.category, c.region
ORDER BY d.quarter, total_revenue DESC;
```
*Giải thích*: Star schema là mô hình chuẩn cho lớp "Serving/Gold" trong kiến trúc Lakehouse (Medallion Architecture: Bronze → Silver → Gold). Việc join `fact` với nhiều `dimension` cần chuẩn bị index/partition trên các khóa (`date_key`, `product_key`...) để BI tool (Looker, Power BI, Superset) truy vấn nhanh khi có hàng triệu dòng fact.

### Bài 4.2 — Tối ưu truy vấn BI lặp lại bằng Pre-aggregated Table (Rollup)
*Yêu cầu*: Dashboard doanh thu theo ngày được hàng trăm người mở mỗi ngày, mỗi lần đều quét `fact_sales` (hàng tỷ dòng). Tối ưu bằng bảng rollup được cập nhật theo batch.
```sql
CREATE TABLE agg_daily_revenue AS
SELECT date_key, category, region, SUM(amount) AS total_revenue, COUNT(*) AS order_count
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY date_key, category, region;

-- Job batch cập nhật incremental mỗi ngày (không build lại toàn bộ):
INSERT INTO agg_daily_revenue
SELECT date_key, category, region, SUM(amount), COUNT(*)
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE date_key = CURRENT_DATE
GROUP BY date_key, category, region;
```
*Giải thích*: Dashboard chỉ query bảng đã gộp sẵn (`agg_daily_revenue`, vài nghìn dòng) thay vì bảng fact gốc (hàng tỷ dòng) — giảm tải nghiêm trọng cho hệ thống, đây là kỹ năng "tối ưu hiệu năng" mà JD nhắc trực tiếp.

---

## NHÓM 5: DATA GOVERNANCE — PHÂN QUYỀN, METADATA, TRUY VẾT

### Bài 5.1 — Row-Level Security bằng VIEW (mỗi team chỉ thấy dữ liệu vùng của mình)
*Bối cảnh JD*: "Tuân thủ và triển khai các yêu cầu về data governance: phân quyền truy cập..."

*Yêu cầu*: Team Sales miền Bắc chỉ được truy vấn dữ liệu đơn hàng thuộc miền Bắc, không thấy dữ liệu miền khác.
```sql
CREATE VIEW vw_orders_north_region AS
SELECT order_id, customer_id, amount, order_date, region
FROM fact_sales
WHERE region = 'Miền Bắc';

-- Cấp quyền chỉ trên view, không cấp quyền trực tiếp trên bảng gốc:
GRANT SELECT ON vw_orders_north_region TO role_sales_north;
```
*Giải thích*: Đây là pattern Row-Level Security (RLS) đơn giản không cần engine hỗ trợ RLS native. Ở các nền tảng như Snowflake/BigQuery có thể dùng `ROW ACCESS POLICY` native, nhưng nguyên lý cốt lõi giống nhau: **không bao giờ cấp quyền trực tiếp trên bảng chứa toàn bộ dữ liệu nhạy cảm**.

### Bài 5.2 — Column Masking cho dữ liệu nhạy cảm (PII)
*Yêu cầu*: Team Data/BI cần xem dữ liệu khách hàng để phân tích nhưng không được thấy số điện thoại đầy đủ (chỉ 4 số cuối).
```sql
CREATE VIEW vw_customers_masked AS
SELECT
    customer_id,
    name,
    CONCAT('****', RIGHT(phone_number, 4)) AS phone_number_masked,
    CONCAT(LEFT(email, 2), '***@', SPLIT_PART(email, '@', 2)) AS email_masked,
    city
FROM customers;
```
*Giải thích*: Masking ngay ở tầng view đảm bảo **mọi** truy vấn xuống bảng đều tự động được che dữ liệu nhạy cảm, không phụ thuộc vào việc từng analyst có "nhớ" che thông tin hay không — đúng tinh thần "an toàn theo thiết kế" (security/privacy by design) mà governance yêu cầu.

### Bài 5.3 — Bảng Audit Log truy vết ai đã truy cập dữ liệu nhạy cảm (Data Lineage / Access Trail)
*Bối cảnh JD*: "...quản lý metadata, truy vết dữ liệu."

*Yêu cầu*: Ghi lại lịch sử truy cập vào bảng chứa PII để phục vụ audit tuân thủ (compliance).
```sql
CREATE TABLE data_access_audit (
    access_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    accessed_by   VARCHAR(100) NOT NULL,
    table_name    VARCHAR(100) NOT NULL,
    query_text    TEXT,
    accessed_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_count_returned INT
);

-- Truy vấn phục vụ audit định kỳ: ai đã truy cập bảng PII nhiều bất thường trong tuần?
SELECT accessed_by, COUNT(*) AS access_count
FROM data_access_audit
WHERE table_name = 'customers'
  AND accessed_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY accessed_by
ORDER BY access_count DESC;
```
*Giải thích*: Trong thực tế, log này thường được sinh tự động từ `query_history` của warehouse (Snowflake `ACCOUNT_USAGE.QUERY_HISTORY`, BigQuery `INFORMATION_SCHEMA.JOBS`), nhưng hiểu được **mô hình dữ liệu** cần thiết để trả lời "ai đã truy cập gì, khi nào" là kiến thức governance cốt lõi.

### Bài 5.4 — Truy vấn Metadata Catalog để tìm cột chứa PII trên toàn hệ thống
*Yêu cầu*: Trước khi mở quyền cho 1 bảng mới, kiểm tra nhanh xem bảng đó có cột nào khả nghi là PII (dựa trên tên cột) không.
```sql
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE column_name ILIKE ANY (ARRAY['%email%', '%phone%', '%cmnd%', '%cccd%', '%address%', '%dob%', '%salary%'])
ORDER BY table_schema, table_name;
```
*Giải thích*: `information_schema` (hoặc catalog tương đương như Unity Catalog trên Databricks, Glue Data Catalog trên AWS) là nơi lưu metadata toàn hệ thống — biết truy vấn nó để tự động rà soát PII là kỹ năng governance thực chiến, thể hiện tư duy chủ động thay vì chỉ làm theo yêu cầu.

---

## NHÓM 6: CHUẨN HOÁ DỮ LIỆU CHO AI / CHATBOT

### Bài 6.1 — Thiết kế bảng metadata cho hệ thống RAG (Retrieval-Augmented Generation)
*Bối cảnh JD*: "Chuẩn hoá và mô tả dữ liệu để phục vụ các ứng dụng AI/Chatbot, đảm bảo dữ liệu được sử dụng đúng ngữ cảnh và an toàn."

*Yêu cầu*: Thiết kế bảng lưu các đoạn văn bản (chunk) đã qua xử lý, kèm metadata để chatbot truy xuất đúng ngữ cảnh và có thể trích dẫn nguồn.
```sql
CREATE TABLE knowledge_chunks (
    chunk_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_document VARCHAR(255) NOT NULL,
    source_url      TEXT,
    chunk_text      TEXT NOT NULL,
    chunk_index     INT NOT NULL,          -- vị trí đoạn trong tài liệu gốc, phục vụ truy vết
    department      VARCHAR(50),           -- dùng để lọc theo quyền truy cập (vd: chỉ HR mới thấy tài liệu HR)
    sensitivity_level VARCHAR(20) DEFAULT 'public'
                     CHECK (sensitivity_level IN ('public','internal','confidential')),
    embedding_updated_at TIMESTAMP,
    is_active       BOOLEAN DEFAULT TRUE   -- soft-flag khi tài liệu gốc bị thu hồi/lỗi thời
);
```
*Giải thích*: `sensitivity_level` và `department` chính là cách "đảm bảo dữ liệu được sử dụng đúng ngữ cảnh và an toàn" ở tầng dữ liệu — chatbot khi truy xuất phải luôn filter theo quyền của người hỏi, không được lấy nhầm tài liệu confidential trả lời cho user không có quyền.

### Bài 6.2 — Lọc bỏ PII trước khi dữ liệu được đưa vào pipeline huấn luyện/RAG cho AI
*Yêu cầu*: Trước khi đẩy nội dung ticket hỗ trợ khách hàng vào hệ thống làm dữ liệu huấn luyện chatbot, phải đảm bảo không có email/số điện thoại khách hàng lọt vào.
```sql
SELECT ticket_id,
       REGEXP_REPLACE(
           REGEXP_REPLACE(content, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '[EMAIL_REDACTED]', 'g'),
           '(0|\+84)[0-9]{9,10}', '[PHONE_REDACTED]', 'g'
       ) AS content_sanitized
FROM support_tickets
WHERE status = 'closed';
```
*Giải thích*: Đây là bước "chuẩn hoá dữ liệu an toàn cho AI" cụ thể nhất trong JD — dùng `REGEXP_REPLACE` để tự động ẩn PII ở quy mô lớn trước khi dữ liệu rời khỏi hệ thống nguồn. Trong thực tế production thường kết hợp thêm NER model (qua Python) cho các trường hợp regex không bắt được hết, nhưng lớp SQL này là hàng rào đầu tiên, rẻ và nhanh.

### Bài 6.3 — Truy vấn phục vụ Context Window: lấy đúng và đủ ngữ cảnh liên quan cho 1 câu hỏi
*Yêu cầu*: Khi chatbot cần trả lời câu hỏi về "chính sách nghỉ phép", lấy các chunk liên quan nhất, đã được duyệt (active), và đúng quyền của user hỏi (`user_department`).
```sql
SELECT chunk_id, chunk_text, source_document, chunk_index
FROM knowledge_chunks
WHERE is_active = TRUE
  AND (sensitivity_level = 'public' OR department = 'HR')  -- user thuộc phòng HR hoặc tài liệu public
  AND department IN ('HR', 'General')
ORDER BY chunk_index
LIMIT 5;
```
*Giải thích*: Câu này minh họa nguyên tắc "an toàn theo ngữ cảnh" — filter quyền truy cập phải nằm **ngay trong câu truy vấn lấy context**, không phải là bước lọc "hậu kiểm" sau khi đã lấy dữ liệu và đưa vào prompt LLM (lúc đó đã quá muộn để chặn rò rỉ thông tin).

### Bài 6.4 — Theo dõi chất lượng dữ liệu nguồn cho AI: phát hiện tài liệu chưa được cập nhật embedding
*Yêu cầu*: Dữ liệu nguồn (`source_document`) đã sửa đổi nhưng embedding chưa được tính lại — chatbot đang trả lời dựa trên nội dung cũ. Tìm các chunk cần re-embed.
```sql
SELECT k.chunk_id, k.source_document, k.embedding_updated_at, s.last_modified_at
FROM knowledge_chunks k
JOIN source_documents s ON k.source_document = s.document_name
WHERE k.is_active = TRUE
  AND (k.embedding_updated_at IS NULL OR k.embedding_updated_at < s.last_modified_at);
```
*Giải thích*: Đây chính là bài toán "data freshness cho hệ thống AI" — một dạng chuyên biệt của khái niệm freshness check ở Nhóm 3, nhưng áp dụng cho ngữ cảnh RAG. Biết liên kết 2 khái niệm này với nhau (data quality tổng quát ↔ áp dụng cho AI) là điểm cộng lớn vì JD đặt 2 mục này cạnh nhau có chủ đích.

---

## GỢI Ý TRẢ LỜI PHỎNG VẤN: NỐI BÀI TẬP VỚI NGÔN NGỮ CỦA JD

Khi được hỏi dạng "Kể về một lần bạn xử lý vấn đề chất lượng dữ liệu / tối ưu pipeline", hãy cấu trúc câu trả lời theo 4 bước, viện dẫn đúng SQL pattern tương ứng ở trên:

1. **Bối cảnh** — hệ thống nào, batch hay streaming, quy mô dữ liệu (liên hệ Nhóm 1).
2. **Vấn đề phát hiện qua đâu** — nhờ cơ chế giám sát/cảnh báo nào (liên hệ Nhóm 3: freshness/volume/null-rate check).
3. **Cách xử lý** — pattern SQL cụ thể đã dùng (MERGE, SCD2, dedupe, backfill — Nhóm 1 & 2).
4. **Kết quả & phòng ngừa tái diễn** — thêm Data Quality Gate, audit log, hay alert tự động (Nhóm 3 & 5).

Cách trả lời này cho thấy bạn tư duy **hệ thống** (system thinking) — đúng năng lực cấp Senior/3+ năm kinh nghiệm mà JD đang tìm, chứ không chỉ là người biết viết truy vấn đúng.

## GỢI Ý LUYỆN TẬP THÊM

- Cài thử **DuckDB** hoặc **Databricks Community Edition** (miễn phí) để chạy thật các câu `MERGE`, `TUMBLE window`, `information_schema` — nhiều câu ở trên có cú pháp khác nhau nhẹ giữa Snowflake/BigQuery/Spark SQL/Postgres, nên biết sự khác biệt sẽ ghi điểm khi phỏng vấn.
- Chuẩn bị sẵn 1-2 câu chuyện thực tế (dù ở quy mô nhỏ) áp dụng đúng pattern SCD2, incremental load, hoặc data quality gate — nhà tuyển dụng Data Engineer luôn hỏi ví dụ cụ thể hơn là hỏi lý thuyết suông.
- Vì JD có nhắc "Python phục vụ xử lý dữ liệu" và "mentor cho thành viên level thấp hơn" — nên chuẩn bị thêm cách giải thích lại các pattern SQL này bằng lời đơn giản, như thể đang hướng dẫn một Junior — đây cũng là cách nhà tuyển dụng đánh giá khả năng mentor của bạn.
