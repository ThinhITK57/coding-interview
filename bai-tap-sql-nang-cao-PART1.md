# BÀI TẬP SQL NÂNG CAO - PHẦN 1 (DATA ENGINEER INTERVIEW PREP)

*Tài liệu luyện thi phỏng vấn Kỹ sư Dữ liệu (Data Engineer) - Focus vào hệ thống Lakehouse, Orchestration, và Data Quality.*
*Liên kết bối cảnh: Dự án Fleet Platform.*

---

## NHÓM 1 BỔ SUNG: DATA PLATFORM / LAKEHOUSE — NÂNG CAO

### Bài 1.5 — MERGE với DELETE clause (xử lý soft-delete từ CDC)

**Bối cảnh JD:** Hệ thống Debezium CDC phát ra event với `op='d'` (delete) khi một record bị xóa ở Odoo (ví dụ xóa nhầm work_order). Data Lakehouse cần phản ánh sự thay đổi này nhưng không được xóa vật lý (hard-delete) ngay lập tức để đảm bảo tính tracking và time-travel.

**Yêu cầu:** Viết câu lệnh `MERGE` xử lý các event `INSERT` (`op='c'`), `UPDATE` (`op='u'`) và `DELETE` (`op='d'`). Áp dụng cơ chế soft-delete bằng cách cập nhật cờ `is_deleted` và `deleted_at`.

**SQL Code (Spark SQL / PostgreSQL):**
```sql
MERGE INTO target_work_orders t
USING (
    -- Dữ liệu staging từ Kafka/CDC
    SELECT 
        payload_id AS id, 
        payload_head_id AS head_id,
        payload_customer_id AS customer_id,
        payload_status AS status,
        op, 
        ts_ms AS cdc_timestamp
    FROM staging_cdc_work_orders
) s
ON t.id = s.id
-- Xử lý DELETE: Cập nhật cờ thay vì xóa vật lý
WHEN MATCHED AND s.op = 'd' THEN
    UPDATE SET 
        is_deleted = TRUE, 
        deleted_at = TO_TIMESTAMP(s.cdc_timestamp / 1000)
-- Xử lý UPDATE
WHEN MATCHED AND s.op IN ('u', 'c') THEN
    UPDATE SET 
        head_id = s.head_id,
        customer_id = s.customer_id,
        status = s.status,
        is_deleted = FALSE,
        deleted_at = NULL,
        updated_at = TO_TIMESTAMP(s.cdc_timestamp / 1000)
-- Xử lý INSERT
WHEN NOT MATCHED AND s.op IN ('c', 'u') THEN
    INSERT (id, head_id, customer_id, status, is_deleted, created_at, updated_at)
    VALUES (s.id, s.head_id, s.customer_id, s.status, FALSE, TO_TIMESTAMP(s.cdc_timestamp / 1000), TO_TIMESTAMP(s.cdc_timestamp / 1000));
```

**Giải thích:** 
Tại sao Lakehouse ưu tiên Soft-delete hơn Hard-delete?
1. **Bản chất File Parquet/Delta:** File columnar là bất biến (immutable). Xóa một record vật lý đòi hỏi phải rewrite toàn bộ file (I/O operation cực kỳ tốn kém). Soft-delete chỉ là một thao tác append/update metadata.
2. **Audit & Time-travel:** Cho phép hệ thống phục hồi dữ liệu bị xóa nhầm hoặc truy vấn trạng thái hệ thống tại một thời điểm trong quá khứ.

🎤 **Khi phỏng vấn hỏi...** "Nếu database nguồn thực sự muốn xóa bỏ dữ liệu do quy định GDPR, bạn làm thế nào?" -> Trả lời: Áp dụng Soft-delete ở layer Silver, sau đó có các job Vacuum/Compaction chạy định kỳ hàng tuần ở layer Gold để xóa vật lý (hard-delete) dữ liệu đã soft-delete quá 30 ngày.

---

### Bài 1.6 — High Watermark Incremental Load Pattern

**Bối cảnh JD:** Thay vì dùng MERGE so khớp toàn bộ batch mỗi lần ETL chạy, chúng ta cần track "watermark" (thời điểm cập nhật lớn nhất đã xử lý) để chỉ lấy phần dữ liệu mới (delta) từ source.

**Yêu cầu:** Viết script tạo bảng watermark, câu lệnh truy xuất delta data, và câu lệnh cập nhật watermark sau khi load thành công.

**SQL Code (PostgreSQL / Data Warehouse):**
```sql
-- 1. Tạo bảng Watermark Tracking
CREATE TABLE IF NOT EXISTS etl_watermarks (
    table_name VARCHAR(100) PRIMARY KEY,
    last_processed_updated_at TIMESTAMP,
    last_processed_lsn BIGINT -- Track Log Sequence Number từ Debezium
);

-- 2. Query lấy dữ liệu Delta (Incremental Pull)
SELECT * 
FROM raw_invoices 
WHERE updated_at > (
    SELECT COALESCE(MAX(last_processed_updated_at), '1970-01-01') 
    FROM etl_watermarks 
    WHERE table_name = 'raw_invoices'
);

-- 3. Cập nhật Watermark sau khi xử lý thành công batch
UPDATE etl_watermarks 
SET 
    last_processed_updated_at = (SELECT MAX(updated_at) FROM staging_invoices_batch),
    last_processed_lsn = (SELECT MAX(lsn) FROM staging_invoices_batch)
WHERE table_name = 'raw_invoices';
```

🚚 **Liên hệ dự án Fleet:**
Trong Debezium CDC, thuộc tính `confirmed_flush_lsn` hoặc Kafka offset đóng vai trò như một watermark. Spark Structured Streaming quản lý watermark này tự động thông qua checkpointing (lưu offset vào HDFS). Nếu chạy batch, ta phải tự track watermark như SQL ở trên.

---

### Bài 1.7 — Late-Arriving Facts (Dữ liệu đến trễ)

**Bối cảnh JD:** Một `invoice` được tạo ngày `31/07` ở Odoo, nhưng do Kafka bị lag hoặc network rớt, đến ngày `02/08` dữ liệu mới rơi xuống HDFS Data Lake. Nếu dùng Partition Overwrite theo ngày thực thi (`02/08`), ta sẽ ghi đè nhầm hoặc bỏ sót partition `31/07`.

**Yêu cầu:** Viết SQL để MERGE dữ liệu đến trễ vào đúng partition của nó, độc lập với ngày chạy ETL.

**SQL Code (Spark SQL):**
```sql
-- Bật Dynamic Partition Overwrite trong Spark
-- SET spark.sql.sources.partitionOverwriteMode=dynamic;

MERGE INTO fact_invoices target
USING staging_late_invoices source
ON target.id = source.id 
   AND target.invoice_date = source.invoice_date -- Quan trọng: Điều kiện on partition column
WHEN MATCHED THEN 
    UPDATE SET 
        target.service_amount = source.service_amount,
        target.parts_amount = source.parts_amount,
        target.total_amount = source.total_amount,
        target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN 
    INSERT (id, work_order_id, customer_id, invoice_date, service_amount, parts_amount, total_amount)
    VALUES (source.id, source.work_order_id, source.customer_id, source.invoice_date, source.service_amount, source.parts_amount, source.total_amount);
```

🚚 **Liên hệ dự án Fleet:** Spark `dynamic` partition overwrite mode cho phép Spark chỉ ghi đè những partition (ví dụ thư mục `invoice_date=2023-07-31`) có xuất hiện trong DataFrame staging mới nhất, giữ nguyên các partition khác, giải quyết bài toán Late-Arriving data cực kỳ thanh lịch.

---

### Bài 1.8 — Sliding Window vs Tumbling Window Aggregation

**Bối cảnh JD:** Fleet có dữ liệu telemetry (nhiệt độ động cơ `engine_temp_c`) gửi về Kafka mỗi 10 giây. Cần tính toán trung bình nhiệt độ.

**Yêu cầu:** Viết Flink SQL hoặc Spark SQL xử lý window 5 phút, nhưng với Tumbling Window và Sliding Window (trượt mỗi 1 phút). Giải thích khi nào dùng loại nào.

**SQL Code (Flink SQL):**
```sql
-- 1. Tumbling Window (Cửa sổ cố định, không gối lên nhau)
-- Dùng để: Tính report tổng kết định kỳ, vd: "Tổng doanh thu mỗi giờ".
SELECT 
    window_start, window_end, truck_plate,
    AVG(engine_temp_c) as avg_temp
FROM TABLE(
    TUMBLE(TABLE telemetry_stream, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, truck_plate;

-- 2. Sliding Window / HOP (Cửa sổ trượt, gối lên nhau)
-- Dùng để: Cảnh báo realtime (Alerting), vd: "Nhiệt độ trung bình 5 phút gần nhất vượt 90 độ, cập nhật mỗi phút".
SELECT 
    window_start, window_end, truck_plate,
    AVG(engine_temp_c) as avg_temp
FROM TABLE(
    HOP(TABLE telemetry_stream, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, truck_plate;
```

---

### Bài 1.9 — Exactly-Once Semantics: Idempotent MERGE vs INSERT

**Bối cảnh JD:** Pipeline bị fail giữa chừng. Khi chạy lại (retry), nếu dùng `INSERT`, dữ liệu có thể bị nhân đôi. Thiết kế pipeline theo hướng Idempotent (chạy 1 hay N lần kết quả không đổi) là bắt buộc.

**Yêu cầu:** Viết SQL thể hiện sự khác biệt giữa Non-idempotent và Idempotent target.

**SQL Code:**
```sql
-- NON-IDEMPOTENT (Sẽ gây duplicate nếu retry)
INSERT INTO dim_head (id, name, lat, lng, capacity)
SELECT id, name, lat, lng, capacity FROM staging_heads;

-- IDEMPOTENT (An toàn khi retry nhiều lần)
MERGE INTO dim_head target
USING staging_heads source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET name = source.name, lat = source.lat, lng = source.lng, capacity = source.capacity
WHEN NOT MATCHED THEN
    INSERT (id, name, lat, lng, capacity)
    VALUES (source.id, source.name, source.lat, source.lng, source.capacity);
```

🚚 **Liên hệ dự án Fleet:** Trong Spark Structured Streaming, ghi ra HDFS Parquet với `FileStreamSinkLog` kết hợp cơ chế idempotent sink đảm bảo được Exactly-Once. Spark sẽ theo dõi từng batch id trong thư mục `_spark_metadata`. Dù Kafka lag và replay lại messages, bản chất MERGE/Upsert hoặc metadata log của Spark sẽ đảm bảo không có duplicate records.

---

### 🚚 Kịch bản Phỏng vấn Fleet — Nhóm 1

1. **"Bạn xử lý CDC events từ Odoo vào Lakehouse như thế nào?"**
   * **Trả lời:** Em sử dụng Debezium để bắt các event (Insert, Update, Delete) dưới dạng JSON đẩy vào Kafka (RF=3 để đảm bảo high availability). Sau đó dùng Spark Structured Streaming đọc từ Kafka, parse payload và apply cú pháp `MERGE INTO` vào Delta/Iceberg format trên HDFS. Với record Update, em dùng SCD Type 2 để track lịch sử thay đổi của Dimension (như `dim_customer`). Với record Delete, em không xóa vật lý mà dùng soft-delete flag để duy trì lịch sử và đảm bảo Exactly-Once qua Idempotent MERGE.
2. **"Nếu Kafka consumer bị lag 5 triệu messages, dữ liệu có bị mất không?"**
   * **Trả lời:** Dạ không. Kafka lưu trữ log theo retention policy (ví dụ 7 ngày), việc consumer lag chỉ là chậm trễ về mặt thời gian xử lý (latency tăng). Nhờ cơ chế Checkpointing trong Spark, khi consumer chạy lại, nó sẽ resume từ offset cuối cùng được commit. Kết hợp với Idempotent Sink (như MERGE), việc replay data (nếu xảy ra) hoàn toàn an toàn và không gây ra duplicate data.
3. **"Streaming aggregation trên telemetry data được thiết kế ra sao?"**
   * **Trả lời:** Đối với dữ liệu telemetry từ Fleet (truck sensors), em áp dụng Event Time Processing kết hợp với Watermark (để xử lý late data khoảng 10-15 phút). Để trigger các cảnh báo (alerts) realtime như quá nhiệt, em dùng Sliding Window (cửa sổ 5 phút, trượt 1 phút). Để lưu trữ historical aggregation vào Data Warehouse, em dùng Tumbling Window (cửa sổ 15 phút cố định) để giảm thiểu I/O operations xuống HDFS.

---

## NHÓM 2 BỔ SUNG: ORCHESTRATION — NÂNG CAO

### Bài 2.4 — Cross-DAG Dependency Sensor Pattern

**Bối cảnh JD:** Trong Airflow, DAG `monthly_report` tính toán doanh thu tổng kết tháng không thể chạy nếu DAG `scd2_customer_dimension` của ngày hôm đó chưa chạy xong.

**Yêu cầu:** Viết logic/SQL thể hiện việc kiểm tra trạng thái dependency giữa các quy trình (ETL orchestration metadata).

**SQL Code (Truy vấn metadata DB của Airflow hoặc custom control table):**
```sql
-- Giả lập logic của ExternalTaskSensor trong Airflow
SELECT state 
FROM dag_run 
WHERE dag_id = 'scd2_customer_dimension'
  AND execution_date = '2023-10-31 00:00:00'
  AND state = 'success';
-- Pipeline báo cáo hàng tháng sẽ poll câu lệnh này, chỉ chạy tiếp khi trả về 'success'
```

🚚 **Liên hệ dự án Fleet:** Trong mã Airflow thực tế, thay vì query SQL thủ công, ta dùng `ExternalTaskSensor(task_id='wait_for_dim_customer', external_dag_id='scd2_customer_dimension', external_task_id='merge_customer_data', ...)`.

---

### Bài 2.5 — Partition-Level Watermark Tracking

**Bối cảnh JD:** Bạn cần backfill dữ liệu cho 6 tháng trước nhưng chỉ muốn chạy lại những ngày (partition) bị lỗi. 

**Yêu cầu:** Viết SQL track trạng thái partition và query tìm các partition cần backfill.

**SQL Code:**
```sql
CREATE TABLE partition_audit_log (
    table_name VARCHAR(50),
    partition_date DATE,
    status VARCHAR(20), -- 'SUCCESS', 'FAILED', 'RUNNING'
    processed_records INT,
    last_run TIMESTAMP,
    PRIMARY KEY (table_name, partition_date)
);

-- Query các partition cần Backfill (bị FAILED hoặc chưa từng chạy)
SELECT partition_date
FROM partition_audit_log
WHERE table_name = 'fact_repair_service_revenue'
  AND (status = 'FAILED' OR status IS NULL)
  AND partition_date BETWEEN '2023-01-01' AND '2023-06-30'
ORDER BY partition_date ASC;
```

---

### Bài 2.6 — Dead Letter Queue Pattern

**Bối cảnh JD:** CDC event có thể chứa payload bị lỗi định dạng (corrupted JSON) hoặc vi phạm business logic (ví dụ `work_order` có `customer_id` bị null). ETL không được sập mà phải rẽ nhánh (route) record lỗi ra bảng DLQ để team Data Quality review.

**Yêu cầu:** Viết SQL `INSERT...SELECT` phân luồng dữ liệu hợp lệ và không hợp lệ.

**SQL Code:**
```sql
-- Route Bad Records vào DLQ
INSERT INTO dlq_work_orders (payload, error_reason, ingested_at)
SELECT 
    raw_payload, 
    'MISSING_CUSTOMER_ID' AS error_reason,
    CURRENT_TIMESTAMP
FROM staging_raw_work_orders
WHERE customer_id IS NULL OR customer_id = '';

-- Route Good Records vào Pipeline chính
INSERT INTO clean_work_orders (id, head_id, customer_id, status)
SELECT id, head_id, customer_id, status
FROM staging_raw_work_orders
WHERE customer_id IS NOT NULL AND customer_id != '';
```

---

### 🚚 Kịch bản Phỏng vấn Fleet — Nhóm 2

1. **"Pipeline bị lỗi 3 ngày, bạn backfill như thế nào mà không ảnh hưởng production?"**
   * **Trả lời:** Em sử dụng cơ chế Partition-level tracking kết hợp với tính năng Backfill của Airflow. Kịch bản backfill chạy trong một DAG riêng rẽ không cạnh tranh tài nguyên với DAG daily. Để đảm bảo dữ liệu toàn vẹn, em gói logic overwrite trong một Transaction (nếu là PostgreSQL) hoặc dùng Partition Overwrite Mode của Spark. Các truy vấn đọc (từ user hoặc dashboard dashboard WebSocket) sẽ không bị gián đoạn hay đọc phải dữ liệu dở dang (dirty read).
2. **"Airflow sensor bị treo chiếm hết worker slot, xử lý sao?"**
   * **Trả lời:** Hiện tượng này xảy ra khi dùng Sensor với chế độ mặc định `mode='poke'`. Sensor sẽ liên tục giữ worker slot để kiểm tra điều kiện. Giải pháp của em là đổi sang `mode='reschedule'`. Lúc này, nếu điều kiện chưa thỏa mãn, Sensor sẽ release worker slot cho các task khác chạy, và chỉ ngủ theo khoảng thời gian `poke_interval` rồi mới queue lại.

---

## NHÓM 3 BỔ SUNG: DATA QUALITY — NÂNG CAO

### Bài 3.6 — Distribution Drift Detection (Schema Stability)

**Bối cảnh JD:** Bạn cần phát hiện xem phân bố dữ liệu có bất thường không. Ví dụ cột `status` của `work_orders` bình thường có 80% là 'completed', nhưng hôm nay chỉ có 10% 'completed'.

**Yêu cầu:** Viết SQL tính tỷ lệ % distribution của cột `status` hôm nay so với trung bình 30 ngày trước.

**SQL Code:**
```sql
WITH today_dist AS (
    SELECT status, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percent_today
    FROM work_orders
    WHERE DATE(created_at) = CURRENT_DATE
    GROUP BY status
),
hist_dist AS (
    SELECT status, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percent_hist
    FROM work_orders
    WHERE DATE(created_at) >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY status
)
SELECT 
    t.status, 
    t.percent_today, 
    h.percent_hist,
    ABS(t.percent_today - h.percent_hist) AS drift_variance
FROM today_dist t
JOIN hist_dist h ON t.status = h.status
WHERE ABS(t.percent_today - h.percent_hist) > 15; -- Cảnh báo nếu lệch > 15%
```

---

### Bài 3.7 — Cross-Table Referential Integrity Check

**Bối cảnh JD:** Bảng Fact `fact_repair_service_revenue` tham chiếu đến Dimension `dim_customer`. Tuy nhiên do lỗi xử lý SCD2 hoặc Data CDC bất đồng bộ, có thể xuất hiện các `customer_id` trong Fact không tồn tại trong Dimension.

**Yêu cầu:** Viết SQL tìm các record Fact bị mồ côi (orphaned foreign keys).

**SQL Code:**
```sql
SELECT 
    f.id AS fact_invoice_id, 
    f.customer_id AS missing_customer_id,
    f.invoice_date
FROM fact_repair_service_revenue f
LEFT JOIN dim_customer d 
  ON f.customer_id = d.id 
  -- Nếu dùng Surrogate Key trong SCD2: ON f.customer_key = d.customer_key
WHERE d.id IS NULL;
```

---

### Bài 3.8 — Schema Evolution Detection

**Bối cảnh JD:** Team Backend Odoo tự ý thêm cột mới vào bảng ERP. Debezium đọc được, ném vào Kafka, nhưng schema Spark đọc tĩnh nên dẫn đến rớt data cột mới hoặc gây lỗi pipeline.

**Yêu cầu:** Viết SQL (Information Schema) để so sánh schema hiện tại của DB so với schema snapshot mong đợi.

**SQL Code:**
```sql
-- PostgreSQL Information Schema check
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'customers'
EXCEPT 
SELECT column_name, data_type 
FROM expected_schema_metadata 
WHERE table_name = 'customers';
```

🚚 **Liên hệ dự án Fleet:** Thay vì hardcode schema bằng `StructType` trong PySpark, nên bật tính năng `schemaEvolution` (nếu dùng Delta Lake) hoặc thiết kế landing zone lưu raw dưới dạng JSON/Avro để schema registry tự động detect, sau đó đẩy cảnh báo qua Slack thay vì làm crash Spark Job.

---

### Bài 3.9 — Multi-Layer Data Quality Gate (Bronze → Silver → Gold)

**Bối cảnh JD:** Triển khai Medallion Architecture cần kiểm soát Data Quality ở từng lớp.

**Yêu cầu:** Viết các SQL query check đại diện cho từng lớp.

**SQL Code:**
```sql
-- 1. BRONZE LAYER (Completeness Check: Không có null ở Primary Key)
SELECT COUNT(*) 
FROM bronze_invoices 
WHERE id IS NULL OR payload IS NULL;

-- 2. SILVER LAYER (Consistency Check: Ngày hóa đơn không được ở tương lai)
SELECT COUNT(*) 
FROM silver_invoices 
WHERE invoice_date > CURRENT_DATE;

-- 3. GOLD LAYER (Business Logic Check: Doanh thu = Tiền dịch vụ + Tiền phụ tùng)
SELECT id, total_amount, (service_amount + parts_amount) AS calc_total
FROM gold_fact_repair_service_revenue
WHERE total_amount != (service_amount + parts_amount);
```

---

### 🚚 Kịch bản Phỏng vấn Fleet — Nhóm 3

1. **"Làm sao bạn biết pipeline đang chạy đúng? Có hệ thống giám sát nào không?"**
   * **Trả lời:** Em xây dựng một framework Data Quality chạy sau mỗi step. Nó kiểm tra 4 khía cạnh: Freshness (dữ liệu có về đúng giờ không), Volume (số lượng records có drop bất thường không), Null-rate (tỉ lệ giá trị rỗng của các cột quan trọng), và Reconciliation (so khớp tổng doanh thu trong HDFS với DB Odoo gốc). Nếu một trong các metric vi phạm threshold, Airflow sẽ trigger hook gửi cảnh báo qua Slack kèm theo truy vấn SQL lỗi.
2. **"Nếu Debezium connector chết 2 ngày, bạn phát hiện bằng cách nào?"**
   * **Trả lời:** Em sẽ giám sát Kafka Consumer Lag thông qua Burrow hoặc Prometheus (nếu lag tăng liên tục nghĩa là đầu ra đang nghẽn). Tuy nhiên, vì Debezium là connector đọc từ Postgres, em sẽ monitor thêm metric `pg_replication_slots` ở phía Postgres. Nếu replication slot's lag size tăng vọt đột biến, hoặc connector status trong Kafka Connect REST API báo FAILED, Prometheus Alertmanager sẽ ngay lập tức ping team Data Engineer để xử lý thay vì đợi 2 ngày.
