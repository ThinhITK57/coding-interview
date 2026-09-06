# KIẾN TRÚC TỔNG THỂ HỆ THỐNG DATA INGESTION & GENBI PLATFORM
*(Adaptive Enterprise ELT Framework: API -> MinIO -> Spark 2.3.2 -> Hive/Trino -> dbt -> GenBI)*

---

## 1. TƯ DUY THIẾT KẾ KHUNG THÍCH ỨNG (ADAPTIVE FRAMEWORK MINDSET)

Khi bước vào một hệ thống mới và **chưa có thông tin chi tiết về schema database hoặc cấu trúc dữ liệu chính xác**, nguyên tắc kỹ thuật cốt lõi là **không xây dựng các xử lý cứng (hard-coded logic)**. Thay vào đó, chúng ta xây dựng một **Khung Thích Ứng Cao (Adaptive Framework)** với các trụ cột sau:

1. **Metadata-Driven (Cấu hình điều khiển hành vi):**
   - Mọi thông số (Endpoint, Rate Limit, Header, Cửa sổ thời gian, Thư mục lưu trữ, Quality Rules) đều nằm ở lớp cấu hình (`config.json`, env vars, metadata tables).
   - Khi có thêm endpoint mới hoặc schema thay đổi, ta chỉ cần cập nhật cấu hình hoặc bổ sung adapter mà không phải đập đi viết lại core pipeline.

2. **Tách biệt ranh giới rõ ràng (Decoupled Deep Modules):**
   - **Tầng Thu Thập (Ingestion Layer):** Chịu trách nhiệm bảo vệ API, giữ fault-tolerance, checkpoint an toàn và đổ raw data.
   - **Tầng Xử Lý (Processing Layer - Spark 2.3.2):** Làm phẳng JSON phi cấu trúc, lọc lỗi, deduplicate, đảm bảo tính toàn vẹn phiên bản.
   - **Tầng Lưu Trữ (Tri-Storage Sinks):** Phân định rạch ròi giữa Backup bất biến (MinIO), Sandbox thử nghiệm (Trino DB1), và Warehouse chuẩn hóa (Trino DB2 qua Hive Metastore).
   - **Tầng Ngữ Nghĩa & AI (Semantic & GenBI):** Mô hình hóa dữ liệu bằng dbt để cung cấp ngữ nghĩa (Metrics, Dim, Fact, Chart Roles) cho AI LLM.

3. **Nguyên tắc "Lưu Raw Bất Biến Trước, Xử Lý Sau" (ELT True-Nature):**
   - Không bao giờ transform dữ liệu trước khi lưu trữ bản raw gốc. Khi thuật toán xử lý sai hoặc business rule thay đổi, ta luôn có thể replay 100% dữ liệu gốc từ MinIO Backup.

---

## 2. BỨC TRANH TOÀN CẢNH HỆ THỐNG (END-TO-END ARCHITECTURE)

```
                     ┌──────────────────────────────────────────────┐
                     │            REST API NGUỒN (4 Mục)            │
                     └──────────────────────┬───────────────────────┘
                                            │ (Rate Limit: RPM / RPS / Daily Quota)
                                            ▼
                     ┌──────────────────────────────────────────────┐
                     │          PREFECT HQ ORCHESTRATION            │
                     │          (Docker-compose on Server)          │
                     └──────────────────────┬───────────────────────┘
                                            │
               ┌────────────────────────────┴────────────────────────────┐
               │                                                         │
               ▼                                                         ▼
    ┌──────────────────────┐                                  ┌──────────────────────┐
    │  EXTRACTOR (DRIVER)  │ ── Checkpoint & State ─────────▶ │   CHECKPOINT STORE   │
    │  - ResilientHTTPClient│                                  │   (Local / MinIO)    │
    │  - RateLimiter (Token)│                                  └──────────────────────┘
    │  - CircuitBreaker    │
    │  - OffsetPaginator   │
    └──────────┬───────────┘
               │
               ├──────────────────────────────────────────────┐
               │ Stream Raw JSON theo batch                   │
               ▼                                              ▼
    ┌──────────────────────┐                       ┌──────────────────────┐
    │    SINK 1: MinIO     │                       │     STAGING AREA     │
    │   (RAW BACKUP BUCKET)│                       │ (Raw JSON Batch Dir) │
    │   s3a://lakehouse/   │                       └──────────┬───────────┘
    │   raw_backup/        │                                  │
    └──────────────────────┘                                  │
                                                              ▼
                                                   ┌──────────────────────┐
                                                   │   SPARK 2.3.2 CORE   │
                                                   └──────────┬───────────┘
                                                              │
               ┌──────────────────────────────────────────────┼────────────────────────────────┐
               ▼                                              ▼                                ▼
    ┌──────────────────────┐                       ┌──────────────────────┐         ┌──────────────────────┐
    │    JSON FLATTENER    │                       │   SCHEMA VALIDATOR   │         │  STATISTICS PROFILER │
    │  Recursive Struct &  │                       │  Single-Pass Concat  │         │  Null, Min/Max, Mean │
    │  Array Explode       │                       │  Tagging Engine      │         │  Distinct Counts     │
    └──────────┬───────────┘                       └──────────┬───────────┘         └──────────┬───────────┘
               │                                              │                                │
               ▼                                              ├────────────────┐               ▼
    ┌──────────────────────┐                                  │                │    ┌──────────────────────┐
    │  WINDOW DEDUPLICATOR │                                  ▼                │    │    QUALITY REPORT    │
    │  Spark 2.3.2 Idemp.  │                       ┌──────────────────────┐    │    │  (Prefect Artifact)  │
    │  Row_Number Over ()  │                       │     DLQ ROUTER       │    │    └──────────────────────┘
    └──────────┬───────────┘                       │  s3a://lakehouse/    │    │
               │                                   │  dlq/<endpoint>/     │    │
               ├─────────────────────────┐         └──────────────────────┘    │
               │                         │                                     │
               ▼                         ▼                                     │
    ┌──────────────────────┐  ┌──────────────────────┐                         │
    │    SINK 2: TRINO     │  │    SINK 3: TRINO     │ ◀───────────────────────┘
    │    (PERSONAL RAW)    │  │    (GLOBAL CLEAN)    │ (Valid records only)
    │  personal_raw.table  │  │  global_clean.table  │
    │  (Sandbox thử nghiệm)│  │  (Đồng bộ Hive Meta) │
    └──────────────────────┘  └──────────┬───────────┘
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │       DBT CORE       │
                              │ Bronze ➜ Silver ➜ Gold│
                              │ Fact / Dimension     │
                              │ Semantic Layer (Meta)│
                              └──────────┬───────────┘
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │  GENBI CONTEXT PACK  │
                              │  Manifest & Schema   │
                              │  Rich Prompt Context │
                              └──────────┬───────────┘
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │    AI / LLM ENGINE   │
                              │  (Text-to-SQL +      │
                              │   Chart Spec JSON)   │
                              └──────────┬───────────┘
                                         │
                        ┌────────────────┴────────────────┐
                        ▼                                 ▼
             [ VĂN BẢN TRẢ LỜI ]                 [ BIỂU ĐỒ TRỰC QUAN ]
             (Insights & Phân tích)              (ECharts / Chart.js Spec)
```

---

## 3. GIẢI QUYẾT 5 BÀI TOÁN KỸ THUẬT THEN CHỐT

### 3.1. Luồng Kéo API Chuẩn Xác — Bảo Vệ Connection Pool & Ổn Định Spark
- **Vấn đề:** Nếu để worker Spark (executor) tự mở connection HTTP trong `map()` hoặc `foreachPartition()`, khi cụm scale hàng chục node, số lượng request đồng thời sẽ vượt ngưỡng cho phép, làm sập connection pool hoặc dính HTTP 429 / 503.
- **Giải pháp: Driver-Bounded Extraction (Driver chỉ huy kéo dữ liệu):**
  1. **Toàn bộ việc gọi HTTP được điều phối duy nhất tại Driver** thông qua `ResilientHTTPClient` (RateLimiter Token Bucket + CircuitBreaker + Retry Jitter).
  2. Driver kéo theo từng batch (ví dụ 5,000 - 10,000 records) và stream trực tiếp ra Staging (MinIO hoặc Local disk).
  3. Khi quá trình kéo hoàn tất, Spark Session mới được kích hoạt: các Executor chỉ đọc dữ liệu từ Staging/MinIO qua I/O filesystem phân tán.
  4. **Zero HTTP Connection từ Worker**: Đảm bảo 100% an toàn connection pool, không bao giờ gây nghẽn mạng hay rò rỉ socket.

### 3.2. Kiến Trúc Lưu Trữ 3 Nơi (Tri-Storage Sinks)
Hệ thống tận dụng tính chất: Trino là Query Engine, MinIO là S3 Object Storage, và Hive cung cấp Hive Metastore (HMS):

1. **Nơi 1: MinIO Raw Backup (`s3a://lakehouse/raw_backup/<endpoint>/year=YYYY/month=MM/day=DD/*.json.gz`):**
   - Lưu trữ dữ liệu gốc dạng JSON nén `.gz`.
   - Bất biến (Immutable), append-only, dùng làm nguồn phục hồi thảm họa (Disaster Recovery) hoặc Replay khi cần re-process lịch sử.
2. **Nơi 2: Trino DB1 - Database Cá Nhân Raw (`personal_raw.<endpoint>`):**
   - Lưu trữ Parquet thô với đầy đủ các trường hệ thống và raw payload.
   - Mục đích: Dành riêng cho Data Engineer / Data Scientist query trực tiếp bằng Trino để phân tích nhanh cấu trúc, thử nghiệm thuật toán ETL mới mà không sợ ảnh hưởng đến bảng production.
3. **Nơi 3: Trino DB2 - Database Tổng Sạch (`global_clean.<endpoint>` qua Hive Metastore):**
   - Dữ liệu đã qua `JSONFlattener`, `SchemaValidator`, deduplicate theo ID, được ghi dưới định dạng Parquet Snappy phân vùng theo ngày (`ingest_date` hoặc `event_date`).
   - Đăng ký metadata với Hive Metastore (`hive.global_clean.table_name`), cho phép cả Trino và Hive truy vấn đồng thời với hiệu năng cao.

### 3.3. Xử Lý Race Condition Giữa Fact & Dim và Incremental Hẹp (Spark 2.3.2)
Trong Spark 2.3.2 không có lệnh `MERGE INTO` (tính năng của Delta Lake / Iceberg trên Spark 3.x). Chúng ta giải quyết bài toán đồng bộ bằng 3 kỹ thuật:

1. **Deduplication bằng Window Ranking:**
   Khi batch incremental chứa nhiều version của cùng một entity cập nhật trong thời gian ngắn:
   ```python
   from pyspark.sql import Window
   import pyspark.sql.functions as F

   window_spec = Window.partitionBy("entity_id").orderBy(
       F.col("updated_at").desc(),
       F.col("_ingest_timestamp").desc()
   )
   deduped_df = incoming_df.withColumn("_rk", F.row_number().over(window_spec))\
                           .filter(F.col("_rk") == 1)\
                           .drop("_rk")
   ```
2. **Idempotent Overwrite bằng Dynamic Partition Overwrite:**
   Kích hoạt cờ Spark:
   ```python
   spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
   ```
   Khi ghi `.mode("overwrite")`, Spark chỉ ghi đè đúng các partition có dữ liệu xuất hiện trong batch hiện tại, giữ nguyên toàn bộ các partition lịch sử khác.
3. **Giải Quyết Race Condition Fact - Dim (Late-Arriving Dimensions):**
   Nếu bảng Fact (ví dụ: Task) được kéo về trước bảng Dimension (ví dụ: Project) khiến khóa ngoại `project_id` chưa tồn tại:
   - Áp dụng **Inferred Dimension Member Pattern**: Tự động chèn một bản ghi tạm thời vào bảng Dimension với `project_id = missing_id`, `project_name = 'PENDING_ENRICHMENT'`, `is_inferred = true`.
   - Khi job Dimension chạy sau lấy được thông tin đầy đủ của Project đó, cơ chế Deduplication + Dynamic Overwrite ở trên sẽ tự động đè thông tin thật lên bản ghi tạm thời mà không làm đứt gãy quan hệ dữ liệu trên Trino.

### 3.4. Metadata Data Quality & Phân Loại Dead-Letter-Queue (DLQ)
Tất cả các bản ghi đi qua `SchemaValidator` đều được đánh giá đơn luồng (single-pass) bằng `F.concat_ws`:
- **Valid Stream:** Bản ghi thỏa mãn mọi rule (not null, range, in-set) ➔ Đẩy vào Trino DB2 (Global Clean).
- **DLQ Stream:** Bản ghi vi phạm ➔ Gắn thẻ lỗi chi tiết, đóng gói metadata và đẩy vào `s3a://lakehouse/dlq/<endpoint>/date=YYYY-MM-DD/`.

**Cấu trúc bản ghi DLQ chuẩn:**
```json
{
  "_batch_id": "batch_20260906_180000",
  "_endpoint": "mục_1",
  "_failed_at": "2026-09-06T18:00:15Z",
  "_error_tags": "sysid:null;percent_completed:out_of_range",
  "_error_severity": "FATAL",
  "_raw_payload": { ... }
}
```
**Kế hoạch xử lý (Remediation Plan):**
- *Lỗi Schema Drift nhẹ (Thiếu trường không bắt buộc):* Tự động điều chỉnh `DocstringRegistry` và replay lại bằng Prefect Replay Task.
- *Lỗi Dữ liệu hỏng (Corrupted ID):* Bắn cảnh báo lên Prefect Markdown Artifact để Data Steward kiểm tra. Dữ liệu trong DLQ có thể được sửa chữa và trigger pipeline đọc lại từ thư mục DLQ.

### 3.5. Multi-Plan Execution (Khả Năng Scale-Up Linh Hoạt)
Code được thiết kế với tham số `--plan`:
- **Plan A (Local / Dev):** `master = local[2]`, memory 2GB, phục vụ test nhanh tại máy dev hoặc debug container nhỏ.
- **Plan B (Standard Batch Production):** `master = yarn` hoặc `k8s`, tối ưu broadcast join cho dimension < 100MB, partition động theo ngày.
- **Plan C (Heavy Scale / Historical Backfill):** Tự động áp dụng Salting Key trên các cột phân phối lệch (skewed keys), chia partition theo giờ (`hour`) để tránh OOM.

---

## 4. DBT SEMANTIC LAYER VÀ BỘ TIẾP HỢP GENBI (AI LLM TEXT + CHARTS)

Để công cụ **GenBI (Generative BI)** sử dụng LLM có thể đọc hiểu dữ liệu từ dbt và trả về kết quả gồm cả **Chữ (Text) + Biểu đồ (Charts)**, hệ thống áp dụng kiến trúc 3 tầng:

```
[ Trino Global Clean ] ──▶ dbt Silver (Cleaned) ──▶ dbt Gold (Fact/Dim) ──▶ dbt Semantic Layer
                                                                                      │
                                                                                      ▼
                                                                           [ GenBI Context Pack ]
                                                                                      │
                                                                                      ▼
                                                                                [ AI LLM Engine ]
```

### 4.1. Kiến Trúc Mô Hình Hóa dbt
- **Tầng Bronze:** Trỏ trực tiếp vào bảng Parquet do Spark ghi ra (`stg_bronze__<endpoint>`).
- **Tầng Silver:** Chuẩn hóa kiểu dữ liệu, giải mã các status code (ví dụ: chuyển `1` thành `'In Progress'`), parse datetime UTC chuẩn ISO.
- **Tầng Gold (Marts):** Xây dựng Star Schema:
  - `fct_task_execution`: Bảng Fact ghi nhận trạng thái công việc và tiến độ.
  - `dim_project`, `dim_user`: Các bảng Dimension mô tả thực thể.

### 4.2. Rich Meta Tags Phục Vụ Trực Quan Hóa (Chart Generation)
Trong `schema.yml`, mỗi cột được gắn thẻ định hướng trực quan hóa cho LLM:
```yaml
version: 2
models:
  - name: fct_task_execution
    description: "Bảng Fact theo dõi tiến độ và hiệu suất công việc"
    meta:
      synonyms: ["công việc", "task", "tiến độ nhiệm vụ"]
      default_time_col: "report_date"

    columns:
      - name: report_date
        description: "Ngày báo cáo"
        meta:
          chart_role: "x_axis"        # Gợi ý AI: Trục hoành thời gian (Line/Bar chart)
          data_type: "date"

      - name: completion_rate
        description: "Tỷ lệ hoàn thành công việc"
        meta:
          chart_role: "y_axis"        # Gợi ý AI: Trục tung chỉ số (Aggregation: AVG)
          aggregation_type: "avg"
          synonyms: ["tiến độ", "phần trăm", "progress"]

      - name: status_name
        description: "Trạng thái công việc"
        meta:
          chart_role: "series"        # Gợi ý AI: Phân nhóm màu sắc (Legend/Color Series)
          valid_values: ["Đang làm", "Hoàn thành", "Trễ hạn"]
```

### 4.3. Định Dạng Phản Hồi Hai Chiều của LLM (Contract)
Prompt của GenBI được thiết kế để yêu cầu LLM luôn trả về cấu trúc JSON chuẩn:
```json
{
  "sql": "SELECT report_date, status_name, AVG(completion_rate) AS avg_rate FROM fct_task_execution GROUP BY 1, 2",
  "text_insights": "Trong tháng vừa qua, tiến độ trung bình đạt 82%, trong đó các công việc 'Đang làm' chiếm đa số...",
  "visualization": {
    "chart_type": "line",
    "title": "Xu hướng tiến độ theo ngày",
    "x_field": "report_date",
    "y_field": "avg_rate",
    "series_field": "status_name"
  }
}
```
Khung frontend hoặc API của GenBI chỉ cần render đoạn văn bản và truyền spec JSON vào thư viện biểu đồ (ECharts hoặc Chart.js) là người dùng lập tức có cả chữ lẫn biểu đồ trực quan.
