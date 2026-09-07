# API Ingestion & GenBI Data Platform

> **Adaptive Enterprise ELT Framework**  
> Quản lý thu thập dữ liệu REST API quy mô lớn, xử lý làm phẳng và kiểm tra chất lượng bằng Apache Spark 2.3.2, lưu trữ đa tầng (MinIO Raw Backup, Trino DB1 Sandbox, Trino DB2 Clean qua Hive Metastore), đồng bộ tầng ngữ nghĩa dbt và cung cấp tri thức cho hệ thống GenBI (AI LLM Text + Charts).

---

## 🗺️ MỤC LỤC TÌM KIẾM NHANH (QUICK SEARCH INDEX & CODE MAP)

Dưới đây là bảng chỉ đường nhanh để bạn tra cứu file, class, hàm cần sửa trong vòng **5 giây**:

| Bạn muốn làm gì? | File cần mở / chỉnh sửa | Class / Hàm quan trọng |
| :--- | :--- | :--- |
| **1. Cập nhật bảng và danh sách trường từ BA (Sáng mai)** | [`tables_registry.json`](tables_registry.json)<br>[`config.json`](config.json) | Khối `"tables"` hoặc `"endpoints"` (Chỉ cần điền trường, không cần sửa code Python). |
| **2. Tinh chỉnh Rate Limit, Quota ngày, Circuit Breaker** | [`reliability/rate_limiter.py`](reliability/rate_limiter.py)<br>[`reliability/circuit_breaker.py`](reliability/circuit_breaker.py) | `RateLimiter.acquire()`<br>`CircuitBreaker.call()` |
| **3. Sửa số lần Retry, Exponential Backoff, HTTP Error** | [`reliability/retry.py`](reliability/retry.py)<br>[`client/resilient_client.py`](client/resilient_client.py) | `RetryExecutor.execute()`<br>`ResilientHTTPClient.request()` |
| **4. Điều chỉnh Cửa sổ trượt (Time Window), Watermark, Lookback** | [`ingestion/extractor.py`](ingestion/extractor.py)<br>[`checkpoint/checkpoint_store.py`](checkpoint/checkpoint_store.py) | `Extractor._calculate_window()`<br>`CheckpointStore.get_last_watermark()` |
| **5. Phân trang API (Offset lồng trong JSON body)** | [`pagination/offset_paginator.py`](pagination/offset_paginator.py) | `OffsetPaginator.get_next_params()`<br>`OffsetPaginator._inject_into_body()` |
| **6. Đổi Catalog Trino, Schema hoặc sửa câu lệnh DDL cho trino.exe** | [`storage/trino_ddl_generator.py`](storage/trino_ddl_generator.py) | `TrinoDDLGenerator.generate_all_ddl()`<br>`export_sql_file()` |
| **7. Quản lý 3 kho đích: MinIO Backup, DB1 Sandbox, DB2 Clean** | [`storage/tri_storage_sink.py`](storage/tri_storage_sink.py) | `TriStorageSink.persist_all()`<br>`sink_minio_raw_backup()` |
| **8. Tinh chỉnh thuật toán khử trùng lặp Spark 2.3.2** | [`transform/dedup_engine.py`](transform/dedup_engine.py) | `DedupEngine.deduplicate()` (Window Ranking `row_number()`) |
| **9. Xử lý lỗi Race Condition Fact đến trước Dim** | [`transform/race_condition_router.py`](transform/race_condition_router.py) | `InferredDimensionRouter.generate_inferred_stubs()` |
| **10. Kiểm tra bản ghi bẩn bị cách ly (Dead Letter Queue)** | [`storage/dlq_router.py`](storage/dlq_router.py)<br>[`quality/validator.py`](quality/validator.py) | `DLQRouter.route_dlq()`<br>`SchemaValidator.validate()` |
| **11. Tinh chỉnh Metadata cho AI LLM & Biểu đồ GenBI** | [`transform/docstring_registry.py`](transform/docstring_registry.py)<br>[`transform/genbi_context_packer.py`](transform/genbi_context_packer.py) | `DocstringRegistry.export_dbt_schema()`<br>`GenBIContextPacker.build_context_pack()` |
| **12. Chạy test tương tác trên Apache Zeppelin (%livy.spark)** | [`notebooks/zeppelin_livy_test.py`](notebooks/zeppelin_livy_test.py) | Paragraphs 1 ➜ 10 (Copy paste vào Zeppelin) |
| **13. Triển khai Docker-compose, kết nối Prefect HQ Server** | [`docker-compose.yml`](docker-compose.yml)<br>[`Dockerfile`](Dockerfile)<br>[`.env.example`](.env.example) | Biến `PREFECT_API_URL`, `PREFECT_WORK_POOL_NAME` |
| **14. Chạy toàn bộ Pipeline từ CLI** | [`prefect_flow.py`](prefect_flow.py) | `python prefect_flow.py --endpoint muc_1 --env dev` |

👉 *Xem tài liệu chi tiết đầy đủ tại: [Bản Đồ Kiến Trúc Chi Tiết (docs/CODE_MAP.md)](docs/CODE_MAP.md)*

---

## 📌 Tổng Quan Hệ Thống

Dự án được thiết kế theo nguyên tắc **Metadata-Driven** và **Deep Module**, mang lại khả năng thích ứng cao khi hệ thống mở rộng hoặc khi schema database có sự thay đổi:

- **Orchestration:** Prefect 3 HQ (Quản lý flow, task retries, concurrency limits, artifacts reporting).
- **Ingestion Engine:** Driver-Bounded HTTP Client với Token-Bucket Rate Limiter, Exponential Backoff + Jitter, Circuit Breaker và Atomic Watermark Checkpoint.
- **Processing Engine:** Apache Spark 2.3.2 xử lý đệ quy Flattening JSON phi cấu trúc, Deduplication đa phiên bản và kiểm định chất lượng dữ liệu (Data Quality & DLQ).
- **Tri-Storage Persistence:**
  1. *MinIO:* S3-compatible raw immutable backup (`raw_backup/*.json.gz`).
  2. *Trino DB1:* Database cá nhân (`personal_raw`) dùng thử nghiệm thuật toán và phát triển ETL.
  3. *Trino DB2:* Database tổng (`global_clean`) chuẩn hóa và tối ưu truy vấn thông qua Hive Metastore.
- **dbt & GenBI Enablement:** Xây dựng mô hình Star Schema và dbt Semantic Layer (rich meta tags, chart roles, synonyms) giúp AI LLM trả về câu trả lời gồm cả văn bản phân tích và biểu đồ trực quan (ECharts).

---

## 📚 Tài Liệu Kỹ Thuật Chi Tiết

1. 🗺️ **[Bản Đồ Điều Hướng & Tra Cứu Mã Nguồn (Code Map)](docs/CODE_MAP.md):** Tra cứu theo bài toán, sơ đồ end-to-end data flow.
2. 📖 **[Kiến Trúc Tổng Thể (Architecture Blueprint)](docs/ARCHITECTURE_BLUEPRINT.md):** 5 bài toán kỹ thuật cốt lõi.
3. 📋 **[Lộ Trình và Danh Sách Tickets (Roadmap & Tickets)](docs/ROADMAP_AND_TICKETS.md):** Chi tiết 5 tickets và acceptance criteria.
4. 🧑‍🏫 **[Cẩm Nang Giải Trình Với Leader & Live Demo Thực Nghiệm](docs/LEADER_DEEP_DIVE_GUIDELINE.md):** Hướng dẫn chi tiết chạy thực nghiệm Mock Server, phân tích benchmark limit/offset và giải phẫu hành trình dữ liệu từ JSON thô đến bảng Marts trong Trino.
5. 📊 **[Phân Tích Review Code Crawler & DBT Benchmark](docs/CRAWLER_CODE_REVIEW_AND_DBT_BENCHMARK.md):** Đánh giá chuyên sâu mô hình dbt, ERD, công thức EVM chuẩn PMI và bộ câu hỏi review code.
6. 🛠️ **[Cẩm Nang Triển Khai & Test Tuần Tự Cho Máy Công Ty](docs/OFFLINE_IMPLEMENTATION_AND_TESTING_GUIDE.md):** Thứ tự gõ code và test từng module khi không có Git.

---

## 🚀 Hướng Dẫn Chạy Dự Án & Thực Nghiệm (Live Demo Guide)

Hệ thống cung cấp sẵn bộ công cụ giả lập Mock Server và Thực nghiệm toàn trình độc lập (không cần kết nối internet hay API thật), giúp bạn tự tin chạy demo và giải trình với Leader:

### ⚙️ Bước 0: Thiết lập biến môi trường chuẩn (PowerShell)
```powershell
# Chuyển vào thư mục mã nguồn
Set-Location "d:\dataguystory\coding-interview-university\working\api_ingestion"

# Khai báo môi trường Java 8 & PySpark 2.3.2 cho Conda env planview-spark37
$env:JAVA_HOME = "D:\miniconda-envs\envs\planview-spark37\Library"
$env:SPARK_HOME = "D:\miniconda-envs\envs\planview-spark37\lib\site-packages\pyspark"
$env:PYTHONIOENCODING = "utf-8"
$env:PYSPARK_PYTHON = "D:\miniconda-envs\envs\planview-spark37\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "D:\miniconda-envs\envs\planview-spark37\python.exe"
```

---

### 🖥️ Bước 1: Khởi động Mock Planview API Server (Terminal 1)
Khởi chạy mock server giả lập API Planview Clarizen AdaptiveWork với 186 trường nghiệp vụ:
```powershell
& "D:\miniconda-envs\envs\planview-spark37\python.exe" scripts/mock_epm_server.py --port 8088 --records 1500
```
- Server sinh 1.500 bản ghi Tasks (chứa đầy đủ WBS, EVM, Financials, Dates, Duplicates và DLQ errors).
- Server lắng nghe tại `http://127.0.0.1:8088/Task/query` và endpoint kiểm tra `http://127.0.0.1:8088/health`.
- Thêm cờ `--faults` nếu muốn mô phỏng lỗi gián đoạn mạng (HTTP 429/503) để kiểm tra Circuit Breaker & Retry.

---

### 📈 Bước 2: Chạy Benchmark Phân Trang (Limit / Offset) (Terminal 2)
Mở một cửa sổ PowerShell mới và chạy benchmark:
```powershell
& "D:\miniconda-envs\envs\planview-spark37\python.exe" scripts/benchmark_pagination_chunks.py http://127.0.0.1:8088
```
- Đo lường và so sánh 6 phương án phân trang (`limit = 10, 50, 100, 250, 500, 1000`).
- **Kết luận thực nghiệm:** Phương án `limit = 250` đạt hiệu năng tối ưu nhất (giảm 77% số lượng requests HTTP so với mặc định `50`, tốc độ 10.714 rec/s, kích thước gói tin an toàn ~400KB).

---

### ⚡ Bước 3: Chạy Pipeline Biến Đổi Spark 2.3.2 (Terminal 2)
Chạy quy trình làm phẳng, cách ly DLQ và khử trùng lặp Window Ranking bằng Spark 2.3.2:
```powershell
& "D:\miniconda-envs\envs\planview-spark37\python.exe" scripts/run_spark_transform_experiment.py ./storage_data/raw_backup/tasks_raw.json ./storage_data/warehouse
```
- **Làm phẳng đệ quy (`JSONFlattener`):** Tách các struct lồng nhau như `State.id` $\rightarrow$ `state_id`.
- **Cách ly DLQ (`DLQRouter`):** Tự động phát hiện và chuyển 30 bản ghi lỗi (`id = null`) vào `./storage_data/warehouse/dlq`.
- **Khử trùng lặp (`DedupEngine`):** Sử dụng `ROW_NUMBER() OVER (PARTITION BY id ORDER BY LastUpdatedOn DESC)` loại bỏ 90 bản ghi trùng lặp.
- **Lưu trữ Parquet:** Ghi 1.380 bản ghi sạch vào kho Trino DB 1 (`personal_raw`) và Trino DB 2 (`global_clean`).

---

### 📐 Bước 4: Chạy Mô Hình Hóa dbt & Tính Toán Chỉ Số EVM Chuẩn PMI (Terminal 2)
Chạy mô phỏng 3 tầng dbt (`stg_`, `int_`, `dim_`, `fct_`):
```powershell
& "D:\miniconda-envs\envs\planview-spark37\python.exe" scripts/run_dbt_simulation.py ./storage_data/warehouse/hive/global_clean/tasks ./storage_data/warehouse/marts
```
- **Tính toán EVM:** Tự động tính các chỉ số quản trị dự án quốc tế: $PV, EV, AC, CV, SV, CPI, SPI, EAC, ETC$.
- **Gắn nhãn sức khỏe (Health Tag):** Gán trạng thái `ON_TRACK`, `AT_RISK`, `CRITICAL_DELAY` dựa trên tiến độ và chi phí.
- **Xuất bản DWH:** Tạo bảng chiều `dim_tasks` và bảng sự kiện `fct_task_daily_snapshot` kèm bảng điều khiển trực quan (Dashboard Preview).

---

### 🛡️ Bước 5: Chạy Kiểm Định Tự Động Toàn Trình 6 Pha (One-Click Master Audit)
Để chạy toàn bộ chu trình kiểm thử tự động, đánh giá chất lượng mã nguồn, kiểm tra 0 bugs và sinh toàn bộ artifacts báo cáo:
```powershell
& "D:\miniconda-envs\envs\planview-spark37\python.exe" scripts/run_automated_audit_and_experiments.py
```
*Script tự động thực thi trong ~22 giây: Phase 1 (Kiểm thử 10 module) ➜ Phase 2 (Mock Server) ➜ Phase 3 (Benchmark 6 limit plans) ➜ Phase 4 (Spark 2.3.2 Transform, DLQ & Dedup) ➜ Phase 5 (dbt EVM Modeling) ➜ Phase 6 (Xuất báo cáo tổng hợp vào `experiment_results/`).*

---

### 🔄 Bước 6: Chạy Pipeline Điều Phối & Các Chế Độ Khác

#### A. Chạy kiểm tra kết nối trích xuất cơ bản (Client Test):
```powershell
& "D:\miniconda-envs\envs\planview-spark37\python.exe" main.py
```

#### B. Chạy Pipeline Điều Phối Prefect:
```powershell
# Chạy trích xuất 1 endpoint với cửa sổ tự động
python prefect_flow.py --endpoint muc_1 --env dev

# Chạy với cửa sổ thời gian chỉ định
python prefect_flow.py --endpoint muc_1 --window-start 2026-09-01T00:00:00Z --window-end 2026-09-06T00:00:00Z

# Chạy toàn bộ các endpoints
python prefect_flow.py --all --env prod
```

#### C. Chạy tương tác trên Apache Zeppelin (%livy.spark):
Mở file [`notebooks/zeppelin_livy_test.py`](notebooks/zeppelin_livy_test.py), copy lần lượt từng Paragraph từ 1 đến 10 và paste vào Apache Zeppelin để chạy thử nghiệm tương tác.

#### D. Sinh DDL và chạy trên Trino CLI (`trino.exe`):
Mỗi khi pipeline chạy hoặc khi gọi `TrinoDDLGenerator`, file DDL tương thích với catalog `hive` sẽ được tự động xuất ra thư mục `generated_ddl/<table_name>_trino_ddl.sql`. Bạn mở `trino.exe`, dán các câu lệnh SQL vào là bảng hiện lên ngay lập tức!

#### E. Khởi chạy bằng Docker-compose (Kết nối Prefect HQ từ xa):
```powershell
docker-compose up -d
```
Worker sẽ tự động kết nối với Prefect HQ server theo địa chỉ `PREFECT_API_URL` và lắng nghe các flow runs.
