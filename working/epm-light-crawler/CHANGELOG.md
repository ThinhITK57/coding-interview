# Changelog: EPM Crawler Modernization & Lightweight Refactor

Tài liệu ghi nhận toàn bộ các thay đổi kiến trúc, các tệp mã nguồn bị loại bỏ (pruned), các tệp được nâng cấp (upgraded), và các thành phần mới (added) khi chuyển đổi từ phiên bản `epm-crawler` (Spark-based) sang phiên bản tinh gọn chuẩn hóa (Zero-Spark, Zero-JVM).

---

## 1. Tổng quan phiên bản

- **Phiên bản trước (Legacy `epm-crawler`):**
  - Chạy trên nền Java 11 JRE + Apache Spark 3.5.3 + Py4J + Hadoop AWS bundle JARs.
  - Dung lượng Docker image: ~2.5 GB.
  - Tiêu tốn RAM tĩnh: ~2.5 GB – 4.0 GB.
  - Thời gian khởi động tiến trình: 25s – 45s (chờ JVM và SparkContext).
  - Checkpoint & Quota: Giới hạn daily request lưu in-memory (bị reset khi container restart).
  - Trách nhiệm chồng chéo: Tự tạo Spark DataFrame, chạy Window functions dedup, và tự sinh DDL Silver/Trino.

- **Phiên bản mới (Modernized Lightweight `epm-crawler`):**
  - Chạy trên nền Python 3.12 Slim thuần túy (`requests`, `tenacity`, `minio`, `sqlite3`, `prefect`).
  - Dung lượng Docker image: **< 150 MB** (giảm 94%).
  - Tiêu tốn RAM tĩnh: **< 180 MB** (tiết kiệm 93%).
  - Thời gian khởi động: **< 0.8 giây**.
  - Giữ nguyên 100% cấu trúc thư mục quen thuộc (`epm/`, `client/`, `ingestion/`, `storage/`, `monitoring/`, `reliability/`, `epm_crawler_flow.py`, `docker-compose.yml`).

---

## 2. Danh sách các file bị loại bỏ (Pruned / Removed)

Các file dưới đây thuộc về việc dư thừa tải trọng (over-engineering), vi phạm nguyên tắc phân tầng Lakehouse (Crawler chỉ làm nhiệm vụ Bronze Ingestion, không can thiệp transform/dedup/DDL của Silver):

| Tệp / Thư mục đã xóa | Lý do loại bỏ |
| :--- | :--- |
| `epm/transform/spark_session.py` | Loại bỏ việc khởi tạo SparkSession cục bộ và JVM. |
| `epm/transform/contract_conformer.py` | Schema casting bằng Spark DataFrame được chuyển về tầng Silver (dbt). |
| `epm/transform/dedup_engine.py` | Deduplication bằng `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY LastUpdatedOn DESC)` bằng Spark được chuyển sang model Silver dbt. |
| `epm/transform/json_flattener.py` | Làm phẳng JSON bằng Spark DataFrame không cần thiết; dữ liệu Bronze được lưu nguyên bản dạng JSON nén `.json.gz`. |
| `epm/transform/schema_contract.py` | Định nghĩa schema Spark kiểu StructType. |
| `epm/transform/docstring_registry.py` | Metadata packer phục vụ tài liệu hóa tự động từ Spark. |
| `epm/transform/genbi_context_packer.py` | Đóng gói context GenBI từ Spark DataFrame. |
| `epm/transform/race_condition_router.py` | Điều hướng race condition Spark DataFrame. |
| `epm/storage/spark_sql_ddl_generator.py` | Tự sinh câu lệnh `CREATE TABLE IF NOT EXISTS` Spark SQL (việc này thuộc về dbt). |
| `epm/storage/trino_ddl_generator.py` | Tự sinh câu lệnh DDL cho Trino (việc này thuộc về dbt). |
| `epm/data_type/` (*.sql) | Các mock schema SQL cũ. |
| `epm/generated_ddl/` | Các file DDL sinh ra tự động. |
| `epm/generated_schema/` | Các schema yml sinh ra tự động. |
| `epm/generated_context/` | Context json sinh ra tự động. |
| `tools/debug_read_parquet.py` | Script debug đọc parquet cục bộ. |
| `epm/data/warehouse/` | Thư mục mock parquet warehouse cục bộ. |
| Dockerfile: Khối Java 11 & Hadoop JARs | Bỏ stage `FROM eclipse-temurin:11-jre` và lệnh tải `hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`. |

---

## 3. Danh sách các file được nâng cấp & bổ sung (Upgraded & Added)

Giữ nguyên vị trí thư mục trong `epm/`, nâng cấp ruột mã nguồn thành pure Python:

### A. Root Directory
- **`epm_crawler_flow.py` (Nâng cấp):**
  - Tích hợp 11 deployment Prefect 3 (`to_deployment` và `serve()`) bám sát tài liệu `CRAWL-DATA-PLAN.md`.
  - Hỗ trợ cờ `--dry-run` để kiểm thử cấu hình không gây block.
- **`docker-compose.yml` (Nâng cấp):**
  - Giữ nguyên convention service: `prefect-server`, `epm-crawler`, `epm-adhoc`, `minio`.
  - Ánh xạ đúng các volume bền vững: `./state:/app/state`, `./data:/app/data`, `./epm/config.json:/app/epm/config.json:ro`.
- **`Dockerfile` (Nâng cấp):**
  - Chuyển sang base `prefecthq/prefect:3-python3.12`.
  - Tích hợp `docker/entrypoint.sh` hỗ trợ cả 2 chế độ: `serve` (mặc định) và `adhoc <args...>`.
- **`requirements.txt` (Nâng cấp):**
  - Loại bỏ `pyspark==3.5.3` (317MB) và `py4j==0.10.9.7`.
  - Thay bằng: `requests`, `tenacity`, `minio`, `python-dotenv`, `pydantic`, `pytest`.

### B. Thư mục `epm/client/`
- **`epm/client/auth.py` (Bổ sung/Chuẩn hóa):** Quản lý sinh header Authorization từ biến môi trường `PROJECT_API_KEY`.
- **`epm/client/resilient_client.py` (Nâng cấp):** Client HTTP chống chịu lỗi cao, tích hợp sliding delay (RPM) và chặn sớm khi cạn quota ngày qua `StateStore`.

### C. Thư mục `epm/reliability/`
- **`epm/reliability/rate_limiter.py` (Nâng cấp):**
  - Bổ sung cơ chế persistent quota: Kiểm tra và tăng số đếm request trong ngày trên SQLite (`state.db`) với transaction `BEGIN IMMEDIATE`.
  - Tự động rollover khi ngày UTC bước sang ngày mới.

### D. Thư mục `epm/ingestion/`
- **`epm/ingestion/extractor.py` (Nâng cấp):**
  - `LightweightExtractor`: Tự động tính toán cửa sổ incremental `[window_start, window_end]`, thế biến template `__WINDOW_START__`, `__WINDOW_END__`.
  - Khi chạy mode `full`, tự động bóc tách bộ lọc watermark.
  - Phân trang offset-limit, commit checkpoint từng page vào SQLite.

### E. Thư mục `epm/storage/`
- **`epm/storage/state_store.py` (Bổ sung mới):**
  - Quản lý 4 bảng SQLite: `daily_request_quota`, `checkpoints`, `manifest_pending_uploads`, `crawler_audit_runs`.
  - Hoàn toàn độc lập với MinIO, giải quyết triệt để bài toán lưu trạng thái cục bộ tốc độ cao.
- **`epm/storage/minio_sink.py` (Bổ sung mới):**
  - Nén batch thành `.json.gz` và stream trực tiếp lên MinIO Bronze path:
    `lakehouse/bronze/clarizen/{endpoint}/year=YYYY/month=MM/day=DD/{batch_id}.json.gz`
  - Tự động phát hiện MinIO mất kết nối và chuyển hướng ghi fallback vào `./data/buffer/{endpoint}/`, ghi nhận manifest `PENDING`.

### F. Thư mục `epm/backfill/`
- **`epm/backfill/backfill_worker.py` (Bổ sung mới):**
  - Triển khai mô hình commit decoupled mô phỏng Apache Iceberg:
    1. Kiểm tra MinIO có kết nối lại chưa.
    2. Đọc manifest `PENDING` theo thứ tự FIFO.
    3. Upload file buffer lên đúng intended key trên MinIO.
    4. Đối soát kích thước byte qua `stat_object`.
    5. Cập nhật manifest sang `COMMITTED` và xóa an toàn file buffer cục bộ.

### G. Thư mục `epm/monitoring/`
- **`epm/monitoring/metrics.py` (Nâng cấp):** Định nghĩa dataclass `Batch`, `CrawlRunMetrics`, `EndpointConfig`, enum `ExtractionMode`, `CrawlStatus`.
- **`epm/monitoring/audit_tracker.py` (Bổ sung mới):**
  - Ghi nhận telemetry từng lần chạy vào SQLite.
  - Xuất daily log dạng JSONL và tự động sync lên `lakehouse/audit/epm_crawler_audit_YYYY-MM-DD.jsonl`.
  - Cung cấp API `get_system_health()`.

### H. Thư mục `epm/prefect_flow.py`
- Tối giản từ 1.125 dòng Spark/DDL xuống còn ~170 dòng pure Python.
- Định nghĩa 3 flow chính:
  1. `crawl_epm_endpoint`: Crawl một endpoint cụ thể.
  2. `crawl_epm_master_dag`: Chạy tuần tự theo DAG 5 bảng.
  3. `background_backfill_flow`: Flow nền định kỳ quét đẩy bù buffer và sync audit log.
- Hỗ trợ CLI adhoc cho container `epm-adhoc` (`adhoc --endpoint tasks --mode incremental`).
