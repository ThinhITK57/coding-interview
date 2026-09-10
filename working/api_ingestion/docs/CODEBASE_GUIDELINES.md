# HƯỚNG DẪN KIẾN TRÚC & MÃ NGUỒN TOÀN TRÌNH (ENTERPRISE ELT CODEBASE GUIDELINE)

> **Mục tiêu tài liệu:**  
> Bản tài liệu này đóng vai trò là **Cẩm nang Chuẩn mực (Source of Truth)** về toàn bộ kiến trúc, tổ chức thư mục, định nghĩa vai trò, hợp đồng dữ liệu (Data Contracts), nguyên lý thiết kế và chi tiết kỹ thuật của từng file mã nguồn trong dự án `api_ingestion`.  
> Phục vụ trực tiếp cho các mục đích:
> 1. **Tra cứu nhanh (Lookup):** Định vị chính xác file, hàm cần can thiệp khi có yêu cầu nghiệp vụ hoặc phát sinh sự cố.
> 2. **Nâng cấp & Mở rộng (Upgrade & Extension):** Cung cấp tiêu chuẩn để thêm endpoint mới, đổi data sink, đổi thuật toán mà không làm vỡ các ràng buộc hệ thống.
> 3. **Đánh giá & Review mã nguồn (Code Review & Quality Audit):** Thiết lập khung tiêu chí kỹ thuật chuẩn mực để đánh giá pull request, rà soát hiệu năng và tính tương thích hạ tầng.

---

## MỤC LỤC

1. [Tổng Quan Kiến Trúc & Luồng Dữ Liệu Toàn Trình](#1-tổng-quan-kiến-trúc--luồng-dữ-liệu-toàn-trình)
2. [Nguyên Tắc Thiết Kế Bất Biến (System Invariants)](#2-nguyên-tắc-thiết-kế-bất-biến-system-invariants)
3. [Bản Đồ Thư Mục & Phân Tầng Kiến Trúc](#3-bản-đồ-thư-mục--phân-tầng-kiến-trúc)
4. [Định Nghĩa Chi Tiết Từng Module & Từng File Mã Nguồn](#4-định-nghĩa-chi-tiết-từng-module--từng-file-mã-nguồn)
   - [4.1. Lớp Cấu Hình & Metadata Contract (`config/`, root JSON)](#41-lớp-cấu-hình--metadata-contract-config-root-json)
   - [4.2. Lớp Giao Tiếp Mạng & Khả Năng Chịu Lỗi (`client/`, `reliability/`)](#42-lớp-giao-tiếp-mạng--khả-năng-chịu-lỗi-client-reliability)
   - [4.3. Lớp Xác Thực & Quản Lý Lỗi (`auth/`, `exceptions/`)](#43-lớp-xác-thực--quản-lý-lỗi-auth-exceptions)
   - [4.4. Lớp Phân Trang & Thu Thập Dữ Liệu (`pagination/`, `ingestion/`)](#44-lớp-phân-trang--thu-thập-dữ-liệu-pagination-ingestion)
   - [4.5. Lớp Thủy Ấn & Điểm Kiểm Soát (`checkpoint/`)](#45-lớp-thủy-ấn--điểm-kiểm-soát-checkpoint)
   - [4.6. Lớp Biến Đổi & Chuẩn Hóa Spark 2.3.2 (`transform/`)](#46-lớp-biến-đổi--chuẩn-hóa-spark-232-transform)
   - [4.7. Lớp Quản Trị Chất Lượng & Dead Letter Queue (`quality/`)](#47-lớp-quản-trị-chất-lượng--dead-letter-queue-quality)
   - [4.8. Lớp Lưu Trữ Tri-Storage & Trino Catalog (`storage/`)](#48-lớp-lưu-trữ-tri-storage--trino-catalog-storage)
   - [4.9. Lớp Đo Lường, Báo Cáo & Giám Sát (`monitoring/`, `utils/`)](#49-lớp-đo-lường-báo-cáo--giám-sát-monitoring-utils)
   - [4.10. Lớp Điều Phối & Entry Points (`prefect_flow.py`, `main.py`)](#410-lớp-điều-phối--entry-points-prefect_flowpy-mainpy)
   - [4.11. Lớp Hạ Tầng, Scripts & Notebooks (`scripts/`, `notebooks/`, Docker)](#411-lớp-hạ-tầng-scripts--notebooks-scripts-notebooks-docker)
5. [Khung Tiêu Chí Review & Nâng Cấp Hệ Thống (Reviewer Playbook)](#5-khung-tiêu-chí-review--nâng-cấp-hệ-thống-reviewer-playbook)

---

# 1. TỔNG QUAN KIẾN TRÚC & LUỒNG DỮ LIỆU TOÀN TRÌNH

Hệ thống được thiết kế theo mô hình **Modern Data Lakehouse ELT** chuyên biệt cho việc trích xuất khối lượng lớn dữ liệu phân tán từ các hệ thống quản trị doanh nghiệp (EPM/ERP - điển hình là Clarizen/Planview) về Data Lake với độ tin cậy tuyệt đối, hỗ trợ phân tích đa tầng (Analytics, Semantic Layer, GenBI AI LLM).

### Sơ đồ luồng dữ liệu (End-to-End Data Pipeline Flow):

```
                                  [ CLARIZEN REST API ]
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │   Reliability & Client Layer    │
                           │  • RateLimiter (RPM + Daily RPD)│
                           │  • CircuitBreaker (Fail-fast)   │
                           │  • RetryExecutor (Exp Backoff)  │
                           └────────────────┬────────────────┘
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │   Ingestion & Extraction Layer  │
                           │  • Ingestion Mode (Cold/Incr)   │
                           │  • OffsetPaginator (Nested Body)│
                           │  • Staging JSON Batch Writer    │
                           └────────────────┬────────────────┘
                                            │
                     ┌──────────────────────┴──────────────────────┐
                     ▼                                             ▼
           [ Staging Raw JSON ]                         [ MinIO Raw Backup ]
                     │                                (s3a://lakehouse/raw/)
                     ▼
       ┌───────────────────────────────────────────────────────────┐
       │             Apache Spark 2.3.2 Transform Layer            │
       │  • JSONFlattener (Unnest dicts to snake_case columns)     │
       │  • DedupEngine (Window Ranking row_number() over PK)      │
       │  • InferredDimensionRouter (Reconcile Fact-Dim stubs)     │
       │  • DocstringRegistry (dbt schema.yml + Rich Meta Tags)    │
       │  • GenBIContextPacker (Schema + Sample rows for AI LLM)   │
       └────────────────────────────┬──────────────────────────────┘
                                    │
                                    ▼
       ┌───────────────────────────────────────────────────────────┐
       │                 Quality & Governance Layer                │
       │  • SchemaValidator (PK not_null, range, enum in_set)      │
       │  • Sparse Business Fields -> Profiling (Null Ratio %)     │
       │  • Invalid records (Missing PK) -> DLQ Parquet Storage    │
       └────────────────────────────┬──────────────────────────────┘
                                    │
                                    ▼
       ┌───────────────────────────────────────────────────────────┐
       │                     Tri-Storage Sink                      │
       │  ├─ Bronze Parquet: s3a://lakehouse/bronze/{source}/{ep}  │
       │  ├─ DB1 personal_raw (Sandbox Trino partition table)      │
       │  ├─ DB2 global_clean (Curated Trino analytics table)      │
       │  └─ Auto Trino DDL generation (.sql scripts for trino.exe)│
       └────────────────────────────┬──────────────────────────────┘
                                    │
                                    ▼
       ┌───────────────────────────────────────────────────────────┐
       │             Two-Phase Commit & Prefect HQ                 │
       │  • CheckpointStore.mark_completed(final_watermark)        │
       │  • Publish Daily Markdown & Table Artifacts to HQ         │
       └───────────────────────────────────────────────────────────┘
```

---

# 2. NGUYÊN TẮC THIẾT KẾ BẤT BIẾN (SYSTEM INVARIANTS)

Khi xây dựng, review hoặc nâng cấp code, các kỹ sư **bắt buộc tuân thủ 5 nguyên tắc bất biến**:

1. **Nguyên tắc "Thủy ấn Hai giai đoạn" (Two-Phase Atomic Checkpoint Invariant):**
   - Checkpoint **chỉ được phép đánh dấu hoàn thành (`completed: true`)** khi và chỉ khi toàn bộ pipeline (Trích xuất API $\rightarrow$ Ghi Staging $\rightarrow$ Spark Flatten & Dedup $\rightarrow$ Ghi Bronze/Silver $\rightarrow$ Quality Pass) hoàn tất 100%.
   - Nếu bất kỳ bước trung gian nào bị gián đoạn, checkpoint vẫn ở trạng thái chưa hoàn thành để các lần chạy sau tự động phục hồi (Resume) hoặc chạy lại toàn bộ cửa sổ đó, **đảm bảo Zero Data Loss**.
2. **Nguyên tắc "Tách biệt Dữ liệu Thưa và Dữ liệu Hỏng" (Sparse Tolerance vs. DLQ):**
   - Trong bảng thực thể doanh nghiệp (như Clarizen Projects có 364 cột, Tasks có 181 cột), đại đa số các trường nghiệp vụ là tự nhiên bị `NULL`.
   - **Chỉ cách ly vào Dead Letter Queue (DLQ)** các bản ghi vi phạm khóa định danh cốt lõi (như `SYSID is null`). Các trường nghiệp vụ rỗng phải được chấp nhận và ghi nhận vào Data Lake, đồng thời đo lường bằng tỷ lệ `null_ratio` trong báo cáo thống kê, không được làm gián đoạn dòng dữ liệu.
3. **Nguyên tắc "Mô-đun Sâu" (Deep Module Philosophy - John Ousterhout):**
   - Mọi class nghiệp vụ (`RateLimiter`, `CircuitBreaker`, `OffsetPaginator`, `DedupEngine`, `TriStorageSink`) phải tuân theo tiêu chí: **Giao diện công khai cực nhỏ (Small Interface) nhưng năng lực xử lý bên trong cực sâu (Deep Implementation)**. Người dùng chỉ cần gọi 1-2 method trực quan, mọi độ phức tạp về khóa luồng, máy trạng thái, giải thuật Spark được ẩn giấu hoàn toàn.
4. **Nguyên tắc "Tương thích ngược Spark 2.3.2":**
   - Toàn bộ code Spark phải chạy ổn định trên Spark 2.3.2 / Python 3.7.
   - **Nghiêm cấm** sử dụng các hàm Spark 3.x như `F.array_compact()`, `df.write.format("delta")` (khi chưa cấu hình driver), hay các API PySpark 3+.
   - Thay vào đó sử dụng `F.concat_ws()` cho single-pass error tagging, Window Ranking `row_number()` cho Idempotent Deduplication.
5. **Nguyên tắc "Khởi động lạnh Tự Thích Ứng" (Adaptive Cold Start):**
   - Ở lần chạy đầu tiên của một endpoint (chưa từng có watermark trong checkpoint), hệ thống phải tự động kích hoạt chế độ **Initial Baseline Load** (quét toàn bộ lịch sử từ `1970` hoặc bỏ điều kiện `where LastUpdatedOn`), tuyệt đối không lùi 24 giờ để tránh mất vĩnh viễn dữ liệu lịch sử.

---

# 3. BẢN ĐỒ THƯ MỤC & PHÂN TẦNG KIẾN TRÚC

```text
working/api_ingestion/
├── config.json                     # [Core Config] Master config khai báo API, endpoints, paging, storage
├── tables_registry.json             # [Schema Registry] Khai báo metadata bảng, phụ thuộc DAG, cột nghiệp vụ
├── prefect_flow.py                  # [Orchestrator] Prefect flow điều phối pipeline toàn trình & CLI
├── main.py                          # [CLI Entrypoint] Chạy pipeline ở chế độ độc lập (Standalone)
├── prefect.yaml                     # [Deployment] Khai báo deployment trên Prefect HQ
├── Dockerfile                       # [Infra] Đóng gói môi trường chạy Python 3.7 / Spark worker
├── docker-compose.yml               # [Infra] Dựng cụm MinIO, Trino, Prefect agent cục bộ
├── requirements.txt                 # [Dependencies] Danh mục thư viện Python
│
├── config/                          # [Module Config] Đọc, nội suy và xác thực cấu hình
│   ├── __init__.py
│   ├── api_config.py
│   ├── job_config.py
│   ├── loader.py
│   └── validator.py
│
├── client/                          # [Module HTTP Client] Giao tiếp API mạng
│   ├── __init__.py
│   ├── base_client.py
│   ├── http_client.py
│   ├── proxy.py
│   └── resilient_client.py
│
├── reliability/                     # [Module Resilience] Chống quá tải và chịu lỗi mạng
│   ├── __init__.py
│   ├── rate_limiter.py
│   ├── retry.py
│   └── circuit_breaker.py
│
├── auth/                            # [Module Authentication] Xử lý cơ chế cấp phép API
│   ├── __init__.py
│   ├── base_auth.py
│   ├── token_auth.py
│   └── session_auth.py
│
├── exceptions/                      # [Module Exceptions] Hệ thống lỗi tùy biến chuẩn hóa
│   ├── __init__.py
│   └── errors.py
│
├── pagination/                      # [Module Pagination] Các chiến lược phân trang API
│   ├── __init__.py
│   ├── base_paginator.py
│   ├── offset_paginator.py
│   ├── page_paginator.py
│   └── cursor_paginator.py
│
├── checkpoint/                      # [Module Checkpoint] Lưu trữ trạng thái và thủy ấn
│   ├── __init__.py
│   └── checkpoint_store.py
│
├── ingestion/                       # [Module Ingestion] Trích xuất dữ liệu và ghi đệm Staging
│   ├── __init__.py
│   ├── extractor.py
│   └── writer.py
│
├── transform/                       # [Module Transform] Biến đổi, khử trùng và sinh Metadata (Spark 2.3.2)
│   ├── __init__.py
│   ├── spark_session.py
│   ├── json_flattener.py
│   ├── dedup_engine.py
│   ├── race_condition_router.py
│   ├── docstring_registry.py
│   └── genbi_context_packer.py
│
├── quality/                         # [Module Quality] Kiểm soát chất lượng dữ liệu & Profiling
│   ├── __init__.py
│   ├── validator.py
│   ├── statistics.py
│   └── profiler.py
│
├── storage/                         # [Module Storage] Quản lý lưu trữ Data Lake, DLQ & Trino DDL
│   ├── __init__.py
│   ├── dlq_router.py
│   ├── tri_storage_sink.py
│   └── trino_ddl_generator.py
│
├── monitoring/                      # [Module Monitoring] Theo dõi thông số và sinh báo cáo Markdown
│   ├── __init__.py
│   ├── job_tracker.py
│   └── metrics.py
│
├── utils/                           # [Module Utils] Tiện ích hỗ trợ
│   ├── __init__.py
│   ├── env_loader.py
│   └── helpers.py
│
├── scripts/                         # [Scripts] Công cụ thực nghiệm, Benchmark & Mock Server
├── notebooks/                       # [Notebooks] Mẫu kiểm thử trên Apache Zeppelin / Livy
└── docs/                            # [Documentation] Bộ tài liệu kiến trúc & cẩm nang vận hành
```

---

# 4. ĐỊNH NGHĨA CHI TIẾT TỪNG MODULE & TỪNG FILE MÃ NGUỒN

---

## 4.1. Lớp Cấu Hình & Metadata Contract (`config/`, root JSON)

### 1. `config.json`
- **Đường dẫn:** `working/api_ingestion/config.json`
- **Vai trò:** Bản khai báo cấu hình toàn thể (Declarative Master Configuration) cho toàn bộ hệ thống.
- **Nội dung chính:**
  - Khối `"api"`: Tên nguồn (`clarizen`), version (`v2.0`), `base_url`, danh sách các `endpoints` (`tasks`, `projects`, `bsc`, `assignments`, `targets`).
  - Cấu hình từng endpoint: headers, phương thức (`POST`), cấu hình auth (`api_key`), timeout (30s), rate limit (`requests_per_minute: 60`, `requests_per_day: 10000`), retry policy (5 lần, backoff factor 2), pagination strategy (`offset`, `location: body`, `page_size: 50`), và cấu trúc body payload với danh sách 100+ đến 300+ trường kèm khối `"where"` lọc theo `LastUpdatedOn`.
  - Khối `"job"`: Extraction mode (`incremental`), batch size, max records, max runtime, watermark field, checkpoint path, storage layer (`bronze`, định dạng `parquet`, partition `ingest_date`).
- **Lưu ý khi bảo trì:** Khi bổ sung trường mới do BA yêu cầu, chỉ cần thêm tên trường vào mảng `"fields"` của endpoint tương ứng trong file này; không cần sửa đổi mã nguồn Python.

### 2. `tables_registry.json`
- **Đường dẫn:** `working/api_ingestion/tables_registry.json`
- **Vai trò:** Bản đăng ký siêu dữ liệu ngữ nghĩa (Semantic Metadata Registry) của Data Lakehouse.
- **Nội dung chính:**
  - Khai báo danh mục bảng nghiệp vụ, mô tả bảng, danh sách phụ thuộc (`depends_on`) phục vụ tính toán thứ tự chạy DAG (Topological Sort Waves).
  - Khai báo chi tiết thuộc tính từng cột: `source_name`, `target_name`, `data_type`, `description`, `chart_role` (dimension, metric, timestamp), `synonyms` (các từ đồng nghĩa để AI LLM hiểu trong truy vấn GenBI).
- **Lưu ý:** Đây là cầu nối giúp hệ thống tự động sinh file `schema.yml` cho dbt và tạo `genbi_context_pack.json` cho LLM.

### 3. `config/api_config.py`
- **Đường dẫn:** `working/api_ingestion/config/api_config.py`
- **Vai trò:** Định nghĩa các Dataclass biểu diễn mô hình dữ liệu cấu hình API.
- **Các Class chính:**
  - `HttpMethod(Enum)`: `GET`, `POST`, `PUT`, `DELETE`.
  - `PaginationType(Enum)`: `NONE`, `PAGE`, `OFFSET`, `CURSOR`, `LINK`.
  - `AuthConfig`: `type`, `credentials: Dict[str, str]`.
  - `RateLimitConfig`: `requests_per_minute`, `requests_per_day`.
  - `RetryConfig`: `max_attempts`, `backoff_factor`, `max_backoff_seconds`, `retry_status_codes`.
  - `PaginationConfig`: `type`, `location` ("query" hoặc "body"), `page_size`, `max_pages`, `offset_param`, `limit_param`, v.v.
  - `EndpointConfig`: Đóng gói trọn vẹn thông tin 1 endpoint REST API.
  - `APIConfig`: Đóng gói cấu hình chung của API và danh sách `endpoints: List[EndpointConfig]`.
- **Nguyên tắc:** Sử dụng dataclass thuần túy (POPO) để đảm bảo tính tường minh về kiểu dữ liệu (Type Safety).

### 4. `config/job_config.py`
- **Đường dẫn:** `working/api_ingestion/config/job_config.py`
- **Vai trò:** Định nghĩa các Dataclass biểu diễn mô hình thực thi Job Ingestion.
- **Các Class chính:**
  - `ExtractionMode(Enum)`: `FULL`, `INCREMENTAL`, `INITIAL`, `BACKFILL`.
  - `IncrementalConfig`: `watermark_field`, `lookback_minutes`, `datetime_format`, `initial_start_time`.
  - `ExtractionConfig`: `mode`, `batch_size`, `max_records`, `max_pages`, `max_runtime_seconds`, `window_start`, `window_end`, `incremental`.
  - `CheckpointConfig`: `enabled`, `store_type`, `path`.
  - `StorageConfig`: `format`, `layer`, `path`, `partition_columns`, `compression`.
  - `JobConfig`: Đóng gói cấu hình Job hoàn chỉnh.

### 5. `config/loader.py`
- **Đường dẫn:** `working/api_ingestion/config/loader.py`
- **Vai trò:** Đọc file JSON cấu hình và chuyển đổi thành đối tượng `IngestionConfig`.
- **Hàm cốt lõi:**
  - `load(config_path: str) -> IngestionConfig`: Mở file JSON, thực hiện duyệt đệ quy qua `_substitute_env_vars()` để tự động thay thế các placeholder dạng `${VAR_NAME}` bằng giá trị thực từ biến môi trường (ví dụ: `${PROJECT_API_KEY}`, `${MINIO_ENDPOINT}`).
- **Thiết kế:** Xử lý lỗi `FileNotFoundError` và `json.JSONDecodeError` thành thông báo thân thiện.

### 6. `config/validator.py`
- **Đường dẫn:** `working/api_ingestion/config/validator.py`
- **Vai trò:** Thẩm định tính hợp lệ của cấu hình (Pre-flight Validation) trước khi khởi chạy job.
- **Hàm cốt lõi:**
  - `validate(config: IngestionConfig) -> IngestionConfig`: Kiểm tra tính toàn vẹn của API URL, auth credentials, retry codes, rate limit, pagination parameters.
  - Ném ra `ValueError` chi tiết nếu phát hiện cấu hình bất hợp lý (ví dụ: page size <= 0, retry attempts < 1).

---

## 4.2. Lớp Giao Tiếp Mạng & Khả Năng Chịu Lỗi (`client/`, `reliability/`)

### 7. `client/base_client.py`
- **Đường dẫn:** `working/api_ingestion/client/base_client.py`
- **Vai trò:** Abstract Base Class quy định hợp đồng giao tiếp cho mọi HTTP Client.
- **Phương thức:** `@abstractmethod request(method, url, headers, params, json_body, timeout, ssl_context) -> Dict[str, Any]`.

### 8. `client/http_client.py`
- **Đường dẫn:** `working/api_ingestion/client/http_client.py`
- **Vai trò:** Client HTTP tầng trệt sử dụng thư viện chuẩn `urllib` của Python (không phụ thuộc external library nặng nề).
- **Đặc điểm:** Tự động parse JSON response, giải mã headers, ném ngoại lệ chuẩn hóa `APIHTTPError` khi mã lỗi HTTP $\ge 400$, ném `APINetworkError` khi đứt kết nối vật lý hoặc timeout.

### 9. `reliability/rate_limiter.py`
- **Đường dẫn:** `working/api_ingestion/reliability/rate_limiter.py`
- **Vai trò:** Bộ kiểm soát tốc độ gọi API hai tầng (Dual-layer Rate Limiter) chống vi phạm chính sách API của nhà cung cấp.
- **Giải thuật:**
  - **Tầng 1 (Per-minute):** Thuật toán **Token Bucket**. Token được bơm đều đặn theo giây (`refill_rate = rpm / 60.0`). Nếu hết token, hàm `acquire()` sẽ tính toán chính xác số giây cần ngủ và gọi `time.sleep()`.
  - **Tầng 2 (Per-day):** Bộ đếm trượt theo ngày (`daily_limit`). Tự động phát hiện qua mốc nửa đêm (Midnight) để reset bộ đếm. Nếu vượt ngưỡng ngày, lập tức ném ngoại lệ `RateLimitExceededError`.
- **An toàn đa luồng:** Sử dụng `threading.Lock` để đồng bộ truy cập token, nhưng thực hiện `time.sleep()` **bên ngoài Lock** để không chặn các worker khác.

### 10. `reliability/retry.py`
- **Đường dẫn:** `working/api_ingestion/reliability/retry.py`
- **Vai trò:** Bộ thực thi cơ chế gọi lại khi gặp lỗi tạm thời (Transient Faults).
- **Giải thuật:**
  - **Exponential Backoff with Full Jitter:** Độ trễ cơ sở tăng theo cấp số nhân ($2^{\text{attempt}-1} \times \text{backoff\_factor}$), kết hợp ngẫu nhiên Full Jitter (`random.uniform(0, capped_delay)`) để tránh hiện tượng sụp đổ đồng loạt (Thundering Herd Problem).
  - **Tôn trọng Header `Retry-After`:** Tự động trích xuất thời gian chờ từ header response của máy chủ khi bị mã lỗi 429.
  - Bộ mã lỗi cho phép retry mặc định: `[408, 429, 500, 502, 503, 504]`. Các lỗi 400, 401, 403, 404 sẽ fail ngay lập tức (fail-fast).

### 11. `reliability/circuit_breaker.py`
- **Đường dẫn:** `working/api_ingestion/reliability/circuit_breaker.py`
- **Vai trò:** Cầu dao ngắt mạch (Circuit Breaker Pattern) ngăn chặn sập dây chuyền khi API phía đối tác bị sập.
- **Máy trạng thái 3 pha:**
  - `CLOSED`: Hoạt động bình thường. Nếu số lỗi liên tiếp $\ge \text{failure\_threshold}$ (mặc định 5), chuyển sang `OPEN`.
  - `OPEN`: Từ chối ngay lập tức mọi request mới bằng ngoại lệ `CircuitBreakerOpenError`, không gửi request ra mạng. Chờ hết thời gian phục hồi (`recovery_timeout`, ví dụ 60s), chuyển sang `HALF_OPEN`.
  - `HALF_OPEN`: Thử cho phép 1 request đi qua. Nếu thành công $\rightarrow$ Reset lỗi về 0 và chuyển về `CLOSED`. Nếu thất bại $\rightarrow$ Quay lại `OPEN`.

### 12. `client/resilient_client.py`
- **Đường dẫn:** `working/api_ingestion/client/resilient_client.py`
- **Vai trò:** HTTP Client tối cao tích hợp trọn gói toàn bộ ngăn xếp ổn định (Resilience Stack).
- **Thứ tự thực thi cho mỗi Request:**
  1. `RateLimiter.acquire()`: Đảm bảo không vượt quá RPM và RPD.
  2. `CircuitBreaker.call()`: Kiểm tra trạng thái cầu dao.
  3. `RetryExecutor.execute()`: Bọc request trong cơ chế retry exponential backoff.
  4. `HTTPClient.request()`: Thực thi kết nối qua socket mạng.
  5. Ghi log có cấu trúc JSON (đo lường chính xác mili-giây, mã HTTP, token còn lại).
- **Hàm tiện ích:** `get_stats()` trả về tổng số request, retry, lỗi phục vụ dashboard giám sát.

---

## 4.3. Lớp Xác Thực & Quản Lý Lỗi (`auth/`, `exceptions/`)

### 13. `auth/base_auth.py`
- **Đường dẫn:** `working/api_ingestion/auth/base_auth.py`
- **Vai trò:** Interface trừu tượng xác thực API (`get_headers() -> Dict[str, str]`).

### 14. `auth/token_auth.py`
- **Đường dẫn:** `working/api_ingestion/auth/token_auth.py`
- **Vai trò:** Xử lý xác thực qua Token/API Key (ví dụ: `Authorization: ApiKey <KEY>` hoặc Bearer token). Đọc an toàn từ biến môi trường, không lưu cứng bí mật.

### 15. `auth/session_auth.py`
- **Đường dẫn:** `working/api_ingestion/auth/session_auth.py`
- **Vai trò:** Xử lý xác thực dạng Session Cookie hoặc login động lấy token định kỳ.

### 16. `exceptions/errors.py`
- **Đường dẫn:** `working/api_ingestion/exceptions/errors.py`
- **Vai trò:** Định nghĩa cây phả hệ ngoại lệ (Custom Exception Hierarchy) chuẩn hóa cho toàn pipeline:
  - `APIError` (Gốc)
    - `APIHTTPError`: Lỗi HTTP kèm `status_code` và `response_body`.
    - `APINetworkError`: Lỗi đứt cáp, DNS, socket timeout.
    - `RateLimitExceededError`: Vượt quota ngày.
    - `CircuitBreakerOpenError`: Cầu dao đang mở.
    - `PaginationError`: Lỗi lặp vô tận hoặc sai định dạng trang.
    - `CheckpointError`: Lỗi đọc/ghi file checkpoint.

---

## 4.4. Lớp Phân Trang & Thu Thập Dữ Liệu (`pagination/`, `ingestion/`)

### 17. `pagination/base_paginator.py`
- **Đường dẫn:** `working/api_ingestion/pagination/base_paginator.py`
- **Vai trò:** Hợp đồng trừu tượng cho mọi thuật toán phân trang.
- **Phương thức chính:**
  - `get_next_params(base_params, base_body) -> (params, body)`
  - `extract_records(response_body) -> List[dict]`
  - `update_state(response_body, records_count)`
  - `has_more() -> bool`
  - `get_state() / restore_state(state)`

### 18. `pagination/offset_paginator.py`
- **Đường dẫn:** `working/api_ingestion/pagination/offset_paginator.py`
- **Vai trò:** Bộ phân trang chuyên biệt cho API Clarizen và các API phân trang theo Offset/Limit.
- **Tính năng đặc biệt:**
  - **Tự động dò tìm cấu trúc lồng nhau (Nested Body Injection):** Tự động phát hiện cấu trúc `body["paging"]["from"]` và `body["paging"]["limit"]` trong JSON body để cập nhật offset mà không làm thay đổi các trường dữ liệu khác.
  - **Cơ chế dừng an toàn (Dual Termination):** Dừng phân trang khi số bản ghi trả về nhỏ hơn `page_size` (Partial Page) hoặc khi mảng bản ghi rỗng (Empty Page).

### 19. `pagination/page_paginator.py` & `cursor_paginator.py`
- **Đường dẫn:** `working/api_ingestion/pagination/page_paginator.py`, `cursor_paginator.py`
- **Vai trò:** Bộ phân trang theo số trang (`page=1, 2, 3`) và theo con trỏ (`cursor=dXNlcjEw`).

### 20. `ingestion/extractor.py`
- **Đường dẫn:** `working/api_ingestion/ingestion/extractor.py`
- **Vai trò:** Cỗ máy điều phối trích xuất dữ liệu API trung tâm.
- **Trách nhiệm cốt lõi:**
  - **Tính toán Cửa sổ trượt (`_calculate_window`):** Xác định `[window_start, window_end)` dựa trên Checkpoint Watermark và lookback buffer.
  - **Nội suy tham số (`_interpolate_params`):** Thay thế giá trị thời gian vào các điều kiện lọc `where` của Clarizen (`GreaterThanOrEqual`, `LessThan`).
  - **Vòng lặp kéo dữ liệu (`iterate_batches`):**
    - Khởi tạo batch, gọi client mạng, trích xuất bản ghi.
    - Cập nhật tiến độ `max_watermark_seen` từ trường thời gian của từng bản ghi (`LastUpdatedOn`).
    - Ghi checkpoint tạm thời (chưa đánh dấu `completed`).
    - Sinh (`yield`) đối tượng `Batch` chứa dữ liệu thô cho tầng tiếp theo.

### 21. `ingestion/writer.py`
- **Đường dẫn:** `working/api_ingestion/ingestion/writer.py`
- **Vai trò:**
  - `BatchWriter`: Ghi các lô bản ghi JSON thô ra thư mục tạm Staging (`./data/staging/.../{batch_id}_p{page}.json`).
  - `PartitionedParquetWriter`: Ghi DataFrame Spark ra định dạng Parquet nén Snappy, phân vùng theo ngày (`_ingest_date=YYYY-MM-DD`).

---

## 4.5. Lớp Thủy Ấn & Điểm Kiểm Soát (`checkpoint/`)

### 22. `checkpoint/checkpoint_store.py`
- **Đường dẫn:** `working/api_ingestion/checkpoint/checkpoint_store.py`
- **Vai trò:** Quản lý điểm phục hồi và thủy ấn thời gian (Watermark) của từng endpoint.
- **Nguyên lý An toàn Tai nạn (Crash-Safe Atomic Write):**
  - Khi lưu checkpoint, dữ liệu được ghi vào file tạm `.tmp` kèm lệnh `os.fsync()`, sau đó mới thực hiện thao tác đổi tên nguyên tử (`os.replace()`). Đảm bảo nếu server mất điện giữa lúc ghi, file checkpoint cũ vẫn nguyên vẹn 100%, không bao giờ bị hỏng file (Corrupted JSON).
- **Phương thức chính:**
  - `load(skip_completed=True)`: Tải trạng thái để khôi phục sau crash.
  - `get_last_watermark()`: Lấy mốc thời gian cập nhật lớn nhất đã thành công của lần chạy trước.
  - `commit(state)`: Lưu trạng thái tạm thời.
  - `mark_completed(final_watermark, ...)`: Đánh dấu lần chạy thành công hoàn toàn.

---

## 4.6. Lớp Biến Đổi & Chuẩn Hóa Spark 2.3.2 (`transform/`)

### 23. `transform/spark_session.py`
- **Đường dẫn:** `working/api_ingestion/transform/spark_session.py`
- **Vai trò:** Factory khởi tạo `SparkSession` tối ưu hóa cho môi trường MinIO S3A và Hive Metastore.
- **Cấu hình nhúng:**
  - Cấu hình driver S3A: `org.apache.hadoop.fs.s3a.S3AFileSystem`.
  - Tự động nhận diện biến môi trường `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`.
  - Cấu hình `fs.s3a.path.style.access = true`, tắt SSL cho môi trường test nội bộ nếu cần.

### 24. `transform/json_flattener.py`
- **Đường dẫn:** `working/api_ingestion/transform/json_flattener.py`
- **Vai trò:** Duỗi phẳng cấu trúc JSON lồng nhau (Struct/Nested Objects) thành các cột đơn cấp phẳng theo định dạng `snake_case`.
- **Đặc điểm:** Tự động phân tách các trường dạng `Manager.SYSID`, `Manager.Name` thành `manager_sysid`, `manager_name`. Tương thích tuyệt đối với PySpark 2.3.2.

### 25. `transform/dedup_engine.py`
- **Đường dẫn:** `working/api_ingestion/transform/dedup_engine.py`
- **Vai trò:** Cỗ máy khử trùng lặp dữ liệu mang tính lũy đọng (Idempotent Deduplication).
- **Thuật toán:**
  - Sử dụng Spark Window Ranking:  
    `ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {watermark_col} DESC)`
  - Chỉ giữ lại bản ghi có thứ tự xếp hạng bằng 1. Đảm bảo nếu một bản ghi bị kéo lặp lại nhiều lần do lookback buffer, bản ghi có mốc cập nhật mới nhất sẽ luôn được giữ lại.

### 26. `transform/race_condition_router.py`
- **Đường dẫn:** `working/api_ingestion/transform/race_condition_router.py`
- **Vai trò:** Giải quyết bài toán kinh điển trong Data Warehouse: **Fact đến trước Dimension** (Race Condition).
- **Giải thuật Kimball Inferred Dimension:**
  - Nếu bảng Task (Fact) chứa khóa ngoại `c_project` trỏ tới một Project chưa từng xuất hiện trong bảng Dimension Project:
  - Hệ thống tự động trích xuất các khóa này, tạo thành các bản ghi giả định tạm thời (**Inferred Stub Records**) với trạng thái `is_inferred = True`.
  - Nạp các bản ghi này vào bảng Dimension để giữ toàn vẹn tham chiếu (Referential Integrity), sau đó tự động dung hòa (Reconcile) khi dữ liệu Project thực tế cập bến.

### 27. `transform/docstring_registry.py`
- **Đường dẫn:** `working/api_ingestion/transform/docstring_registry.py`
- **Vai trò:** Đăng ký tài liệu cột và xuất khẩu file `schema.yml` cho dbt.
- **Tính năng:** Tự động gắn Rich Meta Tags (`chart_role`, `format`, `synonyms`) cho từng cột, giúp dbt docs và Data Catalog hiển thị đầy đủ ngữ nghĩa nghiệp vụ.

### 28. `transform/genbi_context_packer.py`
- **Đường dẫn:** `working/api_ingestion/transform/genbi_context_packer.py`
- **Vai trò:** Đóng gói bối cảnh tri thức dữ liệu cho AI LLM (Text-to-SQL / GenBI Context Pack).
- **Đầu ra:** File `genbi_context_pack_{endpoint}.json` bao gồm:
  - Tên bảng Trino tương ứng (`hive.global_clean.{table}`).
  - Danh sách cột, kiểu dữ liệu, mô tả và vai trò biểu đồ.
  - 3 dòng dữ liệu mẫu thực tế (`sample_rows`) để LLM tham chiếu cấu trúc giá trị khi sinh câu lệnh SQL.

---

## 4.7. Lớp Quản Trị Chất Lượng & Dead Letter Queue (`quality/`)

### 29. `quality/validator.py`
- **Đường dẫn:** `working/api_ingestion/quality/validator.py`
- **Vai trò:** Cỗ máy kiểm định quy tắc dữ liệu và phân tách dòng dữ liệu sạch / dữ liệu hỏng.
- **Kỹ thuật Single-Pass Error Tagging (Spark 2.3.2):**
  - Xây dựng mảng biểu thức lỗi, sử dụng `F.concat_ws(";", *error_expressions)` để gắn nhãn lỗi vào cột `_error_tags`.
  - Phân tách DataFrame thành 2 luồng chỉ trong một lần quét (Single Pass):
    - `valid_df`: Những dòng có độ dài `_error_tags` bằng 0.
    - `dlq_df`: Những dòng có lỗi (đưa vào DLQ).
- **Quy tắc:** Chỉ kiểm tra `not_null` đối với Khóa chính định danh (`SYSID`, `Name`); không ép các trường nghiệp vụ thưa.

### 30. `quality/statistics.py`
- **Đường dẫn:** `working/api_ingestion/quality/statistics.py`
- **Vai trò:** Tính toán các chỉ số thống kê phân phối của từng cột trên Spark DataFrame:
  - `null_ratio`: Tỷ lệ phần trăm giá trị rỗng của cột.
  - `distinct_count`: Số lượng giá trị duy nhất.
  - `min`, `max`, `mean`: Giá trị biên và trung bình.

### 31. `quality/profiler.py`
- **Đường dẫn:** `working/api_ingestion/quality/profiler.py`
- **Vai trò:** Đóng gói kết quả kiểm định chất lượng thành đối tượng `QualityReport`. Cung cấp phương thức `log_summary()` in bảng tóm tắt chất lượng ra log console.

---

## 4.8. Lớp Lưu Trữ Tri-Storage & Trino Catalog (`storage/`)

### 32. `storage/tri_storage_sink.py`
- **Đường dẫn:** `working/api_ingestion/storage/tri_storage_sink.py`
- **Vai trò:** Quản lý chiến lược lưu trữ 3 tầng (Tri-Storage Architecture):
  1. **Tầng MinIO Raw Backup (`sink_minio_raw_backup`):** Lưu trữ bản sao JSON gốc không nén trên MinIO làm kho lưu trữ bảo hiểm (Disaster Recovery).
  2. **Tầng Trino Sandbox DB1 (`sink_trino_personal_raw`):** Lưu trữ dữ liệu thô phục vụ Data Engineer thử nghiệm và đối soát.
  3. **Tầng Trino Production DB2 (`sink_trino_global_clean`):** Lưu trữ dữ liệu đã làm sạch, khử trùng lặp và đạt chuẩn chất lượng phục vụ toàn doanh nghiệp.

### 33. `storage/trino_ddl_generator.py`
- **Đường dẫn:** `working/api_ingestion/storage/trino_ddl_generator.py`
- **Vai trò:** Tự động chuyển đổi Spark Schema thành các câu lệnh SQL DDL (`CREATE TABLE IF NOT EXISTS`) chuẩn cú pháp Trino Hive Connector.
- **Đầu ra:** File `.sql` trong thư mục `./generated_ddl/` sẵn sàng để CLI `trino.exe` thực thi tạo bảng phân vùng bên ngoài (External Partitioned Table).

### 34. `storage/dlq_router.py`
- **Đường dẫn:** `working/api_ingestion/storage/dlq_router.py`
- **Vai trò:** Định tuyến và lưu trữ các bản ghi bị lỗi vào Dead Letter Queue (`./data/warehouse/dlq/{table_name}`).
- **Tính năng:** Phân loại thống kê số lượng lỗi theo từng nguyên nhân và lưu vết chi tiết phục vụ Data Stewards kiểm tra nguyên nhân gốc.

---

## 4.9. Lớp Đo Lường, Báo Cáo & Giám Sát (`monitoring/`, `utils/`)

### 35. `monitoring/job_tracker.py`
- **Đường dẫn:** `working/api_ingestion/monitoring/job_tracker.py`
- **Vai trò:** Đo đạc hiệu năng toàn diện của phiên chạy (Throughput records/giây, thời gian từng chặng).
- **Hàm cốt lõi:** `to_daily_report_markdown(summary) -> str`: Sinh báo cáo Markdown tổng kết toàn trình chuyên nghiệp hiển thị trực quan số trang, số bản ghi, thống kê client HTTP, và chất lượng DLQ.

### 36. `monitoring/metrics.py`
- **Đường dẫn:** `working/api_ingestion/monitoring/metrics.py`
- **Vai trò:** Thu thập số liệu metrics (Counters, Gauges, Timers) sẵn sàng đẩy sang Prometheus / Grafana.

### 37. `utils/env_loader.py` & `helpers.py`
- **Đường dẫn:** `working/api_ingestion/utils/env_loader.py`, `helpers.py`
- **Vai trò:** Tiện ích nạp biến môi trường từ file `.env` cục bộ và các hàm trợ giúp xử lý chuỗi, định dạng ngày tháng.

---

## 4.10. Lớp Điều Phối & Entry Points (`prefect_flow.py`, `main.py`)

### 38. `prefect_flow.py`
- **Đường dẫn:** `working/api_ingestion/prefect_flow.py`
- **Vai trò:** **Trái tim điều phối luồng (Master Orchestration Flow)** của toàn bộ nền tảng.
- **Các Task Prefect chính:**
  - `@task load_and_validate_config`: Nạp và xác thực cấu hình.
  - `@task extract_from_api`: Quản lý concurrency slot, trích xuất API, ghi đệm Staging và sao lưu MinIO Raw.
  - `@task transform_with_spark`: Khởi tạo Spark, flatten JSON, dedup, bù Fact-Dim stub, xuất dbt schema, đóng gói GenBI context pack, ghi Bronze Parquet và lưu Tri-Storage.
  - `@task run_quality_checks`: Chạy SchemaValidator, cô lập DLQ, đo lường StatisticsProfiler.
  - `@task publish_daily_report`: Đẩy Markdown Artifact và Table Artifact lên Prefect HQ Dashboard.
- **Thuật toán giải phụ thuộc:** `resolve_dag_execution_waves()` sử dụng thuật toán Kahn để phân rã các bảng liên kết thành các làn chạy song song độc lập (Execution Waves).
- **Hỗ trợ Standalone Fallback:** Tự động phát hiện nếu môi trường thiếu Prefect hoặc chạy cờ `--standalone` để chuyển sang chế độ hàm Python thông thường không bị crash.

### 39. `main.py`
- **Đường dẫn:** `working/api_ingestion/main.py`
- **Vai trò:** Điểm kích hoạt dòng lệnh (CLI Entry Point) phục vụ việc chạy thử cục bộ hoặc chạy qua Crontab OS truyền thống.

### 40. `prefect.yaml`
- **Đường dẫn:** `working/api_ingestion/prefect.yaml`
- **Vai trò:** Khai báo cấu hình triển khai Prefect Deployments (cho 5 endpoints: `tasks`, `projects`, `bsc`, `assignments`, `targets`) gắn với Work Pool và lịch Cron chạy định kỳ.

---

## 4.11. Lớp Hạ Tầng, Scripts & Notebooks (`scripts/`, `notebooks/`, Docker)

### 41. `Dockerfile` & `docker-compose.yml`
- **Đường dẫn:** `working/api_ingestion/Dockerfile`, `docker-compose.yml`
- **Vai trò:** Đóng gói image Docker cho Prefect Worker và dựng cụm dịch vụ MinIO Object Storage cục bộ (port 9000/9001).

### 42. `scripts/`
- `mock_epm_server.py`: Giả lập máy chủ API Clarizen trên cổng `8088` phục vụ kiểm thử cô lập Offline.
- `mock_data_generator.py`: Sinh dữ liệu mẫu chân thực với 186 trường thuộc tính EPM.
- `benchmark_pagination_chunks.py`: Kịch bản đo lường hiệu năng các mức page size (từ 10 đến 1000 bản ghi/trang).
- `run_spark_transform_experiment.py`: Chạy thử nghiệm độc lập toàn bộ các bước biến đổi Spark.
- `run_dbt_simulation.py`: Mô phỏng tính toán các chỉ số quản lý dự án (EVM: PV, EV, AC, CPI, SPI) theo chuẩn dbt.
- `run_automated_audit_and_experiments.py`: Kịch bản tổng kiểm tra tự động toàn diện 6 pha của hệ thống.

### 43. `notebooks/zeppelin_livy_test.py`
- **Đường dẫn:** `working/api_ingestion/notebooks/zeppelin_livy_test.py`
- **Vai trò:** Gồm 10 Paragraphs mã nguồn PySpark thiết kế riêng cho Apache Zeppelin thông qua `%livy.spark` interpreter, phục vụ Data Scientist / Data Analyst kiểm tra dữ liệu tương tác.

---

# 5. KHUNG TIÊU CHÍ REVIEW & NÂNG CẤP HỆ THỐNG (REVIEWER PLAYBOOK)

Khi tiến hành Review Pull Request hoặc nâng cấp tính năng, người phụ trách kỹ thuật cần đối chiếu theo bảng kiểm định sau:

| Hạng mục kiểm tra | Tiêu chí chuẩn mực (Pass Criteria) | File phụ trách |
| :--- | :--- | :--- |
| **1. Thêm Endpoint mới** | Chỉ thêm vào `config.json` và `tables_registry.json`. Tuyệt đối không hardcode endpoint name trong code Python. | `config.json`<br>`tables_registry.json` |
| **2. Cold Start & Watermark** | Endpoint mới phải tự nhận diện Initial Load hoặc quét từ 1970. Không được để lùi 24h làm mất dữ liệu lịch sử. | `ingestion/extractor.py` |
| **3. Điểm cam kết Checkpoint** | Lệnh `mark_completed()` chỉ được nằm ở cuối flow sau khi Spark & Quality thành công. Không commit sớm trong Extractor. | `ingestion/extractor.py`<br>`prefect_flow.py` |
| **4. Kiểm soát DLQ & Null** | Chỉ cách ly vào DLQ khi thiếu Primary Key (`SYSID`). Các cột nghiệp vụ NULL phải được chấp nhận và ghi nhận tỷ lệ qua Profiler. | `quality/validator.py`<br>`prefect_flow.py` |
| **5. Tương thích Spark 2.3.2** | Không dùng API Spark 3.x. Khử trùng lặp phải dùng Window Ranking `row_number()`. Ghép nhãn lỗi dùng `F.concat_ws`. | `transform/dedup_engine.py`<br>`quality/validator.py` |
| **6. Khả năng chịu lỗi Mạng** | Mọi request API phải đi qua `ResilientHTTPClient` (Token bucket RPM, Daily cap, Retry jitter, Circuit breaker). | `client/resilient_client.py`<br>`reliability/` |
| **7. An toàn Tai nạn (Crash Safety)**| Ghi file Checkpoint và Parquet phải tuân theo cơ chế ghi file tạm `.tmp` rồi đổi tên nguyên tử (`atomic rename`). | `checkpoint/checkpoint_store.py`<br>`ingestion/writer.py` |
| **8. Đồng bộ Metadata AI GenBI**| Khi thay đổi cấu trúc bảng, phải xuất lại `schema.yml` cho dbt và `genbi_context_pack.json` cho LLM. | `transform/docstring_registry.py`<br>`transform/genbi_context_packer.py` |

---
*Tài liệu này được biên soạn và chuẩn hóa phục vụ công tác kỹ thuật, chuyển giao và vận hành hệ thống Ingestion & Data Lakehouse cấp Doanh nghiệp.*
