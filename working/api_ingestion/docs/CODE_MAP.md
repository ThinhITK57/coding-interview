# BẢN ĐỒ ĐIỀU HƯỚNG & TRA CỨU MÃ NGUỒN (CODE MAP & QUICK SEARCH INDEX)

> **Mục đích:** Tài liệu này là bản đồ chỉ đường (Index Guide) giúp bạn tìm kiếm mã nguồn trong vòng **5 giây**, biết chính xác file nào cần sửa khi muốn nâng cấp logic, fix bug, hoặc tinh chỉnh cấu hình khi làm việc với BA và hệ thống thực tế.

---

## 1. MỤC LỤC TRA CỨU NHANH THEO TÌNH HUỐNG (QUICK SEARCH INDEX)

| Bạn muốn làm gì? | File cần xem / chỉnh sửa | Class / Hàm quan trọng |
| :--- | :--- | :--- |
| **1. Cập nhật bảng và danh sách trường từ BA (Sáng mai)** | [`tables_registry.json`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/tables_registry.json)<br>[`config.json`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/config.json) | Khối `"tables"` hoặc `"endpoints"` (Chỉ cần paste trường vào, không cần sửa code Python). |
| **2. Tinh chỉnh Rate Limit, Quota ngày, Circuit Breaker** | [`reliability/rate_limiter.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/reliability/rate_limiter.py)<br>[`reliability/circuit_breaker.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/reliability/circuit_breaker.py) | `RateLimiter.acquire()`<br>`CircuitBreaker.call()` |
| **3. Sửa số lần Retry, Exponential Backoff, HTTP Error** | [`reliability/retry.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/reliability/retry.py)<br>[`client/resilient_client.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/client/resilient_client.py) | `RetryExecutor.execute()`<br>`ResilientHTTPClient.request()` |
| **4. Điều chỉnh Cửa sổ trượt (Time Window), Watermark, Lookback** | [`ingestion/extractor.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/ingestion/extractor.py)<br>[`checkpoint/checkpoint_store.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/checkpoint/checkpoint_store.py) | `Extractor._calculate_window()`<br>`CheckpointStore.get_last_watermark()` |
| **5. Phân trang API (Offset lồng trong JSON body)** | [`pagination/offset_paginator.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/pagination/offset_paginator.py) | `OffsetPaginator.get_next_params()`<br>`OffsetPaginator._inject_into_body()` |
| **6. Đổi Catalog Trino, Schema hoặc sửa câu lệnh DDL cho trino.exe** | [`storage/trino_ddl_generator.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/storage/trino_ddl_generator.py) | `TrinoDDLGenerator.generate_all_ddl()`<br>`export_sql_file()` |
| **7. Quản lý 3 kho đích: MinIO Backup, DB1 Sandbox, DB2 Clean** | [`storage/tri_storage_sink.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/storage/tri_storage_sink.py) | `TriStorageSink.persist_all()`<br>`sink_minio_raw_backup()` |
| **8. Tinh chỉnh thuật toán khử trùng lặp Spark 2.3.2** | [`transform/dedup_engine.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/transform/dedup_engine.py) | `DedupEngine.deduplicate()` (Window Ranking `row_number()`) |
| **9. Xử lý lỗi Race Condition Fact đến trước Dim** | [`transform/race_condition_router.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/transform/race_condition_router.py) | `InferredDimensionRouter.generate_inferred_stubs()` |
| **10. Kiểm tra bản ghi bẩn bị cách ly (Dead Letter Queue)** | [`storage/dlq_router.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/storage/dlq_router.py)<br>[`quality/validator.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/quality/validator.py) | `DLQRouter.route_dlq()`<br>`SchemaValidator.validate()` |
| **11. Tinh chỉnh Metadata cho AI LLM & Biểu đồ GenBI** | [`transform/docstring_registry.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/transform/docstring_registry.py)<br>[`transform/genbi_context_packer.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/transform/genbi_context_packer.py) | `DocstringRegistry.export_dbt_schema()`<br>`GenBIContextPacker.build_context_pack()` |
| **12. Chạy test tương tác trên Apache Zeppelin (%livy.spark)** | [`notebooks/zeppelin_livy_test.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/notebooks/zeppelin_livy_test.py) | Paragraphs 1 ➜ 10 (Copy paste vào Zeppelin) |
| **13. Triển khai Docker-compose, kết nối Prefect HQ Server** | [`docker-compose.yml`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/docker-compose.yml)<br>[`Dockerfile`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/Dockerfile)<br>[`.env.example`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/.env.example) | Biến `PREFECT_API_URL`, `PREFECT_WORK_POOL_NAME` |
| **14. Chạy toàn bộ Pipeline từ CLI** | [`prefect_flow.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/prefect_flow.py) | `python prefect_flow.py --endpoint muc_1 --env dev` |
| **15. Chạy Kiểm Định Tự Động & Xem Báo Cáo Thực Nghiệm** | [`scripts/run_automated_audit_and_experiments.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/scripts/run_automated_audit_and_experiments.py)<br>[`experiment_results/`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/experiment_results/) | `run_automated_audit_and_experiments.py` (Chạy 6 phases kiểm định toàn trình và sinh báo cáo) |

---

## 2. SƠ ĐỒ CÂY THƯ MỤC VÀ TRÁCH NHIỆM TỪNG FILE

```text
working/api_ingestion/
├── Dockerfile                          # [Ticket 01] Container đóng gói worker kết nối Prefect HQ
├── docker-compose.yml                  # [Ticket 01] Service orchestration, gắn volume data/ và checkpoints/
├── requirements.txt                    # [Ticket 01] Dependencies (prefect>=3.0.0, requests, urllib3, pyyaml)
├── .env.example                        # [Ticket 01] Mẫu cấu hình biến môi trường
├── config.json                         # [Ticket 02] Cấu hình 4 endpoints: muc_1, muc_2, muc_3, muc_4
├── tables_registry.json                # [Ticket 03] Mẫu khai báo danh sách bảng & trường BA cấp ngày mai
├── prefect_flow.py                     # [Ticket 01-05] Flow điều phối trung tâm & CLI entry point
│
├── config/                             # LỚP CẤU HÌNH & VALIDATION
│   ├── api_config.py                   # Dataclass cho API: Auth, RateLimit, Retry, Pagination, Endpoint
│   ├── job_config.py                   # Dataclass cho Job: ExtractionMode, IncrementalConfig, Checkpoint, Storage
│   ├── loader.py                       # Đọc JSON config, nội suy biến môi trường ${ENV_VAR}
│   └── validator.py                    # Kiểm tra tính hợp lệ của cấu hình trước khi chạy
│
├── client/                             # LỚP GIAO TIẾP HTTP
│   ├── base_client.py                  # Abstract base client interface
│   ├── http_client.py                  # urllib3 client cơ bản
│   └── resilient_client.py             # [Ticket 01] Client bọc 3 tầng: RateLimiter -> CircuitBreaker -> RetryExecutor
│
├── reliability/                        # LỚP ỔN ĐỊNH & CHỐNG NGHẼN HỆ THỐNG
│   ├── rate_limiter.py                 # [Ticket 01] Token bucket (RPM) + Counter ngày (RPD)
│   ├── retry.py                        # [Ticket 01] Exponential backoff + full jitter + Retry-After header
│   └── circuit_breaker.py              # [Ticket 01] Máy trạng thái CLOSED -> OPEN -> HALF_OPEN
│
├── checkpoint/                         # LỚP CHECKPOINT THỦY ẤN (WATERMARK)
│   └── checkpoint_store.py             # [Ticket 02] Ghi atomic, lưu watermark, khôi phục offset sau crash
│
├── pagination/                         # LỚP PHÂN TRANG API
│   ├── base_paginator.py               # Abstract paginator
│   └── offset_paginator.py             # [Ticket 02] Bơm offset/limit vào query hoặc cấu trúc lồng body.paging
│
├── ingestion/                          # LỚP KÉO DỮ LIỆU TỪ API
│   ├── extractor.py                    # [Ticket 02] Cửa sổ trượt [start, end), Lookback buffer, nội suy ${WINDOW}
│   └── writer.py                       # Ghi JSON batch và Partitioned Parquet Writer
│
├── storage/                            # LỚP LƯU TRỮ VÀ DATA LAKEHOUSE
│   ├── tri_storage_sink.py             # [Ticket 03] Ghi 3 đích: MinIO Backup + Trino DB1 Sandbox + Trino DB2 Clean
│   ├── trino_ddl_generator.py          # [Ticket 03] Tự động dịch Spark Schema sang file DDL .sql cho trino.exe
│   └── dlq_router.py                   # [Ticket 04] Định tuyến bản ghi bẩn vào thư mục Dead Letter Queue
│
├── transform/                          # LỚP XỬ LÝ & BIẾN ĐỔI SPARK 2.3.2
│   ├── spark_session.py                # [Ticket 03] Factory Spark 2.3.2, cấu hình MinIO S3A, Hive Metastore
│   ├── json_flattener.py               # Làm phẳng JSON lồng nhau thành snake_case
│   ├── dedup_engine.py                 # [Ticket 04] Khử trùng lặp Idempotent bằng Window Ranking row_number()
│   ├── race_condition_router.py        # [Ticket 04] Bù Dimension tạm thời (Inferred Stub) chống lỗi Fact-Dim
│   ├── docstring_registry.py           # [Ticket 05] Xuất dbt schema.yml kèm Rich Meta Tags (chart_role, synonyms)
│   └── genbi_context_packer.py         # [Ticket 05] Đóng gói genbi_context_pack.json nạp cho AI LLM
│
├── quality/                            # LỚP KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU
│   ├── validator.py                    # [Ticket 04] Single-pass error tagging qua F.concat_ws (Spark 2.3.2)
│   ├── statistics.py                   # Tính toán thống kê null_ratio, min, max, distinct
│   └── profiler.py                     # Đóng gói QualityReport
│
├── monitoring/                         # LỚP GIÁM SÁT & BÁO CÁO
│   ├── job_tracker.py                  # Theo dõi throughput (records/s, RPM), sinh báo cáo Markdown
│   └── metrics.py                      # Metrics collector
│
├── notebooks/                          # THỬ NGHIỆM TƯƠNG TÁC ZEPEPLIN
│   └── zeppelin_livy_test.py           # [Ticket 03-05] 10 Paragraphs chạy với %livy.spark
│
├── scripts/                            # SCRIPTS MOCK SERVER & BENCHMARK THỰC NGHIỆM
│   ├── mock_data_generator.py          # Sinh giả lập 186 trường dữ liệu Task Planview
│   ├── mock_epm_server.py              # Mock HTTP Server lắng nghe cổng 8088
│   ├── benchmark_pagination_chunks.py  # Đo lường 6 mức chunk size (10 -> 1000)
│   ├── run_spark_transform_experiment.py # Spark 2.3.2 Pipeline (Flatten -> DLQ -> Dedup -> Parquet)
│   ├── run_dbt_simulation.py           # dbt Modeling, EVM Metrics (PV, EV, AC, CPI, SPI)
│   └── run_automated_audit_and_experiments.py # Master runner kiểm thử toàn bộ 6 phases
│
├── experiment_results/                 # KẾT QUẢ VẬT LÝ VÀ BÁO CÁO THỰC NGHIỆM
│   ├── phase1_component_audit.json     # Kết quả kiểm thử 10 module (100% PASS)
│   ├── phase2_mock_server_health.json  # Trạng thái Mock Server
│   ├── phase3_pagination_benchmark.json# Bảng đo lường 6 mức limit
│   ├── phase4_spark_transform_audit.json# Số liệu Spark (1500 raw -> 30 DLQ -> 75 dup -> 1395 clean)
│   ├── phase5_dbt_evm_audit.json       # Báo cáo tài chính & sức khỏe danh mục EVM
│   └── EXECUTIVE_EXPERIMENT_AND_AUDIT_REPORT.md # Báo cáo kiểm định tổng hợp
│
└── docs/                               # TÀI LIỆU DỰ ÁN
    ├── ARCHITECTURE_BLUEPRINT.md       # Thiết kế kiến trúc tổng thể End-to-End
    ├── ROADMAP_AND_TICKETS.md          # Chi tiết 5 tickets và acceptance criteria
    ├── LEADER_DEEP_DIVE_GUIDELINE.md   # Cẩm nang giải trình thực nghiệm với Leader
    ├── CRAWLER_CODE_REVIEW_AND_DBT_BENCHMARK.md # Đánh giá code crawler & benchmark dbt
    ├── OFFLINE_IMPLEMENTATION_AND_TESTING_GUIDE.md # Hướng dẫn gõ code máy công ty
    └── CODE_MAP.md                     # File bản đồ này
```

---

## 3. LUỒNG DỮ LIỆU HOẠT ĐỘNG (END-TO-END DATA FLOW)

```
[REST API]
    │
    ▼ (Cửa sổ trượt [WINDOW_START, WINDOW_END) + Rate Limit Token Bucket)
[ingestion/extractor.py]
    │
    ├──────────────────────────────────────────┐
    ▼                                          ▼
[ĐÍCH 1: MinIO Raw Backup]            [staging/ json files]
(s3a://lakehouse/raw_backup/*.json.gz)         │
                                               ▼
                                      [transform/spark_session.py] (Spark 2.3.2)
                                               │
                                               ▼
                                      [transform/json_flattener.py] (Flatten nested)
                                               │
                                               ▼
                                      [transform/dedup_engine.py] (Window Ranking row_number() == 1)
                                               │
                                               ├──────────────────────────────────────┐
                                               ▼                                      ▼
                                  [transform/race_condition_router.py]   [quality/validator.py]
                                  (Sinh Inferred Dim Stubs cho FK)       (Single-pass concat_ws)
                                               │                                      │
                         ┌─────────────────────┴─────────────────────┐                │
                         ▼                                           ▼                ▼
             [ĐÍCH 2: Trino DB1 Sandbox]                 [ĐÍCH 3: Trino DB2 Clean]  [DLQ Storage]
             (hive.personal_raw.<table_name>)            (hive.global_clean.<table_name>) (warehouse/dlq/...)
                         │                                           │
                         └─────────────────────┬─────────────────────┘
                                               ▼
                                  [storage/trino_ddl_generator.py]
                                  (Xuất generated_ddl/*.sql cho trino.exe)
                                               │
                                               ▼
                                  [transform/genbi_context_packer.py]
                                  (Xuất genbi_context_pack_*.json cho AI LLM: Chữ + Biểu đồ ECharts)
```

---

## 4. BÍ KÍP XỬ LÝ NHANH CHO NGÀY MAI (BA HANDOVER CHEAT SHEET)

Khi BA gửi danh sách bảng và các trường, thực hiện đúng 3 bước:
1. **Bước 1:** Mở file [`tables_registry.json`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/tables_registry.json), điền tên bảng và danh sách các trường vào danh sách `tables`.
2. **Bước 2:** Chạy kiểm tra nhanh trên Zeppelin bằng cách mở [`notebooks/zeppelin_livy_test.py`](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/notebooks/zeppelin_livy_test.py), paste Paragraph 2 và Paragraph 5 để sinh DDL.
3. **Bước 3:** Mở `trino.exe`, chạy file SQL vừa sinh trong thư mục `generated_ddl/` để tạo bảng trên catalog `hive`.
4. **Bước 4:** Kích hoạt chạy pipeline:
   ```bash
   python prefect_flow.py --endpoint muc_1 --env dev
   # Hoặc chạy toàn bộ:
   python prefect_flow.py --all --env prod
   ```
