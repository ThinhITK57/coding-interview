# Tickets: Fleet Data Platform — Param-Driven Multi-Environment Architecture

Xây dựng kiến trúc tham số hóa đa môi trường (local/dev/staging/prod) cho Fleet Data Platform, giúp 1 codebase duy nhất chạy trên mọi hạ tầng chỉ bằng cách chuyển cờ `--env` hoặc biến `APP_ENV`.

Work the **frontier**: any ticket whose blockers are all done.

## Ticket 1 — Config-as-Code: Kiến trúc tham số hóa đa môi trường

**What to build:** Hệ thống quản trị cấu hình trung tâm bằng Dataclass + YAML cho phép bất kỳ Job Spark/Python nào chạy ở Local, Dev, Staging, Prod chỉ bằng cách truyền `APP_ENV=prod` hoặc `--env local`. Mọi hardcoded IP, password, path tĩnh bị loại bỏ khỏi source code.

**Blocked by:** None — can start immediately.

- [x] Tạo thư mục `configs/` với `base.yaml`, `local.yaml`, `dev.yaml`, `prod.yaml`
- [x] Xây dựng `src/core/config.py` với `AppConfig` dataclass + `load_config()` hỗ trợ 3 tầng ưu tiên: YAML → Env YAML → OS Env Vars
- [x] Phân giải `${VAR_NAME}` trong YAML từ biến môi trường OS (cho prod passwords)
- [x] Hàm `create_spark_session(cfg)` tạo SparkSession chuẩn hóa từ config
- [x] CLI `python -m src.core.config` để kiểm tra cấu hình nhanh từ Terminal
- [x] Unit tests `tests/test_config.py` cover env resolution, deep merge, local/prod loading

## Ticket 2 — Local-First Sandbox: Docker Compose + Makefile

**What to build:** Dựng hạ tầng phụ thuộc hoàn chỉnh trên máy dev (PostgreSQL, Redis, Redpanda/Kafka, MinIO) qua Docker Compose, kèm Makefile tự động hóa toàn bộ thao tác qua Terminal (up/down/test/run/config).

**Blocked by:** Ticket 1.

- [x] `docker/docker-compose.local.yaml` với PostgreSQL 14, Redis 7, Redpanda, MinIO
- [x] SQL init scripts tự động tạo schema OLTP + seed data khi container khởi động lần đầu
- [x] SQL init script tạo sẵn Hive Metastore database
- [x] `Makefile` với các target: `up`, `down`, `clean`, `status`, `smoke-test`, `topics`, `test`, `lint`, `config`, `run-scd2`, `run-agg`
- [x] Smoke test script kiểm tra kết nối tới toàn bộ 4 mock services

## Ticket 3 — Decoupled Compute: Tách logic ETL khỏi Airflow DAGs

**What to build:** Tái cấu trúc toàn bộ PySpark jobs (SCD2, DWH Aggregation, Streaming Telemetry) thành Containerized CLI Applications độc lập. DAG Airflow chỉ gọi `KubernetesPodOperator` hoặc `SparkSubmitOperator`.

**Blocked by:** Ticket 2.

- [ ] Migrate `scd2_customer_dimension.py` → `src/jobs/scd2_customer_job.py` sử dụng `load_config()`
- [ ] Migrate `batch_dwh_aggregation.py` → `src/jobs/batch_dwh_job.py` sử dụng `load_config()`
- [ ] Viết `Dockerfile` multi-stage build cho Compute Engine
- [ ] DAG Airflow mới sử dụng `KubernetesPodOperator` truyền `APP_ENV` động
- [ ] Chạy thử E2E thành công trên local sandbox

## Ticket 4 — CI/CD Pipeline: Kiểm thử & Deploy theo nhánh

**What to build:** Pipeline CI/CD tự động: push `feature/*` → Lint + Test; merge `develop` → Deploy Dev; merge `main` → Deploy Prod.

**Blocked by:** Ticket 3.

- [ ] Stage 1: Lint (`flake8`, `sqlfluff`, `gitleaks`)
- [ ] Stage 2: Unit tests chạy SparkSession local < 60s
- [ ] Stage 3: Build Docker image gắn tag Commit SHA
- [ ] Stage 4: Deploy Helm chart / K8s Manifest theo môi trường

## Ticket 5 — Observability: Prometheus + Grafana Metrics Dashboard

**What to build:** Gắn thư viện đo lường vào PySpark jobs, đẩy metrics (records_processed, job_duration, disk_spill) về Prometheus Pushgateway, hiển thị trên Grafana.

**Blocked by:** Ticket 4.

- [ ] Decorator `@track_execution_metrics` cho các hàm xử lý dữ liệu
- [ ] Metrics: `records_processed_total`, `job_duration_seconds`, `disk_spill_bytes`
- [ ] Grafana Dashboard JSON cho 4 môi trường
- [ ] Cảnh báo Telegram/Slack khi job vượt ngưỡng 200% thời gian trung bình
