# API Ingestion & GenBI Data Platform

> **Adaptive Enterprise ELT Framework**  
> Quản lý thu thập dữ liệu REST API quy mô lớn, xử lý làm phẳng và kiểm tra chất lượng bằng Apache Spark 2.3.2, lưu trữ đa tầng (MinIO Raw Backup, Trino DB1 Sandbox, Trino DB2 Clean qua Hive Metastore), đồng bộ tầng ngữ nghĩa dbt và cung cấp tri thức cho hệ thống GenBI (AI LLM Text + Charts).

---

## 📌 Tổng Quan Hệ Thống

Dự án được thiết kế theo nguyên tắc **Metadata-Driven** và **Deep Module**, mang lại khả năng thích ứng cao khi hệ thống mở rộng hoặc khi schema database có sự thay đổi:

- **Orchestration:** Prefect 3 HQ (Quản lý flow, task retries, concurrency limits, artifacts reporting).
- **Ingestion Engine:** Driver-Bounded HTTP Client với Token-Bucket Rate Limiter, Exponential Backoff + Jitter, Circuit Breaker và Atomic Checkpoint.
- **Processing Engine:** Apache Spark 2.3.2 xử lý đệ quy Flattening JSON phi cấu trúc, Deduplication đa phiên bản và kiểm định chất lượng dữ liệu (Data Quality & DLQ).
- **Tri-Storage Persistence:**
  1. *MinIO:* S3-compatible raw immutable backup (`raw_backup/*.json.gz`).
  2. *Trino DB1:* Database cá nhân (`personal_raw`) dùng thử nghiệm thuật toán và phát triển ETL.
  3. *Trino DB2:* Database tổng (`global_clean`) chuẩn hóa và tối ưu truy vấn thông qua Hive Metastore.
- **dbt & GenBI Enablement:** Xây dựng mô hình Star Schema và dbt Semantic Layer (rich meta tags, chart roles, synonyms) giúp AI LLM trả về câu trả lời gồm cả văn bản phân tích và biểu đồ trực quan.

---

## 📚 Tài Liệu Kỹ Thuật Chi Tiết

Mọi chi tiết về thiết kế kiến trúc và lộ trình triển khai được lưu trữ đầy đủ trong thư mục `docs/`:

1. 📖 **[Kiến Trúc Tổng Thể (Architecture Blueprint)](docs/ARCHITECTURE_BLUEPRINT.md):**
   - Tư duy thiết kế khung thích ứng (Adaptive Framework Mindset).
   - Sơ đồ luồng dữ liệu chi tiết từ API đến GenBI.
   - Lời giải cho 5 bài toán kỹ thuật cốt lõi: Bảo vệ Connection Pool, Lưu trữ 3 nơi, Race condition Fact-Dim trên Spark 2.3.2, Phân loại DLQ, và Multi-Plan scaling.
   - Chuẩn tích hợp dbt Semantic Layer cho AI sinh biểu đồ.

2. 📋 **[Lộ Trình và Danh Sách Tickets (Roadmap & Tickets)](docs/ROADMAP_AND_TICKETS.md):**
   - **Ticket 01:** Docker-compose & Prefect HQ Remote Integration.
   - **Ticket 02:** Incremental Window Pulling & 4-Endpoint Adaptive Configuration.
   - **Ticket 03:** Tri-Storage Sink Engine (MinIO Backup + Trino DB1 + Trino DB2).
   - **Ticket 04:** Spark 2.3.2 Idempotent Dedup, Race Condition & DLQ Router.
   - **Ticket 05:** dbt Semantic Layer Modeling & GenBI Knowledge Context Pack.

---

## 🚀 Khởi Động Nhanh (Local & Docker)

### 1. Cấu hình môi trường
Sao chép và thiết lập các biến môi trường:
```bash
cp .env.example .env  # hoặc thiết lập PREFECT_API_URL, PROJECT_API_KEY
```

### 2. Kiểm tra nhanh ở chế độ Dry-Run (Local)
```bash
python prefect_flow.py --endpoint entity_query --env dev --dry-run
```

### 3. Khởi chạy bằng Docker-compose (Kết nối Prefect HQ)
```bash
docker-compose up -d
```
Worker sẽ tự động kết nối với Prefect HQ server theo địa chỉ `PREFECT_API_URL` và lắng nghe các lệnh điều phối.

---

## 📂 Cấu Trúc Thư Mục

```text
api_ingestion/
├── auth/             # Quản lý TokenAuth và xác thực API
├── checkpoint/       # Lưu vết vị trí cào dữ liệu an toàn (Atomic write-rename)
├── client/           # HTTP Client và ResilientHTTPClient
├── config/           # Data classes & Loader xác thực cấu hình config.json
├── docs/             # Tài liệu kiến trúc & Lộ trình triển khai (Blueprint, Tickets)
├── exceptions/       # Hệ thống ngoại lệ tùy biến (RateLimit, CircuitBreaker,...)
├── ingestion/        # Extractor điều phối kéo dữ liệu và Writer ghi file
├── monitoring/       # JobTracker đo lường tiến độ và MetricsCollector
├── pagination/       # Chiến lược phân trang (OffsetPaginator hỗ trợ nested body)
├── quality/          # SchemaValidator, StatisticsProfiler, QualityReport
├── reliability/      # RateLimiter, RetryExecutor, CircuitBreaker
├── transform/        # SparkSessionFactory, JSONFlattener, DocstringRegistry
├── prefect_flow.py   # Flow điều phối chính bằng Prefect 3
├── config.json       # Cấu hình API endpoints và tham số pipeline
└── README.md         # Tài liệu hướng dẫn này
```
