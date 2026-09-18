# EPM Crawler (Standardized Pure-Python Lakehouse Ingestion Service)

Một dịch vụ microservice thu thập dữ liệu Clarizen EPM đạt chuẩn doanh nghiệp: loại bỏ 100% Spark/Java JVM, quản trị Quota API ngày qua SQLite bền vững, stream dữ liệu Bronze nén `.json.gz` lên MinIO, hỗ trợ cơ chế lưu đệm và đẩy bù tách rời mô phỏng Apache Iceberg, và xuất nhật ký kiểm toán (Enterprise Observability).

Toàn bộ cấu trúc thư mục, tệp thực thi, container docker, và tên gọi deployment tuân thủ **100% quy ước của dự án `epm-crawler`**.

---

## 1. Tài Liệu Nghiệp Vụ & Kiến Trúc Chuyên Sâu

- [CHANGELOG.md](CHANGELOG.md): Báo cáo chi tiết các file Spark/DDL đã được dọn sạch, các file nâng cấp và thêm mới.
- [TECHNICAL_ANALYSIS.md](TECHNICAL_ANALYSIS.md): Báo cáo phân tích chuyên sâu về tại sao loại bỏ Spark, giải pháp Quota SQLite, kiến trúc Fallback Iceberg decoupled, và giám sát Observability.
- [CRAWL-DATA-PLAN.md](CRAWL-DATA-PLAN.md): Kế hoạch tần suất, khung giờ chạy và ước tính request của 6 nhóm thực thể EPM.

---

## 2. So Sánh Hiệu Năng & Tài Nguyên

| Tiêu chí | Bản Crawler cũ (Spark) | Bản Crawler mới (Pure Python) | Mức độ tối ưu |
| :--- | :--- | :--- | :--- |
| **Nền tảng chạy** | Java 11 JRE + Py4J + Hadoop + PySpark | Python 3.12 Slim (`requests`, `tenacity`, `minio`) | **Loại bỏ hoàn toàn JVM/Spark** |
| **Dung lượng Image** | ~2.5 GB | **< 150 MB** | **Giảm 94% dung lượng** |
| **Bộ nhớ RAM tiêu tốn** | 2.5 GB – 4.0 GB | **< 180 MB** | **Tiết kiệm 93% RAM** |
| **Thời gian khởi động** | 25 – 45 giây (chờ SparkContext) | **< 0.8 giây** | Khởi động tức thời |
| **Quản trị Quota** | In-memory (mất khi restart) | **SQLite Bền Vững** (`state/state.db`) | Quota 1.000 req/ngày bền vững qua các lần chạy |
| **Mất mạng MinIO** | Treo hoặc dừng crawl | **Decoupled Iceberg Manifest** | Crawl tiếp tục bình thường, không trễ lịch sau |
| **Trách nhiệm Lakehouse** | Tự tạo bảng Silver & Trino | **Bronze Nguyên Bản** (`.json.gz`) | Đúng chuẩn phân tầng Lakehouse |

---

## 3. Cấu Trúc Thư Mục Chuẩn Hóa

```text
epm-light-crawler/
├── .dockerignore
├── .env.epm                         # Biến môi trường kết nối API & MinIO
├── CHANGELOG.md                     # Nhật ký thay đổi & dọn dẹp mã nguồn
├── TECHNICAL_ANALYSIS.md            # Báo cáo phân tích kỹ thuật chuyên sâu
├── README.md                        # Hướng dẫn sử dụng & vận hành
├── requirements.txt                 # Phụ thuộc Python thuần (<10 libraries)
├── Dockerfile                       # Base prefecthq/prefect:3-python3.12 (Zero-Spark)
├── docker-compose.yml               # Cụm prefect-server, epm-crawler, epm-adhoc, minio
├── epm_crawler_flow.py              # Entrypoint chính serve() 11 deployments
├── docker/
│   └── entrypoint.sh                # Script entrypoint hỗ trợ 'serve' và 'adhoc'
├── epm/
│   ├── config.json                  # Cấu hình 5 API endpoints (path, rate_limit, pagination)
│   ├── prefect_flow.py              # Các flow Prefect thuần túy & CLI ad-hoc
│   ├── client/
│   │   ├── auth.py                  # TokenAuth header generator
│   │   └── resilient_client.py      # HTTP Client chống chịu lỗi, quản lý RPM & Quota
│   ├── reliability/
│   │   └── rate_limiter.py          # Rate limiter RPM + SQLite daily quota check
│   ├── ingestion/
│   │   └── extractor.py             # LightweightExtractor: watermark window & pagination
│   ├── storage/
│   │   ├── minio_sink.py            # MinIOSink: nén .json.gz Bronze + local buffer
│   │   └── state_store.py           # SQLite Store: Quota ngày, Checkpoints & Manifest queue
│   ├── monitoring/
│   │   ├── audit_tracker.py         # AuditTracker: ghi nhận telemetry & sync daily JSONL
│   │   └── metrics.py               # Data models: Batch, CrawlRunMetrics, EndpointConfig
│   └── backfill/
│       └── backfill_worker.py       # Decoupled background worker đẩy bù buffer lên MinIO
└── tests/
    ├── test_extractor.py            # Unit test Extractor, Quota, StateStore (100% pass)
    └── test_sink_and_backfill.py    # Unit test MinIOSink, BackfillWorker, Audit (100% pass)
```

---

## 4. Hướng Dẫn Vận Hành

### Chạy bằng Virtualenv / UV cục bộ:
```bash
# Cài đặt môi trường Python 3.12
uv venv --python 3.12
uv pip install -r requirements.txt

# Chạy toàn bộ test suite (10/10 tests PASS)
python -m pytest tests/ -v

# Kiểm tra đăng ký 11 deployments (Dry-run mode)
python epm_crawler_flow.py --dry-run

# Chạy crawler daemon giữ lịch
python epm_crawler_flow.py

# Chạy lệnh ad-hoc thủ công
python epm/prefect_flow.py --endpoint tasks --mode incremental
python epm/prefect_flow.py --dag --mode full
python epm/prefect_flow.py --backfill
python epm/prefect_flow.py --status
```

### Chạy bằng Docker Compose:
```bash
# 1. Khởi động cụm Prefect Server 3 + Crawler Daemon
docker compose up -d

# Xem logs tiến trình crawler
docker compose logs -f epm-crawler

# 2. Truy cập Dashboard UI của Prefect
# Mở trình duyệt tại: http://localhost:4200

# 3. Khởi động kèm MinIO cục bộ (nếu cần)
docker compose --profile with-minio up -d

# 4. Thực thi lệnh ad-hoc qua container epm-adhoc
docker compose run --rm epm-adhoc adhoc --endpoint tasks --mode incremental
docker compose run --rm epm-adhoc adhoc --dag --mode full
docker compose run --rm epm-adhoc adhoc --backfill
docker compose run --rm epm-adhoc adhoc --status
```
