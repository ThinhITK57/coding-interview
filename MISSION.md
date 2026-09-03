# Mission: Production Enterprise ELT Platform & Data Platform Architecture

## 1. Enterprise Production ELT Mission (Current Company Assignment)
### Why
Thiết kế, xây dựng và tối ưu hệ thống ELT data pipeline thực chiến tại công ty mới: Kéo dữ liệu API (4 mục tiêu bảo mật), làm phẳng JSON phi cấu trúc, kiểm soát chất lượng dữ liệu với Dead Letter Queue (DLQ), lưu trữ đa mục tiêu (MinIO backup + Trino Dev/Prod) và xử lý đồng thời (concurrency) giữa Fact & Dimension.

### Success looks like
- Thiết kế luồng pull API an toàn, kiểm soát connection pool và rate limiting, không gây nghẽn hệ thống.
- Pipeline Spark 2.3.2 xử lý làm phẳng JSON động và thực thi bộ Metadata Quality Rules tách luồng hợp lệ vs DLQ tự động.
- Ghi dữ liệu đồng thời vào 3 đích: MinIO Raw Backup, Trino Database Cá nhân (Sandbox/Dev), và Trino Database Tổng (Cleaned/Prod) với chi phí I/O tối ưu.
- Giải quyết triệt để bài toán đồng thời (Concurrency / Race Condition) khi Fact và Dimension cập nhật cùng lúc trong cửa sổ thời gian hẹp.
- Trả lời và bảo vệ phương án kiến trúc tự tin, sắc sảo trong buổi họp kỹ thuật ngày mai.

### Constraints
- Orchestrator: Prefect (Prefect 3.0).
- Data Engine: Apache Spark 2.3.2 (Legacy constraint, không có native MERGE INTO).
- Storage/Query: Trino (Sandbox/Prod Iceberg catalogs) + MinIO S3A Raw Backup.
- Tuân thủ bảo mật doanh nghiệp (4 mục tiêu mã hóa nội bộ).

---

## 2. Fleet Maintenance & Repair Platform Mission (Foundational Mastery)
### Objective
Build a complete, production-grade end-to-end Data Platform for Fleet Maintenance & Repair based on the project specification (`de-interview-prep-fleet-platform.md`).

### Key Learning Outcomes
1. **Infrastructure & Setup**: Configure Docker Compose with Kafka, Debezium CDC, PostgreSQL (Odoo OLTP), Redis, MinIO (Data Lake), Spark, Airflow, and WebSocket Backend.
2. **Stream Processing (Spark Structured Streaming)**: Real-time ingestion pipelines consuming Kafka telemetry & repair requests, writing raw Parquet to Data Lake.
3. **CDC & Redis Serving Store**: Deploy Debezium log-based CDC to replicate Odoo PostgreSQL changes into Kafka, sync to Redis GEO and Hash.
4. **Data Warehousing & Star Schema (Spark Batch + Airflow)**: Model facts and dimensions (SCD Type 2).
5. **Push-Based Serving Layer**: WebSocket backend subscribing to Redis Pub/Sub channels.
