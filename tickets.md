# Tickets: Fleet Maintenance & Repair Data Platform

Building the full end-to-end Data Platform codebase, infrastructure configs, streaming/batch pipelines, CDC, Redis serving store, and WebSocket backend.

Source Spec: [de-interview-prep-fleet-platform.md](file:///f:/thinhnt/Finance/coding-interview-university/de-interview-prep-fleet-platform.md)
Domain Context: [CONTEXT.md](file:///f:/thinhnt/Finance/coding-interview-university/CONTEXT.md)

> **Constraint**: Tất cả platform được cài đặt trực tiếp trên Ubuntu (KHÔNG dùng Docker). Hadoop chạy multinode.

Work the **frontier**: any ticket whose blockers are all done.

---

## 1. Environment & Infrastructure Setup (Direct Ubuntu Installation)

**What to build:** Cài đặt và cấu hình toàn bộ infrastructure trực tiếp trên Ubuntu:
- **Hadoop HDFS Multinode** (1 NameNode + N DataNodes) — Data Lake lưu Parquet
- **Kafka & Zookeeper** — Message bus trung tâm
- **PostgreSQL** (Odoo OLTP simulation, WAL `wal_level=logical`)
- **Debezium Connect** (Kafka Connect worker + Debezium PostgreSQL connector)
- **Redis** (v7+)
- **Apache Spark** (Master & Workers, kết nối HDFS)
- **Apache Airflow** (Postgres Metadata DB + Webserver + Scheduler)
- **Backend** (Python WebSocket server)

**Blocked by:** None — can start immediately.

- [x] Hadoop HDFS multinode setup: `core-site.xml`, `hdfs-site.xml`, `workers` file, format NameNode, start HDFS cluster ✅ (user confirmed: 1 master + 2 slaves, MapReduce tested)
- [x] Kafka multi-broker cluster (3 brokers: master, slave1, slave2) + Zookeeper: `infra/kafka/config/server.properties`, `setup-broker.sh`, `create-topics.sh`, `INSTALL-KAFKA-UBUNTU.md`
- [x] Postgres setup: `infra/postgres/config/postgresql.conf`, `pg_hba.conf`, `INSTALL-POSTGRES-UBUNTU.md`
- [x] Debezium Connect worker: `infra/debezium/config/connect-distributed.properties`, `fleet-cdc-connector.json`, `INSTALL-DEBEZIUM-UBUNTU.md`
- [x] Redis configuration: `infra/redis/config/redis.conf`, `INSTALL-REDIS-UBUNTU.md`
- [x] Spark standalone cluster: `infra/spark/config/spark-defaults.conf`, `spark-env.sh`, `INSTALL-SPARK-UBUNTU.md`
- [x] Airflow installation: `infra/airflow/config/airflow.cfg`, `INSTALL-AIRFLOW-UBUNTU.md`
- [x] Environment variables file (`.env`) with credentials, hostnames, port mappings — `.env`
- [x] Verification script: healthcheck tất cả services đang chạy — `infra/scripts/verify-all-infra.sh`

---

## 2. OLTP Schema & Log-Based CDC Pipeline (Postgres + Debezium + Kafka)

**What to build:** Odoo OLTP database schema (`heads`, `parts_inventory`, `customers`, `work_orders`, `invoices`) and Debezium PostgreSQL CDC connector configuration publishing mutations into Kafka topics (`fleet-cdc.public.*`).

**Blocked by:** Ticket 1 (Environment & Infrastructure)

- [x] DDL schema: 7 bảng (heads, customers, components, parts_inventory, work_orders, work_order_items, invoices) + REPLICA IDENTITY FULL — `oltp/sql/01_schema.sql`
- [x] Seed data thực tế (10 Heads, 15 components, 8 customers, 45 inventory, 15 work orders, 8 invoices) — `oltp/sql/02_seed_data.sql`
- [x] CDC permissions + publication — `oltp/sql/03_cdc_permissions.sql`
- [x] Tạo CDC topics cho 2 bảng mới (components, work_order_items) trên Kafka — `infra/kafka/scripts/create-topics.sh`
- [x] Đăng ký Debezium CDC connector via REST API — `infra/debezium/config/fleet-cdc-connector.json`
- [x] Data Producer mô phỏng nghiệp vụ (work orders, inventory, SCD, invoices) — `oltp/scripts/data_producer.py`
- [x] CDC verification script (connector status, event format, REPLICA IDENTITY check) — `oltp/scripts/verify_cdc.py`

---

## 3. Real-Time Telemetry & Repair Request Streaming (Spark Structured Streaming)

**What to build:** PySpark Structured Streaming job consuming raw Kafka topics `truck-telemetry` and `repair-request`:
- Writes raw payload to **HDFS Data Lake** (`hdfs:///fleet-datalake/raw/...`) in Parquet format partitioned by `year/month/day`.
- Extracts location (`latitude`, `longitude`) and issue details for Head matching.

**Blocked by:** Ticket 1 (Environment)

- [x] PySpark Structured Streaming script — `streaming/jobs/streaming_telemetry_ingestion.py`
- [x] Schema definition for telemetry & repair requests — `streaming/schemas/telemetry_schemas.py`
- [x] Parquet write configuration with HDFS checkpointing & year/month/day partitioning — `streaming/jobs/streaming_telemetry_ingestion.py`
- [x] Telemetry & Repair Mock Producer script — `streaming/producers/telemetry_mock_producer.py`
- [x] Execution, HDFS verification & Interview Q&A Guide — `streaming/TICKET-3-GUIDE.md`

---

## 4. Multi-Role Redis Sync & Matching Engine (GEO + Hash + Atomic Reservation)

**What to build:**
1. **Redis CDC Sync Worker**: Python worker consuming Debezium CDC events from Kafka to update Redis GEO (`geo:heads`) and Redis Hash (`head:parts:{head_id}`).
2. **Matching Engine**: Python module querying Redis GEO for Heads within N km radius + checking parts availability & pricing in Redis Hash.
3. **Atomic Slot Reservation**: Concurrency control module using Redis atomic Lua script / `SETNX` with TTL to prevent booking overcapacity.

**Blocked by:** Ticket 2 (OLTP & CDC)

- [x] Redis CDC Sync Worker script — `serving/workers/redis_cdc_sync.py`
- [x] Matching Engine module using Redis GEOSEARCH & Hash — `serving/services/head_matching_service.py`
- [x] Atomic Slot Reservation script with Lua script & TTL — `serving/services/slot_reservation.py`
- [x] Unit & Concurrency test suite verifying latency & 0% overbooking — `serving/tests/test_redis_matching.py`
- [x] Execution & Interview Q&A Guide — `serving/TICKET-4-GUIDE.md`

---

## 5. Data Warehouse Star Schema & SCD Type 2 Batch Pipeline (Spark Batch + Airflow)

**What to build:**
1. **Star Schema Data Warehouse**: PySpark scripts defining Fact tables (`fact_repair_service_revenue`, `fact_parts_sales`, `fact_inventory_movement`) and Dimension tables (`dim_customer` with SCD Type 2, `dim_head`, `dim_component`, `dim_date`).
2. **PySpark SCD Type 2 Pipeline**: Parquet merge script updating customer status history on **HDFS**.
3. **Airflow DAGs**: Scheduled DAGs for Weekly, Monthly, Quarterly, Fiscal-Year, and Year-End reporting aggregations.
4. **Redis Aggregates & Pub/Sub Publisher**: Write aggregate results to Redis keys `report:agg:*` and publish notification to Redis channel `channel:report-updates`.

**Blocked by:** Ticket 2 (OLTP Schema), Ticket 3 (Data Lake Ingestion)

- [x] PySpark SCD Type 2 logic script — `analytics/jobs/scd2_customer_dimension.py`
- [x] PySpark Batch DWH Aggregation script — `analytics/jobs/batch_dwh_aggregation.py`
- [x] Airflow DAGs (Weekly, Monthly, Fiscal Year) — `analytics/dags/dag_weekly_report.py`, `dag_monthly_report.py`, `dag_fiscal_year_report.py`
- [x] Redis Aggregates & Pub/Sub Publisher module — integrated into `analytics/jobs/batch_dwh_aggregation.py`
- [x] Execution & DWH Interview Q&A Guide — `analytics/TICKET-5-GUIDE.md`

---

## 6. Push-Based Serving Layer (Redis Pub/Sub -> WebSocket Backend)

**What to build:** Asynchronous Python WebSocket server (using `websockets` & `aioredis`):
- Subscribes to Redis Pub/Sub channel `channel:report-updates`.
- Upon notification, reads updated aggregate metrics from Redis and pushes JSON payload down to connected dashboard WebSocket clients.
- On client reconnection, fetches current state from Redis to sync immediately.

**Blocked by:** Ticket 5 (Data Warehouse & Redis Publisher)

- [x] Asynchronous Python WebSocket Server — `websocket_backend/websocket_serving_backend.py`
- [x] Client Reconnection & Initial State Sync handler — `websocket_backend/websocket_serving_backend.py`
- [x] Standalone HTML/JS Executive Live Dashboard mockup with Chart.js — `websocket_backend/dashboard_mockup.html`
- [x] Operational Guide & Push vs Pull Interview Q&A — `websocket_backend/TICKET-6-GUIDE.md`

---
