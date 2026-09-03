# 🚚 Fleet Maintenance & Repair Real-Time Data Platform

An end-to-end, production-grade **Data Engineering Platform** designed for logistics fleet management, service station (Head) matching, real-time telemetry streaming, transaction Change Data Capture (CDC), data warehousing, and live executive dashboard serving.

> 🚀 **Deployment Architecture**: Built on a **3-Node Bare-Metal/VM Ubuntu Cluster** (`master`, `slave1`, `slave2`) with **Hadoop Multinode HDFS**, **Multi-Broker Kafka**, **Debezium CDC**, **PySpark**, **Redis 7+**, **Airflow**, and **WebSocket Push Serving Layer**.
> 📖 **Full Design & Trade-Off Spec**: See [SYSTEM-DESIGN-SPEC.md](SYSTEM-DESIGN-SPEC.md) for detailed business requirements, data modeling, and technology selection trade-off analysis.

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph Data Sources & Producers
        A1[Truck IoT Sensors] -->|JSON Telemetry| K1[Kafka Topic: truck-telemetry]
        A2[Mobile App Booking] -->|Repair Request| K2[Kafka Topic: repair-request]
        A3[(PostgreSQL OLTP: Odoo DB)] -->|WAL Logical Replication| D1[Debezium CDC Connect Worker]
    end

    subgraph Messaging & Streaming Bus
        D1 -->|CDC Mutations| K3[Kafka CDC Topics: fleet-cdc.public.*]
        K1 & K2 & K3 --> KAFKA[Kafka Multi-Broker Cluster - 3 Nodes]
    end

    subgraph Data Lake & Real-Time Ingestion
        KAFKA -->|Structured Streaming| SP1[PySpark Streaming Engine]
        SP1 -->|Parquet year/month/day + Checkpoints| HDFS[(Hadoop Multinode HDFS Data Lake)]
    end

    subgraph High-Speed Serving & Concurrency Control
        K3 -->|CDC Sync Worker| R1[Redis GEOSEARCH & Hash Cache]
        R1 -->|Sub-ms Station Matching| M1[Head Matching Service]
        R1 -->|Lua Script Check-and-Set| M2[Atomic Slot Reservation Engine]
    end

    subgraph Data Warehousing & Orchestration
        HDFS & A3 -->|Batch Ingestion| SP2[PySpark Batch Processing]
        SP2 -->|Star Schema & SCD Type 2| DWH[(HDFS Data Warehouse)]
        AIRFLOW[Apache Airflow DAGs] -->|Weekly/Monthly/Fiscal-Year| SP2
        SP2 -->|Metrics Summary| R2[Redis Aggregate Keys]
        SP2 -->|Signal Trigger| PUB[Redis Pub/Sub Channel]
    end

    subgraph Push-Based Presentation Layer
        PUB -->|Async Event| WS[Python WebSocket Backend Server]
        R2 -->|Initial Sync| WS
        WS -->|WebSocket Push - Zero Polling| DASH[Live Executive Dashboard UI]
```

---

## 🛠️ Technology Stack & Engineering Rationale

| Layer / Technology | Role in Platform | Production Rationale |
|---|---|---|
| **Hadoop HDFS (Multinode)** | Data Lake & Data Warehouse Storage | Distributed columnar storage for long-term raw telemetry & Parquet analytical tables (`/fleet-datalake/`). |
| **Apache Kafka (Multi-Broker)** | Central Event Streaming Bus | 3-broker cluster (`RF=3`, `min.insync.replicas=2`) providing high throughput, fault tolerance, and decoupled pub-sub. |
| **Debezium PostgreSQL Connector** | Log-Based Change Data Capture (CDC) | Reads PostgreSQL WAL (`wal_level=logical`) without polling OLTP DB, zero performance impact on core business transactions. |
| **PostgreSQL 15+** | Source OLTP DB (Odoo Simulation) | Configured with `REPLICA IDENTITY FULL` to record complete before-images for SCD Type 2 & financial delta auditing. |
| **Apache Spark (Streaming & Batch)** | Distributed Data Processing Engine | Handles Structured Streaming ingestion into HDFS and Batch Star Schema / SCD Type 2 transformation jobs. |
| **Redis 7+** | Serving Store & Concurrency Control | (1) Sub-ms station matching using `GEOSEARCH`; (2) 100% zero-overbooking slot reservation using atomic Lua Scripts; (3) Pub/Sub event bus. |
| **Apache Airflow** | Workflow Orchestration | Scheduled DAGs managing Weekly, Monthly, Quarterly, and Fiscal-Year batch aggregations with dependency management. |
| **Python WebSockets & AsyncIO** | Push-Based Serving Backend | Listens to Redis Pub/Sub and pushes JSON metric updates to browser clients via WebSockets, eliminating UI database polling. |
| **HTML5 / CSS3 / Chart.js** | Executive Live Dashboard UI | Modern glassmorphic dashboard receiving real-time push events and rendering animated financial & inventory charts. |

---

## Key Feature Highlights & Architectural Patterns

### 1. Log-Based CDC with Full Before-Image (`REPLICA IDENTITY FULL`)
- PostgreSQL is configured with `wal_level = logical`.
- Tables (`customers`, `heads`, `parts_inventory`, `work_orders`, `invoices`) are set to `REPLICA IDENTITY FULL`.
- **Why?** Guarantees Debezium CDC payloads contain complete `before` images during `UPDATE` and `DELETE` operations. This enables:
  - **SCD Type 2 Customer History**: Spark can close old customer state records without querying PostgreSQL.
  - **Cache Invalidation in Redis**: Redis CDC Sync Worker extracts old coordinates on `DELETE` to run `ZREM geo:heads head_{id}` cleanups.

### 2. Sub-Millisecond Station Matching & Zero-Overbooking Concurrency
- **Station Matching (`serving/services/head_matching_service.py`)**: Uses Redis `GEOSEARCH` to find repair stations within radius $R$ km and checks inventory stock in Redis Hash in **< 1ms**.
- **Atomic Slot Reservation (`serving/services/slot_reservation.py`)**: Uses a **Redis Lua Script** to perform atomic Check-and-Set against station capacity (`head:{id}:slots:{date}:{hour}`). Evaluates atomically inside single-threaded Redis, guaranteeing **0% overbooking under high concurrency**.

### 3. PySpark Structured Streaming to HDFS Data Lake
- Reads JSON streams from `truck-telemetry` and `repair-request` Kafka topics.
- Applies explicit `StructType` schemas to prevent Schema Drift.
- Writes partitioned Parquet files (`year=YYYY/month=MM/day=DD`) to HDFS (`hdfs:///fleet-datalake/raw/`) with HDFS Checkpointing for Exactly-Once ingestion semantics.

### 4. Data Warehouse Star Schema & SCD Type 2 Customer Dimension
- **Fact Tables**: `fact_repair_service_revenue`, `fact_parts_sales`, `fact_inventory_movement`.
- **Dimension Tables**: `dim_customer` (SCD Type 2 with Surrogate Keys & effective/expiration dates), `dim_head`, `dim_component`, `dim_date` (with Fiscal Year attributes).
- **Airflow DAGs**: Scheduled workflows for Weekly, Monthly, and Fiscal-Year (Oct 01 - Sep 30) reporting.

### 5. Push-Based Serving Layer (Zero-Polling)
- Replaces traditional REST API Polling with an Event-Driven Push Model.
- Spark/Airflow completion $\rightarrow$ Redis String Key $\rightarrow$ Redis Pub/Sub Signal $\rightarrow$ Async Python WebSocket Server $\rightarrow$ Executive Live Dashboard.

---

## 📂 Repository Directory Layout

```
fleet-platform/
├── infra/                          # Infrastructure installation & configuration scripts
│   ├── kafka/                      # Kafka multi-broker configs & systemd guides
│   ├── postgres/                   # PostgreSQL WAL & pg_hba configs
│   ├── debezium/                   # Debezium Connect worker & connector JSON configs
│   ├── redis/                      # Redis 7+ configs & deployment guides
│   ├── spark/                      # Spark standalone cluster & HDFS integration configs
│   ├── airflow/                    # Airflow systemd & postgres metadata configs
│   └── scripts/
│       └── verify-all-infra.sh     # Infrastructure health-check script
│
├── oltp/                           # Source OLTP Database & CDC Pipeline
│   ├── sql/
│   │   ├── 01_schema.sql           # DDL: 7 tables + REPLICA IDENTITY FULL
│   │   ├── 02_seed_data.sql        # Realistic Vietnamese seed data
│   │   └── 03_cdc_permissions.sql  # Debezium user grants & publication setup
│   └── scripts/
│       ├── data_producer.py        # Python script simulating continuous Odoo transactions
│       └── verify_cdc.py           # Automated CDC verification & payload inspection script
│
├── streaming/                      # Real-Time Telemetry & Repair Request Ingestion
│   ├── schemas/
│   │   └── telemetry_schemas.py    # PySpark StructType explicit schemas
│   ├── jobs/
│   │   └── streaming_telemetry_ingestion.py # PySpark Structured Streaming job to HDFS Parquet
│   └── producers/
│       └── telemetry_mock_producer.py       # Continuous truck telemetry & DTC code generator
│
├── serving/                        # High-Speed Serving & Concurrency Engine
│   ├── workers/
│   │   └── redis_cdc_sync.py       # Python worker syncing CDC events -> Redis GEO & Hash
│   ├── services/
│   │   ├── head_matching_service.py # Sub-ms station search via Redis GEOSEARCH
│   │   └── slot_reservation.py     # Atomic Lua Script slot reservation engine
│   └── tests/
│       └── test_redis_matching.py  # Concurrency test suite (50 parallel threads test)
│
├── analytics/                      # Data Warehousing & Airflow Batch Pipeline
│   ├── jobs/
│   │   ├── scd2_customer_dimension.py # PySpark SCD Type 2 customer pipeline
│   │   └── batch_dwh_aggregation.py   # PySpark Batch financial/inventory aggregator
│   └── dags/
│       ├── dag_weekly_report.py    # Airflow Weekly DAG
│       ├── dag_monthly_report.py   # Airflow Monthly DAG
│       └── dag_fiscal_year_report.py # Airflow Fiscal-Year DAG (Oct 01)
│
├── websocket_backend/              # Push-Based Serving Layer
│   ├── websocket_serving_backend.py # Async Python WebSocket Server (Port 8765)
│   └── dashboard_mockup.html       # Executive Live Dashboard (Chart.js + Glassmorphism UI)
│
├── .env                            # Cluster environment variables & hostnames
├── requirements.txt                # Python dependencies
└── README.md                       # Repository Documentation
```

---

## ⚡ Quickstart & Deployment Steps

### 1. Prerequisites
- **3 Ubuntu Machines / VMs**: `master`, `slave1`, `slave2` with SSH keys configured.
- Python 3.10+ installed on all nodes.

### 2. Infrastructure Setup & Verification
Follow the step-by-step setup guides inside `infra/`:
```bash
# Verify all cluster services are running
bash infra/scripts/verify-all-infra.sh
```

### 3. Initialize OLTP Schema & CDC Pipeline
```bash
# 1. Initialize OLTP Schema & Seed Data on master
psql -h master -U fleet_app -d fleet_oltp -f oltp/sql/01_schema.sql
psql -h master -U fleet_app -d fleet_oltp -f oltp/sql/02_seed_data.sql
sudo -u postgres psql -d fleet_oltp -f oltp/sql/03_cdc_permissions.sql

# 2. Register Debezium CDC Connector
curl -X POST http://master:8083/connectors \
  -H "Content-Type: application/json" \
  -d @infra/debezium/config/fleet-cdc-connector.json

# 3. Start Redis CDC Sync Worker
python3 serving/workers/redis_cdc_sync.py &
```

### 4. Start Real-Time Telemetry Streaming
```bash
# 1. Start PySpark Streaming Ingestion Job to HDFS
spark-submit \
  --master spark://master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/jobs/streaming_telemetry_ingestion.py &

# 2. Start Mock Telemetry Producer
python3 streaming/producers/telemetry_mock_producer.py --interval 2 --trucks 20 --rounds 100 &
```

### 5. Launch Concurrency & Matching Tests
```bash
# Test sub-ms matching & 50 parallel reservation requests
python3 serving/tests/test_redis_matching.py
```

### 6. Launch WebSocket Push Backend & Executive Dashboard
```bash
# 1. Start Async WebSocket Server
python3 websocket_backend/websocket_serving_backend.py --port 8765 &

# 2. Open dashboard in browser
# Open websocket_backend/dashboard_mockup.html in Chrome/Firefox

# 3. Trigger batch DWH aggregation to test real-time push update
python3 analytics/jobs/batch_dwh_aggregation.py --granularity monthly
```

---

## 📜 License & Portfolio Author
- Project Architecture & Codebase created for **Data Engineer Portfolio & Interview Demonstrations**.
