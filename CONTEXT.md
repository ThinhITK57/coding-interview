# Fleet Maintenance & Repair Data Platform — Domain Context & Ubiquitous Language

> **Domain Boundary**: Data Engineering, Telemetry Ingestion, CDC Replication, Matching Engine, Data Warehousing, and Real-time Reporting for Fleet Maintenance & Repair.

---

## 1. UBIQUITOUS LANGUAGE (Glossary)

### Core Telemetry & Ingestion
* **Telemetry Event**: Raw sensor reading emitted by on-truck IoT devices (e.g., brake wear, oil pressure, engine temp, tire pressure) sent to Kafka topic `truck-telemetry`.
* **Repair Request**: Action initiated by a fleet driver/owner detailing vehicle symptoms and required repairs, sent to Kafka topic `repair-request`.
* **Data Lake (Raw & Curated)**: HDFS multinode storage holding raw telemetry and CDC records in columnar **Parquet** format for auditability and historical analysis.

### Service Station & Matching Engine
* **Head (Service Station)**: Authorized service and repair center managed within the OLTP source system (Odoo). Has a geographic location, parts inventory, and labor pricing.
* **Parts Inventory**: Stock level and pricing of replacement components at each Head.
* **Matching Engine**: High-performance lookup process utilizing **Redis GEO** (for distance calculation) and **Redis Hash** (for parts availability & pricing) to recommend optimal Heads to drivers without querying the OLTP database.
* **Atomic Reservation**: Concurrency control mechanism using Redis atomic commands (`SETNX`/`EVAL` Lua scripts) with Time-To-Live (TTL) to prevent overbooking of repair time slots across multiple concurrent drivers.

### Change Data Capture (CDC) & Data Pipeline
* **Log-Based CDC**: Asynchronous, non-intrusive capture of database mutations (INSERT, UPDATE, DELETE) by reading the PostgreSQL Write-Ahead Log (WAL) via **Debezium** into Kafka CDC topics.
* **Multi-Consumer CDC Pattern**: Single Kafka CDC topic consumed by multiple downstream targets with different latency SLAs (near real-time Redis sync for Head matching vs. scheduled batch loads into Data Warehouse).
* **ELT (Extract-Load-Transform)**: Strategy where raw Kafka streams are loaded directly to the Data Lake first, followed by downstream transformation in Spark.

### Data Warehousing & Analytics
* **SCD Type 2 (Slowly Changing Dimension)**: Technique for tracking customer classification history (New $\rightarrow$ Existing $\rightarrow$ Annual/VIP) over time using `effective_start_date`, `effective_end_date`, and `is_current` flags without overwriting history.
* **Fact Repair Service Revenue**: Fact table capturing revenue generated from labor, diagnostic fees, and repair work orders.
* **Fact Parts Sales**: Fact table capturing revenue and margin generated from selling replacement vehicle components.
* **Fact Inventory Movement**: Fact table tracking stock inflows, outflows, and adjustments per Head.

### Serving & Reporting Layer
* **Push-Based Serving Layer**: Architecture where Spark/Airflow batch aggregations write to Redis, triggering a Redis Pub/Sub notification to an intermediary Backend Layer, which actively pushes updates down to PowerBI/Web dashboards via **WebSocket** connections.

---

## 2. BOUNDED CONTEXTS & DATA FLOW BOUNDARIES

```
[ Truck Sensor / App ] ──► (Kafka) ──► [ Spark Streaming ] ──► [ Data Lake (Parquet) ]
                                            │
                                            ▼ (Extract Location & Issue)
[ Odoo OLTP (Postgres) ] ──► (Debezium CDC) ──► (Kafka) ──► [ Redis Sync Job ] ──► [ Redis GEO + Hash ]
                                                                                      │ (Matching & Booking)
                                                                                      ▼
                                                                             [ Driver Matching Service ]

[ Data Lake + CDC Topics ] ──► [ Spark Batch Jobs (Airflow) ] ──► [ Star Schema DW ] ──► [ Redis Aggregates ]
                                                                                               │ (Pub/Sub)
                                                                                               ▼
                                                                                     [ WebSocket Backend ]
                                                                                               │ (Push)
                                                                                               ▼
                                                                                      [ PowerBI / Dashboard ]
```

---

## 3. KEY DOMAIN INVARIANTS (Rules)

1. **Zero OLTP Querying on Hot Paths**: Matching and driver search queries MUST NEVER execute SQL against Odoo PostgreSQL. All search state must be served from Redis.
2. **At-Least-Once CDC Handling**: All CDC consumers MUST handle duplicate events idempotently using natural transaction keys `(table, primary_key, transaction_lsn)`.
3. **Historical Accuracy via SCD Type 2**: Historical revenue reports MUST link to customer dimension records matching the status *at the time of transaction*, never the current overwritten status.
4. **Push-Over-Pull for Reporting**: Dashboards do NOT poll Redis directly. Updates are pushed via WebSockets immediately upon completion of batch pipeline runs.
